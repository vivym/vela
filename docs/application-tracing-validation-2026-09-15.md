# 应用传输层追踪验收 — 2026-09-15

本次完成 Vela 应用 HTTP/gRPC 的追踪实现和现有 Collector → Tempo 的现场验证。
02:34 CST 的 Linux canary 通过 39 项检查；04:27 CST 进一步启用实际 Control
两副本的 OTLP 追踪，46 项生产进程/网关检查通过。其他应用组件尚未完成启用。
结果归入固定清单 R6，不代表真实模型任务、异步业务链路或整体生产验收完成。

09-15 后续增量：Job → Outbox → NATS → Inbox 的持久化追踪已实现，真实
PostgreSQL/三 NATS 副本的发布重试和消费重投递验证通过；生产尚未采用，
需要迁移 97。详见 [异步消息追踪](async-message-tracing-validation-2026-09-15.md)。

## 实现和代码验证

新增 `internal/tracing`：显式开关、OTLP HTTP 导出、父级采样传播、有限队列、
关闭时 flush。Control、Fleet Controller、Node Agent、Stage Worker Agent 和
ModelRuntime 的运行入口已接入启动/关闭；现有 HTTP 指标中间件、五类 gRPC
服务端及客户端接入了传输 span。TLS、身份校验、取消和消息大小限制保留。

HTTP 使用路由模板作为 span 名，不记录具体 URL、query、请求体或 Authorization。
gRPC 只记录已注册 protobuf service/method 和状态码，不记录载荷或原始错误。
跨服务只传递经过校验的 `traceparent`，排除 baggage 和 vendor tracestate。
数据库角色日志关联 trace/span ID；Prometheus 不增加这些高基数标签。

测试发现并修复两项 SDK 行为：`WithResource` 自动合并环境属性，因此在导出
边界固定资源属性；直接 defer `Span.End` 会捕获 panic 原文，因此在重新抛出
异常之前结束 span，仅保留通用错误状态。没有为了通过测试关闭异常或采样验证。

| 检查 | 结果 |
| --- | --- |
| `go test -race ./internal/tracing ./internal/telemetry -count=1` | 通过；日志关联新增断言随后单独通过 telemetry race 测试 |
| HTTP → gRPC 的真实客户端/服务端 | 相同 trace ID，正确的三层 parent span，授权 metadata 保留 |
| HTTP 流式输出、gRPC Watch 取消 | flush 不阻塞；流结束前 span 不提前结束；取消状态正确 |
| 5000 次 HTTP 请求期间 Collector 持续 503 | 请求保持 204；不等待导出；日志不泄露 Collector 响应或地址 |
| OTLP protobuf、采样和 shutdown | 通用/专用端点路径正确；根采样率为零时仍遵守已采样父级；进程 context 取消后 flush 成功 |
| 非法开关/端点/采样率 | 拒绝；错误文本不回显输入中的敏感内容 |
| 五个应用命令及 `internal/modelruntime` 既有测试 | 通过 |
| 四个 transport 包和 `internal/deploymentcontract` | 通过 |
| Linux amd64 静态 canary 构建、Python 语法、定向空白检查 | 通过 |

这些本地 Go 测试在开发机执行；下面另列真实 Linux 现场验证，避免混淆平台。

## Linux 与现有遥测服务

02:34 CST 在 `.70` / `llmpool01` 上完成验证。两个 UID 65534 进程仅监听
`127.0.0.1`，前端 HTTP 实际调用另一个进程的 gRPC，再由应用 SDK 导出至
集群现有 OTel Collector，最后查询现有 Tempo。没有另建 Collector、Tempo
或 Kubernetes 测试工作负载，没有使用真实业务身份或申请 GPU/PVC。

第一次启动因 `/run` 的 `noexec` 挂载而被拒绝，尚未发送请求且已清理。
检查挂载选项后，最终验证使用可执行的 `/var/tmp`。两次尝试的日志保留在
`.70:/opt/vela-cluster/application-traces-20260915/`，成功记录为
`canary-183357/receipt.json`。

| 场景 | HTTP | Tempo spans | 结果 |
| --- | --- | --- | --- |
| 正常认证探针 | 204 | 3 | HTTP → gRPC client → gRPC server 父子关系正确 |
| 后端主动失败 | 503 | 3 | gRPC 状态 14，三个 span 为 Error，未记录原始错误消息 |
| 错误认证探针 | 401 | 1 | 保留 HTTP 拒绝记录，没有调用后端 |

共 **39 项检查通过**；3 条 trace、7 个 span。完整返回数据不含合成秘密、
请求内容或任意环境属性。两个进程正常关闭并 flush；监听端口、临时可执行
文件和运行目录清理通过。现场没有改动主机服务、生产路由、应用配置或数据盘。

证据：

- [现场回执](evidence/application-traces-2026-09-15.json)
- [成功 trace](evidence/application-trace-success-2026-09-15.json)
- [错误 trace](evidence/application-trace-error-2026-09-15.json)
- [未授权 trace](evidence/application-trace-unauthorized-2026-09-15.json)
- [源文件与构建哈希清单](evidence/application-tracing-source-2026-09-15.json)

二进制 SHA256：`880b22de6065ca2483ea072b6758ea53435580a16bf8b6b31b2ee76956abf87f`。
源码为当前工作树的专用验证程序 `hack/trace-canary` 和实际 `internal/tracing` 包；
源文件哈希及构建命令随专项归档保存。它不是 clean HEAD 的 canonical release。

## 实际 Control 与 APISIX

04:27 CST 完成当前两个 CPU Control Pod 的配置切换。镜像使用已验证的
schema-96 digest，配置见 site overlay 的 `tracing.patch.json`：

```text
VELA_TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.monitoring.svc.cluster.local:4318
OTEL_TRACES_SAMPLER_ARG=0.1
```

默认根采样 10%，子 span 遵循有效上游 sampled 标志。三个显式环境变量纳入
Deployment 渲染；既有 16 个不可变材料引用与本次镜像保持一致。没有重新安装
Collector/Tempo 或更改网关路由。切换前确认无活动业务 Jobs、PostgreSQL 与遥测
服务健康、APISIX 已启用父级采样；服务端 dry-run 通过后逐个替换 Control Pod。

对两个 Pod 分别通过仅监听 loopback 的临时端口转发发送一次只读未认证请求；
再经 `.70/.71:30443/api/v1/jobs` 各发送一次，使用当前私有 CA 验证 HTTPS。
四次请求均为 401，全部在 Tempo 找到对应生产 Control span；两个网关请求
同时找到 APISIX span，Control 的 parent ID 精确指向网关 span。两个 Pod
均实际通过集群 DNS/网络将数据交给现有 Collector/Tempo。

共 **46 项检查通过**，4 条 trace、6 个 span，其中包括 trace ID 传播、HTTP 状态、Control span 属性
白名单、父子关系与临时转发进程清理。Control 保持 2/2 Ready，NATS 3/3 Ready。
证据见 [生产追踪](evidence/control-production-tracing-2026-09-15.json)。
随后 [9 项清单/现场核对](evidence/control-production-tracing-postcheck-2026-09-15.json)
确认镜像、完整 env、配置字节、pull Secret 和 NATS 模板与已审计输入一致。
源码/日志位于 `.70` root 私有目录 `/opt/vela-cluster/control-tracing-run-20260915/`，
部署前后模板、回执及 trace 在 `/opt/vela-cluster/control-tracing-20260915/`。
这是实际服务处理未认证请求的追踪证据；不等于认证成功的模型执行、数据库
角色日志关联、gRPC 生产身份链路或完整业务 SLO。

## R6 尚需完成

1. Control 实际 Pod 的 HTTP 追踪与环境注入已完成；继续将 Fleet、Node Agent、
   Stage Worker 与 isolated ModelRuntime 的追踪配置纳入各自经过验证的镜像/
   配置版本，验证生产 gRPC 身份链路和到 Collector 的网络。配置模板和步骤见
   [追踪 runbook](runbooks/observability-traces.md)。此次 host 回环探针不能代替这些检查。
2. Job/Outbox/NATS/Inbox 的持久化关联已实现并在独立环境验证，待生产迁移和
   镜像采用；调度、StageRun 和 runtime 的逐任务关联仍待完成。长连接只产生
   一个运输层 span，不能当成每个任务的阶段追踪。
3. 实际业务通过 APISIX 完成认证、调度、模型执行、产物校验与存储，取得业务
   SLI/SLO、Stage residency 和必要的 Launch Receipt。此次没有执行真实模型任务，
   也没有把较早的 APISIX trace 检查拼接为新的完整业务证据。

R1 的 `.19` 连通性/`.59` 缺卡，R3 的事件流容量与 Outbox replay，R4 的正式
release，R5 的空间和留存要求继续保留在固定清单。GitLab 对接、外部通知和
worker 磁盘初始化遵循用户的暂缓决定。
