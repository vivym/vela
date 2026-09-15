# Stage 与 Runtime 逐任务追踪验证 — 2026-09-15

本次补齐 Job → StageAssignment → worker → ModelRuntime 的逐任务诊断关联，
并覆盖 worker 日志重新打开、续租、重连、旧任务输出重试和 runtime 超时取消。
代码和 Linux 候选二进制已准备，**尚未部署到生产，不关闭整体 R6**。

## 实现和边界

数据库迁移 98 在现有授权 assignment snapshot 函数外增加一层读取：先执行原
command/claim 校验，再读取该 Job 的 `origin_trace_parent`。没有给 worker SQL
角色增加 jobs 表 SELECT 权限，旧内部 wrapper 的直接执行权限也保持撤销。
它依赖迁移 97；生产数据库仍为 CNPG 的 `app`，schema 96。

`StageAssignment.origin_trace_parent` 是可选的 55 字节 W3C v00 字段，不在
签名授权或 execution-spec digest 内。已完成的 Acquire 仍返回原始持久化
protobuf 字节；重试不重建授权，也不以新请求 trace 替换原 Job trace。
非法、超长、零 ID、非规范 flags 和大小写被丢弃，不拒绝合法执行。

worker 的首次有效入场把诊断 parent 与原 execution identity 一起持久化。
续租和同一 assignment 回放不会替换它；新的 StreamAgent 从重新打开的
日志恢复 parent。旧任务进入 Pending history 后，输出落盘重试仍从该任务
记录读取 parent，不会使用较新任务的 trace。历史回收仍遵守既有终态证明。

| Span | 实际范围 |
| --- | --- |
| `vela.stage.assignment` | 授权 snapshot 读取后，生成 assignment 到持久化完成 |
| `vela.stage.start` | 输入处理、Runtime Prepare/Start barrier、Control Start ACK 和本地入场收尾 |
| `vela.stage.heartbeat/status/reattach/stop/fail` | 每次对应 worker 操作 |
| `vela.stage.seal/materialize` | 本地封存或一次 L2/Control 提交尝试；每次重试独立记录 |
| `vela.runtime.prepare/start/cancel/status/seal` | 验证授权后的 Runtime Service 操作，包括后端调用；记录应用 decision |
| `vela.runtime.deadline_cancel` | 以首次 Prepare 为 parent 的独立 watchdog 取消调用 |

同一 Job 的各 Stage 操作是原 admission span 下的不同 span；Runtime 的实际
gRPC client/server span 位于对应 worker 操作下。原长连接的 transport span
继续表示连接本身。这里没有把 Control stream 每条服务端命令都重写成独立
span，也没有插桩模型驱动内部的 token/CUDA 执行；上述操作时长不是完整模型
推理时长或 GPU 利用率。

诊断属性只有 `vela.job.id`、`vela.stage_run.id`、`vela.stage_attempt.id`、
`vela.worker_instance.id` 和 Runtime decision；没有加入 Prometheus 标签。
trace 不包含 prompt、下载地址、凭据、lease token、execution nonce、原始
错误或 vendor tracestate。错误 span 使用固定描述。原取消/超时上下文保留；
watchdog 按原语义使用独立取消预算，不继承已结束调用的 cancellation。

## 验证

| 验证 | 结果 |
| --- | --- |
| 真实 PostgreSQL 17 admission、98 次迁移、98 Down/Up | 通过；原 HTTP 服务端 parent 进入 assignment |
| 新 backend/连接池回放 | protobuf 字节相同，只有首次生成 assignment 的 span；角色未扩权 |
| worker + 真实 gRPC + FakeRuntime | 开始、续租、重新打开日志后重连、停止均关联原任务；Prepare 只调用一次 |
| 实际文件输出日志恢复 | 首次 L2 注入故障，新任务已入场后重新打开日志，旧任务重试成功并清空输出日志 |
| 超时 watchdog | 手动时钟触发真实后台取消；原请求已取消，仍关联最初 Prepare，Runtime 进入 CANCELING |
| 错误与隐私 | 应用拒绝即使 gRPC error 为 nil 也记录错误状态；合成秘密不进入 span |
| 兼容性 | 缺失/非法追踪字段可入场、回放不覆盖、schema 5 历史可读取；trace 无执行授权效力 |
| 回归 | 7 项真实数据库/NATS 测试通过，包含 ordered authority、签名身份拒绝、scheduler 故障恢复和异步重投递 |
| 并发与入口 | 四个受影响包完整 race 测试、四个命令包和部署契约通过；定向 vet、protobuf lint/generate 通过 |

核心日志：

- [数据库](evidence/stage-tracing-database-2026-09-15.log)
- [最终定向 race](evidence/stage-tracing-focused-final-2026-09-15.log)
- [完整包 race](evidence/stage-tracing-race-2026-09-15.log)
- [7 项集成回归](evidence/stage-tracing-regression-2026-09-15.log)
- [入口及部署契约](evidence/stage-tracing-contracts-2026-09-15.log)
- [文件和候选二进制清单](evidence/stage-tracing-tests-2026-09-15.json)

首次测试发现两个夹具错误：输入回放前未记录既有 drain checkpoint；迁移
Down/Up 后重复调用同时创建登录角色的初始化 helper。已修正测试使用方法，
保留 [worker 初始失败](evidence/stage-tracing-worker-initial-2026-09-15.log) 和
[数据库初始失败](evidence/stage-tracing-database-initial-2026-09-15.log)。没有
放宽 drain 规则或数据库角色限制。

这些是独立数据库、文件日志重新打开、真实 gRPC 和 FakeRuntime 验证。没有
执行 OS 进程崩溃或主机重启，没有真实模型负载、生产 Collector/Tempo 接收
本次逐任务 trace 或长期留存证据。

## 候选采用步骤

1. 先恢复 `.66` 磁盘余量、三副本数据库/NATS/APISIX etcd 和三主机 MinIO
   布局，按既有 R1–R6 预检通过后再采用。不能以降低驱逐阈值绕过容量问题。
2. 在正式 release 中同时固定 Control、Node Agent、Stage Worker、ModelRuntime
   镜像/host package 和配置引用；应用 97、98 两个迁移，再采用新 Control。
3. **先升级所有 worker journal 读写者，再交付带 trace 的 assignment。** 新
   reader 同时读取 schema 5/6，首次持久化有效 trace 的入场原子写成 schema 6。
   schema 6 仅增加诊断字段，原签名、floor、退役证明和目录绑定继续保留。
   旧 reader 会拒绝 schema 6，因此已有此类日志时不能直接回滚旧 host binary，
   不能通过清空日志完成降级。停止 tracing 不会把日志自动降回 schema 5。
4. 用实际认证业务请求验证从 admission 到模型产物、当前 Collector/Tempo
   的关联，以及 SLO、输入容量、模型驻留和 Launch Receipt。采样和 48 小时
   留存仍按现有配置；过长任务可能晚于父 span 的留存期限。

四个 Linux amd64 二进制和本次增量源码/证据单独归档在 `.70`，归档回执见
[archive](evidence/stage-tracing-archive-2026-09-15.json)。这是脏工作树生成的
候选包，不能代替覆盖全组件、可独立重建的 canonical release。既有 schema-97
归档保留，当前源文件的最新 hash 以本次清单为准。

## 同期现场状态

[07:15 CST 只读快照](evidence/cluster-readiness-before-stage-tracing-2026-09-15.json)
为 54 注册、53 Ready、407 GPU；`.19` 离线/cordon，`.66` DiskPressure。
PostgreSQL/NATS/APISIX etcd 各 2/3，MinIO 5/6，健康成员 `.70:2/.71:3`；
双管理节点 HTTPS 入口可用。未部署本次代码、未删除待授权目录、未重启主机。

[07:28 CST 发布权限复核](evidence/platform-publishing-readonly-followup-2026-09-15.json)
确认 Argo 没有 ClusterRoleBinding，旧 CI 不能 patch Deployment，controller
能写本项目 Deployment、不能读取 Secret 或创建 Role/NetworkPolicy。项目
仓库白名单仍为空，GitLab 对接仍按用户要求暂缓；原 SSO/MFA 与真实发布/回滚
验收见 [发布平台记录](platform-publishing-validation-2026-09-15.md)。
