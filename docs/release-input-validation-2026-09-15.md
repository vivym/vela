# 发布输入与 Node Agent 入口验收 — 2026-09-15

本轮推进固定清单 R4，完成发布资源契约修复、Node Agent 网络入口切换，
以及 Control/NATS 的不可变材料切换。04:12 CST 新 Control 镜像 2/2 Ready、
NATS 3/3 Ready，25 项切换检查通过；数据库 schema-96 基线和精确权限校验
已完成。未产出完整 canonical release bundle。详见
[材料与数据库基线采用记录](release-material-adoption-2026-09-15.md)。
此前 GitLab 之外的发布与管理权限隔离已完成，见
[发布平台验收](platform-publishing-validation-2026-09-15.md)。

07:55–07:58 CST 后续增量已完成全部 51 个在线 worker 的来源测量和单策略
切换；只允许各实测 `/32` 的 TCP8444，667 项连通/拒绝检查通过。`.19` 离线，
不在放行范围。详见 [worker 网络验收](worker-control-network-validation-2026-09-15.md)。
下文 `.66` 的专项结果保留为首次切换历史，不再代表当前完整允许来源。

## 已完成

1. schema-3 增加显式 `kubernetes-v2` 资源契约，并纳入 configuration digest。
   旧 bundle 保持原编码/资源集合；未知版本、降级及资源增删均拒绝。
2. NATS 配置绑定外部 Secret 的 `nats.conf`；control-storage 要求 11 个资源、
   9 个固定 digest 镜像。Barman 依赖镜像去掉 tag，digest 未改变。
3. 应用 observability 渲染复用既有规则与看板，精确包含 `monitoring` 中的
   3 个 ConfigMap、1 个 PodMonitor、1 个 PrometheusRule；不用重复安装平台监控。
4. Control 清单显式列出 33 个 Secret 环境变量、9 组文件 Secret 的键。
   现场逐项确认键存在，并在内存中核验新旧环境值一致；现已随新镜像切换，
   未导出 Secret 值。
5. `.66` → 两个 CPU 节点的 Service 探针测得 CNI 来源 `10.42.2.0`。
   已单独部署精确 `/32 TCP8444` 策略并删除文档地址 placeholder。

## 现场结果

| 检查 | 结果 | 原始证据 |
| --- | --- | --- |
| CNI 来源与限制来源后的连通性 | 9 项通过，临时 Pod/Service/策略/SSH 进程清理通过 | [来源](evidence/node-agent-source-2026-09-15.json) |
| Control 切换前 | `.66` 到两 Pod 和 Service 的 8444 均失败；此前各轮探针无残留 | [预检](evidence/node-agent-ingress-preflight-2026-09-15.json) |
| Control 切换后 | 11 项通过：3 条路径可连，其他 CPU 来源和 8 条其他端口路径拒绝 | [入口切换](evidence/node-agent-ingress-2026-09-15.json) |
| 候选清单 | 51 项通过，应用监控、Control/NATS 键引用 server dry-run 成功 | [dry-run](evidence/release-candidate-dryrun-2026-09-15.json) |
| 发布引用盘点 | Control 2/2、NATS 3/3、PostgreSQL 3/3；16 个当前引用 | [引用盘点](evidence/release-live-inputs-2026-09-15.json) |
| 数据库来源 | 从实际 URL Secret 在内存解析，32 个数据库环境变量都指向 `app` | [数据库](evidence/release-database-inputs-2026-09-15.json) |

入口切换保留两个 Control Pod 的 UID/Ready 状态，`.66` boot ID 及所检查服务的
InvocationID 不变。首次切换仅允许当时验证的 `.66`，后续扩展见本文顶部。这不是节点进程身份证明；
相同 CNI 来源仍须受 mTLS 和应用注册/授权约束，尚未验证真实 Node Agent RPC。
其他 worker 加入应用层前要分别确定 CNI 来源和身份，不能把整个 Pod 网段放开。

上表的早期检查没有改变 workload；随后已使用独立切换脚本，部署新 Control
镜像和 16 个不可变材料引用，保留两个失败尝试及回退记录。完整
site/control-storage 渲染仍含历史验证配置；实际切换只修改已审计的 Pod
模板和 ServiceAccount pull Secret，保留 NATS 镜像、PVC 和副本数。

## 发布输入缺口

- 当前 Control/NATS 的 2 个 ConfigMap、14 个 Secret 均已采用版本化不可变副本，
  绑定 `vela.ai/release-revision` 与 `ExternalResourceContentV1` 摘要；原材料保留。
  NATS server auth 副本只含 `nats.conf`，bootstrap/outbox/scheduler 凭据保留
  在原 Secret。轮换证书或凭据时需要生成新副本并切换消费者。
- 这份盘点只覆盖运行中的 Control/NATS 引用，不是未来完整 release 或全部
  CNPG/应用组件依赖集合。Fleet Controller、Stage Worker 尚未部署，仍缺
  全组件 OCI descriptor/config、Host package、已批准模型/ResidencyPlan 及完整 PKI。
- 实际业务库 `app` 的 214 张 public 基表已与真实 96 次迁移生成的独立
  PostgreSQL 参考库比对。完成 Goose 标准 ledger 结构和 0–96 版本基线登记，
  补齐两项 Fleet 函数权限，同时修复旧应用精确 allowlist 漏项。最终 3,004 个
  schema 对象定义匹配，213 个业务表行数和九个迁移种子表 hash 不变。
  基线登记时间不代表历史迁移执行时间；没有向现有业务库重放迁移。


这些事项归入原 R4。NATS stream/磁盘容量属 R3/R5；真实模型 API、应用追踪和
Launch Receipt 属 R6。`.19` 本轮 SSH 仍返回 113，继续归入 R1。

## 本地验证与归档

通过的定向验证：

```text
go test ./internal/releasebundle ./internal/deploymentcontract -count=1
go test -race ./internal/releasebundle ./internal/h3launchevidence ./internal/catalogpromotion ./cmd/vela-release-bundle -count=1
go vet ./internal/releasebundle ./internal/deploymentcontract ./internal/h3launchevidence ./internal/catalogpromotion ./cmd/vela-release-bundle
go test ./internal/releasebundle -run 'TestRenderContract|TestCurrentRepositoryRenders' -count=1
```

最后一条覆盖增加 IPv4-mapped 文档地址拒绝后的定向回归。真实渲染测试同时核验
应用的 5 个 observability 对象与全平台渲染内容一致、Control 有 12 个具名键集的
Secret 引用、存储依赖的 9 个镜像均进入 release inventory。

现场输入、探针源码和候选清单位于 `.70` root 私有目录
`/opt/vela-cluster/release-inputs-20260915/`。源码快照见
[source manifest](evidence/release-input-source-2026-09-15.json)。
未提交或推送；未重启受保护主机，未操作暂缓的数据盘或 GPU 驱动。
