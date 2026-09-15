# 异步消息追踪验证 — 2026-09-15

本次完成 Job admission → PostgreSQL → Outbox → NATS → Inbox 的持久化
追踪实现及独立环境验证，归入既有 R6。生产 Control 尚未采用这些代码；
07:00 CST 现场数据库仍为 schema 96，新的可空字段需要迁移 97。

后续 [Stage/Runtime 增量](stage-runtime-tracing-validation-2026-09-15.md) 已完成
逐任务关联、日志恢复和真实 gRPC 测试，增加迁移 98 及 worker journal schema 6。
下文是本次 schema-97 验证的历史边界；新的候选及源文件 hash 见后续记录。

## 实现

- `jobs.origin_trace_parent` 与 Job 一起提交，只接受规范化的 55 字节 W3C v00
  traceparent，可空且不允许随后替换。幂等提交保留首次请求的 trace；此前的
  Job 和没有追踪的调用可保持 NULL。它是诊断元数据，不参与权限、路由、
  计费或业务幂等判断。
- Outbox claim 按 Job ID、Organization、Project 关联持久化 trace。每次
  发布尝试生成新的 producer span，成功 PubAck 及数据库记账仍使用原规则。
  消息 ID 和 protobuf 载荷不变，`traceparent` 放入 NATS header。
- Inbox 在校验事件身份后建立 consumer span，覆盖数据库事务、post-commit
  hook 和 DoubleAck。真实重投递产生独立 span，记录 delivery count 和是否
  实际处理；重复消息继续由 PostgreSQL receipt 去重。
- 只记录 trace/Job/event 标识与有限的操作状态。请求体、凭据、原始错误、
  baggage 和 vendor tracestate 不持久化到 trace 或消息 header。无效/缺失
  parent 不继承轮询循环的无关 span；取消信号保留。

所有该 Job 的 Outbox 事件关联首次 admission 的起始 span。它没有把不同阶段
伪装成直接同步调用：后续每个 StageRun、runtime 执行和异步调度的独立 span
仍需接入，长期队列延迟也可能使父 span 超出 Tempo 留存期。

## 实测

开发机 Docker 上启动独立 PostgreSQL 17 与三个 NATS 2.12 实例。NATS 使用
实际 release stream/consumer 配置，包括三副本与 64GiB 声明额度；实际仅写入
极少测试消息，不是生产磁盘容量或七天留存的验收。使用实际 Admission、
Outbox Publisher、JetStreamBroker 和 Inbox 实现，不替换生产 stream 校验器。

| 检查 | 结果 |
| --- | --- |
| 97 次真实迁移、合法/非法 trace 的 PostgreSQL CHECK | 通过；覆盖换行、大小写、零 ID、非法 flags；有效的 sampled/unsampled 通过 |
| 真实认证 HTTP admission 与不同 trace 的幂等重放 | Job 保存服务端 span，重放不更换原 trace；UPDATE 受到不可变约束拒绝 |
| PubAck 后、数据库 marker 前取消 | 首次报错且保持待重试；新 Publisher/连接池从持久化 Job 恢复原 trace |
| Outbox 再发布 | 两次 producer span 不同；事件 ID/载荷不变；三个 NATS 副本只存一条消息，保留首次 producer header |
| Inbox 提交后、ACK 前故障 | 数据库事务成功，hook 失败；NATS 实际重投递后只确认消息，不重复处理 |
| 最终状态 | 两次发布尝试、一条消息、一条 Inbox receipt、一次业务 handler 调用、零未确认消息 |
| trace 关联/隐私 | 两个 producer 与两个 consumer span 的 parent/trace ID、成功/失败状态正确；无合成秘密或原始错误 |
| 未启用追踪的请求 | 仍可 admission，起始 trace 为 NULL |
| 既有回归 | 数据库角色隔离、Outbox 重试/崩溃恢复、拒绝单副本和错误 stream 均通过 |
| race 与部署契约 | tracing/inbox race 检查、Control 和 deploymentcontract 测试通过 |

这里重新创建了 Publisher、数据库连接池和 consumer 对象来排除进程内缓存依赖，
故障使用取消与 post-commit hook 注入；没有把它表述成真实 OS 进程崩溃或生产
Pod 重启演练。所有测试容器和网络已清理。

首次集群测试把整个 90 秒窗口交给选举前的一次 stream 创建请求，超时失败。
修复为每次请求最多三秒、总窗口仍受限；随后两次成功，最终一次包含完整的
数据库约束检查。保留首次失败和全部后续日志，未覆盖为绿色结果。

测试命令、日志 hash、13 个源文件 hash 与二进制信息见
[测试清单](evidence/async-message-tracing-tests-2026-09-15.json)。核心记录：

- [最终集成测试](evidence/async-message-tracing-integration-2026-09-15.log)
- [角色与 Outbox 回归](evidence/async-message-tracing-regression-2026-09-15.log)
- [race](evidence/async-message-tracing-race-2026-09-15.log)
- [Control/部署契约](evidence/async-message-tracing-contracts-2026-09-15.log)
- [首次失败](evidence/async-message-tracing-initial-2026-09-15.log)

Linux amd64 Control 二进制已构建，SHA256：
`aa4485c2ca55f155fbd0f6eeea275812afa7ea904c0a36d9284b646e3f06afcf`。
这是当前工作树构建，尚未制成推广到三仓库的正式镜像或 canonical release。

## 采用条件和剩余范围

[07:00 生产预检](evidence/async-tracing-production-preflight-2026-09-15.json) 确认
`.66` 仍有 DiskPressure，PostgreSQL/NATS/APISIX etcd 各 2/3、MinIO 5/6。
Control 仍运行先前 schema-96 镜像，两副本 Ready；本次未更改它或数据库。
新二进制不能直接用于 schema 96，必须先完成迁移 97 的正式采用，再滚动发布。

现场数据库名来自 CNPG 的 `bootstrap.initdb.database`，实际为 `app`。首次
只读核查误用了 `vela`，失败于连接阶段，已保留
[初始记录](evidence/async-tracing-preflight-initial-2026-09-15.log)。也修正了
Fleet foundation 安装器的同一假设；新的版本默认只预检，并增加管理节点压力、
APISIX etcd、MinIO 三主机各两成员和 Longhorn 状态检查。
07:03 的[现场预检](evidence/fleet-foundation-preflight-v2-2026-09-15.json) 确认
节点压力会阻止发布，记录为预期拒绝，没有把它计为 Fleet 已部署。

后续仍须完成：健康集群上的 97 迁移/镜像发布；实际 Control → NATS → Inbox
到现有 Collector/Tempo 的验证；StageRun 与 runtime 的逐任务追踪；真实模型
任务的业务 SLO/产物/Launch Receipt。不能用本次独立数据库与 NATS 验证替代
这些现场工作，也不因此关闭 R3、R4、R5 或整个 R6。
