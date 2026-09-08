# Runtime 首次启动的 Fleet 持久预留

日期：2026-09-09。基线：`e460219`。范围：本地 CPU/PostgreSQL 验证，
不包含 GPU、部署或 Production Gate 放行。

## 本轮补齐的边界

此前 `StartRuntimeServer.RemoteStartup` 已在创建 backend 前调用独立授权器，
但其 native fixture 通过测试控制管道允许启动。Fleet 的 bootstrap claim
只消耗 journal 初始化权，Node 的 startup ledger 只保存进程观察；二者均
不能代替一次性的 Runtime 启动预留。

迁移 `00095` 新增不可修改的 `runtime_startup_reservations` 和窄接口
`vela_reserve_runtime_startup` / `vela_lookup_runtime_startup`，Go 入口位于
[`internal/fleet/runtime_startup.go`](../internal/fleet/runtime_startup.go)。

- 只允许已有完整 bootstrap journal receipt 的首次、未观察 Worker。
  在 Worker 行锁内核对 instance epoch、控制 session、观察状态、已有
  members/residencies/device bindings；随后核对当前 bundle 与留存批准
  manifest 的 digest、plan 归属及 bundle 状态。已 fencing、已观察、
  已放弃或配置变化的目标不能获得新预留。
- 从 Registry 留存的批准 bundle 推导该成员的全部 Runtime epoch：
  每个 Runtime 单独使用 `floor + 1`，完整向量必须与请求一致。
  AUX 不允许只预留其中一个 Runtime，也不允许把不同 floor 合成一个 epoch。
  不修改 `model_residencies`，不把预留写成 readiness 或可调度容量。
- 一个 bootstrap member 只能有一条预留；绑定 reservation request、
  journal ID/scope、incarnation、Node/actor、launch digest 和原始 owner
  observation digest。所有字段的变更均不能复用同一个 request ID。
- 仅本次成功提交的 INSERT 返回 `Fresh=true`。精确重放只返回最初时间和
  `Fresh=false`；独立 history 查询也不返回许可。Go 在提交／quorum 错误时
  丢弃可能已读到的结果，返回空 reservation。
- 预留必须通过恢复 admission gate 和延迟至提交的 synchronous quorum
  guard。不可 UPDATE、DELETE、TRUNCATE；留存预留后不能 Down 到 schema 94。
  Fleet role 只能执行窄接口，不能直接访问表或调用旧 recovery inventory。

## 恢复语义

schema 95 的数据库静默证明必须包含 `runtime_startup_reservations=0`；Go
receipt 验证器同步拒绝缺项或非零值。关闭 admission 后禁止创建预留，历史
查询／精确重放仍不产生新权限。

本轮没有释放、过期后重发或替换进程的接口。已提交预留即使回包丢失，也一直
阻止静默证明。只有后续具备独立的原始进程退出、后代 containment 和 journal
对账协议后，才能设计终态。这是明确的可用性限制，不能靠删记录或 bump epoch
绕开。尚无预留的历史 bootstrap 路径与旧 schema 的 receipt 仍可验证。

## 验证

[`runtime_startup_reservation_test.go`](../internal/integration/runtime_startup_reservation_test.go)
使用真实 PostgreSQL 17、完整迁移及受限 Fleet role。覆盖 16 路并发同 request
仅一个 Fresh；更换 request／incarnation／owner／launch／journal／scope／epoch
拒绝；另一个 Node/actor 无法读历史；完整 AUX 向量原子提交；不完整 journal
receipt 和 abandonment 拒绝；空库 Down/Up；非空历史禁止回退。

并发 fencing 测试先在独立事务内 fence Worker，再由 `pg_stat_activity`
确认预留调用确实等待数据库锁；提交 fence 后预留拒绝且表仍为空。
quorum 测试在 INSERT 已产生结果但提交约束失败的情况下确认 Go 返回空对象，
表没有残留记录；恢复独立测试连接后仍可首次预留。

回包丢失场景在应用层丢弃已提交结果，再新建连接池读取与重放，证明数据库
不能重新产生 Fresh。这里没有真实网络丢包注入，也没有证明 Node 可以安全
恢复同一启动调用。owner digest 是明确标注的测试输入，不是 Linux 进程证明。

最终结果：30 个 PostgreSQL integration 主测试在 race 下通过，其中 8 个为
本轮 startup reservation 主测试；用时 **191.077 秒**，无 skip、无 race。
覆盖原有 bootstrap TLS/命令进程与独立数据库恢复。Fleet/recovery 的 focused
race、仓库常规测试、`go vet`、普通 lint 和 Linux/amd64 编译检查也通过。
编译检查使用 `-exec=true`，不能当成 Linux 运行证据。

integration-tag lint 报告 **82 项**（50 errcheck、4 staticcheck、28 unused），
位于 23 个文件；逐字节核对这些文件与基线 `e460219` 完全相同，本轮修改文件
没有诊断。这里未将遗留项修复，也不把此检查标为通过。

最终验证命令、退出码、源码 digest 和原始日志见
[`runtime-startup-reservation-evidence-2026-09-09.json`](runtime-startup-reservation-evidence-2026-09-09.json)。
普通 lint 与 integration-tag lint 分开记录；后者存在不属于本轮改动的遗留
诊断，不能写成全量 lint 通过。

## 下一段必须完成的装配

这仍是 **Fleet 预留基础，不是生产 Node issuer**。Node/actor 由可信调用方
提供；PostgreSQL 不验证 launch/owner digest 的原像、不持有 pidfd，也不证明
签名 Registry binding、有效 OCI 配置或 root journal custody。现有 mTLS
bootstrap transport、CLI、Node grant ledger、Fleet Pod 挂载及 readiness
registration 尚未消费此预留。

下一段需要 Node 从自己的已验证 launch plan、root journal identity 和仍然
存活的原始 pidfd 构造请求，将完整向量与 Runtime 提案匹配；只有收到新预留且
完成本地持久 grant 事务后，才可在原始调用上回许可。不能把 lookup 的历史
记录改造成可重复使用的启动 token，也不能把 Node 重启后反序列化的 PID 当作
原始 pidfd。必须同时明确中途崩溃、失联、撤销和 Node 重启的拒绝／对账路径。

PostgreSQL schema **95**；Worker journal **5**、Runtime journal **8**、
Production Gates **0/9**。上一轮 native startup 证据仍属于 `e460219`；
完整 CPU Job campaign 仍属于 `637c80e` 的本地 journal 装配，本轮没有重新
测量或将其升级为 remote-owner Job 证据。整体目标与
[剩余清单](remaining-validation-2026-09-09.md)仍未完成。
