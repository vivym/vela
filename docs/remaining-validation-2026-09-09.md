# Vela 剩余验证与实施顺序

更新：2026-09-09。范围：`feature/vela-mock-hardening` 的本地 CPU/mock
正确性闭环，不包含部署、GPU 或 Production Gate 放行。

## 当前结论

控制平面持久 authority、Stage 分解、常驻 Runtime、Node 保管 journal 的职责
划分可以继续推进，目前没有证据要求推倒整套架构。主要缺口是这些边界尚未在
同一条完整执行与恢复路径中全部成立，不能仅凭组件测试宣布架构正确。

已确认的增量包括独立 Linux Worker/Runtime 的实际 RPC 屏障、Node 过载后
恢复、取消与持久 drain 分离，以及 journal 无响应和 admission 排队之后
watchdog 仍发出停止。证据分别见
[Worker 屏障](journal-worker-barrier-evidence-2026-09-08.md)、
[停止路径](journal-stop-outage-evidence-2026-09-08.md)及
[watchdog 排队](watchdog-lock-wait-evidence-2026-09-09.md)。

前一轮 [Runtime server 增量](remote-runtime-server-evidence-2026-09-09.md)
已使 `StartRuntimeServer` 支持远程 journal 启动，并要求 factory 前独立授权。
其授权器目前仍为测试装配。新增的
[Fleet 首次启动预留](runtime-startup-reservation-evidence-2026-09-09.md)
已在 PostgreSQL schema 95 原子保留完整 epoch 向量与 journal/incarnation/
owner 摘要，重放和回包丢失不产生新预留；未解决预留会阻止数据库静默证明。
新增的 [认证预留通道](runtime-startup-transport-evidence-2026-09-09.md) 已接到
Control 的 Fleet listener，Node/actor 由已注册的 mTLS principal 推导，真实
PostgreSQL/TLS 回包丢失不会重新发放 Fresh。新增的
[Node 持久关联](node-runtime-reservation-evidence-2026-09-09.md) 在 root journal
和原始 pidfd 检查后先 fsync intent，再单次预留并持久记录回执；重启只恢复
历史，不重建原 pidfd。新增 [Node/Fleet 同链验证](node-fleet-reservation-evidence-2026-09-09.md)
已将 root Node 与原始非 root Runtime 通过真实 TLS1.3/Fleet 连接 PostgreSQL；
正常和提交后丢回包均只预留一次。后续
[受保护 Runtime 与实际 journal pair](node-protected-pair-evidence-2026-09-09.md)
验证了不可 dump caller 的 Node 检查权限，并将 Worker journal 替换为实际
Node 创建且持锁的状态；Pod/CRI 仍是 fixture，Worker 输入与 materialization
业务尚未通过 Node 独立保管接口执行。
Node helper 已由外层 parent 在另一组 7 个精确
持久化/回包边界实际 SIGKILL 并重开检查；这不证明掉电恢复或生产后代隔离。
有效 OCI/executable/config 批准、一次性
Node grant、Node 端命令和 Fleet 挂载仍未完成；`NewJournalWorkerClient`、
`NewJournalServer` 也尚无生产入口装配。
新增 [Runtime 内存保护](runtime-process-protection-evidence-2026-09-09.md) 在实际
Linux 服务入口关闭 dumpability，防止无特权同 UID backend ptrace/mem 访问；
它不认证 executable/config，也不替代 backend UID/mount 与后代隔离。
CPU Job campaign 在
`internal/integration/cpu_mock_load_campaign_test.go` 仍通过
`NewSupervisorWithExecutionFloor` 使用本地 `runtime-admission` 目录。
因此，真实 RPC 组件证据与完整 Job 证据目前仍来自两种不同装配。

新增 [candidate 持久化期间的生命周期保护](journal-dispatch-lifetime-evidence-2026-09-09.md)
修复调用前与 backend 确认时的 Service 锁阻塞：watchdog/Shutdown 能在这两处
I/O 未返回时处理当前执行，调用前还会重新核对 generation 与生命周期。
共享 admission、其他持久化和不合作 backend 的停止时限仍需独立验证。

新增 [containerd task bundle 来源](node-task-launch-evidence-2026-09-09.md)
已从 root 私有的实际 task bundle 读取配置，匹配原始 caller/CRI/native task，
真实 CRI 反例证明它不随 `Containers.Get.spec` 改写而变化。它要求可信 Node
配置给出同 daemon 的 state root；配置批准、runtime options、有效挂载、
CLI 装配及一次性 grant 仍需完成。

## 依赖顺序与通过标准

| 顺序 | 尚需实施或验证 | 最低通过标准 |
| --- | --- | --- |
| 1 | 受保护的生产启动装配 | 将 Registry/Fleet 批准、原始进程身份、epoch、挂载与 Node owner 配置接到真实入口；Worker/Runtime 无法直接改写 journal；缺失、替换、过期身份一律拒绝，不能靠测试注入绕过 |
| 2 | Worker 输入与 materialization journal 的独立保管 | 为 Worker input/transfer/materialization 状态明确写入角色与持久边界；验证 resolver/子进程不能篡改，Stop 后不能重新引入内容或绕过清理屏障 |
| 3 | 同一装配下完整 remote-owner CPU Job | 真实 PostgreSQL、Control、ProductionAgent、Node、Runtime 完成四 Stage Job；同一路径覆盖 cache miss/admission/hit/reuse、transfer、终态清理与每 Job 一次 Charge；故障恢复由实际执行循环驱动 |
| 4 | Node/Runtime/Worker 故障与替换 | 对各持久写入、回包、读回边界注入退出/丢包；验证不确定写不能重执行，同 owner 的对账规则明确；替换进程不能借用旧 pidfd/epoch，旧进程及后代停止证据完整 |
| 5 | 多成员部分失败与停止时限 | 当前 native Worker barrier 仅单成员；补部分成员已 Start、另一成员失败、取消失败、并发锁等待和不可配合后台；证明不虚报 all-stopped、不恢复共享容量，并测出可支持的停止时限 |
| 6 | 历史记录安全回收 | 当前 `maxRetainedExecutions=32`，满后拒绝准入；设计并验证持久 cutoff、历史精确证明和重启语义，再测试超过上限后的长期合法执行；不能用简单删除历史消除安全边界 |
| 7 | 持续到达与资源上界 | 在第 3、4、6 项成立后持续施加 offered arrivals；记录队列、处理/拒绝速率、journal 大小、scratch、RSS、FD、goroutine 的趋势及故障恢复；有限批次成功和波次末 scratch=0 不能代替持续运行证明 |
| 8 | 整体验收矩阵和遗留失败 | 将每条架构断言对应到固定 source/config、明确输入和原始证据；单独定位 `11ce026` STALE 历史失败，并处理 integration-tag lint 遗留项；禁止把后续相似测试通过写成旧失败已解释 |

第 1、2 项涉及尚未完成的实现，不能靠重复现有测试补齐。第 3 项应作为下一条
可验收的端到端主线，实施时保留第 1、2 项的真实隔离边界。第 4、5 项在该主线
上注入故障；第 6 项是第 7 项持续运行的前置条件。

## 证据边界

现有回归可以证明被覆盖的签名、状态机、RPC、文件和 Linux 进程身份行为。
native backend 的 `FinishStop` 是测试控制的观察，不是真实后台与全部后代
已终止的证据。生产启动许可、真实硬件/驱动、签名发布、恢复演练与九项
Launch Receipt 仍未闭环，Production Gates 保持 **0/9**。

继续采用“明确不变量 → 最小可复现故障 → 修复 → 同源验证与留证 → 本地提交”
的顺序。避免把局部修复标成整体验收完成，也不重新运行没有新风险依据的历史
campaign 来替代尚缺的装配和故障路径。
