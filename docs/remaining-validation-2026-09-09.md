# Vela 剩余验证与实施顺序

## 2026-09-09 本轮复核

在当前提交点重新执行了 CPU/mock 关键 campaign：

```text
go test -tags=integration ./internal/integration -run '^TestCPUMockExactCacheSourceTargetCampaign$|^TestCPUMockExactCacheProductionLoopCampaign$|^TestCPUMockProductionLoopClockOffsetCampaign$|^TestCPUMockDurableStreamExactCacheCampaign$' -count=1
```

结果：通过，耗时约 0.862s。随后 `go test ./...` 与 `go vet ./...` 均通过，
`git diff --check` 通过。该复核确认当前 CPU/mock exact-cache、ProductionAgent
loop、clock-offset 与 durable-stream 路径没有回归；它仍不等于完整 integration
套件、真实 Node/CRI/Fleet 装配或 Production Gate 证明。

更新：2026-09-09。范围：`feature/vela-mock-hardening` 的本地 CPU/mock
正确性闭环，不包含部署、GPU 或 Production Gate 放行。

## 当前结论

截至本报告更新，当前分支已完成一次新的基础回归：`go test ./...`、`go vet ./...`
和 `git diff --check` 均通过；`go test -tags=integration ./internal/integration -run '^$'`
也通过，说明 integration build tag 下的测试代码可编译。默认回归没有启动
PostgreSQL、原生 mock 子进程或 Linux 特权环境，因此这些命令只证明源码/状态机
回归，没有提升下方端到端、故障恢复或生产启动的证据等级。

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
真实 CRI 反例证明它不随 `Containers.Get.spec` 改写而变化。后续
[daemon/state 关联](node-task-state-evidence-2026-09-09.md) 已用原始 socket peer
pidfd、CRI Status 和 daemon 的文件系统视图验证 state 目录来源；复制的私有
目录被拒绝，Node bind-mount 别名下的覆盖挂载不改变读到的真实 bundle。
新增 [task runtime options/hook 内容检查](node-task-mechanism-evidence-2026-09-09.md)
从实际 task/sandbox bundle 关联读取 runtime options 与 shim 路径。显式路径与
state root 可匹配，隐式 binary、未批准选项、hook 声明及矛盾关联被拒绝。
新增 [批准镜像入口与 caller 关联](node-planned-image-evidence-2026-09-09.md)
已从签名 fixture 绑定的镜像 manifest/config 推导并测量默认入口，与同 daemon
的实际 OCI argv、CRI image ID 和活进程 executable 对照；声明镜像不变但通过
bind mount 替换入口的反例被拒绝。这些仍是启动批准的前置条件：生产批准策略、
有效 env/config 文件/挂载和执行连续性、CLI 装配及一次性 grant 仍需完成。

新增 [同次启动请求的镜像检查与预留](node-startup-image-evidence-2026-09-09.md)
已将 canonical BackendStartupRequest、实际 root-held Runtime journal、镜像
默认入口、task mechanism 和单次 Fleet 预留串到 `ReserveImageRemote`。真实
CRI 的 17 个场景覆盖请求绑定错误、入口替换、预留期间 task 变化、丢回包及
镜像测量期间丢失 journal custody；持久 intent 不能重试，重开只能读取历史。
这里 Pod/Registry/Fleet 仍为 fixture，尚未与真实 PostgreSQL/TLS、生产
Node/Runtime CLI 或完整 Job 合并验证，返回回执没有启动授权含义。

新增 [实际 Runtime 远程 CLI](runtime-remote-cli-evidence-2026-09-09.md) 提供
`serve-remote --bootstrap-file`：非 root Runtime 从受保护的 root-owned 只读
配置快照装配真实 Node journal/startup 通道，不再在这个模式下创建本地 epoch
或 journal。18 个原生 CLI 场景覆盖实际 process backend 初始化/关闭、实际
Worker 发现 epoch=2、旧环境变量不能覆盖配置及文件/编码/绑定反例。
Node Permit、Registry/CRI 仍为 fixture；配置发布、生产 Node/Fleet 接线、
镜像关联与一次性 grant、同一装配下完整 Job 尚未完成。

后续 [Node 配置发布](node-bootstrap-publication-evidence-2026-09-09.md) 已从
verified plan、当前持锁 Runtime journal 及其实际公钥生成配置，在私有记录
持久化并核对文件身份后开放给计划中的 Runtime GID。真实 CLI 已使用这个
发布产物完成初始化、Worker 发现和关闭；发布异常、并发、内容/身份替换以及
六个 Node SIGKILL 边界有独立证据。此处的“创建一次”仅针对一个发布目录，
不是每个 incarnation 的唯一授权。生产挂载/发布者来源、镜像检查和 Fleet
预留仍未与这个 CLI 调用合并，一次性 grant 与完整 Job 仍未完成。

新增 [调用者视图中的发布文件关联](node-startup-publication-evidence-2026-09-09.md)
将原始 pidfd 的 procfs/root、实际只读挂载、发布文件 inode/摘要与同次镜像检查、
held journal 和单次 Fleet 预留连接起来。Node 持久 intent 保留 publication
记录及 mount ID，并纳入 Fleet owner observation 摘要；同内容复制文件和
同 inode 重新挂载均不能替代原观察。这里证明的是采样时调用者可以看到的文件，
不是它之前实际读取并使用的配置。真实 CRI 调用者仍是测试 probe，尚未将实际
CLI 的配置消费、有效 argv/env 和执行连续性关联到 grant。

新增 [实际 CLI 配置消费声明](node-bootstrap-consumption-evidence-2026-09-09.md)
使用 schema 2 将解析时冻结的 SHA256 和实际 bootstrap 路径带入启动请求，
Node 与独立 publication/plan/held journal 匹配。实际 CLI 在首次 journal
RPC 暂停后替换配置，仍发送原摘要而被拒绝；同内容不同路径也被拒绝。
发送前重读路径的 overlay 按预期失败。真实 CRI 的 17 个场景通过，旧 API
不能静默丢弃 schema 2 的关联，恢复也保留摘要/路径约束。CLI 和 CRI 仍是
两组装配；有效 argv/env、可执行代码连续性、真实 Fleet 与一次性 grant
必须接到同次启动，不能把这一消费声明当成任意 caller 的可信执行证明。
本轮首次全库测试的纯读超时重试失败已通过分离故意读取 deadline 与正常
fsync 预算修正并留证；它不解释下表的历史 `11ce026` STALE 失败。

后续 [实际 CLI/CRI 同次预留](node-remote-cli-reservation-evidence-2026-09-09.md)
用严格入口把实际 CLI、Node journal、image/task、发布只读 mount 和 Fleet
fixture 连接到同次调用。默认命令、固定 PATH/HOME、计划派生 HOSTNAME
与原始进程 procfs argv/env 精确匹配；10 个原生场景覆盖正常初始化/关闭、
task 隐藏真实参数/环境、预留前后修改及丢回包。关闭 procfs 比较的 overlay
在两个隐藏反例上错误地产生预留，正式源码均提前拒绝。真实 PostgreSQL/TLS、
生产 Pod→CRI 配置转换、Node 入口及一次性 grant 仍未合并。procfs 采样不是
配置历史或代码执行连续性的证明。生产 journal enrollment 还必须落实
grant 前只读、grant 后才能进行相应角色写入，避免先注册后无条件开放写权限。

新增 [启动 journal 只读通道](journal-readonly-startup-evidence-2026-09-09.md)
提供生命周期内不可升级的 `NewReadOnlyJournalEndpoint`，实际 CLI、publication
CLI 和 CLI/CRI 同次预留均改用该入口。两种原始角色可读但八类写操作均被
拦截；正确签名 admission/floor 的独立 owner 对照排除了无效命令假阳性。
实际 pregrant-floor 在 reservation 前和测试 Permit 后均不能写入；关闭
只读分支的 overlay 在三处按预期失败。该组件与测试装配已完成，生产 Node
接线、执行连续性、once-only grant 与 grant 后的写路由转换仍需完成。已有
可写构造器的独立授权前置条件不能靠手工选择该构造器来替代。

新增 [Node startup socket 协调器](runtime-journal-observation-evidence-2026-09-09.md)
已提供 `RuntimeStartupCoordinator`：exact canonical `BackendStartupRequest`、
预先独立签发的 grant 和 observer activation 成功后才返回 `Permit=true`；mismatch
不会消费 grant，handler 也不能重试。它目前是 Node-side adapter，实际 `serve-remote`
runner 仍使用 fixture listener，尚未成为生产 startup socket 的唯一装配路径。

新增 `RuntimeStartupServer` listener 适配器，负责 Unix peer credential、challenge、
request size/deadline 和 bounded shutdown，再将 canonical request 交给
`RuntimeStartupCoordinator`。它不创建或修改 socket 路径，避免把文件权限管理误当
作授权。原生 runner 已覆盖其无效配置拒绝与 coordinator/observer 组合；生产 Node
入口还需将真实 listener 创建、权限发布和该 server 设为唯一装配。

对 `cmd/vela-node-agent` 的当前入口做了架构核对：它只装配 WorkerInstance 证据、
remediation 和 gRPC 服务，没有 Runtime startup ledger、launch plan/Fleet reservation、
grant issuer 或 observer custody。因而不能直接把新 startup server 塞入该入口并
生成默认 Permit；下一项必须先定义真实 Node startup orchestration 的配置、来源
和 shutdown 所有权，再把 listener 设为唯一路径。当前 library/server 测试证据不提升
生产装配等级。

新增 `RuntimeStartupOrchestration` composition root，强制显式传入 ledger、plan、
expected request、grant、observer custody、caller credentials 和有界超时，并统一
Serve/Shutdown/Close。缺少任一 authority-bearing input 都拒绝；它不创建 socket 或
推导授权。下一步仍需从真实 Node/CRI/Fleet 生命周期生成这些对象并接入命令入口。

## 依赖顺序与通过标准

执行连续性的 [旧 mem 句柄候选实验](runtime-execution-continuity-evidence-2026-09-09.md)
已否证“保留 `/proc/PID/mem` 且仍可读即可证明未 exec”：`CLONE_VM` 独立进程
保留旧 mm 后，同文件重执行及 A→B→A 都不使旧句柄失效，原目标 `Threads=1`
也不能排除该反例。7 个 Linux/arm64 原生场景和缺少 ptrace 权限对照已留证，
该机制不接入授权。后续须验证全部线程的 exec 转换约束，或完整可信创建来源链；
当前仍未关闭第 1 项。具体反例与候选路线的验收条件见该报告。

后续 [创建时 syscall observer 与实际 CLI 实验](runtime-exec-observer-evidence-2026-09-09.md)
已验证从受控创建时覆盖原线程组 exec，并用 syscall 入口限制处理
`CLONE_UNTRACED` 逃逸与 clone3 参数采样竞态。独立 10 场景包含关闭 clone
约束和 EXITKILL 的反例；实际 CLI 四场景覆盖正常启动/关闭、拒绝和 observer
在 fixture Permit 前后退出，原始 Runtime 与真实 backend 的保留 pidfd
给出退出证据。CLI runner 共 12 个主测试通过，关闭 EXITKILL 的镜像使两个
故障场景失败且正常场景仍通过。它仍是创建关系/CRI/Permit fixture，尚缺
生产 Node 与 containerd 的可信创建交付、observer 挂起处理、同次真实 Fleet、
once-only grant 和写路由授权，不能将原型直接升级为生产入口。

新增 [observer 原始句柄交付与挂起保护](runtime-observer-custody-evidence-2026-09-09.md)：
Node 私有 socketpair 接收单一 `SCM_RIGHTS` target pidfd，在确认前保持目标
ptrace stop；`Start` 单次释放，`Check` challenge/response，SIGSTOP、通道丢失、
队列取消和撤销均有实际测试。实际 CLI runner 新增 6 个 custody 场景和 7 个
descriptor 场景，生产创建关系、持久恢复和 once-only grant 仍未闭合。

新增的 grant transition 已把这条缺口收窄：`JournalWriteGrant` 是绑定具体
endpoint 的内存单次 capability，激活前只读，激活后再次使用、错 endpoint、
过期或进程句柄失效均拒绝；它不接受序列化 receipt、历史记录或 caller reply
作为替代。当前只完成 Node API/CPU fixture 验证，grant 的真实 Fleet/TLS/
PostgreSQL 来源和 production route assembly 仍需接入。

后续 [统一启动协调设计与持久消费边界](runtime-startup-coordinator-design-2026-09-09.md)
增加了 schema 3 `grant_attempt`：完整 startup/reservation 摘要与独立证据摘要绑定
同一 operation/journal，消费前要求原始 Runtime owner 仍在；重复、重开及不确定
append 都不能重新消费。六组原生 race 测试、三处实际 Node SIGKILL 和真实
PostgreSQL/TLS 的正常/丢回包/缺权限场景通过。旧 schema 1/2 可读取但不能消费；
这只是持久限制记录，不验证授权摘要、不激活 endpoint、不发 Permit。统一入口
仍须把独立批准、执行连续性与消费后激活绑定，不能把本增量当作生产 grant 闭合。

后续 [持久消费与 journal 激活](runtime-startup-activation-evidence-2026-09-09.md)
已将独立签发的 operation-bound 内存 grant、原始 Runtime/具体 endpoint、持久
消费与最后复核后的角色写入连接到同次调用；ledger Close 撤销关联路由，且不会
等待另一启动的 ledger append/fsync 才撤销。普通 grant 无法绕过已 claim 的
endpoint，历史记录不能恢复激活。真实 PostgreSQL/TLS 正常分支完成 Worker
RPC 的 `Floor=1` 写入及关闭后拒绝；丢回包分支拒绝消费并实际观察 `Floor=0`。
这补齐了上述消费边界的正向转换；独立 issuer 仍为 fixture，生产批准、observer
连续监控、Permit 回包与真实 CLI/CRI 的统一装配仍未完成。

新增 [observer 连续检查与路由撤销](runtime-journal-observation-evidence-2026-09-09.md)
已将原始 target pidfd 匹配、消费前后 Check、运行期有界监控及不可逆过期封禁
接入 `ActivateObservedJournalWriteGrant`。实际 observer 挂起/退出/通道丢失和
服务取消会撤销路由并请求终止原始进程，ledger 持久化阻塞不阻止撤销；Close
并发附加和撤销失败也有回归。新增场景的 Fleet/CRI/issuer 仍为 fixture，实际
CLI、生产 observer 创建交付、批准策略及 Permit 尚未合并为统一入口。

当前代码的 [CPU exact-cache campaign](cpu-exact-cache-evidence-2026-09-09.md)
已重新执行：source miss/admit 后 target 两轮 hit/reuse，5 次 Transfer consume，
两 Job 各一次 Charge，总计 2500 minor units，终态 queue/allocation/lease/
scratch 清零。该证据仍是 local CPU/mock，不替代真实 Fleet/Node/Pod→CRI 或
持续运行验证。

新增 [CPU mock ProductionAgent loop campaign](cpu-production-loop-evidence-2026-09-09.md)
已在 schema 95 上用 4 个 persistent CPU mock workers 完成 2 波、8 个 Job 的真实
ProductionAgent loop：readiness、capacity、heartbeat、mTLS、durable stream、
materialization replay 和每 Job 一次 Charge 均通过，终态 queue/running/allocation/
lease/credit/scratch 清零。日志中 4 次 `AlreadyExists` 是回包丢失后的重复提交被
authority 拒绝并由 durable replay 收敛；这验证了幂等拒绝/收敛，不等于远端执行
绝不会发出重复请求。该 campaign 仍将 Node custody、protected mount、真实
ProductionAgent 进程隔离、remote storage 和 sustained arrivals 留作未完成项。

同一套当前源码的 [clock-offset campaign](cpu-production-loop-clock-offset-evidence-2026-09-09.md)
又以 verifier `-1s` 偏移运行 6 个 Job：所有 worker 都观察到 future-issued
assignment，仍在 `30s` authority skew 上限内，未产生错误接受或额外 Charge。这
只关闭允许偏移的 CPU/mock freshness 检查；超限、跨主机时钟同步和生产装配仍未验证。

[Multi-member barrier](multi-member-barrier-evidence-2026-09-09.md) 的当前 Agent/RPC
状态机也已通过定向 `-race` 验证：部分 Start 失败会取消全 allocation，取消 ACK
不会冒充 `AllStopped`，部分 drain 不会恢复共享容量。真实生产后代停止与隔离仍未
覆盖。

[Assignment history retention](assignment-history-retention-evidence-2026-09-09.md)
已验证达到 `MaxRecords` 后 fail closed，重启不丢 watermark/profile/closed history。
本轮新增 `AssignmentHistoryCutoff` proof contract（commit `d25429e`），并在
`582fdb4` 接入 admission journal 持久状态：固定
scope/Worker identity/epoch、连续 sequence range、累计 digest、terminal/input/
materialization proof 和 previous-cutoff digest 链，并以单元测试拒绝缺 proof、gap、
身份漂移和错误链。追加 cutoff 时还要求范围内历史连续保留、execution 已关闭且
具备 input-drain proof；恢复会重新验证整条链。`8da611a` 已实现 cutoff 驱动的原子
连续前缀删除，并保留 proof 链、推进 `HistoryBase`；当前仍需补齐删除后 digest
对账和完整 recovery 的更多故障注入验证；
`HistoryBase` 及按已持久 cutoff 删除连续前缀的实现，删除后仍保留 proof 链并允许
从新 base 继续校验；`7733c27` 已验证删除后关闭 journal、重开并继续接纳下一条
execution，`efd96fe` 又覆盖了 rename 后 directory sync 失败并重开恢复的边界。
`1e99ea5` 覆盖了调用者篡改 cutoff digest 时的拒绝路径，`13a0402` 又验证了
磁盘中持久 cutoff 被直接篡改后 recovery 拒绝；`0ca81e9` 又验证了两个连续
cutoff 的 digest 链、分段回收和继续准入。`bc4544f` 增加了 40 个连续 CPU/mock
arrival，逐条完成 input drain、关闭、记录 cutoff、回收并检查 journal 保持空保留
前缀。`0a7173a` 同时记录 GC 后 goroutine/heap 趋势并以宽松阈值拒绝明显泄漏；
该测试证明的是有限本地 campaign，不是开放环长期吞吐或资源上界；安全 reclamation
仍需补齐多 cutoff 连续中断和长期压力验证，
sustained arrivals 仍受该上限约束。

本轮还通过了回收相关测试的 `go test -race`、integration build-tag 编译检查、
全库 `go test -race ./...`、`go test ./...`、`go vet ./...` 和 `git diff --check`。
这些结果证明当前
实现没有观测到 Go 数据竞争或源码回归，但仍不替代真实多进程/远程故障验证。

新增 [assignment history race validation evidence](assignment-history-race-validation-evidence-2026-09-09.md)：
`go test -race ./internal/stageworkeragent -run 'AssignmentHistory|History' -count=1 -v`
通过，覆盖 cutoff 链、持久恢复、40 条 bounded arrival、完整 history preflight、
terminal drain/recovery、未知输入保留，以及两个连续 cutoff 的同步故障注入。故障后
重开会根据 rename 是否已落盘，安全地看到旧 `HistoryBase` 或新 `HistoryBase`，均不
接受损坏状态；最终持久文件还逐项对账了 `HistoryBase`、cutoff 记录和 digest chain。
同一 bounded campaign 还测得 state 文件从 `540` 增长到 `41,398` bytes；这确认
增长来自完整 cutoff proof chain 的保留。它不是 goroutine/heap 泄漏，但暴露了长期
空间上界缺口。需要设计并验证 checkpoint/压缩协议后，才能关闭长期 history 压力；
掉电 durability 和真实 Node/Fleet/CRI 装配仍未闭合。

已先加入 `AssignmentHistoryCheckpoint` 的 canonical/identity/range/proof 校验层，
并接入 recovery 对 `HistoryBase` 与保留 cutoff anchor 的边界校验；相关
`go test -race ./internal/stageworkeragent -run 'AssignmentHistory(Cutoff|Checkpoint|Reclaim)'`
通过。现在 bounded 40-arrival campaign 还实际执行了单 checkpoint compaction，
并在重开后确认 `HistoryBase=40`、`HistoryCutoffs=0`、无 retained execution。
本轮还注入了 compaction 后的 directory sync failure，重开后能安全区分旧链和
完整 checkpoint 两种结果，并验证压缩前缀后 retained suffix 的每一条
`PreviousCutoffDigest` 都重新链接到新的 checkpoint anchor；同一 campaign 随后又
完成第二次 checkpoint compaction，将 8 条 retained cutoff 再压缩为零。掉电级别故障、
外部签名和长期上界仍需继续验证；本轮还直接篡改最终 checkpoint 的
`cumulative_digest`，重开按预期拒绝，随后恢复原始文件。
同一 campaign 还覆盖了 state 截断、空文件和 trailing bytes，均按预期拒绝；这些
是本地损坏恢复证据，不等同于真实 power-loss durability。
此外，错误 `CompactedCutoffCount` 的 checkpoint 在提交前被拒绝，且 state 文件大小
保持不变，随后合法 compaction 才继续执行。
重复 compaction 还覆盖了旧 revision 的 successor checkpoint，确认非递增 revision
会被拒绝且 state 不变。

checkpoint/repeated-compaction 改动后的全库回归也已通过：`go test ./...`、
`go vet ./...`；其中 `internal/stageworkeragent` 包含完整 compaction/race campaign。
这只证明当前源码没有观测到回归，不替代 power-loss 或真实生产装配验证。
随后单独执行 `go test -tags=integration ./internal/integration -run '^$'`，integration
build-tag 编译通过，未运行测试用例。

本轮尝试完整 `go test -tags=integration ./internal/integration -count=1 -timeout=20m`
未在 20 分钟内完成，堆栈停在 materialization cancellation fixture 的数据库 seed；
同一测试隔离运行约 6.6 秒通过。详见 [integration full-suite timeout evidence](integration-full-suite-timeout-evidence-2026-09-09.md)。
随后 `535ea5d` 记录两 shard 进程均成功退出。分配覆盖 505 个顶层测试名称，
但需要 image 或原生环境的 opt-in 测试仍可能跳过，不能将进程通过换算成
505 条实际执行通过。本轮另以源码对应 image 显式执行 Node/Fleet/PostgreSQL/TLS
正常、提交后丢回包和缺少 ptrace 权限三种情景，均通过。

新增的 [grant 原生验证](journal-grant-transition-evidence-2026-09-09.md) 修正了
此前直接构造内部 grant 的测试缺口，改用公开签发 API、真实原始进程/pidfd 和
角色写入 RPC，并覆盖过期、取消、退出及并发激活。它与上述 Fleet 预留仍是
两套独立验证；不能把 reservation receipt 自动解释为 backend Permit，生产
同次启动授权、执行连续性与 grant 接线仍是第 1 项的未完成工作。

新增 [database role boundary evidence](database-role-boundary-evidence-2026-09-09.md)：
在完整 migration `00001` 至 `00095` 后重跑 `TestDatabasePoolsFailClosedOnRoleConfusion`，
确认 Fleet 及其他 service role 的精确 privilege boundary 当前仍通过。该结果不替代
真实部署的 secret、网络、TLS 或生产连接池装配验证。

`0973e59` 将 `HistoryBase` 和 cutoff 数量加入 `PrepareAssignmentJournal` 的只读
状态，恢复审计可以直接确认已回收前缀，而不必读取私有 JSON 文件；该状态仍只报告
经过校验的本地 journal，不代表生产启动授权或远程 owner 已成立。

`499fd9d` 收紧了恢复不变量：`HistoryBase` 必须为零或落在已持久 cutoff 的边界
上，且不能超过 cutoff 链的末端；新增的磁盘 base 篡改测试确认 recovery fail closed。
`204acb3` 又要求首个 cutoff 从 execution sequence `1` 开始，拒绝跳过未证明的
历史前缀。
`797facb` 将每个 cutoff 的 scope、Worker instance/member 和 epoch 绑定到当前
journal，并在追加、恢复、回收三条路径都 fail closed；跨 journal identity 的
cutoff 不能被接受。`e4f4ba1` 增加了直接篡改磁盘 `worker_member_id` 后 recovery
拒绝的测试。

随后 `go test -race ./...` 全库通过，未观测到 identity binding 变更引入的 Go
data race；native subprocess、Docker VM 和 PostgreSQL 内部仍不在该检测范围内。

`9045e8e` 的 16-job、两波并发 arrival campaign，以及 `13d7bbe` 的同装配 `-race`
重跑均通过：16 个 completion、16 次 Charge、无 acquire deadlock/serialization
retry，终态队列/运行/分配/租约清零。它们仍是 bounded CPU/mock PostgreSQL campaign，
不是 production soak 或开放环长期资源上界。

[Linux validation boundary](linux-validation-boundary-2026-09-09.md) 已确认当前
Darwin 宿主只执行非-Linux 测试；Linux 源码可在 `linux/arm64` 容器中编译，但当前
Docker 沙箱禁止测试 helper 的 fork/exec，故 pidfd/ptrace/namespace/Node startup
运行语义仍缺真实 Linux runner 证据。

本轮在当前工作树重新编译并运行了三个特权 `linux/arm64` test binary：
`internal/runtimechannel`、`internal/nodeagent` 和 `internal/modelruntime` 的
`-race` binary 均 `PASS`。首次复验因 `golang:1.26-bookworm` 的 `bash -lc` PATH
没有包含 `/usr/local/go/bin` 而得到 `go: command not found`；显式设置 PATH 后重跑
通过。该工具链入口问题已确认并清除，不能计为源码失败；显式跳过的 native exec
observer、actual CLI、CRI/Fleet 外部装配仍保持未验证。

随后将 test binary 输出到可执行 workspace 路径后，完整 Linux
[Node/Runtime test binary](linux-nodeagent-runtime-evidence-2026-09-09.md) 已在
特权 `linux/arm64` 容器中实际 `PASS`。这关闭了大部分 Linux journal/channel/
pid namespace/bootstrap/startup ledger 运行证据；仍被显式 `SKIP` 的 native exec
observer、actual CLI、CRI Node Agent 和 host Fleet orchestrator 仍不能视为已验证。

完整 Linux [ModelRuntime test binary](linux-modelruntime-evidence-2026-09-09.md) 也已
通过，覆盖 process backend child-writer 清理、watchdog/cancel、journal owner API、
durable execution state、SIGKILL recovery、allocation/epoch fencing 和 terminal
non-admission。外部 Fast H3 driver 与 non-root permission 单项仍显式跳过。

底层 [runtimechannel test binary](linux-runtimechannel-evidence-2026-09-09.md) 也已
通过，真实验证 pidfd interruption retry、目标进程退出和 descriptor error；它仍只
是底层包证据，不代表 containerd/observer 创建关系已经完成。

为避免科学性误报，CPU campaign 入口现在对 `exactCache && productionLoop` 明确
由同一 `ProductionAgent.Run` 执行循环驱动。新增组合 campaign 在真实
PostgreSQL 17、四个常驻 CPU mock Runtime、source miss/admit、target hit/reuse、
TransferTicket、materialization 和终态清理上通过；source/target 阶段成功数按
真实 `attempt_id` 等待，避免把 `job_id` 误当成 attempt。该组合为隔离调度路径而
注入了 committed-response loss；ProductionAgent 通过关闭旧 stream、按 stream
generation 丢弃旧 consumer 错误、重建 durable `StreamAgent` 并复用 journal，在新
control session 上 replay request identity 后收敛，scratch、journal 和 allocation
均清零。该证据仍属于 CPU/mock + loopback control，不等同于真实 Node/Fleet 网络
替换，但 ProductionAgent 的装配级 reattach 已有直接证据，详见
[ProductionAgent reattach evidence](production-reattach-evidence-2026-09-09.md)。

| 顺序 | 尚需实施或验证 | 最低通过标准 |
| --- | --- | --- |
| 1 | 受保护的生产启动装配 | 将 Registry/Fleet 批准、原始进程身份与执行连续性、epoch、实际 CLI argv/env、挂载与 Node owner 配置接到真实入口；明确 journal enrollment 授权前只读和 grant 后角色写入；Worker/Runtime 无法直接改写 journal；缺失、替换、过期身份一律拒绝，不能靠测试注入绕过 |
| 2 | Worker 输入与 materialization journal 的独立保管 | 为 Worker input/transfer/materialization 状态明确写入角色与持久边界；验证 resolver/子进程不能篡改，Stop 后不能重新引入内容或绕过清理屏障 |
| 3 | 同一装配下完整 remote-owner CPU Job | 真实 PostgreSQL、Control、ProductionAgent、Node、Runtime 完成四 Stage Job；同一路径覆盖 cache miss/admission/hit/reuse、transfer、终态清理与每 Job 一次 Charge；故障恢复由实际执行循环驱动 |
| 4 | Node/Runtime/Worker 故障与替换 | 对各持久写入、回包、读回边界注入退出/丢包；验证不确定写不能重执行，同 owner 的对账规则明确；替换进程不能借用旧 pidfd/epoch，旧进程及后代停止证据完整 |
| 5 | 多成员部分失败与停止时限 | 当前 native Worker barrier 仅单成员；补部分成员已 Start、另一成员失败、取消失败、并发锁等待和不可配合后台；证明不虚报 all-stopped、不恢复共享容量，并测出可支持的停止时限 |
| 6 | 历史记录安全回收 | 当前 `maxRetainedExecutions=32`，满后拒绝准入；持久 cutoff、连续前缀回收、`HistoryBase`、proof 链、重启/中断/篡改/连续 arrival 语义已有 CPU/mock 证据；仍需删除后 digest 对账、更多 power-loss 等价故障边界和长期合法执行压力，不能用简单删除历史消除安全边界 |
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

## 2026-09-09 验证收口记录

提交 `a2ac1a7` 后，`go test ./internal/stageworkeragent -run
'TestAssignmentHistoryCutoff|TestAssignmentHistoryReclaim' -count=1` 通过。
随后执行 `go test -tags=integration ./...`：除 `internal/integration` 外已运行到的
所有 command/internal 包均通过；integration 测试进程在超过五分钟无新输出后中止，
因此不能记为全仓库 integration PASS。此前的
`go test -tags=integration ./internal/integration -run '^$'` 编译检查仍有效；真实
campaign 需显式环境变量，不能由这次无界运行推断已完成。

之后将 integration 测试按列出的 `506` 个测试拆成批次运行。前 `80` 个测试通过；
第二批最初暴露 `TestDatabasePoolsFailClosedOnRoleConfusion/Fleet` 的权限契约漂移：
最终 schema 授予 `vela_fleet` 的 worker-bootstrap 和 runtime-startup 函数没有进入
`verifyFleetPrivileges` 的精确允许集合。提交 `d3d0128` 补齐这 `7` 个实际授权函数，
并以同一第二批（`80` 个测试，约 `189s`）复跑通过。该结果关闭了一个真实的
生产装配拒绝问题，但尚未覆盖剩余 integration 测试，也不改变 Production Gates
`0/9` 的状态。

随后完成全部 `505` 个 integration 测试的分批复跑：批次大小为
`80 + 80 + 80 + 80 + 80 + 105`，每批均使用独立 PostgreSQL 17 Testcontainer、
`-count=1` 和有界 `-timeout`，全部通过。该结果证明当前提交链下的 integration
契约集合可在本地 Docker 环境完整运行；它仍然是单机 Testcontainers 证据，不等价
于真实多节点 Fleet/CRI、进程替换、长期 soak 或 Production Gates。
当前提交又以 `VELA_RUN_CPU_MOCK_CAMPAIGN=1` 运行
`TestCPUMockExactCacheProductionLoopCampaign` 的 `-race` 版本。首次运行发现
campaign 在 race 调度下对 source drain 做即时采样，可能在后台清理完成前误报
`ScratchBytes`；提交 `43bf104` 将 source/target drain 改为 30 秒有界轮询，并保留
最终 allocation、lease、credit、watchdog 等断言。修复后 race campaign 通过。

随后以 `VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=2
VELA_CPU_MOCK_WIDTH=8` 运行 `TestCPUMockConcurrentAdmissionRuntimeCampaign` 的
`-race` 版本并通过：`16` 个 Job、`16` 次 Charge、两波并发 arrival，队列/运行中
分配/lease/credit/scratch 清零，acquire deadlock/serialization retry 均为 `0`。
该证据仍是 bounded CPU/mock campaign，不是长期 open-loop 资源上界。

新增 [CPU mock concurrent long campaign evidence](cpu-concurrent-long-campaign-evidence-2026-09-09.md)：
8 波 × 8 并发、共 64 个 Job 的当前提交运行通过，短期 goroutine/worker FD 和每波
scratch/watchdog 收敛稳定；同时记录了 `MaterializedBytes`/PostgreSQL size 随保留
历史增长的事实。因此它加强了有限长压证据，但没有关闭长期 journal/history 上界。

另外对 Worker bootstrap、Worker Registry、runtime startup、Node Agent reporter 和
Postgres reattachment 相关 fixture integration 做了组合 `-race` 重跑：
`go test -race -tags=integration ./internal/integration -run
'^(TestWorkerBootstrap|TestWorkerRegistry|TestRuntimeStartup|TestPostgresReattachmentBackend|TestNodeAgentWorkerInstanceReporter)'`
通过（约 `216s`）。这加强了 Node/Fleet authority、epoch、bootstrap、reservation
和 reattachment 状态机的并发证据；Linux native provisioning、真实 CRI/containerd
和 source-matched runtime image 仍由显式跳过项保持未验证。

### 本轮收口：Prepare composition boundary

`RuntimeStartupLedger.PrepareRemoteStartupOrchestration` 已作为真实装配前的
composition boundary 落地：它要求完整 reservation、live owner、observer custody、
非零独立 `AuthorizationDigest`、caller credentials 和有界 timeout；随后从 ledger
当前 owner 创建 read-only journal endpoint，签发 operation/digest-bound grant，并返回
`RuntimeStartupOrchestration`。它不会自行发 Permit、创建 socket，或把 digest 当作
授权器，因此仍保持独立批准和真实 Node 生命周期的证据边界。

最终 Linux/arm64 native runner 已实际执行并通过 31 个 top-level tests，其中包括
`TestPrepareRemoteStartupOrchestrationRequiresIndependentInputs`；默认 `go test ./...`、
Linux/arm64 `go vet ./internal/nodeagent`、`golangci-lint v2.13.1`（0 issues）、shell
syntax 和 `git diff --check` 均通过。证据摘要已刷新到
`runtime-journal-observation-evidence-2026-09-09.json`，对应 native log 为
`/tmp/vela-prepare-closure3-20260909/native.log`。

这仍未关闭生产接入：`cmd/vela-node-agent` 尚未提供真实 ledger/verified plan/Fleet
reservation/grant issuer/observer custody 来源，也没有把 startup socket 设为唯一生产
listener；Production Gates 继续为 `0/9`。下一步是定义并实现真实 Node startup
orchestration 的配置、来源及 shutdown 所有权，然后在同一次真实 PostgreSQL/TLS +
CLI/CRI/Fleet 路径验证 reservation、grant consumption、observed activation、Permit
和失败恢复。

### 本轮生命周期审查修复

复审发现并修复了三个会影响真实装配的生命周期问题：

- `PrepareRemoteStartupOrchestration` 在任何 reservation 结果（包括回包不确定的错误）
  后都清理 observer custody；credentials、exchange timeout 和 observer timeout/interval
  先于不可重试 reservation 做 preflight，避免无效配置消耗 Fleet authority；读取 ledger
  owner/startup 状态也在 ledger mutex 保护下完成。
- `RuntimeStartupServer.Shutdown` 保存并关闭活动 Unix listener，能唤醒空闲的
  `AcceptUnix`；listener 的 unlink 配置在发布给 Shutdown 前完成，避免初始化竞态。
- `RuntimeStartupCoordinator.Close` 在 observation 关闭后继续关闭 observer custody，
  不遗留 pidfd、target 或私有 socket。

新增了消耗性输入 preflight 和空闲 listener shutdown 回归；Linux/arm64 native runner
再次通过 31 个 top-level tests，且无 `DATA RACE`。本轮 runner 证据位于
`/tmp/vela-lifecycle-fixes2-20260909/native.log`。

### Integration 全量运行边界（2026-09-09）

对 `internal/integration` 做了 build-tag 编译、相关 runtime startup focused 测试和全量运行。
`TestRuntimeStartupMutualTLSReservationPreservesLostResponse`、
`TestJetStreamConsumerRedeliveryAfterCommitBeforeAckAppliesOnce` 以及
`TestStatisticalSLOMigrationEmptyDownUpAndDurableEvidenceRefusal` 均独立通过，说明相关
PostgreSQL/TLS、JetStream 和 migration 逻辑在隔离进程中可运行。

全量 `go test -tags=integration ./internal/integration -count=1` 仍未闭合：在长套件运行约
15 分钟后，Docker Desktop 中的 PostgreSQL 容器在 goose migration 39 写入
`goose_db_version` 时停止响应，最终由 package timeout 终止。失败栈显示是
PostgreSQL/Docker I/O 阻塞，不是 Go assertion 或稳定的单测试失败。为避免基础设施故障
无限占用套件，integration fixtures 已给 PostgreSQL 连接加入 `lock_timeout=30s`、
`statement_timeout=120s`，并给 PostgreSQL/MinIO/NATS Testcontainers cleanup 加入 30 秒
deadline；Docker daemon 整体无响应时仍需要宿主恢复后重跑全量套件。

因此 focused integration 证据不能升级为“全库 integration 通过”；完整套件仍是开放验证项，
Production Gates 继续为 `0/9`。

全量套件按领域分片后，`go test -tags=integration ./internal/integration -run '^TestRuntimeStartup'`
已在约 35 秒内通过；该分片覆盖 10 个 runtime startup 顶层测试及其 PostgreSQL/TLS、并发、
quorum、拒绝、journal receipt 和 recovery 子场景。分片结果可以作为 runtime startup 的
有效证据，但不能替代剩余领域和全量套件的闭合运行。

CPU/mock 相关分片
`go test -tags=integration ./internal/integration -run '^TestCPUMock|^TestRuntimeUsageRecordsAllocationAndCacheWithoutInventingGPUTime$|^TestWorkerBootstrap'`
也已通过，耗时约 104 秒；覆盖 durable stream campaign、exact-cache campaign、runtime
usage 的无 GPU 时间声明，以及 Worker bootstrap mTLS/lost-response 三个场景。该结果仍是
领域分片证据，不改变全量 integration 尚未闭合的结论。

Stage authority 分片
`go test -tags=integration ./internal/integration -run '^TestStageCutover|^TestStageWorkerControl|^TestStageCapacity'`
已通过，耗时约 99 秒；覆盖 Stage cutover 的零 backlog/evidence fencing、worker control
renewal/replay、session capacity 和 observation lifecycle。该分片补充了调度 authority 的
持久化、并发和 fail-closed 证据，仍不替代全量 integration。

权限与撤销安全分片
`go test -tags=integration ./internal/integration -run '^TestBreakGlass|^TestLegalHold|^TestCancellation'` 已通过，耗时约 62 秒；覆盖 break-glass 独立 principal/approval、legal-hold 的不可逆释放与并发序列化，以及 cancellation credential 变化后的 fail-closed/no-mutation 语义。这些分片补充了高风险 authority 和副作用边界证据，但全量 integration 仍未闭合。

资源与分配一致性分片
`go test -tags=integration ./internal/integration -run '^TestWorkerRegistry|^TestStageAssignment|^TestStageArtifact|^TestArtifact'` 已通过，耗时约 89 秒；覆盖 Worker registry authority/replay、Stage assignment connector/replay、artifact lifecycle 及不可变内容边界。该分片仍属于领域证据，不替代全量 integration。

业务一致性分片
`go test -tags=integration ./internal/integration -run '^TestAdmission|^TestWebhook|^TestOutbox|^TestInbox'` 已通过，耗时约 101 秒；覆盖 Admission 无副作用拒绝与幂等、Webhook 重放/权限、Outbox 提交后重投、Inbox exactly-once/版本边界。

Finance/catalog/H3 分片
`go test -tags=integration ./internal/integration -run '^TestFinance|^TestUsage|^TestCatalog|^TestH3|^TestRuntimeUsage'` 已通过，耗时约 109 秒；覆盖 finance reconciliation、usage cost ledger、catalog promotion production gates、H3 campaign evidence，以及 runtime usage 的无 GPU 时间边界。

身份与租户隔离分片
`go test -tags=integration ./internal/integration -run '^TestHuman|^TestOrganization|^TestServicePrincipal|^TestCredential|^TestRequestContext|^TestScope|^TestOpenAPI|^TestDatabasePools'` 已通过，耗时约 136 秒；覆盖 human/OIDC/RLS、service principal/credential revocation、request context scope revalidation、organization isolation 和 OpenAPI error contract。

恢复、保留与 remediation 分片
`go test -tags=integration ./internal/integration -run '^TestRetention|^TestNonContent|^TestRecovery|^TestRemediation'` 已通过，耗时约 88 秒；覆盖 retention/content expiry、non-content hold/expiry、recovery gate/snapshot/role isolation，以及 remediation claim、quarantine、approval、replay 和 recovery。

Stage execution 核心分片
`go test -tags=integration ./internal/integration -run '^TestStageScheduler|^TestStageGraph|^TestStageMaterialization|^TestStageLease|^TestStageJob'` 已通过，耗时约 304 秒；覆盖 scheduler lock order/claim/replay/capacity、graph cancellation/finalization/public-copy、materialization retry/expiry/replay、lease recovery 及 job expiry convergence。

并发与幂等分片
`go test -tags=integration ./internal/integration -run '^TestAttemptCoordinator|^TestIdempotency|^TestQueued|^TestConcurrent|^TestServiceClass'` 已通过，耗时约 47 秒；覆盖 attempt claim/replay、idempotency request distinction、queued cancellation、并发 admission/credit/webhook/invoice 以及 retry budget contract。

支持系统分片
`go test -tags=integration ./internal/integration -run '^TestBilling|^TestInvoice|^TestSettlement|^TestCloudNative|^TestNATS|^TestSchedulerConsumer|^TestS3|^TestProtected|^TestPublic|^TestLocalWorker|^TestNodeAgent'` 已通过，耗时约 65 秒；覆盖 billing/invoice/settlement、CloudNativePG failover、NATS workload identity、S3 artifact version/multipart、protected provisioning 和 Node Agent/worker bootstrap。

Schema/legacy/cache 分片
`go test -tags=integration ./internal/integration -run '^TestFoundation|^TestAtomic|^TestHierarchical|^TestModelRuntime|^TestResidency|^TestLegacy|^TestStageExecutionCatalog|^TestStageCache|^TestStageTelemetry|^TestStageTerminal|^TestStageTransfer|^TestStageSuccessful|^TestStageRun|^TestStageQuorum|^TestStageAttempt'` 已通过，耗时约 189 秒；覆盖 migration round-trip/rollback refusal、legacy H3 contraction、execution catalog authority、exact cache identity/quota、telemetry privilege、terminal history、transfer clock、storage reservation 和 stage attempt authority。

Miscellaneous job/worker 分片
`go test -tags=integration ./internal/integration -run '^TestAccepted|^TestAutomatic|^TestJob|^TestInvalid|^TestRead|^TestProject|^TestPlatform|^TestServiceAuthentication|^TestLocal|^TestOneOfSeven|^TestSplit|^TestCPUMedia'` 已通过，耗时约 66 秒；覆盖 accepted request snapshot、automatic expiry、job child-row/attribution/expiry、project/admin debug dump、platform operator、local worker journal 和 CPU media stage worker。
