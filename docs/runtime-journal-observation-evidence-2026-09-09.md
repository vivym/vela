# Observer 连续检查与 journal 路由撤销

日期：2026-09-09。基线 `275be0a`。范围是 trusted-Node 组件的原生 CPU 验证；
本轮将实际 observer 的生命周期接到持久 grant 消费和角色激活，仍未实现生产
批准策略、containerd 可信创建交付或完整 remote-owner Job。

## 接口与生命周期

`ActivateObservedJournalWriteGrant` 接受已独立签发的 operation-bound 内存 grant
及 `RuntimeObserverCustody`。它比较 observer 原始 target pidfd 与 endpoint
保留的 Runtime pidfd，要求 custody 已 Start，endpoint 仍只读且未被协调/激活。
无关的健康 observer 不可替代本次 Runtime；匹配失败不撤销那个无关进程。

匹配后 endpoint 与 `RuntimeJournalObservation` 一次性绑定。监控从 ledger 消费
之前启动，沿用 `ActivateReservedJournalWriteGrant` 的持久消费与最后激活入口。
同次调用在消费前、持久化后分别向实际 observer 的事件循环发送新 challenge；
失败会封禁角色路由并请求终止原始 target/observer。记录或非零证据摘要仍不能
重建这个对象，也不能代替独立启动批准。

运行期按配置周期执行 Check，周期限制为 `(0,1s]`，单次预算 `(0,5s]`。
一次成功响应的有效期最多为该次检查开始时间加上周期与预算，排队时间也计入。
使用包含单调时间的期限；并发检查用 CAS 避免旧响应缩短新期限。已有期限一旦
过期就不可恢复，后续成功响应不能重新开放路由。初次检查完成前路由保持拒绝。
角色处理入口和实际 Apply 调用前都会检查有效期；后台调度延迟不会导致无限期
可写。这个参数界限不是 Linux 调度、存储或进程终止的硬实时保证。

服务 context 取消、observer Check 失败或直接 custody revoke 都停止监控。
`observation.Close()`、endpoint Close 和已激活路由的 ledger Close 同样触发不可逆撤销。
`Done()` 表示监控退出且已尝试进程撤销，`Err()` 保留第一退出原因及撤销错误。
调用方仍保管 custody，可通过 `TargetExited` 检查原始进程，然后再 Close custody。
不确定 Permit 回包必须由上层 Close 本次 observation；此处没有发送 Permit。

## 并发与失败边界

- observer 网络交换不持有 endpoint 锁；取消先原子封禁路由，再请求进程撤销，
  不等待 ledger append/fsync。因此持久化阻塞不会阻止 observer 故障处理。
- endpoint 锁内的过期检查只封禁并取消；进程撤销由 monitor 完成，避免把进程撤销绑定到 journal 锁。attachment 使用
  endpoint → custody 锁序，并提前拒绝已附加对象；不能持 custody 等待 endpoint I/O。`Close` 在锁外请求撤销，锁内
  再次捕获 observation，解锁后补撤销同时附加的对象。
- endpoint 的现有 Handle/Apply 仍可能持锁等待自身 journal I/O。Close 的资源
  释放可等待它，但原子封禁与进程终止请求先发生。已经发出的 Apply 仍可能在
  撤销后落盘；不能把撤销当作回滚或历史未执行证明。
- 消费中断与重开继续沿用上轮的保守规则。这里的周期性 challenge 依赖可信
  observer 的 exec/clone 约束，不能单靠心跳证明任意 caller 的执行连续性。

## 本轮验证与审查修复

真实 C observer 创建非 root PID-1 Go caller，通过私有 socketpair/pidfd 完成
custody 交付、Start 和 live Check。该 caller 的实际 RPC、Node 持锁 journal、
ledger reservation 与有效 Worker floor 命令用于本次组合测试；Pod/CRI、Fleet
reservation 和独立 issuer 在新增 observer 场景中仍为 fixture。

七组新增测试覆盖：

1. 激活和 Worker 实际写入成功；Close、取消、observer SIGSTOP/SIGKILL、通道
   丢失、ledger Close、endpoint Close 后拒绝写入，且原始 target pidfd 观察到退出。
2. 无关 observer 被拒绝，不消费记录，也不终止该无关 observer。
3. ledger append 前或 fsync 后阻塞期间取消/挂起，在解除阻塞前检查 target 退出、
   route 拒绝；解除后消费调用不能完成激活。
4. 注入已过期响应期限后拒绝写入，主动 Check 也不能恢复，monitor 退出并保留原因。
   这是延迟调度的定向模拟，原生 SIGSTOP 则独立覆盖真实 event-loop 挂起。
5. 八次并发 attachment/Close 交错不死锁；已附加 observation 必须终止并拒绝写入。
6. 持有 Handle/Apply 使用的 endpoint 锁来模拟 journal 阻塞；重复附加立即拒绝，
   取消后原始 target 退出不等待此锁释放。
7. 丢失 observer 描述符后保留 `EBADF` 撤销错误；路由仍拒绝，原始 target 退出。

提交前 Standards/Spec 双轴审查找出并修复了：过期期限可能被后续成功检查刷新、
Close 漏撤销并发附加 observation、进程撤销错误被吞掉，以及重复附加持 custody 等待 endpoint I/O。相应回归测试保留在源码中。

## 最终同源运行

最终原生 Linux/arm64 runner 退出 `0`：**26 个顶层 race 测试**全部通过，无
SKIP、FAIL 或 DATA RACE。其中包含上述 **7 组** observation 测试、前轮 5 组
持久激活测试和现有实际 remote CLI / publication / custody 回归。

同一镜像的 PostgreSQL/TLS 三场景通过，integration package 耗时 **19.119s**。
这条回归仍使用普通 reserved-grant 入口：正常分支实际 `Floor=1` 且关闭后拒绝，
丢回包分支拒绝消费并观察 `Floor=0`，缺少 ptrace 在预留前拒绝；它没有把新增
observer 场景与真实 CLI/CRI/Fleet 合并，不能据此宣称整条启动链已经统一。

全库默认 `go test ./...` 通过（可复用缓存）；Linux Node `go vet` 和固定版本
`golangci-lint v2.13.1` 通过（`0 issues`），shell 语法及 diff 检查通过。
源码、镜像、二进制、逐测试结果和原始日志摘要见
[机器可读证据](runtime-journal-observation-evidence-2026-09-09.json)。最终证据目录为
`/tmp/vela-observed-grant-closure-20260909`；较早目录是审查修复前的中间结果。

```sh
VELA_REMOTE_CLI_EVIDENCE=/tmp/vela-observed-grant-repro \
  bash hack/run-remote-runtime-cli-native.sh
VELA_RUNTIME_STARTUP_NODE_IMAGE="$(cat /tmp/vela-observed-grant-repro/image.txt)" \
  go test -race -tags=integration ./internal/integration \
  -run '^TestRuntimeStartupNodeProcessPostgresTLS$' -count=1 -v -timeout=3m
```

## 剩余工作

本轮新增路径使用真实 observer 与真实进程，但不是实际 `serve-remote` 的统一
启动批准入口；现有 CLI observer 测试仍是独立装配。生产 Node/containerd 的可信
observer 创建与交付、Registry 批准策略、实际 CLI/CRI/Fleet 同次检查和 Permit
交付仍需统一。普通组件 grant API 仍供 trusted Node 使用，本轮没有强制所有
生产调用必须走 observed 入口。后续才能验证完整 remote-owner CPU Job。

原始 target 的退出证据不等同于所有生产后代停止；SIGKILL 不等同于断电恢复。
Production Gates 保持 **0/9**。

## Node startup socket 协调器增量

`RuntimeStartupCoordinator` 是 Node 侧 startup socket handler 的严格适配器。它固定
expected `BackendStartupRequest`、具体 plan、operation-bound grant 和 observer
custody；只有 exact canonical request 通过 `ActivateObservedJournalWriteGrant`
完成持久消费及路由激活后，才生成 request-digest 匹配的 `Permit=true`。错误或
mismatch 返回拒绝；handler 本身 one-shot，不能通过重复请求重新获得 Permit。

新增原生测试覆盖正确请求和 mismatch 不消费 grant。当前实际 `serve-remote` CLI
runner 仍由测试 harness 直接提供 listener/fixture decision；本适配器尚未成为
生产 socket listener 的唯一装配路径，也没有引入通用 `Authorize() == nil`。下一步
是把真实 Node startup listener 的认证、request handler、coordinator 和 Permit
回包接成同一生命周期，再做 PostgreSQL/TLS 与 CLI/CRI 同次验证。

## Startup socket listener 接入

新增 `RuntimeStartupServer`，复用 Node 的 Unix peer credential 与 RuntimeCaller
challenge，限制 request payload 大小和 exchange deadline，再把 canonical request
交给 `RuntimeStartupCoordinator`。它不创建、chmod、unlink socket，也不解析请求
来推导允许 UID；这些仍由 trusted Node assembly 提供。`Serve` 负责 bounded accept
和 shutdown，`HandleConnection` 可被受保护的现有 listener 直接调用。

Linux runner 已重新编译并通过 `TestRuntimeStartupServerRejectsInvalidConfiguration`
以及协调器/observer/CLI 组合测试；固定 lint 为 `0 issues`。当前仍没有把所有生产 Node 启动代码强制改为该 server。审查 `cmd/vela-node-agent`
后确认其现有入口只装配 WorkerInstance/Remediation；它没有 Runtime startup ledger、
plan/Fleet reservation、grant issuer 或 observer custody 来源。直接在该入口构造
默认授权会扩大权限并破坏证据边界，因此真实 socket 路径、权限发布和这些生命周期
来源仍需先作为明确的 Node startup orchestration 配置接入，再做 PostgreSQL/TLS
同次回归。


## Explicit startup orchestration composition root

新增 `RuntimeStartupOrchestration` 作为 Node-owned composition root。构造必须同时提供
ledger、verified plan、exact expected request、operation-bound grant、observer custody、
非 root caller credentials 和 bounded observer/exchange timeouts；没有默认值，也不从
history/receipt 重建。`Serve` 只接收 trusted assembly 已创建的 Unix listener，
`Shutdown`/`Close` 统一收敛 server、coordinator 和 observation 生命周期。

新增 native 配置缺失拒绝测试，并在最终 runner 中编译验证。它仍是 library composition
boundary；现有 `cmd/vela-node-agent` 尚未提供这些 Runtime startup 对象的真实来源，
所以本轮没有把它伪装成 production command integration。
