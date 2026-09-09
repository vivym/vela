# Startup 请求与运行期寿命分离，以及可取消的关闭

日期：2026-09-10。基线 `78b4f0e`；最终受测源码由附带 patch 精确标识。
Production Gates 仍为 **0/9**。本轮不等于生产 Node 启动装配或全仓验证闭合。

## 实际问题与修复

`RuntimeStartupServer.HandleConnection` 为一次握手创建 deadline context，并在返回时
取消它。此前 coordinator 将该 context 直接传给长期 observer，导致即使 Permit
成功发出，handler 返回也会立即撤销 journal 路由，甚至终止原始 Runtime。

现在 coordinator 自己拥有运行期 context。请求取消仍能中止正在进行的激活；只有
激活成功且请求未取消时才解除这条关联。成功回包后的 observer 由 Node 装配的
关闭和监听服务寿命管理。回包失败会立即停止 coordinator，不能因为运行期与请求
分离就遗留可写路由。socket write 成功仍不证明对端已收取，不是确认回执协议。

此前 `Close` 只关闭 coordinator，未关闭 listener；`Serve` 的 context 取消也不能
唤醒 `AcceptUnix`。另外 coordinator 在完整激活和 ledger append 期间持有自己的锁，
shutdown 会在取得锁之前阻塞，调用者设置的 deadline 无法约束该等待。

修复后的关闭先停止接入、取消运行期并通过独立 pidfd 请求撤销，再等待 handler、
observer 和 endpoint 清理。`Shutdown(ctx)` 的等待可超时，后台清理继续；`Close`
完整等待，同步或并发重复调用均等待同一个清理结果。Shutdown 在 Serve 之前调用
也永久封禁这个 server。装配对象不再维护另一份容易漂移的 `closed` 标志。

关闭取消或请求撤销不等于中断已进入内核的 fsync，也不证明所有后代已经退出。
已经获准的在途 journal 写入仍可能落盘，需要历史对账；不能把超时返回当作清理完成。

## 验证

[机器可读证据](runtime-startup-lifetime-evidence-2026-09-10.json) 包含确切命令、
源码 patch、镜像/二进制摘要、40 个测试名称及逐项结果。

- 同源 Linux/arm64 原生 race suite：40 PASS、0 FAIL、0 SKIP、0 MISSING，
  无 `WARNING: DATA RACE`。测试容器无网络、无 host mount、无 GPU。
- 新增正常 socket 路径使用 ledger 实际保留的同一个原始进程进行第二次认证。
  成功回包并结束 exchange 后，跨多个 observer 周期仍可写；消费落盘后关闭连接，
  回包失败则撤销。该第二次握手是测试适配，不是已经连通的真实 CLI 启动入口。
- 阻塞 ledger 的 `before-append`：shutdown 在自己的 deadline 返回，同时在释放
  append 阻塞前观察原始 target 退出、journal 拒绝写入；恢复 append 后不能 Permit。
- Close 或服务 context 取消同时停止 listener 与已激活路由；并发 Close 等到 custody
  handles 实际释放。Shutdown-before-Serve 不能重新开放。
- `go test ./internal/nodeagent ./cmd/vela-node-agent ./hack -count=1` 通过。
- Linux/arm64 golangci-lint v2.13.1：0 issues；shell syntax、diff 检查通过。
  Darwin 默认测试不能替代上述 Linux 原生验证。Diff whitespace 检查排除原始
  `source.patch` 的 context 前缀和反证日志的原始空白，保留证据字节不做格式化。

作为反证，把三个被修改的实现文件替换回 `78b4f0e`，保留新增测试和测试 helper，
重新编译 Linux race 二进制。四个选定顶层测试均 FAIL，退出码 1：

- `TestRuntimeStartupCoordinatorOutlivesSuccessfulExchange`：`context canceled`。
- `TestRuntimeStartupServerCancellationClosesIdleListener`：仍阻塞在 `AcceptUnix`。
- `TestRuntimeStartupServerShutdownBeforeServeIsTerminal`：重新进入 Accept，直到 I/O timeout。
- `TestRuntimeStartupServerOwnsPostReplyLifetime/delivered`：成功回包后 backend 被取消。

旧实现的 `connection-lost` 子场景仍通过，因为旧逻辑无论成功或失败都会取消路由；
本轮新增显式失败撤销，是为了在修复成功路径寿命后仍保持拒绝行为。
见[反证日志](evidence/startup-lifetime-2026-09-10/baseline-native.log)。

## 仍待实现的关键装配

1. `PrepareRemoteStartupOrchestration` 已要求一个通过 Receive 认证的 caller，
   `RuntimeStartupServer.HandleConnection` 却还会再次 Receive。应将已认证 caller、
   Fresh reservation、激活与回复纳入同一次连接生命周期，避免生产 CLI 被迫二次握手。
2. Server 当前检查 kernel caller 的 UID/GID 和 request bytes；还需把实际回复对象与
   reservation 保留的原始 Runtime pidfd 显式比对。同 UID、同请求不能替代 same-process
   证明。应加入同 UID 的另一进程尝试获取 Permit 的拒绝测试。
3. 生产 plan/CRI/Fleet、可信 observer 创建及独立授权策略仍未接入
   `cmd/vela-node-agent`。本轮正常测试仍使用批准和 inventory fixture。
4. 全链 remote-owner CPU Job、独立 Worker journal 保管权限、真实进程替换与后代隔离、
   长期资源上界、power-loss 恢复，以及此前未通过的完整 CI integration 分片仍未闭合。
