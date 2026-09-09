# Multi-member Worker barrier and stop semantics

日期：2026-09-09。验证命令：

```bash
go test -race ./internal/stageworkeragent -run \
  'TestMultiMember(StartBarrier|Cancellation|ExecutionDrain|TerminalLive)|TestCancellation' \
  -count=1
```

结果：通过，耗时约 2.3 秒。

验证到的语义：

- 所有成员必须先 Prepare，之后才允许 Start；
- 任一成员 Start 失败会取消整个 allocation，并向已启动和未启动成员发送
  cancellation；不会返回 `BarrierPassed`；
- `Cancel` 的 `AcknowledgedMembers` 只表示 backend 接受取消请求，`AllStopped`
  保持 false；只有后续对全部成员做 authority/digest/identity 校验的 `Status`
  都返回 `STOPPED` 才能得到 `AllStopped=true`；
- 单个成员缺失、错误、stale authority、malformed failure evidence 或部分
  execution drain 都会保持不完整结果，不会恢复共享容量；
- 取消期间的精确 terminal stale response 可以作为已结束成员处理，但不会把
  未确认的成员计入 stopped；
- barrier rollback 有明确 timeout，不等待不合作 backend 无限阻塞。

这组测试证明的是 Agent/Runtime RPC 层的多成员状态机。仍未证明真实生产后代
进程的停止、跨 UID/mount namespace 的隔离、Node custody 和同一 remote-owner
Job 的完整装配；这些仍属于生产验证缺口。

相关实现：[agent.go](../internal/stageworkeragent/agent.go)、
[execution_drain.go](../internal/stageworkeragent/execution_drain.go)。
