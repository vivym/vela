# Journal grant transition

`7a7a9ff` 之后新增了 `JournalWriteGrant`：它只存在于 Node 内存，绑定一个
具体 `JournalEndpoint`、随机 nonce 和短期限。`IssueJournalWriteGrant` 重新
检查 endpoint 保留的 Runtime/Worker 原始 pidfd 与当前 owner 相同且仍存活；
`ActivateJournalWriteGrant` 只允许一次，将启动 endpoint 从 read-only 切换到
writable，并清空 nonce。错 endpoint、重复激活、过期 grant、上下文取消和
失效 pidfd 均拒绝。

`TestJournalReadOnlyGrantTransition` 通过，`go test ./...`、Linux arm64
`go vet` 和 golangci-lint 通过。这个验证只覆盖 Node 内存状态机和 CPU fixture；
grant 来源尚未接入真实 Fleet/TLS/PostgreSQL，一次性外部授权的持久记录、
崩溃恢复和生产 role routing 仍未闭环。Production Gates 保持 **0/9**。
