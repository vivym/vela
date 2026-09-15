# Worker journal validation matrix with broker-bound signed Pod

这组 receipt 使用最新 validation launcher：signed Pod 同时声明
`VELA_WORKER_JOURNAL_SOCKET` 和 `VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET`，helper
在创建 Worker 前核对两者，并把两个 parent directory 只读挂载到 Worker。目标机使用
真实 `/run/vela/pidfd-broker.sock`，kernel 为 `6.8.0-137-generic`。

结果：`normal=completed`；`caller-replacement`、`observer-channel-loss`、
`policy-response-loss`、`helper-timeout`、`helper-crash` 均为 `failed`，且六个场景
全部 `cleanup_verified=true`。

这仍属于 validation-only；它证明了 broker-bound Worker journal mount 和 fail-closed
cleanup，尚未证明真实 Node/Fleet/ModelRuntime composition 或 Production Gates。
