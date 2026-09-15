# Worker journal validation matrix with deployed pidfd broker

目标机：`marslab@100.111.196.116`，Ubuntu 24.04，kernel `6.8.0-137-generic`。
本次运行使用已部署的 `/run/vela/pidfd-broker.sock`，并把 broker parent directory
以 read-only mount 注入 Worker；signed Pod 同时声明
`VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET`。

`matrix.json` 的六个场景结果：

```text
normal                  completed  cleanup_verified=true
caller-replacement      failed     cleanup_verified=true
observer-channel-loss   failed     cleanup_verified=true
policy-response-loss    failed     cleanup_verified=true
helper-timeout          failed     cleanup_verified=true
helper-crash            failed     cleanup_verified=true
```

这组 receipt 证明 validation helper 能在不支持 `pidfs` 的目标机上使用真实 broker
完成 Worker journal socket mount 和精确 CRI cleanup。它仍是 validation-only，未接入
真实 signed Node composition、Fleet reservation、ModelRuntime Permit 或 Production Gate。
