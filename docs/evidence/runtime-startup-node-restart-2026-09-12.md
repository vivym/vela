# Runtime startup Node restart evidence

目标机：`marslab@100.111.196.116`，Ubuntu 24.04，kernel `6.8.0-137-generic`。
执行：

```text
TestRuntimeStartupReservationProcessCrashRecovery PASS
```

测试在十个 durable boundary 注入 `SIGKILL`：Node intent append 前后、Fleet reply、
reservation receipt append 前后、journal grant append 前后。每个场景均重新打开
startup ledger 并验证：

- 已持久化的 intent/receipt/grant history 保持原值；
- Fleet reservation 不被重复调用；
- Node 不重建原始 owner pidfd；
- 失去原始句柄时 `RecordExit` 返回 `ErrRuntimeNamespaceOwnerLost`；
- restart 不产生新的 grant 或 permission。

该测试证明的是 Node startup ledger 的 restart-safe 语义和 fail-closed 边界。它仍使用
受控 Fleet fixture，不是完整 signed plan → CRI → observer → Fleet → journal →
ModelRuntime 生产 composition，因此 `Production Gates` 仍为 `0/9`。
