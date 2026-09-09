# CPU/mock concurrent arrival campaign

命令：

```bash
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=2 VELA_CPU_MOCK_WIDTH=8 \
  VELA_CPU_MOCK_TIMEOUT=5m go test -tags=integration ./internal/integration \
  -run TestCPUMockConcurrentAdmissionRuntimeCampaign -count=1 -v
```

结果：通过。PostgreSQL 17 Testcontainer 完成 schema 95 migration，4 个 persistent
CPU mock worker 处理 2 波、每波 8 个并发 arrival，共 16 个 Job；`jobs_per_second`
约 `3.94`，`mean_queue_seconds` 约 `0.877`，`max_queue_seconds` 约 `1.367`。

关键终态与计费结果：

- `visible_completions=16`、`charges=16`、`posted_credit_minor=20000`；
- `queued_jobs=0`、`running_jobs=0`、`active_allocations=0`、`active_leases=0`；
- `reserved_credit_minor=0`、`runtime_scratch_bytes=0`、`pool_active_counters=0`；
- `acquire_transaction_retries=0`、`acquire_deadlock_retries=0`、
  `acquire_serialization_retries=0`；
- 两个 wave 的 maintenance 均没有失败。

这证明有限并发 arrival 下 admission、Runtime subprocess、数据库调度、计费和
清理路径能收敛。它不是 production soak：PostgreSQL/Docker VM 资源未完整采样，
没有真实 Node/Fleet custody、远程对象存储、GPU 或 power-loss recovery；长期开放环
资源上界仍未得到证明。
