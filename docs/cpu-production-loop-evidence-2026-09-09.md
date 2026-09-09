# CPU mock ProductionAgent loop campaign

日期：2026-09-09。命令：

```bash
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=2 VELA_CPU_MOCK_WIDTH=4 \
  VELA_CPU_MOCK_TIMEOUT=5m go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockProductionLoopJobCampaign$' -count=1 -v -timeout=10m
```

结果：通过，8 个 Job、2 波各 4 个并发到达、4 个 persistent CPU mock workers，
总耗时约 28.9 秒。PostgreSQL schema migration 到 95；production loop、readiness、
capacity、heartbeat、mTLS transport、durable stream 和本地 Runtime subprocess
均实际运行。

主要观察：

- `visible_completions=8`，`charges=8`，总计每 Job 一次 Charge；
- 两波之后 `queued_jobs=0`、`running_jobs=0`、`active_allocations=0`、
  `active_leases=0`、`reserved_credit_minor=0`、`runtime_scratch_bytes=0`；
- 每个 worker 接受 4 次 registration、3 次 capacity report；heartbeat 总数为 8；
- 所有 worker 的 durable control session epoch=3，capacity observation sequence
  为 `4294967299`；
- `replayed_materialization_commits=4`，每个 worker 有 8 条 durable records；
- encoder 观测到 2 次 confirmed stale acquire，其余 worker 为 0，均未产生额外
  completion 或 Charge；
- `acquire_transaction_retries=0`、`acquire_deadlock_retries=0`、
  `acquire_serialization_retries=0`。

原始日志中记录了 4 条 `resume-materialization: ... AlreadyExists ... duplicated`
重试信息。这是 ACK/回包丢失后的幂等收敛路径：重复请求被数据库 authority 拒绝，
随后从 durable journal 恢复，最终只产生一次可见结果和 Charge。它证明了有限
campaign 的拒绝与收敛，不证明网络下绝不会发出重复请求。

证据仍有明确边界：Fleet membership、Node custody、protected mount、production
startup issuer、remote object storage、真实 UID/mount isolation、GPU、掉电恢复、
历史回收和 sustained arrivals 都不在本 campaign 内；Production Gates 仍为
**0/9**。

原始日志：[campaign.log](evidence/cpu-production-loop-2026-09-09/campaign.log)
