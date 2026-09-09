# Durable CPU exact-cache campaign

基线：`7cce03c`。命令：

```bash
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_TIMEOUT=5m \
  go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockDurableStreamExactCacheCampaign$' -count=1 -v -timeout=10m
```

该 campaign 在 PostgreSQL schema migration 95、实际 CPU subprocess、loopback
Runtime gRPC、local journal 与 exact-version object store 上完成。它记录的是
durable stream 与 exact-cache 的当前 CPU/mock 装配，不包含 GPU、生产
Kubernetes/containerd、真实 Fleet/ProductionAgent 或 Production Gate。

关键结果：

- source Job 完成 2 个 admission、2 个 admitted、1 次 Charge；target Job 两轮
  `HitCandidates=2, Hits=1`；
- `consumed_transfers=5`，`direct_allocation_receipts=6`，`replayed_commits=4`；
- durable records：`dit=1`、`encoder=1`、`thumbnail=2`、`vae=2`；
- 两个 Job 各一次 Charge，总计 `2500` minor units；
- queue/running/allocation/lease/storage/scratch 终态均为 0；
- `local_journal_bytes=70479`，exact-cache entries=2。

这证明了该有限 CPU/mock 装配中的 miss/admit/hit/reuse、TransferTicket 消费、
durable replay、一次 Charge 和终态资源清理。`replayed_commits=4` 与 durable
records 是运行时 receipt 的证据，不是掉电、磁盘损坏或跨重启 crash recovery
的证明；有限 campaign 也不证明 sustained arrivals、生产启动装配或 GPU 正确性。

原始日志：[campaign.log](evidence/cpu-durable-exact-cache-2026-09-09/campaign.log)
