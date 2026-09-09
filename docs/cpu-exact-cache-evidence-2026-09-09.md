# Current CPU exact-cache campaign

基线：`104db9a`。命令：

```bash
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_TIMEOUT=5m \
  go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockExactCacheSourceTargetCampaign$' -count=1 -v -timeout=10m
```

当前代码通过。campaign 使用 PostgreSQL Testcontainer、schema migration 到 95、
实际 CPU subprocess、loopback Runtime gRPC、local exact-version object store 和
ffprobe 8.0.1；没有 GPU、真实生产 Fleet 或 Production Gate。

证据中的关键结果：

- source Job：2 个 admission candidate、2 个 admitted、1 次 Charge；
- target Job：两轮 `HitCandidates=2, Hits=1`，复用 source 的 exact bindings；
- 传输：`consumed_transfers=5`；直接分配回执 6；materialization artifacts 6；
- 商业不变量：两 Job 各一次 Charge，总计 `2500` minor units，reserved credit=0；
- 终态：queued/running/active allocations/active leases/reserved storage/scratch
  全部为 0；source/public target copies 和 cache carrying references 仍有明确
  持久记录；campaign 没有把 cache 命中伪装成 GPU 计算节省。

这是同一 CPU/mock campaign 的 exact-cache 证据，证明 miss/admit/hit/reuse、
TransferTicket 消费、一次 Charge 与终态资源清理在该装配中成立。它不证明
真实 GPU、生产 Node/Fleet/Pod→CRI、长期历史回收、持续 offered arrivals 或
Production Gates。

原始日志：[campaign.log](evidence/cpu-exact-cache-2026-09-09/campaign.log)
