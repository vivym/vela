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

## 2026-09-09 durable-stream rerun

另外执行了：

```bash
VELA_RUN_CPU_MOCK_CAMPAIGN=1 go test -tags=integration ./internal/integration \
  -run TestCPUMockDurableStreamExactCacheCampaign -count=1 -v
```

该次真实启动 PostgreSQL 17 Testcontainer 并完成 schema 95 migration，四个 CPU
mock Runtime 通过 durable stream 执行 source miss/admit 与 target exact-cache
hit/reuse；`consumed_transfers=5`、两次 Charge 共 `2500` minor units、终态
queue/running/allocation/lease/scratch 均为 0，committed response replay 为 4。
进程采样中四个 mock subprocess 的 RSS 与 FD 均保持有限增长，campaign 用时约
`2.655s`（不含容器启动）。这是一次 bounded rerun；它不证明长期开放环资源上界、
生产 Node/Fleet custody 或 history reclamation（后者由独立测试覆盖）。
