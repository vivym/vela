# CPU mock concurrent long campaign evidence

日期：2026-09-09。当前提交在本地 PostgreSQL 17 Testcontainer、4 个常驻 CPU
mock Runtime 下运行：

```bash
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
VELA_CPU_MOCK_WAVES=8 \
VELA_CPU_MOCK_WIDTH=8 \
VELA_CPU_MOCK_TIMEOUT=15m \
go test -tags=integration ./internal/integration \
  -run '^TestCPUMockConcurrentAdmissionRuntimeCampaign$' \
  -count=1 -timeout=20m -v
```

结果：`PASS`，总耗时约 `20.4s`。

本次 offered arrival 共 `8` 波、每波 `8` 个 Job，总计 `64` 个 Job。每个 Job
均完成一次 Charge，总计 `64` 次 Charge；每波结束时 queued/running、active
allocation、active lease、reserved credit、scratch 和 runtime watchdog 均回到
`0`。每波 maintenance 也均无失败，acquire transaction、deadlock 和
serialization retry 均为 `0`。

观察到的资源趋势：

| 指标 | 观察 | 解释边界 |
| --- | --- | --- |
| goroutine | 全程约 `93` | 该 fixture 内没有明显 goroutine 随波次增长 |
| worker FD | 每个 native mock worker 约 `12` | 未观察到 worker FD 随波次增长 |
| test process RSS | 约 `92 MB` 到 `96 MB` | 仅是本进程采样，不包含 Docker VM/PostgreSQL |
| scratch/watchdog | 每波清零 | 本地执行临时资源能收敛 |
| `MaterializedBytes` | `108,168` 到 `865,344` | 已提交历史/Artifact 保留，不能按 scratch 泄漏解释 |
| PostgreSQL size | 约 `30.0 MB` 到 `38.8 MB` | 数据库历史和审计记录仍按设计累积 |

这次运行扩大了有限 CPU/mock 压力范围，并未证明 sustained open-loop throughput、
无限期 RSS/FD/journal 上界、历史安全回收或真实多节点生产装配。尤其是
`MaterializedBytes` 与数据库大小的增长说明，长期运行仍必须依赖已证明的 cutoff/
reclamation 协议和独立的长期压力验证；不能用 wave 末 scratch=0 推断存储无界增长
已经解决。

