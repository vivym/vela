# CPU mock ProductionAgent clock-offset campaign

命令：

```bash
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=2 VELA_CPU_MOCK_WIDTH=3 \
  VELA_CPU_MOCK_TIMEOUT=5m go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockProductionLoopClockOffsetCampaign$' -count=1 -v -timeout=10m
```

结果：通过，6 个 Job、4 个 persistent CPU mock workers，耗时约 27.5 秒。
worker verifier 注入 `-1s` offset，仍在声明的 `authority_max_clock_skew=30s`
范围内；所有 worker 都观察到 future-issued assignments（每 worker 6 次），但
没有错误接受或额外 Charge。最终 `visible_completions=6`、`charges=6`，队列、
运行、allocation、lease、reserved credit、scratch 均清零；每 worker 接受 4 次
registration、3 次 capacity report、6 次 heartbeat，control session epoch=3。

这验证了当前 CPU/mock production loop 对允许时钟偏移的 freshness 行为和 durable
replay 收敛。它不证明超出 skew 上限、跨主机时钟同步、真实 Node/Fleet 装配或
Production Gates。

原始日志：[campaign.log](evidence/cpu-production-loop-clock-offset-2026-09-09/campaign.log)
