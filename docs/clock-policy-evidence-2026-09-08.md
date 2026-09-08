# CPU campaign production clock-policy validation

Date: 2026-09-08. Baseline: `0a3169b`. PostgreSQL 94, Worker journal 5,
Runtime journal 8. Production Gates remain **0/9**.

## Confirmed discrepancy and causal boundary

The CPU campaign omitted `MaxClockSkew` in Worker admission, ModelRuntime,
Control ingress, input resolution and materialization. Its execution backend
used one second. Production entry points use the shared
`authoritypolicy.ProductionMaxClockSkew` of 30 seconds.

This increment aligns the fixture consumers and execution backend with that
existing production policy. It does not change the production bound. Stream
admission errors now identify Begin versus Runtime entry while preserving
`errors.Is` through `%w`; production-loop campaign errors identify the Worker
and stage. No STALE retry or uncertain allocation release was added.

The old `11ce026` no-race failure remains preserved in the
[lookup report](journal-lookup-evidence-2026-09-08.md). It lacks the exact Worker,
authority timestamps and rejecting boundary. The new deterministic reproduction
proves that clock mismatch can cause zero-skew rejection. It does **not** prove
that this caused the old failure, or that lookup optimization repaired it.

## Deterministic admission evidence

`TestAssignmentAdmissionProductionClockPolicy` freezes the consumer verification
clock against the unchanged signed assignment. It checks:

- One-second consumer lag fails under zero skew, reproducing the configuration
  defect without Docker timing or scheduler assumptions.
- One-second lag and the inclusive 30-second bound permit Begin, input completion
  and Runtime entry under the production policy.
- The bound plus one nanosecond, exact wall-clock expiry, and the end of a shorter
  signed monotonic-validity interval all return `ErrStale`.
- Rejection returns no admission handle, changes neither durable bytes nor the
  in-memory watermark, and allows the original legal assignment after reopening
  the journal with a valid clock. Expiration is not treated as storage corruption.

The monotonic-validity case expires while wall-clock validity remains. This test
uses a deterministic verifier clock; existing Runtime watchdog and queued-call
tests separately exercise local execution deadline enforcement. A frozen clock
test does not simulate host suspend, NTP steps or cross-host clock discipline.

## End-to-end scope

`TestCPUMockProductionLoopClockOffsetCampaign` uses a fixed -1-second offset for
Worker/Runtime StageAuthority verification, Worker MaterializationAuthority
verification and input ticket time checks. Control/DB issuance, signatures,
capacity observations, transport and scheduling clocks remain unchanged. Runtime
Service, Supervisor and journals run in the test host; model drivers are native
CPU mock subprocesses. This isolates consumer validation rather than pretending
to shift the whole machine clock.

Production-loop load receipts include the configured bound/offset in nanoseconds and the count of
assignments actually received with `issued_at` later than the consumer clock.
Every Worker must observe at least one such assignment in the lagged campaign,
in addition to all existing Job, billing, replay and journal assertions.

The 32-Job / 128-Stage race run passed in 113.246 package seconds. Each of the four
Workers observed 32 future-issued assignments. All 32 Jobs completed with exactly
32 Charges, four lost materialization responses replayed, and 32 retained records
per Runtime. Final active allocations, leases, payload scratch, watchdogs and
execution/finalization/cache pins were zero. Retained journal metadata remained
1,299,072 bytes; this is not bounded-history or production-soak evidence.

These are correctness runs. Other repository validation ran concurrently during
the offset campaign. Their elapsed times are provenance only and must not be used
as a before/after performance comparison with earlier lookup measurements.

## Current-source verification

All four campaign receipts have Go/SQL/module/media source digest
`fefd0031f9e8936ee12f3d8c195e4c2fe3c4dbf49cffbb74d14a3416574b9368`.
The [evidence JSON](clock-policy-evidence-2026-09-08.json) contains their full
receipts plus byte sizes and SHA-256 hashes for all eleven preserved raw logs.
Tool exit codes were checked before committing. Go is 1.26.7; native media
validation uses ffprobe 8.0.1; ordinary golangci-lint is 2.13.1.
Raw Testcontainers output ends its `Connected to docker:` line with a space.
Whitespace checking excludes only `clock-policy-offset-race.log`,
`clock-policy-normal.log` and `clock-policy-cache-race.log` in
`docs/evidence/clock-policy-2026-09-08/`; all three retain byte-identical raw output
verified against their recorded hashes. Source and report whitespace checks pass.

| Validation | Result |
| --- | --- |
| Deterministic authority/Worker/production-ingress clock tests, race | PASS across five packages; Worker package 14.097 s |
| Complete Worker package, uncached race | PASS, 56.793 s |
| Runtime clock/skew, pre-write expiry, queued-call and watchdog selection, uncached race | PASS, 4.644 s |
| 32-Job production loop, -1 s consumer verification offset, race | PASS, 113.246 s |
| 32-Job production loop, zero injected offset, no race | PASS, 48.737 s |
| Direct and durable-stream exact-cache source/target, race | PASS, 21.074 s combined |
| Ordinary repository tests / vet / lint | PASS; lint 0 issues; unchanged packages may use the Go test cache |
| Static Linux/amd64 cross compilation | PASS; Linux tests were not executed |

The zero-offset campaign also completes 32 Jobs / 128 physical Stages, four exact
commit replays and final resource reconciliation. It observed no future-issued
assignment at receipt time. Both modes advance all Control sessions to epoch 3.
The cache checks preserve physical source/target, ADMIT/HIT, exact-version,
pin/reference and one-Charge assertions. They are compatibility checks through
direct-service/durable-stream fixtures, not production-loop cache scheduling or
a new cross-organization isolation campaign. Ordinary lint does not supersede
the previously recorded integration-tag lint backlog.

Reproduction commands (from repository root; Docker and ffprobe 8.0.1 required):

```sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=4 VELA_CPU_MOCK_WIDTH=8 VELA_CPU_MOCK_TIMEOUT=5m go test -race -tags=integration ./internal/integration -run '^TestCPUMockProductionLoopClockOffsetCampaign$' -count=1 -v -timeout=10m
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=4 VELA_CPU_MOCK_WIDTH=8 VELA_CPU_MOCK_TIMEOUT=5m go test -tags=integration ./internal/integration -run '^TestCPUMockProductionLoopJobCampaign$' -count=1 -v -timeout=10m
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_TIMEOUT=5m go test -race -tags=integration ./internal/integration -run '^TestCPUMock(DurableStreamExactCache|ExactCacheSourceTarget)Campaign$' -count=1 -v -timeout=10m
```

## Remaining architecture work

Protected Node custody with Registry/Fleet startup, full owner process replacement,
safe history reclamation beyond 32 records and sustained offered arrivals remain
open. The [current validation priorities](mock-hardening-validation-2026-09-05.md#current-closure-priorities)
now distinguish those gaps from the superseded schema-89/90 checklist.
No GPU, remote rollout, launch receipt or full-project closure is claimed.
