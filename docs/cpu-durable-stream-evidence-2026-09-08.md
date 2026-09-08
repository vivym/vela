# Complete CPU Jobs through durable Stage Worker Control

Date: 2026-09-08. Base: `47771ee`. PostgreSQL schema 94; Worker journal 5;
Runtime journal 8. Production Gates remain **0/9**.

The new opt-in campaigns complete native CPU Jobs through the real mTLS
StageWorkerControl transport, production Handler/Executor/PostgreSQL operations,
durable StreamAgent, Worker assignment admission and Runtime execution journal.
Every resident Worker loses one accepted materialization response, reconstructs
its StreamAgent from the on-disk materialization record, confirms the committed
result and continues executing. Exact-cache source/target Jobs use the same path.

This increment changes test composition and evidence, not production authority,
startup permissions or journal formats. The older CPU load harness called Go
control services directly; its 512-Job result did not cover this durable path.
The old entrypoints retain their original behavior and limitations.

## Measured result

Final source-tree digest:
`75ebfdbc4548bba541fbb2b5653f3ae0042b6301de3a4d568ce594712b2ea4eb`.
The campaign hashes Go/SQL sources, module files and media fixtures before and
after execution. Full receipts and local validation-log hashes are retained in
[the evidence JSON](cpu-durable-stream-evidence-2026-09-08.json).

| Observation | Concurrent Job campaign | Exact-cache campaign |
| --- | --- | --- |
| Jobs | 32, four waves of eight arrivals | Two distinct semantically identical Jobs |
| Physical Stage executions | 128; 32 for each of four resident Workers | Source 4, target 2 |
| Lost accepted commit responses recovered | 4, one per Worker | 4, one per Worker |
| Visible completions / Charges | 32 / 32 | 2 / 2 |
| Posted credit, minor CNY units | 40,000 | 2,500 |
| Consumed transfers / direct allocation usage receipts | 96 / 128 | 5 / 6 |
| Exact cache | Disabled | 2 ADMIT, 2 HIT; exact source versions and active pins checked |
| Final local payload scratch | 0 bytes | 0 bytes |
| Retained local journal bytes | 1,298,173 | 70,428 |
| Worker/Runtime retained records | 32 each for encoder, dit, vae, thumbnail | 1, 1, 2, 2 respectively |

The final race campaign took 78.160 package seconds (63.21 seconds for the Job
test and 11.93 seconds for exact cache). Measured Job-wave elapsed time was
54.334116 seconds. These are bounded, instrumented local runs, with another
compatibility campaign running on the host; they are not sustained throughput,
latency SLOs or a direct-versus-durable performance comparison.

Assertions independently compare each Worker's retained allocation count with
PostgreSQL, match every Runtime original authority to its Worker record, and
require Runtime seal/drain plus Worker CLOSED/input-drain checkpoints. Final
materialization journals have no unresolved records. Job conservation checks
retain one Charge/completion per Job, non-overlapping allocations, released
leases, credit/storage/capacity counters and allocation usage receipts. Cache
checks retain exact-version binding, separately owned public copies, payload
digests and real ffprobe validation. These establish logical artifact/receipt
conservation; they do not claim physical storage writes occurred only once.

All four native mock process PIDs remain unchanged through each campaign.
Post-maintenance active execution/finalization/cache pins and runtime watchdogs
are zero. Cache entries and their carrying references intentionally remain in
the cache campaign.

## Failures that shaped the test

Initial assembly was rejected because the eager native Runtime startup had
already populated scratch before first-use Worker journal initialization.
The test now establishes the Worker journal before starting native processes.
The Service epoch remains allocated by its epoch store; the independent Worker
binding records the expected allocated epoch.

Replaying a request ID in the same live stream produced `AlreadyExists`.
Recovery now closes and replaces the mTLS transport before reconstructing the
StreamAgent. The fault is injected after the real client receives ACCEPTED and
before its caller receives the result: this is application acknowledgement loss,
not a packet-loss or process-crash experiment. The new connection deliberately
uses the same fixture-authenticated logical session; production epoch advance,
readiness and reattachment are separate obligations.

Concurrent scheduling exposed a confirmed STALE result from a changed
CapacityPool snapshot. The harness permits at most 32 confirmed STALE polls,
each with a new command ID; it never treats uncertain transport failures as
confirmed negatives. The final run observed 11 STALE polls, recorded separately
from PostgreSQL transaction retries. ProductionAgent discovery/orchestration is
not covered by this harness, so its contention recovery remains open.

The production input resolver also rejected the initially uniform connector
configuration with `TransferTicket destination mismatch`. The fixture now uses
the actual catalog connector revision for DiT, VAE and thumbnail independently.
Authority, destination and empty-directory checks were not relaxed.

## Reproduce and validation scope

Prerequisites: local Docker, Go 1.26.7, ffprobe 8.0.1, ps and lsof. PostgreSQL
uses disposable Testcontainers; model subprocesses use native CPU mock binaries.

```sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
VELA_CPU_MOCK_WAVES=4 VELA_CPU_MOCK_WIDTH=8 VELA_CPU_MOCK_TIMEOUT=5m \
go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockDurableStream(Job|ExactCache)Campaign$' \
  -count=1 -timeout=10m -v
```

The final-source compatibility race batch passed in 65.590 package seconds:
the original CPU load and exact-cache entrypoints, terminal materialization
replay after TTL/reconnect, rejection without a durable receipt, and file
materialization-journal replay after TTL. Earlier six-Job and sixteen-Job durable
campaigns also passed before the final lint-only selector correction.

Ordinary `go test ./...`, `go vet ./...`, golangci-lint 2.13.1 (0 issues), and
`make test-cross` passed. The final integration-tag lint reports **82 existing
findings**, none in the three changed campaign files. It is not a clean full
integration lint result. Integration campaigns remain opt-in and skip without
`VELA_RUN_CPU_MOCK_CAMPAIGN=1`; ordinary CI alone does not run them.

## Remaining closure, in dependency order

1. **Trusted custody and full production composition.** Integrate the candidate
   Node-private typed journal service with separate Worker/Runtime roles,
   protected mounts, current Registry/Fleet activation, startup outcomes and
   ProductionAgent orchestration. Fixture registration, direct capacity reports
   and loopback Runtime gRPC do not validate that deployment. Preserve the
   [custody design's](node-journal-custody-design-2026-09-08.md) explicit trust and
   migration constraints.
2. **Safe process replacement and recovery progress.** Exercise real Worker,
   Runtime and Node exits, new logical sessions, lost durable acknowledgements,
   startup reconciliation and valid replacement without unloading healthy models.
   Retained health metadata and a live-process StreamAgent reconstruction do not
   establish independent backend retirement or health clearance.
3. **Bounded durable history.** The 32-Job run reaches the actual Runtime retained
   history limit. Local journal bytes grew after successive waves to 331,615,
   653,845, 975,985 and 1,298,173. Payload retirement does not reclaim execution
   authorities or transfer history. Design and verify a monotonic reclamation
   protocol that preserves floors, replay, health and retirement proof before
   attempting operation beyond that bound; do not delete records or enlarge the
   bound to claim closure.
4. **Sustained arrivals and comparative architecture evidence.** Once recovery
   and reclamation permit continued operation, run open-loop arrivals with
   backpressure, scheduler contention, disconnects and long-lived members.
   Measure offered/completed work, queue age, timeouts/rejections, resource slopes
   and tail latency. Compare direct persistence and broker persistence under the
   same operation mix, sync policy and load; drained waves are insufficient.
5. **External system boundaries.** Repeat latest-source composition with real
   object storage, deployment mounts/roles, cancellation and deletion/isolation
   races, and database/object recovery. Existing focused evidence is retained;
   this campaign does not combine every fault into the new full Job path.

No GPU, remote rollout or new Production Launch Receipt is part of this result.
The whole-project correctness/scientific-validation objective remains open.
