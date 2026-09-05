# Local CPU Mock Load Campaign

This opt-in integration campaign runs the real Admission, scheduler, authority,
native subprocess Runtime, exact transfer, materialization, media inspection and
Visible Completion paths on one local host. Its evidence class is
`LOCAL_CPU_MOCK_RUNTIME_LOAD`; it does not advance any Production Gate.

## Requirements

- The repository's Go toolchain and a local Docker daemon able to run PostgreSQL 17.
- `ffprobe 8.0.1`, `ps` and `lsof` on the host.
- No concurrent edits to `.go` or `.sql` under `cmd`, `internal`, `proto/gen`, or
  `db`, to `go.mod` and `go.sum`, or to the embedded media fixtures. The campaign
  rejects a changed source digest.

The ordinary integration shards skip the campaign unless explicitly enabled.
The default workload is two waves of eight concurrent Admission requests.

```sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockConcurrentAdmissionRuntimeCampaign$' \
  -count=1 -timeout=3m -json
```

The extended workload uses 64 waves, 512 Jobs and the same four persistent
workers. Each Job executes Encoder, DiT, VAE and Thumbnail. Every wave drains
before the next wave starts; this is bounded extended CPU load, not an open-loop
arrival-rate experiment or a production soak.

```sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
VELA_CPU_MOCK_WAVES=64 \
VELA_CPU_MOCK_WIDTH=8 \
VELA_CPU_MOCK_TIMEOUT=10m \
go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockConcurrentAdmissionRuntimeCampaign$' \
  -count=1 -timeout=12m -json
```

| Setting | Default | Accepted Range |
| --- | --- | --- |
| `VELA_CPU_MOCK_WAVES` | `2` | `2..256` |
| `VELA_CPU_MOCK_WIDTH` | `8` | `3..10` |
| `VELA_CPU_MOCK_TIMEOUT` | `90s` | `10s..30m` |

The project running limit remains two. The test timeout must also cover builds,
database setup, observation and cleanup. More waves do not change the modeled
hardware, service limits or synthetic media payloads.
Before arrivals, the fixture's initial credit limit is set to planned Jobs times
the actual catalog fixed price. The receipt records that budget, and final posted
Charges must equal it. The workload uses normal Admission reservations and billing;
the campaign does not replenish credit while it runs.
Each Worker calls the real `ReportCapacityObservation` backend before its first
Acquire and every 30 seconds while polling. Each report retains the same Stage
worker observation sequence and renews its two-minute validity. The receipt
records successful report counts per Worker.

The initial HTTP credential is valid for one day, and the fixture cache policy
has a one-day TTL. Profile certification uses durable state rather than a short
lease. Each Job receives its own deadline with a 7,200-second queue allowance;
StageArtifact retention follows that Job deadline. Stage leases, local deadlines,
transfer/materialization and finalization authorities are issued for each new
operation. The campaign preserves their normal checks; the periodically renewed
capacity observation is the only two-minute authority shared across many waves.

The independent exact-cache campaign uses a fresh database and the same four
native Runtime processes. It physically executes all four source Stages, enables
Project cache and admits the actual Encoder/DiT artifacts through
`H3ExactReconciler`. A distinct Job with identical frozen inputs then hits those
two entries and physically executes VAE/Thumbnail. It checks exact bindings and
pins, six total physical Stages, five consumed transfers, two independent Charges
and four readable Job-owned public copies. Its result and hit states are not
preseeded. The policy/equivalence declarations remain static fixture catalog data.

```sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockExactCacheSourceTargetCampaign$' \
  -count=1 -timeout=3m -json
```

This cache campaign drives reconciliation before target Acquire. It verifies the
native source-to-target contract, not concurrent cache/scheduler arbitration, and
does not change the pure-physical 512-Job workload.

## Assertions and Receipt

`CPU_MOCK_CAMPAIGN_START` records the source, binary hashes and initial budget
before execution. `CPU_MOCK_LOAD_RECEIPT` is emitted only after the assertions pass. `go test -json`
can split this line into multiple Output events; concatenate Output fields in
order before extracting its JSON object.

- All submitted Jobs, graph Attempts and Stages succeed, with one Charge and one
  Visible Completion per Job. Replaying finalization preserves both identities.
- Each allocation has one direct `ALLOCATION_NANOSECOND` receipt. Every graph
  edge has a consumed transfer, and allocated intervals on one worker do not overlap.
- Running/queued capacity limits hold at every observation. Final project/pool
  counters, active allocations/leases/buffers, active storage and reserved credit
  return to zero. Posted credit equals the sum of Charges.
- Every wave drains local input/output scratch using
  `stageworkeragent.FilesystemScratchRetirer.RetireCommitted`, after the actual
  materialization result and successful close of the source file. Cleanup has
  the same exact-commit validation as the production stream path.
- The four native worker PIDs remain unchanged. Idle watchdogs return to zero,
  and the final one-second idle period persists no additional Acquire intents.
- Each raw `after_wave` precedes one real `retention.ReconcileBatch` call.
  Separate `after_maintenance` observations verify terminal execution/finalization
  pins and edge buffers return to zero, while unexpired committed artifacts remain. The cache
  campaign retains both LIVE entries and their two cache carrying references
  after maintenance. Cache hits create EXECUTION pins; the observation separately
  reports `pin_kind='CACHE'` without assuming that ADMIT creates such pins.
- Retryable Acquire failures preserve the original command identity and are
  separately counted for SQLSTATE `40001` and `40P01`; at most 32 retries are allowed.

The receipt records every wave's timestamp, test heap/goroutines, worker and test
process RSS/open FDs, scratch input/output bytes, database bytes, active
execution/finalization/cache pins, LIVE cache entries, cache carrying references,
committed StageArtifacts, resource
counters, credit and Charges. It also records the Go/ffprobe versions, actual Runtime
binary hashes and a source-tree digest. That digest is SHA-256 over a compact
JSON object mapping sorted repository-relative Go/SQL paths, `go.mod`, `go.sum`,
the two embedded `internal/h3mockbackend/testdata` media files and the embedded
`internal/artifactvalidator/testdata/h264_16x16_1fps.mp4.b64` sample to their file
SHA-256 values.

## Measurement Boundaries

The workers execute CPU-hosted logical GPU profiles and real mock media. There
is no real H3 model or GPU execution. The test process hosts the four ModelRuntime
gRPC services on loopback; each service drives a resident native mock backend.
This does not launch four separate `cmd/vela-model-runtime` daemons. Control
services are invoked through their public Go interfaces. Exact versions live
in the local in-memory object store rather than remote S3. Host `ffprobe`
inspection does not validate a Linux sandbox.
The production `StageWorkerControl` stream handler and `StreamAgent` durable
journal are not exercised by this campaign. Their authority and replay behavior
requires the separate real-handler and journal regressions; the CPU campaign
alone is not a full production-stream end-to-end result.

Heap/goroutine metrics belong to the test process. `ps` RSS and `lsof` numeric
FDs cover that process and the four native workers; PostgreSQL container and
Docker VM RSS/FD are not sampled. Database history and exact objects are retained
for the campaign, so their growth is not evidence of leaked live authority.
Crash/restart scratch journal recovery is tested separately.
`go test -race` instruments the test process; the native mock subprocesses are
built without race instrumentation.

`sampled_peak` contains independent observed maxima, not one simultaneous state
or a continuous upper bound. Polling includes SQL, stack and filesystem reads;
per-wave process sampling also adds elapsed time. Throughput and latency describe
this instrumented local mock workload only.

The [2026-09-05 diagnostic projection](../cpu-mock-load-diagnostic-2026-09-05.json)
retains an earlier extended run that stopped after 14 completed waves when
StageArtifact commit and scheduler claim commit deadlocked. It includes the
companion short/cache receipts and original log hashes. It is failed-run history,
not a completed 512-Job acceptance receipt.

## Schema-88 Revalidation

The [schema-88 evidence](../cpu-mock-schema88-evidence-2026-09-05.json) is a new
source-bound checkpoint after the ordered physical-execution repair. It retains
the complete 628-file source map, both native runtime receipts and log hashes.
Source digest is
`54a961e7f3d5c8a8b5479ce9fafce013912f183ee7b4231d3f35061399f711a7`.

| Campaign | Measured Work | Go Test Package Result |
| --- | --- | --- |
| Extended load | 512 Jobs, 2,048 physical Stages, 1,536 transfers, zero Acquire retries | PASS, 121.436 s |
| Independent exact cache | Source 4 physical Stages; target 2; 5 transfers; 2 Charges | PASS, 9.776 s |

The measured workload interval is 110.824 s, or 4.620 Jobs/s, including the same
observations and maintenance. This is not a controlled performance comparison
with schema 87. All 64 waves finish with zero scratch and Runtime watchdogs;
maintenance releases their 24 terminal execution pins. Final active execution,
storage/credit reservations and pool counters are zero, while 2,048 committed
StageArtifacts remain. Posted Charges equal 640,000 CNY minor units. Exact-cache
entries and their two carrying references remain live after maintenance.

The sequence fence prevents a retired allocation from reentering Runtime RPC
execution. These load results do not establish terminal Worker namespace gating,
input/download writer drain, backend descendant quiescence or cleanup journal
recovery. Those remain in the [terminal scratch design](../terminal-scratch-retirement-design-2026-09-05.md).

## Schema-87 Recorded Validation

The [final 2026-09-05 evidence](../cpu-mock-load-evidence-2026-09-05.json) contains
all three schema-87 receipts, the complete 622-file source hash map, native binary
hashes and diagnostic log hashes. All three runs use source digest
`8e1a59e2226b2cbb620336aa999702a477d2adb26d4f04a65bf1e640cb864d70`.
The scheduler pool/counter deadlock was repaired before these runs.

| Campaign | Measured Work | Go Test Package Result |
| --- | --- | --- |
| Short load | 16 Jobs, 64 physical Stages, 48 transfers | PASS, 14.540 s |
| Independent exact cache | Source 4 physical Stages; target 2; 5 transfers; 2 Charges | PASS, 8.925 s |
| Extended load | 512 Jobs, 2,048 physical Stages, 1,536 transfers | PASS, 118.438 s |

The extended workload interval was 108.994 seconds, including observations and
per-wave maintenance, or 4.698 Jobs/s. Mean queue time was 0.659 seconds and mean
Job latency was 0.957 seconds. These are instrumented local CPU mock measurements.
Each of the 64 completed waves had zero scratch bytes and watchdog goroutines,
93 test-process goroutines and 11 numeric file descriptors per native backend.
Every raw wave had 24 terminal execution pins; each real maintenance batch
released them to zero and preserved the unexpired artifacts. Final authority,
pool and credit reservations were zero, with 512 Charges totaling 640,000 CNY
minor units. Every worker successfully reported capacity four times. This run
observed zero Acquire transaction retries; earlier failures remain in the evidence.

The exact-cache run retained its two LIVE entries and two carrying references.
Maintenance released eight terminal execution pins and one edge buffer. Both
Jobs' VIDEO and THUMBNAIL public copies remained independently readable, with
matching source/target content hashes and unchanged one-Charge-per-Job behavior.

The extended run retained 2,048 committed StageArtifacts. Database size ended at
94,216,719 bytes; test-process RSS ended at 314,048,512 bytes and heap after GC at
37,315,248 bytes. Native backend RSS ended between 21,856,256 and 22,740,992 bytes.
Retained objects, database history and instrumentation grow with the campaign;
this is not evidence of constant total memory. The successful load does not
establish failed/canceled/expired execution scratch retirement. That lifecycle
and production stream/journal recovery have their own tests and evidence.
