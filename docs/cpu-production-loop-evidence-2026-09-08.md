# Native CPU Jobs through ProductionAgent.Run

Date: 2026-09-08. Base: `423fa63`. PostgreSQL 94, Worker journal 5, Runtime
journal 8. Production Gates remain **0/9**.

The CPU campaign now has a mode that runs the real `ProductionAgent.Run` for
all four resident Workers. Runtime identity discovery, readiness probes,
registration, zero/ready capacity publication, Acquire polling, execution,
heartbeat and materialization recovery use the production implementations and
real mTLS Control stream. The test no longer drives these Workers with its own
Acquire/execute loop, direct capacity reporting or manual StreamAgent rebuild.

This is a test-composition increment. It does not change production permission,
scheduling, session or persistence behavior. The previous
[durable stream campaign](cpu-durable-stream-evidence-2026-09-08.md) remains a
separate mode, including its explicit StreamAgent reconstruction experiment.

## Final-source results

Source digest: `97291ec7df3d483ec9296842dab881638a6c3d199b72bbbcd27d5241af78874b`.
Both sixteen- and thirty-two-Job race campaigns use this same source. Their
full receipts and validation-log hashes are in
[the evidence JSON](cpu-production-loop-evidence-2026-09-08.json).

| Final 32-Job observation | Result |
| --- | --- |
| Arrivals | Four drained waves, eight concurrent HTTP submissions per wave |
| Physical Stage executions | 128; four native resident CPU mock processes |
| Customer Charges / visible completions | 32 / 32; 40,000 minor CNY units posted |
| Successful heartbeat responses | 128, 32 per Worker |
| Successful registration responses | 52, 13 per Worker |
| Successful capacity-report responses | 20, 5 per Worker |
| Automatically confirmed lost commit responses | 4; one REPLAYED result per Worker |
| Final durable Control session epoch | 3 for each Worker |
| Confirmed STALE Acquire responses | 6 on encoder; all recovered by production polling |
| Final input/output payload scratch | 0 bytes |
| Final retained local journal bytes | 1,299,218 |
| Final active allocations / leases | 0 / 0 |
| Final runtime watchdogs / active execution pins | 0 / 0 after maintenance |

Response counters count successful RPC responses, not distinct database rows.
The final local capacity sequence is 4,294,967,300 for each Worker. The test
independently checks the latest accepted PostgreSQL capacity session against
the file state and client session, and requires the accepted sequence not to
exceed the locally reserved sequence. This preserves the distinction between
a reserved sequence and a successfully published observation.

Both admission journals match all 128 PostgreSQL allocations, including original
signed authority identities, Runtime seal/drain and Worker CLOSED/input-drain
checkpoints. No materialization recovery records remain. Native model PIDs remain
unchanged; no manual model restart, StreamAgent reconstruction or transport
redial occurs in this mode. Existing conservation checks still verify one
Charge/completion per Job, exact usage/transfer counts, non-overlapping
allocations and released credit, storage and pool counters.

The 32-Job test passed in 72.25 seconds; package time was 74.401 seconds.
Measured wave elapsed time was 63.52214 seconds. The 16-Job test passed in 36.07
seconds (38.924 package seconds). These are host race regression measurements,
not a throughput comparison: another compatibility test ran concurrently, the
arrival process uses drained waves, and native subprocesses are not instrumented
by the host race detector.

## What the fault proves

For each Worker, a test wrapper lets the actual mTLS client receive an ACCEPTED
materialization response, then withholds it from the StreamAgent caller. The
production loop retains the file recovery record and retries its exact command
ID. The still-open server stream rejects that duplicate ID with `AlreadyExists`,
which closes that stream. The production client reconnects using its durable
session sequencer; ProductionAgent republishes evidence/capacity and replays the
original command. Control returns REPLAYED and normal execution continues.

Thus this experiment observes actual stream replacement and logical-session
advance, unlike the previous same-session/manual-rebuild mode. The injected
fault is application acknowledgement loss, not packet loss before client receipt,
power loss or a Worker process crash. Its extra `AlreadyExists` retry is recorded,
not suppressed. The four final replay counters require the same command ID and
a real REPLAYED response; an unrelated ACCEPTED response cannot satisfy them.

The first run completed all six Jobs but failed the new replay counter because
that counter incorrectly expected ACCEPTED on replay. The counter was corrected
to require REPLAYED; no production behavior was loosened to make it pass.

The review also resolved an earlier uncertainty: a STALE response makes
`ProductionAgent.Discover` return an error, but the enclosing `Run` already retries
with backoff. The final concurrent run demonstrates progress through six such
responses without the harness's separate stale-poll retry loop. This finite
sample does not establish starvation freedom under arbitrary contention.

## Reproduction and compatibility

Prerequisites remain Docker, Go 1.26.7, ffprobe 8.0.1, ps and lsof.

```sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
VELA_CPU_MOCK_WAVES=4 VELA_CPU_MOCK_WIDTH=8 VELA_CPU_MOCK_TIMEOUT=5m \
go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockProductionLoopJobCampaign$' -count=1 -timeout=8m -v
```

The same-source compatibility race batch passed in 49.604 package seconds:
original direct-service Job/exact-cache campaigns and durable-stream
Job/exact-cache campaigns. Focused Production/DurableStream/Reconnect race tests
passed for stageworkeragent (11.219 seconds) and stageworkertransport (3.083
seconds). `go vet -tags=integration ./internal/integration` passed. Integration-tag
golangci-lint remains at 82 existing findings, with none in the four changed
campaign files. No full integration-suite or new deployment validation is claimed
for this increment; production source and schemas are unchanged.

## Boundaries and next architectural work

Fleet membership and residency are fixture-created. The fixture approves the
aggregate of actual static native mock readiness responses before production
registration; this is not independent real-model canary certification. Runtime
gRPC is loopback, the artifact store is local exact-version storage, and Worker
and Runtime journals are explicitly initialized without signed Registry journal
pair bindings. This mode does not assemble the production executable's protected
Node mounts/startup protocol, independent process custody or its complete
terminal-history recovery configuration. Exact-cache production-loop scheduling
is also separate; the compatibility batch exercises cache through the existing
direct-service and durable-stream modes.

The 32 retained records per Runtime remain a real limit. A quick deletion by
sequence would remove evidence still queried by terminal recovery: exact drain,
accepted/confirmed renewals, sealed receipt replay and per-allocation health.
An installed floor only prohibits admission; it does not prove those recovery
obligations are discharged. Therefore this increment neither deletes records nor
raises the retention limit, and does not claim progress beyond 32 Jobs per member.

The reclamation protocol must establish, before dropping exact history:

1. Durable completion of publication/terminal outcome and exact namespace
   retirement, including unresolved Worker input writers and all Runtime members.
2. Monotonic evidence that prevents old admission, reattachment and cleanup
   permission from being recreated after the exact records are gone.
3. Preservation of every explicit health denial and unknown legacy-health state;
   a retirement or compaction acknowledgement must not become health clearance.
4. A specified replay result for retired history and crash-safe ordering across
   Worker, Runtime and trusted Node; missing local history is never fresh state.
5. Verified progress beyond the current bound, bounded retained bytes and
   sustained arrivals with failures, once those guarantees are implemented.

The [Node custody candidate](node-journal-custody-design-2026-09-08.md) remains
the next architecture integration target. Full process replacement, startup
outcome recovery, safe history reclamation and open-loop operation are still
required before calling the original correctness/scientific-validation goal
complete. No GPU or remote system was used or modified by this increment.
