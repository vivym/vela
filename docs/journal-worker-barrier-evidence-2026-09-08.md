# Actual Worker barrier recovery across the Node journal service

Date: 2026-09-08. Baseline: `444b205`.
This test-only increment extends the
[live Supervisor recovery correction](journal-overload-recovery-evidence-2026-09-08.md)
through the actual `stageworkeragent.Agent` barrier and Runtime gRPC protocol.
PostgreSQL 94, Worker journal 5, Runtime journal 8 and Production Gates **0/9**
are unchanged. The complete correctness/architecture objective remains open.

## Execution and fault boundary

`TestJournalServerWorkerBarrierRecoversAfterOverload` runs an actual Worker Agent
and remote Supervisor in two separate Linux non-root UID/GID 65532 PID-1
processes. Worker uses `modelruntimetransport.Dial` over a workload-owned mode-0600
Unix socket and `NewJournalWorkerClient` with its own Node journal transport.
Node retains the original role-specific pidfds and owns the root-private durable
journal. The existing fixture verifies that workload processes cannot write that
journal directly. A same-UID sibling fills the two authenticated-service handshake
slots with idle connections; it does not receive Worker or Runtime authority.

The parent controls fault timing through test pipes immediately before the
Worker's Prepare or Start RPC. It also controls the fake backend's actual stop
and output-ready observations. It does not substitute RPC replies or journal
operations. CRI/Registry/Fleet enrollment, signing authority, backend behavior
and workload socket assembly remain fixtures; this is not production startup.

## Observed behavior

| Fault placement | Old backend Prepare / Start / Cancel calls | Recovery evidence | Subsequent execution |
| --- | --- | --- | --- |
| Before Prepare | 0 / 0 / 0 | Worker installs signed floor 1; durable exact non-admission checkpoint | Sequence 2 passes Worker barrier, starts once, seals and drains |
| After Prepare, before Start | 1 / 0 / 1 | Worker installs signed floor 1; drain has no checkpoint while CANCELING; explicit STOPPED observation then permits durable exact drain | Sequence 2 passes Worker barrier, starts once, seals and drains |

Both failed barriers return errors with `BarrierPassed=false` and zero started
members. Cancellation has zero acknowledgements without an admitted target, and
one acknowledgement for the prepared execution. The journal bytes are unchanged
across the overloaded barrier and cancellation. The test does not treat either
the failure or cancellation acknowledgement as proof of stopped writers.

The Worker validates checkpoint identity and authority using the existing
transport validators. Runtime/Worker processes, Supervisor, Agent, owner, journal
and epochs remain the same during recovery. The test parent supplies a newly
signed sequence-2 allocation after the old execution is proven safe; this is
explicit test orchestration, not automatic scheduler retry.

Final root-owned status is `Floor=1`, `Highest=2`, `PendingExecutions=0` in both
cases. Node service shutdown joins all exchanges:

| Case | Accepted | Overloaded | Authenticated / replied | Failed idle connections | In flight / peak |
| --- | --- | --- | --- | --- | --- |
| Before Prepare | 44 | 2 | 40 / 40 | 2 | 0 / 2 |
| Before Start | 61 | 3 | 56 / 56 | 2 | 0 / 2 |

These are finite exchange counts, not Jobs, mutation counts or throughput.
The first test run incorrectly required exactly one overloaded exchange; the
real cancellation path also inspects admission state. Its retained failure log
shows correct backend/cancellation counts with two or three refused reads.
The final assertion requires observed overload and exact backend effects without
coupling the scenario to the number of internal reads. No production code was
changed to obtain this result.

## Validation and remaining obligations

Final source passes native Linux/arm64 race checks (51 ModelRuntime behavioral
main tests plus one helper; 13 Node main tests, including both new cases; no
skips/races), repository tests, vet, ordinary and Linux lint, and Linux/amd64
Node package cross compilation. The new native test completes in 1.14 seconds.
Raw logs, build/source/binary hashes and the captured source patch are retained
under `docs/evidence/journal-worker-barrier-2026-09-08/`, with a hashed artifact
inventory in the adjacent JSON receipt.

```sh
bash hack/run-journal-owner-native.sh
go test ./...
go vet ./...
golangci-lint run ./...
GOOS=linux golangci-lint run ./...
GOOS=linux GOARCH=amd64 go test ./internal/nodeagent -c -o /tmp/vela-node-barrier-amd64.test
```

The preceding four CPU Job/cache campaigns remain bound to `444b205`; they were
not rerun for these test-only changes and still use local Runtime persistence.
This increment closes the single-member Worker barrier recovery case, not the
complete protected remote-owner Job/cache path. Full production-loop recovery,
Worker input/materialization journal custody, startup permission and mount
composition, multi-member partial failure, process replacement and descendant
containment, watchdog stop during journal outage, uncertain-write reconciliation,
history reclamation beyond 32 records, and sustained arrival/queue/resource
validation remain open. The historical `11ce026` STALE failure remains unresolved.
