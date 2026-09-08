# Request cancellation at the remote journal boundary

Date: 2026-09-08. Implementation baseline: `8104a7d`.
This extends the [remote integration checkpoint](journal-remote-integration-evidence-2026-09-08.md).
PostgreSQL 94, Worker journal 5, Runtime journal 8 and Production Gates **0/9**
are unchanged. The full correctness objective remains in progress.

## Problem and behavior

Remote journal reads and writes previously derived their context only from the
Supervisor lifetime and configured timeout (up to 45 seconds). Canceling a
Prepare/Start/Status/Seal or inspection RPC did not interrupt its current journal
exchange. A mutation's verification readback also started a new timeout budget.

The private journal boundary now carries the calling context through readiness,
admission, identity/history inspection, candidate/health/seal/drain persistence,
and Worker restriction confirmation. A remote exchange observes the earliest of
request cancellation, Supervisor lifetime cancellation and configured timeout.
Readback inherits the mutation's remaining deadline; read contention still has
at most three attempts and mutations are never automatically retried.

The error boundary remains conservative:

- A pure read interrupted by its request or timeout exposes no snapshot and
  does not permanently fail admission. A subsequent request must read and
  validate the owner again before dispatch.
- Once a mutation is attempted, cancellation during its reply or readback can
  hide an already committed result. Admission remains fenced, the retained
  sequence stays consumed, and retry cannot enter the backend.
- Supervisor lifetime loss, journal corruption, identity mismatch and state
  regression still require recovery. Request cancellation does not renew the
  lifetime or turn an invalid snapshot into usable state.

The watchdog now establishes its cancellation timeout before admission checking.
If that budget expires while checking the journal, it cannot proceed with an
expired context; this does not prove that a backend has stopped. Guaranteed
stop/containment during journal outage remains a separate obligation.

## Deterministic regressions

`TestJournalRemoteCancellationBoundsAdmission` blocks at a signaled phase with
a 30-second journal budget, then cancels explicitly. Its five-second guard
detects a stuck request; it does not drive cancellation. Cases cover pure read,
committed write reply, committed write readback and Supervisor lifetime loss.
The tests assert zero backend calls before cancellation returns, inspect actual
owner watermarks, and verify successful read-only retry versus persistent
fencing after a possible write or lifetime loss.

`TestJournalRemoteWriteReadbackSharesDeadline` compares actual context deadlines
at the write and readback boundary. `TestJournalRemoteReadTimeoutCanRetry`
intentionally exhausts a read timeout and verifies subsequent legal admission.

## Evidence scope and remaining work

Final-source checks passed:

| Check | Result |
| --- | --- |
| Full uncached ModelRuntime race | PASS, 80.443 seconds |
| Ordinary repository tests and vet | PASS |
| Ordinary lint and Linux changed-package lint | PASS, zero issues |
| Linux/amd64 changed-package cross compilation | PASS; not execution |
| Native Linux/arm64 race | 47 ModelRuntime behavioral tests, one helper entrypoint, seven Node tests; no skips or races |
| Four opt-in CPU integration/race campaigns | PASS, 61.260 seconds |

The campaigns cover durable-stream exact cache, direct exact cache, the
production-loop clock-offset Job path and PostgreSQL replacement-Runtime
recovery. The production-loop case completes 16 Jobs, 64 Stage artifacts and
16 Charges (20,000 minor units); these are finite-wave CPU compatibility checks.
The previously recorded 30 ms inspection-test failure did not recur in this
full race run. This does not establish that its timing sensitivity is fixed.

The [machine-readable receipt](journal-context-evidence-2026-09-08.json) records
the source digest, commands' outcomes and all 18
[raw artifacts](evidence/journal-context-2026-09-08/).
The native runner's source patch reverses cleanly against the validated source
and is based on `8104a7d`. Preserve raw whitespace in exactly
`campaigns.log`, `native/node-startup.log` and `native/source.patch`; the receipt
hashes their exact bytes. Other changed files pass whitespace checks.

```sh
go test -race ./internal/modelruntime -count=1
go test ./...
go vet ./...
bash hack/run-journal-owner-native.sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 go test -race -tags=integration ./internal/integration \
  -run '^(TestCPUMockProductionLoopClockOffsetCampaign|TestCPUMockExactCacheSourceTargetCampaign|TestCPUMockDurableStreamExactCacheCampaign|TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity)$' \
  -count=1 -v -timeout=8m
```

These tests use actual Supervisor and journal-owner state machines with a
controlled in-process transport. The native runner additionally exercises
authenticated root/non-root Linux IPC and the existing actual Supervisor
Prepare/Start/Seal/Drain assembly. New cancellation fault injection is at the
transport interface, not a new full remote CPU Job campaign.

Context propagation does not make mutex acquisition interruptible, preempt a
local filesystem fsync, stop a noncooperative transport, or solve the watchdog's
wait behind another operation. Local journal transitions retain synchronous
filesystem behavior and check context before entry. The remote implementation
still reads a full verified snapshot per check; no throughput improvement is
claimed. Protected production startup/mount assembly, separate Worker journal
custody, positive replacement/recovery, history reclamation and sustained
offered-load validation remain open.
