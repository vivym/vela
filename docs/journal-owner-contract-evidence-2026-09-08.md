# Typed journal owner contract for startup and non-admission

Date: 2026-09-08. Baseline: `5e86a04`. PostgreSQL 94, Worker journal 5,
Runtime journal 8. Production Gates remain **0/9**.

## Result and implementation boundary

Backend startup intent, execution non-admission and terminal non-admission now
use typed owner transitions. Together with the preceding six transitions, all
ordinary Runtime journal mutations pass through a private candidate and complete
publication validation. Direct `persist` callers outside that boundary are only
explicit initialization/upgrade and test-only corruption probes.

The existing Supervisor still serializes admission, active calls and checkpoint
construction with its admission lock. Its current approved Service supplies a
copied route binding and clock-skew policy. These are trusted owner configuration,
not fields to accept from a future client request. The draft contains no backend
or filesystem handles and independently verifies signed input, scope, route,
floor, retained execution intent, cross-format identity and history bounds.
Caller-supplied `Verified.Digest` is recomputed from signed bytes.

Startup intent additionally binds the launch manifest's member-wide scope to the
held journal. The owner selects its incarnation and timestamp; the transition
cannot replace UNRESOLVED or LEGACY_UNKNOWN with a fresh incarnation, clear a
health denial, grant execution permission or attest process exit. It records the
launch digest but does not independently approve the effective launch configuration.

Exact non-admission retries return the retained proof without changing its
timestamp, identity, bytes or fsync count. Backend startup is deliberately not a
new remotely retryable grant API: repeated startup cannot replace unresolved
intent. A future Node issuer must separately implement its exact-owner outcome
reconciliation.

## Reproduced expiry defect

The new owner check also closes a concrete pre-publication gap. The baseline
checked a terminal disposition at ingress, then constructed and published the
checkpoint without validating freshness at the durable owner's transition.

The deterministic regression gives ingress a valid clock reading and advances
the independent Service clock by one hour immediately afterwards. On baseline
`5e86a04`, it returns and persists a non-admission checkpoint observed at
**09:00 UTC**, although the signed disposition expired at **08:01 UTC**.
The isolated baseline test fails in 1.333 package seconds; its original log is
preserved. This is a controlled clock-boundary reproduction, not an observed
production incident or an explanation of the earlier `11ce026` campaign failure.

The final source rejects at the owner boundary with `ErrStale`, leaves disk bytes
unchanged and accepts a freshly signed disposition afterwards. Domain rejection
is distinguishable from storage/check/publication failure: rejected drafts have
not published state, while actual persistence failures still poison admission
and require recovery. Existing sentinel errors remain discoverable by `errors.Is`.

The two authority forms retain different contracts. An expired execution
envelope may authenticate historical identity for an otherwise proven
non-admission checkpoint. A new terminal non-admission checkpoint requires a
currently valid terminal disposition. Historical inspection grants no new
execution time. The new check defines the owner mutation boundary; it does not
claim that a clock cannot advance during a later fsync or response delivery.

## Verification

New owner-boundary regressions bypass Service ingress intentionally. Eighteen
negative cases cover signature/scope, wrong route, old epoch, missing floor,
persisted execution intent, invalid timestamp, cross-format allocation collision
and abort after candidate construction. Rejection checks disk bytes, inode,
in-memory document and zero syncs, then demonstrates a legal checkpoint on the
other current route. Additional cases check canonical digest reconstruction,
idempotent replay, startup scope/incarnation/time validation, retained unresolved
startup and final-owner expiry.

| Check | Result |
| --- | --- |
| Initial existing non-admission/startup compatibility selection | PASS, 6.040 s |
| Final owner-boundary selection, host race | PASS, 3.627 s |
| Full uncached ModelRuntime race after production changes | PASS, 108.167 s |
| Ordinary repository tests, final vet and final ordinary lint | PASS; lint 0 issues |
| Static Linux/amd64 cross compilation | PASS; not execution of Linux tests |
| Final native Linux/arm64 race | 30 behavioral main tests plus one helper entrypoint; no skips/race reports |
| Existing root Node/non-root PID-1 Runtime startup exchange | PASS: mock permit, denial, lost response, uncertain record |
| PostgreSQL terminal-history replacement-Runtime recovery, host race | PASS, 7.763 s |
| 32-Job / 128-Stage production loop with -1 s consumer verification clock, host race | PASS, 78.353 s |
| Direct and durable-stream exact-cache source/target compatibility, host race | PASS, 21.299 s combined |

The production implementation was unchanged after the full host suite. The
expiry regression was then strengthened to advance the independent Service clock
instead of only varying successive validator reads, and runner provenance was
improved. Final host focused tests, native tests, integration campaigns, vet and
lint cover that final source. Unchanged ordinary packages may use Go's test cache.
Ordinary lint does not supersede the existing integration-tag lint backlog.

The production-loop receipt and both exact-cache receipts share source digest
`9a95c7f754b5f76f73d7765f096109329f1b7200d3cd4e0cf10e8bd5f6a8c37b`.
The loop completed 32 Jobs with 32 Charges and four exact lost-response replays.
Each Worker received 32 future-issued assignments and retained 32 Runtime records.
Final allocations, leases, payload scratch, watchdogs and execution/finalization/
cache pins were zero; journal metadata remained **1,299,133 bytes**.
Other validation shared host resources. These timings are not a performance
comparison, sustained arrival measurement or proof of bounded retained history.

The PostgreSQL/mock commands use Docker, Go 1.26.7 and ffprobe 8.0.1:

```sh
go test -race ./internal/modelruntime -count=1
go test -race -tags=integration ./internal/integration -run '^TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity$' -count=1 -v -timeout=5m
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=4 VELA_CPU_MOCK_WIDTH=8 VELA_CPU_MOCK_TIMEOUT=5m go test -race -tags=integration ./internal/integration -run '^TestCPUMockProductionLoopClockOffsetCampaign$' -count=1 -v -timeout=10m
VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_TIMEOUT=5m go test -race -tags=integration ./internal/integration -run '^TestCPUMock(DurableStreamExactCache|ExactCacheSourceTarget)Campaign$' -count=1 -v -timeout=10m
```

## Native reproduction and provenance

Run `bash hack/run-journal-owner-native.sh`. Optional absolute paths are
`VELA_OWNER_EVIDENCE` and `VELA_OWNER_BUILD_CACHE`. The runner pins the Go 1.26.7
builder, captures tracked and untracked ModelRuntime changes, builds native static
race binaries and records their hashes and the resulting image identity.
The first run's source patch omitted untracked files; the final runner and rerun
capture both new Go files. Final native logs and complete source patch are retained.

Tests run without network or host mounts. ModelRuntime checks use UID/GID 10001
with all capabilities dropped. The separate Node test needs SYS_ADMIN/SYS_PTRACE
and an unconfined seccomp profile to create and inspect its PID namespace; it is
an existing mock startup-decision exchange, not a new production permission issuer.
Both containers have 4 CPU / 4 GiB / 256 PID limits on the local Docker VM.

The final native image is
`sha256:175eaa6f16ca0e750454739d5e4a9b5f5e858d78ecfcc167cf3ad69802c641e6`.
The [evidence JSON](journal-owner-contract-evidence-2026-09-08.json) records raw
log hashes, full campaign receipts, binary hashes and the intentionally failing
baseline result. Test binaries remain local artifacts, not release executables.

Source/report whitespace checks pass. Only five byte-preserved raw artifacts
under `docs/evidence/journal-owner-contract-2026-09-08/` are excluded from that
check: `native/source.patch` (diff context markers), `native/node-startup.log`
(indented subprocess output), and `owner-contract-cache.log`,
`owner-contract-production-loop.log`, `owner-contract-terminal-recovery.log`
(Testcontainers' trailing space). All match their original bytes and hashes;
the complete native source patch also passes `git apply --check` on the baseline.

## Remaining work

This increment completes the Runtime's internal mutation discipline. It does
not implement the [Node-private custody candidate](node-journal-custody-design-2026-09-08.md)
or turn private Go APIs into a security boundary. Authenticated original caller
selection, current Registry/Fleet activation, root-private mounts, Worker journal
custody, IPC failure/replay behavior and startup outcome reconciliation remain.
Full process replacement, independent exit/writer proof, safe history reclamation
beyond 32 records and sustained offered arrivals are still required. The existing
replacement-Runtime fixture does not establish that complete production protocol.
No GPU, remote rollout or full-project closure is claimed.
