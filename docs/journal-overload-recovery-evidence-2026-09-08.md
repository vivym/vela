# Recover live Runtime admission after an unavailable journal read

Date: 2026-09-08. Baseline: `7950416`.
This extends the [bounded Node service](journal-server-evidence-2026-09-08.md).
PostgreSQL 94, Worker journal 5, Runtime journal 8 and Production Gates **0/9**
are unchanged. The complete correctness/architecture objective remains open.

## Reproduced defect and correction

The original server test filled its handshake slots before constructing the
Supervisor. A new native test keeps the same non-root PID-1 Supervisor alive:
construct and Prepare, fill both server slots with idle peers, attempt Start,
release the idle peers, then attempt Start again. With baseline production code,
the overloaded read permanently failed admission. The second Start returned
`ModelRuntime execution admission requires state recovery`; neither Start
entered the backend. The baseline failing test output and source patch are
retained in this checkpoint's evidence directory.

The Unix journal reader now marks channel failures that returned no usable page
as `ErrJournalReadUnavailable`. The current operation still fails. A subsequent
pure read can authenticate the channel and validate a complete snapshot again,
without permanently failing the otherwise live Supervisor. There is no new
automatic retry loop, mutation retry or fallback to cached authority.

This classification does not assert that an outage is transient or that the
failed exchange authenticated a valid Node. An unverifiable exchange exposes
no data. Every subsequent attempt repeats the existing socket/process/challenge
checks and canonical, digest-bound snapshot validation. Channel authentication
rules and wire formats have not changed.

The error boundary is deliberately stronger for state uncertainty:

| Failure point | Required behavior |
| --- | --- |
| Pure read returns no usable channel result | Fail this operation; allow a later fully verified read |
| Write response or write readback is unavailable | Preserve the consumed sequence and persistent admission fence; no dispatch or automatic mutation retry |
| Authenticated owner reports `UNCERTAIN` | Require recovery |
| Malformed authenticated journal response/page, wrong journal identity, invalid snapshot or regressing state | Require recovery; accept no snapshot |
| Recovery/integrity error joined with cancellation, contention or unavailable markers | Recovery/integrity takes precedence |
| Supervisor lifetime ends | Require recovery; a new request cannot renew it |

## Verification of useful progress and restrictions

`TestJournalServerLiveSupervisorRecoversAfterOverload` now passes using the same
Supervisor object and original process throughout. It proves all of the
following against actual root-owned journal bytes and Linux IPC:

- Prepare completed before overload and retained one pending execution.
- The overloaded Start was rejected, with zero backend Start calls and
  byte-identical journal contents before and after that attempt.
- Once the slots were released, Start succeeded exactly once without replacing
  the Supervisor, owner, journal, authority or Runtime epoch.
- The same execution subsequently sealed and drained, leaving `Highest=1` and
  `PendingExecutions=0`.
- Server shutdown joined all work: 36 accepted connections, one overload,
  33 verified handshakes/replies, two failed idle connections, zero in flight,
  peak two. These are exchange counts, not successful mutations or Jobs.

Focused regressions distinguish a missing pure read from unavailable replies
after a committed write or during its readback. Only the former permits a new
admission; the latter two retain their watermark and backend fence. Other tests
preserve owner/protocol failure classification, including combined errors and
concurrent request cancellation. A Worker floor installed between the failed
read and retry is observed by the retry, which rejects below-floor execution
without another mutation or backend call. This rules out cached-state fallback.

The native tests use fake backends and fixture CRI inventory alongside actual
root/non-root process handles, authenticated Unix exchanges and durable files.
They are not production startup authorization or full process replacement proof.

## Evidence and remaining scope

Final-source validation passed:

| Check | Result |
| --- | --- |
| Focused read/remote race regressions | PASS |
| Complete uncached ModelRuntime race | PASS, 82.350 seconds |
| Native Linux/arm64 race | 51 ModelRuntime behavioral tests plus one helper, 12 Node main tests; zero skips/races |
| Repository tests, vet, ordinary and Linux lint | PASS; lint reports zero issues |
| Linux/amd64 changed-package cross compilation | PASS; compile-only |
| Four opt-in CPU integration/race campaigns | PASS, 68.847 seconds |

The CPU campaigns cover durable-stream exact cache, direct exact cache,
production-loop clock-offset execution and PostgreSQL replacement-Runtime
recovery. The production-loop case completes 16 Jobs, 64 Stage artifacts and
16 Charges (20,000 minor units), with zero payload scratch after the waves.
These campaigns use the existing local-persistence assembly; they provide
compatibility evidence, not a complete remote-owner Job path or sustained TPS.

```sh
go test -race ./internal/modelruntime -count=1
bash hack/run-journal-owner-native.sh
go test ./...
go vet ./...
VELA_RUN_CPU_MOCK_CAMPAIGN=1 go test -race -tags=integration ./internal/integration \
  -run '^(TestCPUMockProductionLoopClockOffsetCampaign|TestCPUMockExactCacheSourceTargetCampaign|TestCPUMockDurableStreamExactCacheCampaign|TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity)$' \
  -count=1 -v -timeout=8m
```

The [machine-readable receipt](journal-overload-recovery-evidence-2026-09-08.json)
binds the final source and [raw artifacts](evidence/journal-overload-recovery-2026-09-08/).
It preserves the failed baseline as well as final validation. The baseline patch
contains test changes only; the final native patch includes the implementation
and strengthened tests. All source/artifact hashes are checked before commit.
Exactly five raw artifacts retain whitespace exempted from the changed-file
check: `campaigns.log`, `native/node-startup.log`, `native/source.patch`,
`baseline/node-startup.log` and `baseline/source.patch`. The receipt hashes all
26 retained artifacts, including those exact raw bytes.

The native scenario explicitly initiates the second Start after releasing
capacity. It does not change the Worker start-barrier retry policy, RPC decision
mapping, backoff or deadlines. In particular, the existing Worker barrier can
cancel members after a failed Prepare/Start response; this test does not claim
that an entire Job transparently survives every overload. That requires the
complete Worker/Control/remote-journal campaign and its failure protocol.

Protected production startup and mounts, separate Worker journal custody,
same-owner outcome reconciliation after uncertain writes, Node/Runtime/Worker
replacement, outage containment, history reclamation and sustained offered load
remain open. Successful finite CPU campaigns cannot advance Production Gates.
