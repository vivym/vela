# Preserve installed-execution stop during an unresponsive journal read

Date: 2026-09-08. Baseline: `89fd3d1`.
This extends the [actual Worker barrier checkpoint](journal-worker-barrier-evidence-2026-09-08.md).
PostgreSQL 94, Worker journal 5, Runtime journal 8 and Production Gates **0/9**
are unchanged. The complete correctness/architecture objective remains open.

## Reproduced defect and correction

Cancellation admission previously checked durable state before resolving the
installed target. Immediate journal refusal still allowed a known cancellation,
but a journal read that consumed the entire cancellation deadline returned
`context deadline exceeded` before dispatch. The one-shot watchdog also returned
without calling backend Cancel. The baseline regression reproduces both cases:
explicit Cancel returns STALE without acknowledgement; watchdog backend calls
remain at two (Prepare and Start), with one blocked journal read and no stop.

Admission now verifies the signed cancellation request and checks whether it
names an already installed execution before attempting journal I/O. Such a
request may stop its known target with `allowSuccessor=false`. The serialized
operation resolves the target again before backend dispatch, including existing
inspection of ambiguous current/previous backend envelopes. This branch does
not install a new envelope, reset a watchdog, mutate a journal, prove drain,
assert readiness or restore capacity. Other commands retain durable-state
validation; an unseen successor still requires that validation.

This change removes the stop operation's own dependency on an unavailable
journal read. It does not remove mutex waits or make fsync interruptible. An
already executing operation can hold admission or backend serialization locks;
budget consumption while waiting for those locks requires separate validation.
Backend refusal, uncooperative calls, descendant containment and process death
also remain distinct from a successfully delivered cancellation signal.

## Native evidence

`TestJournalServerCancellationSurvivesUnresponsiveNode` starts the actual Worker
Agent and Supervisor in separate Linux non-root PID-1 processes. The Worker
executes Prepare/Start through real Runtime gRPC, while Node owns the protected
journal. The test then stops the journal server and creates a root-owned
mode-0660 socket at the same fixture path, with no accept/response loop.

An actual Worker Drain RPC reaches that unresponsive socket and times out
without a checkpoint. With the outage still in force, the test independently
exercises explicit Worker Cancel and the Runtime watchdog. The latter uses a
controlled clock to fire the installed timer rather than sleeping through the
lease. In each case, exact backend counts are Prepare=1, Start=1, Cancel=1,
with the expected CONTROL_PLANE_STOP or MONOTONIC_DEADLINE reason.

After a controlled fake-backend STOPPED observation, a second Drain RPC still
fails during the outage. Root-owned journal bytes remain unchanged, with
`Highest=1`, `PendingExecutions=1`. Restoring the accept loop against the same
owner allows an exact validated checkpoint and clears the pending execution.
Worker/Runtime, Supervisor, journal identity and epoch remain unchanged.

The original server joins 18 authenticated/replied exchanges with no failures.
The restored server joins eight accepted exchanges: six replies and two failed
connections left by timed-out reads; in-flight count returns to zero. The test
waits for those disconnected peers to drain before requesting new work.
Both native scenarios pass; these are fake-backend stop signals and fixture
startup identities, not real compute-process termination or Launch Receipts.

## Validation

The adjacent JSON receipt pins source, runner and raw artifact hashes. Evidence
is retained under `docs/evidence/journal-stop-outage-2026-09-08/`, including the
baseline failure and its test-only patch. Final-source checks include:

- Focused cancellation, authority, watchdog, floor and remote-journal race tests.
- Complete uncached ModelRuntime race tests.
- Native Linux/arm64 race: 52 ModelRuntime behavioral main tests plus one helper,
  and 14 Node main tests, with no skips or races.
- Repository tests, vet, ordinary/Linux lint and Linux/amd64 Node cross compile.

The preceding CPU Job/cache campaigns remain bound to `444b205`; this checkpoint
does not reattribute those measurements to the new source. Full protected
remote-owner Job/cache execution, production startup and mount composition,
separate Worker input/materialization journal custody, replacement and descendant
containment, uncertain-write reconciliation, concurrent outage/lock behavior,
history reclamation and sustained arrival/resource validation remain open.
