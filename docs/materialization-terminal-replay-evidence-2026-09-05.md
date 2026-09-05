# Stage terminal command replay evidence

Date: 2026-09-05. Scope: local PostgreSQL integration, production control handler
and backend, and focused Worker StreamAgent tests. These receipts do not advance
Production Gates or establish a bound for failed/canceled/expired input scratch.

## Defects and implemented behavior

COMMIT changed its materialization lease to COMMITTED and SOURCE_LOST changed it
to REVOKED. The active-authority reader then rejected even an immediate exact
repeat; after TTL, signature validation rejected the request before reading its
durable receipt. SOURCE_LOST also returned RETRY_WAIT or FAILED from SQL while
the backend accepted only READY, losing the acknowledgment after SQL committed.

Migration 85 permits the existing authority reader to recognize a terminal lease
with its matching durable COMMIT or LOCAL_SOURCE_LOST receipt. The normal token,
SPIFFE identity, Worker/member epochs, current session, and readiness checks are
retained. The mutation functions already replay only an exact recorded command
digest, so a terminal lease cannot grant a new command or altered payload. An
expiry-only signature validation path still rejects future-issued or incorrectly
signed authority. SOURCE_LOST now checks the actual materialization and Job
deadlines after acquiring its locks. Its backend accepts RETRY_WAIT and FAILED.

FailStage had the same backend READY mismatch and also failed replay after its
StageRun fence/version advanced and execution lease was revoked. Migration 86
adds a read-only terminal FAIL receipt probe bound to the original lease, token,
allocation, physical attempt, fences, version, current Worker epoch/session, and
recorded renewal authority digest when a renewal exists. Remaining signed
identity, member, device, runtime, and deadline-envelope fields continue through
the existing authority snapshot matcher. Only the completed run's mutable state
checks are bypassed, and only for FAIL with durable evidence.

The StageAuthority replay validator grants zero remaining execution time. For
FAIL, the authorizer refuses that expiry-only result unless the terminal receipt
probe succeeds. A newly authorized FAIL also rechecks effective wall/local and
Job deadlines after lock acquisition. The complete deterministic FailStageRequest
protobuf SHA256 enters the existing coordinator command JSON digest, binding
diagnostic detail and WorkerReusable as well as the execution fields.

StreamAgent.Fail now derives its command UUID from the StageAttempt and FAIL
operation. Repeated reports use the same ID; changing the payload cannot select a
new command identity. The focused caller test repeats the same failure after
simulated response loss and confirms that changed diagnostic evidence keeps the
same ID. FileProductionState still does not persist a failure request or active
assignment across process restart. That path uses the existing lease-expiry
recovery model; a new failure journal was not introduced here.

## Measured verification

`/tmp/vela-stage-failure-materialization-final.log`: PASS, package 46.971s.

- FAIL RETRY_WAIT and FAILED: first acknowledgment, immediate and natural-TTL
  exact replay, unchanged authority digest, real session reconnect, old-session
  rejection, and Worker epoch fence rejection.
- FAIL altered command ID, failure class, whitespace, fingerprint, units, event
  times, detail, WorkerReusable, or signature: rejected. A signed future authority
  and an expired active authority without a terminal receipt are rejected.
- A terminal physical attempt and released/revoked allocation/lease without a
  durable FAIL receipt cannot authorize replay.
- Natural FAIL lock wait: schema 85 returned ACCEPTED after authority expiry;
  schema 86 returned STALE and preserved the full authority, receipt, retry,
  storage, credit-reservation, and Charge snapshot.
- COMMIT, retrying SOURCE_LOST, and terminal SOURCE_LOST: first/immediate/TTL
  acknowledgment, changed operation or payload rejection, real reconnect, and
  epoch fence rejection. The same snapshot remains unchanged during replay and
  rejected requests.
- Migration 85 and 86 Up/Down restore the preexisting function OIDs, owners, ACLs,
  and bodies exactly. The new 86 probe grants only StageWorkerControl execution,
  with no direct write permission on command receipts.

`/tmp/vela-stage-materialization-terminal-replay-final.log`: PASS, 42.911s. This
earlier schema-85-focused run additionally covers SOURCE_LOST waiting on a real
PostgreSQL lock until TTL, missing COMMIT/SOURCE_LOST receipts, the existing
materialization expiry reconciler and COMMIT lock-wait guard, source-loss budget
terminalization, and parent-before-materialization lock order.

Focused Go packages passed: stageauthority, materializationauthority,
stageworkercontrol, database, and stageworkeragent. The coordinator package
compiled and has no standalone unit tests. The final stageworkeragent run was
1.402s. `git diff --check` passed after the source freeze.

## Compatibility and remaining boundaries

New StageWorkerControl startup requires the schema-86 probe through the existing
VerifyRole contract. A running handler encountering a database rolled back to 85
detects the missing capability and returns STALE for terminal FAIL replay; it does
not issue an undefined-function query. Applying 86 restores replay. Normal role
verification intentionally rejects starting this new control binary on schema 85.

Existing direct coordinator commands omit the optional Worker request digest and
retain their original serialization. New wire FAIL requests include it. Historical
wire FAIL receipts created without that digest are not claimed as compatible
exact replays under the new format. Similarly, the stable Worker command IDs do
not reconstruct an unknown random ID used by an older process.

Replay remains contingent on the original Worker/member/runtime epochs and the
currently connected session matching durable identity. It does not allow a new
Worker epoch to consume an old execution command. Materialization durability
across the real FileJournal, StreamAgent, publisher, and scratch path is exercised
separately by `stage_stream_materialization_replay_test.go`; the control-layer
fixtures here do not substitute for that caller-level evidence.

Terminal failure/cancellation/expiry input retirement is described, but not
implemented, in `terminal-scratch-retirement-design-2026-09-05.md`. No claim of
bounded retention follows from successful command replay alone.
