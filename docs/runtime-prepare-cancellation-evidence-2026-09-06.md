# Canceled Prepare retains execution cancellation

This follow-up to `3eaceda` preserves PostgreSQL schema 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Reproduced failure

The watchdog repair preserved PREPARING after monotonic expiry, but caller
cancellation still set FAILED before rejecting Prepare. CancelStage then returned
`StageAttempt no longer has cancellable compute`, and the original watchdog
skipped the terminal state. Neither outcome established that the backend had
stopped: the test backend had already accepted preparation before the caller
canceled its request.

Four direct-service counterexamples reproduce the failure on `3eaceda`:
cooperative cancellation errors and uncooperative late success, each followed
by either explicit cancellation or original watchdog expiry. Explicit Cancel
rejected; deadline expiry never reached backend Cancel.

## Repair and evidence

The canceled-context branch now retains PREPARING and its installed authority
and watchdog, with reuse disabled. It rejects the Prepare reply without
synthesizing a backend FAILED state. Ordinary backend errors without observed
context cancellation retain their existing failure/drain handling.

All four counterexamples pass after the change. Start remains rejected without
a confirmed PREPARED result. Explicit Cancel and the original monotonic deadline
reach the backend with the original envelope and correct reason. No drain
checkpoint appears, and another resident runtime cannot acquire the shared slot.

The focused watchdog/cancellation suite passes (`6.120s`), full `go test ./...`
passes, and `make lint` passes with zero issues. Related race suites pass for
ModelRuntime (`61.828s`), Runtime transport (cached), member transport (`9.790s`),
Worker Agent (`67.608s`) and the Worker command (`10.909s`).

## Boundaries

These tests use direct Runtime calls, a durable CPU fixture and a manual clock.
They prove request-cancellation state handling, not transport deadlines,
physical containment or GPU behavior. Context cancellation does not itself
revoke the signed execution lease or prove backend stop. A later authoritative
Status observation may still reconcile the actual backend state under normal
authority validation.

Ordinary backend errors with unproven drain, explicit CancelStage preemption,
failed-backend and pending-input-writer recovery, durable sealed receipts,
terminal retirement, bounded reclamation and default Fleet durable activation
remain open. No remote deployment, push, GPU use or Production Gate acceptance
occurred. The full correctness goal remains incomplete.
