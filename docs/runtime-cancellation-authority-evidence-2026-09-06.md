# Cancellation preserves installed execution authority

This increment follows `f271cfe`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Reproduced failure

`CancelStage` previously called `requireActive(verified, allowRenewal)` before
calling the backend. A fresh successor envelope therefore replaced active
authority and reset the watchdog before any cancellation acknowledgement.

Two initial counterexamples reproduced this behavior. The ordinary FakeRuntime
rejected cancellation because the new envelope had never reached its execution;
an explicitly failing backend also rejected. Both paths replaced the watchdog
and made exact inspection of the original authority report unknown. The
forwarded-command Worker journal repair deliberately preserved cancellation for
recovery, so its execution guards did not address this separate renewal path.

## Contract and implementation

Cancellation now resolves its target without changing active state or timers.
The existing operation mutex serializes target selection with execution calls.
The active-state mutex covers only the snapshot; it is not held over the backend
call. The backend receives a defensive copy of the installed authority. A
successful signal changes that execution to CANCELING; a backend error leaves
its installed authority, state and original watchdog deadline intact.

The exact installed envelope can cancel after expiry. With healthy Runtime
admission above its terminal floor, a fresh compatible successor may authorize
stopping the same immutable execution, using the existing `ValidateRenewal`
relationship. It grants no renewal. The current envelope is revalidated after
waiting for the operation mutex, as before. A superseded envelope, unseen
expired/future successor, changed execution identity or bad signature rejects.
Runtime journal failure or a terminal floor still restricts cancellation to the
exact installed envelope. No new RPC or schema is required.

The response authority digest remains bound to the caller's request. This
acknowledges a cancellation command, not installation of its envelope. Exact
inspection and drain continue to name the actual installed envelope. A query
for the successor cannot inherit exact evidence; allocation-level checkpoint
inspection may return the original signed proof without rewriting it.

## Validation

- Initial acknowledged/failed-backend regressions fail on the parent behavior
  and pass after the repair. Neither successful nor failed cancellation creates
  a new watchdog. After failure, the original deadline still delivers Cancel to
  the backend under the original installed authority.
- An additional backend mutates its received envelope and returns an error.
  The stored authority and later watchdog remain unchanged. Compatible cancel
  replay does not call the backend again or install its request envelope.
- Tests cover exact current/expired authority, fresh successor after an earlier
  Status renewal, superseded authority, unseen expired/future successors,
  changed allocation identity and bad signatures. Existing queued-authority,
  floor, inspection, drain and expired-cancellation tests also pass.
- Actual mTLS -> UDS -> durable CPU Runtime tests close the Worker journal
  after Prepare. A successor Cancel can signal or fail without becoming known
  to inspection. Installing a floor then rejects that uninstalled successor;
  exact original cancellation and durable drain still work. Exact successor
  drain inspection stays empty while allocation inspection returns the
  unchanged original checkpoint.
- A compiled `h3-encoder` CPU command executes through ProcessBackend and the
  ModelRuntime service. A successor-authorized Cancel stops it while retaining
  original inspection identity and resident readiness. The command name follows
  the existing executable-identity contract; no GPU is opened or requested.
- Full `go test ./...` passes. Related race suites pass for ModelRuntime
  (`60.292s`), Runtime transport (`7.616s`), member transport (`15.749s`), Worker
  Agent (`66.446s`) and the Worker command (`28.537s`). The final native-command
  helper change also passes its focused process race test (`2.707s`).
- PostgreSQL integration passes the authenticated terminal disposition,
  replacement-Runtime terminal recovery and lease-expiry/latest-renewal
  cancellation tests (`25.096s` combined). These protect Control and recovery
  contracts; the targeted local/mTLS/process tests establish the new Runtime
  cancellation behavior.
- `make lint` and `git diff --check` pass.
- Linux arm64 Runtime cancellation, actual H3 process, member TLS/UDS and
  historical cancellation tests pass as UID/GID 65534 without network or
  capabilities, with read-only root/binaries, tmpfs and `no-new-privileges`.
  Image: `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

Linux binaries are `/tmp/vela-cancel-authority-runtime-linux.test`,
`/tmp/vela-cancel-authority-member-linux.test` and
`/tmp/vela-cancel-authority-linux/h3-encoder`. The process test reuses
`nativeDrainCommands` and accepts the existing
`VELA_TEST_DRAIN_COMMAND_DIRECTORY` test-only override for cross-built commands.

## Remaining work

This repair prevents cancellation from granting execution time. It does not
make cancellation preempt a blocked backend operation: Cancel and the watchdog
still serialize through the Service operation mutex. Failed-backend
containment, physical replacement, pending input writer recovery, sealed receipt
recovery, terminal retirement, bounded reclamation and default Fleet durable
activation remain open. A stop acknowledgement is not writer drain or device
reuse evidence. No remote deployment, push, GPU or Production Gate acceptance
occurred. The full correctness goal remains incomplete.
