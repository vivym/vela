# Watchdog interruption of blocked execution calls

This increment follows `2cfaf58`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Reproduced failure

Prepare, Start, Status and Seal held the Runtime execution mutex across backend
calls. The watchdog also acquired that mutex before enforcing its deadline.
Four blocking-backend counterexamples therefore fired the monotonic timer but
received no context cancellation until the test manually released the call.

## Contract and implementation

Each execution call now registers a derived context with the installed watchdog
generation. Expiry latches that generation as expired and cancels its context
under the active-state mutex, before waiting for the execution mutex. The later
backend Cancel remains serialized and uses the installed authority. Old timer
generations cannot interrupt renewed calls; observed expiry rejects further
renewal and successor cancellation. Exact installed cancellation remains valid.
Shutdown also cancels the registered context before closing the backend.

Prepare, Start and Status reject late successful backend replies after context
cancellation. Canceled Prepare retains the active slot without automatically
checkpointing drain. A successful late Seal validates and retains the receipt
in active memory, records OUTPUT_SEALED, and rejects the reply without claiming
drain. Its exact identity remains available for explicit recovery.

The admitted call and journal ownership remain live until the call actually
returns. Canceling a context does not manufacture a return, release shared
execution capacity, or prove that writers have stopped.

## Validation

- The original four blocked-call counterexamples fail before the repair and
  pass with cooperative backend cancellation.
- Four uncooperative-call cases observe cancellation but remain blocked until
  explicitly released. Floor installation stays available; waiting for accepted
  operations times out while each call is still active. Late success rejects,
  and no drain checkpoint appears automatically. Late Seal retains its exact
  OUTPUT_SEALED identity and supports a subsequent explicit original drain.
- A stale timer cannot interrupt a renewed blocked Status; its current timer
  does interrupt it. Existing watchdog tests use non-renewing inspection after
  expiry, and a fresh successor cannot revive expired execution.
- An actual ProcessBackend helper blocks in Prepare after creating a marker.
  Deadline interruption terminates its process group and stops its child writer.
  The runtime still has no drain checkpoint and rejects another shared-slot
  execution. This uses CPU ResourceClass and requests no GPU.
- The durable journal lifetime test confirms Shutdown rejects a late Prepare
  success and prevents competing journal ownership until the call returns.
- Full `go test ./...` passes. Related race suites pass for ModelRuntime
  (`69.611s`), Runtime transport (cached), member transport (`7.885s`), Worker
  Agent (`74.918s`) and the Worker command (`18.963s`).
- Four PostgreSQL integrations pass (`36.846s` combined): authenticated terminal
  disposition, replacement-Runtime terminal recovery, lease expiry with latest
  renewal during cancellation, and compiled Node bootstrap binding discovery.
  These protect surrounding recovery contracts; the targeted local and process
  tests establish watchdog interruption.
- `make lint` passes with zero issues; `git diff --check` passes.
- Linux arm64 watchdog, late-success, generation, process/child-writer, admitted
  journal lifetime and blocked request-write tests pass as UID/GID 65534 with
  no network, dropped capabilities, read-only root and binary, tmpfs and
  `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Binary: `/tmp/vela-watchdog-call-runtime-linux.test`.

## Remaining work

Explicit CancelStage still waits for the execution mutex and does not interrupt
an in-flight command before the original deadline. Readiness and other backend
RPCs are not covered by these execution-call contexts. A backend that ignores
context may remain indefinitely in flight, retaining admission and ownership.

ProcessBackend interruption uses its existing exceptional abort/group-termination
path and may terminate the resident driver. This is not ordinary model eviction.
The process test proves only bounded same-process-group behavior, not escaped
descendants, GPU quiescence or general physical containment. Process exit and
cancellation acknowledgement are not writer-drain or device-reuse evidence.

Late Seal retention is in memory, not durable sealed-receipt recovery. Pending
input writers, failed-backend recovery, physical replacement, sealed receipt
recovery, terminal retirement, bounded reclamation and default Fleet durable
activation remain open. No remote deployment, push, GPU use or Production Gate
acceptance occurred. The full correctness goal remains incomplete.
