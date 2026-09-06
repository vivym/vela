# Live backend authority discovery for allocation recovery

This increment follows `fb5c8bd`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Recovery gap

The preceding increment could reconcile cancellation after an unacknowledged
renewal, but recovery tests already possessed the backend's actual signed
envelope. A caller holding only the latest grant could cancel, then fail exact
drain if the backend still held the previous grant. Allocation drain inspection
could only retrieve an existing checkpoint, so it could not resolve this gap
before the first successful drain. No existing RPC returned the live backend
envelope.

## Contract and implementation

`InspectAllocationExecution` is added to ModelRuntimeService and its authenticated
StageWorkerMemberService forwarding path. A historical signed query must match
the current resident Runtime and the active immutable execution. It may name a
different renewal, but cannot add members or cross an allocation, Worker/member
epoch, Runtime epoch, execution spec or profile boundary.

Runtime copies at most the accepted and last backend-confirmed candidate, then
uses only exact read-only backend inspection. One valid Known candidate yields
its unmodified signed envelope and an exact inspection record. The outer digest
correlates the query; the nested digest binds the observed envelope. Neither
the query nor the response is writer-drain proof. No candidates or no Known
observation returns unknown. Contradictory, malformed or unavailable observations
reject; deadline/cancellation drops late replies. Runtime also rejects if its
generation or captured candidate identities changed during inspection.

The read never acquires the execution/admission mutex, confirms a candidate,
updates execution state, resets the watchdog, dispatches Cancel/Status, releases
capacity or writes a journal. The timeout bounds a cooperative inspection, not
a backend that ignores context. An observation can become stale immediately;
subsequent exact cancellation/drain must repeat their existing validation.

Both forwarding boundaries independently validate query correlation, current
Runtime/member identity, the returned signature, same-execution relationship and
the nested exact observation. Defensive request/response copies prevent
in-process adapters or callers from changing expected/retained identities.
Only the authenticated leader can read through the member interface, including
after Worker journal closure or terminal-floor installation. Old Runtime epochs
cannot discover state from a replacement backend. Peers without the additive
RPC return Unimplemented; recovery must not fall back to renewal-capable Status.

## Validation

- A UDS recovery test begins with only the latest query envelope after renewal
  response loss and a terminal floor. Both before-application failure and
  application followed by response loss discover the actual backend envelope,
  then pass that returned value to exact cancellation and durable drain. Reads
  themselves invoke no Status/Cancel and create no checkpoint.
- Error, unknown, both-known, malformed and timed-out backend observations
  expose no envelope; subsequent healthy discovery still succeeds.
- Unseen compatible renewal queries do not install that envelope or change the
  watchdog. Caller mutation of a response cannot alter retained authority.
  Unrelated allocations and restarted Services return unknown.
- Invalid request schema, unknown fields, signatures, future-issued envelopes,
  Runtime epochs and canceled contexts never reach backend inspection. Missing
  inspector capability cannot fall back to Status.
- Concurrent renewal, Shutdown and canceled callers discard late observations.
  Blocked read tests permit explicit cancellation and deadline watchdog dispatch.
- Existing actual compiled `h3-encoder` CPU tests now discover the envelope used
  for subsequent exact inspection/drain; both renewal outcomes preserve resident
  readiness. The command opens no GPU and uses synthetic device identity metadata.
- Actual mTLS -> UDS recovery after Worker journal closure and terminal-floor
  installation rejects nonleader discovery. The leader queries the latest grant
  and uses the returned backend envelope for exact drain and allocation lookup.
- Forty-two malformed-response cases run across Runtime -> member and member ->
  client boundaries. Missing/partial records, bad signatures, other executions,
  wrong query/observed digests, wrong Runtime/member identities, unknown fields,
  invalid time/sequence/state and rejected observations return DataLoss.
- Full unit verification initially caught the protocol method allowlist missing
  the new RPC; its contract test was updated. No Load/Unload/ReplaceModel method
  was added. Full `go test ./...` passes after the allowlist update.
- Related race packages pass: ModelRuntime `80.473s`, Runtime transport `2.843s`,
  member transport `9.515s`, Worker Agent `73.591s`, and Worker command `27.546s`.
  The first invocation named nonexistent `cmd/vela-stage-worker`; the four
  library packages still ran successfully, and the correct
  `cmd/vela-stage-worker-agent` target passed separately.
- Four PostgreSQL integrations pass (`19.634s`): authenticated terminal
  disposition, replacement-Runtime terminal recovery, latest renewal during
  cancellation/expiry, and compiled Worker bootstrap binding.
- `make lint` reports zero issues. Buf breaking-change validation against
  `fb5c8bd` passes. Linux arm64 Runtime and member tests pass using cross-built
  `/tmp/vela-allocation-discovery-*-linux.test`, the compiled H3 command from
  `/tmp/vela-renewal-recovery-linux`, and image
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Containers run as UID/GID 65534 with no network/capabilities, read-only
  root/binaries, private tmpfs and `no-new-privileges`.

## Remaining work

The candidate pair remains in memory. This RPC discovers a live observation;
it does not restore candidates after a Runtime restart, recover lost signatures
from a durable journal or prove physical replacement/containment. Automatic
Worker recovery orchestration still needs to compose discovery with exact drain
and existing terminal-history/floor proofs. The existing Agent exact drain
collector has not been relaxed to accept another envelope's checkpoint.

Successful terminal retirement, bounded capacity/history reclamation, pending
input writers, sealed receipt persistence and default Fleet durable activation
remain open. No remote deployment, push, GPU use or Production Gate acceptance
occurred. The full correctness goal remains incomplete.
