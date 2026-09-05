# Resident ProcessBackend execution drain

Local CPU/mock increment over `831f83f`. Database schema remains **90**;
Runtime admission, Worker admission and launch/Fleet schemas remain **2**.
Production Gates remain **0/9**. No GPU, remote lab refresh or deployment was used.

## Independent Protocol

ProcessBackend supplies a second private Unix datagram socket inherited as fd 4,
declared by `VELA_MODEL_DRIVER_DRAIN_FD=4`. Drivers opt in through the initialize
response `drain_protocol: "vela-driver-drain-v1"`. The existing inspection socket
remains fd 3 and strictly read-only. The shared `driverchannel` package owns only
socket-pair and inherited-descriptor plumbing; inspection and drain keep separate
clients, gates, request IDs and protocols.

Each drain request binds its schema, request ID, exact authority digest and positive
immutable execution sequence. A positive reply must echo that identity and declare
`vela-execution-writer-drain-v1`. Negative or absent proof is unproven. Packets are
bounded to 1024 bytes; malformed JSON, duplicate/unknown fields, mismatched identity
and wrong contracts fail closed. Each call, including gate wait and socket I/O,
has a maximum one-second lifetime. Up to 32 stale replies can be discarded; a late
positive reply cannot authorize a later request. Cancellation never uses driver
signals, shutdown or the stdio command gate. Protocol failure affects only this
channel. A timed-out request may have frozen the execution, but requires a fresh
acknowledgement before Runtime can persist proof.

Only negotiated drivers receive `execution_sequence` in normal command identities.
Other drivers keep the previous strict wire shape and return unproven for drain.
The parent still supervises explicit Close and ordinary command failure through
the existing process-group lifecycle; this is separate from per-execution drain.

## H3 And CPU Thumbnail Behavior

Both built commands open fd 4 and use the shared H3 session implementation. All
mock execution work and file publication run synchronously under a command mutex.
Writable files close before the command releases that mutex. These implementations
launch no execution child processes or asynchronous writer tasks.

Drain uses `TryLock`: a concurrent command returns unproven promptly, allowing
retry after its work finishes. With the lock held, drain requires the exact known
authority and execution sequence in STOPPED, FAILED or OUTPUT_SEALED. It then
irreversibly freezes that execution. PREPARED, RUNNING, OUTPUT_READY, unseen,
superseded, zero-sequence and mismatched identities cannot produce proof.

Frozen executions reject Prepare/Start, identity renewal and further mutating
commands. Exact terminal Status and replay of an already sealed manifest remain
available. Idempotent STOPPED Cancel performs no work. Shutdown and Close skip
namespace cleanup for drained executions, including failed output remnants; those
files belong to later retirement. Sealed output and resident models are preserved.
The positive execution watermark excludes old/equal sequences after replacement,
including changed-digest replay and attempts to return to unsequenced execution.

This capability covers only a currently retained exact execution. A Prepare that
failed before the mock installed its active record remains unproven; no inference
from missing state is added. The driver keeps no persistent drain history of its
own and returns unproven for replaced identities. Runtime must persist the positive
checkpoint before releasing a durable slot. Its existing history and restart
barriers remain authoritative.

## Validation

- Full `go test ./...`: PASS; build-tagged integration campaigns are excluded.
- Related Runtime, transport, Worker and command race suite: PASS.
- `make lint`: PASS, 0 issues.
- Linux arm64 non-root protocol, process, durable Runtime and floor regressions:
  PASS, including the real compiled H3 and thumbnail commands.

Protocol tests exercise exact/negative proof, wrong identity/sequence/contract,
malformed/oversized/duplicate/unknown JSON, late proof, retry, backpressure, gate
wait, request-ID exhaustion and malformed requests that never invoke freeze.
H3 tests hold an actual command in flight, reject drain until it returns, freeze
terminal identities and retain files through Close for each terminal state.

Process tests inject silent, malformed and delayed replies while cancellation,
read-only inspection, readiness and graceful shutdown remain usable. The built
H3 command accepts a renewal, rejects proof for the superseded envelope, drains
sealed output, rejects Start replay, runs another execution in the same resident
driver and preserves the original sealed payload through shutdown.

Durable Service tests execute two successive assignments per scenario: H3 success,
H3 cancellation, H3 reusable failure and CPU thumbnail success. They verify exact
persisted checkpoints, slot reuse, driver liveness, sealed file hashes, checkpoint
recovery after Runtime epoch advancement and subsequent admission. This is local
Service/ProcessBackend integration, not an authenticated all-member drain receipt.

Linux binaries are under `/tmp/vela-process-drain-linux`, built with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64`. The test image is
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
Runs use no network, read-only root and binary mounts, UID/GID 65534, all capabilities
dropped and a private `/tmp` tmpfs. Runtime selection includes
`TestProcessBackendDrain`, `TestProcessDrainDurableNativeCommands`,
`TestProcessBackendInspection`, `TestExecutionDrain`, `TestDurableExecutionState`
and `TestExecutionFloorRPC`. Protocol and H3 drain/inspection tests run separately.

## Remaining Boundary

External GPU or asynchronous drivers still need their own validated implementation
of execution task admission, task joins, handle closure and descendant containment.
A negotiated acknowledgement is a trusted driver contract, not independent proof
against a compromised driver. The CPU mock has no descendants to contain.

Authenticated all-member drain collection, Worker input-writer exclusion and
retirement journal orchestration, unresolved historical writer recovery, durable
sealed receipts, checkpoint reclamation, trusted default assembly and validated
schema-1 migration remain open. The optional in-process CPU-media adapter is a
separate backend and is not covered by the thumbnail subprocess capability.
Default `RetainScratchRetirer` remains active. These checks do not authorize scratch
deletion, prove bounded steady-state scratch usage or produce Launch Receipts.
