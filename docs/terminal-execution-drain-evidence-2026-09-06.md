# Terminal allocation drain inspection

Local CPU/mock increment over `69701e0`. Database schema remains **90**;
Runtime admission, Worker admission and launch/Fleet schemas remain **2**.
Production Gates remain **0/9**. No GPU or remote lab deployment was used.

Follow-up: [durable non-admission proofs](execution-non-admission-evidence-2026-09-06.md)
adds a separate current-epoch never-admitted checkpoint and mixed terminal-history
collector at Runtime journal schema 3. The schema versions and validation below
describe this earlier increment; absent records alone still never prove drain.

## Partial Renewal Gap

An allocation can reach different authority renewals on different members. Their
durable drain checkpoints correctly retain those different signed envelopes.
The exact-envelope query must not count a checkpoint for another renewal as proof
for its query. Consequently, requiring all members to return the same envelope
cannot collect even an already drained allocation after partial renewal.

The new two-member, two-allocation UDS regression reproduces this distinction:
each member drains its actual envelope, exact inspection of the original returns
only one member's proof, and terminal allocation inspection recovers both actual
checkpoints. This is an observation/recovery issue; no execution authority is
extended or synthesized to resolve it.

## Read-Only Allocation Query

`InspectStageAllocationDrain` is additive on ModelRuntimeService and
StageWorkerMemberService. It uses the existing signed execution scope and current
trusted journal reader, with the same deterministic leader/mTLS and private UDS
boundaries. The query may name an older Runtime epoch or retired profile. It
never enters a backend, installs a floor, renews authority, changes Service health
or creates a checkpoint.

The result's outer authority digest echoes the query. A checkpoint preserves the
exact signed authority/digest, execution sequence, member, drain contract and time
actually persisted by Runtime. The receiver independently verifies that authority's
signature and replay time, then compares the full immutable execution identity.
Only the existing renewal fields are excluded from identity comparison: Stage
version, signing key ID, issued/expiry times, monotonic validity and signature.
Allocation/lease/attempt scope, fences, nonce, lease token, execution-spec digest,
capacity, device/member topology, residency/profile and Runtime epochs still match.
The existing monotonic `ValidateRenewal` rule remains unchanged.

Exact drain and exact read retain their previous contracts. Even an authentic
unseen renewal can only query the same allocation's previously persisted proof;
the response does not claim that this unseen envelope itself executed or drained.
Unknown, pending and absent records still have no checkpoint. A matching allocation
ID with a different immutable identity cannot inherit proof. Both forwarding hops
and the Worker reject invalid signatures, future-issued checkpoint envelopes,
malformed results, request mutation and late success.

## Complete Signed History

`Agent.InspectTerminalExecutionDrains` requires a fresh verified terminal
disposition, one authentic execution envelope keyed by every signed allocation,
and an explicitly configured current reader for every member. Before any RPC it
checks the complete allocation set, original lookup digest, terminal scope and
sequence/nonce/barrier, all signed members/devices and the trusted identity/subset
bindings. Missing, extra, ambiguous or mismatched input fails before dispatch.
Control response validation and Worker collection share `ValidateTerminalAllocation`
so these identity rules have one implementation.

Each allocation's members may return checkpoints for different renewals, while each
checkpoint must match that allocation's full immutable execution identity. One
deadline covers preflight and the whole history. Allocations are queried in order;
member calls use the existing bounded parallel collector. Failure preserves
positive results as partial evidence. `AllDrained` requires proof for every signed
allocation/member pair. The result is bound to the disposition digest and cutoff.

This covers the complete history signed **for this Worker**, not executions on
other Workers. It is an explicit observation API and not a durable Worker retirement
receipt. It neither proves that floors are installed nor excludes Worker input
writers, and cannot independently authorize scratch deletion.

## Validation

- Full `go test ./...`: PASS.
- Related authority, Runtime, transport, Worker and command race suite: PASS.
- `make lint`: PASS, 0 issues after test-only switch formatting fixes.
- `make generate`: PASS; only the expected four generated protobuf files changed.
- Protobuf additive compatibility against `69701e0`: PASS.
- Eight PostgreSQL terminal-history/disposition integration tests: PASS, including
  authenticated Control, exact original lookup, undelivered allocation history,
  renewal lookup, metadata expiry and historical Runtime scope.
- Linux arm64 non-root Runtime, Worker and member transport selections: PASS.

The real UDS test executes two allocations with independent member journals,
records partial renewals, preserves missing history as unproven, completes all
four allocation/member checkpoints, retires the original profile and recovers
the identical checkpoints through alternate current readers at new Runtime epochs.
The mTLS -> member -> UDS -> durable Runtime test reads a historical allocation
checkpoint after restart, keeps exact-envelope inspection unknown, and denies
nonleader reads. Fault tests cover signed but different nonce/token, malformed
proof, incomplete history, mutation, cancellation and whole-history timeout.

The PostgreSQL tests validate the shared terminal authority contract; they do not
wire the new collector into default Worker retirement. The UDS/mTLS tests use
FakeRuntime, not an external GPU driver. Neither test layer is a Launch Receipt.

```sh
go test ./...
go test -race ./internal/stageauthority ./internal/modelruntime ./internal/modelruntimetransport ./internal/stageworkeragent ./internal/stageworkermembertransport ./cmd/vela-model-runtime ./cmd/vela-stage-worker-agent
go test -tags=integration ./internal/integration -run '^(TestStageTerminal|TestPostgresTerminalHistory)' -count=1
make lint
make generate
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking . --against '.git#ref=69701e0'
```

Combined tracked generated-file SHA-256:
`03b801c29e424cdfd5c65281eb772634d9ac95eecd8c7ba2fd59ff2db5f86d09`, using
`git ls-files -z api/gen internal/store/sqlc proto/gen | xargs -0 shasum -a 256 | shasum -a 256`.

Linux binaries under `/tmp/vela-terminal-drain-linux` were built with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c`. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
Runs used no network, read-only root and binary mounts, UID/GID 65534, all
capabilities dropped and a private `/tmp` tmpfs. Selections:

- Runtime: `^(TestAllocationDrain|TestExecutionDrainRPC|TestDurableExecutionState)`.
- Worker: `^(TestTerminalExecutionDrain|TestExecutionDrainCollection|TestExecutionFloorCollection)`.
- Member: `^(TestMemberDrain|TestMemberInspection|TestMemberFloor|TestMemberCancellation)`.

## Remaining Work

Default `RetainScratchRetirer` remains active. A Worker retirement state machine
must still obtain and retain complete signed history, install/recover input and
Runtime floors, exclude input writers and persist its retirement intent/results
before filesystem cleanup. This increment does not resolve a pending checkpoint
whose backend envelope was lost, prove that an absent record was never admitted,
or recover writers left by a crashed process.

Durable sealed receipts, checkpoint reclamation (Runtime still retains at most
32 records without eviction), trusted bootstrap/default assembly, validated
schema-1 migration and external asynchronous/GPU driver drain remain open. No
bounded steady-state scratch usage or Production Gate completion is claimed.
