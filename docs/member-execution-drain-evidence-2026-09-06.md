# Authenticated per-execution member drain

Local CPU/mock increment over `7ba0184`. Database schema remains **90**;
Runtime admission, Worker admission and launch/Fleet schemas remain **2**.
Production Gates remain **0/9**. No GPU, remote lab refresh or deployment was used.

## Protocol And Authority

ModelRuntimeService and StageWorkerMemberService now expose two distinct RPCs:

- `DrainStageExecution` retries the explicit writer-drain contract for an exact
  execution on its current resident Runtime and persists the resulting checkpoint.
- `InspectStageExecutionDrain` reads an existing durable checkpoint without
  entering a backend or changing execution/admission state. It permits an original
  Runtime epoch or profile that is no longer resident.

The versioned scope separates the currently configured journal reader identity
from the exact signed original execution authority. Both must describe the same
Worker instance/epoch, member/epoch, device set and membership. A historical read
may name a different configured resident profile/Runtime epoch on that member;
it does not authorize the replacement backend to execute or drain old work.

Every request requires a canonical signed schema-2 StageAuthority, bounded to
64 KiB, and replay validation that rejects future-issued authority outside the
existing clock-skew policy. Expiry permits exact historical drain or observation,
never new execution or renewal. Runtime requires durable admission and a configured
current reader; the original topology is independently checked against the retained
journal scope. Existing execution operations keep their current-runtime matching.

Member forwarding reuses mTLS authentication and the deterministic member leader.
The client requires a pinned target identity and the existing floor verifier.
Both transport hops validate response wrappers and nested proofs, including exact
reader identity, original signed authority/digest, member, execution sequence,
`vela-execution-writer-drain-v1` and a valid persisted timestamp. Unknown fields,
malformed proof and success arriving after cancellation are rejected. Separate
request copies prevent a client from mutating the expected validation scope.

`ACCEPTED` without a checkpoint means the exact durable proof is unknown. STOPPED,
a cancellation acknowledgement, missing history, process restart or a proof for
another renewal cannot substitute for that checkpoint. Checkpoints are returned
only after durable persistence, and retries preserve the original checkpoint.

## Cancellation And Collection

An exact Service record in CANCELING may now retry backend drain. This covers a
backend that reached STOPPED after authority expiry, when ordinary Status can no
longer update the Service record. Only successful explicit backend drain can be
checkpointed. The Service remains CANCELING; the operation does not invent terminal
state, `WorkerReusable` health or permission to release the shared slot. Existing
terminal drain rules still apply to STOPPED and reusable FAILED records.

`Agent.DrainExecution` resolves complete signed membership against independent
`ExecutionFloorConfig` bindings before making any RPC. `InspectExecutionDrain`
additionally requires an explicit current journal reader for every original member,
with each reader matched to those bindings. Missing, extra, untrusted or ambiguous
members/readers fail before dispatch. Calls run concurrently under the configured
floor timeout. Errors preserve collected positive member results as partial evidence;
only one valid persisted checkpoint for every required member sets `AllDrained`.

This result covers **one exact execution envelope**. It does not enumerate all
allocations of a terminal StageRun, prove a signed cutoff was installed, exclude
Worker input writers, persist a Worker retirement receipt or authorize deletion.
The collector is an explicit API; default command retirement assembly is unchanged.

## Validation

- Full `go test ./...`: PASS; build-tagged integration campaigns are excluded.
- Related Runtime, transport, Worker and command race suite: PASS.
- `make lint`: PASS, 0 issues.
- Protobuf compatibility against `7ba0184`: PASS.
- `make generate`: PASS; tracked generated files reproduce identically.
- Linux arm64 non-root Runtime, Worker collector and member transport selections:
  PASS, with independent persistent Runtime instances and real mTLS/UDS transport.

The full suite initially rejected the added RPC names in its exhaustive protocol
allowlist. The allowlist now includes both methods and retains its prohibition on
request-path LoadModel/UnloadModel/ReplaceModel operations; the rerun passed.

Runtime tests cover persistence before reply, idempotent retry, read-only inspection,
expired authority after a signed floor, epoch recovery, invalid scopes and CANCELING
drain without inferred slot reuse. A real mTLS -> member -> UDS -> durable Runtime
chain rejects nonleaders, loses a reply after checkpoint persistence, recovers the
same proof after restart and rejects unseen renewal proof. Both hops reject malformed
evidence, request mutation and late success for both RPCs.

The collector tests use two independent persistent Runtimes: one drained member
stays partial; both drained members complete collection. After profile retirement
and Runtime epoch advancement, alternate configured readers recover the original
checkpoints. Further tests reject incomplete trusted scope before dispatch, wrong
proofs, unresponsive members and replies after cancellation.

Commands:

```sh
go test ./...
go test -race ./internal/modelruntime ./internal/modelruntimetransport ./internal/stageworkeragent ./internal/stageworkermembertransport ./cmd/vela-model-runtime ./cmd/vela-stage-worker-agent
make lint
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking . --against '.git#ref=7ba0184'
make generate
```

The combined tracked generated-file SHA-256 before and after generation was
`2dc2d4d44eed520363fa38674969257800e004955c7d53cdb55047437dd59e9c`, computed with
`git ls-files -z api/gen internal/store/sqlc proto/gen | xargs -0 shasum -a 256 | shasum -a 256`.

Linux binaries under `/tmp/vela-drain-transport-linux` were built with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c`. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
Runs used no network, read-only root and binary mounts, UID/GID 65534, all
capabilities dropped and a private `/tmp` tmpfs. Selections:

- Runtime: `^(TestExecutionDrain|TestExecutionFloorRPC|TestDurableExecutionState)`.
- Worker: `^(TestExecutionDrainCollection|TestExecutionFloorCollection)`.
- Member: `^(TestMemberDrain|TestMemberInspection|TestMemberFloor|TestMemberCancellation)`.

## Remaining Boundary

Worker retirement still needs complete signed terminal allocation history, durable
retirement intent/receipt recovery, signed floors and Worker input-writer exclusion
combined with these per-execution proofs. Pending historical writers, durable sealed
receipt recovery, checkpoint reclamation, trusted bootstrap/default assembly and
validated schema-1 migration remain open. Runtime history remains bounded to 32
records without eviction. External asynchronous/GPU drivers require their own
validated task/handle/descendant drain; the optional in-process CPU-media adapter
remains separate from the thumbnail subprocess capability.

Default `RetainScratchRetirer` remains active. These checks do not establish safe
scratch deletion, bounded steady-state scratch usage or any Production Gate PASS.
