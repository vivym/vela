# Durable Runtime execution drain

Local CPU-only increment over `ef6f217`. Database schema remains 90; the local
Runtime admission journal is now schema **2**. Production Gates remain **0/9**.
No GPU, remote lab refresh or deployment was used.

## Retained Execution Contract

The prior journal retained only its highest signed allocation and floor witness.
A later Prepare overwrote that execution identity, while successful Seal cleared
the Service active record. Neither a sealed receipt cache nor STOPPED supplied
durable writer-drain evidence.

The journal now retains the original canonical signed authority for each admitted
execution, atomically with advancing the watermark and before backend Prepare.
It retains at most 32 executions without eviction or expiry-based reclamation.
The complete JSON file is bounded to 12 MiB, including the signed floor and
original/drained authorities, each independently limited to 64 KiB. A full history
rejects new Prepare and readiness without poisoning checkpoint reads or consuming
another sequence. Reclamation requires a future durable ownership handoff.

Each record starts with no drain proof. A completed checkpoint adds the exact
signed envelope used for drain, `vela-execution-writer-drain-v1`, its SHA-256
digest, immutable execution sequence and UTC observation time. A renewal must
validate against the retained original. Recovery validates canonical encoding,
signatures, scope, sequence ordering, watermark coverage, checkpoint identity,
contract and timestamp, in addition to the existing directory/inode/lock binding.
No persistent method can overwrite a checkpoint. Schema-1 journals are rejected;
they need validated migration and historical writer recovery, not reinitialization.

## Backend And Service Behavior

`BackendExecutionDrainer` explicitly requires execution task admission to close,
all execution tasks to join, writable handles to close and execution-owned child
writers to be reaped. It preserves resident models and sealed output. Success
binds the exact authority digest and execution sequence. Status and Cancel have
no fallback conversion into this contract. The Service passes a deadline bounded
by its cancellation timeout; implementations must honor that context. It never
invokes backend Close as a drain shortcut.

With durable admission configured:

- Seal keeps its local receipt and active identity until drain and checkpoint
  persistence succeed. Retry uses that receipt without repeating physical Seal.
- STOPPED and reusable FAILED cannot release the shared slot before the same
  checkpoint boundary. Drain cannot override `WorkerReusable=false`.
- Partial Prepare failure retains its identity and slot until drain is proven.
- Unsupported drain, errors, malformed/mismatched results and timeout preserve
  pending state. Persistence failure latches recovery without claiming success.
- Status cannot move a local terminal execution back to a nonterminal state or
  contradict OUTPUT_SEALED. A failed Prepare cannot reopen through PREPARED/Start.

The nonpersistent Service path preserves its existing behavior and does not
claim durable drain. FakeRuntime implements the capability: it owns synchronous
mutex-protected state and no execution goroutines, files or child processes.
Drain joins those state changes and fences reentry by sequence. This checkpoint
covered FakeRuntime. The subsequent [process drain increment](process-drain-evidence-2026-09-06.md)
adds negotiated ProcessBackend/H3/thumbnail support; unnegotiated drivers and the
optional in-process CPU-media adapter still retain durable terminal slots. The
independent read-only inspection channel remains observation only.

## Local Retry And Recovery

`Supervisor.DrainExecution` is a local Go API for an exact terminal identity still
held by the current resident Service. It permits a valid expired signature and
installed floor but does not install an unseen renewal. A changed Runtime epoch
or missing active record cannot enter a backend. `InspectExecutionDrain` reads an
exact persisted checkpoint across Runtime epochs, rejects invalid/future-issued
envelopes and returns no checkpoint for pending/unknown/superseded-envelope history.
Neither operation has new gRPC/member forwarding in this increment.

On recovery, any record without a drain checkpoint blocks new Prepare and readiness
across resident profiles. Restoring a floor or finding an empty replacement backend
does not remove this barrier. All-drained history permits new execution subject to
retained-record capacity. Existing recovery synchronization resolves an uncertain
rename/fsync before returning a checkpoint. A sealed receipt held during a failed
drain is still process-local; its durable recovery remains separate work.

These checkpoints cover one member's backend through the trusted backend contract
and private local journal. They do not prove Worker input-writer exclusion,
account for unseen allocations, collect other members, authorize deletion or
establish external driver process containment.

## Validation

- Full `go test ./...`: PASS; build-tagged integration campaigns are excluded.
- Related Runtime/transport/Worker/command race suite: PASS.
- `make lint`: PASS, 0 issues.
- Linux arm64 non-root drain, durable-state and floor-RPC tests: PASS.

Tests retain intent before backend entry; block drain on an actual file writer and
require handle closure before Seal/slot release; inject unsupported, incomplete,
malformed and timed-out drain; retry Seal without repeating physical work; preserve
failed-Worker health; reject terminal regression; retry exact expired identity after
a signed floor; reject unseen renewals; recover exact renewed checkpoints; reject
damaged persisted proofs; exercise 32-record backpressure; and recover after abrupt
helper-process exit with pending/drained history. Process helpers test journal
recovery, not execution-descendant containment. Existing floor transport tests now
reject newer work after recovery when their old execution was never drained.

The Linux binary `/tmp/vela-execution-drain-linux.test` was built with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c`. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
Execution used network isolation, read-only root, all capabilities dropped,
UID/GID 65534, private `/tmp` tmpfs and a read-only binary mount. Selection:
`^(TestExecutionDrain|TestDurableExecutionState|TestExecutionFloorRPC)`.

## Remaining Work

Implement authenticated all-member drain collection, validated external-driver
containment and durable Worker retirement history combined with signed floors
and input-writer exclusion; unresolved historical writer recovery; durable sealed
receipts; checkpoint reclamation; trusted bootstrap/default assembly and validated
schema-1 migration. Default Worker `RetainScratchRetirer` remains active, so automatic
scratch deletion and sustained production-loop progress remain disabled pending the
complete retirement protocol.
