# Durable execution non-admission proofs

Local CPU/mock increment over `504143d`. Runtime admission journal advances to
**3**. Worker admission and launch/Fleet schemas remain **2**; database schema
remains **90**. Production Gates remain **0/9**. No GPU or remote lab deployment
was used.

## Undelivered Allocations

A complete signed terminal disposition includes allocations that Runtime may
never have received. Missing execution history cannot establish either backend
drain or historical non-admission. The new proof records that one allocation
never entered one member's backend and that a persisted floor prevents its
future entry. This is distinct from the existing backend writer-drain contract.

`Supervisor.CheckpointNonAdmission` requires durable admission, the original
currently resident Runtime epoch/profile, a verified signed authority and a
persisted floor covering its sequence. Under the admission mutex it rejects any
persisted execution intent or active operation at that sequence, including a
conflicting identity or an intent that failed before backend entry. It persists
the immutable `vela-execution-never-admitted-v1` checkpoint before responding.
It does not enter or cancel a backend, change the watermark, release slots,
clear recovery barriers or close resident models.

This proof depends on trusted unique Runtime epoch assignment, construction with
unused Services and durable intent before backend entry. It does not establish
hostile-owner rollback resistance. A missing old-epoch record stays unknown;
restarting under a new epoch cannot create an absence proof for the old epoch.

`Supervisor.InspectNonAdmission` only reads an existing durable checkpoint. It
may use a new epoch/profile as the trusted journal reader. Compatible authority
renewals retain the actual signed checkpoint envelope and digest, validated
against the full immutable execution identity.

## Journal And Transport

Schema 3 adds a separate bounded non-admission history: at most 32 entries, with
no eviction. Recovery verifies canonical signatures, retained topology,
sequence/digest, increasing order, contract/time, cutoff within the persisted
floor and no overlap with admitted execution records.

`ExecutionFloorStateConfig.UpgradeV2` explicitly permits validated schema-2 to
schema-3 recovery. It preserves journal ID, scope, filesystem ownership binding,
signed watermark/floor and pending/drained records, and creates no absence
proof. Recovery without opt-in rejects schema 2 without replacing its state.
Schema 1, damaged evidence, schema-2 data containing a new proof and simultaneous
initialization/upgrade reject. Repeating opt-in after schema 3 was published is
idempotent. No CLI or default upgrade assembly is provided by this increment.

`CheckpointStageNonAdmission` and `InspectStageNonAdmission` are additive RPCs on
ModelRuntimeService and StageWorkerMemberService. They use private UDS and the
existing deterministic leader/mTLS, pinned target and trusted reader checks.
Both forwarding hops independently validate wrappers, query binding, signed
checkpoint authority, replay time, immutable execution identity, member,
sequence, cutoff, contract and timestamp. ACCEPTED without a checkpoint means
unknown. Malformed, rejected, canceled and late replies cannot become proof.

## Complete Mixed Evidence

The Worker now shares complete signed-history preflight between its drain and
exclusion collectors. `InspectTerminalExecutionExclusions` is read-only;
`CheckpointTerminalExecutionExclusions` may establish current-epoch absence
proofs after Runtime floors have been independently installed.

Each allocation/member pair contributes exactly one typed proof: backend drain
or never-admitted. A valid unknown drain read permits an absence read; only a
valid unknown absence read permits optional checkpoint creation. Errors and
malformed/rejected replies never fall through to an alternative success. One
timeout covers preflight and the whole history, with bounded member concurrency.
`AllExcluded` requires every pair; partial evidence stays partial.

Creating a new proof requires the selected member's original current binding.
Other members may supply existing proofs through configured current readers
after their original epoch/profile retires. Complete signed-history and trusted
reader preflight still covers every member before any RPC. The follow-up below
records the correction from the initial all-member residency requirement.

The collector neither installs floors nor drains backends. Its result is not a
durable Worker retirement receipt, input-writer exclusion proof or filesystem
deletion authority. Default `RetainScratchRetirer` remains active.

## Validation

- Full `go test ./...`: PASS.
- Related Runtime, transport, Worker and command race suite: PASS.
- `make lint`: PASS, 0 issues.
- `make generate`: PASS; changes are confined to the expected four protobuf
  generated files, with no OpenAPI or SQL output changes.
- Protobuf additive compatibility against `504143d`: PASS.
- Linux arm64 non-root Runtime, Worker and member selections: PASS.

The real two-member/two-allocation UDS regression collects one drain and three
never-admitted proofs. It covers missing floors, read-only unknown results,
lost checkpoint replies, retries and identical checkpoint recovery after profile
retirement and Runtime epoch changes. The original `AllDrained` stays false for
mixed evidence. The mTLS -> member -> UDS -> durable Runtime test also covers
nonleader denial, lost responses, restart and invalid proof at both hops.
Other regressions cover concurrent admitted Prepare, persistence uncertainty,
corrupt journals, the history limit, schema upgrade preservation/rejection,
incomplete signed history and late replies.

```sh
go test ./...
go test -race ./internal/modelruntime ./internal/modelruntimetransport ./internal/stageworkeragent ./internal/stageworkermembertransport ./cmd/vela-model-runtime ./cmd/vela-stage-worker-agent
make lint
make generate
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking . --against '.git#ref=504143d'
```

Combined tracked generated-file SHA-256:
`61327d8ac74aa5d12e35e71b83536278269d323dbdaf9f28bb475950bc19e6f0`, using
`git ls-files -z api/gen internal/store/sqlc proto/gen | xargs -0 shasum -a 256 | shasum -a 256`.

Linux binaries under `/tmp/vela-non-admission-linux` were compiled with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c`. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
Runs used no network, a read-only root/binary mount, UID/GID 65534, all capabilities
dropped and a private `/tmp` tmpfs. Selections, each with `-test.count=1`:

- Runtime: `^(TestExecutionNonAdmission|TestExecutionDrainRPC|TestDurableExecutionState)`.
- Worker: `^(TestTerminalExecutionExclusion|TestTerminalExecutionDrain|TestExecutionFloorCollection)`.
- Member: `^(TestMemberNonAdmission|TestMemberDrain|TestMemberFloor)`.

These tests use CPU/FakeRuntime. PostgreSQL integration was not rerun for this
increment, which changes neither migrations nor database queries. Earlier
terminal-history integration evidence retains its original source boundary.
No sustained throughput, external asynchronous driver drain or Launch Receipt
is established by these checks.

## Member Residency Follow-Up

A regression against `f13a98d` reproduced one member's retired original profile
preventing a still-resident member from checkpointing non-admission. The collector
required all original bindings again even when the retired member had already
supplied valid historical drain evidence. Its independent peer therefore lost
one of two available absence checkpoints.

The shared route validator now also resolves one member without requiring other
members' original residency. Complete-history preflight still resolves and
authenticates every historical reader. Only creating a new checkpoint uses the
selected member's exact original epoch/profile; reading historical evidence
continues to preserve the actual saved authority. No protocol or journal schema
changes are involved.

The deterministic collector regression covers both a peer with historical drain
and a peer with unknown history. The first completes mixed proof collection;
the second keeps the result partial while preserving both resident-member
proofs. The retired original profile cannot create a new absence checkpoint.
These new cases model profile retirement in trusted routing configuration with
stubbed RPC replies; the earlier real UDS/mTLS tests separately cover persistence
and transport. Full unit tests, Worker race, lint (0 issues) and non-root Linux
Worker tests pass after the correction. Linux used `worker-resident.test` under
the same directory/image and restrictions above, selecting:
`^(TestTerminalExecutionExclusion|TestTerminalExecutionDrain|TestExecutionDrainCollection|TestExecutionFloorCollection)`.

## Remaining Work

Worker retirement still needs durable orchestration of signed history, input
and Runtime floors, input-writer exclusion, complete typed member evidence and
retirement intent/results before cleanup. Pending historical execution recovery,
lost intermediate renewal envelopes, old-epoch absence without a saved proof,
durable sealed receipts and bounded checkpoint reclamation remain open.
Trusted default/bootstrap assembly, schema-1 migration and external
asynchronous/GPU task/handle/descendant drain also remain open. The 32-entry
bounds retain evidence but do not establish sustained Worker progress.
