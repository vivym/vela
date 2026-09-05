# Terminal allocation non-admission evidence

Date: 2026-09-06. Database schema 90; Runtime admission journal 4;
Worker admission journal 3; launch/Fleet schemas 2. Production Gates: **0/9**.

## Architectural gap

The existing PostgreSQL terminal-history regression allocates a retry, then
cancels the Job before Control creates its signed assignment. The complete signed
terminal disposition includes that allocation, but no execution envelope exists.
Requiring an original `StageAuthority` for every allocation therefore cannot
close this case, even with perfect recovery of locally delivered assignments.

Runtime now accepts the signed terminal allocation directly through the local
`Supervisor.CheckpointTerminalNonAdmission` API. It does not synthesize an
execution envelope. The separate contract is
`vela-terminal-allocation-never-admitted-v1`.

## Proven boundary

A new checkpoint requires all of the following under the shared admission lock:

- A valid, currently fresh Control terminal signature and selected allocation.
- Complete matching trusted Worker/member/device topology, including identity
  and device-subset digests for every member in every historical allocation.
- The selected allocation's original local Runtime epoch, profile, residency and
  runtime identity still present in the Supervisor. Other allocations do not need
  current residency for this operation.
- An independently persisted Runtime floor covering the selected sequence.
- No retained execution intent or active operation at that sequence. A completed,
  drained, failed-before-entry or conflicting execution record also precludes
  claiming that the sequence was never admitted.
- No conflicting identity in either non-admission proof format.

The checkpoint durably retains the canonical signed disposition, its digest,
selected allocation ID and sequence, installed cutoff, contract and observation
time in the existing bound and locked Runtime journal. Persistence completes
before a proof is returned. It neither calls nor unloads the backend.

`InspectTerminalNonAdmission` replays existing proof across Runtime epoch/profile
changes and accepts expired historical terminal facts. Future facts reject.
A refreshed signed query may change observation/expiry, signing key, original
query anchor or control session; the returned proof still carries its actual
original signed checkpoint. Tenant, Job/Attempt/StageRun, terminal state/version,
Worker/device topology and selected allocation identity must match. Missing
historical proof remains unknown. Replaying proof does not create backend drain
or an execution-envelope non-admission checkpoint.

## Journal compatibility

Schema 4 adds optional `terminal_non_admissions` to the existing Runtime state.
`ExecutionFloorStateConfig.UpgradeV3` explicitly validates schema 3 before
upgrading, preserving all identity, ownership, floor, watermark, pending/drained
execution records and existing envelope-based non-admission records. It creates
no proof. Schema 2 can still upgrade with `UpgradeV2`, now to schema 4. Ordinary
recovery cannot migrate either schema implicitly. Initialize and upgrade modes
are mutually exclusive, as are the two upgrade flags. Schema 1 and mislabeled
new evidence reject.

Terminal proofs are bounded to 32 records with a 64 KiB signed disposition per
record, also subject to the existing 12 MiB whole-journal limit. No proof is
evicted. Overflow preserves existing proof and cannot justify cleanup. Recovery
checks canonical signatures, scope, record order/uniqueness, cutoff, contract,
observation ordering and conflicts with both retained execution and the other
proof format. Existing file identity, locking and durability recovery apply.

## Validation

Passed locally:

```sh
go test ./...
go test -race ./internal/modelruntime
make lint
go test -tags=integration ./internal/integration -run '^TestStageTerminalHistoryCoversAllocatedUndeliveredRetry$' -count=1 -timeout=5m
git diff --check
```

The PostgreSQL test checks zero assignment-authority receipts for the allocated
retry, obtains the signed disposition through the authenticated Control handler,
configures Runtime topology independently from the prior assignment and database
registration, persists a floor and the new proof, then reads it after reopening
with the next Runtime epoch. The Runtime calls in this test use the local API,
not a new Runtime RPC.

Focused tests cover concurrent Prepare, persisted intent, missing floor, memory
state, invalid signatures and topology, conflicting signed queries and proof
formats, expiry, old-epoch absence, damaged journals, explicit migration,
checkpoint bounds, post-rename sync failure, lost reply and abrupt subprocess
exit without cleanup.

Linux arm64 verification uses the same selected Runtime tests in a non-root
container: UID/GID 65534, no network, read-only root and binary mounts, dropped
capabilities, no-new-privileges and a private `/tmp` tmpfs. Test image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
Selection: `^(TestTerminalNonAdmission|TestExecutionNonAdmission|TestDurableExecutionState)`.

## Authenticated RPC and complete-history collection

A subsequent increment adds `CheckpointStageTerminalNonAdmission` and
`InspectStageTerminalNonAdmission` to both Runtime and WorkerMember services.
The request binds a trusted current reader, signed terminal history and selected
allocation ID. Creation additionally requires the selected original residency;
inspection can replay through an alternate current profile or Runtime epoch.
The reply echoes the query digest while preserving the checkpoint's actual
original disposition, digest, allocation, member, cutoff, contract and time.

The member path authenticates the deterministic leader for every historical
allocation, verifies the target member identity digest, and forwards to a private
Runtime socket. Each hop clones requests and independently validates response
scope, canonical signatures, original proof identity, wrappers and decisions.
Malformed or canceled/late replies never become successful observations.

The complete exclusion collector accepts omitted execution envelopes and requires
terminal non-admission proof for those allocations on every member. Supplied
envelopes must still belong to signed history; explicit nil values reject.
All history and trusted readers are checked before any RPC. The collector first
inspects existing evidence and can create a checkpoint only after a valid accepted
read returns no proof. Required allocation counts come from signed history, not
the number of available envelopes. The separate drain-only collector still
requires every execution envelope and cannot reinterpret non-admission as drain.

Additional validation passed:

```sh
go test ./...
go test -race ./internal/modelruntime ./internal/stageworkeragent ./internal/stageworkermembertransport ./internal/stageauthority ./internal/modelruntimetransport
make lint
go test -tags=integration ./internal/integration -run '^(TestStageTerminalHistoryCoversAllocatedUndeliveredRetry|TestStageTerminalDispositionThroughAuthenticatedControl)$' -count=1 -timeout=5m
make generate-proto
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking --against '.git#ref=d73fe27'
git diff --check
```

Regeneration leaves generated files byte-identical. Real TCP/mTLS-to-UDS tests
cover unsigned allocations, missing floors, prior execution intent, nonleader
requests, lost replies and recovery after profile/epoch changes and expiry.
Malformed-response cases exercise both forwarding hops. Two-member UDS
collection combines one drain, one envelope non-admission and two terminal
non-admission checkpoints, without ever constructing the second execution
envelope. Recovery retains the original proofs; missing old-epoch evidence and
invalid reads remain unproven. The PostgreSQL tests remain local Runtime API
evidence; the transport and collector tests independently cover the RPC paths.

The same non-root Linux arm64 container configuration above passes these sets:

- Runtime: `^(TestTerminalNonAdmission|TestExecutionNonAdmission|TestDurableExecutionState|TestExecutionFloor)`.
- Worker: `^(TestTerminalNonAdmission|TestTerminalExecutionExclusion|TestTerminalExecutionDrain|TestAgentExecution)`.
- Member transport: `^(TestMemberTerminalNonAdmission|TestMemberExecution)`.

Versions remain database 90, Runtime journal 4, Worker journal 3 and launch/Fleet 2.

## Remaining work

The authenticated RPC and complete-history collector are now available as
explicit components. Pending historical Runtime writer recovery,
unknown input-writer recovery, complete retirement intent/results, default
assembly and bounded evidence reclamation remain unfinished. The default
`RetainScratchRetirer` stays active. No scratch deletion, sustained Worker
throughput, external driver containment, GPU validation or Production Gate is
established by this increment.
