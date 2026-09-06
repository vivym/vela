# Runtime journal validation before backend startup

Local CPU/mock increment over `dcec0fb`. Runtime and Worker journals remain
**4**, materialization journal **2**, database **90**, and launch/Fleet **2**.
Production Gates remain **0/9**. No GPU or remote deployment.

## Finding and behavior

`StartRuntimeServer` previously allocated every Runtime epoch and started every
backend before opening its configured durable execution journal. Regression
coverage reproduced two epoch allocations and two backend factory calls for an
AUX Worker before rejection of missing, corrupt, locked or wrong-scope state,
repeated initialization, or conflicting upgrade flags.

Configured durable startup now derives journal ownership from the validated
launch manifest and trusted floor membership before allocating epochs or
starting backends. It opens, validates and locks the journal at that point,
checks its binding before each Runtime startup, and transfers the same open
store to the Supervisor without closing or reacquiring the lock. Attachment
rechecks the started Services' ownership digest, configured directory and
current file identities/content before socket publication.

Journal scope no longer depends on constructed Services. The ownership digest
retains its existing serialized shape, sorted member/device ordering and
exclusion of residency/profile/Runtime epoch. Historical signature/scope checks
still restore only restrictions; new floor installation separately requires the
allocation's original local Runtime route. Existing journals are not rewritten
by ordinary startup, and pending historical execution still blocks readiness.

An already canceled startup does not initialize the journal. Startup failure
cancels the runtime context and attempts Service shutdown before closing the
preopened store. Successful rollback releases the journal for ordinary recovery;
initialization is never inferred from missing state or reused on that recovery.
After successful attachment, existing Supervisor admission/shutdown ownership
applies.

## Validation

Passed:

```sh
go test ./internal/modelruntime -run '^(TestRuntimeServer|TestStartRuntimeServer)' -count=1
go test ./...
go test -race ./internal/modelruntime ./internal/stageworkeragent ./internal/modelruntimetransport
make lint
go vet -tags=integration ./internal/integration
go test -tags=integration ./internal/integration -run '^(TestStageTerminalHistoryCoversAllocatedUndeliveredRetry|TestStageTerminalDispositionThroughAuthenticatedControl|TestStageTerminalHistoryPreservesHistoricalRuntimeScopesWhileDraining)$' -count=1 -timeout=3m
git diff --check
```

New server regressions require:

- Zero epoch/backend calls and no socket for the six preexisting journal faults.
- A competing server cannot allocate epochs or start a backend from inside either
  epoch allocation or either backend factory, or after Supervisor attachment.
- Directory, state inode, lock inode and content replacement during either AUX
  backend's warmup stops startup before socket publication. Replacement during
  the first backend also prevents allocating the second Runtime's epoch.
- Epoch failure, backend failure and cancellation roll back the already started
  backend, publish no socket, and permit reopening the journal after clean shutdown.
- Cancellation before startup leaves the bootstrap directory empty.
- A journal created through the standalone Supervisor reopens through the actual
  Runtime server with new epochs. Its bytes, retained floor and pending execution
  evidence survive; public UDS calls cannot advertise readiness or bypass the floor.

Linux arm64 selection
`^(TestRuntimeServer|TestStartRuntimeServer|TestDurableExecution|TestExecutionFloor|TestTerminalNonAdmission)`
also passes as UID/GID 65534 with no network, a read-only root/binary mount,
all capabilities dropped, no-new-privileges and a private `/tmp` tmpfs. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

## Remaining scope

This applies only when `RuntimeServerConfig.ExecutionFloor.State` is configured.
Default command assembly still omits it. Independent first-use provisioning,
default durable Worker/Runtime wiring, historical writer/receipt recovery and
bounded checkpoint reclamation remain open. No new bootstrap API or reusable
initialization environment flag is introduced.

The pre-attachment rollback closes the journal even if backend shutdown reports
an error. These tests establish successful cleanup and lock transfer, not
containment of a backend that fails to stop, crashes with descendants, or retains
external asynchronous writers/handles. That failure requires a separate durable
containment/recovery contract before production ownership can be claimed.
