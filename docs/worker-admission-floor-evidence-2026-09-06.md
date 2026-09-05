# Worker admission floor and bootstrap repair

Local CPU-only increment over `491047c`, database schema 90. Production Gates
remain **0/9**. No GPU, remote deployment or new database/load campaign was used.

## Reproduced Recovery Failure

`TestAssignmentAdmissionRecoveryRejectsCompleteStateLoss` admitted and closed an
execution, shut down the journal, retained all three original directories, and
recreated empty state/input/output directories at the same paths. The previous
constructor automatically initialized a new journal and accepted the old signed
execution again: `old execution admission error=<nil>`.

`AssignmentAdmissionConfig.Initialize` is now explicit. Default recovery opens
existing state only; missing/replaced roots cannot create another journal.
Explicit bootstrap still rejects existing state or content and uses exclusive
lock creation. Repeated initialization cannot overwrite an existing journal.
This is an explicit component API, not an independently issued provisioning
grant. Default command wiring must never retain a reusable `Initialize=true`.

## Signed Input Cutoff

`FileAssignmentAdmission.InstallExecutionFloor` verifies a fresh signed terminal
disposition against complete trusted Worker/member/device/runtime bindings,
including opaque member subset digests. Every historical route must match;
missing, ambiguous or mismatched scope fails before persistence. The same journal
stores the monotonic cutoff and deterministic signed witness before returning.
Begin, EnterRuntime and ObserveRuntimeAuthority reject sequences through that
cutoff. Lower requests return the retained higher cutoff, bound to the current
request digest, without replacing its stronger witness.

The journal format is now schema 2. Schema-1 journals fail closed and must be
preserved for a separately validated migration; deleting them or reinitializing
the same Worker is not a migration. Recovery checks signature, cutoff, canonical
encoding and trusted topology, even after envelope expiry or resident profile/
Runtime epoch changes. Restoring a restriction does not authorize historical
execution. Existing Runtime-entered records remain pending for recovery.

`StreamAgent.InstallExecutionFloor` snapshots the request, persists local input
exclusion, and then collects every Runtime's durable acknowledgement. Its result
keeps input and Runtime checkpoints separate. Partial RPC failure preserves the
local restriction; retry reconfirms every Runtime. The captured input handle can
be awaited with `WaitInputWriters`, which waits for the admitted resolver's
Release, not backend or arbitrary filesystem writers. Installation does not
cancel downloads, stop compute, unload resident models or permit deletion.

## Validation

- Full `go test ./...`: PASS.
- Related-module race suite (Worker, member/Runtime transports, ModelRuntime and
  production Worker command): PASS; final Worker package race rerun also PASS.
- `make lint`: PASS, 0 issues.
- `make test-cross`: PASS for Linux amd64 compilation only.
- Linux arm64 non-root execution: PASS with the command below.

Tests cover all-state loss, explicit bootstrap/recovery, installed cutoff with
unseen later assignments, delayed successful resolution before Prepare,
renewal rejection, retained Runtime recovery intent, lower-cutoff replay,
expired witness recovery, changed Runtime profiles/epochs, invalid signed scope,
modified/missing witnesses, cancellation, persistence failure and caller mutation.
The real-UDS two-member test now combines the Worker journal with independent
Runtime journals, response loss, full retry and restart. Running fake backends
stay RUNNING and resident after installation; this is not a writer-drain proof.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-worker-floor-linux.test ./internal/stageworkeragent
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-worker-floor-linux.test,dst=/worker.test,readonly \
  --entrypoint /worker.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^(TestAssignmentFloor|TestAssignmentAdmission|TestWorkerFloor|TestDurableStream|TestExecutionFloorCollection)' \
  -test.count=1
```

Default trusted bootstrap and command configuration, historical read-only
inspection, execution-specific backend writer drain, terminal retirement journal,
pending-record reclamation and automatic recovery remain open. The process-local
input-handle wait does not prove that a previous crashed process has no writers.
Filesystem sync tests are not physical power-loss or hostile-owner rollback
evidence. No Production Gate or full scratch-lifecycle completion is claimed.
