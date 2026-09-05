# Durable assignment input writer completion

Local CPU/mock increment over `3054098`. Worker admission journal advances to
**3**, with explicit validated schema-2 upgrade. Runtime admission journal remains
**3**, launch/Fleet schemas remain **2**, and database schema remains **90**.
Production Gates remain **0/9**. No GPU or remote lab deployment was used.

## Reproduced Recovery Gap

Previously `AssignmentFloorInstallation.WaitInputWriters` only waited for an
in-process admission handle. After reopening the Worker journal, no such handle
existed, so it returned success even when the persisted input intent had no
writer-completion evidence. A regression reproduced that false success against
the previous commit.

`Release` still releases an in-process handle but cannot create durable evidence.
`AssignmentAdmission.CompleteInputs` separately persists
`vela-assignment-input-writer-drain-v1`, with a UTC observation time, under the
admission mutex. The checkpoint belongs to its record's immutable execution
identity and original Acquire lookup evidence. The live handle must still own
the current record. Completion neither changes its execution phase nor cancels
or releases any Runtime. Retrying completion preserves the original checkpoint.

The caller must join all input tasks and close their writable handles before
calling `CompleteInputs`, then perform no further input work under that handle.
The InputResolver interface explicitly requires the same lifecycle on every
return, including errors. The repository HTTPS and object-store resolvers perform
synchronous input work and close their file targets before returning. This is a
trusted implementation contract, not process containment for arbitrary custom
resolvers or protection against a hostile journal owner.

## Admission And Stream Integration

A retry must have completed its previous input invocation. `Begin` invalidates
that checkpoint durably before admitting the retry, so failure in the new
invocation cannot reuse the old proof. Unproven retained input records also
block higher-sequence assignments, including history from earlier software.
Direct `EnterRuntime` requires input completion after rechecking authority
freshness/floors. Existing Runtime recovery barriers retain their meaning.

The durable Stream persists input completion before Runtime entry. When Resolve
returns an error or Stop cancels it, completion is persisted after the resolver
actually returns and before releasing its handle. Only this local persistence
uses an uncanceled context; no RPC or new input work runs under it. A resolver
panic preserves unknown input state. Persistence failure propagates to the
assignment result and cannot become successful Runtime entry.

`WaitInputWriters` rechecks the live journal and installed floor, then requires
completion for every retained input record at or below the captured cutoff.
It waits without holding the admission mutex when a live input invocation can
still complete. Missing/released historical handles, invalid state binding and
a closed gate return errors. Input completion may precede releasing the admission
handle for Runtime work. This check is conservatively Worker-wide through the
cutoff; it does not omit older unresolved input records from other StageRuns.

## Journal Recovery

Schema 3 adds input completion to existing bounded records. The record-count
limit and no-eviction behavior remain unchanged. Snapshots copy the checkpoint.
Recovery validates its contract and nonzero UTC timestamp in addition to signed
execution history and filesystem bindings.

`AssignmentAdmissionConfig.UpgradeV2` explicitly permits schema-2 recovery. It
preserves journal/Worker identity, directory and lock ownership, original/latest
authority, execution phases, watermark, signed floor and pending history. It
creates no input checkpoint. Schema 2 rejects without opt-in; schema 1, invalid
authority signatures, new proof mislabeled schema 2, and simultaneous
initialization/upgrade reject. Opt-in is idempotent after schema 3 was published.
No CLI or default bootstrap/upgrade assembly is added by this increment.

After validating recovered state, the opener synchronizes the bound journal,
lock and containing directory before exposing it. Failed post-rename sync stays
sticky in the old instance. A later open can establish durability for a valid
published checkpoint instead of assuming file visibility proves durability.
Recovery cannot invent completion for a preexisting intent without a checkpoint.

## Verification

- Full `go test ./...`: PASS.
- Worker and Worker command race suites: PASS.
- `make lint`: PASS, 0 issues.
- Linux arm64 non-root selected Worker suites: PASS.
- `git diff --check`: PASS.

Tests cover live open input files, completion before handle release, idempotency,
snapshot isolation, retry invalidation, higher-sequence exclusion, unresolved
older records, malformed checkpoints, state binding, upgrade preservation and
rejection, directory sync failure and lost completion acknowledgement. Actual
subprocess exits compare missing versus persisted completion. They do not treat
process exit alone as drain proof.

Durable Stream tests verify completion before actual Runtime Prepare, input
retry after an error and reopen, Stop while a late writer is active, and panic
without false completion. Real loopback HTTPS tests cancel partial responses on
direct/unsolicited Stop, verify removal of the partial input file and recover
the same checkpoint after resolver return. Existing non-durable Stream tests
retain their original coverage.

```sh
go test ./...
go test -race ./internal/stageworkeragent ./cmd/vela-stage-worker-agent
make lint
env CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -o /tmp/vela-non-admission-linux/worker-input-drain.test ./internal/stageworkeragent
```

Linux image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
The run used UID/GID 65534, no network, all capabilities dropped, a read-only
root/binary mount and private `/tmp` tmpfs. Selection, with `-test.count=1`:
`^(TestAssignment|TestDurable|TestExecutionFloorCollection|TestTerminalExecutionExclusion|TestStreamAgentStop)`.

No protobuf, OpenAPI, SQL or deployment manifests changed. PostgreSQL integration
and GPU/load campaigns were not rerun; previous source-bound evidence remains
separate. These tests are not Launch Receipts.

## Remaining Work

Default `RetainScratchRetirer` remains active. Persistent terminal-retirement
orchestration must combine complete signed allocation history, input/Runtime
floors, these input checkpoints, complete typed Runtime proofs and retirement
intent/results before cleanup. No automatic scratch deletion or sustained
Worker throughput is established here.

Unknown input invocations left by crash/legacy recovery require independent
writer recovery before new admission. Pending historical Runtime writers, lost
intermediate renewal envelopes, durable sealed receipts, bounded checkpoint
reclamation, trusted default bootstrap and schema-1 migration remain open.
External asynchronous resolvers/drivers require independently validated task,
handle and descendant lifetime contracts.
