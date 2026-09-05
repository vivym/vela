# Stage Worker Scratch Retirement

## Ownership Contract

Production `vela-stage-worker-agent` explicitly selects
`attempt-owned-filesystem-scratch/v1` through
`MaterializationConfig.OutputOwnershipContract`. Its filesystem scratch
retirer requires every output locator to start with the manifest lineage's
`<StageAttemptUUID>/`. Inputs belong to `stage-runs/<StageRunUUID>/`.

This is a production filesystem ownership contract, not a change to the generic
`LocalOutputManifestV1` parser or materialization authority issuer. Generic v1
locators such as `outputs/encoder.bin` remain valid without filesystem scratch
retirement. They do not prove unique ownership of a reusable output pathname.

The StreamAgent validates this contract before requesting materialization
authority or publishing output. If a runtime produces a nonconforming locator,
the local sealed receipt and payload remain in the recovery journal and scratch;
the operation fails before L2 publication or COMMIT. The driver must satisfy the
selected ownership contract before production use. Do not rename signed manifests
or discard their journals to bypass this check.

## Confirmation and Recovery

For successful materialization, the worker receives an accepted or replayed
COMMIT response, durably records `COMMITTED`, then retires the completed
StageRun's inputs and the exact output named by its manifest.

For source loss, the worker receives an accepted or replayed SOURCE_LOST response,
durably records `SOURCE_LOST`, then retires only the old StageAttempt output.
The same StageRun can retry, so its input namespace remains available. A journaled
SOURCE_LOST request continues that operation even if the old source reappears.

In both cases, journal deletion follows successful local retirement. Control
response loss or failure to persist confirmation preserves scratch. Cleanup
failure or cleanup response loss preserves the confirmed recovery record. A
confirmed record retries local cleanup after restart without requiring a fresh
control request, including after its materialization authority expires.

Before the first COMMIT or SOURCE_LOST request, its stable canonical command ID
is persisted together with the exact timestamp or source-loss evidence. Retries
and process restarts use that same ID and payload. A timed-out transport exchange
closes its Connect stream before retrying; request IDs remain unique within each
stream. If an ACK was received but journal persistence failed, the existing
same-stream duplicate rejection forces reconnect before durable command replay.

Retirement binds each directory component by inode. Recursive input removal is
confined to the exact StageRun handle, and output removal uses one leaf name
relative to its bound parent. Parent-path replacement cannot redirect payload
deletion into another StageRun or StageAttempt.

## Journal Compatibility

New workers write materialization journal `schema_version=2`. The new reader
accepts schema 1 records as unconfirmed and schema 2 records with or without a
confirmed disposition. A schema 1 record claiming the new confirmation field is
rejected. Existing generic v1 output manifests are not rewritten during recovery.

Reading an old record does not recover an unknown historical command ID. An
unconfirmed legacy record with `committed_at` or source-loss evidence but no
original command ID returns `ErrLegacyMaterializationCommandIdentity` and remains
intact. It needs authoritative reconciliation of the original control command;
the worker does not guess the old transport-generated UUID. If no COMMIT or
SOURCE_LOST request was prepared, the new worker can persist a new stable ID
before its first send. A previously confirmed disposition can finish local
cleanup without a historical command ID.

Old workers reject schema 2 records. This is forward recovery compatibility,
not binary downgrade compatibility. Roll forward using the new worker with its
existing journal. To roll back to an old worker, first drain pending records with
the new worker and verify the journal is empty. Do not erase confirmation fields,
change the version marker, or delete pending records for rollback. Drain legacy
generic-locator records with their compatible worker before adopting the new
production filesystem contract. Different worker generations must not operate
the same journal concurrently.

## Remaining Lifecycle Boundary

The schema-87 public-gRPC probe reproduced Prepare/Start with the same still-valid
authority after both Seal and STOPPED. Both paths returned RUNNING. Consequently
the confirmation and inode-bound deletion behavior above is implemented, but it
does not yet exclude a delayed runtime writer. The successful 64-wave campaign
did not inject those duplicates. See the
[Checkpoint](../schema87-validation-checkpoint-2026-09-05.json) and
[Retirement Design](../terminal-scratch-retirement-design-2026-09-05.md).

No mtime-based or generic TTL/quota sweeper is added. Committed input files from
failed, canceled or expired StageRuns require an explicit control-plane terminal
retirement disposition plus proof that compute has stopped and no recoverable
materialization remains. A cancellation acknowledgment alone is insufficient.
Every retirement path also needs persistent prevention of future Resolve/Prepare/
Start calls, drain of already admitted writers, and a Runtime restart barrier.
Until those conditions hold, successful-load scratch measurements do not
establish adversarial cleanup safety or bounded scratch across failure campaigns.

These CPU/mock checks do not establish real GPU readiness or Production Gate
completion.
