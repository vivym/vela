# Lab Database Quiescence and Restore

`vela-lab-recovery` exercises the PostgreSQL layer without GPUs. It does not
replace a running database, restore object storage, replay external webhooks or
Invoices, or produce a Production Gate receipt. Keep the source database and
all object-store data until their separate recovery requirements are satisfied.

## Prerequisites

- Apply the release role bootstrap and migrations through `00072` or later.
- Provision an operator login belonging only to `vela_recovery` and set
  `VELA_RECOVERY_DATABASE_URL` through the existing secret-loading mechanism.
- For capture, provision `VELA_RECOVERY_BACKUP_DATABASE_URL` for the **same
  source database**. It needs full dump/catalog read access, PostgreSQL system
  identity access, temporary-table creation, and a shared lock on the recovery
  gate. This separate backup connection is not an application workload login.
- Install PostgreSQL 17 `pg_dump` and Docker. Restore creates a fresh PG17
  container with `--network=none`, no published ports, and no host mounts. It
  accepts no target connection string, database name, or existing volume.
- Use a protected parent directory. Capture creates a new `0700` evidence
  directory and `0600` files; it refuses an existing destination. Raw database
  dumps can contain Customer Content and must never enter Git.

## Procedure

Generate and retain one operation UUID. Reuse it after a lost response or an
interrupted drain; a new UUID cannot take over an already closed gate.

```sh
operation=$(uuidgen | tr '[:upper:]' '[:lower:]')
go run ./cmd/vela-lab-recovery quiesce --operation "$operation" --timeout 15m
go run ./cmd/vela-lab-recovery status
go run ./cmd/vela-lab-recovery capture --operation "$operation" \
  --output /absolute/private/evidence/new-snapshot \
  --roles db/bootstrap/roles.sql --pg-dump /absolute/path/to/pg_dump
go run ./cmd/vela-lab-recovery restore-drill \
  --output /absolute/private/evidence/new-snapshot \
  --roles db/bootstrap/roles.sql --postgres-image postgres:17-alpine
```

The default image is for local mock validation. A pinned image reference may
be supplied; the actual local image digest is recorded in the restore receipt.
Source and target must both report PostgreSQL 17, with different system
identifiers. The target container and its anonymous volume are removed when the
drill finishes, including failure paths.

Closing Admission takes the same row lock as the `jobs` INSERT trigger, waiting
for earlier in-flight inserts to commit or roll back. It never takes a Project
or Job lock while holding that gate. New requests receive HTTP `503` and
`Retry-After`; existing idempotency results remain readable. Existing execution
can drain normally. A timeout leaves Admission closed and emits no success
receipt. `status` reports the owning operation, exact generation, and remaining
authority counts without exposing request content.

Quiescence requires zero nonterminal Jobs, Attempts, StageRuns, StageAttempts,
active StageLeases, allocations, materialization leases, transfer tickets,
finalization claims, execution/finalization pins, edge credits, and unconsumed
storage reservations. PostgreSQL issues an immutable receipt binding these
counts to its system identifier, database name/OID, durable database UUID,
authenticated session user, gate generation, schema version, and clock.

Capture rechecks the source identity and inventory, holds a shared closed-gate
lock, and exports a `REPEATABLE READ` snapshot used by `pg_dump --snapshot`.
`snapshot.json` binds the dump SHA-256 to that snapshot, every public table's
row count and content fingerprint, the schema/ownership/grants/RLS/constraint/
function fingerprint, and the actual operator binary and role-bootstrap bytes.
CHECK expressions are normalized through PostgreSQL's parser because a normal
dump/restore can flatten equivalent Boolean groups. Effective default ACLs are
normalized as well. Neither normalization drops semantic checks or privileges.

Restore verifies these bytes before starting a new isolated container. It
bootstraps roles without source passwords, performs `pg_restore --exit-on-error`,
compares every captured public table and catalog fingerprint, verifies unbound
tenant reads return no Jobs, and confirms the original source operation cannot
reopen Admission in the restored instance. A passing `restore-receipt.json`
explicitly declares `DATABASE_ONLY` and `production_gate=false`.

## Reopen and Evidence Limits

To resume the **original** source after the operation, take the exact generation
from `status` or its quiescence receipt:

```sh
go run ./cmd/vela-lab-recovery reopen --operation "$operation" --generation 2
go run ./cmd/vela-lab-recovery status
```

`2` is only an example; never infer the generation. An old operation, wrong
generation, or restored system identifier cannot reopen the gate. Reopening is
explicit and never occurs as capture/drill cleanup.

The snapshot is a consistent database image, not a freeze of every background
writer or S3 object. Artifact exact versions, deletions across storage tiers,
Outbox/Invoice/webhook replay, credential rotation, and actual service recovery
must be validated separately. The isolated target remains closed and is deleted;
this procedure does not demonstrate production RPO/RTO or approve replacement
of retained database/object-store data by itself.

Schema 88 adds a separate execution-order recovery condition. A restored database
must not resume assignment signing against surviving Runtime epochs when PITR or
data loss may have rolled back allocation sequences. Fence/advance and
re-register every affected Runtime epoch first; an old signed authority that was
never delivered can have a higher number than both the restored database and
the Runtime's observed watermark. The isolated drill does not perform that
service recovery, and database quiescence alone does not prove local backend or
descendant processes have stopped. See [Ordered physical execution](../runtime-execution-order-2026-09-05.md).

Repository checks:

```sh
go test ./internal/recovery ./hack
go test -tags=integration ./internal/integration -run '^TestRecovery' -count=1
```
