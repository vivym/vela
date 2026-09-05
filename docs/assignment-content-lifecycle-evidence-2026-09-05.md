# Stage assignment content lifecycle

Date: 2026-09-05. Local schema 90 follows committed schema-89 history reader
`4560b49`. This is CPU/PostgreSQL evidence, not a production rollout or a
Production Gate receipt. Production Gates remain 0/9.

## Confirmed defect and resulting contract

The public regression acquired a StageAssignment, terminalized its StageRun,
accepted Customer Content deletion through `retention.Service`, reconciled it
to COMPLETED, and replayed the same Acquire command. Before this change, the
replay still returned the full assignment. Its execution parameters and root
download URLs survived in immutable `stage_worker_acquire_results.assignment_wire`.
Deleting `jobs.request_content` alone did not delete this second content copy.

Schema 90 separates two lifetimes:

- `stage_assignment_authority_receipts` retains only the canonical original
  signed StageAuthority, its digest, the original delivery digest, and retained
  Job/lease identity. It does not retain execution parameters, root URLs, or
  unknown authority fields. The signature and original delivery hash are never
  regenerated from a newly signed envelope.
- The acquire result retains its immutable ASSIGNMENT outcome and delivery
  digest. Its only permitted update clears `assignment_wire` and sets
  `assignment_retired_at` once. This transition cannot be reversed.

The active Acquire replay returns the exact original delivery. After customer
deletion, request-content expiry, or Job metadata deletion, the same command
returns REJECTED / `ASSIGNMENT_CONTENT_DELETED`; it never schedules another
execution. Terminal history uses the retained authority and continues checking
the full original signature, caller identity, lease and allocation history.
The SQL history response is schema 2 with `authority_wire`; the request remains
schema 1. The Go reader also understands the old response during explicit
historical-schema testing, while current control startup requires schema 90.

This does not recall a delivery already returned before deletion, authorize
Worker scratch deletion, or establish physical erasure of old PostgreSQL pages,
WAL or backup copies. Those stores retain their separate lifecycle contracts.

## Concurrency

New completion and legacy backfill take the retained Job root `FOR KEY SHARE`,
then the live Job `FOR SHARE`, then the acquire intent and result. This order is
compatible with metadata expiry's root `FOR UPDATE` followed by Job deletion.
The coordinator owner receives only the additional `UPDATE(id)` column privilege
needed for an explicit root row lock; runtime roles receive no direct table grants.

The root ordering was tested before the repair: backfill held the live Job while
waiting for a root owned by the expiry side; that side could not acquire the Job
lock (`55P03`). The regression then verifies that the expiry side can acquire
the Job while backfill waits, and that backfill finishes after lock release.

Customer deletion and metadata deletion retire delivery within the Job
transaction. Completion that started before deletion but reaches the final
database transaction afterward stores only the historical outcome and authority;
the content is cleared before commit. Replay checks expiry using
`clock_timestamp()` after acquiring the Job lock, including when the deadline
passes while it waits. Failed transactions cannot publish a partial receipt.

## Nonempty upgrade

1. Apply schema 90 with the normal migration identity. Up/Down acquire all named
   authority table locks with NOWAIT before changing anything. An active writer
   causes a retryable migration failure instead of waiting with a partial lock
   set. Retry after the existing maintenance/quiescence process has drained it.
2. Schema 90 permanently rejects schema-1 ASSIGNMENT completion, including an
   old Acquire that spans several transactions. It creates a finite worklist of
   existing delivery rows. These pending rows prevent control startup, lab
   bootstrap continuation, content deletion and deletion-task completion.
3. Run the backfill with a dedicated login inheriting only
   `vela_assignment_history_migration`, using the historical **public verifier**
   keyring in the existing StageAuthority format. The keyring must be derived
   with Vela's key derivation, not a raw Ed25519 interpretation of signing bytes.

   ```sh
   go run ./cmd/vela-assignment-history-migrate \
     --database-url-file /absolute/private/migration-database-url \
     --verifier-keyring-file /absolute/private/stage-authority-verifiers.json \
     --batch-size 20 --timeout 5m
   ```

4. Resume startup after the worklist is empty. A retry reads remaining candidates;
   exact concurrent receipts are accepted without rewriting them. The CLI's
   processed count counts observed candidates, not distinct signed receipts.

Each batch is limited to 1..100 records, each original delivery to 4 MiB. The Go
reader releases its candidate query before writing, so a one-connection pool
works. It accepts expired signatures only for historical identity verification,
rejects future-issued authority, and clears byte buffers after use. Normal valid
backfill preserves the original delivery bytes, including unknown outer delivery
fields, while rejecting unknown fields anywhere inside the retained authority.

Missing keys, malformed protobuf, invalid signatures, unknown authority fields
and retained-identity mismatches fail without changing the pending content.
The explicit `--retire-unverifiable` option instead records an immutable digest
and bounded reason, clears those delivery bytes, and removes the pending item.
It creates no signed authority receipt. Only the exact mapping constraint may
classify a database failure this way; network or other database failures stop
the operation. A missing wire that schema 89 admitted through its nullable CHECK
is retired during Up with `MISSING_ASSIGNMENT_WIRE` and SHA-256 of empty bytes;
that digest is explicitly a missing-payload sentinel, not recovered evidence.

Already-deleted or expired Jobs are retired during valid backfill. A legacy
deletion task cannot newly report COMPLETED while any delivery remains unmapped,
even when its Job was already tombstoned before the upgrade. Once authority
receipts or retirement records exist, Down refuses to remove the new contract.
An empty Down/Up restores the exact old function bodies and grants.

## Local verification

Public integration coverage includes active replay; completed Customer deletion;
request-content expiry; Job metadata expiry; late first completion after deletion;
post-lock expiry; valid nonempty backfill with expired historical signatures and
one connection; already-deleted legacy data; malformed, missing-key, missing-wire
and mismatched-identity legacy records; strict versus explicit retirement; writer
fencing; private helper/role boundaries; retained evidence immutability; and
guarded Down/Up. Terminal-history tests also cover renewal, unseen allocated
retries, incomplete history and distinct historical Runtime scopes.

The initial content deletion test failed before implementation. The root-lock
test also failed before its lock-order repair. Local results:

| Check | Result |
| --- | --- |
| Content deletion before repair | FAIL: completed deletion returned the original assignment |
| Root/Job ordering before repair | FAIL: expiry-side Job lock rejected with `55P03` |
| Repaired root/Job ordering | PASS, 4.853 s |
| Assignment lifecycle, all terminal-history variants, exact signed original, database role confusion and migration rollback | PASS, 64.298 s |
| Assignment lifecycle with `-race` | PASS, 48.606 s |
| Exact original signature and substituted SQL candidate with `-race` | PASS, 6.600 s |
| Final legacy backfill and active-writer Up/Down checks | PASS, 6.462 s |
| Earlier assignment/retention/migration compatibility batch | PASS, 103.276 s; bootstrap PASS, 11.799 s |
| `go test ./...` | PASS |
| `make lint` | PASS, 0 issues |
| `make generate` | PASS; only the schema-90 SQL models changed |

The earlier compatibility batch preceded the additional root-order repair.
The subsequent lifecycle/role batch includes that repair and the active-root
and active-deletion-table migration checks. Logs are retained under
`/tmp/vela-schema90-*.log`; the relevant commands are also recorded in the
[validation ledger](mock-hardening-validation-2026-09-05.md).

## Remaining work

The signed terminal disposition, persistent Worker input-admission gate,
complete writer drain and restart-safe local retirement journal remain open.
This change supplies retained original authority for those protocols and closes
the control database delivery-content copy. It does not close terminal scratch
retirement or supersede the schema-88 CPU load and CNPG campaign receipts.
