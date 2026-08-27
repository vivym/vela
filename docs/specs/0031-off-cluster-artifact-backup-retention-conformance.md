# Off-cluster Artifact backup retention and restore replay conformance

Date: 2026-08-27

Status: Repository conformance implemented, fixed-point reviewed, and committed
in `4f7aafe`.

This slice completes direct repository evidence for Acceptance Scenario 19 and
advances ADRs 0008, 0012, 0013, and 0015. It exercises separate PostgreSQL
roles, versioned primary and backup MinIO buckets, and a real PostgreSQL
dump/restore. It does not implement Artifact replication, perform live WAL PITR,
or create a Production Gate Launch Receipt.

## Scope

The conformance exercise proves:

1. Migration 00028 adds `PRIMARY` and `OFF_CLUSTER_BACKUP` storage tiers. Only a
   `COMMITTED` Artifact under Customer Content Deletion or automatic successful
   Artifact expiry receives a backup target; STAGING Artifacts and debug dumps
   do not.
2. The existing `vela_retention` role claims only PRIMARY targets. The separate
   `vela_backup_retention` role can claim, complete, or retry only backup
   targets through three exact SECURITY DEFINER functions.
3. Backup deletion lists and deletes every version and delete marker for the
   exact object key, is idempotent when the key is already absent, and fails
   closed above the 10,000-entry safety bound. A partial purge count survives a
   retry and the completed target records the cumulative purged version count.
4. A Content Deletion request forms its immutable receipt only after every
   PRIMARY and OFF_CLUSTER_BACKUP target completes. Per-target evidence fixes
   storage tier, storage outcome, attempt count, and purged version count.
5. A PostgreSQL snapshot taken after Content Deletion authority is durable but
   before any target completes can be restored after the physical objects were
   deleted. The existing Reconciler replays pending PRIMARY and backup targets,
   keeps both object copies absent, preserves the prompt tombstone, Charge, and
   actor attribution, and creates one complete immutable receipt.
6. The exact N-1 Reconciler completes only PRIMARY targets on schema 28 and
   cannot prematurely complete a request with pending backup targets. The
   current Reconciler then completes the new targets through the new role.

Migration Down refuses with SQLSTATE `55000` and constraint
`off_cluster_retention_requires_empty_evidence` once any backup target exists;
durable evidence cannot be silently contracted away.

## Public seams

The tests use established production boundaries:

- `retention.Service.AcceptContentDeletion` for durable deletion authority,
  prompt tombstone, and access revocation;
- `retention.Reconciler` with independent least-privilege PostgreSQL pools;
- `artifactstore.S3` against versioned MinIO for exact-version deletion,
  multipart abort, and all-version backup purge;
- PostgreSQL `pg_dump` and `pg_restore` inside the pinned PostgreSQL 17 test
  container; and
- current and exact fixed-commit N-1 control/retention binaries.

Direct database writes seed catalog prerequisites, make expiry deterministic,
and inspect immutable authority. They do not manufacture deletion completion,
receipts, object-store outcomes, or restored replay results.

## Recovery and replication boundary

The repository does not contain the component that copies a committed Artifact
to the off-cluster bucket. A backup deletion target and an `ALREADY_ABSENT`
outcome therefore do not prove that replication occurred. Production needs a
separate lifecycle receipt proving the copy, its failure-domain independence,
and the fence that prevents a late replication after deletion authority.

This release also has no external deletion journal. The restore test deliberately
selects a point after the protected Content Deletion request committed. Restoring
to a point before that authority can resurrect content and is not covered or
claimed safe. The runbook must keep traffic and Artifact replication stopped
unless the selected restore point is at or after every protected deletion
authority, or a future independently durable deletion journal can replay the
missing authorities.

## Verification

Required checks before the evidence commit are:

- focused package tests for all-version S3 purge, its safety bound, Reconciler
  dependency configuration, and `vela-control` configuration;
- focused PostgreSQL integration tests for Reconciler behavior and role
  preflight, plus versioned-MinIO deletion and PostgreSQL restore replay;
- exact N-1 retention and current startup compatibility tests;
- the complete integration suite;
- `make generate`, `make test`, `make lint`, `make test-cross`, and
  `make validate-deployment`; and
- a fixed-point P0-P2 review against the implementation base.

Container pulls may use the local registry proxy. Go build cache may be cleared
after large verification phases; the module cache remains retained.

## Evidence boundary

This slice advances Scenario 19 to direct repository conformance evidence. ADR
0015 remains `Partial` because real committed-Artifact replication, replication
versus deletion races, restore points before deletion authority, metadata and
financial lifecycle enforcement, legal holds, live production scratch
lifecycle, and Launch Receipts remain external work. Production Gates remain
`0/9 PASS`.
