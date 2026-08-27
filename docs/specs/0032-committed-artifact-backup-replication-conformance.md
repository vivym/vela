# Committed Artifact backup replication conformance

Date: 2026-08-27

Status: Repository conformance implemented in this change and fixed-point
reviewed; local commit receipt pending.

This slice completes direct repository evidence for the committed-Artifact copy
portion of Acceptance Scenario 19 and advances ADRs 0008, 0012, 0013, and 0015.
It adds an immutable PostgreSQL replication intent, a least-privilege runtime,
exact-version S3-compatible copying, and serialization with Content Deletion.
It does not create a Production Gate Launch Receipt or claim live provider and
network fault coverage.

## Contract

The repository proves the following contract:

1. Migration 00029 enqueues exactly one immutable replication intent only when
   an Artifact transitions to `COMMITTED`. STAGING, UPLOADED, VERIFIED, losing,
   expired, and debug-dump content do not create replication work.
2. The intent freezes the Artifact ID, PRIMARY object key and version ID, size,
   SHA-256, and content type. Terminal completion or cancellation evidence is
   immutable and migration Down refuses while any intent or evidence remains.
3. `vela_artifact_replication` is a separate NOLOGIN runtime role. It can only
   claim, complete, or retry through three SECURITY DEFINER functions; it has
   no direct table access and cannot invoke backup deletion authority.
   `vela_backup_retention` cannot invoke replication authority.
4. The Replicator reads the exact frozen PRIMARY version and conditionally
   creates the backup key. It rejects source-identity mismatches and a current
   backup object whose size, SHA-256, or content type conflicts.
5. If the conditional write succeeded but its response was lost, the retry
   validates the existing current backup object and records that exact backup
   version instead of creating another copy.
6. Claim tokens and expiry fence stale workers. A worker that loses its claim
   cannot complete or retry a replacement claim, and concurrent Replicators
   complete each intent once.
7. A Replicator holds the replication-row lock and PostgreSQL transaction across
   storage I/O. Content Deletion updates the same row: deletion before claim
   changes the intent to `CANCELED`; deletion during an active copy waits; a
   completed copy is then removed by the existing all-version backup retention
   target before the deletion receipt can complete.
8. The replication operation timeout is strictly shorter than its claim TTL.
   Tick, TTL, retry delay, timeout, and batch size are bounded configuration.

## Storage and identity

`vela-control` uses three independent storage authorities for this lifecycle:

- PRIMARY exact-version read credentials for replication;
- backup conditional-write and current-object-head credentials for replication;
  and
- backup list/delete credentials for retention.

The replication database login inherits only `vela_artifact_replication`; the
retention login inherits only `vela_backup_retention`. Both PRIMARY and backup
buckets require versioning. The backup object records its provider version ID,
size, SHA-256, content type, completion timestamp, attempt count, and last
attempt owner in PostgreSQL.

For a non-seekable exact-version source stream, `artifactstore.S3.PutIfAbsent`
uses the already frozen SHA-256 as the SigV4 payload hash and sends the same
digest as `x-amz-checksum-sha256`. This preserves streaming behavior and
service-side integrity checking without buffering an entire Artifact in memory
or a local temporary file.

## Migration and rollout

Migration 00029 backfills schema-28 `COMMITTED` Artifacts. An Artifact with an
existing Content Deletion target is backfilled `CANCELED`; another committed
Artifact is backfilled `PENDING`. Claim expiry is bounded to one hour, retries
to 24 hours, and terminal evidence cannot be updated or deleted.

The exact adjacent N-1 binary remains fixed at
`cd5f22dea259aa7188d52d8ab232522fa3ff8d67`. It runs against additive schema 29
without receiving the new replication role or environment variables. Only the
current binary opens the new role and starts the Replicator. This preserves the
expand-first boundary while migration 29 owns the backfill.

## Evidence

The integration suite uses real PostgreSQL plus separate versioned PRIMARY and
backup MinIO buckets to prove:

- no intent exists before Visible Completion commits the Artifact set;
- a newer current PRIMARY version does not replace the frozen source version;
- concurrent Replicators produce one completed row and one backup version per
  committed Artifact;
- a successful backup write with a simulated lost response recovers through
  current-object validation;
- deletion before claim cancels replication without writing a backup;
- deletion blocks while a copy transaction is active, cannot create a completed
  receipt during that window, and resumes after the copy commits; and
- copy-first deletion purges every backup version and delete marker before its
  immutable receipt completes.

Focused migration tests prove claim expiry, stale-token refusal, terminal
immutability, named SQLSTATE `55000` Down refusal, and schema-28 backfill.
Database-role tests prove direct-table and cross-function denial.

## Evidence boundary

The response-loss test replaces the response from a real successful MinIO write
with a transport failure. A later conditional retry observes
`ErrObjectAlreadyExists`, validates the current object, and completes the same
intent. The transaction-lock test models bounded process behavior while the
database connection remains alive. A real process or network failure could
allow a provider-side write to complete after the database connection has
failed; this repository does not convert that residual provider behavior into a
proven impossibility.

Production therefore still requires the exact deployed object-store policy,
network partition, process-kill, credential rotation, lag/SLO, restore, and
failure-domain receipts. Restore to a point before durable Content Deletion
authority also remains outside this slice. Production Gates remain `0/9 PASS`.
