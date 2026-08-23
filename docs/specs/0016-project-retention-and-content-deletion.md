# Project Retention And Content Deletion

Date: 2026-08-24

Status: Implementation target for Slice 16. This specification defines
repository-verifiable control-plane and Artifact Store behavior. It does not by
itself satisfy a Production Gate or prove deletion from Worker disks, debug
paths, or off-cluster backups.

## Goal

Make the launch Retention Policy a versioned Project choice and lock the policy
used by every Job during Admission. Enforce request-content and Artifact expiry
through a PostgreSQL-authoritative reconciler, and provide an idempotent,
Project-scoped Content Deletion API that immediately removes logical access and
asynchronously deletes exact object versions within a fixed 24-hour deadline.

Content Deletion removes Customer Content. It does not remove the Job, Attempt,
PricingSnapshot, CreditReservation, Charge, Invoice reference, immutable
ArtifactSet manifest, checksum, safe audit evidence, or any other non-content
record that has a longer required retention period.

## Governing Decisions

- ADR 0003: deletion cannot rewrite the historical Visible Completion or its
  Charge.
- ADR 0004 and ADR 0007: policy and deletion interfaces are Project-scoped and
  preserve Organization Isolation through PostgreSQL RLS and composite keys.
- ADR 0005 and ADR 0006: a Human `ProjectAdmin` may manage policy and Content
  Deletion. A Service Principal receives deletion authority only through the
  explicit `content_deletion:manage` scope; that scope grants no Artifact read.
- ADR 0008: deleted request content becomes an irreversible tombstone and no
  Customer Content enters deletion errors, audit payloads, or logs.
- ADR 0011, ADR 0026, and ADR 0027: deleting content never refunds or removes a
  Charge. Deletion of an active Job first wins the existing Customer
  Cancellation authority, including the existing Billable Start boundary.
- ADR 0013: migration 00016 is additive on the up path and preserves exact N/N-1
  database/control behavior during rollout.
- ADR 0015: request content defaults to 30 days; successful Artifact retention
  is selected from 7, 30, or 90 days; early Content Deletion completes
  asynchronously within 24 hours.
- ADR 0020: the shortest launch retention is longer than the accepted Job
  lifecycle ceiling. Admission fails closed if a configured policy would expire
  request content before `job_expires_at`.
- ADR 0029: repository tests are implementation evidence only. Production Gates
  remain `0/9 PASS` without versioned Launch Receipts.

## In Scope

1. Seed immutable ACTIVE Retention Policy revisions for successful Artifact
   retention of 7, 30, and 90 days. Every revision fixes:
   - request content: 30 days from Admission;
   - successful Artifacts: 7, 30, or 90 days from Visible Completion;
   - incomplete objects/uploads: at most 24 hours;
   - terminal scratch: at most 24 hours;
   - opted-in debug content: at most 72 hours;
   - non-content Job/Attempt metadata: 365 days;
   - financial and settlement audit records: 2557 days, subject to a longer
     statutory hold outside this slice.
2. Add one current Retention Policy revision to each Project. `artifact-30d-v1`
   is the migration default. A Human `ProjectAdmin` can read and change the
   Project choice by stable option (`7`, `30`, or `90` days); the change affects
   only later Admissions and records exact Principal/session attribution.
3. Admission locks the Project row and copies the policy revision ID and all
   relevant durations into the immutable Job snapshot. Request-content expiry
   is computed from PostgreSQL transaction time. Visible Completion derives
   ArtifactSet, Artifact, and access-grant expiry from the Job snapshot instead
   of a Go constant.
4. Add `POST /v1/projects/{project_id}/jobs/{job_id}/content-deletion-requests`
   with a required Project-scoped `Idempotency-Key`, and
   `GET /v1/projects/{project_id}/content-deletion-requests/{request_id}`.
5. A customer request targets both request content and every Artifact or
   incomplete upload belonging to the Job. Acceptance and any required
   Customer Cancellation commit in one transaction. Replays return the same
   request; reuse of the key for another Job returns `409`.
6. Acceptance immediately:
   - replaces `jobs.request_content` with the fixed tombstone
     `{"deleted":true}` and records a one-way deletion timestamp;
   - revokes any Artifact access grant;
   - prevents later Visible Completion from publishing content for the Job;
   - creates an immutable deletion deadline exactly 24 hours after the
     PostgreSQL acceptance time;
   - preserves the original request hash and every non-content commercial or
     lifecycle record.
7. For a nonterminal Job, the same transaction invokes the existing Customer
   Cancellation authority. QUEUED/ASSIGNED/RETRY_WAIT jobs cancel without a
   Charge. RUNNING/FINALIZING jobs use the existing Billable Start and Charge
   rules, fence the current Attempt, and become `CANCELING` until stop evidence
   arrives. A deletion request never invents a second cancellation path.
8. PostgreSQL owns deletion request, claim, retry, target, and receipt state.
   Claims are leased, crash-recoverable, and safe across multiple replicas.
   Storage failures retain a bounded sanitized error and schedule retry; passing
   the 24-hour deadline is observable and does not stop retries or silently mark
   the request complete.
9. The reconciler handles each storage identity idempotently:
   - known object versions are deleted with both object key and exact version;
   - a STAGING object with no recorded version is discovered by its unique key,
     then the discovered exact version is deleted;
   - all incomplete multipart sessions below the Job's immutable object prefix
     are aborted;
   - `NoSuchKey`, `NoSuchVersion`, and `NoSuchUpload` count as already absent;
   - success is recorded only after the adapter operation returns successfully.
10. Request completion atomically marks covered Artifact rows `DELETED`, covered
    ArtifactUpload rows `ABORTED` or `EXPIRED`, and writes an immutable receipt.
    ArtifactSet and ArtifactSetItem history remains immutable. The receipt lists
    only IDs, action kinds, attempt counts, timestamps, and storage outcomes; it
    contains no signed URL, prompt, credential, or object body.
11. The automatic retention reconciler creates internal, idempotent deletion
    work independently for:
    - request content whose Job snapshot deadline has arrived; and
    - successful Artifacts whose ArtifactSet retention deadline has arrived.
    A 90-day Artifact choice never extends the 30-day request-content deadline,
    and a 7-day Artifact choice never shortens request-content retention.
12. Existing 15-minute signed URLs do not change retention. Immediate grant
    revocation prevents new URL issuance; object deletion makes an already
    issued URL fail once storage reconciliation reaches that exact version.

## Public Contracts

### Retention Policy

`GET /v1/projects/{project_id}/retention-policy` returns the current immutable
revision and its durations. `PUT` accepts only:

```json
{"artifact_retention_days": 7}
```

The accepted values are `7`, `30`, and `90`. A repeated selection returns the
same current revision without creating a duplicate audit event.

### Content Deletion

The POST response is `202 Accepted` and includes:

```text
request_id
project_id
job_id
state: PENDING | IN_PROGRESS | RETRY_WAIT | COMPLETED
requested_at
deadline_at
completed_at (nullable)
overdue
```

`overdue` is derived from PostgreSQL time when `state != COMPLETED` and
`deadline_at` has passed. It is not a persisted success/failure override.

The GET projection additionally includes safe aggregate target counts and the
last sanitized operational error. It never exposes object keys, object version
IDs, checksums, prompt content, Artifact media facts, signed URLs, or raw storage
errors.

### Error Mapping

- `400`: malformed UUID, Idempotency-Key, or retention option.
- `401`: absent, invalid, expired, or revoked credential/session.
- `403`: Principal lacks the exact Project authority.
- `404`: Project, Job, or deletion request is outside the authenticated Project.
- `409`: Idempotency-Key was already used for a different deletion target.
- `202`: a new or replayed Content Deletion request is durably accepted.

## Database And Authorization Invariants

- Retention Policy revisions are immutable and only one seeded revision for each
  7/30/90 stable option is ACTIVE.
- Project policy changes are serialized on the Project row. Existing Job policy
  snapshots and expiry timestamps cannot change.
- The request tombstone transition is one-way and can occur only through the
  narrow deletion owner function. No runtime role can restore or replace it.
- Customer deletion requests have one immutable actor, target Job, class set,
  request hash, acceptance time, and deadline. Runtime state may advance only
  through the claim/retry/complete functions.
- `vela_retention_request` is NOLOGIN, non-superuser, non-BYPASSRLS and owns no
  tables. It can establish only retention/deletion request context and execute
  the narrow public request functions.
- `vela_retention` is NOLOGIN, non-superuser, non-BYPASSRLS and owns no tables.
  It can execute only the claim, target receipt, retry, completion, and automatic
  expiry functions.
- `vela_retention_owner` is NOLOGIN and BYPASSRLS, owns the retention/deletion
  tables and functions, and is never granted to a login or another runtime role.
- Neither retention runtime role can read request content, Artifact object
  metadata outside a claimed target, Credential proof/digest, Human OIDC proof,
  Webhook secret, or billing mutation state.
- `content_deletion:manage` can request and read deletion status but cannot issue
  signed URLs or read Artifact metadata. ProjectAdmin policy authority is not
  issuable as a Service Credential.
- Cross-Project and cross-Organization request reads/mutations return no target
  and cannot be distinguished from absence.

## Compatibility

- Migration 00016 uses nullable/backfilled columns before setting required
  constraints, does not remove or reinterpret an existing enum label, and keeps
  all pre-00016 API contracts valid.
- An N-1 control binary can admit and visibly complete a Job after migration
  00016 using the default policy values supplied by PostgreSQL.
- The N control binary fails startup against schema 00015 because its dedicated
  retention roles/functions are absent; it never silently runs without deletion
  enforcement.
- Down migration is allowed only when no Content Deletion request, target,
  receipt, Project policy audit event, or non-default policy selection exists.
  It restores the previous Admission and Human role-scope functions exactly.

## Required Evidence

- Unit tests prove S3 exact-version delete input, absent-object idempotence,
  incomplete multipart abort, bounded retry scheduling, and no sensitive error
  projection.
- Production HTTP handler integration proves ProjectAdmin and explicit Service
  Principal deletion authority, lack of Artifact read, Project isolation,
  idempotency replay/conflict, immediate tombstone/revocation, and GET status.
- Integration proves queued and active Job deletion use existing cancellation
  and Charge rules, and completion/cancel/deletion races preserve one authority.
- Integration proves 7/30/90 Admission snapshots, policy changes affecting only
  later Jobs, request-content/Artifact independent expiry, and no hard-coded
  30-day Visible Completion path.
- Integration with the S3-compatible test store proves exact versions become
  unreadable, incomplete multipart sessions are aborted, crash claims recover,
  storage failure retries, and completion writes one immutable receipt.
- Database-role tests prove request/worker least privilege, owner non-login
  isolation, direct-table mutation denial, and cross-Organization negative paths.
- Migration tests prove 00015 -> 00016 -> 00015 -> 00016, N/N-1 behavior, and
  fail-closed down migration with live deletion evidence.
- `make generate`, focused tests, full unit/race/integration tests, `go vet`,
  lint, OpenAPI/Protobuf breaking checks, and two-pass generated-output hashes
  are clean before closure.

## Explicitly Deferred

This slice does not claim ADR 0015 complete across infrastructure it cannot
control from this repository:

- Worker Agent deletion of terminal NVMe scratch and Local Recovery State;
- opt-in debug-dump creation, authorization, and 72-hour deletion;
- off-cluster Artifact backup expiry and deletion replay after restore;
- legal-hold policy, jurisdiction-specific retention extension, and proof from a
  production object-store lifecycle implementation;
- production metrics, alerts, runbooks, deployment receipts, or any of the nine
  Production Gate Launch Receipts.

These boundaries remain `Partial` in `docs/implementation-status.md` until the
later Worker, deployment, DR, and Production Gate slices provide direct
evidence.

## Completion Boundary

Slice 16 is complete only when the committed repository implements and verifies
the Project policy, immutable Admission/Visible Completion snapshots, customer
and automatic control-plane deletion flows, exact-version/multipart Artifact
Store operations, least-privilege roles, N/N-1 behavior, and immutable receipts
described above with no P0-P2 review finding.

Completion of this slice advances ADR 0015 and acceptance scenario 19 but does
not close Worker scratch, debug, backup replay, or Production Gate evidence.
