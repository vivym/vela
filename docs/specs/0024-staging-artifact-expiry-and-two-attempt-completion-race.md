# Staging Artifact Expiry And Two-Attempt Completion Race

Date: 2026-08-26

Status: Implementation target. This slice closes the repository-verifiable part
of acceptance scenario 13 and advances the incomplete-object portion of scenario
19. It does not declare a Production Gate PASS or prove a production bucket
lifecycle.

## Goal

Prove that two different Attempts for one Job cannot produce two business results,
and durably delete every non-winning Artifact object after the Job reaches a
terminal state. The winning Attempt alone may form Visible Completion, one
ArtifactSet, one Charge, and customer access. A stale Attempt may have uploaded and
verified exact object versions, but those versions remain private, are never
charged, and enter a PostgreSQL-authoritative deletion flow whose immutable
deadline is no later than 24 hours after the Job terminal transition.

## Governing Decisions

- ADR 0003: the winner commits Job `SUCCEEDED`, ArtifactSet, Charge, and access in
  one transaction.
- ADR 0007: cleanup preserves Organization and Project ownership through composite
  keys, least-privilege roles, and exact storage identities.
- ADR 0008: losing output is Customer Content and cannot be retained for reuse.
- ADR 0011: a failed or losing Attempt never creates a Charge.
- ADR 0013: migration 00024 is additive on the up path; the previous control binary
  continues to process existing retention work after the migration.
- ADR 0015: incomplete objects and uploads live for at most 24 hours. PostgreSQL,
  not an object-store lifecycle rule alone, owns the deadline, claim, retry, and
  receipt.
- ADR 0020 and ADR 0021: a replacement Attempt does not extend the Job Expiry or
  reuse the old Attempt's finalization authority.
- ADR 0026 and ADR 0027: all Attempts compete through the same Job/fence authority;
  exactly one business result consumes the Credit Reservation.
- ADR 0028: losing objects are cleanup inputs, not cross-Worker checkpoints.
- ADR 0029: repository verification is implementation evidence only; Production
  Gates remain `0/9 PASS` without Launch Receipts.

## In Scope

1. Exercise two real Attempt identities for one Job. Attempt 1 reaches finalization
   and produces verified exact-version Artifacts, then loses execution authority and
   is replaced by Attempt 2 at a higher fence. Both call Visible Completion
   concurrently. Only Attempt 2 can win; Attempt 1 receives a stable stale-authority
   result.
2. Preserve the existing one-active-Attempt database invariant. This slice does not
   allow two active Leases or weaken fencing to manufacture the race.
3. After a Job reaches `SUCCEEDED`, `FAILED`, or `CANCELED`, enqueue one internal
   incomplete-Artifact deletion request for every non-winning Artifact generation of
   that Job. `CANCELING` is not terminal and remains ineligible.
4. Bind the deletion deadline to the Job terminal transition time plus the immutable
   24-hour incomplete-content retention duration. A delayed reconciler observes an
   overdue request; it never resets or extends the deadline.
5. Reuse the existing PostgreSQL-authoritative Content Deletion claim, retry,
   target, and immutable receipt ledger. Internal incomplete cleanup is distinct from
   successful-Artifact retention so a later 7/30/90-day deletion remains possible.
6. Snapshot each losing Artifact's generated object key and recorded exact version.
   If the version was durably recorded, delete that exact `(object_key, version_id)`.
   If object completion won externally but its database report was lost, resolve the
   unique current version and then delete that exact version.
7. Abort incomplete multipart uploads under the terminal Job's generated prefix.
   `NoSuchKey`, `NoSuchVersion`, and `NoSuchUpload` remain successful idempotent
   absence results.
8. Perform no Artifact Store call while holding Job, Attempt, Artifact, Charge, or
   Credit Reservation publication locks. Claims commit before adapter work and the
   exact result is recorded through a versioned claim CAS.
9. On completed cleanup, transition only targeted non-winning Artifact rows out of
   their staging lifecycle and terminalize their ArtifactUpload rows. Never revoke
   the winning access grant, mutate ArtifactSet history, remove safe validation
   evidence, or change the Charge.
10. Storage failure records only a bounded safe error code, releases the claim for a
    bounded retry, and leaves the request incomplete. A crash after storage success
    replays the same exact identities and converges to one immutable receipt.
11. Multiple control replicas use PostgreSQL leased claims and `SKIP LOCKED`; one
    target cannot be concurrently owned or completed twice.
12. The production `vela-control` caller runs incomplete cleanup through the same
    retention runtime role and Artifact Store adapter already used for successful
    retention and Content Deletion.

## Public Test Seams

The architecture already fixes and the implementation must test these seams:

1. Worker `Start`, `BeginFinalization`, upload/verification, `Fail`, replacement
   `Acquire`, and `CompleteVisibleCompletion` under exact Attempt/Lease authority;
2. the PostgreSQL-authoritative retention `ReconcileBatch` interface;
3. the S3-compatible exact-version delete, current-version discovery, multipart
   listing, and multipart-abort adapter contract;
4. Project committed-Artifact read/access, which must expose only the winning set;
5. migration, database-role, constraint, receipt, N/N-1, and down/up behavior.

Tests observe behavior only through these boundaries. Database inspection is used
for immutable ledger and migration evidence, not as a replacement for exercising
the Worker, retention, or storage interfaces.

## Database And Authorization Invariants

- A Job has at most one internal incomplete-Artifact deletion request, distinct
  from its successful-Artifact retention request and customer Content Deletion.
- Internal incomplete cleanup is eligible only for terminal Jobs and never targets
  a `COMMITTED`, `DELETED`, or already expired Artifact.
- Every target carries the exact Organization, Project, Job, Attempt, Artifact, and
  storage identity selected by PostgreSQL. Cross-domain references fail by
  composite foreign key.
- The cleanup deadline equals the Job terminal timestamp plus 24 hours. Enqueue and
  retries cannot move it.
- `vela_retention` remains NOLOGIN, non-superuser, non-BYPASSRLS, owns no tables,
  and can invoke only narrow claim/enqueue/receipt functions. Mutation owners remain
  unavailable to login roles.
- Request, authentication, Worker, and customer roles cannot enqueue, claim,
  complete, inspect, or forge internal cleanup receipts.
- Cleanup receipts remain immutable and contain only safe IDs, action kinds,
  attempt counts, timestamps, and storage outcomes.

## Compatibility And Migration

- Migration 00024 adds a distinct `RETENTION_INCOMPLETE_ARTIFACT` source to the
  existing retention request ledger. Every pre-00024 source and the N-1
  successful-Artifact retention query keep their original meaning, while the new
  source identifies only the terminal cleanup class.
- Existing function signatures, REST, OpenAPI, Protobuf, event payloads, Worker
  transport, and sqlc callers remain compatible.
- The current Reconciler selects retention protocol `24` inside the enqueue
  transaction. An N-1 caller that does not select that protocol retains the
  exact pre-00024 enqueue semantics and cannot invoke the new helper through
  the compatibility entry point.
- The exact N-1 control binary can start, complete a Job, and run the previous
  retention paths after Up. It ignores the new internal enqueue function.
- Down refuses when any incomplete cleanup request or receipt exists. Empty
  00023 -> 00024 -> 00023 -> 00024 is supported and restores the same role and
  function surface.

## Required Evidence

- A two-Attempt completion race produces one Visible Completion, ArtifactSet,
  Charge, access grant, and success event; the stale Attempt cannot publish or
  charge.
- Before cleanup, losing exact versions are private and absent from Project Artifact
  reads. After cleanup, only losing versions were deleted and winning exact versions
  remain readable and charged exactly once.
- Terminal `FAILED` and `CANCELED` Jobs enqueue incomplete cleanup without creating
  a Charge or deleting required non-content history.
- Enqueue is idempotent, terminal-only, bounded, preserves the original deadline,
  and is safe across concurrent replicas.
- Exact-version, lost-report discovery, multipart abort, already-absent replay,
  storage failure/retry, expired-claim takeover, and post-storage-success replay all
  converge to one immutable receipt.
- Database-role tests deny direct table mutation and cleanup function execution to
  unrelated roles.
- Migration tests prove empty down/up, refusal with durable cleanup evidence, and
  exact N/N-1 startup/runtime compatibility.
- Focused unit/integration/race tests, `make generate`, full tests, lint, cross-build,
  deployment validation, migration checks, generated-output stability, and a
  two-axis review pass before closure.

## Explicitly Deferred

- Production object-store lifecycle, bucket policy, versioning durability, backup
  deletion replay, and real storage-fault receipts;
- opt-in debug dump, legal hold, financial/metadata expiry, and production scratch
  lifecycle evidence from scenario 19;
- real two-Worker fault injection and all nine Production Gate Launch Receipts.

## Completion Boundary

Slice 24 is complete only when the production caller implements the terminal
incomplete-Artifact cleanup flow, the two-Attempt race and every required cleanup
seam above pass, migration/N-1 contracts are proven, and a two-axis review has no
unresolved P0-P2 finding. Scenario 13 may then move from partial to direct repository
evidence. Scenario 19 remains partial until its deferred infrastructure lifecycles
and Production Gates are proven.
