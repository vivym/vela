# Artifact Finalization And Visible Completion

Status: implementation target

Fixed point: `c94e140`

This specification is the implementation authority for the seventh production
vertical slice. It closes the success path that Customer Cancellation deliberately
left open. It does not declare any Production Gate PASS.

## Goal

Turn one authorized RUNNING Attempt into exactly one externally visible success:
all required immutable Artifacts, one committed ArtifactSet, Job `SUCCEEDED`, one
full-quote Charge, Artifact access eligibility, and durable terminal events commit
as one PostgreSQL business result. A stale or losing Attempt can upload temporary
objects but can never publish them or create another Charge.

## Governing Decisions

- ADR 0001: a billable result creates one immutable Charge and does not wait for
  external Invoice settlement before Artifact access.
- ADR 0003: ArtifactSet, Job `SUCCEEDED`, Charge, and access eligibility commit in
  one transaction.
- ADR 0007: Project ownership, RLS, exact object key/version authorization, and
  restricted request roles enforce Organization Isolation.
- ADR 0011: validation, finalization, expiry, or platform failure creates no Charge.
- ADR 0013: migration, REST, Protobuf, events, and long-running authority remain
  compatible with the exact `c94e140` N-1 release.
- ADR 0015: committed Customer Content receives a versioned retention deadline;
  incomplete and losing output remains separately short-lived.
- ADR 0018: validation uses the immutable OutputSpec and generation count fixed at
  Admission; actual compute cost never changes the quote.
- ADR 0020/0021: Job Expiry and the immutable per-Attempt finalization budget bound
  upload, validation, recovery, and commit without creating another compute Attempt.
- ADR 0026: the existing CreditReservation is consumed exactly once by success or
  released exactly once by non-billable terminal failure.
- ADR 0027: Visible Completion and Customer Cancellation use the same durable
  authority; one wins and the loser returns the already-committed result.
- ADR 0028: upload may resume only while the exact local/shared source is available;
  node or NVMe loss does not become fictional cross-Worker checkpoint recovery.

## In Scope

1. `BeginFinalization` authenticates the Worker and EXECUTION Lease, atomically
   switches Job and Attempt from RUNNING to FINALIZING, switches the same Lease to
   FINALIZATION, and fixes `finalization_started_at` and an unextendable
   `finalization_deadline_at` capped by `job_expires_at`.
2. The first transition deterministically creates the complete required Artifact
   and ArtifactUpload plan from immutable OutputSpec plus `generation_count`.
   Replays return the same identities, object keys, deadline, and authority.
3. The launch video contract requires one VIDEO and one THUMBNAIL per generation.
   OutputSpec records immutable container/content expectations in addition to its
   existing width, height, duration, frame rate, and codec definition.
4. ArtifactUpload supports claim, multipart-session persistence, completed-part
   reconciliation, exact object version, size, checksum, content type, retry state,
   and an expiry no later than the finalization deadline. An external multipart
   session created before its database CAS may become an orphan but cannot become a
   committed Artifact; reconciliation or bucket lifecycle removes it.
5. A production S3-compatible adapter uses private versioned objects, an exact
   non-customer-derived key, conditional create where supported, exact version
   reads, checksum/size constraints, multipart resume, and 15-minute signed GET
   URLs. MinIO/Testcontainers conformance exercises the same adapter contract.
6. Artifact validation resolves the exact object version and verifies size,
   SHA-256, content type, kind/ordinal/count, and fixed `ffprobe` media facts. VIDEO
   must match duration, resolution, frame count, codec, and container; THUMBNAIL
   must match its fixed image contract. Worker-provided metadata is never treated
   as proof without adapter/validator verification.
7. `CompleteVisibleCompletion` accepts a complete candidate only under the current
   FINALIZATION Lease, owner identity, Worker epoch when applicable, fence, Job
   version, unexpired finalization deadline, and unexpired Job. It creates immutable
   ArtifactSet and item snapshots, marks every required Artifact COMMITTED, stores
   `jobs.result_artifact_set_id`, terminalizes Attempt and Job, revokes the Lease,
   decrements exact running counters, consumes CreditReservation, posts one
   `VISIBLE_COMPLETION` Charge, and opens Artifact access in one transaction.
8. The same transaction writes one Visible Completion record and canonical typed
   `job.succeeded`, `charge.posted`, and `invoice.export_requested` Outbox events.
   Payloads contain stable IDs, aggregate version, fixed object metadata, and no
   prompt, upload credential, Lease token, local path, or signed URL.
9. Replay with the same authority/candidate returns the immutable success.
   A different candidate, stale Lease, replaced Attempt, cancellation winner,
   finalization expiry, or Job Expiry returns a stable non-mutating decision.
   Cancellation after success returns `ALREADY_SUCCEEDED` with the winning
   ArtifactSet summary and never creates cancellation history or a second Charge.
10. `ReconcileNextFinalization` may claim recoverable FINALIZING work with the same
    fence only after Worker authority is no longer usable. Reconciler ownership
    cannot run inference, change the deadline, replace object identities, or accept
    unverifiable Worker-local source. It either resumes existing upload/validation,
    commits the complete set, or records an explicit unrecoverable outcome.
11. Finalization deadline or Job Expiry atomically fences authority and selects
    retry versus terminal FAILED through the existing Retry Budget. Recoverable
    upload failure remains FINALIZING; terminal failure releases CreditReservation,
    records no Charge, and preserves uploaded temporary evidence for bounded cleanup.
12. Project-scoped HTTP reads expose only the committed ArtifactSet. Artifact access
    requires an active credential with explicit read scope, rechecks revocation,
    expiry, scope, Project, and exact object version in the request transaction, and
    returns short-lived signed URLs only after the committed database read.
13. Artifact, ArtifactUpload, ArtifactSet, item snapshot, Visible Completion,
    Charge, and access-grant history use composite Organization/Project foreign keys,
    forced RLS where customer-readable, immutable definition triggers, and narrow
    role grants. Request/auth roles cannot forge finalization or list shared storage.
14. The production Worker transport is a bidirectional gRPC stream protected by TLS
    1.3 mutual authentication. The server accepts exactly one verified SPIFFE URI
    from the client certificate, resolves it through the exact registered Worker
    identity, and invokes only coordinator methods. Worker requests contain no
    caller-supplied `worker_id` and receive no direct database mutation authority.

## State And Identity

Artifact states are `STAGING`, `UPLOADED`, `VERIFIED`, `COMMITTED`, `EXPIRED`, and
`DELETED`. Upload states are `INITIATED`, `UPLOADING`, `UPLOADED`, `VERIFIED`,
`ABORTED`, and `EXPIRED`. Only lifecycle transitions explicitly owned by the
coordinator or Artifact services are allowed; object identity, version, checksum,
size, kind, ordinal, Job, Attempt, and fence never change after verification.

Artifact object keys use generated identities only:

```text
artifacts/{organization_id}/{project_id}/{job_id}/{attempt_id}/{artifact_id}/video.mp4
artifacts/{organization_id}/{project_id}/{job_id}/{attempt_id}/{artifact_id}/thumbnail.webp
```

No key, Outbox payload, validation receipt, or customer response contains prompt,
customer filename, Worker-local path, or mutable bucket listing authority.

ArtifactSet stores a canonical manifest hash over ordered immutable item snapshots.
The database verifies that the set contains exactly each required `(kind, ordinal)`
once and that every item belongs to the winning Job and Attempt. `jobs` may enter
SUCCEEDED only with a committed result pointer and matching Visible Completion and
Charge; direct partial success mutations are rejected.

## Locking And Races

Worker-owned finalization follows the coordinator order: Worker, Lease write
relation, Lease, Attempt, Job, Project/pool counters, CreditReservation,
Organization credit, then Artifact rows. Reconciler-owned work locks the referenced
Worker before the same authority chain so cancellation, expiry, failure, and
completion do not invert the order.

No object-store call occurs while a PostgreSQL transaction holds Job, Worker, Lease,
credit, or Artifact publication locks. External work records a claim, commits,
performs the adapter operation, then reports the exact result through versioned CAS.
Every publication predicate is rechecked under the final transaction locks.

Concurrent complete/complete, complete/cancel, complete/fail, complete/heartbeat,
complete/Job Expiry, Worker/Reconciler completion, upload claim/recovery, and
reconciliation/deadline expiry produce one winner without deadlock, duplicate
Charge, partial ArtifactSet, or lost credit accounting.

## Compatibility And Migration

Migration 00007 is additive. Released migrations 00001-00006 remain byte-identical.
The exact `c94e140` binary must start and perform its auth/request/internal/N-1 smoke
path after Up; its Outbox publisher forwards unknown payload bytes unchanged. New
events are additive Protobuf oneof fields and old readers ignore them.

Down is allowed only when no Artifact/Upload/ArtifactSet/Visible Completion evidence
and no new active FINALIZING authority exists. It refuses representationally unsafe
state instead of deleting or weakening immutable success evidence. Empty Up/Down/Up
is supported and restores the same role/function surface.

## Explicitly Deferred

- Project Webhook Subscription delivery; this slice writes `job.succeeded` only.
- The external monthly Invoice exporter and settlement reconciliation; this slice
  writes an idempotent intent keyed by `charge_id`.
- General Content Deletion execution, backup deletion replay, and expired Artifact
  deletion; this slice fixes retention identities/deadlines and access rules.
- Production hardware/media certification and real-bucket durability receipts.
  Adapter conformance is implementation evidence, not a Production Gate Launch
  Receipt.

## Test Seams And Required Evidence

The architecture already fixes these public seams:

1. Worker `BeginFinalization`, upload reporting, validation, and
   `CompleteVisibleCompletion` with authenticated Lease authority;
2. Artifact Reconciler takeover under PostgreSQL time and the same fence;
3. S3-compatible adapter multipart/versioning/conditional-write/signed-read contract;
4. Project HTTP committed Artifact read and exact-version authorization;
5. PostgreSQL migration, roles, constraints, ledger, RLS, and canonical Outbox bytes;
6. existing Customer Cancellation, Fail, Heartbeat, and Job Expiry authority.

Required evidence includes:

- begin-finalization crash before/after commit replays one fixed plan and deadline;
- multipart create/CAS crash leaves only a cleanable orphan and never a visible item;
- restart resumes completed parts without changing object or upload identity;
- exact object-version overwrite attempts cannot change committed reads;
- wrong/missing kind, ordinal, count, checksum, size, content type, duration,
  resolution, frame count, codec, container, or thumbnail rejects the whole set;
- complete commits ArtifactSet, item snapshots, Job, Attempt, Charge, credit,
  counters, access, and every Outbox event together, and replay changes nothing;
- forced failure at each publication write rolls back the entire business result;
- complete/cancel and all other authority races have one winner and no deadlock;
- stale, expired, replaced, mismatched Worker/Reconciler, and post-cancel authority
  cannot publish or charge;
- a real network gRPC stream completes TLS 1.3 mutual authentication, maps the exact
  certificate SPIFFE URI through PostgreSQL, and commits coordinator finalization;
- finalization recovery never resets deadline or creates a compute Attempt;
- terminal finalization failure and Job Expiry release credit and create no Charge;
- request/auth roles cannot read staging content, forge verification/publication, or
  cross Organization/Project/object-version boundaries;
- generated OpenAPI/sqlc/Protobuf artifacts, exact N-1 expansion, empty Down/Up,
  adapter conformance, unit, lint, full integration, and both race suites pass.

## Completion Boundary

This slice is complete only when every required seam above is on a production caller
path; generation is stable; released migration identity is proven; unit, lint,
PostgreSQL/MinIO/JetStream integration, N/N-1, rollback, authorization, migration,
concurrency, and race tests pass from a clean commit; and a two-axis review finds no
unresolved P0-P2 issue. Production remains `0/9 PASS` until versioned Launch Receipts
prove the separate deployment and hardware gates.
