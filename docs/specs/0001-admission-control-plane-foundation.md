# Admission Control Plane Foundation

| Field | Value |
| --- | --- |
| Status | Approved for implementation |
| Architecture baseline | `48efd6c` |
| Primary seam | Project-scoped Job REST API |
| Persistence seam | PostgreSQL schema, transaction and RLS contract |
| Event seam | Transactional Outbox Publisher |

## Goal

Build the first executable production slice of Vela: authenticate a Project Service Principal, evaluate a certified discrete video SKU, atomically admit a bounded Job against Project, pool and Customer Organization credit constraints, persist all immutable snapshots and an Outbox event, and expose the accepted Job through the Project-scoped API.

This slice establishes the authority and concurrency rules used by every later Worker, Scheduler, Artifact and Billing feature. It must use PostgreSQL as the fact source; an in-memory queue, credit ledger or idempotency cache is not an acceptable substitute.

## Governing Decisions

- ADR 0004: Customer Organization and Project are distinct boundaries.
- ADR 0005: inference calls use Project-owned Service Principals and rotating credentials.
- ADR 0007: request paths use forced RLS and composite organization/project ownership.
- ADR 0010: Admission and queues are bounded; rejected Admission creates no Job.
- ADR 0016: Generation Preset and Service Class revisions are independent snapshots.
- ADR 0018: pricing resolves an ACTIVE discrete OutputSpec SKU.
- ADR 0020: `job_expires_at` is a lifecycle ceiling, not a deadline promise.
- ADR 0021: Retry Budget is immutable in the execution policy snapshot.
- ADR 0026: Admission atomically creates Credit Reservation, Job, snapshots, idempotency result and Outbox event.

## In Scope

1. Reproducible Go module and generation/check commands.
2. OpenAPI contract for `POST` and `GET` Project Job endpoints.
3. Protobuf envelope for versioned Outbox events.
4. PostgreSQL roles, schema, constraints, forced RLS and initial migration.
5. Project Service Principal bearer credential authentication.
6. Catalog lookup for Model, Generation Preset, Service Class, OutputSpec, Profile Certification and ACTIVE Rate Card line.
7. Admission transaction with Project/pool queue counters and Contract Credit Ledger locking.
8. Project-scoped successful idempotency replay and request-hash conflict detection.
9. Outbox claim, publish receipt and retry-safe marking contract.
10. Unit, PostgreSQL integration, concurrency and isolation tests.

## Explicitly Deferred

- Worker Assignment, Lease, heartbeat and Job state transitions after QUEUED.
- H3 runner, GPU health, remediation and Kubernetes controllers.
- Artifact upload, Visible Completion, cancellation and Charge posting.
- Full hierarchical scheduling and Dynamic ETA calibration.
- Human OIDC administration UI and organization provisioning API.

The deferred work must build on the contracts in this slice rather than replacing them.

## API Contract

`POST /v1/projects/{project_id}/jobs` requires a bearer Service Principal credential and `Idempotency-Key`.

- `202 Accepted`: the complete Admission transaction committed and returns `job_id`, `QUEUED`, PricingSnapshot and `job_expires_at`.
- `402 credit_limit_exceeded`: available Contract Credit is insufficient; no Job or reservation exists.
- `429 project_limit_exceeded`: the Project queue bound is reached; `Retry-After` is present and no Job exists.
- `503 capacity_unavailable`: the compatible pool is closed or bounded; `Retry-After` is present and no Job exists.
- `409 idempotency_conflict`: the same Project and key were already accepted with a different canonical request hash.

An Admission rejection is not persisted as a permanent idempotency result. After the blocking condition changes, the same key and request may be evaluated again.

`GET /v1/projects/{project_id}/jobs/{job_id}` returns only a Job visible to the authenticated Project. Cross-Project and cross-Organization identifiers fail closed without revealing whether the Job exists.

## Atomic Admission

The transaction must:

1. serialize the Project-scoped idempotency key;
2. replay the existing Job or reject a mismatched request hash;
3. resolve immutable ACTIVE catalog and rate revisions;
4. lock and recheck the Project admission counter;
5. lock and recheck the compatible Worker pool admission counter;
6. lock the Customer Organization credit account and recheck available credit;
7. increment counters and reserved credit;
8. create PricingSnapshot and ExecutionPolicySnapshot;
9. create the QUEUED Job and RESERVED CreditReservation;
10. persist the successful idempotency result;
11. create a versioned `job.ready` Outbox event;
12. commit before returning `202`.

Any error before commit leaves all twelve effects absent.

## Database Invariants

- Every organization-owned row carries `organization_id`; every Project-owned row also carries `project_id`.
- Composite foreign keys prevent a Project-owned row from referencing another Organization.
- Request transactions set immutable local Organization, Project and Principal context and run under the restricted request role.
- RLS is enabled and forced on every organization-owned table.
- `reserved_minor + unsettled_posted_minor <= contract_credit_limit_minor` is enforced under the locked credit row.
- Project and pool queued counters cannot be negative or exceed their configured hard bound.
- A Job has exactly one PricingSnapshot, one ExecutionPolicySnapshot and one CreditReservation.
- A successful `(project_id, idempotency_key)` identifies exactly one request hash and Job.
- Money uses integer minor units with one currency per organization credit account and resolved Rate Card line.
- Outbox `event_id` is stable across publish retries and becomes published only after an acknowledged broker receipt.

## Authentication Contract

Service Principal credentials use a public credential identifier plus a high-entropy secret. PostgreSQL stores only a keyed digest, scopes, expiry, creator and revocation state. Overlapping active credentials are allowed for rotation. Authentication resolves Organization, Project and Principal before opening the request transaction; the authentication database role can execute only the narrow credential lookup function and cannot read organization tables directly.

The submit endpoint requires `jobs:submit`; the get endpoint requires `jobs:read`.

## Test Seams And Evidence

The pre-agreed public seams are:

1. REST responses and authenticated Project visibility.
2. PostgreSQL migration, constraints, transaction result and RLS behavior.
3. Outbox Publisher behavior with acknowledged, timed-out and duplicated publication.

Required evidence:

- same key and request returns the original Job after a lost response;
- same key with different request returns 409;
- 402, 429 and 503 create no Job, reservation, snapshots, idempotency result or Outbox row;
- concurrent Project submissions cannot exceed the Project or pool bound;
- concurrent Projects in one Organization cannot exceed Contract Credit Limit;
- Organization A cannot read or reference Organization B data under the request role;
- a committed Job always has both snapshots, one reservation and one Outbox event;
- publishing failure leaves the Outbox row retryable;
- publish acknowledgement followed by a local crash may republish the same event id but cannot create a second aggregate transition;
- generated OpenAPI and Protobuf sources are reproducible and breaking checks run in CI.

## Completion Boundary

This slice is complete only when all commands documented in the repository run from a clean checkout, the full Go test suite passes, PostgreSQL integration tests pass against a real PostgreSQL container, generated sources are clean, and a two-axis code review finds no unresolved correctness or specification issue.
