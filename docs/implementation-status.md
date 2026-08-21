# Vela Implementation Status

Date: 2026-08-21

This file is an evidence index, not a launch declaration. `Implemented` means the
repository has a committed vertical slice and verification for the stated part of
an ADR. `Partial` means at least one required production behavior remains. A
Production Gate is PASS only when its versioned Launch Receipt exists; repository
tests alone do not satisfy a gate.

## Implemented Slices

| Slice | Commit | Primary evidence |
| --- | --- | --- |
| Admission Control Plane Foundation | `edfa253` | `docs/specs/0001-admission-control-plane-foundation.md` |
| Worker Assignment And Execution Lease | `7fad532` | `docs/specs/0002-worker-assignment-and-execution-lease.md` |
| Worker Start And Billable Start | `450dd5c` | `docs/specs/0003-worker-start-and-billable-start.md` |
| Worker Heartbeat, Lease Renewal, And Progress | `9cb1a52` | `docs/specs/0004-worker-heartbeat-lease-renewal-and-progress.md` |
| Execution Failure, Retry, And Job Expiry | `d0a8c01` | `docs/specs/0005-execution-failure-retry-and-job-expiry.md` |
| Customer Cancellation And Charge | `9fdf955` | `docs/specs/0006-customer-cancellation-and-charge.md` |

## ADR Evidence Matrix

| ADR | Status | Current evidence | Remaining production behavior |
| --- | --- | --- | --- |
| 0001 Contract credit billing | Partial | Admission reserves contract credit; billable cancellation atomically consumes it into an immutable Charge and durable Invoice export intent. | Visible Completion Charge, settlement records, and an idempotent external Invoice exporter. |
| 0002 Full quote after RUNNING | Implemented for Customer Cancellation | Cancellation before Billable Start releases credit; RUNNING/FINALIZING cancellation posts the immutable full quote and fences execution. | Visible Completion and finalization failure paths must preserve the same boundary. |
| 0003 Atomic Visible Completion | Not started | State and Outbox foundations only. | Artifact validation, ArtifactSet, Charge, access, and SUCCEEDED in one transaction. |
| 0004 Organization and Project | Implemented for current API | Composite ownership and Project-scoped Admission/Get. | Administrative and billing interfaces must retain the same boundary. |
| 0005 Human and Service Principals | Partial | Project Service Principals, rotating credentials, scope and audit attribution. | OIDC Human Principals and administrative credential lifecycle APIs. |
| 0006 Fixed launch roles | Not started | Domain vocabulary only. | Fixed Human RBAC and audited Break-glass Access. |
| 0007 Organization Isolation | Partial | Forced RLS, composite foreign keys, request/auth/internal roles, negative DB tests. | Artifact, billing, webhook, administration, and deployment isolation evidence. |
| 0008 No Customer Content reuse | Partial | Prompt is isolated as request content with an expiry. | Access audit, support authorization, policy enforcement, and deletion across storage/backups. |
| 0009 Statistical SLOs | Partial | API exposes no Hard Deadline; Job Expiry and Dynamic ETA are distinguished. | SLO measurement, eligibility envelopes, dashboards, and certification receipts. |
| 0010 Bounded admission and queues | Partial | Admission and compatibility queue counters are transactionally bounded. | Hierarchical Scheduler lanes, prediction, and fault-responsive Admission. |
| 0011 No failed-Job Charge | Implemented for pre-finalization failure | Failure releases reservations and records no Charge; Customer Cancellation is separately attributed and charged only after Billable Start. | Finalization failure and Visible Completion must preserve this invariant. |
| 0012 Single-region DR | Not started | PostgreSQL/Outbox recovery semantics are designed. | Cluster manifests, WAL/archive, restore, JetStream rebuild, Artifact backup, and drills. |
| 0013 Non-interrupting releases | Partial | Six additive migrations and actual N/N-1 control-binary tests. | Worker/event/API rollout, drain, rollback, and retained-backlog evidence. |
| 0014 Project webhooks | Not started | Transactional Outbox publisher exists. | Subscriptions, HMAC rotation, delivery/retry/dead-letter/manual replay. |
| 0015 Class-specific retention | Partial | Request content has a retention timestamp. | Artifact/upload/scratch/debug/metadata/financial retention and Content Deletion. |
| 0016 Preset versus Service Class | Implemented for Admission/Retry | Both immutable revisions are resolved and retained separately. | Scheduler and SLO reporting must continue to keep them separate. |
| 0017 Three presets | Partial | Catalog restricts stable IDs to `quality`, `balanced`, and `fast`. | Independent certification and ACTIVE promotion receipts for every saleable SKU. |
| 0018 Certified output SKUs | Implemented for Admission | ACTIVE certification and RateCard line resolve to an immutable integer quote. | Output validation and certification lifecycle/invalidations. |
| 0019 Attempt-scoped progress | Implemented | Current Attempt/phase progress, staleness, replay, and retry reset are covered. | FINALIZATION progress and production telemetry calibration. |
| 0020 Job Expiry | Partial | Queue, retry, assignment, and running expiry are fenced by PostgreSQL time. | FINALIZING expiry and Artifact recovery must enforce the same ceiling. |
| 0021 Bounded retry | Partial | Attempt and cumulative compute budgets, retry backoff, and expiry are enforced. | Finalization budget, cross-Job fingerprint circuit, and certified runtime values. |
| 0022 Hierarchical fairness | Not started | Assignment accepts a Scheduler-selected candidate only. | Organization/Service Class/Project/Job fairness, aging, Protected Lane, retry lane. |
| 0023 Certified remediation | Not started | Worker can be drained/offlined after failure. | Identity-bound L0-L6 operations, receipts, quarantine, validation, and node agent. |
| 0024 Work-conserving capacity | Partial | READY Workers are not reserved and retry returns to shared capacity. | Bounded retry lane, risk Admission, cross-profile placement, and measured SLO effect. |
| 0025 Three control/storage nodes | Not started | Components are compatible with PostgreSQL and JetStream. | RKE2/CNPG/JetStream/S3 deployment, anti-affinity, disks, backup, and failover evidence. |
| 0026 Reserve credit at Admission | Implemented for Admission/Cancellation | Job, snapshots, reservation, counters, idempotency, and Outbox commit atomically; cancellation consumes or releases the reservation exactly once. | Visible Completion and finalization failure must close the remaining reservation lifecycle. |
| 0027 Charge when cancel wins | Partial | Cancel CAS, fencing, full-quote Charge, stop acknowledgement, expiry reconciliation, and replay are atomic and idempotent. | Visible Completion must race the same authority and return `ALREADY_SUCCEEDED` with the winning ArtifactSet. |
| 0028 Recompute after Worker loss | Partial | LOST Attempt, higher-fence replacement, exclusions, and whole-job recompute. | Worker local recovery implementation and finalization-only recovery. |
| 0029 Evidenced Production Gates | Not started | Gate definitions exist in `docs/architecture.md`. | Nine versioned Launch Receipts; current result is `0/9`. |

## Acceptance Coverage

The 30 scenarios in `docs/architecture.md` remain the completion authority. The
implemented slices provide direct evidence for Admission (1-3), Customer
Cancellation state/Charge/stop lifecycle (4), failure/retry and stale execution
authority (6, 8), Attempt progress (9), Assignment replay (24), PostgreSQL-time
Lease behavior (25), Organization database isolation (part of 27), and
database/control N/N-1 compatibility (part of 30). Scenario 5 has only the
cancellation-side CAS/fencing evidence; Visible Completion is still unproven.
Every other scenario is unproven or only partially proven until a later slice
records its exact evidence.

## Production Gates

Current result: `0/9 PASS`. No production traffic is authorized by this document.
