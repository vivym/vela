# Vela Implementation Status

Date: 2026-08-23

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
| Artifact Finalization And Visible Completion | `82d5752` | `docs/specs/0007-artifact-finalization-and-visible-completion.md` |
| Hierarchical Scheduler | `a606c48` | `docs/specs/0008-hierarchical-scheduler.md` |

## ADR Evidence Matrix

| ADR | Status | Current evidence | Remaining production behavior |
| --- | --- | --- | --- |
| 0001 Contract credit billing | Partial | Admission reserves contract credit; billable cancellation and Visible Completion atomically consume it into an immutable Charge and durable Invoice export intent. | Settlement records and an idempotent external Invoice exporter. |
| 0002 Full quote after RUNNING | Implemented for current lifecycle | Cancellation after Billable Start and successful Visible Completion use the immutable full quote; platform/finalization failure releases credit without a partial Charge. | Later terminal paths and settlement integrations must preserve the same boundary. |
| 0003 Atomic Visible Completion | Implemented | Validated VIDEO plus THUMBNAIL, ArtifactSet, Charge, credit, access, Job/Attempt success, and canonical Outbox events commit in one transaction and replay exactly. | Deployment-level fault receipts remain part of the separate Production Gates. |
| 0004 Organization and Project | Implemented for current API | Composite ownership and Project-scoped Admission/Get/Cancel plus committed Artifact reads. | Administrative and billing interfaces must retain the same boundary. |
| 0005 Human and Service Principals | Partial | Project Service Principals, rotating credentials, scope and audit attribution. | OIDC Human Principals and administrative credential lifecycle APIs. |
| 0006 Fixed launch roles | Not started | Domain vocabulary only. | Fixed Human RBAC and audited Break-glass Access. |
| 0007 Organization Isolation | Partial | Forced RLS, composite foreign keys, request/auth/internal/Artifact roles, private staging, exact-version access grants, and cross-Project/Organization negative tests. | Human roles, billing, webhook, administration, NATS, and deployment isolation evidence. |
| 0008 No Customer Content reuse | Partial | Prompt has an expiry; staging Artifacts are private and committed exact-version reads require a short-lived Project grant. | Access audit, support authorization, no-reuse policy enforcement, and deletion across storage/backups. |
| 0009 Statistical SLOs | Partial | API exposes no Hard Deadline; Job Expiry and Dynamic ETA are distinguished. | SLO measurement, eligibility envelopes, dashboards, and certification receipts. |
| 0010 Bounded admission and queues | Implemented for current control plane | Transactional queue counters, pool-scoped bounded projection, risk-aware Admission prediction, Dynamic ETA, hierarchical lanes, and fail-closed counter-drift detection are integrated. | Deployment calibration and Production Gate receipts remain separate. |
| 0011 No failed-Job Charge | Implemented for current lifecycle | Execution, finalization-deadline, validation, and unrecoverable Artifact failure release credit and create no Charge; only Visible Completion or post-Billable-Start Customer Cancellation posts one. | Future failure sources must use the same terminal authority. |
| 0012 Single-region DR | Not started | PostgreSQL/Outbox recovery semantics are designed. | Cluster manifests, WAL/archive, restore, JetStream rebuild, Artifact backup, and drills. |
| 0013 Non-interrupting releases | Partial | Eight additive migrations, exact N/N-1 database/control compatibility, Protobuf/OpenAPI breaking checks, and migration down/up evidence. | Deployed Worker/event/API rollout, drain, rollback, and retained-backlog receipts. |
| 0014 Project webhooks | Not started | Transactional Outbox publisher exists. | Subscriptions, HMAC rotation, delivery/retry/dead-letter/manual replay. |
| 0015 Class-specific retention | Partial | Request content has a retention timestamp; expired staging uploads and multipart sessions have bounded cleanup paths. | Successful Artifact, scratch, debug, metadata, and financial retention plus Content Deletion. |
| 0016 Preset versus Service Class | Implemented for current control plane | Admission, Retry, and Scheduler retain both immutable revisions separately; Scheduler reads ServiceClassRevision policy and never derives priority from Preset or price. | SLO reporting must preserve the same boundary. |
| 0017 Three presets | Partial | Catalog restricts stable IDs to `quality`, `balanced`, and `fast`. | Independent certification and ACTIVE promotion receipts for every saleable SKU. |
| 0018 Certified output SKUs | Implemented for Admission/finalization | ACTIVE certification and RateCard resolve an immutable quote; finalization enforces the certified media facts and fixed complete ArtifactSet. | Certification lifecycle, invalidation, and saleable-SKU receipts. |
| 0019 Attempt-scoped progress | Implemented | Current Attempt progress covers QUEUED through FINALIZING, with staleness, replay, and retry reset. | Production telemetry calibration and SLO receipts. |
| 0020 Job Expiry | Implemented for current lifecycle | Queue, retry, assignment, running, and finalization expiry are fenced by PostgreSQL time; recovery cannot extend the immutable deadline. | Scheduler and deployment receipts must preserve the ceiling. |
| 0021 Bounded retry | Partial | Attempt, cumulative compute, finalization recovery budgets, retry backoff, and Job Expiry are enforced. | Cross-Job fingerprint circuit and certified runtime values. |
| 0022 Hierarchical fairness | Implemented | PostgreSQL-authoritative Organization/Service Class/Project weighted-deficit selection, bounded Job score, per-Job retry risk, aging, Protected Lane, retry lane, durable claims, and multi-replica recovery are integrated. | Production fairness/SLO measurement remains a separate gate receipt. |
| 0023 Certified remediation | Not started | Worker can be drained/offlined after failure. | Identity-bound L0-L6 operations, receipts, quarantine, validation, and node agent. |
| 0024 Work-conserving capacity | Implemented for current control plane | Every compatible READY Worker/profile remains available to ordinary work; bounded retry lane, risk Admission, worker scoring, and physical-slot queue projection hold no hard idle reserve. | Fleet deployment and measured SLO effect remain separate gate evidence. |
| 0025 Three control/storage nodes | Not started | Components are compatible with PostgreSQL and JetStream. | RKE2/CNPG/JetStream/S3 deployment, anti-affinity, disks, backup, and failover evidence. |
| 0026 Reserve credit at Admission | Implemented for current lifecycle | Admission reserves atomically; cancellation, execution/finalization failure, and Visible Completion consume or release exactly once with counters and Outbox. | Future terminal paths must close the same reservation authority. |
| 0027 Charge when cancel wins | Implemented | Visible Completion and Customer Cancellation serialize through one Job authority; the winner owns the only Charge and late completion returns the winning ArtifactSet. | Production fault-injection receipt remains a separate gate. |
| 0028 Recompute after Worker loss | Partial | LOST execution creates a higher-fence whole-Job retry; finalization loss is recovered on the same Attempt/fence by a Reconciler without resetting its deadline. | Worker local recovery implementation and certification. |
| 0029 Evidenced Production Gates | Not started | Gate definitions exist in `docs/architecture.md`. | Nine versioned Launch Receipts; current result is `0/9`. |

## Acceptance Coverage

The 30 scenarios in `docs/architecture.md` remain the completion authority. The
implemented slices provide direct repository evidence for Admission (1-3),
Customer Cancellation and its Visible Completion race (4-5), no-Charge failure
(6), Scheduler crash/fairness/pool isolation (7), stale execution and bounded
retry (8), Attempt progress (9), Artifact
recovery/immutability and whole-set validation (11-12), begin-finalization replay
(21), Assignment replay (24), and PostgreSQL-time Lease behavior (25). Current
evidence is partial for multipart resume plus whole-Job recompute (10),
competing completion authorities without a two-Attempt Artifact race (13),
immutable pricing/profile behavior (14),
Outbox/Invoice intent without the external exporter (20), Organization database
and Artifact isolation (27), and N/N-1 database/control/Worker compatibility
without a deployed rollout receipt (30). Every other scenario remains unproven
until a later slice records its exact evidence.

## Production Gates

Current result: `0/9 PASS`. No production traffic is authorized by this document.
