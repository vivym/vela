# Vela Implementation Status

Date: 2026-08-27

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
| Invoice Export And Receipt | `bcb5594` | `docs/specs/0009-invoice-export-and-receipt.md` |
| Cross-Job Profile Certification Circuit | `6863bd0` | `docs/specs/0010-cross-job-profile-certification-circuit.md` |
| Project Webhook Delivery | `f5fc532` | `docs/specs/0011-project-webhook-delivery.md` |
| Human OIDC And Fixed RBAC | `7884865` | `docs/specs/0012-human-oidc-and-fixed-rbac.md` |
| Project Service Principal And Credential Administration | `5632d26` | `docs/specs/0013-project-service-principal-credential-administration.md` |
| Human Membership And Fixed Role Administration | `03814c4` | `docs/specs/0014-human-membership-and-fixed-role-administration.md` |
| Organization Billing, Audit, And Settlement Contacts | `2a6a5d5` | `docs/specs/0015-organization-billing-audit-and-settlement-contacts.md` |
| Project Retention And Content Deletion | `e0e9cfc` | `docs/specs/0016-project-retention-and-content-deletion.md` |
| Platform Operator Break-glass Access | `183f3f4` | `docs/specs/0017-platform-operator-break-glass-access.md` |
| NATS Workload Identity And Subject Authorization | `00805ab` | `docs/specs/0018-nats-workload-identity-and-subject-authorization.md` |
| Settlement And Credit-adjustment Reconciliation | `2d27630` | `docs/specs/0019-settlement-and-credit-adjustment-reconciliation.md` |
| Certified Remediation Control Plane | `37849d0` | `docs/specs/0020-certified-remediation.md` |
| Worker Local Recovery State | `ea1053a` | `docs/specs/0021-worker-local-recovery-state.md` |
| H3 Worker Agent And Runner | `baee61a` | `docs/specs/0022-h3-worker-agent-and-runner.md` |
| Fleet Controller And Worker Readiness | `cc9a4f7` | `docs/specs/0023-fleet-controller-worker-readiness.md` |
| Staging Artifact Expiry And Two-Attempt Completion Race | `4f29d18` | `docs/specs/0024-staging-artifact-expiry-and-two-attempt-completion-race.md` |
| JetStream Quorum And Consumer Crash Recovery | `d275a91` | `docs/specs/0025-jetstream-quorum-and-consumer-crash-recovery.md` |
| Production Gate Receipt Validation | `436558e` | `docs/launch-receipts/README.md` |
| Control/Storage Deployment Contract | `25a3d2e` | `deploy/control-storage/README.md` |
| Single-Region Recovery Drill Contract | `ee6524c` | `docs/runbooks/single-region-recovery.md` |
| Guarded Node Agent Transport Contract | `44543f0` | `internal/nodeagent/transport.go`, `proto/vela/v1/worker_control.proto` |
| Certified Remediation Node Agent Runtime | `fa6622c` | `cmd/vela-node-agent`, `internal/nodeagent/dispatcher.go`, `deploy/node-agent` |
| Certified Remediation Durable Execution Hardening | `0cc6a7c`, `b17b0cd`, `75981bd`, `754c275` | `internal/nodeagent/fileledger.go`, `internal/nodeagent/host.go`, `internal/securefile/securefile.go` |
| Certified Remediation Runtime Authority | `ad1bcf6` | `docs/specs/0027-certified-remediation-runtime-authority.md` |
| Authorized Debug-dump Lifecycle | `6603c36` | `docs/specs/0028-authorized-debug-dump-lifecycle.md` |
| N/N-1 Rollout, Drain, Rollback, And Retained Backlog Conformance | `21e0781` | `docs/specs/0029-nminusone-rollout-drain-backlog-conformance.md` |
| Worker Process And Node-loss Recovery Conformance | `5a9bad6`, `4d2bb7f` | `docs/specs/0030-worker-node-loss-conformance.md` |
| Off-cluster Artifact Backup Retention And Restore Replay Conformance | pending local commit | `docs/specs/0031-off-cluster-artifact-backup-retention-conformance.md` |

## ADR Evidence Matrix

| ADR | Status | Current evidence | Remaining production behavior |
| --- | --- | --- | --- |
| 0001 Contract credit billing | Partial | Admission reserves contract credit; billable cancellation and Visible Completion atomically consume it into an immutable Charge; the PostgreSQL-authoritative exporter records one external Invoice line and receipt; Organization reporting exposes the exact credit account, immutable Charge/Invoice references, and audited settlement contacts without ledger mutation; Slice 19 adds immutable settlement, credit-adjustment, and Contract Credit Limit reconciliation with a dedicated Finance Principal and isolated mTLS write listener (`2d27630`). | Production settlement/collection lifecycle and Launch Receipts remain outside this repository slice. |
| 0002 Full quote after RUNNING | Implemented for current lifecycle | Cancellation after Billable Start and successful Visible Completion use the immutable full quote; platform/finalization failure releases credit without a partial Charge. | Later terminal paths and settlement integrations must preserve the same boundary. |
| 0003 Atomic Visible Completion | Implemented | Validated VIDEO plus THUMBNAIL, ArtifactSet, Charge, credit, access, Job/Attempt success, and canonical Outbox events commit in one transaction and replay exactly. | Deployment-level fault receipts remain part of the separate Production Gates. |
| 0004 Organization and Project | Implemented for current API | Composite ownership and Project-scoped Admission/Get/Cancel/Artifact access, Retention Policy, and Content Deletion plus Organization-scoped Human membership, role administration, billing, usage, audit, and settlement-contact interfaces preserve both boundaries. | Project/Organization lifecycle and future policy interfaces must retain the same boundary. |
| 0005 Human and Service Principals | Implemented for current API | Enterprise OIDC Human bindings have short proof-only sessions, permanent disablement, audited membership and fixed-role administration; Project-owned Service Principals have audited create/disable and overlapping digest-only Credential issue/revoke; Platform Operators use a separate issuer, audience, binding, session, proof, authentication role, and HTTP context outside all Customer Principal tables. Every implemented administrative mutation retains exact Principal/session attribution. | Production Human and Platform Operator IdP receipts; future identity surfaces must preserve the same separation. |
| 0006 Fixed launch roles | Implemented for current API | All six fixed Human roles are database-constrained and auditably assignable/revocable; BillingAdmin and OrganizationAuditor have independently testable non-content Organization reporting scopes; no customer role grants support access. Exact-scope Break-glass Access requires two distinct Platform Operators, expires in at most one hour, and is revocable and fully audited. | Future permissions and production identity/deployment receipts must preserve these fixed boundaries. |
| 0007 Organization Isolation | Partial | Forced RLS, composite foreign keys, transaction-revalidated Human and Platform Operator sessions, separate request/auth/Human-membership/identity-administration/Organization-reporting/Break-glass/internal/Artifact/billing/Webhook/retention roles, NOLOGIN mutation owners, exact target tuples, private staging, exact-version grants, cross-Project/Organization administrative/content/Break-glass negative tests, and TLS NATS workload identity with exact signer/user rotation and server-side subject authorization. | Deployment isolation evidence. |
| 0008 No Customer Content reuse | Partial | Prompt expiry and irreversible deletion tombstones remain authoritative; staging Artifacts are private; ordinary and dual-controlled Break-glass reads use short-lived exact-version URLs; support access is purpose/scope/target bound and writes immutable safe audit evidence; deletion removes exact versions and multipart sessions; Worker Local Recovery State and Runner outputs are private, bounded, exact-identity bound, independently revalidated, and terminally cleaned by the Worker runtime. Slice 28 adds ProjectAdmin-authorized failure debug dumps that remain isolated from Artifacts and Charge authority, use exact-version signed reads with safe audit, and expire or delete under retention and Content Deletion (`6603c36`). Slice 31 adds committed-only backup deletion authority, all-version purge, and restore replay after deletion authority is durable. | Live Artifact replication/deletion-race evidence, restore points before deletion authority, production object/scratch isolation, and deployment evidence. |
| 0009 Statistical SLOs | Partial | API exposes no Hard Deadline; Job Expiry and Dynamic ETA are distinguished. | SLO measurement, eligibility envelopes, dashboards, and certification receipts. |
| 0010 Bounded admission and queues | Implemented for current control plane | Transactional queue counters, pool-scoped bounded projection, risk-aware Admission prediction, Dynamic ETA, hierarchical lanes, and fail-closed counter-drift detection are integrated. | Deployment calibration and Production Gate receipts remain separate. |
| 0011 No failed-Job Charge | Implemented for current lifecycle | Execution, finalization-deadline, validation, and unrecoverable Artifact failure release credit and create no Charge; only Visible Completion or post-Billable-Start Customer Cancellation posts one. | Future failure sources must use the same terminal authority. |
| 0012 Single-region DR | Partial | Recovery order, RPO/RTO contract, CNPG off-cluster WAL/base-backup target, explicit JetStream rebuild/Outbox replay contract, and controlled single-node/site recovery runbook are rendered in `deploy/control-storage` and `docs/runbooks/single-region-recovery.md`. Slice 26 proves automatic single-node CNPG failover with RPO 0 and a measured sub-five-minute RTO, then proves database-enforced no-quorum Admission/Assignment rollback in a pinned repository conformance cluster. Slice 31 restores a real PostgreSQL snapshot taken after deletion authority and replays two-tier retention against already-purged versioned MinIO objects without content resurrection. | Live WAL/archive and PITR, Artifact replication lifecycle/race, JetStream rebuild, Outbox replay, secret rotation, Barman Cloud Plugin migration before CNPG 1.31, and quarterly restore-drill receipts. |
| 0013 Non-interrupting releases | Partial | Twenty-eight additive migrations, exact N/N-1 database/control compatibility for the fixed migration points, database-owned no-quorum guards, operator-receipted circuit and Fleet Assignment protocol transitions, Protobuf/OpenAPI breaking checks, and migration down/up evidence. Migration 00027 adds dedicated debug-dump roles without widening the exact N-1 retention or audit role allowlists (`6603c36`). Migration 00028 keeps the N-1 `vela_retention` surface primary-only and adds a separate current `vela_backup_retention` role. Slice 29 builds the exact adjacent N-1 control and Worker probes, preserves raw retained-event identity through the current Inbox/Scheduler, drains without interrupting the active Lease, restores exact N-1 writers on schema 27, and proves current plus N-1 Admission/Scheduler return SQLSTATE `55000` during CNPG quorum loss (`21e0781`). | A deployed Kubernetes Worker/event/API rollout, real long-running H3 drain, release rollback, and retained production backlog receipt. |
| 0014 Project webhooks | Implemented | Project-scoped subscriptions, safe terminal-event fanout, overlapping HMAC secret rotation, durable at-least-once retry, dead letter, crash recovery, visibility, and manual replay are integrated. | Public-DNS deployment validation and a real endpoint Launch Receipt remain separate Production Gate evidence. |
| 0015 Class-specific retention | Partial | Versioned 7/30/90-day Project policies are frozen at Admission; request content and successful exact-version Artifacts expire independently; early Content Deletion uses the existing cancellation/Charge authority, tombstones request content, revokes access, deletes exact S3 versions, aborts multipart uploads, recovers/retries claims, and writes immutable receipts under forced RLS; the Worker runtime terminally removes exact Runner outputs and Local Recovery State, while stale-marker reconciliation remains bounded. Slice 24 schedules exact-version cleanup of incomplete STAGING Artifacts from the version-matched terminal Outbox event at 24 hours, permits a Customer deletion request to coexist, preserves `CANCELING`, and completes one idempotent receipt under concurrent Reconcilers. Slice 28 binds opt-in debug dumps to the immutable 72-hour Job snapshot, keeps failed-attempt uploads non-blocking, and schedules exact-version or incomplete multipart-prefix cleanup with immutable receipts (`6603c36`). Slice 31 adds committed-only OFF_CLUSTER_BACKUP targets, all-version/delete-marker purge, two-tier receipts, exact N-1 role compatibility, and post-authority PostgreSQL restore replay. | Artifact replication lifecycle/deletion-race evidence, restore before deletion authority, metadata and financial lifecycle enforcement, legal holds, live scratch lifecycle evidence, and Launch Receipts. |
| 0016 Preset versus Service Class | Implemented for current control plane | Admission, Retry, and Scheduler retain both immutable revisions separately; Scheduler reads ServiceClassRevision policy and never derives priority from Preset or price. | SLO reporting must preserve the same boundary. |
| 0017 Three presets | Partial | Catalog restricts stable IDs to `quality`, `balanced`, and `fast`, while the circuit independently protects each exact certified revision and OutputSpec. | Independent benchmark, certification, ACTIVE promotion, and Launch Receipts for every saleable SKU. |
| 0018 Certified output SKUs | Implemented for current control plane | ACTIVE certification and RateCard resolve an immutable quote; Assignment rechecks certification, the Cross-Job circuit atomically invalidates repeated-failure profiles, and finalization enforces the certified media facts and fixed complete ArtifactSet. | Benchmark execution, certification issuance, ACTIVE promotion, and saleable-SKU Launch Receipts. |
| 0019 Attempt-scoped progress | Implemented | Current Attempt progress covers QUEUED through FINALIZING, with staleness, replay, and retry reset; the Python Runner validates bounded backend progress/ETA and the Worker Agent forwards it under the exact Attempt/fence Heartbeat authority. | Production telemetry calibration and SLO receipts. |
| 0020 Job Expiry | Implemented for current lifecycle | Queue, retry, assignment, running, and finalization expiry are fenced by PostgreSQL time; recovery cannot extend the immutable deadline. | Scheduler and deployment receipts must preserve the ceiling. |
| 0021 Bounded retry | Partial | Attempt, cumulative compute, finalization recovery budgets, retry backoff, Job Expiry, and an immutable-policy Cross-Job failure-fingerprint circuit are enforced. | Measured and certified launch runtime values plus Production Gate fault receipts. |
| 0022 Hierarchical fairness | Implemented | PostgreSQL-authoritative Organization/Service Class/Project weighted-deficit selection, bounded Job score, per-Job retry risk, aging, Protected Lane, retry lane, durable claims, certification invalidation fencing, and multi-replica recovery are integrated. | Production fairness/SLO measurement remains a separate gate receipt. |
| 0023 Certified remediation | Partial | Slice 20 adds identity/epoch-bound L0-L7 operation ledger, two-person L6 approval, immutable receipts, Worker/Lease fencing, bounded deadlines with orphan recovery, same-epoch quarantine/identity guards, executor allowlist, and exact role/migration evidence. Commit `44543f0` adds guarded transport validation; commit `1c7a49f` adds `ControlPlaneAuthorizer` and `ControlPlaneLedger` adapters over the authoritative remediation operation/completion seams; commit `b25008a` passes the full immutable execution Plan into the runner for device/epoch/capability enforcement; commit `72e5462` adds a PostgreSQL-backed single execution claim and protobuf claim identity; commit `fa6622c` fixes controller-to-agent identity direction and adds the systemd Node Agent runtime, capability/fence/rate-limit/post-check enforcement, durable host receipts, an `EXECUTING` dispatcher, and migration `00021`; commits `0cc6a7c` and `b17b0cd` add exact migration `00022` claim replay, stable claim identity, durable pre-action intent, unknown-outcome quarantine, identity- and FailureClass-bound evidence, persistent rate limiting, atomic receipt publication, cross-process execution locking, secure local-file validation, and independent-pool concurrency evidence; commits `75981bd` and `754c275` require directory-fsync confirmation before receipt or intent replay and bind host execution to a validated inode with trusted Linux/Darwin path ancestry. Commit `ad1bcf6` exposes authenticated Platform Operator request, read, approval, and start paths; binds local and registered Worker epochs plus canonical GPU UUID, unique PCI BDF, failure class, action, and certification revision; and proves dispatcher-to-PostgreSQL quarantine after a certified post-check failure. | Live certificate/endpoint provisioning, hardware/topology capability certification, real host-action and post-check evidence, warm-up/canary, live claim/receipt monitoring, deployment evidence, and Launch Receipt. |
| 0024 Work-conserving capacity | Implemented for current control plane | Every compatible READY Worker/profile remains available to ordinary work; bounded retry lane, risk Admission, worker scoring, and physical-slot queue projection hold no hard idle reserve. Slice 23 adds Worker/pool-scoped scratch and Artifact Store hysteresis, current-epoch observation freshness, separate readiness/Assignment eligibility, and transactional Admission/Assignment rechecks without a hard spare. | Live Fleet deployment and measured SLO effect remain separate gate evidence. |
| 0025 Three control/storage nodes | Partial | `deploy/control-storage` renders a three-instance CNPG cluster, three-replica PVC-backed JetStream, required hostname anti-affinity, two-pod disruption budgets, independent WAL storage, and an external S3 backup contract. Slice 25 binds Publisher and durable Scheduler consumer behavior to that exact release-owned stream/consumer contract and proves quorum loss/recovery plus both Outbox and consumer crash windows in repository integration tests (`d275a91`). Slice 26 applies the release Cluster to a pinned four-node kind environment, verifies three distinct PostgreSQL failure domains, automatically replaces a stopped primary with RPO 0 and RTO below five minutes, and rejects new Admission/Assignment commits after both synchronous standbys stop. | RKE2/CNPG operator installation, approved image digests, three-node disk/topology validation, Artifact S3 durability, backup credentials, Barman Cloud Plugin migration/restore evidence, and a real-environment failover Launch Receipt. |
| 0026 Reserve credit at Admission | Implemented for current lifecycle | Admission reserves atomically; cancellation, execution/finalization failure, and Visible Completion consume or release exactly once with counters and Outbox. | Future terminal paths must close the same reservation authority. |
| 0027 Charge when cancel wins | Implemented | Visible Completion and Customer Cancellation serialize through one Job authority; the winner owns the only Charge and late completion returns the winning ArtifactSet. | Production fault-injection receipt remains a separate gate. |
| 0028 Recompute after Worker loss | Partial | LOST execution creates a higher-fence whole-Job retry; a circuit-opening failure can select a different actively certified profile without changing product snapshots; finalization loss is recovered on the same Attempt/fence by a Reconciler without resetting its deadline; Slice 21 local state is integrated into the Worker Agent and Python Runner with exact Worker/epoch/fence recovery, multipart finalization resume, per-Attempt quotas, watermarks, terminal cleanup, a UID-authenticated host XFS quota service, and an unprivileged H3 deployment contract. Slice 23 materializes identity-bound H3 Workers and requires five ordered readiness checks before a recovered or replacement Worker becomes `HEALTHY + READY`. Slice 30 proves real signed multipart resume after same-Worker Agent process loss and a distinct higher-fence replacement Attempt from an empty local root after the original Worker recovery root becomes inaccessible, with one Visible Completion, ArtifactSet, and Charge (`864c134`, review closure at `5a9bad6`). | Live H3 NVMe/XFS quota and capacity certification, physical node/NVMe-loss exercise, and Launch Receipt. |
| 0029 Evidenced Production Gates | Partial | Nine stable gate IDs and a strict receipt validator require release/configuration/environment/result/owner/threshold/observed-result/evidence bindings; missing, malformed, duplicate, or failed receipts cannot evaluate as PASS. | Nine actual versioned Launch Receipts from real certification, soak, fault, DR, rollback, lifecycle, and on-call exercises; current result is `0/9`. |

## Acceptance Coverage

The 30 scenarios in `docs/architecture.md` remain the completion authority. The
implemented slices provide direct repository evidence for Admission (1-3),
Customer Cancellation and its Visible Completion race (4-5), no-Charge failure
(6), Scheduler crash/fairness/pool isolation (7), stale execution and bounded
retry (8), Attempt progress (9), Artifact
recovery/immutability and whole-set validation (11-12), JetStream/Invoice outage
recovery with idempotent export (20), begin-finalization replay (21), Assignment
replay (24), PostgreSQL-time Lease behavior (25), immutable pricing/profile
retry behavior (14), immediate ProfileCertification invalidation (15), and
Webhook timeout/non-2xx/crash retry, signature, dead-letter, and authority
behavior (28), NATS cross-workload/subject denial plus Organization/Project data
isolation (27), and Credential rotation/revocation and Break-glass expiry with
immutable Principal/session attribution and BillingAdmin/OrganizationAuditor
content isolation (29). Slice 23 adds direct repository evidence for Artifact
Store/scratch capacity isolation and hysteretic recovery (16), exact protected
H3 resource shape plus admission/finalizer retirement authority (17), and
identity/device/backend/model-warm-up/canary readiness before Assignment (22).
Slice 24 adds direct repository evidence for a two-Attempt completion race with
one immutable Visible Completion and exact cleanup of the losing STAGING
Artifacts (13). Slice 25 adds direct repository evidence that only the exact
three-replica stream can produce an accepted PubAck, both Publisher crash windows
retain the stable `Nats-Msg-Id`, and the durable Scheduler consumer serializes
Assignment before receipt commit and confirmed ack under redelivery (23).
Slice 26 adds direct repository evidence that a stopped CNPG primary is
automatically replaced within five minutes without changing the committed Vela
authority, and that deferred database guards leave no Admission or Assignment
authority after synchronous quorum loss and recovery (26).
Slice 27 adds direct repository evidence for the authenticated remediation
control path, distinct L6 approvals, exact Worker epoch and GPU UUID/PCI BDF
capability binding, and authoritative quarantine after failed post-check (18).
Slice 28 adds repository evidence that ProjectAdmin-authorized failure debug
dumps remain isolated from Artifacts and Charge authority, are uploaded only
under the current Worker/Lease identity, and expire or delete through
exact-version and incomplete multipart cleanup without blocking the authoritative
failure transition (19).
Slice 29 adds direct repository evidence that exact adjacent N-1 control,
Admission, Outbox, Scheduler, and Worker binaries coexist with schema 27 and
current consumers; raw retained event identity reaches the durable current
Inbox, an active N-1 Worker continues through current Fleet drain, exact N-1
rollback writers retain authority, and current plus N-1 Admission/Scheduler
writers fail closed without new authority during synchronous-quorum loss (30).
Slice 30 adds direct repository evidence that a same-Worker process loss resumes
only the remaining signed multipart parts without rerunning the Runner, while an
inaccessible Worker-local recovery root leads to a `LOST` Attempt and a distinct
higher-fence replacement Worker that recomputes from an empty local root; both
paths preserve one Visible Completion, ArtifactSet, and Charge (10).
Slice 31 adds direct repository evidence that committed Artifact Content
Deletion and automatic expiry create independent backup targets; PRIMARY exact
versions, multipart uploads, and all backup versions/delete markers are removed;
exact N-1 retention remains primary-only; and a PostgreSQL snapshot restored
after deletion authority replays to complete without resurrecting content while
retaining prompt tombstone, Charge, and actor attribution (19).
The repository coverage is now 30 direct, 0 partial, and 0 unproven.

## Production Gates

Current result: `0/9 PASS`. No production traffic is authorized by this document.
