# Stage Execution Schema And Protocol Migration

Date: 2026-08-29

Status: Implementation in progress. S49.1 implements the immutable Catalog
subset, graph activation transaction, generated sqlc models, and migration
evidence. Runtime authority, resource, Artifact, usage, and protocol expansion
remain pending.

## Purpose

This specification turns the accepted H3 stage architecture into a database,
protocol, and cutover proposal. It replaces the target semantics of specs 0002,
0003, 0004, 0005, 0007, 0008, 0022, 0023, 0024, and 0030 where those specs bind
one Attempt to one machine-level Worker. Their committed evidence remains valid
for the current baseline until the cutover described here.

The migration must preserve Organization Isolation, Admission credit
reservation, Billable Start, Customer Cancellation, Retention Policy, Content
Deletion, one Charge, and exactly-once Visible Completion.

## Confirmed baseline

At commit `bc590e20b3e81ee54651ac7766c8ecd82b394097`:

- `db/migrations/00002_worker_assignment.sql` requires every `attempts` row to
  contain `execution_profile_revision_id`, `worker_pool_id`, `worker_id`, and
  `worker_epoch`;
- `attempt_leases` binds directly to that Worker and Attempt;
- `proto/vela/v1/worker_control.proto` assigns `WorkerAssignment` and uses
  `WorkerLeaseCredentials` containing only Attempt and Worker authority;
- `proto/vela/v1/runner.proto` exposes `RunnerAttemptIdentity` and an
  end-to-end `RunnerExecutionSpec`;
- no StageRun, WorkerInstance, DeviceSet, ModelResidency, StageArtifact, cache,
  or internal cost authority exists.

This is a replacement migration. It is not a claim that those fields already
have stage semantics.

## Target ownership graph

```text
Job
 +-- ExecutionGraphSnapshot
 +-- Attempt (immutable attempt fence; valid while equal to Job.current_fence)
      +-- StageRun (logical node, independently incremented stage fence)
           +-- StageAttempt (physical try)
                +-- StageAllocation
                +-- StageLease
                +-- ResourceUsageRecord
           +-- input ExecutionPins
           +-- winning StageArtifact

ComputeNode
 +-- Device
 +-- WorkerBundle
      +-- WorkerInstance --exclusive--> DeviceSet
           +-- WorkerMember(s)
           +-- ModelResidency
           +-- CapacityObservation
```

## Catalog schema

All Catalog rows are immutable after activation. Draft revisions may be
replaced; active revisions are retired by creating successors and changing
Catalog status, never by editing contract fields.

| Table | Required identity and contract |
| --- | --- |
| `execution_profile_revisions` | existing Job-level internal execution envelope, migrated from one WorkerPool to one graph plus certified option sets |
| `execution_graph_revisions` | organization-independent Catalog id, model revision, graph schema version, output contract, public phase map, status, content digest |
| `stage_interface_revisions` | payload kind, dtype, layout, shape contract, serialization, max bytes, digest algorithm, schema digest |
| `stage_definition_revisions` | stage kind, input/output interface ids, resource class, retry class, cache/checkpoint policy ids, public phase |
| `execution_graph_stages` | graph id, stable stage key, definition id, required flag, bounded fan-out; unique `(graph_id, stage_key)` |
| `execution_graph_edges` | graph id, source key/port, destination key/port, buffer class; unique edge identity and no self-edge |
| `stage_profile_revisions` | stage definition, backend/model component, runtime image, WorkerProfile, certified capacity vector, result-equivalence id |
| `execution_profile_stage_options` | execution profile, graph stage key, allowed stage profile, preference/eligibility metadata; unique option identity |
| `execution_profile_connector_options` | execution profile, graph edge, allowed connector, required/preferred topology policy |
| `stage_result_equivalence_revisions` | exact model/preprocessor/backend/kernel/precision/RNG/interface equivalence digest and evidence receipt |
| `input_canonicalization_revisions` | lossless canonical encoding, exact equivalence rules, test corpus digest, and activation evidence |
| `connector_revisions` | source/destination interface, transport, required/preferred topology, integrity, security, L2 fallback, limits |
| `worker_profile_revisions` | DeviceSet shape, member topology, resident models, runtime identities, capacity limits, readiness checks |
| `stage_runtime_model_revisions` | request cohort schema, per-stage service/output distributions, transfer model, evidence digest |
| `stage_cache_policy_revisions` | allowed stage keys, scope ceiling, TTL, quotas, encryption/deletion behavior |
| `checkpoint_policy_revisions` | resume format, compatibility, interval, max overhead, evidence digest |
| `cost_model_revisions` | resource valuation units, effective window, allocation method, evidence digest |

Activation validates graph acyclicity, stable topological order, connected
required outputs, interface compatibility on every edge, bounded fan-out,
complete certified profile alternatives, connector fallback, and final output
compatibility. The dedicated `vela_stage_catalog_activation` role can execute
only that activation function; the existing five-function
`vela_catalog_promotion` boundary remains unchanged for exact N-1 startup.
Runtime code never reinterprets malformed active JSON.

The existing `execution_profile_revisions.worker_pool_id` remains populated for
legacy profiles during expansion. A target profile instead references one
ExecutionGraphRevision and has complete stage/connector option rows. A
transitional constraint prevents one active profile from mixing the two
authority shapes. Contraction removes the legacy WorkerPool binding after all
legacy Jobs and N-1 readers drain.

## Admission snapshot schema

`execution_graph_snapshots` freezes all execution-affecting authority for one
Accepted Job:

- selected ExecutionProfileRevision and graph revision;
- graph revision and ordered stage/edge digest;
- allowed StageProfile and Connector revisions per stage/edge;
- runtime, cache, checkpoint, retry, storage, and cost attribution policy ids;
- region, security, data-residency, and topology constraints;
- per-stage attempt caps and Job-global resource budget;
- StageRuntimeModelRevision used for Admission and initial ETA;
- snapshot schema version and canonical digest.

`jobs.execution_graph_snapshot_id` is non-null for every new-path Job. Snapshot
rows use the same Organization/Project ancestry as the Job even when their
referenced Catalog definitions are global.

The existing `ExecutionPolicySnapshot` remains immutable Job policy authority
for retry, expiry, customer-visible contract, and related controls. It references
the resolved ExecutionGraphSnapshot; the graph snapshot does not duplicate or
replace those business-policy fields.

Admission validates at least one complete READY capacity path and creates one
`stage_storage_reservations` row in the same transaction as Job, Credit
Reservation, queue counters, graph snapshot, and initial Attempt/StageRuns.

## Runtime execution schema

### Parent Attempt

The target `attempts` row retains Job ancestry, attempt number, state, immutable
`fence`, retry accounting, and finalization authority. Its authority remains
valid only while `attempts.fence = jobs.current_fence`; incrementing the Job
fence invalidates the complete graph without mutating the Attempt identity. The
target row no longer owns Worker, WorkerPool, Worker epoch, or a single
ExecutionProfile.

The target parent Attempt state family is `QUEUED`, `RUNNING`, `FINALIZING`,
`SUCCEEDED`, `FAILED`, and `CANCELED`. Admission creates QUEUED; first effective
graph progress moves it to RUNNING. Legacy `ASSIGNED` and `LOST` meanings are
retained only until the legacy path contracts. Physical loss belongs to
StageAttempt and does not force a parent LOST state.

During expansion the legacy columns remain for old Jobs. Stage-path code treats
them as forbidden and uses a database constraint tying their nullability to an
explicit `execution_authority_kind`:

```text
LEGACY_WORKER -> legacy worker/profile fields are all non-null
STAGE_GRAPH   -> execution_graph_snapshot_id is non-null and legacy fields are null
```

This discriminator is transitional and is deleted with the legacy path after
contraction.

### Stage tables

| Table | Key fields and constraints |
| --- | --- |
| `stage_runs` | ancestry, attempt id, stage key, definition/profile set, state, stage fence, retry count, next retry, winner StageAttempt/Artifact, version; unique `(attempt_id, stage_key)` |
| `stage_dependencies` | StageRun source/destination plus required input port and satisfied Artifact id; no cross-Attempt edge |
| `stage_attempts` | StageRun, physical attempt number, state, selected profile, failure class/fingerprint, start/seal/finish, resource totals; unique attempt number |
| `stage_allocations` | StageAttempt, WorkerInstance, DeviceSet digest, member digest, worker/model/device epochs, capacity vector, allocated/released timestamps |
| `stage_leases` | StageAttempt, attempt/stage fences, allocation identity, token digest, signing key, issued/expiry/deadline, phase; immutable identity |
| `stage_materialization_leases` | StageAttempt, local receipt digest, node/agent epoch, token digest, issued/expiry, state |
| `stage_decision_evidence` | snapshot digest, policy revision, bounded reason counts, score terms, chosen candidate, tie-break, expiry |

StageAttempt state is `ASSIGNED`, `RUNNING`, `OUTPUT_SEALED`, `SUCCEEDED`,
`FAILED`, `LOST`, or `CANCELED`. Partial unique indexes treat ASSIGNED, RUNNING,
and OUTPUT_SEALED as active and enforce at most one active StageAttempt and one
live StageLease per StageRun in the first release. StageAllocation release is
idempotent and independent from StageArtifact commit; StageAttempt SUCCEEDED
still requires the winning durable StageArtifact.

The database validates the normative StageRun transitions from
`docs/h3-stage-execution-architecture.md`. Terminal states never reopen. A
retry transaction terminates the old StageAttempt, consumes both per-stage and
global resource budget, increments the StageRun fence, and either enters
RETRY_WAIT or fails the graph.

## Resource and residency schema

| Table | Key fields and constraints |
| --- | --- |
| `compute_nodes` | region, network/fault domains, attested node identity, lifecycle epoch |
| `devices` | node, GPU UUID, PCI BDF, type, health, device epoch; unique physical identity |
| `device_sets` | immutable Device membership digest and topology digest |
| `worker_instances` | profile, lifecycle, reachability, epoch, DeviceSet, bundle, control session, capacity pool |
| `worker_members` | WorkerInstance, node, member key, identity, epoch, readiness; unique member key |
| `active_device_bindings` | Device to WorkerInstance/epoch; unique active row per Device |
| `model_residencies` | WorkerInstance, model component revision, runtime identity, model runtime epoch, state, warm-up/canary evidence |
| `worker_bundles` | node, ResidencyPlanRevision, desired/observed generation, lifecycle |
| `capacity_pools` | stage/resource/security/region/certification class and queue limits |
| `capacity_observations` | WorkerInstance epoch, sequence, vector, observed/expiry timestamps |

`READY` requires a complete WorkerProfile, DeviceSet, membership, model
residency, runtime, warm-up, capacity, and certification match. A stale or
missing observation fails closed. Dynamic capacity cannot exceed certified
static limits.

No Assignment transaction may insert or alter ModelResidency. Fleet mutation
and Stage scheduling use separate database roles and stored command Interfaces.
Exact placement is recorded as ComputeNode plus GPU UUID/PCI BDF membership in
DeviceSet and WorkerMember rows. Constraint-based placement is READY only after
the Fleet Adapter resolves and attests those exact identities.

## StageArtifact and cache schema

| Table | Key fields and constraints |
| --- | --- |
| `stage_artifacts` | ancestry/scope, producer StageAttempt, interface, exact object key/version, digest, bytes, lineage digest, state, expiry, deletion fence |
| `stage_artifact_inputs` | producer Artifact to exact input Artifact/root-input digest relation |
| `stage_artifact_pins` | Artifact, pin kind, owner Job/StageRun, state, acquired/released; live ExecutionPin blocks delete |
| `stage_cache_entries` | Project/authorized Organization scope, key HMAC, equivalence, Artifact exact version, state, TTL; unique live scoped key |
| `transfer_tickets` | Artifact/version, destination Worker/model epochs, connector, token digest, issued/expiry, outcome digest |
| `durable_checkpoints` | Job/StageRun, step, resume compatibility, exact object version, RNG/state digest, expiry |
| `edge_buffer_credits` | edge capacity class, count/bytes held, owner StageRun/Artifact, acquired/released |
| `stage_storage_reservations` | Job, reserved/consumed bytes, policy, expiry, release state |

Cache lookup and strong pin acquisition are one transaction. Ordinary eviction
locks the Artifact and rechecks no live ExecutionPin before marking the exact
version for deletion. Content Deletion tombstones scope before new pins and
serializes exact-version purge with running executions.

## Usage and cost schema

`resource_usage_records` is append-only and idempotent on
`(source_kind, source_identity, resource_kind, interval_or_receipt_digest)`.
Required dimensions are bounded enums plus Organization/Project/Job/Attempt/
StageAttempt or CapacityPool ancestry kept outside Prometheus labels. Quantities
use explicit integer base units such as GPU nanoseconds, CPU nanoseconds,
byte-nanoseconds, bytes, and object operations.

`cost_allocation_records` references one CostModelRevision and immutable usage
inputs. Revaluation inserts a successor allocation; it cannot update Charge,
PricingSnapshot, or source usage.

## Authoritative transactions

Only narrow command functions or repositories may perform these multi-row
changes:

1. `instantiate_graph`: create parent Attempt, StageRuns, dependency rows,
   initial READY roots, pins, reservations, and outbox wakeups.
2. `acquire_stage`: lock one eligible StageRun and WorkerInstance, revalidate
   `Filter -> Fairness -> Score -> Pick` evidence, then create StageAttempt,
   StageAllocation, and StageLease.
3. `start_stage`: validate all fences/epochs and atomically set Billable Start
   if it is the first effective graph progress.
4. `advance_exact_cache_hit`: revalidate cache and deletion authority, create
   ExecutionPin, set Billable Start if first progress, succeed the StageRun, and
   unblock dependents.
5. `seal_stage_output`: persist the local receipt and release compute capacity;
   optionally issue StageMaterializationLease.
6. `commit_stage_artifact`: conditionally publish exact object metadata,
   select the StageRun winner, acquire downstream pins/credits, and unblock
   dependents.
7. `fail_stage`: terminate physical authority, consume budgets, update circuit
   evidence, and schedule retry or graph failure.
8. `cancel_graph`: increment `Job.current_fence`, revoke all active leases/tickets,
   cancel nonterminal StageRuns, settle cancellation billing, and emit stops.
9. `finalize_graph`: claim the winning final StageArtifact and execute the
   existing atomic Visible Completion transaction without GPU authority.

Every command accepts an idempotency identity and expected version/fence and
returns an explicit replay/stale/accepted/rejected result.

## External Worker control Interface

A new `StageWorkerControlService` is introduced in a new protobuf file and
package version. The current `WorkerControlService` is not extended into a
mixed message whose meaning depends on optional fields.

The bidirectional stream has a closed operation family:

```text
Worker -> Control
  RegisterWorkerEvidence
  ReportCapacityObservation
  AcquireStage
  StartStage
  HeartbeatStage
  SealStageOutput
  CommitStageMaterialization
  FailStage
  ReattachStage

Control -> Worker
  WorkerReadinessDecision
  StageAssignment | NoWork
  StageCommandResult
  StopStage
  MaterializationAuthority
```

`StageAuthority` contains Job, Attempt, StageRun, StageAttempt, attempt/stage
fences, WorkerInstance/model/device/member epochs, StageProfile, execution
nonce, token, issuance, absolute PostgreSQL expiry, and relative monotonic
validity. `StageAssignment` contains exact input Artifact identities plus
destination-bound TransferTickets; it never contains general object-store
credentials.

Multi-member Workers register independent member identities and readiness
evidence. One StageAssignment is accepted only after the WorkerInstance
membership digest and backend start barrier match. Rank-level execution remains
behind the ModelRuntime Interface.

## Local ModelRuntime Interface

Worker Agent uses a versioned local `ModelRuntimeService`:

```text
ProbeReadiness(worker_runtime_identity, check) -> evidence
PrepareStage(stage_authority, execution_spec)  -> prepared | rejected
StartStage(stage_authority)                    -> started | rejected
CancelStage(stage_authority, reason)           -> accepted | stale
Status(stage_authority)                        -> bounded status
SealOutput(stage_authority)                    -> local materialization receipt
```

There is intentionally no `LoadModel`, `UnloadModel`, or `ReplaceModel` method.
Those operations belong to the Fleet residency Adapter after drain. The local
runtime watchdog fences work at the monotonic lease deadline when control-plane
renewal is unavailable.

## Migration sequence

### M0: freeze and inventory

- Record exact schema/protocol/release identities and legacy nonterminal counts.
- Add reachability tests proving every current legacy Assignment entry point.
- Freeze the graph, cache, protocol, reason taxonomy, and migration receipts.

### M1: expand without activation

- Add Catalog, graph, stage, resource, Artifact, usage, and evidence tables,
  roles, RLS, constraints, and generated types.
- Keep current legacy NOT NULL behavior and all current writers passing.
- Add graph validation and trace capture in read-only/shadow operation.

### M2: deploy dual-capable current control

- Current binaries read nullable transitional fields safely but still create
  only legacy Jobs.
- Deploy new Stage Worker protocol, split mock runtimes, and per-GPU Fleet
  inventory with Assignment disabled.
- Prove exact N/N-1 behavior before any stage graph can be Activated.

### M3: transition schema authority

- After every old control writer is drained, relax legacy Attempt columns and
  activate the `execution_authority_kind` constraint.
- Activate one certified H3 graph and stage profiles for an internal cohort.
- Never allow an N-1 binary to claim or interpret a `STAGE_GRAPH` Job.

### M4: cohort cutover

- Admit new H3 Jobs only to the stage path for successively larger cohorts.
- Legacy Jobs keep legacy binaries and frozen authority until terminal.
- Rollback may change only the path for new Jobs; in-flight authority is never
  converted.

### M5: full cutover and drain

- Stop all new legacy Admission.
- Drain nonterminal legacy Attempts, Leases, finalization, uploads, outbox,
  retention, recovery, and N-1 backlog.
- Seal a cutover receipt with zero legacy authority and a bounded observation
  window.

### M6: contract and delete

- Remove legacy protocol handlers, scheduler paths, deployment profiles, and
  machine-level H3 tests.
- Drop legacy Attempt/Lease Worker fields and transitional discriminator only
  after the contraction guard rechecks the sealed receipt and live database.
- Add a permanent negative test proving no machine-level H3 Assignment can be
  formed.

## Failure and compatibility rules

- There is no in-flight conversion between legacy and stage authority.
- Old StageLease, TransferTicket, materialization, cache, and completion writes
  fail on any attempt/stage/Worker/model/device/member epoch mismatch.
- Control loss cannot extend a local execution deadline.
- L2 outage prevents downstream READY and closes new Admission, but does not
  require immediate GPU recompute after a sealed local receipt.
- Kubernetes, JetStream, Redis, and telemetry loss cannot create or advance
  execution authority.
- Schema rollback after the point of no return is refused when stage authority
  or sealed cutover evidence exists.

## Required verification

- property/state-machine tests for graph activation and every transition;
- RLS and composite ancestry negative tests for all new tables;
- concurrent acquire, cache pin/evict, cancel/commit, retry/late result, and
  finalization winner races;
- protocol conformance for replay, stale epochs, reattach, monotonic deadline,
  and multi-member barrier;
- exact N/N-1 expansion and pre-activation rollback tests;
- same-node and cross-node H3 with seven independent single-GPU DiT Workers;
- target object-store versioning, conditional publication, deletion, and L2
  outage tests;
- cutover, drain, rollback-before-contraction, and guarded contraction drills;
- `make generate`, unit, integration, race, lint, protobuf/OpenAPI breaking,
  migration down/up, deployment render, and release-bundle checks.

Repository tests do not satisfy any Production Gate without a versioned Launch
Receipt bound to the target cluster, model, graph, connector, and release.
