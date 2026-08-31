# H3 Stage Execution Implementation Slices

Date: 2026-08-31

Status: In progress. S49.1-S49.11 have committed repository implementations.
S49.12 has started with migrations `00049` and `00050` and remains partial: cutover routing,
immutable execution authority, scoped internal rollout, Production Launch
Receipt gating, legacy database inventory, and rollback protection exist;
automatic Stage Job instantiation now has durable multi-replica claims, expiry
takeover, exact replay, crash reconciliation, and `vela-control` wiring. External
drain evidence, zero-inventory seal, contraction, legacy-path deletion, and
production evidence remain pending.

## Delivery rule

Each slice is a tracer bullet with a closed Interface, migration/evidence
boundary, tests, documentation, and local commit. A slice may add transitional
compatibility but may not create a permanent second H3 product path. Production
activation is later than repository completion and remains gated by Launch
Receipts.

No slice may put model loading on StageAssignment, let multiple WorkerInstances
share one GPU, treat Kubernetes/JetStream/Redis as Job authority, expose L2 to
customers, or change the fixed customer Charge.

## Dependency map

```text
S49.1 contracts and graph validator
  +--> S49.2 WorkerInstance / DeviceSet / residency inventory
  +--> S49.3 parent Attempt / StageRun authority
          +--> S49.4 deterministic stage scheduler and shadow replay
          +--> S49.5 Stage Worker and ModelRuntime protocols
                  +--> S49.6 L2 StageArtifact materialization and transfer
                          +--> S49.7 split H3 Encoder / DiT / VAE execution
                                  +--> S49.8 exact cache and pin/eviction
                                  +--> S49.9 CPU media StageWorker
          +--> S49.10 Usage/Cost Ledger

S49.4 + S49.6 + S49.7 + S49.8 + S49.9 + S49.10
  +--> S49.11 trace-driven capacity and advisory residency planning
          +--> S49.12 legacy H3 contraction and production evidence campaign
```

## S49.1: Catalog contracts and graph validator

Deliver:

- immutable graph, stage, interface, profile, equivalence, connector, runtime,
  cache, checkpoint, WorkerProfile, and cost revision schema;
- a pure `ValidateExecutionGraph` Interface returning canonical topological
  order, content digest, or bounded reason codes;
- a minimal H3 Encoder -> DiT -> VAE graph fixture whose DiT profile is
  single-GPU;
- activation transaction that rejects cycles, incompatible edges, missing
  output paths, unbounded fan-out, or incomplete certification.

Acceptance:

- property tests cover graph order and invalid structures;
- active revisions are immutable;
- no runtime or Assignment behavior changes;
- current and exact N-1 tests pass on the expanded schema.

## S49.2: WorkerInstance, DeviceSet, and ModelResidency

Deliver:

- ComputeNode, Device, DeviceSet, WorkerInstance, WorkerMember, WorkerBundle,
  ModelResidency, CapacityPool, and freshness-bound CapacityObservation schema;
- `WorkerRegistryAndFleet` Interface and Kubernetes actuator Adapter;
- Node Agent evidence for GPU UUID/PCI BDF/device epoch and exclusive binding;
- per-GPU H3 desired layout plus explicit Encoder/VAE AUX exception;
- advisory-only ResidencyProposal with no automatic model repurpose.

Acceptance:

- concurrent attempts to bind one GPU to two WorkerInstances fail;
- normal Scheduler roles cannot mutate residency;
- Agent reconnect preserves ModelRuntime epoch, while runtime/device/model
  changes invalidate old authority;
- a future multi-member fixture proves complete membership fencing without
  implementing an LLM backend;
- short-term demand cannot release a healthy resident ModelRuntime; the
  explicit release path requires approved reason and break-even evidence.

## S49.3: Parent Attempt and StageRun authority

Status: Runtime authority is implemented in migration
`00036_stage_attempt_authority.sql` and the
`internal/attemptcoordinator` Module, with automatic dispatch added by migration
`00050_stage_graph_instantiation_dispatch.sql`. PostgreSQL owns instantiate, assign,
first progress, completion, retry, cancellation, and bounded reconciliation;
state-transition and identity triggers reject owner-level authority rewrites.
Integration coverage includes duplicate replay, same-parent retry, exact-cache
progress, queued and billable cancellation, cancel/progress serialization,
late-progress fencing, Admission-triggered automatic instantiation, concurrent
replica exclusion, expired-claim takeover, post-commit crash reconciliation,
v49 backfill/resume, and empty/durable-authority migration rollback behavior.
The production Admission requirement to create the graph snapshot, initial
Attempt/StageRuns, and storage reservation in the Admission transaction remains
an explicit S49.12 blocker; migration `00050` persists exact work in that
transaction and materializes those rows in a later claimed transaction.

Deliver:

- ExecutionGraphSnapshot, StageRun, StageAttempt, StageAllocation, StageLease,
  dependency, retry budget, and storage reservation schema;
- deep `AttemptCoordinator` Module with instantiate/apply/cancel/reconcile
  Interface;
- graph state machine, immutable Attempt fence, mutable Job current fence, and
  independently incremented StageRun fence;
- first-progress Billable Start for physical start and exact cache advance;
- one active StageAttempt invariant.

Acceptance:

- restart/reconcile recreates every READY transition from PostgreSQL;
- duplicate commands replay without duplicate StageAttempt or Billable Start;
- cancel versus start/commit races are fenced;
- stage retry reuses completed upstream identity in the same parent Attempt;
- existing Charge and Visible Completion tests continue to pass.

## S49.4: Deterministic stage scheduler

Status: Repository implementation complete in migration
`00037_stage_scheduler.sql` and the `internal/stagescheduler` Module. The
versioned decision kernel, durable acquire/claim/reconcile path, immutable
decision and shadow-replay evidence, bounded metrics, dedicated database role,
and `vela-control` maintenance wiring have unit, PostgreSQL integration,
migration round-trip, and deployment-contract coverage. This repository
evidence does not constitute a Launch Receipt or pass a Production Gate.

Deliver:

- static, versioned `Filter -> Fairness -> Score -> Pick` implementation behind
  one `acquire` Interface;
- stage-specific durable READY queues and CapacityPool counters;
- bounded reason taxonomy, score components, input digest, and deterministic
  tie-break evidence;
- hierarchical Organization/ServiceClass/Project fairness using calibrated
  resource-seconds within each CapacityPool;
- shadow replay against captured traces before actual selection.

Acceptance:

- identical snapshots reproduce winner and evidence digest;
- stale capacity/model/device/member evidence fails closed;
- cache/locality never makes an ineligible Worker eligible or bypasses fairness;
- crash between decision and acquire expires cleanly without double assignment;
- high-cardinality identities do not enter metric labels.

## S49.5: Stage Worker and local ModelRuntime protocols

Deliver:

- new versioned StageWorkerControl protobuf and Worker Agent stream;
- local ModelRuntime Interface without load/unload/replace commands;
- signed StageAuthority with attempt/stage fences and all epochs;
- monotonic local deadline watchdog, reconnect, and same-runtime reattach;
- independent fake Encoder, DiT, and VAE runtimes for conformance.

Acceptance:

- stale authority cannot prepare, start, heartbeat, seal, or reattach;
- control connection restart can reattach only to unchanged ModelRuntime;
- Worker restart/model reload changes epoch and rejects old work;
- multi-member start barrier fails the entire allocation on missing member;
- cancellation signal acknowledgement and actual runtime stop are distinct.

## S49.6: L2 StageArtifact and transfer

Status: Repository implementation complete in migration
`00038_stage_artifact.sql` and the `internal/stageartifact`,
`internal/stageworkeragent`, and `internal/stageworkercontrol` Modules. Sealed
output replay, durable local recovery journal, source-loss reporting, immutable
conditional publication, exact-version transfer, storage-reservation checks,
and edge-credit release have unit and PostgreSQL integration coverage. Local
journal saturation rejects new Assignment before ModelRuntime execution. This
repository evidence does not activate split H3 production traffic and is not a
Launch Receipt.

Deliver:

- immutable StageArtifact, lineage, ExecutionPin, buffer credit, storage
  reservation, TransferTicket, and StageMaterializationLease schema;
- versioned object-store Adapter plus local test Adapter;
- seal-local-output, release-GPU, materialize, verify, commit, and downstream
  pin transaction path;
- exact-version object-store pull Connector.

Acceptance:

- downstream remains BLOCKED until durable L2 commit;
- L2 outage retries materialization without holding or rerunning GPU compute;
- local node loss before L2 commit retries compute;
- conditional publication prevents overwrites and stale completion;
- buffer and storage limits remain bounded under downstream outage;
- direct object credentials never reach ModelRuntime.

## S49.7: Split H3 execution

Deliver:

- production-shaped Encoder, DiT, and VAE StageProfiles and runtime Adapters;
- seven independent single-GPU DiT WorkerInstances in the same-node test layout;
- cross-node placement using the identical graph and protocol;
- Encoder/VAE AUX profile only as an explicit one-slot exception;
- phase mapping and final StageArtifact handoff to existing Finalizer.

Acceptance:

- same-node and cross-node output equivalence passes the certified corpus;
- no DiT Assignment requests more than one GPU;
- Encoder, DiT, and VAE capacities can be changed independently;
- failure of one DiT Worker does not fence unrelated Workers or discard a
  committed Encoder Artifact;
- finalization failure does not rerun VAE.

## S49.8: Exact cross-Job cache

Deliver:

- scoped HMAC `StageCacheKeyV1`, StageResultEquivalenceRevision enforcement,
  StageCacheEntry, strong ExecutionPin, weak CacheReference, TTL/quota, and
  deterministic shadow eviction;
- default Project scope, explicit Organization authorization, and permanent
  cross-Organization denial;
- cache hit transaction integrated with graph advancement and Billable Start;
- Content Deletion and policy-disable behavior.

Acceptance:

- every key field has a positive and negative equivalence test;
- tolerance-only or quality-only cross-profile equivalence is rejected;
- Project and Organization isolation fail closed under forged metadata;
- pin versus eviction/deletion races cannot return a deleted exact version;
- cancellation blocks late cache insertion;
- same-Attempt recovery remains available when cross-Job cache is disabled.

## S49.9: CPU media StageWorker

Deliver:

- CPU WorkerProfile with bounded CPU, memory, scratch, and concurrency vector;
- content-changing encode, mux, and thumbnail work as optional StageRuns;
- Artifact Finalizer remains a separate non-graph authority;
- stage-specific retry, telemetry, and cost receipts.

Acceptance:

- CPU saturation backpressures upstream through buffer credit and Admission;
- CPU retry does not rerun GPU stages;
- finalizer cannot perform untracked content-changing computation;
- required multi-output ArtifactSet remains indivisible.

## S49.10: Usage/Cost Ledger

Status: Repository implementation committed as `a691917` in migration
`00047_usage_cost_ledger.sql` and the `internal/usagecostledger` Module. It adds
append-only direct, shared, and counterfactual usage evidence, versioned rational
cost allocation, content-free bounded summaries, and an exact-function database
role. Unit, targeted PostgreSQL integration, role-confusion, migration rollback,
and generated-code verification pass. This evidence does not change the fixed
Customer Charge and is not a Launch Receipt.

Deliver:

- immutable ResourceUsageRecord ingestion and idempotent reconciliation;
- versioned CostModelRevision and append-only CostAllocationRecord;
- direct, shared residency, load/warm-up, retry, cancellation, transfer, storage,
  and finalization usage classes;
- cache avoided-compute as separate counterfactual evidence;
- operator summaries with no Customer Content.

Acceptance:

- retries/replays do not double-count usage;
- revaluation preserves original usage and customer Charge;
- fixed quote and Charge are unchanged by cache or placement;
- direct and shared allocated cost remain separately visible.

## S49.11: Capacity simulation and advisory planning

Status: Repository implementation complete in `internal/capacitysim` and
`cmd/vela-capacity-sim`. Strict content-free inputs, deterministic receipts,
randomized conservation/pin/bounds tests, analytical fixtures, per-stage error,
transfer sensitivity, comparison, owner-only CLI outputs, a checked-in synthetic
H3 example, and a production-dependency negative test pass. Advisory proposals
remain `auto_apply=false` and have no Fleet/Kubernetes authority. This evidence
is not real H3 calibration, a production capacity recommendation, or a Launch
Receipt.

Deliver:

- spec 0051 trace schema, deterministic discrete-event simulator, and replay
  CLI;
- stage queues, transfers, buffers, cache, failures, warm residency, and cost;
- advisory ResidencyProposal output with input digest, confidence, expiry,
  min/desired/max, cooldown, budget, and reason codes;
- shadow comparison to actual traces.

Acceptance:

- simulator determinism and conservation invariants pass;
- calibration error is reported per stage and cohort, not hidden in aggregate;
- proposal cannot actuate Kubernetes;
- sensitivity includes transfer at multiples of measured values even when
  transfer is expected not to dominate.

## S49.12: Cutover, contraction, and evidence campaign

Current repository boundary: migrations `00049` and `00050`, Admission, and the
AttemptCoordinator maintenance loop implement the pre-contraction control and
automatic-instantiation surfaces. They
do not authorize production activation, prove real Worker-local or N-1 drain,
make the monolithic path unreachable, or close the target single-transaction
Admission graph-instantiation boundary.

Deliver:

- staged cohort cutover and rollback-before-contraction controls;
- legacy authority inventory, zero-backlog receipt, guarded schema contraction,
  and permanent reachability negative test;
- deletion of machine-level H3 WorkerAssignment/Runner/deployment paths;
- mixed-load soak, stage/network/storage fault injection, model N/N-1 drain,
  rollback, dashboards, alerts, runbooks, and ownership;
- Production Gate evidence inputs bound to graph/profile/connector/release
  identities.

Acceptance:

- no nonterminal legacy authority exists before contraction;
- no in-flight Job is translated between authority kinds;
- same-node remains available through stage placement;
- `rg`, schema inspection, protocol breaking checks, and runtime negative tests
  find no reachable monolithic H3 path;
- Production Gate status changes only through valid Launch Receipts.

## Global stop conditions

Pause activation, without unloading healthy resident models, when any of these
is true:

- output equivalence is unproven for a stage/profile/connector combination;
- L2 capacity or deletion authority cannot cover Accepted Jobs;
- scheduler replay diverges from persisted evidence;
- stage retry, cancellation, or finalization can duplicate Charge or Visible
  Completion;
- cross-Project cache isolation cannot be proven;
- model/runtime/device/member epochs cannot fence stale execution;
- simulator calibration is being used as measured SLO evidence;
- legacy contraction inventory is nonzero.
