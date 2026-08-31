# H3 Stage Execution Architecture

| Attribute | Value |
| --- | --- |
| Status | Accepted target; S49.12 cutover control started; automatic instantiation and contraction pending |
| Date | 2026-08-29 |
| Workload | MiniMax H3 asynchronous video generation |
| Baseline commit | `bc590e20b3e81ee54651ac7766c8ecd82b394097` |
| Production status | `0/9 PASS`; this design is not a Launch Receipt |

## 1. Normative relationship

This document replaces the H3 execution, placement, Worker, retry, and
intermediate-data assumptions in `docs/architecture.md`. It does not replace
Vela's commercial, identity, retention, disaster-recovery, or exactly-once
Visible Completion contracts.

The implementation at the baseline commit still binds one Attempt to one
machine-level Worker. The target design instead uses a durable execution graph
whose stages are assigned independently. Until the implementation slices in
this package are complete, repository behavior remains the current behavior and
must not be described as stage-disaggregated or production-ready.

The design borrows routing, stage-pipeline, topology, cache-index, planner, and
observability patterns from NVIDIA Dynamo and llm-d. Neither project becomes
Vela's Job, Lease, billing, or Artifact authority. PostgreSQL remains the
durable control-plane source of truth.

The companion research fixed the inspected upstream revisions at Dynamo
`4c7e98162232d24147daa701d6bfcf93f2fa4edf`, llm-d
`1f97eb0f928fd3c6509fce984ad67af83d65d3e8`, llm-d Router
`644a885639ac64ca09d6f35af3a67fe61bcc2e31`, and llm-d KV Cache Manager
`8cf43067afb7fc9fefafc1b64de063c769f2c90f`. The research guide's initial
recommendation to retain one monolithic H3 Worker depended on the old,
incorrect assumption that the seven DiT GPUs formed one multi-GPU execution.
The confirmed topology in section 4 supersedes that recommendation.

## 2. Decision summary

1. H3 Encoder, DiT, and VAE Decoder are independently executable single-GPU
   model components. Each DiT GPU runs an independent process; H3 DiT is not a
   seven-GPU gang.
2. A Job uses an immutable `ExecutionGraphSnapshot`. An end-to-end `Attempt`
   owns durable `StageRun` nodes. Each physical retry is a `StageAttempt` with
   its own `StageLease`.
3. A `WorkerInstance` exclusively owns a `DeviceSet`. H3 normally uses one GPU
   per WorkerInstance. Future LLM profiles may use multiple GPUs and multiple
   nodes through `WorkerMember` records.
4. A GPU has one WorkerInstance owner. The standard WorkerInstance keeps one
   model component resident. A multi-model Worker is a certified exception;
   the current Encoder/VAE AUX layout is such an exception.
5. Model runtimes are long-lived across StageAttempts. Normal scheduling never
   loads or evicts a model. Residency changes use a slow, drained Fleet path.
6. Encoder output and final DiT latent become immutable, durable
   `StageArtifact` objects before downstream execution. Direct transfer can be
   added as an optimization but can never be the only copy.
7. Cross-Job cache reuse is exact, Project-scoped by default, and backed by
   durable cache metadata. Cross-Organization Customer Content reuse is
   forbidden.
8. Each stage has an independent durable READY queue and CapacityPool. Stage
   edges use bounded buffer credits and Admission reserves StageArtifact storage
   capacity.
9. Scheduling order is `Filter -> Fairness -> Score -> Pick`. Cache affinity,
   locality, and load cannot override hard eligibility or tenant fairness.
10. Customer billing remains one fixed-price Charge per Job. Stage usage,
    retries, model residency, transfer, and storage feed a separate internal
    Usage/Cost Ledger.
11. Final Artifact publication remains a special Vela finalization protocol.
    It is not an ordinary graph node and does not hold a GPU.
12. The old monolithic eight-GPU H3 execution path is removed rather than
    retained as a product fallback. Same-node execution remains a valid
    placement produced by the new scheduler.

### 2.1 Borrow, adapt, and reject

The target deliberately separates upstream inspiration from Vela authority:

| Upstream pattern | Vela decision |
| --- | --- |
| llm-d `Filter -> Score -> Pick` | Adapt to `Filter -> Fairness -> Score -> Pick`, then revalidate and claim in PostgreSQL |
| llm-d bounded flow control | Borrow bounded queues and freshness behavior; keep Accepted Job queues durable |
| Dynamo disaggregated serving | Adapt into `ExecutionGraphRevision`, `StageRun`, `StageAttempt`, and `StageLease` |
| Dynamo Planner | Borrow proposal/actuator separation and begin advisory-only |
| Dynamo/llm-d topology-aware transfer | Adapt as certified `ConnectorRevision` with required/preferred placement |
| llm-d KV event index | Reserve for future Online LLM serving-plane state; do not use it as H3 Artifact authority |
| EPP in-memory request queues | Reject for the Async Job Interface |
| Request-path model loading or eviction | Reject; residency is a drained Fleet operation |
| Upstream router or Kubernetes state as Job authority | Reject; PostgreSQL remains authoritative |

These choices use upstream mechanisms, not upstream performance claims. No
Dynamo or llm-d benchmark establishes H3 throughput, transfer cost, cache value,
or production readiness.

## 3. Goals and non-goals

### 3.1 Goals

- Scale Encoder, DiT, VAE, and CPU post-processing capacity independently.
- Place H3 stages on different machines without changing the customer Job
  contract.
- Keep model components loaded for long periods and prevent request-path model
  replacement.
- Reuse completed expensive stage results after downstream failure and across
  exact, authorized Jobs.
- Preserve cancellation fencing, bounded retries, fixed-price billing, content
  deletion, and exactly-once Visible Completion.
- Generalize the resource model to future multi-GPU and multi-node LLM
  WorkerInstances without exposing model-parallel ranks to the Vela graph.
- Make throughput, tail latency, idle residency cost, cache value, and retry
  waste measurable per stage.

### 3.2 Non-goals

- Do not build a general workflow engine, arbitrary user-defined DAG service,
  or per-token scheduler.
- Do not expose DiT denoise steps, LLM token loops, TP ranks, PP stages, or EP
  collectives as Vela StageRuns.
- Do not use EPP memory queues, JetStream messages, Kubernetes readiness, or a
  per-Job in-memory coordinator as execution authority.
- Do not enable approximate embedding or latent reuse. Cache hits are exact.
- Do not implement direct P2P/RDMA transfer in the first slice.
- Do not span one execution graph across serving regions.
- Do not introduce speculative duplicate StageAttempts in the first release.
- Do not combine the future Online LLM API state machine with the Async Job API.

## 4. Confirmed H3 topology

The current physical layout is one AUX GPU and seven independent DiT GPUs:

```text
8-GPU node

GPU-0  AUX Worker
       +-- long-lived Encoder process
       +-- long-lived VAE Decoder process
       +-- at most one active StageLease

GPU-1  independent single-GPU DiT process
GPU-2  independent single-GPU DiT process
GPU-3  independent single-GPU DiT process
GPU-4  independent single-GPU DiT process
GPU-5  independent single-GPU DiT process
GPU-6  independent single-GPU DiT process
GPU-7  independent single-GPU DiT process
```

The target large-cluster layout prefers one resident model component per GPU:

```text
Encoder CapacityPool  -> single-GPU Encoder WorkerInstances
DiT CapacityPool      -> single-GPU DiT WorkerInstances
VAE CapacityPool      -> single-GPU VAE WorkerInstances
CPU Media Pool        -> CPU WorkerInstances with bounded capacity slots
```

The AUX layout remains representable as a certified multi-model WorkerProfile,
but it is not the preferred scaling unit. Encoder and VAE processes inside one
AUX Worker do not register as separate WorkerInstances and cannot execute
concurrently.

## 5. System context

```text
Async Job API
     |
     v
Admission + Job Coordinator ----------------------- Billing / Charge
     |                                                      |
     | immutable ExecutionGraphSnapshot                     |
     v                                                      |
AttemptCoordinator <------ PostgreSQL authority ------------+
     |       |                 |
     |       |                 +-- StageRun READY queues
     |       +-- StageArtifact/cache/pin metadata
     +-- attempt fence and graph advancement
             |
             v
Stage Scheduler: Filter -> Fairness -> Score -> Pick
             |
             v
CapacityPools of pre-warmed WorkerInstances
  Encoder | DiT | VAE | CPU Media
             |
             +--> L1 local NVMe
             +--> L2 StageArtifact Store
                         |
                         v
                  Artifact Finalizer
                         |
                         v
                  L3 Customer Artifact Store
                         |
                         v
                  Visible Completion

Fleet desired state: PostgreSQL/Catalog
             |
             v
Fleet Controller -> Kubernetes actuator -> WorkerBundle / WorkerInstance
             ^                                      |
             +------ attested readiness ------------+
```

## 6. Deep modules and seams

The target architecture uses a small number of deep modules. Their Interfaces
hide state-machine, fencing, transaction, and retry implementation details from
callers.

### 6.1 AttemptCoordinator Module

The AttemptCoordinator owns graph instantiation and advancement. Its Interface
accepts expected-version commands and returns explicit decisions; callers never
update Job, Attempt, StageRun, StageAttempt, Lease, pin, or winner rows directly.

```text
instantiate(job_id, execution_graph_snapshot) -> AttemptHandle
apply(stage_command)                          -> StageDecision
cancel(job_id, expected_version)              -> CancelResult
reconcile(limit)                              -> ReconcileSummary
```

`stage_command` is a closed command family for acquire, start, heartbeat,
failure, local materialization receipt, durable StageArtifact commit, and
checkpoint commit. The closed family keeps transport evolution behind one
Interface while retaining typed command validation.

PostgreSQL is the production Adapter. State-machine tests use a transactional
test Adapter at the same seam. JetStream is not an Adapter for authority; it is
only a wakeup transport behind the module.

### 6.2 StageScheduler Module

The StageScheduler Interface has one operational purpose: select and durably
claim work for a WorkerInstance that currently advertises capacity.

```text
acquire(worker_authority, capacity_observation) -> Assignment | NoWork
```

Its implementation owns candidate snapshots, lanes, hierarchical fairness,
filter reasons, score composition, deterministic tie-break, dispatch-intent
expiry, and transaction-time revalidation. Callers do not sequence individual
filters or scorers.

### 6.3 WorkerRegistry and Fleet Module

This module owns desired revisions, observed identities, exclusive DeviceSet
bindings, model residency, readiness, drain, and fencing.

```text
observe(worker_evidence)       -> ReadinessDecision
propose(plan_inputs)           -> ResidencyProposal
apply(approved_plan_revision)  -> ActuationPlan
drain(target, expected_epoch)  -> DrainOperation
fence(target, expected_epoch)  -> FenceResult
```

Kubernetes is the production actuator Adapter. An in-memory actuator exercises
plan, drain, rollback, and failed-actuation behavior. Pod readiness is input
evidence, not the module's readiness decision.

### 6.4 StageArtifact Module

This module hides object-store versioning, multipart recovery, cache lookup,
pinning, TTL, quota, and deletion races.

```text
commit(materialization_receipt) -> StageArtifactIdentity
pin_exact(cache_lookup)          -> CacheHit | CacheMiss
release(pin_identity)            -> ReleaseResult
delete(deletion_authority)       -> DeletionResult
```

The production object-store Adapter and a versioned local test Adapter satisfy
the same Interface. Direct-transfer connectors are internal Adapters; they do
not change the durable commit or pin Interface.

### 6.5 UsageCostLedger Module

Measured usage and valuation are separate Interfaces:

```text
record_usage(resource_receipt)             -> UsageIdentity
value(usage_identity, cost_model_revision) -> CostAllocation
summarize(cohort, window)                   -> CostSummary
```

The module never creates or modifies a customer Charge. Billing and internal
cost remain separate modules even when they share Job and StageAttempt IDs.

## 7. Domain model

### 7.1 Catalog definitions

| Entity | Purpose |
| --- | --- |
| `ExecutionProfileRevision` | Job-level internal execution envelope binding one graph and its certified stage/profile/connector option sets |
| `ExecutionGraphRevision` | Immutable static DAG, stage definitions, edges, public phase mapping, and final-output contract |
| `StageDefinitionRevision` | Stage kind, resource requirement, cache/checkpoint policy, retry class, and input/output interfaces |
| `StageInterfaceRevision` | Exact payload kind, dtype/layout/shape/serialization, size limits, integrity, and compatibility |
| `StageProfileRevision` | Backend, model component, resource shape, capacity vector, runtime behavior, and interface implementation |
| `StageResultEquivalenceRevision` | Explicitly certified output-equivalence class used in exact cache keys |
| `InputCanonicalizationRevision` | Versioned, lossless canonical encoding and exact input-equivalence rules used before cache hashing |
| `WorkerProfileRevision` | DeviceSet shape, member topology, resident models, runtime images, and readiness contract |
| `ResidencyPlanRevision` | Desired WorkerBundle and model-residency layout |
| `ConnectorRevision` | Transfer protocol, security, integrity, topology, and fallback contract |
| `StageRuntimeModelRevision` | Per-stage service-time and output-size evidence by cohort |
| `StageCachePolicyRevision` | Scope, stage allowlist, quotas, TTL, encryption scope, and deletion behavior |
| `CheckpointPolicyRevision` | Resume format, interval, limits, compatibility, and overhead budget |
| `CostModelRevision` | Internal resource valuation; never a customer RateCard |

`ExecutionProfileRevision` remains the internal method selected to satisfy a
GenerationPresetRevision; it is not replaced by StageProfileRevision. It now
selects one ExecutionGraphRevision plus allowed StageProfile and Connector
options. `ProfileCertification` is extended from one machine-level execution
profile to that complete graph: every required StageProfile, interface edge,
connector, WorkerProfile, and final output path must be active and compatible.

### 7.2 Runtime execution entities

| Entity | Purpose |
| --- | --- |
| `ExecutionGraphSnapshot` | Admission-time resolution of the selected ExecutionProfileRevision into one immutable graph, allowed profiles/connectors, runtime model, and hard constraints |
| `Attempt` | One end-to-end graph execution epoch and immutable attempt fence |
| `StageRun` | One logical graph-node execution within an Attempt |
| `StageAttempt` | One physical try for a StageRun |
| `StageAllocation` | Exact WorkerInstance, DeviceSet, capacity units, membership digest, and epochs |
| `StageLease` | Time-bounded compute authority for one StageAttempt and attempt/stage fences |
| `StageMaterializationLease` | Non-GPU authority to publish sealed local output to L2 |
| `StageArtifact` | Immutable durable intermediate output with exact object version, digest, lineage, and interface revision |
| `ExecutionPin` | Strong execution reference that blocks ordinary eviction |
| `CacheReference` | Weak cache reference that may be reclaimed after execution releases its pin |
| `StageCacheEntry` | Scoped exact cache key mapped to a StageArtifact |
| `TransferTicket` | Short-lived, destination-bound authority to retrieve one exact StageArtifact version |
| `DurableCheckpoint` | Same-Job resumable stage state; never an ordinary cross-Job cache entry |
| `StageStorageReservation` | Admission-time L2 capacity budget for one graph execution |
| `DecisionEvidence` | Bounded, replayable scheduler decision inputs and digest |
| `ResourceUsageRecord` | Immutable measured stage, residency, transfer, storage, or finalization usage |
| `CostAllocationRecord` | Versioned valuation of ResourceUsageRecord data |

### 7.3 Runtime resource entities

| Entity | Purpose |
| --- | --- |
| `ComputeNode` | Physical machine and node-level fault/network domain |
| `Device` | GPU identity, PCI BDF, topology, health, and device epoch |
| `DeviceSet` | Exclusive set of Devices owned by one WorkerInstance |
| `WorkerInstance` | Schedulable resident model executor; owns one DeviceSet |
| `WorkerMember` | Per-node member/rank group of a distributed WorkerInstance |
| `ModelResidency` | Loaded model/component revision, runtime identity, warm-up evidence, and residency epoch |
| `WorkerBundle` | Fleet-managed per-node layout of WorkerInstances; not a Job scheduling unit |
| `CapacityPool` | Interchangeable WorkerInstances for one certified stage/resource/security class |
| `CapacityObservation` | Epoch- and TTL-bound available capacity vector |

H3 uses one member and one GPU per standard WorkerInstance. A future LLM
WorkerInstance may own multiple GPUs across multiple WorkerMembers. Internal TP,
PP, EP, rank rendezvous, and collectives remain backend implementation details.
Physical node placement and schedulable ownership are independent dimensions:
multiple one-GPU WorkerInstances may occupy one node, while one future
WorkerInstance may span multiple nodes.

The existing immutable `ExecutionPolicySnapshot` remains the Job-level policy,
retry, expiry, and public-contract snapshot. It references the resolved
ExecutionGraphSnapshot; neither snapshot absorbs the other's responsibilities.

## 8. Execution graph contract

The graph is a versioned static DAG. The first executor supports fixed nodes,
AND dependencies, and bounded fan-out/fan-in. It rejects cycles, dynamic node
creation, arbitrary conditions, and user-provided code.

An illustrative H3 graph is:

```text
Encoder GPU
    |
    v
DiT GPU
    |
    +--> optional durable DiT checkpoint(s), same Job only
    |
    v
VAE GPU
    |
    v
optional CPU encode / mux / thumbnail
    |
    v
Artifact Finalization
```

Artifact Finalization is referenced by the graph's output contract but remains
a special Vela protocol, not an ordinary StageRun. Any content-changing media
operation before it is a CPU or GPU StageRun.

For multiple required outputs, branches may finish and retry independently, but
Visible Completion still requires one complete, indivisible ArtifactSet. Partial
customer success is not supported.

## 9. Stage and Attempt state

### 9.1 Parent Attempt

One parent Attempt can contain multiple StageAttempts. A stage failure normally
retries inside the same parent Attempt and reuses completed upstream
StageArtifacts. A new parent Attempt is created only when the graph authority,
compatibility snapshot, required upstream artifacts, or attempt fence can no
longer support continuation.

The Job enters RUNNING and reaches Billable Start on the first effective graph
progress: either a StageAttempt starts under a valid StageLease or an exact,
atomically pinned cache hit advances a StageRun. Assignment alone is not
Billable Start. A cache-only completion still receives the fixed Admission-time
quote and follows the ordinary Visible Completion protocol.

The immutable `Attempt.fence` is valid only while it equals the mutable
`Job.current_fence`. Every StageLease, materialization authority, checkpoint,
pin, and finalization command binds that attempt fence plus the StageRun fence.
Incrementing `Job.current_fence` invalidates the entire graph without mutating
the Attempt row or waiting for per-Worker Stop acknowledgements.

```text
QUEUED -> RUNNING -> FINALIZING -> SUCCEEDED
   |         |            |
   +---------+------------+--> FAILED | CANCELED
```

Admission creates the QUEUED parent Attempt and its StageRuns. The first
effective graph progress moves both Job and Attempt to RUNNING. A lost physical
Worker is a StageAttempt outcome and does not by itself make the parent Attempt
LOST; the parent fails only when the graph can no longer continue within its
frozen authority and budgets.

### 9.2 StageRun

```text
BLOCKED -> READY -> ASSIGNED -> RUNNING -> MATERIALIZING -> SUCCEEDED
                       |          |              |
                       +----------+--------------+
                                  |
                                  v
                              RETRY_WAIT -> READY

BLOCKED / READY / ASSIGNED / RUNNING / MATERIALIZING / RETRY_WAIT
    -> CANCELED | FAILED
```

`StageRun` is the logical queue entry. `StageAttempt` records a physical try.
The first release allows at most one active StageAttempt per StageRun. The schema
retains a winner pointer so a future policy can add explicitly certified
hedging without changing exactly-once stage completion.

### 9.3 StageAttempt and leases

```text
ASSIGNED -> RUNNING -> OUTPUT_SEALED -> SUCCEEDED
    |          |            |
    +----------+------------+--> FAILED | LOST | CANCELED
```

`OUTPUT_SEALED` means compute is finished and the StageAllocation may be
released, while L2 materialization remains part of the same active physical
try. `SUCCEEDED` requires the winning durable StageArtifact commit. A stale
command is an operation result, not a state transition.

The compute StageLease binds:

- Job, Attempt, StageRun, and StageAttempt identities;
- attempt and stage fences;
- WorkerInstance, WorkerInstance epoch, membership digest, and DeviceSet digest;
- ModelRuntime epoch and StageProfileRevision;
- exact input StageArtifact versions;
- issued and expiry times plus a signed token digest.

For a multi-member WorkerInstance, one StageLease covers the entire instance.
All members pass a readiness/start barrier. Any required member or Device epoch
change invalidates the whole StageLease; mid-execution rank or GPU replacement
is not supported.

After compute seals a local output receipt, the GPU allocation can be released.
A StageMaterializationLease authorizes a Worker Agent or Data Mover to upload,
verify, and commit the output to L2. Downstream stages remain blocked until the
durable StageArtifact commit succeeds.

### 9.4 Same-runtime reattachment

A Worker Agent or control connection may restart without unloading the model.
An active StageAttempt can reattach only when its StageAttempt ID, fences,
DeviceSet, ModelRuntime epoch, execution nonce, local receipt, and PostgreSQL
StageLease all match. ModelRuntime has a monotonic local deadline watchdog and
cannot extend a Lease while the control plane is unavailable.

`control_session_epoch` changes on Agent connection restart.
`model_runtime_epoch` changes only when the model process, GPU context,
DeviceSet, or resident model identity changes. Old StageLeases never survive a
ModelRuntime epoch change.

## 10. Worker, Device, and residency model

### 10.1 Exclusive ownership

One WorkerInstance exclusively owns its DeviceSet for its lifetime. Two
WorkerInstances cannot reference the same GPU. Kubernetes device allocation,
Node Agent attestation, unique database constraints, and runtime identity all
enforce the same ownership fact.

The standard H3 WorkerProfile owns one GPU and one resident model component. A
multi-model WorkerProfile is an explicit exception with one shared capacity
slot. It does not create multiple Worker identities for one GPU.

### 10.2 Long-lived ModelRuntime

ModelRuntime loads at Fleet residency time, not Assignment time. It remains
loaded across many StageAttempts. The Worker Agent and ModelRuntime use a local,
versioned Interface; the Agent controls StageLease authority while the runtime
owns backend execution.

Normal StageScheduler behavior cannot load, evict, or replace a model. A model
change requires an approved ResidencyPlanRevision, drain, load, warm-up,
readiness, canary, and rollback evidence. Initial Residency Planner operation is
advisory-only. Ordinary automatic scaling may add already-defined layouts but
cannot repurpose a GPU to another model component.

The system keeps a configured minimum warm capacity. Admission counts READY
capacity only. WARMING capacity is non-committed forecast data. Scale-down is
disabled by default for healthy resident ModelRuntimes. Releasing a model
requires an explicit approved Fleet operation for shutdown, hardware/security
response, revision rollout, or a capacity change whose off-time safely exceeds
the measured unload/reload break-even horizon. Short-term demand never triggers
model release.

### 10.3 WorkerBundle and Kubernetes

WorkerBundle describes the desired WorkerInstance layout on one ComputeNode.
For H3, the normal Kubernetes realization is one Worker Pod per one-GPU
WorkerInstance, with separate Worker Agent and ModelRuntime containers. The AUX
exception has one GPU Pod containing the Encoder and VAE runtimes.

ResidencyPlanRevision may assign a WorkerInstance to an exact ComputeNode and
exact Device identities (GPU UUID plus PCI BDF), or to certified placement
constraints that the Fleet Adapter resolves and attests. For a future
multi-node WorkerInstance it lists every WorkerMember placement and the complete
DeviceSet. StageScheduler selects an already-READY WorkerInstance; it never
chooses cards inside that WorkerInstance or changes its placement.

Future single-node LLM profiles may request multiple GPUs in one Pod. Future
multi-node profiles use one logical WorkerInstance with multiple independently
authenticated WorkerMember Pods. A LeaderWorkerSet-style Adapter is a candidate
actuator, not a domain authority.

PostgreSQL/Catalog owns desired revisions. Kubernetes only actuates them. Pod
readiness is evidence; Vela readiness additionally requires exact model,
runtime, DeviceSet, membership, warm-up, capacity, and certification evidence.

### 10.4 Rollout and drain

Model revisions use N/N-1 WorkerInstance coexistence. A new revision loads on
new capacity, passes canary and soak, then receives traffic. Routine rollout and
residency changes stop new Assignments and wait for compute and local
materialization to finish before stopping ModelRuntime. Security or hardware
failures may fence immediately.

The architecture does not retain the old machine-level monolithic Worker path.
Same-node placement is produced by the new stage scheduler and shares the same
StageRun, StageLease, Artifact, and cancellation semantics as cross-node
placement.

## 11. Stage scheduling and capacity

### 11.1 CapacityPools

A Job no longer binds to one WorkerPool. Each StageRun selects from the
CapacityPools allowed by its ExecutionGraphSnapshot. A CapacityPool groups
interchangeable WorkerInstances with the same region, resource class, resident
model compatibility, security class, connector compatibility, and certification
scope.

WorkerInstance uses a versioned capacity vector. H3 advertises one active stage
slot. CPU Media Workers may advertise multiple CPU/memory/scratch slots. Future
LLM runtimes may advertise bounded sequence, batch-token, and KV capacity.
Dynamic observations can reduce but never exceed certified static limits.

### 11.2 Filter, fairness, score, pick

Scheduling order is normative:

1. **Filter.** Validate StageRun readiness, dependencies, attempt fence, pins,
   Worker/Device/model epochs, residency, certification, security, region,
   connector, capacity freshness, drain state, and buffer credit.
2. **Fairness.** Apply Retry, Protected, and Normal lanes, then Organization,
   ServiceClass, and Project fairness using normalized resource-seconds within
   the relevant CapacityPool.
3. **Score.** Evaluate StageArtifact locality, exact cache/local NVMe affinity,
   transfer cost, Worker load, predicted finish time, downstream critical path,
   and age.
4. **Pick.** Use a deterministic tie-break and persist bounded
   DecisionEvidence.

Cache or locality cannot make an ineligible Worker eligible and cannot bypass
tenant fairness. Worker pull is retained: a READY WorkerInstance asks for work,
and the acquire transaction rechecks all authority before returning a durable
StageAssignment.

### 11.3 Pipeline capacity and Admission

For stage `i`:

```text
stage_capacity_i = ready_capacity_units_i / mean_service_time_i

pipeline_throughput = min(stage_capacity_encoder,
                          stage_capacity_dit,
                          stage_capacity_vae,
                          stage_capacity_cpu_if_required)
```

This is a one-slot, steady-state analytical sanity bound. It is not Admission
closure and does not include arrival variance, p99 service, transfer,
materialization, bounded WIP, failures, retries, cache, finalization, or model
residency changes.

Admission predicts the complete graph: every stage queue, service demand,
buffer wait, transfer, fan-out critical path, CPU work, and finalization budget.
It requires at least one compatible READY capacity path for every required
stage and a risk-adjusted finish before Job Expiry. It does not reserve a
specific Worker.

Only a cache hit that is validated and atomically pinned can remove predicted
stage demand. Forecast cache-hit probability cannot justify Admission.

### 11.4 Bounded work in progress

Every materializing graph edge has a count and byte buffer-credit limit. An
upstream StageRun must acquire downstream output credit before execution. The
credit is held by the StageArtifact until downstream consumption or cleanup.
A bounded prefetch window keeps downstream capacity fed without allowing
unbounded embeddings or latents.

Admission also creates a durable, risk-adjusted StageStorageReservation using
the per-stage p99 size and hard maximum. Cache hits pinned to existing objects do
not reserve duplicate payload bytes. Buffer credit controls live WIP;
StageStorageReservation guarantees L2 capacity for Accepted Jobs.

## 12. StageArtifact, transfer, cache, and checkpoint

### 12.1 Three storage tiers

| Tier | Role | Durability |
| --- | --- | --- |
| L1 Worker NVMe | Local input/output cache and sealed materialization source | Evictable; node-local |
| L2 StageArtifact Store | Durable internal Encoder/DiT/VAE/CPU intermediate data | Cross-node durable; short retention |
| L3 Customer Artifact Store | Customer-visible committed ArtifactSet | Formal retention, backup, and access contract |

L2 and L3 may share an object-storage provider but require distinct logical
namespaces, credentials, lifecycle, access, and deletion policy. L2 is never
enumerable through the customer Artifact Interface. PostgreSQL stores metadata,
object version, digest, size, scope, lineage, interface revision, and expiry;
payload remains in object storage.

### 12.2 Stage materialization

A StageRun succeeds only after the L2 object version and digest are committed.
If L2 is unavailable, local materialization retries without rerunning compute.
Loss of the local node before L2 commit requires stage retry. L2 unavailability
closes new Admission; Accepted Jobs wait durably within Job Expiry and retry
budgets.

### 12.3 Transfer

The first Connector Adapter is L2 object-store pull. TransferTicket grants one
destination WorkerInstance short-lived access to one exact object version and
binds StageArtifact identity, digest, size, destination epoch, connector
revision, and expiry. Destination verifies integrity before ModelRuntime starts.

Same-node NVMe, RDMA/NIXL/P2P, or rack-cache connectors can be added later. They
must fall back to L2 and cannot become the sole durable copy. Locality is a
preferred score unless region, security, interface, or certified topology makes
it required.

### 12.4 Exact cross-Job cache

The cache key is a scoped HMAC over a canonical structure:

```text
StageCacheKeyV1 = HMAC(
  scope_key,
  canonical_encode(
    stage_kind,
    stage_result_equivalence_revision,
    input_canonicalization_revision,
    root_input_digests,
    input_stage_artifact_digests,
    normalized_stage_parameters,
    seed_and_rng_revision,
    output_shape,
    adapter_and_lora_digests
  )
)
```

`root_input_digests` covers canonical prompt and customer-input identities for
root stages without placing raw Customer Content in the key or telemetry.
Canonicalization is versioned and lossless except for exact equivalences
declared by that revision.

`StageResultEquivalenceRevision` binds the model component, preprocessor,
backend kernels, precision, RNG/scheduler semantics, and StageInterface.
Different profiles do not share cache entries unless Catalog evidence explicitly
certifies canonical-byte or bitwise result equivalence for the supported input
domain. Quality similarity or tolerance-based numerical closeness is never
enough. The first release otherwise scopes an equivalence revision to one exact
StageProfile identity.

StageCacheEntry and pins are durable PostgreSQL metadata. An optional in-memory
or Redis index can suggest candidates but is rebuildable and cannot authorize a
hit. A cache hit transaction validates scope, policy, TTL, exact object version,
Artifact state, equivalence, and deletion state, then creates a strong
ExecutionPin before advancing the StageRun.

Cross-Job cache defaults to Project scope. Organization-scope sharing requires
explicit Organization authorization. Cross-Organization Customer Content reuse
is forbidden. Project administrators can disable future cache lookup/write and
request weak-cache cleanup. Same-Attempt retry reuse remains execution recovery
and is not disabled by the cross-Job cache switch.

### 12.5 Pinning and eviction

An ExecutionPin is a strong reference and blocks ordinary TTL/capacity
eviction. A CacheReference is weak and can be reclaimed after the Job releases
its pin. Cache-hit acquisition creates the strong pin atomically before the
cache entry can be consumed.

Eviction is versioned and value-aware. After invalid, expired, retired, or
deletion-blocked entries are selected, remaining unpinned objects are ordered by
expected saved recomputation cost minus storage and read-transfer cost, adjusted
for size. The policy begins in deterministic shadow mode before affecting
eviction. It does not use an opaque online learner.

### 12.6 Durable checkpoints

DurableCheckpoint is same-Job resumable stage state, not an ordinary cache
entry. It binds step, RNG/scheduler state, input digest, model/backend/precision,
resume compatibility, and exact object version. A StageProfile enables it only
after correctness, recovery, I/O overhead, and cost certification. Intermediate
DiT state never becomes approximate or cross-Job reuse by default.

## 13. Failure, retry, cancellation, and completion

### 13.1 Retry budget

Automatic retry requires both a per-stage attempt budget and a Job-global
resource budget. The global budget accumulates GPU-seconds times device count,
CPU resource-seconds, and bounded finalization time. Job Expiry remains the hard
system stop. Exhausting any applicable limit stops retry.

A stage retry can select another StageProfile only from the compatible set
frozen at Admission. The replacement must preserve the GenerationPreset,
StageInterface, model semantics, and quality certification.

### 13.2 Circuit scope

Failure handling first isolates the narrowest affected scope:

- Device or WorkerInstance for a local hardware/runtime failure;
- StageProfileCertification for a repeated cross-Worker fingerprint;
- ConnectorRevision for systematic transfer failure;
- exact StageArtifact object version for integrity failure;
- complete ExecutionGraph only when no compatible path remains.

Ordinary Catalog retirement does not mutate Accepted Jobs. Emergency security,
correctness, or integrity revocation can stop an affected StageLease despite an
immutable snapshot, then retry only within the already-frozen alternative set.

### 13.3 Cancellation

Customer Cancellation wins one PostgreSQL transaction that increments
`Job.current_fence`, revokes active StageLeases, cancels nonterminal StageRuns, stops
new TransferTickets, updates Job/Charge authority, and writes Stop outbox events.
Worker Stop delivery is asynchronous; stale StageArtifact commits and late
completion are rejected by the attempt-fence mismatch.

StageArtifacts committed before cancellation can remain under the Project cache
policy. Results not committed before the cancellation fence cannot enter cache.
Cancellation is not Content Deletion.

### 13.4 Content deletion

Content Deletion writes an authoritative tombstone and immediately blocks new
cache hits and pins. Existing running StageAttempts with valid pins may complete
within the deletion SLA. At the deletion deadline they are fenced if necessary.
Deletion covers L1, L2, cache indexes, checkpoints, L3, and exact object
versions while preserving required non-content audit and Charge evidence.

### 13.5 Finalization

The final compute or media StageRun publishes a durable StageArtifact and
releases its compute capacity. A non-GPU Artifact Finalizer uses the existing
special finalization authority to upload or server-side-copy final objects,
validate the complete ArtifactSet, and atomically commit Job success, one
Charge, Artifact access, and Visible Completion.

Finalizer failure retries upload, validation, or commit without rerunning VAE or
other completed stages. No partial ArtifactSet becomes customer-visible.

## 14. Billing and internal cost

Customer billing remains end-to-end and fixed at Admission. Cache hits, retries,
component placement, and actual platform cost do not change PricingSnapshot or
create additional Charges. Billable Start is the first effective graph progress,
whether a physical StageAttempt starts or an authoritative cache hit advances
the graph.

The internal Usage/Cost Ledger records measured facts separately from valuation:

```text
ResourceUsageRecord   immutable resource use
CostModelRevision     versioned internal valuation
CostAllocationRecord  valuation of one or more usage records
```

Direct usage includes stage GPU-seconds, CPU/memory time, materialization,
network bytes, object operations, and finalization. Pool-level usage includes
idle model residency, warm-up/load, WorkerBundle minimum capacity, drain, and
failed reconfiguration. Retry waste and cancellation waste are explicit.

Cache avoided-compute is a counterfactual estimate with its own evidence model;
it is not recorded as negative actual usage. Required operating metrics include
direct Job cost, pool residency cost, retry waste, cache storage cost, estimated
avoided compute, and cost per Visible Completion.

## 15. Customer and operator views

The customer Async Job Interface remains backend-neutral. ExecutionGraphRevision
maps internal StageRuns to stable public phases such as PREPARING, GENERATING,
DECODING, POSTPROCESSING, and FINALIZING. Customers receive Job state,
attempt-scoped phase progress, retry summary, Dynamic ETA, structured failure,
and the final ArtifactSet.

Worker identity, GPU placement, internal StageRun, cache hit, StageArtifact,
TransferTicket, failure fingerprint, and cost details remain in an authorized
operator view. Cache hits may skip phases quickly but do not change the output
contract. Only Visible Completion represents 100 percent.

## 16. Observability and evidence

Per stage/profile/cohort, record:

- queue, buffer, transfer, service, materialization, retry, and finalization
  time at p50/p95/p99;
- StageArtifact size and L1/L2 throughput;
- cache hit/miss, reuse distance, eviction, pin, and rebuild behavior;
- GPU busy/idle/residency/load/warm-up time;
- WorkerInstance/member/runtime/device epoch changes;
- buffer occupancy, pipeline starvation, and downstream backpressure;
- direct cost, shared residency cost, retry waste, and cost per completion;
- bounded filter reasons, score components, deterministic winner, and decision
  digest.

Prometheus labels use bounded enums and revision cohorts. Job, Worker, Device,
and Project IDs belong in trace/evidence storage, not metric labels. Prompt,
embedding, latent, Customer Artifact URLs, and other Customer Content never
enter telemetry.

PostgreSQL stores only bounded authority transitions and summaries. High-rate
GPU, backend-step, token, and transfer telemetry goes to the observability
system. StageRuntimeModelRevision is trained/calibrated from evidence, published
as a versioned model, and shadow-validated before it changes Admission.

## 17. Security and regional scope

Each WorkerMember has an independent SPIFFE/mTLS identity. A WorkerInstance
aggregates membership but members do not share private identity. Fleet and Node
Agent attest Node, GPU UUID, PCI BDF, DeviceSet, model, runtime, and member
epochs. StageLease binds the membership and DeviceSet digests.

ModelRuntime receives neither PostgreSQL credentials nor general object-store
credentials. Worker Agent or Data Mover receives short-lived, exact-version
TransferTicket authority. StageArtifact object names do not expose raw content
hashes, and cache keys use scoped HMACs.

The first release permits same-region cross-node and cross-network-domain
placement only when connector and security constraints are certified. A graph
cannot span serving regions or cross a data-residency boundary.

## 18. Future LLM integration

The resource model supports multi-GPU and multi-node WorkerInstances, but Vela
StageRun remains coarse-grained. Prefill and Decode can be separate stages;
their internal TP/PP/EP and rank topology remains backend-owned.

Online streaming LLM serving uses a separate request state machine and product
Interface. It may share WorkerInstance, Fleet, Catalog, CapacityPool, identity,
cost, and connector infrastructure, while an llm-d or Dynamo Adapter owns
online routing, flow control, continuous batching, and KV-aware decisions. EPP
memory queues and KV indexes never become Async Job or Charge authority.

## 19. Core invariants

1. One Device belongs to at most one active WorkerInstance.
2. One StageRun has at most one active StageAttempt in the first release.
3. Every StageLease validates both attempt and stage fences.
4. A ModelRuntime epoch change invalidates all old StageLeases.
5. A StageRun cannot succeed before an immutable L2 StageArtifact commit.
6. A downstream StageRun cannot become READY without valid input pins.
7. Ordinary cache eviction cannot delete an execution-pinned artifact.
8. A cache hit cannot cross its authorized Project/Organization scope.
9. A canceled parent Attempt cannot publish or cache a late stage result.
10. A Job cannot form more than one Visible Completion or Charge.
11. Artifact Finalization cannot rerun model computation.
12. Scheduler locality and cache scores cannot override hard eligibility or
    hierarchical fairness.
13. Kubernetes readiness cannot authorize StageAssignment.
14. Model loading cannot occur on the normal StageAssignment path.
15. High-frequency telemetry cannot advance execution authority.

## 20. Acceptance evidence

The design is not implemented until evidence proves at least:

- H3 Encoder, seven independent DiT Workers, VAE, and optional CPU stage can be
  placed on different nodes and complete one Job;
- same-node placement uses the identical graph and lease path;
- stage retry reuses committed upstream StageArtifacts;
- Agent restart reattaches only to the same ModelRuntime epoch and Lease;
- parent cancellation fences every active stage and rejects late cache writes;
- L2 outage releases GPU after local sealing, retries materialization, and does
  not start downstream;
- Project-scoped exact cache hit is correct; cross-Project and
  cross-Organization negative tests fail closed;
- cache eviction cannot race an ExecutionPin; Content Deletion blocks new hits
  and meets its deadline;
- per-stage and global retry budgets both stop excess work;
- stage circuit failure does not unnecessarily invalidate unaffected profiles;
- CPU finalization failure does not rerun GPU stages;
- fixed customer Charge is independent from stage Usage/Cost records;
- Scheduler replay of the same snapshot produces the same DecisionEvidence
  digest;
- WorkerBundle drain preserves resident work and emergency fence stops it;
- no old machine-level monolithic Worker path remains reachable;
- all existing commercial, isolation, retention, DR, and Visible Completion
  invariants continue to pass.

Production activation additionally requires updated Launch Receipt contracts,
real H3 quality/performance certification, stage and network fault injection,
mixed-load soak, N/N-1 model rollout/rollback, storage capacity and deletion
evidence, dashboards, alerts, runbooks, and on-call ownership. Repository tests
alone do not advance any Production Gate.

## 21. Calibration inputs, not architecture decisions

The following values remain evidence-driven configuration:

- Encoder, DiT, VAE, and CPU p50/p95/p99 service times by request cohort;
- stage output p50/p99/hard-max bytes;
- L2 sustained write/read throughput and availability;
- buffer-credit count/byte windows;
- StageStorageReservation risk margins;
- StageLease, materialization, reattachment, and drain deadlines;
- per-stage retry caps and Job-global resource multipliers;
- cache TTL, Project quotas, reuse-distance model, and eviction weights;
- minimum warm capacity, scale-up horizon, and scale-down cooldown;
- checkpoint interval and maximum overhead;
- required versus preferred network domains;
- cost weights and shared-residency allocation method.

No placeholder value can become ACTIVE without a revisioned evidence receipt.

## 22. Delivery package and implementation boundary

The accepted decisions are recorded in:

- `docs/adr/0030-execute-accepted-jobs-as-durable-stage-graphs.md`;
- `docs/adr/0031-bind-worker-instances-exclusively-to-resident-device-sets.md`;
- `docs/adr/0032-materialize-and-pin-stage-artifacts.md`;
- `docs/adr/0033-separate-internal-usage-cost-from-customer-charge.md`;
- `docs/adr/0034-remove-the-monolithic-h3-worker-path.md`.

The proposed implementation contracts are in:

- `docs/specs/0049-stage-execution-schema-and-protocol-migration.md`;
- `docs/specs/0050-h3-stage-execution-implementation-slices.md`;
- `docs/specs/0051-trace-driven-stage-capacity-simulator.md`.

Those documents are design evidence only. Current runtime behavior continues to
use the machine-level Attempt/Worker protocol until the cutover evidence in the
specifications is complete.
