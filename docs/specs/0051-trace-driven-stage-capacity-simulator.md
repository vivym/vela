# Trace-Driven Stage Capacity Simulator

Date: 2026-08-29

Status: S49.11 repository implementation complete. `internal/capacitysim` and
`cmd/vela-capacity-sim` implement the bounded deterministic simulator, replay
CLI, comparison receipt, and advisory proposal boundary. Checked-in inputs are
synthetic; no real H3 calibration, benchmark, shadow result, production
recommendation, or Launch Receipt exists.

## Question answered

The simulator answers: given a versioned workload trace, stage runtime/output
distributions, resident Worker layout, cache policy, transfer topology, failure
model, and cost model, what throughput, queueing, tail latency, warm capacity,
storage, retry waste, and internal cost should a candidate H3 stage placement
produce?

It does not prove model quality, benchmark hardware, network availability,
Production Gate status, or a customer SLO. It is a decision aid whose error must
be measured against real traces.

## Deep Module Interface

```text
simulate(
  scenario: ScenarioRevision,
  workload: WorkloadTrace,
  evidence: CalibrationBundle
) -> SimulationReceipt
```

The Interface accepts immutable, content-free inputs and returns deterministic
results plus a replay digest. Queue implementation, random sampling,
event-ordering, cache, failure injection, and metric aggregation remain inside
the Module.

A later CLI may expose:

```text
vela-capacity-sim validate --scenario scenario.json --trace trace.ndjson
vela-capacity-sim run      --scenario scenario.json --trace trace.ndjson \
                           --calibration calibration.json --out receipt.json
vela-capacity-sim compare  --baseline receipt-a.json --candidate receipt-b.json
```

`compare` reports differences; it never selects or auto-applies a Fleet plan.

## Input provenance

Every input carries schema version, source kind, collection window, hardware,
runtime/model/profile/connector revisions, units, sample count, freshness,
confidence, and content digest. Inputs are either measured, derived, assumed,
or synthetic. The simulator preserves that classification in every output.

Placeholder distributions can test mechanics but cannot become ACTIVE capacity
or SLO evidence.

## Workload trace

One NDJSON record represents a Job arrival or an observed terminal update. It
contains no prompt, image, embedding, latent, Artifact URL, raw cache key, or
other Customer Content.

Required arrival fields:

```text
schema_version
trace_id                    stable pseudonymous identity
arrival_offset_ns
organization_cohort
project_cohort
service_class_revision
generation_preset_revision
output_spec
request_cohort              bounded, versioned feature bucket
job_expiry_offset_ns
eligible_graph_revision
cache_policy_revision
```

Optional observed fields include stage queue/start/seal/materialize/finish
offsets, profile, output bytes, connector, cache disposition, retry/failure
class, finalization result, and Visible Completion. Raw Organization, Project,
Job, Worker, or Device IDs are not required by the simulation Interface.

## Calibration bundle

For every stage/profile/request cohort:

- empirical or fitted service-time distribution with p50/p95/p99 and hard cap;
- output-size distribution with p50/p99 and hard maximum;
- GPU/CPU/memory demand and capacity vector;
- local seal and L2 materialization distributions;
- cold load, warm-up, drain, and readiness times;
- failure hazard and recovery distributions by bounded class;
- checkpoint overhead and recovery distribution when enabled.

For every Connector/topology cohort:

- setup latency, sustained payload throughput, concurrency limit, and failure
  distribution;
- required/preferred domain rule and L2 fallback behavior;
- object operation and byte cost inputs.

Cache calibration contains exact eligible-key frequency, reuse distance,
Artifact size, TTL, quota, and equivalence cohort. It never needs raw content or
raw scoped HMAC keys.

## Scenario revision

A ScenarioRevision freezes:

- stage graph and allowed profile/connector alternatives;
- resident WorkerInstance counts per CapacityPool and fault/network domain;
- ModelResidency minimum, scale-out latency, cooldown, and no-repurpose rule;
- explicit release-disabled default and unload/reload break-even evidence for
  any scenario that permits healthy residency scale-down;
- scheduler filter/fairness/score/pick policy revision;
- queue, buffer-credit, storage-reservation, and prefetch limits;
- retry, expiry, cache, checkpoint, deletion, and finalization policy;
- L2 capacity/availability model;
- CostModelRevision and shared residency allocation method;
- deterministic seed and simulator algorithm revision.

The first version models a fixed resident layout. Advisory scale events are
added only after fixed-layout replay is calibrated.

## Discrete-event model

The event clock processes stable ordered tuples:

```text
(event_time_ns, event_priority, stable_entity_key, event_sequence)
```

Equal-time ordering is fixed by the simulator revision. Required event classes
include arrival, Admission decision, stage READY, Worker acquire, transfer,
stage start/seal/release, materialization commit, retry wake, failure, cache pin
or eviction, finalization, cancellation/expiry, residency actuation, and
telemetry observation expiry.

The simulator maintains no unbounded queue. A Scenario fails validation if any
queue, buffer, storage, cache, event, or retry limit is absent.

## Capacity semantics

For a stable no-cache linear graph, the analytical sanity bound is:

```text
stage_capacity_i = ready_capacity_units_i / mean_service_time_i
pipeline_capacity = min(stage_capacity_i)
```

This is only a lower-complexity cross-check. The simulation also includes
arrival variance, p99 service time, fairness, transfer, bounded WIP,
materialization, failures, retries, cache hits, expiry, finalization, and warm
capacity changes.

For multi-GPU WorkerInstances, one busy interval consumes the complete certified
DeviceSet. The simulator never schedules ranks independently. H3 DiT consumes
exactly one GPU per WorkerInstance.

## Scheduler model

Simulation order matches production semantics:

1. Filter hard graph, pin, fence, profile, security, region, residency,
   freshness, connector, capacity, drain, and buffer eligibility.
2. Select Retry/Protected/Normal lane and hierarchical Organization,
   ServiceClass, and Project fairness.
3. Score locality, transfer, load, predicted finish, downstream critical path,
   and age.
4. Pick with deterministic tie-break.

The simulator records bounded DecisionEvidence compatible with the target
Scheduler receipt. It must be possible to replay a captured production decision
and explain divergence by input or algorithm revision.

## Cache and Artifact model

- A cache hit removes stage demand only when the exact object is live and a
  simulated strong pin is acquired.
- Cache admission, eviction, TTL, quota, deletion tombstone, and pin races use
  the same state transitions as the target StageArtifact Module.
- Execution retry reuse and cross-Job cache are separate statistics.
- L1 locality reduces transfer according to Connector evidence but never
  removes the authoritative L2 object.
- L2 reservation, consumed bytes, object operations, byte-time, and blocked
  Admission are tracked explicitly.

## Cost model

Actual simulated usage includes:

- per-stage GPU-seconds times device count;
- CPU/memory/scratch resource-time;
- payload bytes and network-domain transfer;
- L2/L3 byte-time and object operations;
- model load/warm-up, idle residency, drain, and failed actuation;
- retry, cancellation, expiry, and finalization waste.

Customer price is reported from PricingSnapshot only as a cohort comparison; it
is not recalculated from usage. Estimated cache avoided-compute is reported as
a counterfactual beside actual storage/read cost, never as negative usage.

## Output receipt

`SimulationReceipt` contains input digests, simulator revision, seed,
validation result, simulated window, and at least:

- Admission acceptance/rejection and reason counts;
- Visible Completion throughput and success rate;
- end-to-end and per-stage queue/transfer/service/materialization/finalization
  p50/p95/p99;
- Dynamic ETA error when an ETA model is supplied;
- Worker busy/idle/residency/load/warm-up time by CapacityPool;
- queue and buffer occupancy, starvation, backpressure, and storage peaks;
- retry/failure/cancellation/expiry counts and wasted resources;
- cache hit, pin, eviction, reuse-distance, saved-work estimate, and storage;
- direct usage, shared allocated cost, retry waste, cache cost, and cost per
  Visible Completion;
- fairness attained service, share error, and maximum starvation by bounded
  cohort;
- all dropped, clamped, or unsupported input records.

The receipt includes conservation checks and an overall deterministic digest.
Raw event output is optional, bounded, partitioned, and content-free.

## Required experiment matrix

Every recommendation study includes:

1. current same-node 1 AUX + 7 independent DiT layout;
2. independently scaled Encoder, DiT, and VAE pools across nodes;
3. request-rate steps, bursts, long/short cohorts, and mixed Service Classes;
4. zero, measured, and high cache hit/reuse-distance scenarios;
5. Connector latency/throughput at 0.5x, 1x, 2x, and outage conditions;
6. one Worker/GPU/node loss, repeated profile failure, L2 outage, and delayed
   materialization;
7. minimum warm capacity and scale-out/load delays;
8. CPU media enabled/disabled and finalization slowdown;
9. per-stage count/byte buffer and storage-reservation sensitivity;
10. retry and Job Expiry sensitivity.

Although component execution time is expected to dominate transfer, the
transfer sweep is mandatory so the conclusion remains evidence-backed.

## Calibration and validation

Validation proceeds in increasing authority:

- unit/property tests: deterministic event order, no negative quantities,
  capacity and byte conservation, bounded queues, pin safety;
- analytical fixtures: M/M/1-like and fixed-service pipelines compared with
  known bounds without treating them as H3 evidence;
- historical replay: per-stage and end-to-end error against held-out traces;
- shadow run: compare predicted next-window queues, completions, and utilization
  with actual observations;
- fault replay: compare simulated and injected failure/recovery outcomes.

Error is reported per stage, profile, and cohort. Aggregate agreement cannot
hide a stage p99, transfer, or failure mismatch. Calibration publication creates
a new StageRuntimeModelRevision; it never edits the prior revision.

## Advisory planning boundary

The simulator may emit a `ResidencyProposal` containing current/desired counts,
min/max, expected SLO and cost effects, input digest, confidence, reason codes,
cooldown, expiry, and unresolved risks. The first implementation is
`auto_apply=false` and has no Kubernetes credentials.

Planner approval, actuation, drain, load, warm-up, canary, rollback, and
observed-result receipts remain separate Fleet Module operations. The normal
Scheduler still cannot repurpose a GPU or release a loaded model.

## Acceptance evidence

The simulator slice is complete only when:

- schemas reject Customer Content and unbounded dimensions;
- repeated runs with identical bytes produce identical receipts;
- all conservation and pin/eviction invariants pass under randomized tests;
- analytical fixtures match declared tolerance;
- held-out replay reports error without silently substituting defaults;
- scenario comparison preserves source/assumption classifications;
- advisory output cannot actuate external state;
- repository docs and implementation status continue to say simulator output is
  not measured production evidence.
