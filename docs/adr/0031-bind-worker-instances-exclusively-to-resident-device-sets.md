# Bind WorkerInstances Exclusively To Resident DeviceSets

Date: 2026-08-29

Status: Accepted and implemented in the repository; production acceptance remains `0/9 PASS`.

## Context

Loading an H3 model component is extremely expensive. Treating model identity
as a request-time scheduling choice would make latency and capacity unstable.
Allowing multiple Workers to share one GPU would also split ownership of memory,
health, fencing, and utilization across processes and make large-cluster
scheduling ambiguous.

H3 normally needs one independent Worker per GPU. A future LLM instance may
need multiple GPUs on one node or a certified gang across nodes. The resource
model must support both without forcing H3 into a multi-GPU shape.

## Decision

- One active `WorkerInstance` exclusively owns one `DeviceSet` for its
  lifetime. A Device cannot belong to two active WorkerInstances.
- A standard H3 WorkerInstance owns one GPU and keeps one model component
  resident. Each DiT process is an independent single-GPU WorkerInstance.
- The current Encoder/VAE AUX GPU is an explicit certified multi-model
  `WorkerProfileRevision` with one shared capacity slot. It does not create two
  WorkerInstances on one GPU and cannot execute Encoder and VAE concurrently.
- A future LLM WorkerInstance may own multiple Devices and multiple
  independently authenticated `WorkerMember` processes, including across
  nodes. One StageLease covers the complete certified membership.
- ResidencyPlanRevision can name the exact ComputeNode and GPU UUID/PCI BDF for
  each WorkerInstance, or certified constraints resolved and attested by Fleet.
  StageScheduler selects a READY WorkerInstance and never reallocates cards
  within it.
- `ModelRuntime` loads during Fleet residency actuation, not StageAssignment.
  Normal scheduling cannot load, evict, replace, or repurpose a resident model.
- A residency change requires an approved `ResidencyPlanRevision`, drain,
  load, warm-up, readiness, canary, soak, and rollback evidence.
- Scale-out may instantiate an already-defined WorkerProfile layout.
  Repurposing existing GPUs to another model component is a separate slow path.
- Healthy resident ModelRuntimes do not automatically scale down. Release
  requires an explicit approved Fleet operation and evidence that shutdown,
  hardware/security response, rollout, or a long capacity change justifies the
  measured unload/reload break-even cost. Short-term demand cannot evict them.

`WorkerRegistryAndFleet` is the deep Module at this seam. PostgreSQL and Catalog
own desired and certified state; Kubernetes is an actuator Adapter. Pod
readiness is evidence, not Assignment authority.

## Enforcement

Exclusive ownership must agree across four layers:

1. Kubernetes device allocation prevents ordinary Pod sharing.
2. Node Agent attests GPU UUID, PCI BDF, Device epoch, and Worker membership.
3. PostgreSQL unique constraints prevent overlapping active DeviceSet bindings.
4. ModelRuntime identity and epoch fence stale StageLeases locally.

A Worker Agent reconnect increments `control_session_epoch` without unloading
the model. Restarting ModelRuntime, changing the GPU context, DeviceSet, or
resident model increments `model_runtime_epoch` and invalidates every old
StageLease.

## Consequences

- Scheduler capacity represents already-loaded execution, not hypothetical
  model placement.
- Idle resident GPUs become visible internal cost rather than a reason to
  perform request-path eviction.
- Same-node and cross-node H3 placement use the same Interface.
- Multi-node LLM readiness requires a complete membership/start barrier and
  fences the whole WorkerInstance on any required member epoch change.

## Rejected alternatives

- Multiple WorkerInstances sharing one GPU: rejected because ownership,
  capacity, memory, and failure fencing are no longer singular.
- Routine one WorkerInstance with multiple resident models: rejected as the
  standard because it reduces scheduler control; AUX remains an explicit
  exception.
- Scheduler-directed model swapping: rejected because load time is extreme and
  makes READY capacity dishonest.
- Treat a Kubernetes Node or WorkerBundle as the schedulable Worker: rejected
  because machine placement and DeviceSet ownership are different dimensions.

## Evidence boundary

The ADR does not prove Kubernetes isolation, multi-node gang readiness, model
load time, safe drain, or H3 performance. Those require implementation tests,
fault injection, target-cluster receipts, and Production Gate evidence.
