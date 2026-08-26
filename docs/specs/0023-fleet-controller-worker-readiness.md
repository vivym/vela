# Fleet Controller And Worker Readiness

Date: 2026-08-25

Status: In implementation. This slice covers the repository-verifiable Fleet
ownership, capacity backpressure, planned drain, and Worker re-entry contract.
It does not claim a live Kubernetes, H3, storage, rollout, or Production Gate
receipt.

Predecessors:

- `docs/specs/0002-worker-assignment-and-execution-lease.md`
- `docs/specs/0004-worker-heartbeat-lease-renewal-and-progress.md`
- `docs/specs/0020-certified-remediation.md`
- `docs/specs/0021-worker-local-recovery-state.md`
- `docs/specs/0022-h3-worker-agent-and-runner.md`
- `docs/adr/0013-do-not-interrupt-jobs-for-routine-releases.md`
- `docs/adr/0023-automate-remediation-through-node-reboot.md`

## Goal

Implement the repository portion of Acceptance Scenarios 16, 17, and 22:

1. Artifact Store failure or scratch pressure removes only the affected Worker
   or pool from Assignment, and recovery requires a fresh storage probe plus the
   configured low-watermark side of the hysteresis band.
2. Only the Fleet Controller may materialize or destructively mutate a protected
   H3 WorkerPool, `OnDelete` DaemonSet, or Worker Pod. A destructive mutation
   additionally requires a completed, identity-bound `DrainOperation`.
3. An OFFLINE, recovering, or newly materialized Worker cannot become
   `HEALTHY + READY` until exact identity, device, Inference Backend, model
   warm-up, and canary checks all pass for its current Worker epoch and
   ExecutionProfileRevision.

## Authority And Public Seams

The agreed public test seams are:

1. PostgreSQL security-definer Fleet functions. They own Worker and pool
   capacity conditions, readiness cycles/evidence, DrainOperation state,
   mutation authorization receipts, and durable retirement-completion receipts.
2. `fleet.Service`. It exposes capacity observation, readiness begin/report/get,
   drain request/get/reconcile, and protected Kubernetes mutation authorization.
3. the Fleet maintenance gRPC adapter. It authenticates one configured Fleet
   Controller SPIFFE identity and maps only bounded typed requests to
   `fleet.Service`; it does not expose table access.
4. the Kubernetes validating-admission handler. Before trusting
   `AdmissionReview.userInfo`, `/validate` requires a client certificate signed
   by the configured kube-apiserver client CA with exactly the configured SPIFFE
   URI. It accepts the Fleet Controller actor and narrowly delegates only
   protected Pod `CREATE` requests from the configured
   `kube-controller-manager` actor after validating the Pod against its live
   protected DaemonSet parent. It asks the authoritative service to validate
   every destructive mutation.
5. the Fleet reconciler resource interface. It materializes an immutable desired
   WorkerPool revision as one protected `OnDelete` DaemonSet, never removes a
   protection finalizer before mutation authorization succeeds, and records
   retirement completion only after observing exact Kubernetes-UID absence.

The dedicated `vela_fleet` runtime role receives only execute privileges on the
Fleet functions. `vela_fleet_owner` is `NOLOGIN BYPASSRLS`, owns the functions,
and has the narrow table privileges those functions require. Fleet Controller,
Scheduler, Worker, Remediation, and customer request identities are mutually
exclusive.

## Capacity State And Hysteresis

Every capacity observation binds Worker UUID, current epoch, strictly increasing
sequence, observed PostgreSQL-compatible timestamp, total/free scratch bytes,
configured high/low/critical thresholds, the Worker-computed watermark state,
and Artifact Store reachability. Numeric relations are revalidated by the
control plane. Exact replay is accepted; a reused sequence with different input
is rejected.

The authoritative states are:

```text
Worker: ADMITTABLE | SCRATCH_PRESSURED | SCRATCH_CRITICAL | STORAGE_UNAVAILABLE
Pool:   ADMITTABLE | SCRATCH_PRESSURED | STORAGE_UNAVAILABLE | MULTIPLE_BLOCKERS
```

A failed Artifact Store observation closes the affected pool immediately. A
Worker at high watermark leaves the READY candidate set immediately; aggregate
pool pressure closes only that pool. Closure never cancels an Accepted Job and
never deletes local output. Re-entry requires all of the following in one locked
recalculation:

- a fresh successful Artifact Store observation;
- the affected Worker at or below its low watermark, not merely below high;
- aggregate pool used scratch at or below the configured pool low watermark;
- no critical Worker; and
- a non-stale current-epoch observation for every Worker counted as available.

Admission and Assignment both recheck the durable condition inside their
existing transactions. An out-of-transaction prediction is not authority. A
busy Worker may finish or resume finalization while the pool is closed, but no
new Assignment is issued.

`pool_readiness_allowed` and `pool_assignment_allowed` are distinct results.
Readiness remains eligible in an otherwise admissible pool whose Workers are all
`WARMING`, so a newly materialized pool can bootstrap its first READY Worker.
The Worker Agent reports capacity and runs pending readiness work before applying
pool Assignment eligibility; passing readiness does not itself authorize an
Assignment.

## Worker Readiness

A readiness cycle is immutable for `(cycle_id, worker_id, worker_epoch,
execution_profile_revision_id, inference_backend_revision, requested_by)`.
Beginning a cycle moves a non-quarantined Worker to `WARMING + SUSPECT`, requires
no active Attempt or Lease, and invalidates all older profile-readiness rows for
that Worker epoch. A quarantined Worker still requires a higher epoch before a
cycle may begin.

Cycle establishment does not require a pre-existing capacity observation for the
new Worker epoch: the identity scheduling gate prevents that Pod from producing
one before the cycle exists. Establishment grants no execution authority. After
the gate is removed, the Worker Agent reports current-epoch capacity before it
fetches or executes readiness work; evidence acceptance and final promotion still
fail closed on missing, stale, or blocked Worker/pool capacity.

The five ordered evidence kinds are:

```text
IDENTITY -> DEVICE -> INFERENCE_BACKEND -> MODEL_WARMUP -> CANARY
```

Each report has a SHA-256 evidence digest and exact actor identity. Exact replay
returns the existing result. A conflicting replay, skipped check, stale epoch,
wrong profile/backend, failed check, expired cycle, active execution authority,
or capacity blocker fails closed. A failed check makes the cycle terminal
`FAILED` and leaves the Worker `DRAINING + SUSPECT`; it never advances to the
next check automatically.

Fetching readiness work uses PostgreSQL time. If the selected cycle deadline has
elapsed, the same locked transaction changes the cycle to terminal `EXPIRED`,
clears `next_check`, moves the Worker to `DRAINING + SUSPECT`, and returns no
work. A Worker Agent or Runner timeout therefore cannot leave a cycle permanently
`CHECKING` or allow stale work to execute.

Only a passing CANARY after the four preceding checks atomically writes the
current profile-readiness row and changes the Worker to `READY + HEALTHY`. The
same transaction rechecks current epoch, identity, no active Attempt/Lease,
ACTIVE or CANARY profile compatibility, and ADMITTABLE Worker/pool capacity.
Remediation success no longer returns a recovering/OFFLINE Worker directly to
service: it enters `WARMING + SUSPECT` and requires this cycle.

The Fleet maintenance Protobuf represents Worker lifecycle and reachability as
closed enums. `UNSPECIFIED` and unknown numeric values are rejected at both
transport boundaries rather than being accepted as arbitrary strings.

## DrainOperation And Mutation Authorization

`request_drain(operation_id, worker_id, expected_epoch, reason, deadline,
requested_by)` is idempotent. It atomically moves a READY/BUSY/WARMING Worker to
`DRAINING`, thereby stopping new Assignment. It does not revoke an active Lease.
When no active Attempt or Lease remains, the operation becomes `COMPLETE`.

The reconciler may complete a drain after normal Job termination. Reaching the
deadline changes the operation to terminal `EXPIRED`; it does not interrupt an
Accepted Job. Emergency fencing remains the separate remediation/fence authority
and must create its own evidence. A routine rollout cannot turn an expired drain
into permission to delete a Pod.

Every destructive protected-resource request binds a completed operation to the
exact Worker UUID/epoch, Kubernetes UID, namespace, name, resource kind,
operation (`DELETE`, `PATCH_SELECTOR`, `PATCH_IMAGE`, or `REMOVE_FINALIZER`),
and normalized request digest. Exact API-server replay is accepted; conflicting
reuse is rejected. The admission handler denies before mutation if the receipt
cannot be persisted.

A mutation-authorization receipt proves only that a Kubernetes request may be
attempted; it is not deletion-completion evidence. Retirement completion has a
separate append-only receipt bound to the complete resource identity, Worker
and epoch, complete DrainOperation set, trusted Fleet observer identity, and
PostgreSQL completion time. The database revalidates the complete `DELETE` plus
`REMOVE_FINALIZER` authorization set when recording that receipt.

For pool/DaemonSet mutations, the request must provide a complete set of
completed current-epoch Worker drain operations for the pool. A missing Worker,
new epoch, live Attempt/Lease, or unrelated operation denies the whole request.
Node loss remains governed by Lease/fence recovery, not by this Kubernetes
guard.

## Kubernetes Ownership Contract

Protected resources carry:

```text
vela.ai/fleet-protected: "true"
vela.ai/worker-pool-id: <uuid>
vela.ai/fleet-revision: <sha256>
finalizer: fleet.vela.ai/drain-protection
```

The H3 DaemonSet remains `OnDelete`, requests exactly `nvidia.com/gpu: 8` in the
runner container, and preserves the dedicated node selector and taint/toleration
contract. Kubernetes RBAC grants the Fleet service account only the resources
needed to observe nodes and manage protected WorkerPool/DaemonSet/Pod objects.
Argo CD may deliver the Fleet Controller, CRD, webhook configuration, and
versioned desired input, but it receives no destructive access to live protected
objects.

The validating webhook is fail closed (`failurePolicy: Fail`) for CREATE,
UPDATE, and DELETE. It denies non-Fleet delete, selector/image patch, finalizer
removal, owner-label removal, and Argo prune. The Fleet identity is necessary but
not sufficient: destructive requests also require authoritative mutation
authorization.

Protected Pods created from the protected DaemonSet are the sole delegated
exception: the configured `kube-controller-manager` username may issue only the
`CREATE`, and the handler verifies the live parent UID, controller reference,
immutable labels, scheduling gate, node binding, images, resource requests, and
other protected shape. The delegated actor cannot update, delete, or remove a
finalizer.

The webhook's server certificate and `caBundle` authenticate the webhook to the
kube-apiserver. The reverse direction is independent: kube-apiserver
`AdmissionConfiguration` must configure the validating-webhook plugin with a
`kubeConfigFile` whose user has a client certificate and key. That certificate
must chain to `VELA_FLEET_ADMISSION_CLIENT_CA_FILE` and contain exactly
`VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID` as its sole URI SAN. `/validate` rejects
the request before parsing or trusting `userInfo` unless this peer proof passes.

The repository Kustomize base intentionally has no portable `NetworkPolicy`:
control-plane source ranges and node/probe paths are cluster-specific. Every
cluster overlay must add a default-deny-compatible policy that limits admission
port ingress to the exact kube-apiserver/control-plane sources and only the
health-probe sources required by that cluster. A successful base render is not
network-isolation evidence.

## Compatibility, Failure, And Replay Contract

Migration `00023` is additive. Existing N-1 control and Worker binaries may
continue current Leases and finalization, but they cannot receive a new
Assignment after the Fleet capacity/readiness protocol is switched to enforced.
The protocol starts expanded but disabled, then requires an operator receipt and
zero active legacy Assignment writers before switching. Rollback disables new
Fleet mutation/readiness calls but retains the schema and evidence rows; schema
contraction is a later migration after N-1 references and retained events reach
zero.

PostgreSQL is authoritative. Controller crash before commit has no effect;
response loss after commit replays the same operation/evidence/authorization.
Kubernetes success without a persisted authorization is not success. Database
unavailability denies readiness promotion, destructive admission, and retirement
completion. Reconciliation first checks the durable completion receipt; after a
restart, a matching receipt completes without touching Kubernetes again. Without
that receipt, exact Kubernetes-UID absence is only an observation: the reconciler
must revalidate the full persisted mutation authorization and atomically record
completion before reporting success. Initial absence without authorization fails
closed. A same-UID terminating object or an object blocked by another finalizer
remains Pending; a replacement object with a different UID does not satisfy or
inherit the retired identity's authorization and is left untouched while the old
UID's absence is receipted independently.

## Required Repository Evidence

- capacity high/low hysteresis, Artifact Store recovery, worker/pool isolation,
  stale observation, exact replay, and Admission/Assignment transactional checks;
- all five readiness checks in order, failed/skipped/conflicting evidence,
  stale epoch/profile/backend, quarantine epoch fence, active Lease denial, and
  atomic final `HEALTHY + READY` promotion;
- idempotent drain request, active-Job preservation, normal completion,
  deadline expiry without fencing, and stale-epoch rejection;
- runtime/owner role separation and denial of direct table access;
- AdmissionReview allow/deny behavior for Pod, DaemonSet, and WorkerPool create,
  delete, selector/image patch, owner/finalizer removal, Argo prune, malformed
  input, database outage, exact replay, verified kube-apiserver client identity,
  forged `userInfo`, and the narrow Pod-controller create delegation;
- Fleet reconciliation of immutable desired revision to protected `OnDelete`
  DaemonSet shape without deleting an undrained Worker, exact-UID absence before
  completion, blocked-finalizer Pending behavior, append-only durable completion,
  and restart replay without Kubernetes mutation;
- deterministic Protobuf/sqlc generation, migration down/up, N/N-1 protocol,
  unit/race/integration/cross-build, and deployment rendering checks.

## Remaining Production Evidence

Repository evidence cannot prove real Kubernetes admission/RBAC behavior, the
cluster-specific admission ingress policy, live Fleet convergence, approved
image digests, H3 topology, SGLang warm-up/canary, XFS/NVMe thresholds, Artifact
Store recovery, long-Job drain/rollback, or any soak/fault/DR/on-call result.
Those require versioned Launch Receipts. Production remains `0/9 PASS`.
