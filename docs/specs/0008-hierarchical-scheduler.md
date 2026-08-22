# Hierarchical Scheduler

Status: implementation specification

## Scope

This slice implements the PostgreSQL-authoritative Scheduler required by ADRs
0010, 0016, 0022, and 0024. It selects one Customer Organization, immutable
ServiceClassRevision, Project, Job, certified ExecutionProfileRevision, and
compatible READY Worker in that order, then delegates the only Assignment state
transition to `workercontrol.Service.Acquire`.

The slice includes weighted deficit fairness, bounded Job aging, a Protected
Lane, a separately capped retry lane, work-conserving placement, durable dispatch
claims, multi-replica coordination, crash reconciliation, and the database
protocol gate needed to drain N-1 direct candidate writers. It does not add a
per-Worker queue, CapacityReservation, Hard Deadline, price-aware scheduling, or
GenerationPresetRevision-derived priority.

## Ownership And Public Seams

The pre-agreed public seams are:

1. `scheduler.Service.RunOnce` for one Worker pool;
2. `workercontrol.Service.Acquire` as the sole Assignment authority;
3. PostgreSQL dispatch claim expiry and reconciliation across Scheduler replicas;
4. existing Worker Start, Heartbeat, Failure, Cancellation, and Visible
   Completion APIs after the scheduled Assignment;
5. PostgreSQL migration, role, RLS, protocol-gate, counter, and N/N-1 behavior.

The Scheduler never updates Job, Attempt, Lease, Worker lifecycle, Project
counters, CreditReservation, or Outbox state. `Acquire` continues to lock and
recheck those authorities and commits Attempt, Lease, fence, counters, Worker,
Job, RetryRuntimeState, and `job.assigned` atomically.

## Persistent Scheduling Policy

`organization_capacity_shares` stores a Customer Organization's weight and hard
running limit within a Worker pool. `project_capacity_shares` stores the Project
weight within that pool; the existing Project `queued_limit`, `queued_count`,
`running_limit`, and `running_count` remain the hard Project limits.

ServiceClassRevision gains immutable scheduling fields:

- `queue_weight`;
- `max_queue_wait_before_protection_seconds`;
- `max_aging_credit_seconds`;
- `max_expiry_urgency_credit_seconds`;
- `max_retry_risk_penalty_seconds`.

Worker pool policy supplies the deficit quantum, bounded deficit magnitude, and
retry-running limit. A retry Assignment is an Attempt with `attempt_number > 1`.
The retry limit is rechecked under the Worker-pool lock in `Acquire`, so
concurrent Scheduler replicas cannot overfill it.

`worker_profile_readiness` binds Worker id and epoch to a certified
ExecutionProfileRevision and records whether the profile is warm or may be
prewarmed. It carries bounded cold-start, locality, and health-risk penalties.
The selected Worker minimizes their sum. A stale epoch row is never compatible.

`job_runtime_predictions` optionally holds a calibrated positive runtime
prediction and a non-negative per-Job `retry_risk_penalty_seconds`, both bound to
a source and source revision. In the absence of a runtime prediction the
Scheduler uses the immutable certified p95 runtime multiplied by the Job result
quantity; in the absence of a risk prediction its penalty is zero. The effective
risk penalty is capped by the immutable
`ServiceClassRevision.max_retry_risk_penalty_seconds`. Prediction rows are
scheduling telemetry, not Customer Content, PricingSnapshot, Preset SLO,
Dynamic ETA, or a Hard Deadline.

## Candidate Eligibility

A Job is eligible only when all of the following remain true at PostgreSQL time:

- state is QUEUED, or state is RETRY_WAIT with `next_retry_at` reached;
- Job Expiry is still in the future;
- CreditReservation is RESERVED and Retry Budget is not exhausted;
- ServiceClassRevision, ExecutionProfileRevision, and ProfileCertification are
  ACTIVE and the certification has not been invalidated;
- Project and Customer Organization running limits have room;
- a matching Worker is READY and HEALTHY, has current profile readiness, has no
  active Attempt or live dispatch claim in any Worker epoch, and is not excluded
  by protected Retry evidence;
- the Job has no live dispatch claim;
- a RETRY_WAIT Job fits the pool retry-running limit.

`vela_scheduler_projectable_jobs` is the shared Job-eligibility relation for
both queue projection and real claim candidates. It applies Project and Customer
Organization running capacity, active Job/claim exclusion, and the retry-running
limit before a Job can consume an ephemeral deficit or Worker tail.
`vela_scheduler_job_worker_compatibility` is their shared profile/Worker
relation; it applies certification, current Worker capacity evidence, readiness,
and per-Job active Worker exclusion. Projection may consume READY or evidenced
BUSY capacity from that relation, while an immediate claim additionally requires
its shared row to be READY. Stateful deficit mutation remains claim-only.

`jobs.created_at` is the immutable original waiting-age authority for both first
execution and retry. Migration 00008 rejects any later rewrite of that timestamp.

The current launch schema keeps each Accepted Job in its Admission-selected
Worker pool. Within that compatibility domain every currently certified profile
and every compatible READY Worker is considered. BUSY Workers receive no claim
or pre-assignment, and no Worker is held idle for failure reserve.

The Scheduler derives an ephemeral full-pool capacity timeline. A READY Worker
contributes one physical slot at PostgreSQL `now` only when no active Attempt or
live dispatch claim exists for that Worker in any epoch; a BUSY, HEALTHY Worker
contributes one slot at its current valid heartbeat `estimated_finish_at`. A
Worker that is ready for multiple ExecutionProfileRevisions still contributes
one physical slot. Jobs are projected in the hierarchy's deterministic order
onto the earliest compatible physical slot, then the projection exposes one row
for every compatible certified profile without duplicating Worker capacity.

Every claim, Admission prediction, and single-Job Dynamic ETA builds this
projection only for its target Worker pool and one captured PostgreSQL time.
These narrow operations never invoke the aggregate cross-pool projection. The
pool's hard normal waiting bound plus its durable current `retry_wait_count`
bounds the number of projectable Jobs. Projection and the claim's materialized
composite candidate snapshot inspect at most that bound plus one row and fail
closed with SQLSTATE `55000` if the extra row exists. The extra row detects
impossible Job/counter drift; it is never silently truncated into a partial
fairness snapshot.

The production `RunCycle` also preserves that isolation above SQL. A pool-local
claim or Assignment error ends that pool's work for the current tick, is returned
with the pool identity for alerting, and does not prevent later healthy pools
from dispatching. Cancellation or deadline expiry of the cycle's shared context
still stops the whole cycle immediately.

The earliest compatible slot becomes `predicted_start_at`, and adding the
immutable runtime prediction produces `predicted_finish_at`. Expiry urgency is
calculated from the remaining slack after that predicted finish, not merely from
`job_expires_at - now`. Customer Dynamic ETA aggregates the earliest compatible
finish to one Job row. These projection rows are recomputed observations: their
projected Worker is not Assignment authority and creates no claim,
CapacityReservation, or binding to a BUSY Worker.

Admission calls the narrow Scheduler-role predictor only after idempotency replay
has been resolved and before any counter, credit, Job, or Outbox mutation. The
predictor folds the projected tail of every physical Worker into the same
READY/BUSY capacity evidence. No compatible evidence or predicted queue wait
above `queue_retry_allowance_seconds` returns `503 capacity_unavailable` with
`Retry-After`; the transactional Project and pool counters remain authoritative.
Production startup always injects this predictor, and the production constructor
fails closed when it is absent. A separately named legacy constructor exists only
so N-1 and legacy component tests can exercise their released behavior without
the V8 Scheduler contract.

## Hierarchical Selection

Each claim transaction takes a PostgreSQL advisory transaction lock scoped to
the Worker pool. The lock serializes the short fairness decision across replicas;
it does not cover `Acquire` or execution.

After taking that lock, one claim materializes its eligible candidates once as a
bounded PostgreSQL composite array. Organization, ServiceClassRevision, Project,
Job, and Worker selection all consume that same point-in-time snapshot instead
of rebuilding Dynamic ETA projection at every hierarchy level. `Acquire` still
locks and rechecks the selected authorities in its separate transaction.

At each hierarchy level, positive persistent deficit is consumed before another
round is opened. When no eligible peer retains positive deficit, the transaction
adds `weight * scheduler_quantum_seconds` to every eligible peer. A fixed
pool-wide credit scale derived from the deficit cap, maximum supported weight,
and quantum is applied to both every refill and every predicted-runtime debit.
This keeps the configured cap effective without changing relative service when
the cap is smaller than an unscaled round. Deficits use exact PostgreSQL
`numeric` values; integer rounding at a small cap must not erase a lower-weight
peer's positive credit. It then selects the greatest deficit, with oldest
eligible Job and stable identity as deterministic ties. It applies this
independently in order:

1. Customer Organization;
2. ServiceClassRevision;
3. Project.

The selected Job's predicted runtime is then subtracted from all three selected
deficits, also bounded. The queue projection begins with the same persisted
deficits, then advances an ephemeral copy through the same three nested
selections, round-based refills, and runtime debits while assigning each selected
Job to the earliest compatible physical Worker tail. It never mutates fairness
authority. This keeps Dynamic ETA and Admission ordering aligned with the next
real claims without reserving a Worker or predicting that future telemetry and
arrivals will remain unchanged.

This round-based refill is required: refilling on every
dispatch when runtime is below the quantum would let a heavier peer accrue faster
than it spends and starve lighter peers. This is weighted deficit fairness; it is
not a global request priority score. An expired unconsumed claim may spend one
turn, but bounded deficits and later refill rounds prevent crash-induced
starvation.

Within the selected Project, RETRY_WAIT Jobs are considered before ordinary
QUEUED Jobs only while the retry lane has capacity. A retry keeps the original
`jobs.created_at` waiting age. For ordinary Jobs:

```text
job_order_score =
    predicted_runtime_seconds
  + bounded_retry_risk_penalty_seconds
  - bounded_expiry_urgency_credit_seconds
  - bounded_aging_credit_seconds
```

Smaller scores run first. The per-Job retry-risk prediction and both credits are
capped by ServiceClassRevision. A Service Class constant is not used as the
per-Job penalty because Job ordering occurs only after Service Class selection.
Waiting at least `max_queue_wait_before_protection_seconds` moves the Job to the
Protected Lane, ordered by Job Expiry, FIFO, then Job id instead of the score.
The Protected Lane remains inside Organization, Service Class, Project, hard
capacity, certification, and compatibility boundaries.

## Dispatch Claim And Acquire

Immediate selection considers only compatible READY Workers and creates one
`scheduler_dispatch_intents` row in state CLAIMED with the exact hierarchy, Job
version, selected Worker epoch, selected profile, lane, prediction, score,
predicted start/finish, claim owner, and PostgreSQL expiry. This durable dispatch
decision receipt is distinct from the ephemeral full-pool queue projection: it
may reuse the projection timestamps, but only its exact READY Worker/profile pair
can authorize `Acquire`. Partial unique indexes permit at most one live claim for
a Job and one live claim for a Worker.

`RunOnce` passes the claim id in `AssignmentCandidate`. `Acquire` locks the claim
and requires exact Worker, epoch, Job, Job version, profile, owner-independent
CLAIMED state, and unexpired PostgreSQL deadline. In the same transaction as the
Assignment it inserts the Attempt with the claim id and changes the claim to
COMMITTED. No separate Scheduler write can claim that Assignment succeeded.

Organization and retry-lane hard limits are serialized by first locking their
Capacity Share or Worker-pool authority row and only then counting active
Attempts in a subsequent statement. A count derived inside the locking statement
would retain its pre-wait Read Committed snapshot and is not valid limit evidence.
Partial active-Attempt indexes cover the pool-plus-Organization count and the
pool retry count; they optimize the post-lock checks but do not replace the locks.

If `Acquire` reports a stale candidate or unavailable Worker, `RunOnce` abandons
the claim and may select another candidate up to its configured bounded attempt
count. Other errors abandon the claim and return. A Scheduler crash before claim
commit leaves no claim; after claim commit but before `Acquire`, expiry makes the
Job and Worker selectable again; after Assignment commit, the claim and Attempt
are already atomically linked.

## Multi-Replica And Reconciliation

All coordination uses PostgreSQL. Scheduler process memory contains no fairness
authority and no irreplaceable queue state. Claim selection uses the pool-scoped
advisory transaction lock plus partial unique indexes. `ReconcileExpired` marks
expired CLAIMED rows ABANDONED; selection performs the same reconciliation before
each claim, so a dedicated reconciler is an optimization rather than a liveness
requirement.

Assignment replay remains Worker-scoped and is unchanged: an active Assignment
is replayed without creating a new Attempt, fence, claim, or Lease deadline. A
Job or Worker can be selected again only after the previous claim is abandoned
or expired and all existing Assignment authority has ended.

## Roles, Isolation, And Compatibility

`vela_scheduler` is a NOLOGIN, non-superuser, non-BYPASSRLS group role. It can
execute narrow SECURITY DEFINER claim, abandon, reconcile, and pool-discovery
functions but cannot read request content, pricing amounts, credentials, credit,
Artifact, Charge, failure diagnostics, or raw Retry evidence. Customer request,
auth, cancellation, and Artifact roles receive no scheduling-table or projection-
function privileges. The request transaction first proves Project-scoped Job
visibility, then the control service obtains Customer Dynamic ETA through the
narrow Scheduler-role `vela_predict_job_dynamic_eta(uuid)` function. This keeps
the released request-role privilege boundary unchanged for N-1 control binaries.

Migration 00008 is additive. N-1 control binaries may continue direct
`AssignmentCandidate` writes while `require_dispatch_intent` is false. The
versioned protocol transition is executable only by the migration/operator
identity and requires an operator receipt for both enable and rollback; every
transition appends its version, receipt, and time to immutable history. The
persistent state row rejects arbitrary UPDATE with SQLSTATE `55000`. The
transition function locks current state and inserts the next contiguous history
row; a migration-owned trigger validates that row, then a nested trigger
atomically advances state to its exact values. The state trigger accepts only
that nested path with a matching immutable history row, so a session GUC or
public token cannot forge transition authority. Any validation, history, or
state failure rolls back the whole statement. The persistent control runtime can
neither read the switch or history, update them,
nor execute the transition function. The enforcement trigger runs under its
migration-owned SECURITY DEFINER boundary.
Once enabled, it rejects every new Attempt without an exact live Scheduler
claim; this also fences an N-1 binary that does not know the new Go field. An
operator may disable the gate for an operational binary rollback after draining
writers; the protocol-row lock serializes that transition with in-flight Attempt
inserts, and the rollback appends its own receipt. Operational binary rollback
retains the expanded schema. Structural Down refuses while any dispatch claim,
claim-linked Attempt, custom policy/readiness/prediction, or protocol transition
history remains.

## Required Evidence

- weighted Organization and Project service follows configured ratios over
  repeated dispatches and survives a Scheduler restart;
- Service Class selection uses its queue weight and never Preset or price;
- a non-zero per-Job retry-risk prediction can change ordinary Job order inside
  one Service Class but cannot exceed that ServiceClassRevision's immutable cap;
- short ordinary Jobs cannot starve an old long Job; threshold crossing moves it
  to Protected Lane and preserves Expiry/FIFO order;
- retries retain original waiting age, precede ordinary Jobs inside the selected
  hierarchy, concurrent replicas cannot exceed the retry-running limit, and a
  full retry lane leaves waiting Retry Jobs out of projection without consuming
  an ordinary Job's Worker tail or fairness credit;
- BUSY, unhealthy, stale-epoch, uncertified, invalidated, excluded, or
  profile-incompatible Workers are never claimed, and projection never places a
  Retry Job on a Worker excluded by that Job's active protected evidence;
- every READY compatible Worker remains available to ordinary work and an idle
  compatible profile is selected by bounded Worker score;
- two Scheduler replicas racing create one claim and one Assignment per Job and
  Worker; stale claims expire and are rediscovered without changing Job state;
- crash before claim, after claim, and after Assignment commit leaves no stuck
  Job, duplicate Attempt, duplicate fence, or duplicate Outbox event;
- `Acquire` rechecks exact claim, limits, Job version/state/time, Retry Budget,
  certification, Worker state/epoch, and exclusion inside its transaction;
- claim projection remains isolated to the requested Worker pool, candidate
  materialization is bounded by the pool's normal/retry waiting authority, and
  impossible counter drift fails closed without partial dispatch or halting a
  later healthy pool in the same production cycle;
- migration up/down/up, RLS/privilege negatives, direct protocol-state UPDATE
  rejection even after a forged session GUC, contiguous atomic transition
  history, exact N-1 startup/writer behavior, generation, lint, unit,
  integration, and race tests pass.

## Completion Boundary

This slice is complete only when all required evidence passes from a clean fixed
point, the Scheduler production loop uses the public seam, migration and N/N-1
receipts pass, and two-axis Standards and Spec review has no unresolved P0-P2
finding. Repository evidence advances ADRs and acceptance scenario 7 but does not
by itself pass any deployment Production Gate.
