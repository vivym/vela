# Cross-Job Profile Certification Circuit

Status: Implemented

Date: 2026-08-23

Predecessors:

- `docs/specs/0002-worker-assignment-and-execution-lease.md`
- `docs/specs/0005-execution-failure-retry-and-job-expiry.md`
- `docs/specs/0008-hierarchical-scheduler.md`

## Goal

Turn repeated structured failures on the same certified
ExecutionProfileRevision into one PostgreSQL-authoritative circuit decision. The
decision must invalidate the exact ProfileCertification before another
Assignment can use it, preserve an immutable receipt, and let the triggering Job
retry only when another actively certified ExecutionProfileRevision can still
satisfy its immutable Model, GenerationPresetRevision, ServiceClassRevision,
OutputSpec, Worker pool, Retry Budget, and Job Expiry.

## Governing Decisions

- ADR 0013: migration 00010 is additive, and the exact `e1c5054` N-1 control
  binary remains safe during an explicit protocol transition.
- ADR 0017: `quality`, `balanced`, and `fast` remain stable product aliases;
  every saleable revision and OutputSpec needs independent evidence.
- ADR 0018: only an ACTIVE, non-invalidated ProfileCertification can satisfy an
  Assignment or become an active priced offer.
- ADR 0021: repeated structured failure is bounded by an immutable circuit
  policy instead of free-text parsing or an unbounded Job-local retry loop.
- ADR 0022: Scheduler selection remains PostgreSQL-authoritative and immediately
  excludes an invalidated ProfileCertification.
- ADR 0028: replacement execution recomputes on another eligible profile; it
  does not mutate the Job's product or service snapshots.
- ADR 0029: repository evidence implements the control behavior but is not a
  three-Preset certification Launch Receipt or any other Production Gate.

## In Scope

1. Versioned circuit threshold and observation-window fields on
   ServiceClassRevision, copied into every Accepted Job's immutable execution
   policy snapshot.
2. A protocol gate that keeps N/N-1 failure writers safe: before activation,
   version-1 and version-2 writers retain the pre-circuit behavior; after an
   operator-receipted switch, legacy failure-decision inserts fail closed.
3. Cross-Job aggregation of one stable `failure_fingerprint` over distinct
   Workers that were HEALTHY when they submitted a trusted Worker-reported
   FailureObservation for the same exact ProfileCertification.
4. Serialization on the ProfileCertification so concurrent Fail transactions
   cannot miss one another at the threshold or create multiple openings.
5. Atomic ProfileCertification invalidation, immutable opening receipt, current
   Job circuit state, RetryDecision, Attempt/Lease/Worker transition, counters,
   credit, and Outbox result.
6. Retry after opening only when a different ACTIVE ExecutionProfileRevision in
   the Job's fixed Worker pool has an ACTIVE, non-invalidated certification for
   the same ModelRevision, GenerationPresetRevision, and OutputSpec.
7. Scheduler and direct coordinator Assignment rechecks that observe the same
   certification row lock and reject stale claimed candidates.
8. Retention by foreign key: the opening receipt keeps the invalidated
   ProfileCertification, ExecutionProfileRevision, triggering decision, Job,
   and Attempt referenced and undeletable.
9. Migration, role, replay, concurrency, rollback, N/N-1, generation, lint,
   integration, and race evidence.

## Explicitly Deferred

- Benchmark execution, quality/statistical analysis, certification issuance,
  ACTIVE promotion, or real Launch Receipts for `quality`, `balanced`, and
  `fast`. Those require measured evidence outside repository tests.
- Human or operator APIs to inspect, acknowledge, close, or override a circuit.
  An invalidated certification is not reactivated; remediation issues a new
  immutable revision and certification.
- ModelRevision-wide or InferenceBackendRevision-wide aggregation. This slice
  opens the narrow exact ProfileCertification circuit proven by the current
  schema and trusted Attempt identity.
- Worker remediation, Quarantine, canary validation, and return to READY.
- Protobuf/gRPC transport for `Fail`; the trusted transport still calls the
  coordinator seam defined by Slice 5.

## Public Seams

The pre-agreed test seams are:

1. coordinator `Fail` after verified Worker identity and current EXECUTION Lease;
2. Scheduler claim plus coordinator `Acquire` for the next Assignment;
3. authenticated Admission for execution-policy snapshot persistence;
4. PostgreSQL migration, protocol transition, constraints, privileges, and
   immutable receipts.

Tests may inspect PostgreSQL after an operation to prove atomic invariants, but
they must trigger behavior through these production seams. Direct SQL mutation
is limited to fixture setup, protocol transition, fault injection, and negative
constraint/privilege evidence.

## Circuit Policy Snapshot

ServiceClassRevision owns two immutable launch policy values:

```text
circuit_fingerprint_window_seconds
circuit_min_distinct_healthy_workers
```

Migration 00010 expands existing revisions and Jobs with the launch defaults of
3600 seconds and 2 distinct healthy Workers. New Admission resolves the fields
from the selected immutable ServiceClassRevision and copies them into:

```text
execution_circuit_fingerprint_window_seconds
execution_circuit_min_distinct_healthy_workers
```

The legacy `execution_circuit_breaker_policy` JSON remains an immutable policy
revision identifier for N/N-1 compatibility. Runtime decisions use the typed
snapshot fields, never mutable current catalog values. A threshold is at least
2 and a window is bounded to 1 through 604800 seconds.

## Protocol Transition

Migration 00010 creates a singleton protocol state and append-only transition
history. Expansion starts at protocol version 1 with circuit aggregation
disabled. An operator-only transition function requires a non-empty receipt,
serializes with every `execution_failure_decisions` insert, appends the next
history version, and changes the singleton state atomically.

Every new failure decision records its circuit protocol version. While the gate
is disabled, both N and N-1 writers create version-1 decisions and no global
circuit is opened. After the operator has drained N-1 failure writers and
enables the gate, N writes version 2 and applies the complete circuit behavior;
the database rejects a legacy version-1 insert with SQLSTATE `55000`. Replays of
already committed version-1 decisions remain valid and immutable.

Operational rollback first disables the gate with another receipt, then may run
the N-1 binary against the expanded schema. Structural Down refuses if the gate
was ever transitioned, any opening receipt exists, or typed policy values differ
from the migration defaults. Production activation requires a separate rollout
receipt and does not happen implicitly during migration.

## Failure Aggregation

For a new Worker-reported failure under protocol version 2, the coordinator:

1. locks the failure-decision write relation and protocol row;
2. locks the exact ProfileCertification selected by the Attempt's immutable
   execution profile plus the Job's fixed Model, Generation Preset, and
   OutputSpec;
3. considers only immutable decisions with source `WORKER_REPORTED`, the same
   exact ProfileCertification identity, the same fingerprint, a Worker that was
   HEALTHY at observation time, and `decided_at` inside the triggering Job's
   immutable window;
4. adds the current observation once and counts distinct Worker identities;
5. opens only when that independent count reaches the immutable threshold.

Replays do not add observations because the one-per-Attempt decision receipt is
found before aggregation. Multiple Attempts on one Worker count once. Lease loss,
Job Expiry, FINALIZATION recovery, an unhealthy Worker, a different fingerprint,
a different certification, and observations outside the window do not count.

## Atomic Opening And Retry

The threshold-crossing transaction changes the ProfileCertification from ACTIVE
to INVALID with one PostgreSQL timestamp and creates exactly one immutable
`profile_certification_circuit_openings` receipt containing:

- ProfileCertification and ExecutionProfileRevision identity;
- triggering execution-failure decision, Job, Attempt, and Worker;
- failure class and fingerprint;
- immutable threshold and window;
- observed distinct healthy Worker count and evidence-window start;
- trusted Inference Backend revision and opening time.

The Job's RetryRuntimeState records only a non-sensitive OPEN circuit marker;
raw fingerprint and Worker evidence remain internal. Customer request roles
cannot read the receipt or raw evidence.

Opening is evaluated independently of whether the current FailureObservation is
otherwise retryable. The current Job enters RETRY_WAIT only when the ordinary
Retry Budget decision succeeds and a different actively certified execution
profile exists. Otherwise it becomes FAILED, releases CreditReservation, and
creates no Charge. A later Scheduler claim and `Acquire` can select only the
alternate profile. The immutable Job product, pricing, Service Class, Job Expiry,
and Billable Start do not change.

If the certification was already invalidated by a concurrent circuit or quality
decision, the current Job applies the same alternate-profile requirement without
creating another opening. The certification row lock linearizes concurrent
Assignment and invalidation: whichever obtains the lock first may commit, and
every later Assignment recheck rejects the invalidated certification.

## Database Invariants

- At most one opening receipt exists for a ProfileCertification.
- An opening receipt is immutable and cannot be deleted or truncated.
- An invalidated ProfileCertification cannot be reactivated or have its original
  invalidation timestamp changed.
- Circuit policy fields on ServiceClassRevision and Job are immutable and
  bounded; N-1 inserts receive the launch defaults.
- The triggering failure decision and opening receipt commit together. Neither
  can exist as a partial threshold transition.
- An opening's exact ProfileCertification identity, Attempt execution profile,
  Job product snapshot, and decision identity must agree through foreign keys
  and a deferred consistency constraint.
- Customer request, auth, artifact, cancellation, billing, and Scheduler roles
  cannot read raw circuit evidence or execute the protocol transition.
- Once protocol version 2 is active, a version-1 failure writer cannot commit.

## Required Evidence

- the first matching failure remains below threshold and does not invalidate;
- the threshold failure from another HEALTHY Worker opens exactly once and
  immediately prevents a stale Scheduler candidate from acquiring the profile;
- same Worker, unhealthy Worker, different fingerprint/certification, expired
  window, Lease-loss reconciliation, and replay do not inflate the count;
- concurrent threshold failures create one opening and preserve exactly-once
  RetryDecision, counters, credit, events, and Worker transitions;
- the triggering Job retries on a different certified profile with a higher
  fence and preserved snapshots, or becomes FAILED when no alternate exists;
- forced failure at the certification update, decision insert, and opening
  receipt leaves every table unchanged;
- invalidation/reactivation/delete, receipt mutation/delete, policy mutation,
  cross-identity receipt, role access, and legacy-writer negative tests fail;
- protocol transition history is contiguous and immutable, transition/Fail is
  serialized, and exact `e1c5054` N-1 startup/writer behavior is accepted before
  the switch and rejected after it;
- migration empty Down/Up restores the default surface, while transition history,
  custom policy, or durable opening evidence refuses structural Down;
- `make generate`, lint, unit, integration, race, and two-axis review pass with
  no unresolved P0-P2 finding.

## Completion Boundary

This slice is complete only when the production `Fail` transaction performs the
protocol-gated aggregation and atomic invalidation, Scheduler/`Acquire` reject
the invalidated certification, alternate-profile or terminal behavior is proven
through the public seams, migration/N/N-1/role/rollback/concurrency evidence
passes, generated code is at a fixed point, and review finds no unresolved
P0-P2 issue. Real benchmark evidence and the nine Production Gates remain
separate and must stay unclaimed.
