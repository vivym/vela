# Execution Failure, Retry Decision, And Job Expiry

Status: Implemented

Date: 2026-08-21

Predecessors:

- `docs/specs/0001-admission-control-plane-foundation.md`
- `docs/specs/0002-worker-assignment-and-execution-lease.md`
- `docs/specs/0003-worker-start-and-billable-start.md`
- `docs/specs/0004-worker-heartbeat-lease-renewal-and-progress.md`

## Goal

Ensure that every failed or abandoned EXECUTION Attempt converges exactly once to a durable RetryDecision, that expired Accepted Jobs cannot remain stuck, and that replacement execution can proceed only within the immutable Attempt, compute, and Job Expiry budgets.

## Governing Decisions

- ADR 0011: a terminal Failed Job releases CreditReservation and creates no Charge.
- ADR 0020: immutable Job Expiry fences active work and ends further Retry.
- ADR 0021: Retry is bounded by Attempts and cumulative compute time.
- ADR 0028: Worker loss makes the Attempt LOST and replacement execution recomputes from the beginning.
- ADR 0013: the additive schema and events remain N/N-1 compatible.
- ADR 0024: Retry re-enters bounded shared capacity rather than reserving an idle Worker.

## In Scope

1. A coordinator `Fail` operation receiving authenticated Worker identity, current EXECUTION Lease credentials, and a stable structured FailureObservation.
2. Internal reconciliation of expired pre-FINALIZATION Jobs and EXECUTION Leases using PostgreSQL time and a configured Worker Lost grace period.
3. An immutable, one-per-Attempt failure and RetryDecision receipt that makes Worker replay, scanner replay, and concurrent replicas idempotent.
4. Conservative whole-second compute accounting for every Attempt that reaches FAILED or LOST.
5. RetryDecision from the immutable Job execution policy, including retryable class, Worker recommendation, Attempt budget, cumulative compute budget, backoff, and Job Expiry.
6. Atomic fence advancement, Attempt terminal state, Lease revocation, Job transition, RetryRuntimeState, Worker state, Project/pool normal-queue and retry-wait counters, CreditReservation, Organization credit, and Outbox event.
7. A real replacement `Acquire` after RETRY_WAIT that uses another eligible Worker, preserves Job snapshots and Billable Start, resets Attempt progress, and receives a strictly greater fence.
8. Additive PostgreSQL, sqlc, and Protobuf evolution compatible with the previous `9cb1a52` control binary.
9. Integration, replay, concurrency, lock-order, expiry, rollback, RLS, migration, and race evidence.

## Explicitly Deferred

- Protobuf/gRPC streaming transport and extraction of Worker identity from an mTLS certificate.
- Hierarchical Scheduler selection, retry-lane fairness, Protected Lane, and capacity prediction.
- Global multi-Job revision circuit aggregation, automatic ProfileCertification invalidation, deterministic OOM profile escalation, and GPU remediation.
- FINALIZING Job Expiry, FINALIZATION failure/recovery, ArtifactUpload, ArtifactSet commit, and Visible Completion. The FINALIZATION slice must apply the same immutable `job_expires_at` ceiling before it can claim completion.
- Customer Cancellation, Charge persistence, cancellation/completion races, and stop acknowledgement.
- Worker health recovery, canary validation, and transition from DRAINING/OFFLINE back to READY/HEALTHY.

The deferred transport must call these coordinator seams. It must not implement a second RetryDecision path or treat a delivery acknowledgement as execution ownership.

## Failure Contract

The trusted Worker transport passes:

```text
AuthenticatedWorker { worker_id }
LeaseCredentials {
  attempt_id
  worker_epoch
  fence
  opaque lease_token
}
FailureObservation {
  failure_class
  failure_fingerprint
  error_summary
  backend_stage
  gpu_uuids
  inference_backend_revision
  retry_recommended
  worker_reusable
}
```

`failure_class` is a stable upper-snake-case identifier of 1 to 100 characters. `failure_fingerprint` is a stable machine identifier of 1 to 200 characters drawn from ASCII letters, digits, `.`, `_`, `:`, `/`, and `-`; it is not free-text log content. `error_summary`, `backend_stage`, and `inference_backend_revision` contain 1 to 2000, 1 to 100, and 1 to 200 printable characters respectively. `gpu_uuids` contains at most eight distinct printable identifiers of at most 100 characters each.

`retry_recommended` is trusted diagnostic input but not sole retry authority. A retry requires both this recommendation and membership of `failure_class` in the immutable `execution_retryable_failure_classes`. `worker_reusable` reports that the Worker Agent has stopped the failed work and still considers its appliance usable; the coordinator may return BUSY to READY only when this is true and the Worker is still HEALTHY. Otherwise the Worker becomes or remains DRAINING.

A committed call returns:

```text
RetryDecision {
  disposition: RETRY_WAIT | FAILED
  failure_class
  attempt_id
  job_id
  attempt_state: FAILED | LOST
  attempt_compute_seconds
  total_compute_seconds
  optional next_retry_at
  job_fence
  job_version
  decided_at
}
```

The exact same Worker credentials and canonical FailureObservation replay the durable decision. A different observation for an already decided Attempt, or any credential that no longer identifies current authority before a decision exists, returns `REJECTED_STALE_LEASE` without mutation.

## Expiry Reconciliation

The internal coordinator exposes a bounded `ReconcileNextExecutionFailure` operation. In this pre-FINALIZATION slice, each call first looks for a QUEUED, RETRY_WAIT, ASSIGNED, or RUNNING Job whose `job_expires_at <= clock_timestamp()`, then for an unrevoked Worker-owned EXECUTION Lease whose `expires_at + worker_lost_grace <= clock_timestamp()`. Candidate discovery is advisory; the transaction locks and rechecks every condition. FINALIZING remains non-terminal in the architecture, but its Job Expiry fencing is implemented with FINALIZATION authority and Artifact recovery rather than partially in this coordinator.

- Job Expiry uses `JOB_EXPIRED`, is never retryable, advances the Job fence, ends an active Attempt as FAILED, revokes its Lease, and makes the Job FAILED.
- EXECUTION Lease loss uses `WORKER_LOST`, ends the Attempt as LOST, marks the Worker DRAINING and OFFLINE, advances the Job fence, and applies the normal RetryDecision.
- A QUEUED or RETRY_WAIT Job may expire without an active Attempt. It still becomes FAILED and releases its queue counters and CreditReservation exactly once.
- An expired Lease before the Worker Lost grace has elapsed is not a candidate. The Lease cannot renew after expiry, so the grace delays replacement but does not restore authority.
- Multiple reconcilers may discover the same candidate. Row locks and the durable decision receipt permit exactly one state/counter/credit/event transition; later calls replay or find no work.

All time comparisons and `next_retry_at` derivation use PostgreSQL time. Caller wall clock is never authoritative.

## Compute Accounting

For ASSIGNED Attempts, `attempt_compute_seconds` is zero. For RUNNING Attempts it is:

```text
ceil(max(0, min(decided_at, lease_expires_at, job_expires_at) - started_at))
```

to whole seconds. The value is computed once, stored in the decision receipt, and added once to `RetryRuntimeState.compute_seconds_consumed`. Replay never measures a later interval or adds compute again. Overflow or inconsistent timestamps fail the transaction without partial mutation.

The initial `billable_started_at` remains immutable across Retry and does not itself create a Charge. Compute accounting is internal Retry Budget / COGS evidence, not customer price.

## Retry Decision

Decision order is fixed:

1. `JOB_EXPIRED`, any class absent from the immutable retryable-class snapshot, or `retry_recommended = false` is terminal FAILED.
2. `attempts_started >= execution_max_attempts` is terminal FAILED.
3. `compute_seconds_consumed + attempt_compute_seconds >= execution_max_total_compute_seconds` is terminal FAILED.
4. Parse the immutable exponential backoff policy `{kind, initial_seconds, max_seconds}`. An invalid policy is a fail-closed terminal configuration failure, not an unbounded Retry.
5. Calculate capped exponential backoff from Attempts already started. If `decided_at + backoff >= job_expires_at`, Retry cannot begin in time and is terminal FAILED.
6. Otherwise the Job enters RETRY_WAIT with authoritative `next_retry_at`, retains its CreditReservation and original waiting age, and excludes the failed Worker through Job Expiry with the reason and Worker epoch.

Global cross-Job fingerprint thresholds and revision circuit opening remain deferred, but the receipt and per-Job fingerprint history are persisted now so that later circuit logic does not parse logs or rewrite historical decisions.

## Atomic Failure Decision

For an Attempt-scoped decision, the transaction must:

1. lock the Worker first, then acquire the shared EXECUTION Lease write relation lock;
2. lock the matching Lease, Attempt, Job, RetryRuntimeState, CreditReservation, Project, Worker pool, and when terminal the Organization credit row in the established order;
3. for Worker `Fail`, require exact Worker, epoch, fence, token digest, Worker owner, EXECUTION phase, unrevoked and unexpired Lease, and current Job fence;
4. for reconciliation, require Job Expiry or Lease expiry plus grace using PostgreSQL time;
5. find and replay an existing decision before applying any second terminal transition;
6. calculate and persist Attempt compute exactly once;
7. end the Attempt as FAILED or LOST, set `ended_at`, revoke the Lease, and advance the Job fence;
8. update Worker lifecycle/reachability according to the trusted failure source;
9. update RetryRuntimeState compute, fingerprint, exclusion, last class, version, and optional `next_retry_at`;
10. if retrying, move Job to RETRY_WAIT, increment Project/pool compatibility `queued_count` and its `retry_wait_count` subset, decrement Project running count, retain RESERVED CreditReservation, and emit one `job.retry_wait` Outbox event;
11. if terminal, move Job to FAILED, decrement the applicable normal queued, retry-wait, or running counters, release CreditReservation and Organization reserved credit, create no Charge, and emit one `job.failed` Outbox event;
12. commit before returning the durable RetryDecision.

Failure before commit leaves Attempt, Lease, Job, Worker, counters, credit, RetryRuntimeState, receipt, and Outbox unchanged.

## Database Invariants

- At most one immutable failure decision exists per Attempt; at most one no-Attempt Job Expiry decision exists per Job.
- Decision Organization, Project, Job, Attempt, Worker, epoch, and fence are bound through composite foreign keys where applicable.
- A decision's disposition, failure class/fingerprint, compute charge, Job fence/version, time, and retry time cannot change.
- FAILED and LOST Attempts have `ended_at`; active Attempts do not.
- A Job in RETRY_WAIT has no active Attempt or Lease, a future `next_retry_at`, RESERVED credit, and dedicated retry-wait counters. During the expand phase, `queued_count` is the N/N-1 compatibility total of all waiting Jobs and `retry_wait_count` is its Retry subset; therefore `queued_count - retry_wait_count` is the normal Admission queue. New Admission limits use that difference, so an already Accepted Job cannot be stranded when the normal queue fills while it runs.
- A FAILED Job has no active Attempt or Lease, no `next_retry_at`, RELEASED credit, and no reserved Organization credit for that Job.
- A replacement Attempt fence is strictly greater than both the lost Attempt fence and the fencing value recorded by its RetryDecision.
- Customer request roles cannot read raw failure evidence, Worker identity, fingerprint, or diagnostic summary.

## Events And Compatibility

`job.retry_wait` and `job.failed` extend the existing version-1 `EventEnvelope` with new oneof field numbers and typed payloads. They carry identifiers, aggregate version, stable failure class, compute accounting, decision time, and optional retry time, but no error summary, GPU UUID, fingerprint, prompt, or Customer Content.

Migration 00005 is expand-only. The exact `9cb1a52` N-1 control binary and its request/internal database startup verification must still pass after migration. Old Outbox publishers continue to forward opaque Protobuf bytes; consumers must ignore unknown additive event payloads. Existing event fields, privileges, and enum values do not change meaning. Initial Up treats the legacy request-writable exclusion/fingerprint columns as untrusted and seeds protected evidence empty; only a private rollback stash can restore trusted evidence. Operational binary rollback retains the expanded schema. Structural Down first blocks every failure/admission write relation, then refuses while any RETRY_WAIT Job or retry subset remains because the N-1 schema cannot represent a full normal queue plus Retry without weakening its bound. After drain it preserves immutable decision receipts and protected evidence in `vela_private` for a later Up instead of exposing them through the request-readable N-1 schema.

`queued_count` is the one deliberate mixed-version bridge: it expands from the normal waiting count to the total waiting count, while additive `retry_wait_count` identifies the Retry subset. New binaries apply normal Admission limits to their difference. Both N and N-1 Assignment decrement the total; a database trigger also decrements the Retry subset on RETRY_WAIT -> ASSIGNED, so an N-created Retry can be consumed by N-1 without counter corruption. The N-1 Admission writer remains safe but may reject conservatively while Retry rows exist because its compiled Project precheck sees the total. This bounded rollout degradation is accepted only during expand/drain; the later contract migration must restore one canonical counter projection after N-1 is absent.

## Test Seams And Evidence

The pre-agreed public seams are:

1. coordinator `Fail` after verified Worker identity;
2. internal `ReconcileNextExecutionFailure` using PostgreSQL time;
3. existing `Acquire`, `Start`, and `Heartbeat` authority after a decision;
4. authenticated Project `GET Job` state/progress projection;
5. PostgreSQL migration, constraints, counters, RLS, credit, and Outbox payloads.

Required evidence:

- exact Worker Fail replay returns one decision; different replay is stale and has no effect;
- concurrent Fail, Heartbeat, Job Expiry, and Lease-loss reconciliation produce one winner without deadlock or double accounting;
- a Lease is not reconciled before expiry plus grace and is reconciled by PostgreSQL time afterward;
- ASSIGNED loss charges zero compute; RUNNING loss uses capped ceiling-to-second accounting and replay does not increase it;
- non-retryable class, false recommendation, max Attempts, compute budget, backoff beyond Job Expiry, invalid policy, and Job Expiry each produce terminal FAILED, released credit, decremented counters, and no Charge;
- retryable early loss produces RETRY_WAIT, exact next retry, retained credit, bounded counters, Worker exclusion, and one event;
- replacement Acquire before `next_retry_at` fails; afterward another READY/HEALTHY certified Worker receives a higher fence, preserved snapshots and Billable Start, and reset progress;
- late old Start, Heartbeat, and Fail are rejected after fencing; the FINALIZATION slice must prove that any later completion authority is rejected before it can claim Visible Completion;
- QUEUED and RETRY_WAIT Job Expiry work without an active Attempt;
- request-role SQL cannot read failure receipts or mutate failure/retry state;
- forced failures at each write roll back the complete transaction;
- migration up/down/up preserves trusted receipts/evidence, rejects concurrent active Retry, ignores poisoned legacy request evidence, and generated sqlc/Protobuf, actual `9cb1a52` N-1 startup, lint, unit, integration, and race tests pass.

## Completion Boundary

This slice is complete only when repository generation, lint, and unit tests pass from a clean commit; PostgreSQL integration and race tests pass; Fail/replay, expiry scans, compute accounting, retry/terminal decisions, replacement fencing, credit/counter invariants, rollback, RLS, N/N-1 compatibility, and Outbox behavior are evidenced; and a two-axis review finds no unresolved P0-P2 issue.
