# Customer Cancellation And Charge

Status: Implemented

Date: 2026-08-21

Predecessors:

- `docs/specs/0001-admission-control-plane-foundation.md`
- `docs/specs/0002-worker-assignment-and-execution-lease.md`
- `docs/specs/0003-worker-start-and-billable-start.md`
- `docs/specs/0004-worker-heartbeat-lease-renewal-and-progress.md`
- `docs/specs/0005-execution-failure-retry-and-job-expiry.md`

## Goal

Make Customer Cancellation a Project-authorized, exactly-once Job Coordinator
decision. Cancellation before Billable Start releases the CreditReservation and
creates no Charge. Cancellation after Billable Start fences execution and posts
the immutable full-quote Charge before returning, without waiting for the Worker
to stop.

## Governing Decisions

- ADR 0001: a Charge is an immutable contract-credit ledger fact, not a payment.
- ADR 0002: Customer Cancellation after Billable Start charges the full quote.
- ADR 0003: later Visible Completion races the same versioned Job authority.
- ADR 0007: the customer request never receives an internal database role.
- ADR 0013: schema, REST, Protobuf, and events remain additive for N/N-1 rollout.
- ADR 0026: every reservation is consumed or released exactly once.
- ADR 0027: cancellation winning the CAS fences late completion and posts Charge.

## In Scope

1. `POST /v1/projects/{project_id}/jobs/{job_id}/cancel` for a current
   Project Service Principal with `jobs:cancel` scope.
2. A narrow `vela_cancel` database role with no table access. It can establish a
   credential-bound request context and execute one audited cancellation command.
3. An immutable, one-per-Job Customer Cancellation decision and one-per-Job
   immutable Charge.
4. Atomic Job fence/version transition, Attempt termination, Lease revocation,
   Worker draining, queue/running counters, CreditReservation, organization
   reserved/unsettled-posted credit, Charge, and Outbox events.
5. Worker stop acknowledgement using the authenticated Worker identity and the
   revoked Lease credentials that were recorded by the cancellation decision.
6. Reconciliation after the canceled Lease's original expiry when stop
   acknowledgement is lost.
7. Additive Protobuf events for cancel requested, canceling, canceled, Charge
   posted, and Invoice export requested.
8. Replay, concurrency, rollback, authorization, migration, N/N-1, and race
   evidence.

## Explicitly Deferred

- Visible Completion and its direct concurrent race with cancellation. This slice
  establishes the Charge and cancellation CAS that the Artifact slice must race.
- ArtifactSet publication and `AlreadySucceeded` Artifact access details. A
  cancellation request for an already-SUCCEEDED Job returns `ALREADY_SUCCEEDED`
  with its optional result pointer once that pointer exists.
- Webhook Subscription delivery. This slice writes the terminal domain event;
  the later Webhook slice creates and delivers Project-scoped deliveries.
- External Invoice export and settlement. This slice writes a durable export
  intent keyed by `charge_id`; an exporter later consumes it idempotently.
- Worker transport, mTLS extraction, and delivery routing of Stop commands. The
  transport must call the coordinator acknowledgement seam and cannot mutate Job.
- Refunds and credit notes, which remain external immutable adjustments.

## HTTP Contract

Cancellation has no body and no request idempotency key. The Job itself is the
idempotency key: only the first valid request records `requested_by_principal_id`
and the durable decision; all later authorized requests replay the same decision
and current terminalization state.

The successful response contains:

```text
CancelResult {
  cancellation_id
  job_id
  decision: CANCELED | CANCELING | ALREADY_SUCCEEDED | ALREADY_FAILED
  state: CANCELED | CANCELING | SUCCEEDED | FAILED
  job_version
  billable
  optional charge { charge_id, amount_minor, currency, reason, posted_at }
  decided_at
}
```

The endpoint returns `401` for invalid credentials, `403` for missing scope or a
different Project, and `404` when the Job is not visible in the authenticated
Project. Scope removal, credential revocation, or expiry after authentication is
rechecked inside the cancellation transaction and fails closed without mutation.

## Cancellation Decision

The transaction uses PostgreSQL time and fixed state rules:

1. `QUEUED` or `RETRY_WAIT`: transition directly to `CANCELED`, decrement the
   matching normal/retry queue counters, release CreditReservation and reserved
   organization credit, create no Charge, and emit `job.canceled`.
2. `ASSIGNED`: advance the Job fence, terminalize the Attempt as `CANCELED`,
   revoke the Lease, mark the Worker `DRAINING`, decrement running counters,
   release credit, create no Charge, and atomically emit `job.cancel_requested`
   plus `job.canceled`. Job terminality does not claim the Worker has stopped.
3. `RUNNING` or `FINALIZING`: advance the Job fence, terminalize the Attempt as
   `CANCELED`, revoke the Lease, mark the Worker `DRAINING`, decrement running
   counters, transition the Job to `CANCELING`, consume CreditReservation, move
   the exact quote from organization reserved credit to unsettled posted credit,
   create one `CUSTOMER_CANCELLATION` Charge, and emit `job.cancel_requested`,
   `job.canceling`, `charge.posted`, and `invoice.export_requested`.
4. `CANCELING` or `CANCELED`: replay the immutable cancellation decision. A
   legacy terminal row without a decision is returned as terminal and not
   retroactively charged.
5. `SUCCEEDED`: return `ALREADY_SUCCEEDED`; never create cancellation history or
   a second Charge.
6. `FAILED`: return `ALREADY_FAILED`; never create cancellation history or Charge.

The decision captures the pre-cancellation state, old Attempt/Worker/epoch/fence
when present, new Job fence/version, billing outcome, and decision time. Its
identity and billing fields cannot change.

## Stop Completion

`AcknowledgeCancellationStop` receives authenticated Worker identity, the revoked
Lease credentials, and the cancellation id. It succeeds only when all values
match the immutable decision. Replay returns the first receipt. A mismatched or
unrelated Worker receives a stale-authority result without mutation.

For a `CANCELING` Job, acknowledgement transitions it to `CANCELED` and emits the
single `job.canceled` event. For an already-terminal ASSIGNED cancellation, it
only records physical stop. A HEALTHY Worker may return from `DRAINING` to `READY`;
otherwise it remains unavailable. The Charge and CreditReservation do not change.

`ReconcileNextCancellationStop` may make `CANCELING -> CANCELED` only after the
revoked Lease's original `expires_at` has passed in PostgreSQL. This proves the
last Worker-visible authority window has closed. Reconciliation records its own
receipt, emits one terminal event, and leaves the Worker unavailable until normal
health recovery proves it READY.

## Charge And Ledger

`charges(job_id)` is unique. A Charge records organization, Project, Job,
CreditReservation, quoted integer amount/currency, reason, posting time, and the
winning cancellation id. It is inserted only while the matching reservation is
`RESERVED` and the organization credit row is locked. The same transaction:

```text
reserved_minor          -= quoted_amount_minor
unsettled_posted_minor  += quoted_amount_minor
CreditReservation       = CONSUMED
Charge                  = POSTED
```

Charge amount, currency, reason, Job, reservation, posting time, and cancellation
link are immutable. Invoice export retries by `charge_id` and cannot modify the
Charge, Job, or CreditReservation.

## Locking And Rollback

For active work the command locks Worker first, then the Lease write relation,
Lease, Attempt, Job, Project, Worker pool when needed, CreditReservation, and
organization credit in the existing coordinator order. Candidate discovery is
advisory and every predicate is rechecked under row locks. Queue-only cancellation
does not acquire a Worker lock and never waits on a Worker row after locking Job.

Any failure before commit rolls back the decision, fence, state, Attempt, Lease,
Worker, counters, credit, Charge, and every Outbox event. Concurrent cancellation,
Heartbeat, Fail, Job Expiry, acknowledgement, and reconciliation produce one
winner without deadlock or double accounting.

## Events And Compatibility

Events use additive `EventEnvelope` oneof fields and contain stable identifiers,
aggregate version, decision time, billing result, and the old execution authority
needed by an internal Stop consumer. Customer-facing terminal events contain no
Worker identity, Lease token, prompt, Artifact, or other Customer Content.

Migration 00006 is additive. The exact `d0a8c01` N-1 binary must still pass its
auth/request/internal startup verification and database smoke test after Up. The
new `vela_cancel` login is provisioned before the N binary and is ignored by N-1.
Old Outbox publishers forward unknown payload bytes unchanged. Down preserves
immutable Charge/cancellation/stop receipts privately for later Up and refuses any
state it cannot represent safely.

## Test Seams And Evidence

The architecture already fixes these public seams:

1. Project HTTP `POST .../cancel` through credential scope and Project ownership;
2. coordinator `AcknowledgeCancellationStop` after Worker mTLS authentication;
3. internal `ReconcileNextCancellationStop` using PostgreSQL time;
4. existing `Start`, `Heartbeat`, `Fail`, and expiry authority after fencing;
5. PostgreSQL migration, roles, constraints, credit ledger, and Outbox payloads.

Required evidence:

- each pre-Billable state cancels once, releases credit, adjusts exact counters,
  creates no Charge, and replays;
- RUNNING and FINALIZING cancel once, post the exact quote, consume reservation,
  preserve Billable Start, and never create a second Charge;
- cancellation is Project-isolated and scope/credential changes fail closed in
  the transaction;
- active cancellation immediately fences late Start, Heartbeat, Fail, and later
  completion authority;
- stop acknowledgement and expiry reconciliation each terminalize at most once;
- concurrent cancel/cancel, cancel/Heartbeat, cancel/Fail, cancel/Job Expiry, and
  ack/reconcile have one winner without deadlock or partial accounting;
- forced failure at every write rolls back the entire decision;
- request/auth roles cannot read cancellation evidence, Worker identity, Charge
  internals, or call the cancellation function;
- migration up/down/up preserves immutable evidence; generated OpenAPI, sqlc and
  Protobuf outputs, actual `d0a8c01` N-1 startup, lint, unit, integration, and race
  tests pass.

## Completion Boundary

This slice is complete only when generation, lint, and unit tests pass from a clean
commit; PostgreSQL integration and race tests pass; authorization, every source
state, billing/non-billing outcomes, fencing, stop completion, concurrency,
rollback, migration, N/N-1, and Outbox behavior are evidenced; and a two-axis
review finds no unresolved P0-P2 issue.
