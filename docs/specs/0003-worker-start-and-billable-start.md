# Worker Start And Billable Start

| Field | Value |
| --- | --- |
| Status | Implemented |
| Architecture baseline | `48efd6c` |
| Coordinator seam | Transactional `Start` service called after Worker mTLS authentication |
| Persistence seam | PostgreSQL Job, Attempt, EXECUTION Lease, CreditReservation and Outbox contract |
| Event seam | Versioned `job.started` Outbox event |

## Goal

Create the third executable Vela slice: allow the Worker holding the current EXECUTION Lease to atomically transition its ASSIGNED Attempt and Job to RUNNING. The committed transition is Billable Start. The Worker must receive `StartGranted` only after that transition and its Outbox event commit, and a lost response must replay the original Attempt start without creating a second transition.

Job state alone is not sufficient billing history. A RUNNING Attempt may fail and return its Job to RETRY_WAIT, but a later Customer Cancellation remains billable because the Job has run before. This slice therefore persists a write-once `jobs.billable_started_at` marker independently from the current Job state and the per-Attempt `attempts.started_at` timestamp.

## Governing Decisions

- ADR 0002: the first committed RUNNING transition is Billable Start and later Customer Cancellation charges the full quote.
- ADR 0003: Job transitions and their externally visible events commit atomically in PostgreSQL.
- ADR 0011: Start does not create a Charge; platform failure still releases the CreditReservation without a Charge.
- ADR 0013: routine drains do not interrupt an already Accepted and Assigned Job, and adjacent control-plane releases replay the same result.
- ADR 0020: Job Expiry is a lifecycle ceiling checked with PostgreSQL time, not a customer deadline.
- ADR 0027: later cancellation uses durable Billable Start history when deciding whether to post the full Charge.

## In Scope

1. A coordinator `Start` operation receiving authenticated Worker identity and opaque Lease credentials.
2. Constant-time comparison of the presented Lease-token digest with the current Worker-owned EXECUTION Lease.
3. Transactional Worker epoch, Attempt, fence, Lease revocation/expiry, Job state/expiry and CreditReservation rechecks.
4. Atomic ASSIGNED to RUNNING transitions for both Attempt and Job.
5. A write-once per-Attempt `started_at` and a write-once first-Job `billable_started_at`.
6. One Job version increment and one `job.started` Outbox event per successful Attempt Start.
7. Exact replay of an already committed RUNNING Attempt, including its original `started_at`.
8. PostgreSQL constraints and triggers preventing either committed start timestamp from being changed or cleared.
9. Integration, concurrency, expiry and rollback tests against PostgreSQL.

## Explicitly Deferred

- Protobuf/gRPC transport and extraction of Worker identity from an mTLS certificate.
- heartbeat, Lease renewal, progress projection and Worker-side monotonic deadline enforcement.
- Attempt failure classification, RetryDecision and compute-budget accounting.
- Customer Cancellation, Charge persistence and cancellation/completion races.
- begin-finalization, Artifact upload, ArtifactSet reconciliation and Visible Completion.

The deferred operations must use `billable_started_at` as the durable customer-cancellation billing boundary. They must not infer prior execution only from the current Job state.

## Start Contract

The trusted transport passes:

```text
AuthenticatedWorker { worker_id }
LeaseCredentials {
  attempt_id
  worker_epoch
  fence
  opaque lease_token
}
```

`worker_id` comes from verified mTLS identity. `worker_epoch`, Attempt, fence and token must identify the same current Worker-owned EXECUTION Lease.

A successful call returns:

```text
StartGranted {
  attempt_id
  job_id
  worker_id
  worker_epoch
  lease_fence
  started_at
}
```

The coordinator returns `Stop` for expected authority or state rejection. Stop reasons are intentionally bounded:

- `INVALID_AUTHORITY`: malformed or mismatched Worker, epoch, Attempt, fence or token; revoked Lease; or non-EXECUTION/non-Worker Lease.
- `LEASE_EXPIRED`: the EXECUTION Lease is no longer valid by PostgreSQL time.
- `JOB_EXPIRED`: Job Expiry has arrived by PostgreSQL time.
- `NOT_STARTABLE`: the Job/Attempt is not ASSIGNED together or its CreditReservation is no longer RESERVED.

Expected Stop decisions are not infrastructure errors. Database, serialization and commit failures return an error and no granted result.

If the same valid Lease calls Start after its Attempt and Job are already RUNNING, the coordinator returns the original `StartGranted`, including the original `started_at`. Replay does not increment the Job version, change either timestamp or add an Outbox event. Replay still requires an unrevoked, unexpired Lease and unexpired Job.

Lifecycle changes used for routine rollout, such as moving a Worker from BUSY to DRAINING after Assignment, do not revoke its Lease and do not prevent Start. Explicit fencing, epoch change, Lease revocation or expiry remains fail closed.

## Atomic Start

The transaction must:

1. lock the authenticated Worker row and require the exact Worker epoch;
2. lock the matching Attempt, Worker-owned EXECUTION Lease, Job and CreditReservation;
3. hash the presented opaque token and compare it in constant time with the stored digest;
4. require matching Worker, epoch and fence, an unrevoked Lease, current Job fence and RESERVED CreditReservation;
5. read PostgreSQL `clock_timestamp()` only after acquiring the rows and require Lease expiry and Job Expiry in the future;
6. replay an already committed RUNNING Attempt without mutation;
7. otherwise require both Attempt and Job to be ASSIGNED and the Attempt start to be unset;
8. transition the Attempt to RUNNING and set its `started_at`;
9. transition the Job to RUNNING, increment its version and set `billable_started_at` only when it is null;
10. leave the CreditReservation RESERVED and create no Charge;
11. write one versioned `job.started` Outbox event for the Attempt start;
12. re-read PostgreSQL time after the Outbox write and require both expiry bounds still in the future;
13. commit before returning `StartGranted`.

Any error or expiry before commit leaves all thirteen effects absent.

## Database Invariants

- `attempts.started_at` is null while the Attempt is ASSIGNED and is set exactly once by ASSIGNED to RUNNING.
- `jobs.billable_started_at` is null before the first RUNNING transition and, once set, cannot be changed or cleared.
- A retry receives a new per-Attempt `started_at`; it does not reset or overwrite the Job's first Billable Start.
- Start never consumes or releases CreditReservation and does not update credit-account amounts.
- Job and Attempt identity, Worker epoch and fence remain immutable.
- Customer request roles cannot read or mutate Attempt/Lease authority and cannot update Billable Start.
- Each successful Attempt Start increments the Job aggregate version once and writes one matching `job.started` event.

## Test Seams And Evidence

The pre-agreed public seams are:

1. the coordinator `Start` contract after verified Worker identity;
2. PostgreSQL migration, transaction, timestamp and permission results;
3. the durable `job.started` Outbox event.

Required evidence:

- concurrent Start calls return the same granted result and create one transition/event;
- replay, including after coordinator restart, preserves the original Attempt start;
- wrong Worker, token, epoch, Attempt or fence, revoked Lease and FINALIZATION Lease return Stop without side effects;
- stale or terminal Job/Attempt and non-RESERVED CreditReservation cannot start;
- Lease or Job expiry discovered after a row-lock wait causes no transition;
- Lease or Job expiry during the Outbox write rolls back Attempt, Job, Billable Start and event together;
- an ordinary Outbox insert failure also rolls back every Start effect;
- routine Worker drain does not interrupt the valid assigned Job;
- a later retry preserves the first Job Billable Start while recording a new Attempt start;
- Start leaves CreditReservation and reserved credit unchanged;
- request-role SQL cannot forge either start timestamp;
- migration up/down/up and generated sqlc/Protobuf sources are reproducible.

## Completion Boundary

This slice is complete only when repository generation, lint and unit tests pass from a clean commit, PostgreSQL integration and race tests pass, expiry and rollback windows are evidenced, and a two-axis review finds no unresolved P0-P2 issue.
