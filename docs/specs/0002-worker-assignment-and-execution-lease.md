# Worker Assignment And Execution Lease

| Field | Value |
| --- | --- |
| Status | Implemented |
| Architecture baseline | `48efd6c` |
| Coordinator seam | Transactional `Acquire` service called after worker mTLS authentication |
| Persistence seam | PostgreSQL Worker, Attempt, Lease, counter and Outbox contract |
| Event seam | Versioned `job.assigned` Outbox event |

## Goal

Create the second executable Vela slice: turn one scheduler-selected, Accepted Job into one durable Attempt and one fenced EXECUTION Lease on a READY, HEALTHY Worker. The operation must be atomic, retry-safe after a lost response, and unable to create duplicate execution authority for either a Job or an H3 Worker.

This slice owns Assignment authority only. It accepts a candidate selected by the Scheduler and rechecks every correctness condition inside PostgreSQL. It must not embed a temporary FIFO or scoring policy that a later hierarchical Scheduler would need to replace.

## Governing Decisions

- ADR 0003: only the current fenced Attempt may eventually form Visible Completion.
- ADR 0007: Project-owned execution records preserve Organization Isolation.
- ADR 0010: Accepted Job and pool counters remain bounded through state transitions.
- ADR 0013: replay across N/N-1 control-plane instances must preserve one Assignment.
- ADR 0021: every new Attempt consumes the immutable Retry Budget.
- ADR 0022: Scheduler policy selects the candidate; the coordinator enforces it transactionally.
- ADR 0024: H3 capacity is work-conserving and a BUSY Worker receives no pre-assignment.
- ADR 0028: later replacement Attempts must receive a strictly greater fence.

## In Scope

1. Worker lifecycle and reachability records with a monotonic Worker epoch.
2. Immutable Attempt identity, number, selected ExecutionProfileRevision and fence.
3. Auditable Lease rows with phase, owner, signing-key id, token digest and fixed expiry.
4. A coordinator `Acquire` operation that receives authenticated Worker identity plus an optional scheduler-owned candidate.
5. Exact replay for the same Worker epoch before evaluating a new candidate.
6. Transactional recheck of Worker, Job version/state/expiry, RESERVED CreditReservation, Project running capacity and ACTIVE ProfileCertification.
7. Atomic Job/Worker state transition, counter movement, RetryRuntimeState increment, Attempt, Lease and `job.assigned` Outbox creation.
8. Database uniqueness constraints that reject two active Attempts for one Job or Worker.
9. Integration and concurrency tests against PostgreSQL.

## Explicitly Deferred

- Hierarchical fairness, aging, Protected Lane, retry lane and Worker scoring.
- Worker gRPC transport and extraction of Worker identity from an mTLS certificate.
- `start()`, heartbeat renewal, progress projection and monotonic-clock enforcement.
- Attempt failure, Lease expiry, retry decisions, Job Expiry and cancellation.
- FINALIZATION Lease takeover, Artifact upload and Visible Completion.

The deferred work must call this coordinator contract or extend its state machine; it must not create a second Assignment authority.

## Acquire Contract

The trusted transport passes:

```text
AuthenticatedWorker { worker_id }
worker_epoch
optional AssignmentCandidate {
  job_id
  expected_job_version
  execution_profile_revision_id
}
```

`worker_id` is derived from verified mTLS identity and is never accepted from an unauthenticated request field. `worker_epoch` is supplied by the Worker and must equal the registered epoch.

The coordinator first locks the Worker and looks for an active Assignment owned by the same Worker epoch. If one exists, it returns the original Assignment, including the original `attempt_id`, Lease token, fence and `expires_at`; it does not evaluate the candidate, create an Attempt, increment counters or renew the Lease.

If there is no active Assignment, a candidate is required. The coordinator rejects rather than mutates when any recheck fails. A successful new Assignment returns:

```text
attempt_id
job_id
worker_id
worker_epoch
execution_profile_revision_id
attempt_number
lease_token
lease_fence
lease_expires_at
```

The Lease token is an unforgeable HMAC-derived opaque secret. PostgreSQL stores only its digest and the signing-key id. The configured keyring must retain every key referenced by an active Lease so any control-plane instance can reconstruct the exact replay token without storing plaintext credentials.

## Atomic Assignment

The transaction must:

1. lock and authenticate the Worker row by `worker_id` and exact epoch;
2. replay an existing active Assignment for the same Worker epoch before inspecting a candidate;
3. require Worker lifecycle `READY` and reachability `HEALTHY` for a new Assignment;
4. lock the candidate Job and require the expected version, `QUEUED` or due `RETRY_WAIT`, and `job_expires_at` in the future;
5. require the Job's CreditReservation to remain `RESERVED`;
6. lock the Project counters and require running capacity;
7. validate that the selected ACTIVE ExecutionProfileRevision belongs to the Worker pool and has an ACTIVE, non-invalidated ProfileCertification for the Job's fixed Model, GenerationPresetRevision and OutputSpec;
8. lock RetryRuntimeState and require `attempts_started < execution_max_attempts` and remaining compute budget;
9. allocate `attempt_number = attempts_started + 1` and `fence = current_fence + 1`;
10. create one ASSIGNED Attempt and one EXECUTION Lease with a fixed expiry;
11. transition Job to `ASSIGNED`, increment its version and fence, and transition Worker to `BUSY`;
12. decrement Project and pool queued counters, increment Project running count, and increment RetryRuntimeState attempts started;
13. write the versioned `job.assigned` Outbox event;
14. commit before returning the Assignment.

Any error leaves all fourteen effects absent.

## Database Invariants

- At most one non-terminal Attempt exists for a Job.
- At most one non-terminal Attempt exists for an H3 Worker.
- At most one unrevoked Lease exists for an Attempt and Worker.
- Attempt numbers and fences are unique per Job and strictly increase while the Job row is locked.
- Attempt Organization and Project must match its Job through a composite foreign key.
- Lease Worker, epoch and fence must match its Attempt through a composite foreign key.
- A Lease expiry is later than issue time and is not changed by `Acquire` replay.
- Project and pool queued counters cannot underflow; Project running count cannot exceed its limit.
- Worker lifecycle and reachability are independent dimensions.

## Test Seams And Evidence

The pre-agreed public seams are:

1. the coordinator `Acquire` contract after verified Worker identity;
2. PostgreSQL migration, transaction, constraints and counter results;
3. the durable `job.assigned` Outbox event.

Required evidence:

- concurrent calls for the same Worker epoch return byte-for-byte equivalent Assignment authority and create one Attempt/Lease/event;
- replay with no candidate returns the original Assignment without extending expiry;
- a different or stale Worker epoch cannot observe or mutate the Assignment;
- BUSY, non-HEALTHY or mismatched-pool Workers cannot claim a new Job;
- stale Job version, expired Job, non-RESERVED credit, exhausted Retry Budget or invalidated ProfileCertification creates no Attempt;
- concurrent Workers cannot both claim one Job, and one Worker cannot hold two active Jobs;
- counter movement and state/version/fence changes are atomic with Attempt and Lease creation;
- migration up/down/up and generated sqlc/Protobuf sources are reproducible.

## Completion Boundary

This slice is complete only when all repository verification commands pass from a clean commit, PostgreSQL integration and race tests pass, exact replay and negative constraints are evidenced, and a two-axis review finds no unresolved P0-P2 issue.
