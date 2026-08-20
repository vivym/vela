# Worker Heartbeat, Lease Renewal, And Attempt Progress

Status: Implemented

Date: 2026-08-21

Predecessors:

- `docs/specs/0001-admission-control-plane-foundation.md`
- `docs/specs/0002-worker-assignment-and-execution-lease.md`
- `docs/specs/0003-worker-start-and-billable-start.md`

## Goal

Keep a current Worker-owned EXECUTION Lease alive only while the same authenticated Worker still holds its exact authority, persist replay-safe Attempt-scoped observations, and project only backend-neutral, non-promissory progress into the customer Job view.

## Governing Decisions

- ADR 0013: routine drains and adjacent control-plane releases do not interrupt Accepted Jobs.
- ADR 0019: Phase Progress belongs to the current Attempt and phase, may reset after Retry, becomes unknown when stale, and never represents Visible Completion.
- ADR 0020: immutable Job Expiry is the final renewal ceiling and is not a Hard Deadline.
- ADR 0021: heartbeat does not create an Attempt or enlarge the immutable Retry Budget.

## In Scope

1. A coordinator `Heartbeat` operation receiving authenticated Worker identity, opaque Lease credentials, a positive Attempt-scoped sequence, and progress/health observations.
2. Constant-time token-digest comparison against the current Worker-owned EXECUTION Lease.
3. PostgreSQL-time renewal of an unexpired Lease, capped by immutable Job Expiry.
4. Durable exact replay for the latest committed heartbeat sequence without a second renewal or progress write.
5. A current Attempt progress record containing backend observations plus a backend-neutral Execution Phase projection.
6. Customer Job view fields for phase, optional non-terminal Phase Progress, Attempts started, next Retry time, Dynamic ETA, and progress update time.
7. PostgreSQL-time staleness of Phase Progress and Dynamic ETA.
8. `lease_valid_for` on Assignment and Heartbeat responses so a Worker adapter can establish a conservative local monotonic deadline.
9. Preservation of the original opaque Lease token across renewal and exact Assignment replay after renewal.
10. A fail-closed, database-serialized protocol gate for the incompatible v1 fixed-expiry to renewable-Lease switch and rollback.
11. Integration, concurrency, expiry, replay, RLS, rollback, migration, and generated-contract tests.

## Explicitly Deferred

- Protobuf/gRPC streaming transport, extraction of Worker identity from mTLS, and the Worker Agent monotonic-timer implementation.
- Lease-expiry reconciliation, Attempt failure classification, compute-time accounting, RetryDecision, and terminal Job transitions.
- GPU-health policy, Worker reachability transitions, scratch/storage circuit decisions, and automated remediation.
- begin-finalization, Artifact upload, ArtifactSet reconciliation, and Visible Completion.
- hierarchical scheduling, Dynamic ETA calibration, and customer SLO evaluation.

The deferred Worker adapter must record its local monotonic time before each request and use `request_started_monotonic + lease_valid_for` as its fail-closed deadline. It must never compare the server `expires_at` with Worker wall time.

## Heartbeat Contract

The trusted transport passes:

```text
AuthenticatedWorker { worker_id }
LeaseCredentials {
  attempt_id
  worker_epoch
  fence
  opaque lease_token
}
HeartbeatObservation {
  sequence
  backend_stage
  optional backend_stage_progress
  optional estimated_remaining_seconds
  gpu_health_summary
  local_artifact_state
  scratch_free_bytes
  artifact_store_reachable
}
```

`worker_id` comes from verified mTLS identity. Credentials must identify the same current Worker-owned EXECUTION Lease. `sequence` is positive and strictly increases within an Attempt. The first received sequence need not be one because an earlier request may be lost before reaching the coordinator.

`backend_stage` is an internal diagnostic label of 1 to 100 printable characters. Both JSON summaries must be valid objects no larger than 16 KiB. `backend_stage_progress`, when present, is in `[0, 1)`; the value `1` is reserved for Visible Completion and is never accepted from heartbeat. `estimated_remaining_seconds`, when present, is an integer in `[0, 9223372036]`, so its server-derived timestamp is representable without duration overflow. `scratch_free_bytes` is non-negative.

A successful call returns:

```text
Continue {
  attempt_id
  job_id
  worker_id
  worker_epoch
  lease_fence
  heartbeat_sequence
  execution_phase
  progress_updated_at
  lease_expires_at
  lease_valid_for
}
```

`lease_valid_for` is calculated from a final PostgreSQL `clock_timestamp()` read to the persisted `lease_expires_at`. Network and transaction time therefore reduce the Worker's local authority window instead of extending it.

Expected Stop reasons are bounded:

- `INVALID_AUTHORITY`: malformed or mismatched Worker, epoch, Attempt, fence, or token; revoked Lease; or non-EXECUTION/non-Worker Lease.
- `LEASE_EXPIRED`: the presented EXECUTION Lease has already expired by PostgreSQL time.
- `JOB_EXPIRED`: immutable Job Expiry has arrived by PostgreSQL time.
- `NOT_HEARTBEATABLE`: Job and Attempt are not both ASSIGNED or both RUNNING, or CreditReservation is not RESERVED.
- `STALE_HEARTBEAT`: sequence is lower than the latest committed sequence, or repeats that sequence with different canonical content.
- `INVALID_PROGRESS`: observation fields violate the bounded public contract.
- `PROTOCOL_MIGRATION_REQUIRED`: authority is valid, but the database-wide renewable-Lease protocol gate has not been switched on.

Database, serialization, or commit failures return an error and no Continue result.

## Replay And Renewal

The canonical observation hash includes every observation field after canonical JSON normalization.

- A sequence greater than the latest committed sequence renews the Lease and replaces the Attempt's current observation.
- The same sequence and canonical hash replays the previously committed durable result. Replay does not change Lease expiry, progress, Worker heartbeat time, Job version, or any event. Its `lease_valid_for` is freshly reduced by PostgreSQL time.
- The same sequence with different content and every lower sequence return `STALE_HEARTBEAT` without mutation.
- Concurrent duplicate calls serialize on the Lease row and produce one renewal/progress write.

Renewal uses:

```text
lease_expires_at = min(
  job_expires_at,
  max(previous_lease_expires_at, postgres_now + configured_lease_ttl)
)
```

An adjacent release with a shorter configured TTL therefore cannot shorten already granted authority, while no renewal can cross Job Expiry. The transaction re-reads PostgreSQL time after all writes and rolls back if either bound has arrived before commit.

Lease renewal does not rotate the opaque token. Because the v1 token was derived from the Lease's initially granted expiry, the database preserves an immutable `token_claim_expires_at` separately from mutable `expires_at`. Existing rows are backfilled, and the migration fills this value for N-1 writers that omit the new column. Assignment replay in the new binary reconstructs the token from `token_claim_expires_at` but returns the current renewed `expires_at`.

That new-binary replay rule does not make an actual N-1 binary compatible with a renewed Lease: N-1 reconstructs the token from mutable `expires_at`. Renewal is therefore an explicit migration operation rather than an ordinary mixed-version behavior:

1. expand adds `token_claim_expires_at`, `renewal_protocol_version`, safe request-role Job projection views, and the database-wide gate in the disabled state; N-1 inserts receive protocol version 1, the new writer explicitly inserts version 2, and the exact N-1 request-role privilege set remains valid;
2. while the gate is disabled, valid Heartbeat returns `PROTOCOL_MIGRATION_REQUIRED`, and PostgreSQL rejects every Lease expiry extension while still allowing fail-closed shortening or revocation;
3. rollout drains all active EXECUTION Leases and proves in the transition receipt that N-1 control replicas and N-1 Worker references are zero;
4. the database-owner transition function locks Lease writes, rechecks that no active EXECUTION Lease exists, removes the legacy request-role table privilege, records the receipt, and atomically enables the gate for every new control-plane replica;
5. after switch, PostgreSQL rejects a version-1 EXECUTION Lease insert, closing the race with a lingering N-1 Worker writer without imposing this protocol on separately owned FINALIZATION Leases;
6. rollback to N-1 first drains all active EXECUTION Leases, then uses the same serialized transition to disable renewal and restore the N-1 request-role privilege before restoring the old binary. A direct N-1 rollback after any renewal is not supported.

PostgreSQL proves serialization, zero active Lease authority at transition, and rejection of legacy writes. The external rollout receipt must prove replica and Worker-version inventory; the database flag alone is not that evidence.

## Atomic Heartbeat

The transaction must:

1. lock the authenticated Worker and require the exact Worker epoch;
2. acquire the EXECUTION Lease write relation lock used by every renewal-protocol participant;
3. lock the matching Attempt, Worker-owned EXECUTION Lease, Job, and CreditReservation;
4. hash the presented opaque token and compare it in constant time with the stored digest;
5. require matching Worker, epoch, fence, current Job fence, and an unrevoked Lease;
6. read PostgreSQL `clock_timestamp()` after locking and require current Lease expiry and Job Expiry in the future;
7. require Job and Attempt to be ASSIGNED together or RUNNING together and CreditReservation to be RESERVED;
8. lock and require the database-wide renewable-Lease protocol gate, returning `PROTOCOL_MIGRATION_REQUIRED` while it is disabled;
9. validate and canonicalize the observation only after authority and protocol eligibility are established;
10. lock the current Attempt progress row, if any, and apply the sequence/hash replay rules;
11. map ASSIGNED to PREPARING and RUNNING to GENERATING without exposing `backend_stage`;
12. compute the capped, non-shortening renewed Lease expiry;
13. atomically renew the Lease, upsert the complete Attempt progress record, and record the Worker's server-observed heartbeat time;
14. leave Job state/version, Attempt state, RetryRuntimeState, CreditReservation, credit amounts, and Outbox unchanged;
15. re-read PostgreSQL time, require the renewed Lease and Job Expiry still in the future, and calculate positive `lease_valid_for`;
16. commit before returning Continue.

Any error or expiry before commit leaves all sixteen effects absent.

Routine Worker lifecycle changes such as BUSY to DRAINING do not invalidate a committed Assignment and do not block Heartbeat. Explicit epoch change, fencing, revocation, Lease expiry, Job Expiry, or state transition remains fail closed.

## Attempt Progress And Job View

The current progress record is keyed by Attempt and carries immutable Organization, Project, Job, Worker, epoch, and fence identity. It records:

- heartbeat sequence and canonical request hash;
- internal `backend_stage` and diagnostic summaries;
- backend-neutral `execution_phase` and optional `phase_progress`;
- optional `estimated_remaining_seconds` and server-derived `estimated_finish_at`;
- scratch/storage observations;
- `progress_updated_at` and `progress_valid_until`, where validity ends no later than the renewed Lease.

The Project Job view exposes only:

```text
state
optional phase: QUEUED | PREPARING | GENERATING | FINALIZING | RETRY_WAIT
optional phase_progress
attempts_started
optional next_retry_at
optional estimated_finish_at
optional progress_updated_at
job_expires_at
```

Projection rules:

- QUEUED, ASSIGNED, RUNNING, FINALIZING, and RETRY_WAIT map to the corresponding backend-neutral phase; terminal states have no current phase.
- Only the progress row whose fence equals the Job's current fence is eligible. A replacement Attempt therefore starts with unknown progress and may reset from its predecessor.
- `phase_progress` and `estimated_finish_at` become null when `progress_valid_until <= clock_timestamp()`. `progress_updated_at` may remain visible to explain staleness.
- `next_retry_at` comes from RetryRuntimeState and is not an ETA.
- `estimated_finish_at` is an observation derived from server receive time, not an SLO, Hard Deadline, or extension of Job Expiry.
- Raw backend stage, GPU health, local Artifact details, Worker identity, Lease token/digest, epoch, and fence are never exposed through the customer Job view or customer database role.

## Database Invariants

- `token_claim_expires_at` is present for every Lease, initially equals its first granted expiry, and cannot change.
- Lease renewal protocol version is immutable; version 1 can never renew, and version 1 inserts are rejected after the database-wide switch.
- Enabling or disabling renewal is serialized against Lease writes and requires zero active EXECUTION Leases.
- A renewed EXECUTION Lease keeps its Attempt, owner, token digest, signing-key id, issue time, and token claim expiry unchanged.
- Attempt progress identity is immutable; sequence and update time strictly increase on replacement.
- A progress row's request hash is 32 bytes, both diagnostic summaries are JSON objects, numeric values are bounded, and validity ends after its update time.
- Customer request roles can obtain Attempt progress only through security-barrier Job projection views for their authenticated Organization and Project; they cannot directly select Attempt progress identity, fence, validity deadline, or raw Worker diagnostics, and cannot mutate progress.
- Heartbeat never increments Job aggregate version and never writes an Outbox event.

## Test Seams And Evidence

The pre-agreed public seams are:

1. the coordinator `Heartbeat` contract after verified Worker identity;
2. PostgreSQL migration, authority locking, renewal, progress, and rollback behavior;
3. authenticated Project `GET Job` projection and RLS visibility;
4. Assignment and Heartbeat response duration used by the future Worker monotonic-deadline adapter.

Required evidence:

- concurrent duplicate heartbeat creates one renewal/progress update and returns the same durable expiry and observation time;
- replay after coordinator restart preserves expiry and progress while returning a smaller current `lease_valid_for`;
- a lower sequence or equal sequence with different content is rejected without mutation, while a higher sequence replaces progress;
- wrong Worker, epoch, Attempt, fence, token, Lease phase/owner, revoked Lease, or stale Job fence is rejected;
- ASSIGNED/PREPARING and RUNNING/GENERATING heartbeat succeed; stale or terminal state and non-RESERVED credit reject without mutation;
- Lease or Job expiry discovered after a row-lock wait or during a delayed progress write causes complete rollback;
- renewal never crosses Job Expiry and never shortens an unexpired Lease after an adjacent TTL change;
- routine DRAINING does not block heartbeat;
- Assignment replay after renewal returns the original token and renewed expiry;
- Job GET exposes current neutral progress, Attempts started and Dynamic ETA, hides raw diagnostics, makes progress unknown by PostgreSQL staleness, and resets on a replacement Attempt;
- Assignment and Heartbeat return positive `lease_valid_for` no greater than the server-side remaining Lease interval;
- the actual `450dd5c` N-1 control binary starts in expand mode, its actual Assignment writer/replay produces a version-1 Lease with unchanged expiry and token, it is rejected after switch, and starts again after rollback;
- compatibility mode leaves v1 expiry and replay token unchanged, gate transition rejects active authority, enabled mode rejects a legacy writer, and disabled mode rejects direct expiry extension;
- request-role SQL cannot read raw progress fields or mutate progress;
- migration up/down/up and generated OpenAPI/sqlc sources are reproducible.

## Completion Boundary

This slice is complete only when repository generation, lint, and unit tests pass from a clean commit; PostgreSQL integration and race tests pass; concurrency, expiry, rollback, N/N-1 stage-gate, replay, RLS, and Job-view behavior are evidenced; and a two-axis review finds no unresolved P0-P2 issue.
