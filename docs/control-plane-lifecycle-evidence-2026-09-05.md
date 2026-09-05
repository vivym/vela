# Control-plane lifecycle evidence, 2026-09-05

This audit covers local PostgreSQL Stage-only control-plane behavior through
schema 84. It does not advance a Production Gate or establish production
retention, recovery, capacity, or latency objectives.

## Storage classification

| State | Lifecycle and evidence boundary |
| --- | --- |
| `stage_ready_queue_entries` | Current projection of READY work. Existing transition triggers remove obsolete entries; it is not an execution audit log. |
| `stage_capacity_pool_counters` and scheduler deficits | Mutable counters keyed by pool and fairness dimensions. Repeated polling does not append one row per poll. The number of configured pools and fairness identities can still grow. |
| `capacity_observations` | Expiring capacity leases. Same-sequence Stage heartbeats already update one row. Schema 83 retires expired superseded leases in batches while retaining every live lease and the highest sequence for each Worker/epoch/source. |
| `stage_worker_acquire_intents` / `stage_worker_acquire_results` | Immutable durable command identity and exact response replay. Schema 78 avoids records for ordinary idle hints; schema 84 also avoids them when all candidates are filtered. Existing durable responses and the compatibility path without the hint remain durable. |
| `stage_scheduler_snapshot_traces`, `stage_decision_evidence`, claims and shadow replay receipts | Durable scheduling and replay evidence. `valid_until` limits eligibility; it is not permission to delete audit data. Explicit snapshot capture and real assignment continue to persist evidence. |
| StageAttempts, StageLeases, signed authority renewals, execution/registration receipts, Usage and Charge | Execution, fencing, replay or accounting history. This change does not delete these records. Business and audit history still require an explicit retention/archive contract before deletion. |

## Capacity observation repair

Before schema 83, 64 real capacity changes through
`PostgresWorkerEvidenceBackend.ReportCapacityObservation` increased one Worker's
rows from 1 to 65. After all 64 leases expired, renewal of the highest sequence
still left 65 rows. Evidence:
`/tmp/vela-stage-capacity-lifecycle-red.log` (expected regression failure).

Schema 83 adds a private Fleet-owned maintenance helper to the existing Node
Agent and Stage Worker observation entrypoints. Both entrypoints already hold
the Worker row lock. Cleanup uses the database clock and removes at most 256
expired rows per successful report. A row must have a higher sequence in the
same Worker epoch and source sequence range. The highest Stage and Fleet
sequences survive expiration, preserving stale-report rejection and the Stage
Worker's takeover from Node Agent capacity reporting. Rejected reports roll
back their maintenance work.

The repaired API regression retains all 65 rows while live, then converges to
2 after expiry and renewal. Another 64 steady renewals add zero rows. The batch
test deletes 256, 256, 2 and 0 rows in successive transactions while preserving
live rows and both epoch/source watermarks. Lower sequences and backward
observation times remain rejected. Canonical function OIDs, owners and ACLs
survive repeated Down/Up; login roles cannot call maintenance directly.

Cleanup is attached to successful reports. A Worker that stops reporting does
not receive automatic background cleanup, and retained epoch watermarks are
not deleted. Thus this bounds repeated transient history for reporting Workers;
it does not impose a global byte quota or delete retired Worker history.

## Blocked acquisition repair

With ready work blocked by a Project's running limit, 64 real public
`AcquireStage` calls previously appended 64 intents, 64 results and 64 snapshot
traces, with zero decisions or physical attempts. This is 192 rows without
execution progress. The before measurement is retained in
`/tmp/vela-stage-capacity-lifecycle-final.log`.

Schema 84 extracts the existing snapshot candidate policy into one private,
read-only helper. Durable snapshot capture reuses it and retains schema 80's
pool-before-counter locks. The advisory path checks only whether any candidate
has no filter reasons. It does not choose a winner, alter fairness, reserve
capacity, persist a snapshot or bypass claim/CAS authority. Active allocation
and durable intent paths continue through the established durable behavior.

After the repair, the same 64 blocked calls append zero rows. Releasing the
Project running limit produces an Assignment that replays exactly. A controlled
race pauses after the blocked candidate snapshot is read, commits newly
available Project capacity, then resumes the probe: the current call returns
one advisory NoWork, and the next call with the same command obtains work.

Compatibility regressions preserve schema 83 durable negative responses after
upgrade, old-client intent/new-client completion, exact signed wire responses
through repeated 83/84 Down/Up, and the existing 77/78 mixed-client boundary.
The original capture definition is restored exactly on Down. Canonical
function identity/ACL/volatility is preserved; the new reader has no direct
login-role execution grant.

## Verification

- Capacity lifecycle and existing Fleet/session tests: PASS, including expired
  reports, takeover, stale session fencing, registration/assignment lock order,
  per-epoch/source watermarks and migration boundaries.
  `/tmp/vela-stage-capacity-lifecycle-final.log`.
- Final combined schema-84 scheduler, assignment, lifecycle, idle/blocked probe,
  rollback, shadow replay, claim crash recovery, lock-order and quorum
  replay/renewal/idle checks: PASS in 117.291 seconds.
  `/tmp/vela-stage-lifecycle-scheduler-final.log`.
- All three new-assignment entrypoints reject missing synchronous quorum:
  public Worker Acquire, Scheduler Acquire and Coordinator ASSIGN. PASS in
  8.785 seconds on schema 84.
  `/tmp/vela-stage-lifecycle-assignment-quorum-final.log`.
- `git diff --check`: PASS after the implementation and focused verification.

The actual CNPG failover and Barman marker PITR evidence is recorded separately.
These lifecycle tests use local PostgreSQL integration fixtures and do not
substitute for those cluster tests or for production audit retention policy.
