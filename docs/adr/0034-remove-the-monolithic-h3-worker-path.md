# Remove The Monolithic H3 Worker Path

Date: 2026-08-29

Status: Accepted and implemented in the schema-v2 repository; production
activation and external evidence remain pending.

## Context

The predecessor path bound a Job Attempt to one machine-level Worker and
modeled H3 as an 8-GPU appliance. Retaining this beside the new
stage graph would create two retry, cancellation, Artifact, scheduler, Worker,
and billing implementations. The product does not require the old path after
the stage architecture is proven.

N/N-1 rollout still requires bounded migration compatibility. That temporary
compatibility must not become a user-selectable fallback or a second product
mode.

## Decision

- The stage graph is the only target H3 execution architecture.
- Same-node H3 execution is a placement produced by the StageScheduler, not a
  monolithic compatibility profile.
- The old `Attempt.worker_id`/machine-level Lease, machine H3 WorkerAssignment,
  and Runner protocol remain only during explicit migration windows.
- New Accepted H3 Jobs stop entering the legacy path at a recorded cutover
  revision. Already Accepted legacy Jobs may drain under their frozen authority.
- Rollback before the point of no return may route only new Jobs back to the
  previous release. It must never translate an in-flight stage graph into a
  legacy Attempt or vice versa.
- Schema contraction and code deletion occur only after durable proof that no
  nonterminal legacy Job, active legacy Lease, staging upload, retained outbox
  event, N-1 writer, recovery backlog, or Launch Receipt dependency remains.
- After contraction, no feature flag, Catalog profile, protocol message, SQL
  path, or deployment manifest may create a machine-level H3 Assignment.

## Removal gates

1. Stage path completes same-node and cross-node H3 conformance.
2. Cancellation, node loss, retry, L2 outage, finalization, and deletion races
   preserve existing business invariants.
3. H3 quality and performance certification covers every active stage/profile
   combination and connector.
4. N/N-1 cutover, drain, rollback-before-contraction, and recovery drills pass.
5. Production observability, capacity, cost, runbooks, and ownership exist.
6. A repository reachability test proves no legacy H3 Assignment can be formed.

## Consequences

- The end state has one execution authority and one failure model.
- Migration work is larger because compatibility is transitional and must be
  retired deliberately.
- Same-node operation remains available without preserving legacy semantics.
- Rollback after schema contraction requires forward repair or database
  restore under the release recovery contract; it cannot resurrect old code
  against incompatible authority.

## Rejected alternatives

- Keep the old path as permanent fallback: rejected because it doubles the
  correctness surface and hides stage-specific failures.
- Rewrite legacy in-flight Jobs into StageRuns: rejected because authority,
  fences, billing boundaries, and intermediate evidence cannot be inferred
  safely.
- Drop the old path in one release: rejected because it violates durable
  backlog, N/N-1, and rollback requirements.

## Evidence boundary

The repository now implements this decision. Migration `00058` is the
irreversible schema-v2 contraction: non-empty upgrades require the unique
release-bound authorization, recheck its current cutover revision and live-zero
inventory under lock, retire graphless profiles and still-valid certifications
while preserving invalidated evidence, permanently prevent their reactivation,
isolate that invariant behind a dedicated NOLOGIN owner, remove the reviewed
legacy schema with `RESTRICT`, and rebuild Stage-only authority. The legacy
Worker/Runner protocol, runtime, scheduler, generated query, deployment, image,
and release surfaces are deleted, and the permanent repository reachability
test passes. Migrations `00059` through `00062` add the subsequent runtime epoch,
multi-member barrier, gang authority, and member identity contracts.

This is repository closure, not a production launch declaration. No real
GPU/DRA execution, production N/N-1 contraction, quality/performance campaign,
or sealed Launch Receipt is supplied by this ADR; Production Gates remain
`0/9 PASS`. Spec 0049 defines the migration boundary and spec 0050 separates
repository completion from production acceptance.
