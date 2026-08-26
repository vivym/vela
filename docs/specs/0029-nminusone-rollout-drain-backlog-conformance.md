# N/N-1 rollout, drain, rollback, and retained-backlog conformance

Date: 2026-08-27

Status: Repository conformance implemented at `21e0781` (initial behavior at
`7cc32b3`, fixed-point P0-P2 review closure at `21e0781`).

This slice adds one repository-level conformance exercise for the adjacent
control and Worker release boundary required by ADR 0013 and Acceptance
Scenario 30. It does not deploy Kubernetes workloads, run a real H3 Job, or
create a Production Gate Launch Receipt.

## Scope

The current release is the repository revision containing migration 00027 and
the authorized debug-dump lifecycle. Its exact adjacent N-1 fixed point is
commit `31991452e60c4254b3b67f72a98ee73e56f7915b`. Tests must build the N-1
control and probes from that commit; compiling current source with compatibility
flags is not N-1 evidence.

The conformance exercise covers:

1. schema 27 expand compatibility with exact N-1 control startup and role
   preflight;
2. exact N-1 Admission creating an Accepted Job on the expanded schema;
3. the exact N-1 Outbox publisher forwarding the retained raw `job.ready`
   payload and message identity to the release-owned JetStream stream;
4. the current JetStream consumer recording its Inbox receipt and invoking the
   current Scheduler public cycle, which creates the Assignment;
5. an exact N-1 Worker transport client acquiring and starting that current
   Assignment over the production mTLS gRPC seam without a debug-dump
   authorization;
6. a current Fleet drain changing the Worker to `DRAINING` without revoking the
   active Lease, while the exact N-1 Worker continues to heartbeat;
7. normal Worker lifecycle termination ending the Lease before Fleet marks the
   drain complete;
8. a current Admission write followed by exact N-1 control startup and exact
   N-1 Scheduler `RunOnce`, representing binary/configuration rollback without
   rolling back schema 27; and
9. current and exact N-1 Admission and Scheduler writers failing closed with
   SQLSTATE `55000` while the CloudNativePG cluster lacks synchronous quorum,
   with no new Job, dispatch intent, Attempt, or Lease authority after recovery.

The test log records the N and N-1 revisions, event, Job, dispatch intent,
Attempt, Lease, Worker, drain operation, Inbox receipt, payload digest, and
final authority counts. A passing test must not infer identities from row
counts alone.

## Public seams

The test uses only release behavior visible through established boundaries:

- `admission.Service.Submit` and the production HTTP Admission handler;
- `outbox.Publisher` with the release-owned JetStream stream contract;
- `inbox.JetStreamConsumer` and the durable Scheduler Inbox receipt;
- `scheduler.Service.RunCycle` / exact N-1 `scheduler.Service.RunOnce`;
- the production Worker mTLS gRPC transport implemented by
  `workertransport.Client` and `workertransport.Server`;
- `fleet.Service.RequestDrain` and `fleet.Service.ReconcileDrain`; and
- the database synchronous-quorum guard exercised through application roles.

Direct fixture writes may seed immutable catalog/capacity prerequisites and may
read authority for assertions. They must not manufacture Assignment, Lease,
Inbox, drain, failure, or rollback outcomes.

## Compatibility rules

- Migration 00027 remains additive. The N-1 binary is not granted either new
  debug-dump role and must ignore the new optional Worker protocol fields.
- The old event payload is compared byte-for-byte before current consumption;
  decoding a reconstructed envelope is insufficient retained-backlog evidence.
- A drain never fences, revokes, or terminates the active Job. The N-1 Worker
  must perform another authenticated operation after the drain begins.
- Fleet may complete the drain only after the Worker lifecycle operation has
  ended the Lease.
- Rollback means restoring the exact N-1 binary/configuration while retaining
  schema 27. It does not run a migration Down.
- Any N-1 probe that encounters a guarded PostgreSQL write returns structured
  SQLSTATE and empty authority identities instead of treating a connection
  timeout as proof of fail-closed behavior.

## Verification

Required repository checks are:

- the focused mixed-version integration test;
- the focused CloudNativePG failover/no-quorum test;
- the complete integration suite;
- `make generate`, `make test`, `make lint`, `make test-cross`, and
  `make validate-deployment`; and
- a fixed-commit P0-P2 code review before the evidence documentation commit.

The complete integration suite, `make generate`, `make test`, `make lint`,
`make test-cross`, and `make validate-deployment` passed for the initial
implementation at `7cc32b3`. After review fixes, the focused mixed-version test
passed in 16.425 seconds, `make test` passed with 44 Runner tests, `make lint`
reported zero issues, and the CNPG build-tag compile passed. The pinned CNPG
exercise then passed in 113.725 seconds with RPO 0, a measured 51.010-second
single-node failover, SQLSTATE `55000` from current and exact N-1 Admission and
Scheduler writers, and unchanged authority after quorum recovery. Final
Standards and Spec reviews found no remaining P0-P2 issue.

All required container images should use matching local platform copies when
present. A registry proxy may be used only when an image is actually missing or
a pull fails. Go build cache may be cleared between large verification phases;
the module cache remains retained unless disk pressure requires a separate
decision.

## Evidence boundary

This slice advances Scenario 30 from partial implementation evidence to direct
repository conformance evidence. ADR 0013 remains `Partial`, because a
real Kubernetes rollout, real long-running H3 execution, production Worker
drain, release rollback, and retained production backlog receipt are still
external work. Production Gates remain `0/9 PASS`.
