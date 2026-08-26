# JetStream Quorum And Consumer Crash Recovery

Date: 2026-08-26

Status: Implementation target. This slice closes the repository-verifiable part
of acceptance scenario 23. It does not prove a deployed Control/Storage cluster,
network partition behavior on production infrastructure, or a Production Gate.

## Goal

Bind every Outbox publish to one release-owned, three-replica JetStream stream,
and prove the two crash windows surrounding the Broker and consumer commit
boundaries. PostgreSQL remains the only business authority. JetStream may reduce
wakeup latency and may redeliver, but it cannot create a second aggregate
transition or cause an unacknowledged Outbox row to be lost.

## Governing Decisions

- Architecture sections 8.2 and 8.3 require a quorum-committed `PubAck`, a stable
  `Nats-Msg-Id`, durable consumers, explicit ack, and consumer ack only after the
  local PostgreSQL transaction commits.
- ADR 0012 treats JetStream as rebuildable delivery state and restores it before
  Outbox replay and reconciliation.
- ADR 0013 keeps the protobuf event envelope and retained Outbox backlog
  compatible across adjacent releases.
- ADR 0025 fixes the launch topology at three PVC-backed JetStream replicas on
  distinct Control/Storage Nodes.
- ADR 0029 requires real-cluster fault-injection and recovery receipts before a
  Production Gate can pass.

## Public Test Seams

The architecture already fixes the following seams; tests must observe behavior
through them rather than through private implementation hooks:

1. `outbox.Publisher.PublishBatch` and `outbox.Broker.Publish` at the
   Broker-acceptance/PostgreSQL-marker boundary.
2. `inbox.Processor.ProcessOnce` and the JetStream message acknowledgement at the
   local-transaction/consumer-ack boundary.
3. The rendered Control/Storage JetStream release configuration, compared with
   the typed Go contract used by Publisher and consumer tests.

## Stream Contract

The release owns exactly one `VELA_EVENTS` stream for `vela.events.>` with:

- file storage and exactly three replicas;
- Broker acknowledgements enabled;
- a duplicate window greater than the Publisher claim and automatic retry
  interval;
- bounded message age, message size, and total logical storage;
- no direct message mutation, purge, or roll-up authority; and
- release metadata identifying the contract revision.

The Publisher sends the PostgreSQL `event_id` as `Nats-Msg-Id`, declares
`VELA_EVENTS` as the expected stream, and rejects a PubAck naming any other
stream or lacking a positive sequence. It reads and validates the live stream
contract immediately before publishing and again after PubAck; drift or an
unavailable stream-info response returns no durable receipt. Its credential may
publish only `vela.events.>` and the exact read-only
`$JS.API.STREAM.INFO.VELA_EVENTS` request. A PubAck is only a delivery receipt;
the Outbox row remains unpublished until PostgreSQL stores that exact stream and
sequence under the current claim token.

## Consumer Contract

The first release consumer contract is the Scheduler `job.ready` wakeup. It is a
named durable pull consumer with:

- explicit ack and three replicated consumer-state replicas;
- file-backed consumer state inherited from the stream;
- an exact `vela.events.job.ready` filter;
- bounded `AckWait`, `MaxAckPending`, pull batch, and request expiry; and
- unlimited redelivery because PostgreSQL reconciliation and idempotency, not a
  Broker delivery-count cutoff, own eventual recovery.

The consumer decodes the versioned protobuf `EventEnvelope`, verifies the
`event_id`, aggregate identity/version/type, schema version, subject, and
Organization/Project identity carried by the payload. It opens the Inbox receipt
transaction, synchronously invokes the aggregate handler, commits the receipt,
and only then uses a confirmed explicit ack. The Scheduler handler waits for the
serialized Scheduler cycle to persist any Assignment before it returns. The
Scheduler and Inbox roles remain separate, so their PostgreSQL transactions are
not merged: a crash after scheduling but before the receipt commit redrives the
idempotent Scheduler boundary, while a committed receipt proves the handler
already completed. A handler or commit error leaves the message unacknowledged.
If the receipt commits and the process or ack fails, redelivery observes the
Inbox event/version uniqueness and does not invoke the handler twice.

The Scheduler consumer uses an independent `vela_scheduler_inbox` database
pool, separate from the scheduling transaction pool. The release bootstrap
creates that NOLOGIN role and migration 00025 grants it only the exact SECURITY
DEFINER `vela_prepare_scheduler_inbox_receipt` and
`vela_record_scheduler_inbox_receipt` functions; the role has no direct Inbox
or Outbox table access. Prepare locks the exact authoritative Outbox row across
the handler and checks whether the receipt already exists without locking the
Job row. Record runs only after the handler completes. Both functions fix
`consumer_name=scheduler`, aggregate type `Job`, event type `job.ready`, and
schema version 1, and require every caller-supplied identity field to match the
authoritative Outbox row. Migration 00025 does not change the existing
`vela_scheduler` privilege set, so an N-1 Scheduler can verify its exact
boundary during the expand phase.

## Required Failure Evidence

1. A real three-node JetStream test stream is created from the release contract.
2. With all replicas healthy and with one follower unavailable, publish returns
   a PubAck and the Outbox row records the target stream and sequence.
3. Without stream quorum, publish cannot return a successful durable receipt and
   the Outbox row remains unpublished.
4. After quorum returns, retry uses the original `Nats-Msg-Id`; JetStream stores
   one logical event and PostgreSQL records one receipt.
5. Cancellation immediately after PubAck but before PostgreSQL marking leaves
   the row unpublished; reclaim and retry use the same message ID and converge.
6. The Scheduler cycle persists any Assignment before the Inbox receipt commits
   and before confirmed ack. Receipt commit followed by no ack causes
   redelivery; the Inbox receipt and aggregate version prevent a second handler
   transition, after which confirmed ack clears pending delivery.
7. A one-replica stream is rejected before publish; an ephemeral or non-explicit
   consumer, memory-backed consumer state, wrong filter, or any other
   semantics-bearing stream/consumer drift fails contract validation.
8. The rendered deployment carries the same stream and consumer contract and
   retains three PVC-backed, anti-affined NATS replicas.

## Compatibility And Non-goals

- Migration 00025 adds only the narrow Scheduler Inbox receipt functions and
  grants them to the new, independently bootstrapped `vela_scheduler_inbox` role;
  it does not rewrite tables or event data or expand an existing runtime role.
  Database expansion precedes the current control binary. N-1 publishers retain
  their current payload, stable message ID behavior, and old versioned
  credential Secret. The current ReplicaSet uses a separately versioned Outbox
  credential containing the new exact stream-info permission; replacing one
  shared credential underneath N and N-1 is forbidden.
- No protobuf schema change is required.
- Duplicate-window suppression is an optimization. Correctness continues to
  depend on PostgreSQL constraints, aggregate CAS, Inbox receipts, and periodic
  reconciliation.
- This slice does not claim a real RKE2 deployment, disk durability, cross-node
  network partition result, old-event production backlog drain, JetStream
  rebuild, or Launch Receipt. Production Gates remain `0/9 PASS`.

## Verification

Before commit:

- run focused unit tests for the typed stream/consumer contract;
- run focused PostgreSQL plus JetStream crash-window integration tests;
- run the real three-node JetStream quorum integration test;
- render and validate the Control/Storage deployment contract;
- run `make generate`, `make test`, `make lint`, `make test-cross`, and
  `make test-integration`;
- run a code review against this specification and repository standards; and
- confirm `git diff --check` and a scoped worktree audit that excludes unrelated
  user changes.
