# Durable Stream admission lifecycle evidence

Status: local CPU-only implementation based on `e6420a9`, schema 90. The explicit
`NewDurableStreamAgent` constructor now integrates the admission journal with the
Stream lifecycle. The production command still builds the existing non-durable
constructor. Production Gates remain **0/9**.

## Implemented behavior

- `ProductionAgent.Run` carries the actual `DiscoveryResult.AcquireCommandID`
  through startAndMonitor into the Stream. Direct callers can use
  RunAcquiredAssignment or ExecuteAcquiredAssignment. The older assignment-only
  entry points reject missing Acquire identity when durable admission is enabled.
- Begin persists INPUTS_PENDING before Resolve and before exposing the pending
  input slot to Stop. The handle stays owned until the call's resolver and local
  preparation work return. EnterRuntime persists RUNTIME_ENTERED before any
  member Prepare RPC; failure to persist prevents that RPC.
- A matching Stop during input resolution persists CLOSED and cancels the input
  context without waiting for download completion or asserting AllStopped.
  The writer handle remains held until Resolve returns, including a late nil
  result. Closing this local admission intent proceeds even if the Stop caller's
  context was canceled; it conveys no execution time or drain proof.
- ObserveRuntimeAuthority validates current signed authority, all trusted Runtime
  bindings and the existing RUNTIME_ENTERED execution before recording a renewal.
  It persists that renewal before a Status RPC can install it. Empty,
  INPUTS_PENDING and CLOSED history cannot authorize Reattach. A manually rebuilt
  Stream can reattach a recorded running execution without another Prepare.
- Heartbeat, production monitoring, pre-reattach inspection and Seal consult the
  same gate. Stop and rejected/failed Control START responses close admission.
  An uncertain member Prepare leaves RUNTIME_ENTERED for explicit recovery.
- Successful Fail closes the immutable execution. Seal persists its local
  materialization receipt and closes admission before clearing active authority.
  ResumeMaterializations repeats that closure before publication/commit, including
  when the record is already CLOSED in the backlog behind a newer allocation.
  It does not close the newer allocation.
- The admission mutex is not held across Resolve, Control Exchange or member
  Runtime calls. Existing input/runtime locks serialize the local handoffs. All
  modeled writers must still be joined by their owning component before release.

The new concurrency test reproduced a pre-existing active-state bug: Fail can be
accepted after a concurrent Heartbeat installs a renewal, but clearActive compared
complete envelope digests and retained the renewed active pointer indefinitely.
clearActive now compares immutable execution identities, retaining allocation,
lease, nonce, Worker/member/Runtime scope, fences and execution-spec identity
while allowing the renewal fields to differ. The regression now rejects a replay
as CLOSED rather than reporting a permanently busy Worker.

## Validation

- Lifecycle tests cover invalid/missing identity before Resolve, input failure
  and retry across journal reopen, durable intent visible at Prepare, direct and
  unsolicited Stop with a late real-file writer, lost accepted Prepare response,
  renewed authority recorded before Status, explicit reattach after reopen,
  closed/missing intent rejection before RPCs, and write-permission failures
  before Prepare and before renewal installation.
- A ProductionAgent.Run test exercises discovery, original Acquire identity,
  execution, heartbeat, output readiness, materialization, COMMIT and the next
  NoWork poll using resident fake Runtime RPCs and fake Control/publication.
- Materialization recovery after an injected L2 outage preserves the CLOSED old
  record and a newer INPUTS_PENDING allocation. The initial test omitted the
  publisher's `failures: 1` setting and was corrected before validation.
- Four additional multi-member Stop-liveness cases run with the durable gate:
  delayed START/HEARTBEAT renewal responses, REATTACH of a renewed execution,
  and cancellation RPC failure. Late responses do not reopen the journal.
- `go test ./...`: PASS; stageworkeragent 7.095 s.
- `go test -race ./internal/stageworkeragent ./internal/stageworkertransport ./internal/stageworkermembertransport ./cmd/vela-stage-worker-agent`:
  PASS; 14.634 s, cached, 5.107 s and 5.977 s respectively.
- `make lint`: PASS, 0 issues.
- Linux arm64: all durable lifecycle, Stop-liveness and assignment-admission
  tests PASS in the existing `postgres:17-alpine` image as UID/GID 65534, with
  no network, read-only root and disposable writable tmpfs. No PostgreSQL server
  was started. The permission-fault tests execute under that non-root identity.

This is local Stream call-order and recovery evidence. The tests do not run a
new database-backed load campaign or validate terminal filesystem deletion.
Linux tmpfs and process reopen do not establish power-loss durability.

## Remaining work

The default command must eventually construct trusted complete Runtime bindings
for this gate, bind it to the actual resolver/output roots before those roots are
used, and refresh/recover that configuration across Fleet and Runtime epoch
changes. Production startup must inspect the journal and explicitly reconcile
RUNTIME_ENTERED and uncertain persistence outcomes. Automatic startup recovery
is not implemented by the manual Reattach test above.

Stop before the Worker has observed an assignment, unseen later allocations,
direct member Prepare/Start RPCs and writes owned by a Runtime are outside the
local already-admitted input slot. The current member transport forwards Runtime
commands; the current Worker input resolver runs on the leader. Closing the
leader's journal alone does not establish the other Runtime entry-point fence.

The [retirement design](terminal-scratch-retirement-design-2026-09-05.md) still
requires the signed terminal cutoff, Runtime floor installation across resident
profiles, separate FLOOR_INSTALLED/DRAINED evidence, execution-specific drain
that preserves resident models, and crash-safe file/record retirement. The
bounded admission backlog still has no removal API and eventually backpressures
without that lifecycle. CLOSED, cancellation ACKs and a signed INPUTS_UNUSED
disposition remain insufficient to authorize deletion by themselves.
