# Certified Remediation Control Plane

Date: 2026-08-24

Status: Partial. The repository-verifiable control-plane slice and guarded
Node Agent runtime contract are implemented; certified host hardware actions
and production deployment evidence remain outside this slice.

Predecessors:

- `docs/specs/0002-worker-assignment-and-execution-lease.md`
- `docs/specs/0004-worker-heartbeat-lease-renewal-and-progress.md`
- `docs/specs/0005-execution-failure-retry-and-job-expiry.md`
- `docs/adr/0023-automate-remediation-through-node-reboot.md`

Successor: `docs/specs/0027-certified-remediation-runtime-authority.md` closes
the repository production caller, local Worker epoch, exact GPU UUID/PCI BDF
matrix, and end-to-end quarantine evidence gaps left by this slice.

## Goal

Record and authorize Worker remediation through an identity-bound, auditable,
fail-closed control-plane contract. Every operation binds the Worker UUID and
epoch, node identity, device identity, failure evidence, certification revision,
action level, idempotency key, and actor identity. A successful post-check may
return a Worker to `READY`; an ambiguous identity, failed post-check, active
Attempt, or uncertified action quarantines it.

## Governing Decisions

- ADR 0023: action levels are ordered from process restart through BMC power
  cycle, with L6 requiring two distinct approvals. L7 is the fail-closed
  quarantine action.
- ADR 0002, 0010, and 0028: remediation is fenced by the current Worker epoch
  and cannot make an active Attempt disappear or extend its authority.
- ADR 0007: the remediation runtime has no direct table access and can execute
  only the exact security-definer functions granted to its dedicated role.
- ADR 0013: migration `00019` is additive for existing control paths; old
  Workers receive a deterministic `node_identity` default from `spiffe_id`.
- ADR 0029: repository tests are implementation evidence, not Launch Receipts.
  Production remains `0/9 PASS`.

## Public Seams

The production-facing repository seams are:

1. `remediation.Service` for request, approval, start, completion, recovery, and lookup;
2. `remediation.AllowlistedExecutor` for an explicitly injected command runner;
3. PostgreSQL `vela_*_remediation` security-definer functions and their
   immutable operation/event ledger;
4. the dedicated `vela_remediation` runtime role and
   `vela_remediation_owner` owner boundary.
5. `nodeagent.RemoteExecutor` and `nodeagent.Client` for the
   controller-to-agent direction. The controller-side `Authorizer` binds the
   request to authoritative operation state before the RPC; the host-side
   `nodeagent.Server` authenticates the controller actor and its local target.
6. `vela_claim_remediation_execution` for one exact-replayable persistent
   execution claim per `EXECUTING` operation before any host action starts.

The transport includes `ControlPlaneAuthorizer` and `ControlPlaneLedger`
adapters over `remediation.Service`, a durable host-local `FileLedger`, and the
`cmd/vela-node-agent` systemd entrypoint. Unit tests use in-memory fakes; no
live cluster receipt evidence or hardware certification is claimed by this
spec. The claim ID is stable for `(operation_id, actor_identity)`, so a lost RPC
response or controller restart can retrieve the durable host receipt without
creating a second execution authority.

The executor seam accepts only absolute allowlisted command paths, rejects NUL
arguments, requires a non-empty certified identity, passes the full immutable
execution `Plan` to the runner for device/epoch/capability checks, runs a host
fence and bounded rate limit, and refuses L6/L7 direct execution. Real command
paths, capability matrices, fence checks, and post-check commands are injected
by the host Node Agent deployment rather than embedded in the control plane.
Every helper receives the immutable Plan as bounded non-shell arguments. Fence
and post-check helpers must return exact identity-bound JSON evidence; exit zero
or arbitrary non-empty output is insufficient. The production entrypoint uses
a host-local locked and fsynced rate-limit ledger. Before a privileged action,
the Agent publishes a non-replaceable execution intent. An intent without a
terminal receipt after restart yields `EXECUTION_OUTCOME_UNKNOWN` and quarantine
rather than repeating an action whose outcome cannot be proven.

The Plan, RPC request hash, helper arguments, and helper evidence also bind the
authoritative failure class; a host action cannot be replayed against a
different failure classification with the same operation identity.

The endpoint registry binds Node identity, Worker UUID, DNS server name, and a
canonical Node Agent SPIFFE URI. The Agent verifies the URI against its local
Node/Worker identity at startup. Controller certificates and actors use the
canonical `spiffe://vela.internal/controller/<id>` to `controller/<id>` mapping.

## Operation State And Identity

The operation state machine is:

```text
REQUESTED -> APPROVAL_REQUIRED -> REQUESTED -> EXECUTING
REQUESTED -> EXECUTING
EXECUTING -> SUCCEEDED | QUARANTINED
REQUESTED | APPROVAL_REQUIRED -> QUARANTINED
```

The operation identity fields are immutable. Terminal states and all
`remediation_operation_events` are immutable. One active operation is allowed
per `(worker_id, worker_epoch)`, and exact idempotent retries replay the original
operation. A conflicting idempotency key fails without a new event.

L0/L1 initially drain a Worker; stronger actions recover it. Every new operation
fences active Worker/RECONCILER Leases in the same transaction before execution.
L6 starts in `APPROVAL_REQUIRED` and requires two distinct approvers before
execution. L7 creates a terminal `QUARANTINED` operation immediately with both
timestamps and no certification revision requirement.

`vela_start_remediation` and `vela_complete_remediation` use the common
Worker-then-operation row-lock order, recheck Worker identity and epoch, and
reject an unfenced Lease. Completion requires a SHA-256 post-check digest for
success and rechecks for an active Attempt or Lease. A remaining `ASSIGNED`,
`RUNNING`, or `FINALIZING` Attempt changes the result to
`ACTIVE_ATTEMPT_REMAINS` and quarantines the Worker. Each operation has a
bounded deadline; `vela_recover_remediation` converts an expired or orphaned
active operation to terminal quarantine and is idempotent. A Worker node
identity cannot change without an epoch advance, and a quarantined Worker
cannot return to service in the same epoch.

## Database And Role Boundary

Migration `00019_certified_remediation.sql` adds:

- `workers.node_identity` with defaulting, validation, and uniqueness;
- `remediation_operations` and immutable `remediation_operation_events`;
- action/state enums, transition and immutability triggers;
- request, approval, start, complete, recovery, and lookup functions;
- operation deadlines, active-Lease fencing, quarantine Lease revocation, and
  Worker identity/epoch fencing;
- exact runtime grants and independent owner grants.

Migration `00020_remediation_execution_claim.sql` adds the durable execution
claim, migration `00021_remediation_execution_dispatch.sql` adds bounded
`EXECUTING` operation discovery, and migration
`00022_remediation_execution_claim_replay.sql` makes only the exact claim tuple
replayable while preserving conflicting cross-process rejection.

The runtime role has no table, sequence, private-schema, or owner inheritance.
The owner is `NOLOGIN BYPASSRLS`; it has only the table privileges needed by the
security-definer functions (`workers` SELECT/UPDATE, `worker_epochs` and
`attempts` SELECT, `attempt_leases` SELECT/UPDATE, and operation/event table
mutation). Role integration tests reject direct runtime table grants and role
confusion.

## Required Evidence

- exact request replay and conflicting-input rejection;
- worker identity and epoch mismatch rejection;
- node-identity immutability and same-epoch quarantine reuse rejection;
- L6 two-person approval and replay behavior;
- L7 immediate quarantine with terminal timestamps;
- failed post-check, active Attempt, and active Lease quarantine/fencing;
- bounded deadline and orphan recovery with terminal quarantine replay;
- immutable operation/event update and delete rejection;
- operation/event audit sequence and worker lifecycle/reachability transitions;
- executor allowlist, unsafe-path, NUL-argument, and privileged-action refusal;
- Node Agent verified controller mTLS identity, local target binding, exact
  durable receipt replay, conflicting operation rejection, authoritative
  controller-side claim, deadline receipt, and fail-closed unverified-post-check
  behavior;
- single execution-claim insertion, exact replay after a lost response,
  conflicting cross-process rejection, interrupted-intent quarantine, durable
  per-node rate limiting, and migrations 00020-00022 down/up evidence;
- helper Plan-argument and structured fence/post-check identity binding, plus
  Node/Worker/server-certificate and controller actor/certificate binding;
- runtime role exact privileges and security-definer owner/search-path checks;
- concurrent remediation request/start/complete lock-order evidence;
- migration 00019 upgrade, empty down/up, and concurrent scheduler migration
  serialization;
- unit, race, integration, generated-code, vet, and lint verification.

## Explicitly Deferred

This repository slice does not implement or claim real `nvidia-smi`, CUDA
cleanup, GPU reset, PCIe FLR, driver reload, node reboot, BMC power cycle,
device/model warm-up, canary execution, hardware topology certification, or
production rollback. The runtime contract intentionally fails closed without
an authoritative controller-side `Authorizer`, host-specific capability
policy, fence, and verified post-check. Those actions require deployment
isolation, live hardware evidence, and a versioned Launch Receipt.

## Completion Boundary

The repository-verifiable Certified Remediation control plane and guarded
transport contract are complete when the implementation commits, this spec, role
evidence, migrations 00019-00022 down/up, narrow and full verification, and
standards/spec review are recorded. The successor Slice 27 supplies direct
repository evidence for Scenario 18. ADR 0023 remains partial until host
hardware evidence and Launch Receipt evidence exist; neither slice changes
Production Gates from `0/9 PASS`.
