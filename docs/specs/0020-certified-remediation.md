# Certified Remediation Control Plane

Date: 2026-08-24

Status: Partial. The repository-verifiable control-plane slice and a guarded
Node Agent transport contract are implemented; host execution and production
deployment evidence remain outside this slice.

Predecessors:

- `docs/specs/0002-worker-assignment-and-execution-lease.md`
- `docs/specs/0004-worker-heartbeat-lease-renewal-and-progress.md`
- `docs/specs/0005-execution-failure-retry-and-job-expiry.md`
- `docs/adr/0023-automate-remediation-through-node-reboot.md`

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
5. `nodeagent.Server` and `nodeagent.Client` for an identity-bound transport
   whose `Authorizer` must bind the request to authoritative operation state.
6. `vela_claim_remediation_execution` for a single persistent execution claim
   per `EXECUTING` operation before any host action starts.

The transport includes `ControlPlaneAuthorizer` and `ControlPlaneLedger`
adapters over `remediation.Service`; their tests use in-memory fakes. No
production service registration, process wiring, or live cluster receipt
evidence is claimed by this spec.

The executor seam accepts only absolute allowlisted command paths, rejects NUL
arguments, requires a non-empty certified identity, passes the full immutable
execution `Plan` to the runner for device/epoch/capability checks, and refuses
L6/L7 direct execution. Real host commands are intentionally injected by a
future Node Agent rather than embedded in the control plane.

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
- Node Agent verified mTLS identity, exact receipt replay, conflicting operation
  rejection, control-plane authorization refusal, deadline receipt, and
  fail-closed unverified-post-check behavior;
- single execution-claim insertion and conflicting cross-process claim
  rejection, with migration 00020 down/up evidence;
- runtime role exact privileges and security-definer owner/search-path checks;
- concurrent remediation request/start/complete lock-order evidence;
- migration 00019 upgrade, empty down/up, and concurrent scheduler migration
  serialization;
- unit, race, integration, generated-code, vet, and lint verification.

## Explicitly Deferred

This repository slice does not implement or claim real `nvidia-smi`, CUDA
cleanup, GPU reset, PCIe FLR, driver reload, node reboot, BMC power cycle,
production Node Agent registration, deployed receipt/claim monitoring, or
device/model warm-up, canary execution, rate limiting, hardware topology
certification, or production rollback. The transport contract intentionally fails closed without
an authoritative `Authorizer` and a verified post-check. Those actions require
an authenticated controller/agent identity contract, host-specific capability
policy, post-check and canary receipts, deployment isolation, and a versioned
Launch Receipt.

## Completion Boundary

The repository-verifiable Certified Remediation control plane and guarded
transport contract are complete when the implementation commits, this spec, role
evidence, migration down/up, narrow and full verification, and standards/spec
review are recorded. ADR 0023 remains partial until production Node Agent
wiring, host evidence, and Launch Receipt evidence exist; this slice does not
change Production Gates from `0/9 PASS`.
