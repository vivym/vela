# Certified Remediation Runtime Authority

Date: 2026-08-27

Status: Implemented for the repository runtime path. Production hardware
certification and the GPU remediation Launch Receipt remain open gates.

Predecessors:

- `docs/specs/0020-certified-remediation.md`
- `docs/specs/0023-fleet-controller-worker-readiness.md`
- `docs/adr/0023-automate-remediation-through-node-reboot.md`

## Goal

Close the repository-verifiable portion of Architecture Acceptance Scenario 18.
The control plane must have a production caller for remediation operations, the
host Agent must reject a stale Worker epoch, and every automatic L0-L5 action
must bind one canonical GPU UUID to one PCI BDF, failure class, action, and
certification revision. A failed host post-check must reach the authoritative
PostgreSQL operation and Worker as `QUARANTINED`.

## Platform Operator Control Path

The public OpenAPI contract exposes four Platform Operator-only operations:

- `POST /v1/platform/remediation/operations` requests an idempotent operation;
- `GET /v1/platform/remediation/operations/{id}` reads its immutable projection;
- `POST /v1/platform/remediation/operations/{id}/approvals` records one L6 approval;
- `POST /v1/platform/remediation/operations/{id}/execution` moves an authorized
  operation to `EXECUTING`, where the existing dispatcher owns delivery.

These paths use the existing Platform Operator OIDC authenticator. Requester and
approver identity comes only from the authenticated operator, never from the
JSON body. PostgreSQL remains the state and approval authority. L6 cannot enter
`EXECUTING` until two distinct operator identities have approved it.

The host transport continues to reject direct L6 and L7 execution. An approved
L6 operation therefore records the human-control decision but still requires a
separately certified BMC executor and production receipt; this slice does not
make BMC power cycle automatic.

## Host Identity And Capability Binding

`vela-node-agent` now requires `VELA_NODE_AGENT_WORKER_EPOCH`. The host server
compares Node identity, Worker UUID, and Worker epoch before writing an execution
intent. The control-plane endpoint registry also records `worker_epoch`, so a
dispatcher cannot resolve an endpoint registered for another epoch.

The capability file is keyed by canonical NVIDIA GPU UUID. Every entry contains:

- one unique lowercase PCI BDF;
- one certification revision;
- a non-empty failure-class allowlist; and
- a non-empty L0-L5 action allowlist.

The policy resolves the requested GPU UUID to that exact binding before the
fence, action, or post-check runs. Every helper receives both `--vela-gpu-uuid`
and `--vela-pci-bdf`; fence and post-check JSON must return both values with the
operation, claim, Worker epoch, failure class, action, revision, and evidence
digest. Unknown GPU UUIDs, reused BDFs, or any mismatch fail closed.

## Repository Evidence

- Node Agent unit tests cover local epoch mismatch, endpoint epoch mismatch,
  GPU UUID/PCI BDF structured-evidence mismatch, failure-class/action/revision
  matrix rejection, duplicate BDF rejection, and helper argument binding.
- `TestRemediationPlatformAPIRequiresDistinctL6ApprovalBeforeExecution` proves
  the authenticated HTTP path, exact replay, pre-approval conflict, two distinct
  approvals, and transition to `EXECUTING` against PostgreSQL.
- `TestRemediationDispatcherQuarantinesFailedCertifiedPostcheck` runs the
  production dispatcher, controller authorizer and claim, host ledger, certified
  executor, structured post-check, control-plane ledger, and PostgreSQL
  completion path. A failed post-check ends in authoritative quarantine.
- `internal/deploymentcontract/node_agent_test.go` pins the systemd service,
  Worker epoch, endpoint registry, matrix fields, and helper arguments.

## Completion Boundary

Scenario 18 has direct repository evidence after generation, unit, integration,
race, cross-platform, deployment-contract, lint, and full-suite verification.
ADR 0023 remains `Partial`: real GPU discovery, reset/FLR/driver/reboot actions,
model warm-up and canary, live certificate and endpoint provisioning, monitoring,
and a versioned GPU remediation Launch Receipt still require the target host
environment. Production Gates remain `0/9 PASS`.
