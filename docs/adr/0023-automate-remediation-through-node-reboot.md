# Automate certified remediation through node reboot

Production remediation is intended to perform process restart and CUDA cleanup,
certified GPU reset and PCIe FLR, and fenced, rate-limited driver reload or node
reboot. BMC power cycle requires human approval or two-person confirmation at
launch. Ambiguous identity, uncertified topology, repeated recovery failure, or
failed validation automatically quarantines the Worker.

The repository implements the verifiable control-plane portion of this decision
in migrations `00019`-`00022` and `internal/remediation`, plus a guarded
controller-to-host Node Agent gRPC runtime in `internal/nodeagent` and
`cmd/vela-node-agent`. The control-plane `RemoteExecutor` authorizes and claims
the authoritative operation before the RPC; the host Agent authenticates an
explicit `spiffe://vela.internal/controller/...` identity, checks its local
Node/Worker target, replays durable local receipts, and fails closed unless the
executor declares a verified post-check. This is a deployable runtime contract,
not a claim that any GPU/topology tuple has a production Launch Receipt. Stable
claim identity lets controller restart or a lost response retrieve a durable
host receipt. A durable pre-action intent prevents repeating a privileged action
when its prior outcome cannot be proven.

## Consequences

Every repository Remediation Operation binds node identity, device identity,
worker epoch, idempotency key, certification revision, failure evidence, and
audit events. L6 requires two distinct approvals; L7 is immediate quarantine;
successful completion requires a post-check digest, an empty active-Attempt set,
and no active Lease. The control plane fences current Worker/RECONCILER Leases
before execution, gives each operation a bounded deadline, and recovers an
orphaned operation into quarantine. A failed post-check, active Attempt/Lease,
identity mismatch, or expired operation leaves the Worker quarantined. Node
identity changes and quarantine reuse require a Worker epoch advance.

The repository now has a controller-side authoritative adapter, a host-side
systemd entrypoint, an explicit controller actor map, a bounded command
allowlist, device/certification policy, host fence, per-node rate limit, health
post-check, and durable local receipt path. Production still requires the
actual Node Agent deployment, certificate provisioning, hardware capability
matrix certification, device and runner checks, model warm-up, canary, live
claim/receipt monitoring, and versioned Launch Receipts before this ADR can be
marked fully implemented. Migration `00020` adds a database-backed execution
claim, `00021` adds bounded dispatch discovery, and `00022` permits only an
exact claim replay while preserving conflict rejection. Host helpers receive
the immutable Plan as bounded arguments and must return identity-bound JSON
evidence. Rate-limit history, execution intents, and terminal receipts are
locked or atomically published and fsynced on host-local storage.

Repository evidence: `docs/specs/0020-certified-remediation.md`.
