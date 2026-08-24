# Automate certified remediation through node reboot

Production remediation is intended to perform process restart and CUDA cleanup,
certified GPU reset and PCIe FLR, and fenced, rate-limited driver reload or node
reboot. BMC power cycle requires human approval or two-person confirmation at
launch. Ambiguous identity, uncertified topology, repeated recovery failure, or
failed validation automatically quarantines the Worker.

The repository implements the verifiable control-plane portion of this decision
in migration `00019` and `internal/remediation`, plus a guarded Node Agent gRPC
transport contract in `internal/nodeagent`. The transport requires an injected
control-plane authorizer, binds requests to verified mTLS identity, replays exact
receipt matches, rejects conflicting operation IDs, and fails closed unless the
executor declares a verified post-check. It does not claim that host actions or
their production wiring are available in the current binary.

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

The repository now has adapters for authoritative operation authorization and
completion persistence, but production process wiring and a deployed persistent
receipt path are still absent. The production Node Agent deployment, actor
binding, hardware capability matrix, device checks, runner checks, model
warm-up, canary, rate limiting, host fencing, and production Launch Receipts
remain required before this ADR can be marked fully implemented. The transport
direction and peer identity contract must also be fixed when the
controller-to-agent deployment is wired. The runner contract now receives the
full immutable execution Plan, but no production host runner has yet certified
that plan against local device and capability state.

Repository evidence: `docs/specs/0020-certified-remediation.md`.
