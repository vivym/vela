# Automate certified remediation through node reboot

Production remediation is intended to perform process restart and CUDA cleanup,
certified GPU reset and PCIe FLR, and fenced, rate-limited driver reload or node
reboot. BMC power cycle requires human approval or two-person confirmation at
launch. Ambiguous identity, uncertified topology, repeated recovery failure, or
failed validation automatically quarantines the Worker.

The repository currently implements the verifiable control-plane portion of this
decision in migration `00019` and `internal/remediation`. It does not claim that
the host actions themselves are available in the current binary.

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

The host-side Node Agent, hardware capability matrix, device checks, runner
checks, model warm-up, canary, rate limiting, host fencing, and production
Launch Receipts remain required before this ADR can be marked fully implemented.

Repository evidence: `docs/specs/0020-certified-remediation.md`.
