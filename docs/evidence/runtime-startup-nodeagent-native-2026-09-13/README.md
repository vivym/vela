# Native Node runtime-startup composition checks (2026-09-13)

The source-matched `internal/nodeagent` Linux test binary was compiled from
this checkout and executed as root on `marslab` (Ubuntu 24.04,
`6.8.0-137-generic`) with a temporary root-owned `exec-observer` and
`exec-probe`. Both helper binaries and the test binary were removed after the
run. No systemd unit, RKE2 process, or business workload was changed.

The following groups passed:

- `TestRuntimeStartupAuthority*` (complete-source validation, reservation and
  grant composition, invalid-policy and missing-authorization fail-closed,
  credential binding);
- `TestRuntimeStartupCompositionReceiptRejectsAuthorityGaps`;
- `TestRuntimeStartupCoordinator*` (observed activation, mismatch rejection,
  custody release, successful exchange lifetime and same-UID process
  rejection);
- `TestRuntimeStartupOrchestration*` (single caller use, source validation,
  shutdown/cancellation and listener/route cleanup);
- `TestRuntimeObserverCustody` (held start, close-before-start, caller
  mismatch, stopped check, queued check/revoke and channel loss).

This is stronger native evidence for the Node authority and observer
components. It remains `validation_only=true`: it does not exercise the
`cmd/vela-node-agent` command-level resource assembly, production Fleet,
production policy issuer, or a real ModelRuntime Permit.
