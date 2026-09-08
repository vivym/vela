# Remote journal custody in the actual Runtime server startup path

Date: 2026-09-09. Baseline: `637c80e`.
PostgreSQL 94, Worker journal 5, Runtime journal 8 and Production Gates **0/9**
are unchanged. The complete correctness/architecture objective remains open.

## Problem and implementation

The prior remote Supervisor constructor attached Services whose backends had
already started. It could validate ordinary journal transitions, but did not
establish safe factory ordering for `StartRuntimeServer`. Swapping constructors
after factory execution would authorize too late and leave local epoch/journal
ownership in the startup path.

`RuntimeServerConfig.RemoteStartup` now provides an explicit remote branch in
the actual server implementation. It requires a Registry binding and verifier,
an authenticated journal transport and an independent remote startup authorizer.
It rejects any simultaneous local `EpochStore`, `ExecutionFloor` or local-file
`BackendStartupGate`. There is no fallback to local persistence or ungated backend
startup if remote configuration fails.

Before any backend factory, the server validates the exact launch snapshot,
Registry-to-journal binding, storage identity and complete owner journal. This
first-startup path requires the owner's already-persisted unresolved incarnation
and empty execution/floor/non-admission history. It does not create that intent,
adopt a replacement or interpret unresolved history as permission.

`RemoteStartupBindings` proposes the first epoch strictly above each manifest
floor, rejecting overflow. The trusted Node authorizer must independently reserve
and approve those exact bindings. Runtime performs no local epoch allocation;
the proposal function is not a durable allocator or permission issuer.

The remote authorizer receives a distinct `RemoteBackendStartupRequest`, including
the Registry digest, journal/storage identity, persisted incarnation, launch digest
and every proposed live Runtime binding. It runs once for the member-wide startup
before any AUX backend factory. After permission, the server rereads the owner;
it also checks freshness before subsequent factories and before Supervisor
attachment. Failed startup closes already-created backends and publishes no RPC
socket. The server and authorizer receive independent snapshots of mutable
configuration/binding slices.

## Verification of the actual path

The host race tests cover permitted and denied AUX startup, missing authorization,
mixed local/remote custody, missing/invalid Registry evidence, manifest/verifier/
incarnation/journal mismatch, epoch overflow, cancellation after permission,
journal change after permission, journal change after the first AUX factory,
and alias mutation. They also verify Registry-bound discovery over real gRPC,
rollback/close of all created backends, socket cleanup and refusal by an
authorizer that rejects a repeated startup invocation.

`TestJournalServerStartsRemoteRuntimeBeforeActualWorkerExecution` runs the actual
`StartRuntimeServer` in a non-root Linux PID-1 process, separate from the real
Worker Agent process and root Node journal owner. Before permission the parent
verifies the declaration against the fixture's owner/launch/Registry bindings,
with zero factory calls and no published Runtime socket. The authorizer uses
inherited test-control pipes; it is explicitly a fixture, not a production
authorization protocol.

With permission, the actual Runtime server creates one fake backend and exposes
its gRPC endpoint. The Worker performs Prepare/Start once, seals output and
obtains a validated exact drain checkpoint through its real client wrapper.
Final root status has `Highest=1`, `PendingExecutions=0` and the unchanged
incarnation. Refusal creates no backend or Runtime socket and leaves `Highest=0`.
After close, the workload-owned directory is empty: Runtime created no local
journal or epoch files. Existing non-root filesystem denial and original-process
Node role authentication remain in the fixture.

The permitted native case uses 39 authenticated/replied journal exchanges and
the denied case two. Both servers join with zero in flight and no failed or
overloaded exchanges. The combined new native test completes in 0.42 seconds.
These are startup/Stage observations, not sustained performance measurements.

## Evidence and remaining assembly

Final-source validation passes focused race (2.157 seconds), full uncached
ModelRuntime race (108.623 seconds), ordinary
repository tests, vet, ordinary/Linux lint, Linux/amd64 cross compilation and
the native Linux/arm64 race runner. The runner now includes the remote-server
startup tests: 55 ModelRuntime behavioral main tests plus one helper and 15 Node
main tests, with no skips or race reports. The adjacent JSON receipt pins source,
runner and raw artifacts under `docs/evidence/remote-runtime-server-2026-09-09/`.

Linux/amd64 verification is compile-only (`go test -exec=true`); native behavioral
execution is the separate Linux/arm64 race run. Neither cross compilation nor
host tests substitute for the root/non-root IPC observations above.

This increment implements the server-side startup composition. It does not
provide the production remote authorizer, durable once-only permission/epoch
reservation transaction, CLI configuration, Node enrollment/accept-loop ownership,
protected Fleet mounts or Worker input/materialization journal custody. In
particular, a permissive callback could authorize repeated factory invocations;
the server does not claim to manufacture independent authorization from its own
configuration. Lost permission replies and process replacement remain recovery
protocol obligations, not automatic retries of this first-startup branch.

The prior CPU Job/cache campaign remains bound to `637c80e` and local Runtime
persistence. A full protected remote-owner Job path remains the next integration
target, followed by replacement, history reclamation and sustained arrivals;
see the [remaining validation plan](remaining-validation-2026-09-09.md).
