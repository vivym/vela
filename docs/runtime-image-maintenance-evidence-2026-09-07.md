# Runtime image maintenance command and release binding

This increment follows `3e1b400`. Expired image observations now have an explicit
Node Agent command and independently supervised deployment unit. The unit is a
required artifact of release bundle schema 3. The CPU test executes the actual
Linux command against a private containerd after a real observer process dies.

## Architecture and behavior

The existing remediation daemon must remain available when Kubernetes or the
container runtime is unavailable. It therefore does not dial the image observer
or own its maintenance loop. `vela-runtime-image-maintenance.service` invokes the
same packaged binary with a dedicated `runtime-image-maintenance` subcommand.
Neither service depends on the other. The new command does not load remediation
configuration, connect to Fleet or probe GPUs.

The command loads a bounded private JSON configuration containing exactly the
schema, local containerd socket, Node identity, namespace and `native`
snapshotter. It rejects unknown, duplicate, case-aliased, missing/null fields,
insecure files and unsupported targets before dialing. Its configuration is
fixed for the process lifetime; changing it requires a service restart.

Each pass authenticates a fresh root-owned local socket and its kernel peer,
with a 10-second dial deadline. It calls `RecoverExpired` with a separate
30-second deadline and closes the connection before emitting a completed-pass
event. The first pass is immediate; subsequent passes wait one minute after
completion. A full batch still waits, bounding work to at most 32 expired
observations per pass. There is no fixed reclamation latency for a backlog.

Dial, recovery, deadline, close or output failure terminates the command.
Recovery errors retain the completed count. systemd retries after five seconds,
with start rate limiting disabled so prolonged runtime unavailability does not
permanently suppress recovery. SIGTERM while idle exits successfully; interrupted
passes close their connection and report an error. No connection or hidden
goroutine is retained between passes.

The release graph now requires `runtime_image_maintenance_unit`, named
`runtime-image-maintenance-systemd-unit`, in addition to `node_agent_unit`.
Both units have distinct exact directive allowlists and package-bound commands.
Artifact inventory, byte budgets, canonical rebuild, nested bundle relocation,
tamper detection and output overwrite protection include the new unit.

The build plan, bundle, configuration schema and all three Vela release media
types move to version 3. Old version 2 bundles must be rebuilt and are rejected
by the new verifier. OCI manifest and release-descriptor `schemaVersion: 2`
remain unchanged. This also corrects an existing validator drift: the deployed
remediation unit's `StateDirectoryMode=0750` is now required and tested using the
actual checked-in unit.

## CPU evidence

The sandbox remains digest-pinned, network-disabled and isolated from the host
runtime socket. It uses Linux/arm64, Docker `28.3.2`, containerd `v2.3.1` revision
`64b425cf570b3b8dd1d4cc46da7c1fce65c6651a` and runc `1.4.2`:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

The host wrapper builds and mounts both the static Linux Node Agent command and
the Linux test binary. All **24 required top-level tests** passed, with no
selected skips and a verified zero container exit status. The final command
completed in **59.696 seconds**, including fixture build and teardown.

`TestRuntimeImageMaintenanceProcess` reuses the actual crash experiment:

1. A child observer creates a real lease, native view and mount activation, then
   is killed by `SIGKILL`. The fixture first validates the requested production
   one-hour TTL and substitutes six seconds only inside this test.
2. The parent verifies protection before expiry, forces an idle GC, waits past
   expiry, and verifies that the crash resources still exist.
3. A separate actual `vela-node-agent runtime-image-maintenance` process starts
   with deliberately unusable remediation environment settings. `setpriv`
   drops all capabilities and enables no-new-privileges before execution.
4. The command emits exactly one completed recovery. `/proc/<pid>/status`
   confirms `CapInh`, `CapPrm`, `CapEff`, `CapBnd` and `CapAmb` are all zero and
   `NoNewPrivs` is one. SIGTERM stops the idle process with exit status zero.
5. The parent requires absence of the lease, view, materialized mount path and
   corresponding kernel mount. A separate live view retains its exact bytes,
   and the original source image can still be observed.

Unit tests use virtual time to establish immediate execution, one-minute
repetition, fresh connections and independent deadlines. Fault cases cover
partial recovery, timeout despite a nil callback error, close/output errors,
cancellation and invalid configuration. An initial race report in the test's
shared observation slice was corrected with explicit synchronization; the
corrected Node command and release packages pass `-race`.

## Repository verification

- `go test -p 2 ./...`: PASS. The initial unconstrained full run, concurrent with
  other builds, hit the existing five-second CPU_MEDIA initialization deadline.
  Its isolated replay passed in 1.59 seconds, including CPU_MEDIA initialization
  in 0.33 seconds, and the complete bounded-concurrency run passed. No production
  timeout was changed; load sensitivity remains a test-environment limitation.
- `go test -race ./cmd/vela-node-agent ./internal/releasebundle ./cmd/vela-release-bundle`:
  PASS on macOS/arm64.
- The three PostgreSQL Catalog tests for atomic replay, nested release loading
  and invalid/mismatched bundle rejection: PASS in 13.435 seconds.
- `go vet ./...`: PASS.
- Full standard lint with `golangci-lint v2.13.1`: zero issues. Linux/arm64
  integration-tag lint for Node Agent and release command/library packages:
  zero issues.
- `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /dev/null ./cmd/vela-node-agent`:
  PASS. The CPU execution evidence above is arm64, not an amd64 execution claim.

## Remaining boundary

This validates the command, its library call, capability removal and static unit
contract. The sandbox does not run systemd as PID 1. Actual systemd restart
policy, mount/address-family restrictions, effective drop-ins, host-specific
configuration binding, daemon/host reboot and installed service enablement still
need deployment evidence. Root access to containerd's API remains privileged even
without Linux capabilities; exact cleanup ownership checks remain necessary.

The serving startup endpoint, effective Runtime configuration/message binding,
durable non-reusable incarnation ownership, overlayfs qualification and
independent retirement remain open. Image observations and maintenance events
grant no startup, execution, readiness, scratch reset or retirement authority.
`UNRESOLVED` and `LEGACY_UNKNOWN` remain recovery-only. PostgreSQL 94, Worker
journal 5, Runtime journal 6, Registry binding 1 and Production Gates **0/9** are
unchanged. No GPU test, production deployment, push or Launch Receipt was made.

The following [daemon restart increment](runtime-image-daemon-restart-evidence-2026-09-07.md)
tests real containerd replacement, fixes socket inode reuse, and expands the
mandatory Linux selection. The remaining statements above describe this
maintenance checkpoint; host reboot and actual systemd operation remain open.
