# Runtime CPU process containment and lifecycle gaps

This increment follows `e0b1bee`. It adds reproducible container experiments and
assertions for the existing Fleet PID namespace contract. It does not implement
durable backend incarnation recovery or change production Runtime behavior.
PostgreSQL schema 94, Worker/Runtime journals 5, Registry binding 1 and Production
Gates `0/9` remain unchanged.

## Observed behavior

All cases use the real `StartRuntimeServer`, Registry-bound execution journal,
default `NewProcessBackend` factory and stdio driver protocol. During model
initialization or Prepare, the CPU test driver launches a writer into its own
process group. The writer appends to a private file every 10 ms. That interval
is an experiment control, not a Runtime timing or throughput claim.

| Boundary | Retained pending executions | Writer after Runtime PID 1 exits | Writer after Runtime child exits under a surviving wrapper | Replacement drivers while the old writer lives |
| --- | ---: | --- | --- | ---: |
| Inside Initialize before acknowledgement | 0 | Stops | Continues | 2 |
| Initialized, idle before first Prepare | 0 | Stops | Continues | 2 |
| Accepted durable Prepare | 1 | Stops | Continues | 0 |
| Idle `Close()` returned, owner has not exited | 0 | Stops on subsequent PID 1 exit | Continues | 2 |

The first three cases request abrupt owner exit with code 72 only after the
external harness confirms writer activity and the intended journal/readiness
boundary. Initializing uses an actual journal opened before backend startup;
Initialize acknowledgement remains blocked and no serving endpoint has been
published. Idle requires successful server startup. Admitted requires accepted
Prepare through the private owner-checked UDS and one pending execution.

For the first three wrapper cases, a replacement Runtime reopens the original
journal only after the wrapper has reaped the original owner with exit 72. It
verifies that journal, obtains its test Registry binding, starts and closes
through the default process factory. The test confirms the original writer's
identity and continued file growth before and after replacement. No recorded
execution means both AUX driver commands can start. The one pending execution
correctly selects the existing process-free recovery endpoint instead.

The fourth case keeps the original owner alive after `RuntimeServer.Close()`
returns successfully. Its driver has exited and its journal lock is released,
but the escaped writer has no inherited stdout/stderr pipe that would expose
its lifetime to the driver's cleanup. A second container, with its own Runtime
PID 1 and a simultaneously distinct PID namespace, starts two drivers from the
same journal while the original writer continues. Thus a clean Close, released
journal lock and a new namespace together do not prove old-writer retirement.

These replacement attempts are deliberate test actions against the Runtime
API. They do not assert that the current Fleet scheduler authorizes duplicate
Pods or that a production container runtime restarts the same container while
its old PID 1 lives. They establish the missing evidence in Runtime recovery.

## Observation controls

- The harness compiles the current package tests for the image's Linux
  architecture and invokes only `TestRuntimeLifecycleProcessHelper`. The
  package's regular test run leaves that helper inactive.
- Each scenario creates a private Docker volume and bounded CPU-only containers.
  A short initialization container gives the empty volume mode `0700` and
  UID/GID 65534; it has only `CHOWN`. Runtime, wrapper and writer run as 65534
  with no capabilities, no network, no GPU/device mapping, read-only root and
  binary, `no-new-privileges`, a 128-process limit and 256 MiB memory limit.
- The static test binary replaces the image entrypoint. The base image supplies
  a container filesystem; its PostgreSQL service never starts. This is not a
  build or execution of the production H3 image.
- Owner, driver and writer observations include PID, parent PID, process group,
  process start ticks, kernel boot ID and PID namespace link. The harness checks
  the actual parent chain, separate writer group and common live namespace.
  Its exact container ID scopes these observations. Wrapper controls re-read
  the writer's `/proc` identity before and after replacement; file growth proves
  that the observed process remains an active writer.
- PID 1 cases require Docker to report exact exit 72, `exited`, not running and
  not OOM-killed. Three subsequent snapshots must retain the same writer bytes,
  with no cooperative writer-stop marker. Wrapper cases retain a live container,
  then use the writer's private stop gate and observe stable bytes before
  stopping the wrapper. Every created container and volume is removed by its
  exact returned ID, including on failed assertions.
- Original and replacement Runtimes share the same persistent epoch directory.
  Snapshots require both replacement epochs and strict increases for every
  previously allocated endpoint; the initialization crash has allocated only
  its first endpoint. Restart never resets the epoch fixture.
- Snapshots verify the journal's version, pending history and the startup
  boundary. Replacement must leave execution-journal bytes unchanged. None of
  the test markers or process observations becomes a Runtime drain checkpoint.
- One test-harness issue was corrected: collecting owner output through
  `CombinedOutput` waits on pipes retained by an initializing driver even after
  the owner is reaped. A regular private log file now separates these lifetimes.
  A repeatedly truncated ready marker also raced archive snapshots; the writer
  now publishes that marker once.

During the observed sequential experiments, the kernel reused the same PID
namespace inode after prior containers exited. Simultaneously live original and
replacement namespaces were different. A namespace inode, even alongside a boot
ID, must not serve as a durable globally unique backend incarnation.

## Reproduction and validation

Local environment: Docker context `desktop-linux`, Engine `28.3.2`, Linux
`6.10.14-linuxkit`, architecture `aarch64`. The existing `linux/arm64` CPU image
has local immutable image ID
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

```sh
VELA_TEST_RUNTIME_CONTAINER_IMAGE=sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  go test -tags=integration ./internal/modelruntime \
  -run '^TestRuntimePIDNamespaceContainsBackendWriters$' -count=3 -v -timeout=3m
```

The integration test is explicitly enabled by a pre-existing digest-pinned
Linux image; it never pulls an image. It can compile for `linux/amd64` or
`linux/arm64`, but this evidence is only the actual arm64 run. Ordinary test
invocations do not silently require Docker.

Validation after correcting the harness races:

- The command above passes all eight scenarios in three consecutive runs:
  24 scenario results, `29.937s` total package time, with persistent epoch
  advancement checked for every replacement.
- `go test ./internal/modelruntime ./internal/fleetcontroller
  ./internal/releasebundle -count=1 -timeout=3m` passes: Runtime `28.261s`, Fleet
  `2.094s`, release bundle `2.530s`.
- Pinned `golangci-lint v2.13.1` with `--build-tags=integration` over Runtime and
  Fleet reports zero issues, including the container harness.
- Diff checks pass. Container/volume cleanup succeeds for every scenario.
  Existing source/generated contracts did not change; this increment did not
  repeat the previous commit's full repository race or PostgreSQL suites.

## Architecture consequence

The release validator requires the exact `vela-model-runtime` entrypoint with
empty Cmd. The Fleet actuator emits `HostPID=false`, explicit
`ShareProcessNamespace=false` and no ModelRuntime command/argument override,
with an unprivileged container security context. Its full normalized Pod
comparison rejects content drift. The materialization test now explicitly
checks those namespace and command constraints alongside the existing OCI
entrypoint validation tests.

Those constraints provide a useful tested CPU containment boundary when the
actual Runtime PID 1 exits. Complete recovery still requires a durable
backend/container incarnation before model startup and independently verified
quiescence for that exact old incarnation before replacement. The updated
[bootstrap lifecycle contract](specs/0053-worker-bootstrap-lifecycle.md) records
the required identity, shutdown and recovery boundaries. Backend lifecycle must
remain distinct from per-Stage drain and device retirement.

The experiment does not establish GPU/device quiescence, external service or
writer containment, production Kubernetes/CRI behavior, durable unhealthy-worker
recovery, sealed receipt recovery, history reclamation, renewal write cost or
Fleet durable activation. No schema update, remote deployment, push, GPU use or
Launch Receipt occurred. The overall correctness and architecture goal remains
active.
