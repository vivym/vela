# Containerd process identity experiment

This increment follows `6e2f9c8`. It tests real containerd task/process behavior
inside a disposable local Linux CPU container. It establishes prerequisites for
the Node startup binding; it does not implement that binding or authorize a
Runtime restart. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry
binding 1 and Production Gates `0/9` remain unchanged.

## Pinned implementation

The existing local Linux arm64 image is
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`.
Its actual binaries report:

- containerd `v2.3.1`, revision `64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`;
- runc `1.4.2`, commit `v1.4.2-0-gc241c0b`, OCI spec `1.3.0`, libseccomp `2.6.0`;
- Docker Engine `28.3.2` is the outer local sandbox host.

The test rejects a different containerd version/revision. Its native API module
is `github.com/containerd/containerd/api v1.11.1` and its typed OCI encoder is
`github.com/opencontainers/runtime-spec v1.3.0`, matching containerd's pinned
dependencies. Their checksums and the API's `ttrpc` dependency are in `go.sum`.
The containerd daemon module is downloaded only to inspect primary sources; it
is not added to Vela's dependencies or production command imports.

Primary-source checks use the exact running revision:

- [Task creation and observation](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/plugins/services/tasks/local.go)
  reads container metadata at `Tasks.Create`, passes its spec into task creation,
  and queries the actual task/process state at `Tasks.Get`. The empty `ExecID`
  selects the task init process, not necessarily its PID namespace init.
- [Container metadata update](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/core/metadata/containers.go)
  permits the `spec` field to change while preserving `CreatedAt`.
- [CRI verbose status](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/internal/cri/server/container_status.go)
  obtains `runtimeSpec` through `container.Container.Spec(ctx)`. That metadata
  read is not an immutable attestation of the task's original OCI configuration.
- [CRI start state guard](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/internal/cri/server/container_start.go)
  permits start only from `CONTAINER_CREATED`. The native task recreation below
  therefore does not demonstrate that CRI or kubelet restarts an exited
  container using its existing ID.

## Experiment

`TestRuntimeContainerdSandbox` cross-compiles the Linux test helper and starts
one exact-ID Docker container. Inside it, `TestRuntimeContainerdProcessEvidence`
starts a new containerd daemon with private root, state, Unix socket and metadata
namespace. CRI plugins are disabled for this native task experiment. Each child
uses a new empty read-only rootfs, a read-only static test executable, its own
mount/network/IPC/UTS namespaces, UID/GID `65532`, empty capabilities and
`NoNewPrivileges=true`. A read-only bind of a synthetic Unix socket directory
allows the child to connect to the parent observer.

The parent reads `SO_PEERCRED` from the accepted connection, opens a `pidfd` for
that live process, and reads its host-view `/proc` start ticks, `NSpid` and PID
namespace. The helper stays alive until the parent sends its exit command.
These controlled process-lifetime conditions are part of the fixture, not a
production race/replay or descriptor-delegation proof.

The subsequent [authenticated caller implementation](node-runtime-caller-evidence-2026-09-06.md)
replaces that original numeric `pidfd_open` fixture path with a challenge-bound
seqpacket exchange using both connection and per-message kernel pidfds. The
current reproduction also exercises inherited-connection rejection; the
measurements below describe the original `5adea05` experiment.

| Case | Actual result | Consequence |
| --- | --- | --- |
| Direct helper is namespace PID 1 | Task PID equals socket peer PID; host `NSpid` ends in `1`; its pidfd stays live | Positive process-correlation control |
| Helper runs below a wrapper | Socket peer PID differs from task init PID; its `NSpid` does not end in `1` | Matching UID and container membership cannot establish the lifetime owner |
| Task joins the first task's PID namespace | Socket peer equals task PID, but `NSpid` does not end in `1` | Task init identity alone cannot establish namespace-init identity |
| Update metadata `spec` while task runs | Two identical subsequent reads return the changed argv/PID configuration while the original process/pidfd stays live | Repeated metadata reads do not attest original running configuration |
| Exit and delete task, then create/start another under the same container record | Container ID, `CreatedAt` and restored spec remain the same; a different live process replaces the exited one | Container metadata identity alone is not a unique backend process incarnation |

The task-recreation case keeps the original process handle open and confirms it
still reports exit while the replacement handle is live. It also records the
kernel PID/start-tick pairs. PID numbers, namespace inode numbers and ticks are
diagnostics in this report, not portable durable identity credentials.

The outer container requires `--privileged` to create the nested runc namespaces
and cgroups. It has a private cgroup namespace, no network, two CPU quota, 1 GiB
memory and a 256-process limit. The only host bind is the read-only generated
test executable. There is no host container-runtime socket, host PID namespace,
project data, model weight or remote system access. No GPU workload or GPU
validation runs. The outer helper removes only the exact container it created,
including on failure, and the test framework removes the generated binary.

## Reproduction and result

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

The measured native experiment passes without skips in `1.37s`; the outer
build/run/cleanup test passes in `3.04s` (`3.724s` package time). The generated
processes make no Vela Registry, Runtime journal, Stage or device-state changes.
This is a real containerd/native-API experiment using synthetic CPU callers.
It is not the earlier CRI protocol mock, an actual Vela Runtime startup, or a
real-containerd CRI validation of `inspect-runtime-container`.

Full repository unit tests and `go vet ./...` pass. Standard full lint and the
Node/command integration-tag lint pass using `golangci-lint v2.13.1` built with
the repository Go toolchain. The relevant tagged lint is also run with
`GOOS=linux CGO_ENABLED=0`, so it includes the Linux-only experiment rather than
only the outer Darwin runner; Linux tagged vet passes as well. This does not
claim a clean full-repository integration-tag lint sweep or Linux race coverage.

## Binding requirements

The next Node adapter must correlate an authenticated caller with the expected
container's actual live task, prove the supported namespace-init boundary, and
retain a kernel process handle while binding a Node-issued, non-reusable
incarnation to the Registry journal pair and schema-6 startup nonce. Matching
container ID/creation time, task PID or namespace inode individually is
insufficient. The privileged native API's task-recreation capability must be
excluded by the deployment contract or accounted for by the lifetime protocol.

Verified configuration must come from a supported trusted launch/configuration
path and be checked against effective containment. Treat neither
`Containers.Get.spec` nor CRI verbose `runtimeSpec` as an immutable launch
receipt. This experiment does not establish complete mount, executable, hook,
runtime-option, external-writer or device containment.

Independent exact-owner termination and durable replay-safe retirement remain
unimplemented. Lost responses, Node/agent restart, metadata deletion, PID reuse
and recovery without the original process handle need explicit fail-closed
rules. No case here clears `UNRESOLVED` or `LEGACY_UNKNOWN`; durable Runtime
starts after the first backend attempt remain recovery-only. Stage execution
drain and GPU/device release continue to require their own evidence.
