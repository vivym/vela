# Runtime caller and actual CRI task correlation

This increment follows `d40d975`. The Linux Node library now correlates a
kernel-authenticated live Runtime caller with the expected actual CRI container,
native containerd task and PID namespace init. The same implementation is tested
against real containerd with CRI enabled in a disposable local CPU sandbox.
It produces an observation, not a Registry startup grant or retirement receipt.

PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1 and
Production Gates `0/9` remain unchanged. No endpoint or Runtime factory consumes
this observation yet; durable Runtime restarts remain recovery-only after the
first backend startup intent.

## Library boundary

`RuntimeContainerObserver.ObserveCaller` requires an opaque `RuntimeCaller`
returned by the existing challenge-bound seqpacket receiver and an exact
`RuntimeContainerTarget` from trusted Node/Fleet inventory. Caller-supplied PID,
UID, namespace, container ID or a serialized observation cannot reconstruct the
retained kernel process handle or establish trusted target inventory.

The adapter uses CRI and native `Tasks.Get` over the same root-authenticated,
identity-pinned local socket connection. Its narrow task reader exposes only
`Get`. It replaces any inherited `containerd-namespace` header with exactly
`k8s.io`, and leaves `ExecID` empty to select the task init. The supported runtime
is currently exactly `containerd` version `v2.3.1`; this is a compatibility guard,
not a cryptographic attestation of the daemon binary.

Within one ten-second context bound, it:

1. Inspects the retained original process, requiring namespace PID 1 and PID
   namespace depth of at least two.
2. Reads the exact CRI container/sandbox/Pod identity through the existing
   repeated-observation adapter, requiring a running container, ready sandbox,
   supported runtime and matching kernel boot ID.
3. Reads the native running task, requiring its exact container ID and host PID
   to match the authenticated live sender, with no actual exit timestamp/status.
4. Repeats CRI, native task and pinned-process observations, rejecting visible
   identity/state changes, lost/closed handles, cancellation, connection identity
   loss or inconsistent time bounds.

Running native tasks in the tested daemon can encode Go's zero time in the
protobuf `ExitedAt` field. The adapter accepts either a missing timestamp or a
valid encoded Go zero time. It rejects malformed protobuf timestamps and actual
exit times. Treating every non-nil timestamp as an exit would reject the real
running CRI positive control.

The result retains the collection interval. These sequential reads are not an
atomic snapshot or a process-lifetime lock. They do not attest executable,
mounts, hooks, external writers, effective launch configuration or device
quiescence. In particular, the earlier native-container experiment's mutable
OCI metadata and task-recreation limitations still apply. The original pidfd
remains the live caller identity; container metadata does not replace it.

## Real CRI evidence

`TestRuntimeCallerContainerCRI` starts a private containerd daemon with CRI
runtime/image plugins enabled and the native snapshotter. It generates two
local CPU fixture images from the test executable, preserves the image RootFS
diff IDs, and imports them through `ctr images import --local`. There are no
image pulls, network connections, weights or GPU workloads.

The fixture runs as PID 1 in the outer Docker container's private cgroup v2
namespace. Before delegating domain controllers, it verifies that the namespace
root contains only itself, moves itself into `/vela-cpu-observer`, and enables
`cpu`, `memory` and `pids` for the separate Pod cgroups. It changes no host
cgroup or existing container-runtime state. Each actual CRI Runtime uses a
non-root UID/GID, dropped capabilities, `NoNewPrivs`, read-only rootfs and a
read-only directory containing the test socket.

| Actual CRI case | Result |
| --- | --- |
| Created container before start | Exact observation reports `CONTAINER_CREATED` |
| Direct Runtime is container/task/PID namespace init | Combined observation succeeds with exact Pod/container/process identities |
| Runtime beneath a surviving wrapper | Combined observation rejects |
| Runtime container shares the sandbox PID namespace | Combined observation rejects although Runtime is its own task init |
| Wrong expected Pod UID | No combined observation |
| Original caller exits | CRI reports exit; retained caller cannot yield a live combined observation |
| Start the exited CRI container under the same ID | Real CRI rejects the restart |
| Remove the exited container | Missing metadata yields no container observation |

The restart result confirms CRI's pinned start guard. It does not contradict
native containerd's ability to recreate tasks under one container record, and
does not independently authorize Vela journal retirement.

The outer sandbox is Linux `6.10.14-linuxkit`, arm64, Docker `28.3.2`, with
containerd `v2.3.1 / 64b425cf570b3b8dd1d4cc46da7c1fce65c6651a` and runc `1.4.2`.
The exact CPU image is
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`.
The daemon test checks its version and revision. There is no host runtime socket,
host PID namespace, production cluster access or physical GPU evidence.

## Fault coverage and verification

The correlation suite combines actual non-root namespace-init processes with
CRI/native protocol mocks. Its 17 scenarios cover both supported exit-time
encodings, wrong task ID/PID, paused/unknown task state, real/malformed exit
times, absent/lost/changing tasks, changing CRI data, changed boot identity,
unsupported runtime version, caller/observer closure and cancellation. A
separate real-process case rejects a caller in the observer's PID namespace.
Every native mock request checks that an inherited namespace header was replaced
and that no exec process was requested.

Full repository unit tests and vet, Node/command race tests, standard full lint,
and Linux integration-tag Node/command lint/vet pass with Go `1.26.7` and
`golangci-lint v2.13.1`. The ordinary complete CPU sandbox selection passes
without skips after the final test cleanup repair (`5.10s` outer test).
This does not claim full-repository integration-tag lint or database integration
coverage for this Node-only increment.

The first complete Linux static race run exposed a fixture cleanup error:
the inherited-connection rejection test killed the opener before it could reap
its child. The race binary's exit delay kept that child visible when the CRI
fixture checked its private cgroup root. Running only CRI passed; running just
the delegated-sender case followed by CRI reproduced the failure. The rejection
test now closes the connection and waits for the opener to reap its child before
ordinary cleanup. The strict cgroup check is retained.

After the repair, both the minimal reproducer and the original full Linux
static race selection pass. All eight selected top-level tests execute without
skips or race reports: the actual CRI portion takes `7.22s`, the correlation
fault suite `0.57s`, and the earlier native-containerd portion `6.50s`.

## Reproduction

The ordinary CPU command includes the authenticated-caller, protocol-fault,
actual CRI and earlier native-containerd experiments:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

For Linux race validation, use the static build from the
[authenticated-caller report](node-runtime-caller-evidence-2026-09-06.md#validation).
Run that binary as the outer private sandbox's PID 1, with the same flags/image
and this expanded selection:

```sh
'-test.run=^Test(RuntimeContainerdProcessEvidence|RuntimeCallerAuthenticatedMessage|RuntimeCallerRejectsInvalidMessages|RuntimeCallerDeadline|RuntimeCallerProcessParser|RuntimeCallerContainerCRI|RuntimeContainerCallerCorrelation|RuntimeContainerCallerRejectsNonInit)$'
```

The statically linked race binary is required for the nested empty rootfs.
Its known glibc NSS linker warnings do not constitute validation of DNS or user
database lookup paths. A dynamically linked helper cannot be substituted without
changing the fixture. The build uses the existing exact Go CPU image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`,
read-only source/module-cache mounts, a task-owned output directory and no
network. Test-created containers and generated binaries are removed after use.

## Remaining work

The next startup layer must authenticate effective launch configuration from a
supported trusted launch path, authenticate Node to the Runtime client, and
durably bind the retained exact process incarnation to the Registry journal
pair and schema-6 startup nonce before backend factory dispatch. Matching CRI
image references, mutable OCI metadata or a Runtime-declared launch digest is
insufficient. No observation JSON may substitute for that grant.

Independent exact-owner retirement must handle original-process exit, lost
responses, Node restart and metadata deletion without inferring quiescence from
absence. `UNRESOLVED` and `LEGACY_UNKNOWN` have no reset path. Durable unhealthy
and receipt recovery, bounded history reclamation, renewal write cost and Fleet
durable activation also remain open. No Production Gate is advanced here.
