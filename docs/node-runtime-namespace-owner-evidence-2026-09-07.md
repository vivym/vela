# Independent Runtime namespace-owner lifetime

This increment follows `e0f5f0f` on `feature/vela-mock-hardening`. It retains an
independent kernel process handle after live CRI/native-task/PID-1 correlation.
It does not implement durable startup authorization or permit a Runtime journal
reset. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1,
release bundle 3 and Production Gates `0/9` remain unchanged.

## Lifetime contract

`RuntimeContainerObserver.RetainNamespaceOwner` first runs the existing bounded
`ObserveCaller` correlation against a target from trusted Node inventory. The
authenticated process must be the running native task and PID 1 in a nested
namespace under the supported containerd `v2.3.1` contract. While holding the
original caller's mutex, it repeats the original process inspection and takes
its own `F_DUPFD_CLOEXEC` copy of the already authenticated pidfd. It never opens
a replacement process by numeric PID.

The opaque `RuntimeNamespaceOwner` owns that descriptor independently of the
request socket, `RuntimeCaller` and CRI observer. It verifies pidfs type,
retained device/inode identity, close-on-exec and the current host boot. The
kernel descriptor must remain readable as an exit event before `ObserveExit`
can return a `RuntimeNamespaceExitObservation`. Live or paused processes return
`ErrRuntimeNamespaceOwnerLive`. A closed, unavailable or invalid handle returns
`ErrRuntimeNamespaceOwnerLost`. Canceled observations return no result.

Exit observation performs no CRI lookup, task lookup or numeric-PID search.
Missing metadata and newly created processes cannot change what the retained
descriptor names. The first successful observation is immutable across repeated
and concurrent calls. `ObservedAt` means the first observed kernel exit event,
not an independently measured timestamp of the process's actual exit.

`Close` releases only the observation handle. It does not signal the process,
retire an incarnation or manufacture exit evidence. An observation value cannot
reconstruct the opaque handle. If a Node process loses that handle before
independently retaining a valid durable result, absent process/container
metadata remains insufficient to recover an exit decision.

## CPU coverage

Three main tests exercise real non-root Linux processes with CRI/task metadata
fixtures:

- `TestRuntimeNamespaceOwnerIndependentLifetime` retains a real PID-1 process,
  verifies that live and SIGSTOP-paused owners do not yield exit, then makes the
  native-task fixture return `NotFound`. Closing the original caller, request
  socket and CRI observer still yields no exit. Only actual SIGKILL of that
  retained process produces the stable exact-owner observation. Descriptor
  accounting and eight concurrent repeated observations are checked.
- `TestRuntimeNamespaceOwnerRejectsUnprovenOwner` rejects non-init callers,
  closed callers, canceled retention and absent native tasks. Closing a live
  owner's handle does not produce an exit observation; rejection leaks no
  retained descriptor.
- `TestRuntimeNamespaceOwnerKeepsGenerationsDistinct` starts two different live
  PID-1 processes and changes the task fixture to name the second under the same
  container target. Both remain live until the first actually exits; its exit
  observation still names the first while the second remains unretired. This
  case changes a metadata fixture, not the actual containerd task registry.

The existing real `TestRuntimeCallerContainerCRI` also retains the owner in its
direct-init and Registry/Pod-fixture modes. After actual process exit and actual
CRI `RemoveContainer`, closing the original caller still leaves the independent
handle able to report the original exit. Wrapper and shared-PID callers retain
their existing rejection. The Registry/Pod data and image remain CPU fixtures;
this does not establish release-image or live Kubernetes conformance.

Validation passed on Docker `28.3.2`, Linux/arm64, containerd `v2.3.1` revision
`64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`, and runc `1.4.2`:

- Complete CPU wrapper: **152.083 seconds**, all **47 mandatory main tests**
  and both volatile-state-reset scenarios passed without skips.
- Final static Linux race binary: all **9 selected main tests** passed without
  skips or race reports, including the three new owner tests, real CRI callers,
  both container-caller correlation tests, authenticated messages, rejected
  messages and actual pidfd descriptor exhaustion. The real CRI portion took
  **10.30 seconds**. This final binary additionally waits for the kernel's
  actual `State: T (stopped)` before checking the paused owner's non-exit.
- Linux/arm64 integration-tag vet and pinned golangci-lint `v2.13.1` passed for
  the Node library and command with zero lint issues. The working diff passed
  whitespace checks.

The full wrapper command is:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Race compilation used Go `1.26.7` from the existing builder image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`.
The static build and isolated sandbox procedure is documented in the
[caller evidence](node-runtime-caller-evidence-2026-09-06.md#validation). This
run selected:

```text
^Test(RuntimeNamespaceOwnerIndependentLifetime|RuntimeNamespaceOwnerRejectsUnprovenOwner|RuntimeNamespaceOwnerKeepsGenerationsDistinct|RuntimeCallerContainerCRI|RuntimeContainerCallerCorrelation|RuntimeContainerCallerRejectsNonInit|RuntimeCallerAuthenticatedMessage|RuntimeCallerPIDFDExhaustion|RuntimeCallerRejectsInvalidMessages)$
```

The builder mounted the repository and module cache read-only with networking
disabled. Build outputs and the copied warm Go build cache stayed in a
task-owned temporary directory. The previously documented static glibc NSS
warnings remain test-build warnings, not production linking qualification.

Retained output SHA-256 values:

```text
full-cpu.log   e6a648a72fe7a5d98e3a882da2c383813552c1c760c2e93e767a1735e7ab1e5c
race-tests.log cd223cf950b9e41b21dda697184eaad02b5c1126e7633ab31c072a869678e9b5
```

## Remaining startup and retirement binding

The handle establishes an independently retained process lifetime. It is not a
persisted backend/container incarnation, a proof of effective launch
configuration, a Stage drain receipt, GPU quiescence or permission to release a
DeviceSet. No backend factory or journal transition consumes this observation
as authority.

The next integration must retain the owner before the first factory, atomically
associate it with the actual held Runtime journal, Registry pair and startup
nonce, and persist the Node's independent operation before replying. Restart and
lost-response behavior must recover that operation without issuing duplicate
factory permission. Replacing a backend requires separately validated retirement
of that exact recorded owner, execution drain and the required device evidence.
Node restart/handle loss, external writers and physical GPU completion remain
separate obligations. `UNRESOLVED` and `LEGACY_UNKNOWN` journals still reopen
only recovery endpoints.
