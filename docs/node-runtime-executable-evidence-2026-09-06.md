# Kernel executable file observation

This increment follows `800529a`. Node can now measure the executable file
referenced by its retained Runtime caller, including when the process's original
pathname has been replaced or deleted. A separate CPU counterexample establishes
why this measurement and the prior authenticated payload still cannot jointly
serve as backend startup authority.

## Observation contract

`RuntimeCaller.InspectExecutable` retains the same original pidfd and procfs
directory used by caller authentication. Under the caller mutex it checks the
live process, opens `.` through the retained `os.Root`, confirms procfs, and
opens only the fixed kernel `exe` magic link with `openat`. Following this one
kernel link deliberately leaves the `os.Root` boundary. It never reopens a
numeric `/proc/PID` path or resolves the replaceable executable pathname inside
the container's filesystem.

The opened descriptor must identify a nonempty regular file of at most 256 MiB.
SHA-256 is computed by streaming at most the observed size plus one byte. Both
the directory and executable descriptors remain close-on-exec. The implementation
checks file device/inode, size, mode, UID/GID, link count, modification time and
change time before and after reading, opens the current `exe` again, and requires
the same file identity. Access time is excluded because reads can update it.
It finally rechecks the original pidfd and process/namespace/credential identity.
Errors return a zero observation and release the temporary descriptors.

The method uses a five-second context and collection interval. Cancellation is
checked between content reads and before return. This is cooperative cancellation;
neither a blocked kernel file operation nor waiting for the caller mutex is
preempted by context cancellation.

`RuntimeExecutableObservation` reports the SHA-256, byte count, file identity and
metadata, process identity and collection interval. It does not claim that those
file bytes are present unchanged in executable memory, that the file is from an
approved release, or that its configuration is authentic. It does not attest
other mapped code, the dynamic loader, scripts, mounts or external writers.
Repeated observations do not exclude intervening ABA changes.
It requires permission to read the other UID's procfs executable link. These
tests use the privileged CPU sandbox, not a production LSM/privilege profile.

`ObservePlannedCaller` now includes this observation after its final Pod read,
and compares the process with its earlier CRI/native-task observation. Its local
output schema advances from 1 to 2. Registry binding schema 1, PostgreSQL 94,
Worker journal 5 and Runtime journal 6 are unchanged. No durable record,
Runtime factory gate or startup endpoint consumes this as a grant.

## CPU evidence

The direct caller test compares the observed digest and size to an independent
hash of the helper binary, repeats observation without descriptor growth, and
rejects canceled/missing contexts, exited processes and closed/nil/empty callers.
File tests reject a non-procfs lookup directory, a nonregular executable, empty
and oversized files; they also check canceled content reads and close-on-exec.

The pathname tests launch actual non-root processes from private executable
copies. Replacing or deleting that path leaves the original executable file
referenced by the running process. The observer returns the original digest and
inode with zero remaining links. A pathname-based reader would read the
replacement or fail to find the deleted path, and cannot satisfy these tests.

The exec counterexample replaces the path with another executable copy whose
file bytes include an added trailer, then asks the existing process to exec it.
The observer reports the new file digest and inode. The PID, retained original
pidfd, start ticks and authenticated original payload remain unchanged. Both
copies run the same test helper logic; this is a file-identity/lifetime experiment,
not arbitrary-code or loaded-memory attestation. It disproves using a later
executable measurement to authenticate an earlier message solely by PID/pidfd.

The existing real-containerd `planned-owner` case now measures its actual
non-root CRI container init. The digest and size must match an independent hash
of the binary placed in that synthetic image. Registry signatures and Kubernetes
Pod reads remain fixtures. The synthetic image is not the release image named
by the fixture plan, so passing does not prove approved-image conformance.

## Validation

The ordinary CPU/containerd wrapper passes in `8.91s`; it requires all 15 named
top-level tests to pass and rejects skips. The pathname/exec tests take `0.32s`
and the actual CRI test takes `2.93s`. Full repository unit tests, full vet/lint
and Linux integration-tag Node vet/lint pass. Linux lint runs a native host
linter with Linux package analysis, using:

```sh
go run -exec 'env GOOS=linux GOARCH=arm64' \
  github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1 \
  run --build-tags=integration ./internal/nodeagent
```

All 15 selected Linux static race tests also pass without skips or data races.
The actual CRI test takes `13.18s`, executable observation `1.21s`, pathname/exec
tests `0.67s` and planned-caller faults `1.39s`. The pinned CPU images and sandbox
limits are those in the [caller evidence](node-runtime-caller-evidence-2026-09-06.md#validation).
No GPU, network, host runtime socket, model weight or external H3 input is used.
The known static-glibc NSS linker warnings remain unrelated to these local
Unix-socket tests and do not establish DNS/user lookup compatibility.

Reproduce the ordinary selection with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

For the static race command, use the existing build recipe and this selection:

```text
^Test(RuntimeContainerdProcessEvidence|RuntimeCallerAuthenticatedMessage|RuntimeCallerRejectsInvalidMessages|RuntimeCallerDeadline|RuntimeCallerProcessParser|RuntimeCallerContainerCRI|RuntimeContainerCallerCorrelation|RuntimeContainerCallerRejectsNonInit|RuntimeLaunchPlanAuthenticatesCompleteConfiguration|RuntimeLaunchPlanRejectsUnboundHistory|RuntimeLaunchPlanPreservesMemberAndAUXTopology|RuntimePlannedCallerCorrelatesTrustedPod|RuntimeExecutableObservation|RuntimeExecutablePathAndExec|RuntimeExecutableFileBounds)$
```

Logs remain under `/tmp/vela-runtime-executable-*.log`. Task-created containers
and test binaries are removed after verification; the two pre-existing stopped
Docker containers are preserved.

## Required continuation

The approved executable bytes must still come from an independently authenticated
release/image content path. The process-to-message/configuration relationship
must survive the demonstrated exec boundary before a Node-issued non-reusable
incarnation can bind actual Registry journal ownership and the schema-6 startup
nonce. A configurable, caller-supplied hash or a matching Pod declaration would
not establish those facts.

Mutually authenticated Runtime/Node assembly, independent exact-owner retirement,
restart and lost-response recovery, durable unhealthy/receipt recovery, bounded
history reclamation, renewal write cost and Fleet durable activation remain open.
Production Gates remain `0/9`; overall Vela correctness and architecture acceptance
are not complete.
