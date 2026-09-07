# Daemon-bound kernel mount observation

This increment follows `ffabc8d`. Its durable image cleanup journal remains
valid, but checking `/proc/self/mountinfo` was insufficient when the maintenance
process had a different mount view from containerd.

## Reproduced failure

The CPU fixture leaves an expired image-observation mount busy with a real open
file. A failed recovery removes containerd's activation metadata while retaining
the kernel mount and the observation's lease/view journal. A new observer
process starts in `CLONE_NEWNS`, makes that clone recursively private and
detaches only its cloned observation mount. The original daemon namespace and
the holder's file descriptor remain alive.

Before the fix, the private observer invoked the production recovery API and
returned **`1, nil`**. It deleted the durable view and lease even though
containerd logged `device or resource busy` for the original mount. The test
failed with `private mount view incorrectly completed daemon recovery: 1 <nil>`.
An initial two-second fixture deadline only exercised a timeout; it was replaced
with the actual 30-second recovery bound before recording this counterexample.
Private detachment is test-only and never appears in production cleanup code.

## Correctness change

The image observer now authenticates the root Unix socket creator on the same
connection used for its containerd RPCs. Linux `SO_PEERPIDFD` supplies a retained,
close-on-exec process handle; a caller-supplied PID is never accepted. The
existing socket inode and trusted-directory checks remain in force. Reconnection
must still resolve to the original live process. Process death, replacement,
missing kernel support and failed authentication cannot produce mount evidence.

The observer reads `/proc/<authenticated-peer-pid>/mountinfo` with the existing
`prometheus/procfs` parser and checks the pinned process before and after that
read. PID reuse therefore cannot turn a replacement process's mount table into
a successful observation. The journal's exact path is compared using the same
kernel-escaping rules as the previous increment. The reader serializes reads,
authentication and close, retains one pidfd across same-process reconnects, and
releases it on close or failed setup.

Image cleanup checks the original peer before mutation. Kernel mount absence
comes from that daemon's mount view, even when the maintenance process has a
private view. A busy result retains the view/lease; successful unmount permits
their ordered removal. The read-only CRI observer does not acquire this extra
pidfd or change its kernel requirements. The additional authentication hook is
used only when constructing the image observer.

The independent maintenance service keeps its existing empty capability set,
`NoNewPrivileges=true` and systemd filesystem restrictions. No `setns`, direct
unmount, caller-selected `/proc` path, service permission, lease protocol or
release schema change is introduced.

## Validation

`TestRuntimeImageRecoveryMountNamespace` first proves that the private mount
view cannot see the original observation mount. A child then removes all
capabilities through `setpriv`, verifies every capability set is zero and
`NoNewPrivs=1`, and calls the production image recovery API. It must authenticate
containerd successfully, return zero completed recoveries with the actual busy
kernel-mount error, and finish without a timeout. The original namespace still
has its mount and its exact view/lease journal.

After the holder closes its file, a second capability-free private observer
must complete exactly one recovery. The original observer then returns zero.
The independent control mount and source image remain valid. The fixture also
kills a pinned daemon with `SIGKILL`: an existing reader cannot report mount
absence, and repeated authentication of the dead socket creator neither leaks
pidfds nor closes unrelated descriptors.

`TestRuntimeImageMountReaderLifetime` checks same-process reconnects, one retained
pidfd, concurrent mount reads, idempotent close, descriptor release, and rejection
of reads or authentication after close. The complete ordinary wrapper now
requires **39 main tests** plus both volatile-state-reset scenarios.

The final-source ordinary wrapper passed in **152.049 seconds**, with all 39
main tests and both state-reset scenarios passing without skips. The main
contract subtest took 115.91s; recorded/unresolved state-reset subtests took
16.26s and 17.01s. The private-mount/dead-peer test passed in 16.88s.

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Full repository `go test -p 2 ./...`, `go vet ./...` and pinned
`golangci-lint v2.13.1` pass with zero lint issues. Final Linux/arm64
integration-tag Node library/command vet and lint also pass with zero issues;
the final Linux/amd64 Node command builds successfully.

Both final Linux/arm64 race binaries were built statically with builder
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`
and `-race -ldflags '-linkmode external -extldflags=-static' -p=2`. They ran in
the same disposable sandbox image, mounted read-only, with no network, two
CPUs, 1 GiB memory, 256 processes and a 180-second test deadline. All **38 main
tests** other than the image-layer campaign passed together; the new
private-mount/dead-peer test passed under race in 20.95s. The five image-layer
scenarios ran separately to bound their larger instrumented executable payloads:

| Race image scenario | Reported subtest time | Result |
| --- | --- | --- |
| `regular-overlay` | 15.49s | PASS |
| `whiteout-recreate` | 16.53s | PASS |
| `opaque-directory` | 16.61s | PASS |
| `hardlink-copy-up` | 16.73s | PASS |
| `relative-symlink` | 15.01s | PASS |

All six race runs exited zero with no selected skips or race reports. The
state-reset scenarios ran in the ordinary wrapper; no state-reset race claim
is made. Static-glibc NSS warnings did not prevent successful builds, but these
test binaries do not qualify production static linking.

The pinned environment remains Docker `28.3.2`, Linux/arm64, containerd `v2.3.1`
revision `64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`, runc `1.4.2` and sandbox image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`.
Sandboxes use no network, GPU, host runtime socket, model weights or production
data. All fault actuation is inside disposable CPU fixtures.

## Remaining scope

This qualifies cleanup against the pinned daemon's actual mount table. The
supported daemon owns its listener and performs mounts in that process's mount
view. Socket activation, listener delegation, daemon mount-namespace replacement
and surviving bind copies in other namespaces are not qualified. A pidfd pins a
process, not a durable mount-namespace history. Recovery after arbitrary daemon
state or namespace loss still needs independently trusted containment evidence.
Image byte inspection still requires visibility of the daemon's image paths;
this cleanup change does not authenticate an alternate private rootfs.

The experiment does not run systemd as PID 1, reboot a host, exercise production
RKE2, or prove recovery from power loss or death inside a mount transaction.
Unrecorded same-boot activation loss remains unresolved, and a persistent bad
record can still stop a maintenance pass. Effective Runtime configuration/message
binding, durable incarnation ownership and independent retirement remain open.
PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1, release
bundle schema 3, image cleanup lease protocol 2 and Production Gates **0/9**
remain unchanged.
