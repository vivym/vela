# Runtime executable provenance: image layers versus a live rootfs

This increment follows `7a66f03`. It tests the next dependency for Node startup
binding: deriving the expected executable file identity from an exact OCI image.
The repository already verifies stored/decoded layer hashes and observes the
kernel executable file. Neither alone establishes how image layers materialize
the file that a container executes.

## Result and implementation direction

The pinned `github.com/google/go-containerregistry v0.22.0` `mutate.Extract`
cannot serve as the executable-identity oracle. Two real containerd experiments
produce a different result from materializing its flattened tar. They are
counterexamples with valid, intentionally constructed CPU fixture layers, not
evidence of arbitrary malicious code or a production incident.

Let `D0` be the SHA-256 of the original static Go test executable and `D1` the
same file with the ASCII trailer `vela-image-upper-layer-v1`. Both files execute
the same Go helper. The independently expected results and actual observations
are:

| Image changeset | Read-only native snapshot and kernel executable | Flattened tar after extraction |
| --- | --- | --- |
| Regular file replaced by an upper layer | `D1` | `D1` |
| `.wh.probe` followed by a new `probe` in the same upper layer | `D1` | `probe` is missing |
| `bin/.wh..wh..opq` plus a new `bin/probe` | `D1`; lower `bin/removed` absent | `D1` |
| Lower `probe` hardlinks `payload`; upper layer replaces `payload` | `probe` remains `D0` | `probe` becomes `D1` |
| Lower `probe` symlinks relatively to `payload`; upper layer replaces `payload` | `D1` | `D1` |

In the whiteout case, the flattener's per-path map hides the new same-layer file.
In the hardlink case, it emits the surviving lower hardlink against the upper
replacement of its target. Replaying that flattened archive creates a new
hardlink relationship instead of retaining the original lower inode's content.
The opaque-directory and relative-symlink controls show why neither treating
every link as a hardlink nor treating every whiteout identically is correct.

The expected executable must therefore be read from the supported runtime's
actual materialized, read-only image snapshot, with its exact image/config/
ordered DiffID chain and snapshot lifetime authenticated. A production reader
must retain the view while measuring the file and close its mount activation,
snapshot and lease explicitly. Adding a caller-supplied executable hash or
reading an unvalidated merged tar would not close this dependency.

This is the selected implementation direction, not an implemented production
image reader or startup grant. It also does not establish that a running
container has no mounts/writes over the expected image path; actual process
configuration and executable/message binding remain separate requirements.

## Experiment construction

`TestRuntimeImageLayerExecutableIdentity` builds two-layer images through the
existing image library, writes an OCI layout, imports that layout into a private
containerd namespace, and verifies `Images.Get.Target.Digest` against the exact
constructed image manifest digest. It mounts by that digest reference through
the `native` snapshotter and requires `ST_RDONLY` on the resulting view.

The expected file digest is independently computed from the source helper plus
the optional trailer. Each case checks the mounted file's digest and size, then
starts an actual non-root namespace-init process from that image view. The old
fixture's `/probe` host-binary bind mount is removed. The kernel `exe` observation
must match the independently expected file bytes and size. The caller uses the
existing authenticated local channel and retained pidfd.

The process uses a test-specific OCI task specification with its Go helper
arguments, a `/proof` socket-directory mount, procfs and a private `/dev` tmpfs.
The image contains the required mount-point directories before its view becomes
read-only. No mount overrides the executable. This is native containerd/runc
content behavior, not a CRI/Kubernetes or production ModelRuntime launch test.
Only the `native` snapshotter is exercised here. Deployment snapshotters such as
`overlayfs` require their own conformance evidence before the production reader
can claim support for them.

The comparison independently feeds `mutate.Extract` to the sandbox's `tar`
utility, then hashes the resulting file. Only the fixed synthetic fixture entries
are extracted. The tests assert both known counterexamples and positive controls;
a library upgrade that changes either counterexample requires reassessment.

Initial fixture runs encountered image-name/namespace mismatches and missing
read-only-root mount points before reaching the comparison. The final path uses
the same private namespace for CLI and gRPC, preserves the OCI manifest identity,
and prepares mount points in the image. Those setup failures are not counted as
content-semantic evidence.

## Validation and cleanup

The expanded `TestRuntimeContainerdSandbox` requires all 16 selected top-level
tests to pass and rejects skips. It passes in `21.58s`, package `22.388s`; the new
five-scenario test takes `12.92s`. The ordinary wrapper cross-compiles a static
Linux/arm64 test binary with `CGO_ENABLED=0`. It uses the existing local CPU image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`,
containerd `v2.3.1` revision `64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`,
runc `1.4.2`, and Docker `28.3.2`. No dependency version changes are made.
Node and Node-command unit tests pass (`2.728s` / `1.239s`), as do host tagged vet
and Linux integration-tag Node lint (`0 issues`).

The sandbox uses `--pull never`, no network or host runtime socket, a private
cgroup namespace, two CPUs, 1 GiB memory and 256 PIDs. It is privileged for its
disposable daemon/mount experiment. This does not validate a production Node
privilege profile. No GPU, model weight or remote registry is used.

Cleanup stops tasks before removing image views. It unmounts the client view,
calls the containerd Mounts service to deactivate the exact temporary activation,
then removes its snapshot and lease. The private daemon tree, image archives and
outer container/test binary are removed; both pre-existing stopped Docker
containers remain. A successful `ctr images unmount` alone did not release the
Mount Manager activation in the pinned runtime and was insufficient cleanup.

Reproduce the complete CPU selection with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Final experiment output is `/tmp/vela-runtime-image-layers-devtmp.log`;
related package/lint output uses `/tmp/vela-runtime-image-layers-*.log`.
No new static-race or full-repository campaign is claimed for this test-only
increment. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1
and Production Gates `0/9` remain unchanged. Startup authorization, endpoint
assembly, independent retirement and normal durable restart remain unfinished.
