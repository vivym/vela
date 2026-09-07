# Image observation recovery after volatile state loss

This increment follows `bc35374`. It adds a two-sandbox CPU experiment to the
mandatory containerd wrapper. Only test fixtures and evidence change; production
recovery behavior, permissions, lease timing and state formats are unchanged.

## Persistence boundary

The pinned containerd `v2.3.1` mount plugin stores both `mounts.db` and mount
targets under its state directory. Its snapshot, content and lease metadata
remain under the persistent containerd root. The primary implementation is
[`plugins/mount/manager.go`](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/plugins/mount/manager.go),
checked against the locally cached source for that exact version.

This is a materially different failure from the same-directory daemon restart
in the [previous experiment](runtime-image-daemon-restart-evidence-2026-09-07.md):

| Resource | After the original sandbox is destroyed |
| --- | --- |
| containerd process and all seed processes | Terminated with the sandbox |
| Private state directory, mount database and socket | Absent in the new sandbox |
| Old observation and control mounts | Absent from the new kernel mount view |
| Observation lease and native snapshot view | Retained under the persistent root |
| Non-expiring control lease and native view | Retained under the persistent root |
| Imported image content and committed snapshot | Retained under the persistent root |

## Experiment

The host wrapper builds the Linux test executable and actual Node Agent command,
then mounts the same two binaries read-only into both disposable sandboxes.
Both use the pinned CPU image, `--network none`, `--pull never`, private cgroup
namespaces, a 256-process limit, 1 GiB RAM and two CPUs. The experiment has no
GPU, network, host runtime socket, model weights or production data.

1. The seed sandbox starts its own pinned containerd with a shared persistent
   data root and a separate private `/run` state tree. It imports a minimal CPU
   image and creates a non-expiring control lease, native view and activation.
2. An observer child creates a distinct production-shaped observation lease,
   view and activation, acknowledges allocation and is killed by `SIGKILL`.
   The existing private fixture verifies the requested production one-hour TTL
   before substituting 15 seconds for the test. Production timing is unchanged.
3. The seed verifies those resources, the actual volatile `mounts.db`, and
   refusal to reclaim an unexpired lease. It atomically publishes a test-only
   receipt containing the exact namespace, resource keys, image identities and
   expected bytes, then remains live without returning through Go cleanup.
4. The host confirms that the seed container is running, sends `SIGKILL`,
   requires exit status 137 with `OOMKilled=false`, checks for prior test
   failures/skips and deletes the seed container before creating recovery.
   Only the containerd data root and test receipt are shared with recovery.
5. The fresh sandbox verifies absence of the old state tree and mounts before
   starting a new containerd. It requires the exact observation lease and view
   to remain, while both observation and control activation metadata are absent.
6. Recovery refuses to collect the unexpired observation. The control view is
   reactivated under its retained non-expiring lease. Explicit GC before expiry
   must preserve the observation; the daemon then remains idle past expiry.
7. The actual `runtime-image-maintenance` command, with all capabilities removed
   and `NoNewPrivs=1`, must report one recovery and stop cleanly on `SIGTERM`.
8. The observation lease/view/activation must be absent. Repeated recovery must
   return zero. The control lease, view and new activation metadata must remain,
   control bytes must match, and a fresh production image observation must
   return the original digest and size without importing or rebuilding it.

The first experiment rejected equal `/proc/self/ns/mnt` strings. Actual sandbox
destruction immediately reused `mnt:[4026532555]` in the new sandbox, showing
that this was an invalid test assumption. Namespace inode numbers are not
durable identities after the old namespace is destroyed. The corrected test
uses the verified container destruction/recreation and absence of old state
and mounts as evidence. Holding the old namespace open merely to force a new
number would preserve the very kernel state this experiment needs to lose.

## Verification

The environment is Linux/arm64 with Docker `28.3.2`, containerd `v2.3.1` revision
`64b425cf570b3b8dd1d4cc46da7c1fce65c6651a` and runc `1.4.2`. The sandbox image is
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`.

The normal wrapper now has two independently selectable subtests:
`runtime-contracts`, retaining all 33 required tests from the previous
checkpoint, and `volatile-state-reset`, requiring the deliberate seed death
and a zero-exit, non-skipped `TestRuntimeImageStateReset` recovery phase.
The seed is intentionally not a completed Go test and is never counted as PASS.

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

For only the two-sandbox experiment, use
`-run '^TestRuntimeContainerdSandbox$/^volatile-state-reset$'`.
The final complete wrapper passed in **107.966 seconds**: all 33 original tests
passed in the `runtime-contracts` subtest (88.74s), followed by the state-reset
subtest (16.28s). The recovery test itself passed in 14.51s. No selected test
was skipped; the original contract sandbox and recovery sandbox both exited
zero, while the seed's deliberate `SIGKILL` exit was verified separately.

Node Agent library/command tests pass. Integration-tag vet passes on macOS and
for Linux/arm64; Linux integration-tag Node Agent lint reports zero issues with
the pinned `golangci-lint v2.13.1`. The host-installed v2.8.0 cannot analyze this
repository's Go version; the working cross-platform lint invocation is:

```sh
go run -exec 'env GOOS=linux GOARCH=arm64 CGO_ENABLED=0' \
  github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1 \
  run --build-tags=integration ./internal/nodeagent ./cmd/vela-node-agent
```

No new full-repository or static-race campaign is claimed for this test-only
increment. All three disposable sandbox containers and their temporary data
were removed by the harness. The two pre-existing stopped PostgreSQL/Go-builder
containers remain untouched.

## Remaining scope

This proves recovery with persistent root retained and both volatile state and
the old sandbox's mount context discarded. It does not reboot the physical
host, change its boot ID, test filesystem crash consistency or power loss, run
systemd as PID 1, or exercise RKE2. It does not qualify loss of mount metadata
while kernel mounts survive, or death inside a mount/metadata transaction.

The state reset must not grant startup or retirement authority. Effective
Runtime configuration/message binding, durable incarnation ownership and
independent retirement remain open. `UNRESOLVED` and `LEGACY_UNKNOWN` remain
recovery-only. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry
binding 1, release bundle schema 3 and Production Gates **0/9** are unchanged.
