# Production launcher smoke evidence (2026-09-12)

This is a production-helper protocol smoke with synthetic Pod/manifest inputs. It
is not a Node/Fleet/ModelRuntime Launch Receipt and does not advance
`Production Gates`.
It is historical three-descriptor evidence; the current four-descriptor protocol is recorded in
[`runtime-startup-validation-matrix-2026-09-13-v2`](../runtime-startup-validation-matrix-2026-09-13-v2/).

## Successful run

- Host: `marslab@100.111.196.116`
- OS/kernel: Ubuntu 24.04, `6.8.0-137-generic`
- CRI socket: `/run/k3s/containerd/containerd.sock`
- Launcher: `/usr/local/bin/vela-runtime-launcher` (source-matched fixed build)
- Images: digest-pinned `busybox` and `mirrored-pause`, imported into the RKE2
  `k8s.io` namespace and queried through the same CRI socket
- Result: `passed=true`, `handoff_verified=true`, `observer_handshake_verified=true`, `cleanup_verified=true`
- Elapsed time: `6.526s`
- Receipt: [`runtime-startup-production-launcher-smoke-2026-09-12.json`](runtime-startup-production-launcher-smoke-2026-09-12.json)

The run created one PodSandbox containing distinct `model-runtime` and
`stage-worker-agent` containers, received three descriptors (Worker pidfd,
observer pidfd, observer endpoint), checked target identity and running CRI
state, validated pidfd/socket descriptor types and receiver-side `FD_CLOEXEC`, completed the
attached observer custody handshake, then closed the control channel. The launcher removed both containers and
the sandbox; the post-run CRI inventory contained no matching objects.

## Bugs found and fixed during the smoke

1. The temporary harness encoded `[32]byte` Pod digests as base64 instead of a
   JSON array of 32 integers.
2. Docker and RKE2 containerd image stores are separate; validation images must
   be imported/tagged in the CRI namespace or a reachable digest registry.
3. The RKE2 systemd cgroup driver requires a valid `CgroupParent`; the launcher
   now defaults to `kubepods.slice` and allows an explicit deployment override.
4. The production container command now explicitly dispatches the pidfd-offer
   wrapper (`--vela-runtime-pidfd-offer`).
5. The offer listener now enables `SO_PASSCRED` and `SO_PASSPIDFD`, and accepts
   cancellation/deadlines without a fixed 20-second wait.
6. Created container IDs are retained before `StartContainer`, so start or offer
   failure cannot leave an untracked container behind. Startup operations have a
   bounded, configurable timeout.

An earlier attempt also observed `error reading from server: EOF` while RKE2 was
restarting; that was a transient CRI availability problem and is retained only
as diagnostic history. It was not used as the final launcher result.

The latest rerun used launcher/observer SHA-256 `a621a2c774996d4d85695d6bd3093e23616a9410c0462cb0c7d9bdee7243ea66`. RKE2 control-plane readiness remains external to this smoke; the CRI socket was directly usable during the run.
