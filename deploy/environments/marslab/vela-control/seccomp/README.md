# Artifact validator seccomp policy

`artifact-sandbox.json` pins the RKE2/containerd RuntimeDefault policy observed on
`llmpool01` on 2026-09-16, with one additional `clone` rule. The rule requires the
full `NEWUSER | NEWNET | NEWNS | NEWPID | NEWIPC | NEWUTS` namespace set and does
not allow `NEWCGROUP`. Ordinary process creation retains the original rule;
`clone3` still returns `ENOSYS` so Go uses the inspectable `clone` argument.

SHA256: `bd44ec5ac61a84a03c37cb405a62e80c16093ceba8ae7cbdcb66a0a59d546ca5`.

Install this exact file as root on every eligible management node before the
Marslab control Deployment is applied:

```sh
sudo install -D -m 0644 artifact-sandbox.json \
  /var/lib/kubelet/seccomp/vela/artifact-sandbox-bd44ec5ac61a.json
sudo sha256sum /var/lib/kubelet/seccomp/vela/artifact-sandbox-bd44ec5ac61a.json
```

The deployment selects `vela.ai/artifact-sandbox=bd44ec5ac61a`. Apply this node
label only after the profile is installed and the production sandbox tests pass
under the current control image. This keeps unprepared replacement nodes from
receiving a control Pod. The file survives reboot; installing it requires no
host, Docker, RKE2 or kubelet restart.

The `vela-control` container uses `Localhost` with this profile. Its init
container keeps RuntimeDefault. UID/GID 10001, all capabilities dropped,
`allowPrivilegeEscalation: false`, the read-only root filesystem, AppArmor,
network policies, namespace isolation, resource limits and Landlock remain in
force. The added rule permits creation of the inner sandbox, not privileged
host operations.

Qualification uses the current control image without business Secrets or a
ServiceAccount token. Compile the existing tests for the target architecture:

```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c \
  -o artifactvalidator-sandbox.test ./internal/artifactvalidator
```

Mount that binary read-only in a diagnostic Pod with an isolated `/tmp` emptyDir,
UID/GID 10001, all capabilities dropped, a read-only root and this Localhost
profile. Set `VELA_REQUIRE_PINNED_FFPROBE=1`, `PATH=/usr/bin` and
`VELA_ARTIFACT_VALIDATOR_HELPER_PATH=/usr/local/bin/vela-artifact-validator`.
Use the control Deployment's `imagePullSecrets` for the private image; image pull
authentication does not require mounting the Secret into the test container.
Set `restartPolicy: Never` and `activeDeadlineSeconds: 120`. Invoke the test
binary with `-test.timeout=60s -test.v` and the three test names below as an
anchored alternation in `-test.run`. Preserve the logs and require the Pod phase
to be `Succeeded` before labeling a node.

Before upgrading containerd, compare its new default profile and rerun
`TestProductionSandboxProbesPinnedVideoAndThumbnail`,
`TestLandlockRestrictsFilesystemToPinnedExecutable`, and
`TestPinnedFFprobePreservesFullH3VideoAndAudioFacts` in an isolated Pod on each
eligible node, using the production control image and a Linux test binary.
Never replace this profile with `Unconfined` to make a probe pass.

The policy was qualified on `.70` and `.71`; the paired default-policy probe
reproduced `fork/exec ... artifact-validator-helper: operation not permitted`.
See [the H3 repair report](../../../../../docs/h3-api-repair-2026-09-16.md) for
live acceptance evidence and the exact deployment sequence.
