# Runtime startup production deployment contract

This document records the deployment boundary for the production launcher while
runtime startup remains in validation. It is an operator contract; it does not
turn `Production Gates` on and it does not treat the validation launcher as a
production component.

## Host prerequisites

The host must provide a CRI v1 RuntimeService and ImageService over the socket
named by `VELA_RUNTIME_LAUNCHER_CRI_SOCKET`, a root-owned Kubernetes kubeconfig,
and a root-owned observer executable. The launcher runs as `root` and creates
one PodSandbox containing exactly the `model-runtime` and `stage-worker-agent`
containers from the signed Pod. Both images and the sandbox image must use
`@sha256:<64 lowercase hex>` references.

The observer executable must implement the existing custody contract. The
production launcher binary itself implements this mode, so the default safe
provisioning is to point `VELA_RUNTIME_LAUNCHER_OBSERVER_PATH` at the same
source-matched `vela-runtime-launcher` binary:

```text
--vela-runtime-observer --attach-fd4 UID GID
```

It receives the inherited FD 3 and target Runtime pidfd FD 4, attaches to that
exact Runtime process before the pidfd gate is released, retains the observer
socket endpoint, and remains a direct child of the launcher helper. The
observer verifies the target process effective UID/GID against the signed
Runtime identity before attaching. The launcher returns the original worker
pidfd, observer pidfd, and observer socket
endpoint using one `SCM_RIGHTS` message. No numeric PID is reopened; the
observer only reads the PID associated with the already retained pidfd for the
ptrace attach.

The launcher creates that observer custody socketpair before creating or
starting the Runtime container. It then starts the observer after receiving the
Runtime's original pidfd offer, so the socket and every returned descriptor stay
bound to the same launch invocation.

The signed Pod must contain exactly `model-runtime` and `stage-worker-agent`.
It may also contain the two approved init containers
`stage-worker-private-materialization` and
`model-runtime-private-materialization`; they are digest-checked, created in
manifest order, started, and required to exit successfully before either
long-running container starts. Init images are prepared and digest-verified
before the sandbox is created.

The launcher maps CPU and memory requests/limits, read-only root filesystems,
`allowPrivilegeEscalation=false`, RuntimeDefault seccomp, pod UID/GID, and the
supported volume sources (HostPath, EmptyDir, ConfigMap, Secret, Projected,
and DownwardAPI). It rejects fields it cannot reproduce in the CRI request,
including sidecars/ephemeral containers, ports, probes/lifecycle, interactive
streams, custom working directories, unsupported security profiles, resource
claims, and volume subpaths or propagation settings. A rejected field fails
closed before the corresponding container is created.

The Runtime bootstrap command follows the OCI split between executable and
arguments: `Command` is `/usr/local/bin/vela-model-runtime`, while `Args` is
`serve-remote --bootstrap-file /run/vela-model-runtime-bootstrap/bootstrap.json`.
The Node launcher combines these fields when validating the CRI task and bind
mounts the per-startup bootstrap directory at the path's parent.

## Launcher environment

`vela-runtime-launcher` requires these variables when started by the Node
adapter:

```text
VELA_RUNTIME_LAUNCHER_CRI_SOCKET=/run/containerd/containerd.sock
VELA_RUNTIME_LAUNCHER_KUBECONFIG=/etc/vela/runtime-launcher/kubeconfig
VELA_RUNTIME_LAUNCHER_NODE_NAME=<signed-node-name>
VELA_RUNTIME_LAUNCHER_SANDBOX_IMAGE=<registry>/<pause>@sha256:<digest>
VELA_RUNTIME_LAUNCHER_OBSERVER_PATH=/usr/local/bin/vela-runtime-launcher
VELA_RUNTIME_LAUNCHER_CGROUP_PARENT=kubepods.slice
VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT=120s
```

`VELA_RUNTIME_LAUNCHER_CGROUP_PARENT` must match the host's CRI/runc cgroup
driver. With the RKE2 systemd driver, `kubepods.slice` is the default valid
parent; omitting it can make runc reject a path such as `/k8s.io/<id>`.
`VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT` bounds image preparation, sandbox
creation, container start and pidfd offer acquisition. It defaults to two
minutes and is capped at fifteen minutes; malformed values fail closed.

The kubeconfig is a regular root-owned, non-writable file. It is not an
executable and is not read from ambient `KUBECONFIG`. The observer path is
checked with `securefile.ValidateExecutable`, including trusted ancestors.

The Node service separately requires the runtime-startup variables documented
in `deploy/node-agent/README.md`, including
`VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE` in addition to
`VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_DIRECTORY`. The policy issuer
must be active before Node startup and its authorization directory must be
shared with the Fleet-side publisher through a controlled provisioning path.

## Installation order

Install the binaries and root-owned configuration first, then enable the
supporting services in this order:

```text
vela-pidfd-broker.service
vela-runtime-policy-issuer.service
vela-node-agent.service
```

The broker socket parent and issuer socket parent must be root-owned and
non-writable. The broker runtime GID must match the signed Runtime/Worker GID.
The issuer reply key and Fleet authorization public key are distinct files
provisioned out of band; this repository does not generate production keys or authorization
attestations.

## Current validation boundary

Before an installer writes any system path, it must run the package verifier
against the exact release revision:

```text
vela-release-artifacts verify-runtime-startup-packages <package-directory> <revision>
```

The verifier checks the complete inventory, every binary digest and ELF target,
the package contracts, both systemd units, both environment examples, and the
runtime-startup provisioning contract. A failed verification must stop before
installation or service reload.

The production launcher now compiles for Linux `amd64` and has focused tests
for strict request JSON, duplicate Runtime/Worker containers, digest-pinned
images, canonical paths, and volume path rules. The target host has passed the
same focused test binary on Ubuntu 24.04 / kernel `6.8.0-137-generic`.

The following evidence is still required before a production Launch Receipt:

1. A source-matched binary must create both CRI containers on `marslab` with a
   real signed Pod and a real observer executable.
2. The Node composition must bind that target to the caller, Fleet reservation,
   immutable authorization, journal grant, Worker journal, and ModelRuntime
   Permit in one operation.
3. The combined composition must pass caller replacement, observer loss, policy
   loss, helper timeout/crash, Node restart, broker restart, and cleanup checks.
4. The release bundle must carry the production launcher, broker, issuer,
   systemd units, environment examples, and key/directory provisioning
   contracts.

Until those receipts exist, keep `Production Gates=0/9` and label all launcher
receipts `validation_only=false` only for the helper protocol smoke itself; a
protocol smoke is not a production Launch Receipt.
