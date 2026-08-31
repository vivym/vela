# H3 real-environment preflight

Date: 2026-09-01 05:09 Asia/Shanghai

Status: blocked before any mutating action. This is a read-only local and remote
candidate preflight, not production evidence and not a Launch Receipt.

## Required environment

A real H3 campaign requires all of the following before capture or fault
injection:

- one reachable Kubernetes API for the exact validation environment;
- at least three schedulable physical nodes for cross-node Encoder -> DiT -> VAE;
- the `resource.k8s.io` DRA API plus NVIDIA `DeviceClass`, allocated
  `ResourceClaim`, and complete `ResourceSlice` inventory;
- visible NVIDIA GPU UUID and PCI BDF identity matching Fleet authority;
- the canonical release bundle, dedicated evidence database login, immutable
  object versions, and externally retained campaign directory; and
- an approved exercise window and owners for network, storage, node, event,
  rollback, and N/N-1 operations.

Healthy resident models remain loaded during normal placement and drain.
Preflight is read-only. A fault action that would terminate a ModelRuntime is a
separate, explicit maintenance decision rather than an implicit scheduler step.

## Observed local state

The configured context is `k3d-heimdall-staging`. Its API endpoint is
`https://0.0.0.0:62281`, and `kubectl cluster-info`, `kubectl get nodes -o wide`,
and `kubectl api-resources --api-group=resource.k8s.io -o wide` all failed with
`connect: connection refused`.

`nvidia-smi -L` failed because `nvidia-smi` is not installed. Docker reports
only `runc` and `io.containerd.runc.v2`; no NVIDIA runtime is present.

Therefore this host cannot execute or attest real GPU work, Kubernetes/DRA
actuation, physical cross-node transfer, production network/storage faults, or
model N/N-1 drain. Repository fixtures must not be substituted for those facts,
and Production Gate status remains `0/9 PASS`.

## Observed `marslab-server` candidate state

The reachable `marslab-server` host exposes eight NVIDIA GPUs with 64 GiB of
memory each and driver `580.159.03`. NVIDIA container runtime support is present,
and the RKE2 server service is active. This inventory is enough to nominate the
host for a future same-node 8-GPU campaign after the remaining inputs and
mutation approval are available.

The current SSH identity cannot read the RKE2 kubeconfig through passwordless
`sudo`, so it cannot yet establish Kubernetes API, DRA, live ResourceClaim, or
Fleet-to-GPU identity closure. Only this one physical GPU host is currently
visible. It therefore cannot satisfy the required three-node Encoder -> DiT ->
VAE campaign or any cross-node fault claim. No deployment, model load/unload, or
other remote mutation was performed.

## Resume condition

Resume the same-node campaign on `marslab-server` only after obtaining an
authorized kubeconfig, registry/model/backend access, the canonical release and
evidence inputs, and explicit approval for remote writes. Resume the cross-node
campaign only after at least three schedulable physical GPU nodes are visible.
Re-run the read-only inventory first, then capture launch evidence and the
same-node/cross-node/cache Job IDs before any fault injection. Do not unload
healthy resident models merely to create scheduling capacity.
