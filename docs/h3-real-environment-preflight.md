# H3 real-environment preflight

Date: 2026-09-01 05:09 Asia/Shanghai

Status: blocked before any mutating action. This is a read-only local preflight,
not production evidence and not a Launch Receipt.

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

## Resume condition

Resume the external campaign only after selecting a reachable real H3 context
and a host with NVIDIA visibility. Re-run the read-only inventory first, then
capture launch evidence and the same-node/cross-node/cache Job IDs before any
fault injection. Do not unload healthy resident models merely to create
scheduling capacity.
