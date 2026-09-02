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
  object versions, externally retained campaign directory, and read access to
  every release-declared external Secret and ConfigMap; and
- an approved exercise window and owners for network, storage, node, event,
  rollback, and N/N-1 operations.

Healthy resident models remain loaded during normal placement and drain.
Preflight is read-only. A fault action that would terminate a ModelRuntime is a
separate, explicit maintenance decision rather than an implicit scheduler step.

## Typed command

`vela-h3-evidence preflight` validates one canonical release-bound
ResidencyPlan before any deployment or fault action. Configure the same
dedicated evidence database role and read-only Kubernetes identity used by
launch capture, then run:

```text
export VELA_H3_EVIDENCE_DATABASE_URL='postgres://...'
export VELA_H3_EVIDENCE_VALIDATION_ENVIRONMENT='h3-production-cn-north-1'
export VELA_H3_EVIDENCE_COLLECTOR_IDENTITY='spiffe://vela/launch-evidence/preflight'
export VELA_H3_EVIDENCE_KUBECONFIG='/secure/path/kubeconfig'
export VELA_H3_EVIDENCE_KUBERNETES_CLUSTER_UID='<kube-system namespace UID>'
export VELA_H3_EVIDENCE_KUBERNETES_NAMESPACE_UID='<vela-system namespace UID>'

make preflight-h3-real-environment \
  RELEASE_BUNDLE=/absolute/path/to/release-bundle.json \
  H3_EVIDENCE_PLAN_REVISION=49320000-0000-0000-0000-000000000001 \
  > /absolute/path/to/campaign/h3-real-environment-preflight.json
```

The strict V2 report contains ten fixed checks: canonical release bundle,
release-declared external resource binding, dedicated evidence role, Kubernetes
API bound to the expected cluster and Vela namespace UIDs, exact
`1 AUX + 7 single-GPU DiT`
deployment unit, three-node rollout, schedulable READY nodes, NVIDIA
`DeviceClass`, complete current NVIDIA `ResourceSlice` generations, and exact
node/GPU UUID/PCI BDF closure for all eight planned GPUs. The external-resource
check requires the exact kind, namespace, name, nonempty UID and resource
version, `immutable=true`, exact `vela.ai/release-revision`, exact Secret key
set, and canonical content digest from the bundle. It emits only sanitized
identity and digest evidence, never ConfigMap or Secret payloads. External failures use
bounded reason codes and never copy DSNs, kubeconfig paths, credentials, or raw
client errors into JSON. Exit status is `0` only when `ready=true`, `1` for a
typed fail-closed report, and `2` for invalid command/configuration input,
including a missing or invalid release bundle or plan selector. Node
schedulability uses the actual Worker Pod hostname selector and toleration
contract, not only the Node `Ready` condition.

This report is a precondition, not a Launch Receipt. It does not actuate Fleet,
load or unload a model, prove model readiness, run a Job, or exercise failure
recovery.

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

The current repository checkout has no canonical production release bundle, so
the typed command cannot yet be run against a truthful release/environment
pair. Repository unit fixtures prove only report behavior.
