# Stage Worker Deployment Contract

This base provisions only the immutable, non-secret configuration and tokenless
ServiceAccount shared by Fleet-created Stage Worker Pods. Fleet materializes
one Pod per authenticated `WorkerMemberActuation`; this directory intentionally
contains no Deployment, DaemonSet, StatefulSet, or static Pod.

`vela-system` is shared with the control and legacy rollback workloads, so this
base does not mutate Namespace-wide Pod Security labels. Cluster overlays own
that policy and must validate every workload in the Namespace before tightening
it.

Before a ResidencyPlan rollout, replace the ConfigMap name and placeholder
values with release-bound values and provision the four immutable Secrets named
by `secret-contract.json`: Stage Worker control mTLS, authority keyring,
Artifact Store credentials, and Artifact Store private CA. The WorkerBundle
actuation must reference their exact names. The control Secret contains
`ca.crt` plus `<worker-member-uuid>.tls.crt` and
`<worker-member-uuid>.tls.key` for every member in the rollout; each Pod
projects only its own pair into the fixed runtime filenames. A root init
container copies those projected files to a memory-backed private volume,
changes ownership to UID/GID 10001, and applies mode `0400`; the Stage Worker
process never mounts a projected Secret directly.

A multi-member WorkerInstance additionally references one immutable aggregate
member-PKI source Secret. Fleet validates each member's independent client and
server certificate chain, key pair, EKU, Service DNS name, SPIFFE URI, and
identity digest, then creates one immutable derived Secret per member. A Pod
mounts only its own derived Secret; the aggregate source is never mounted into
a Worker Pod. Peer client/server credentials and control-plane client
credentials remain separate trust domains. A standard single-member H3
WorkerInstance still projects its own control identity, but creates neither a
member Service nor a derived member Secret.

The separate `vela-model-runtime-verifier-placeholder` ConfigMap is public
authority, not a Secret. Replace it with an immutable release-bound ConfigMap
whose `verifier-keyring.json` is derived from the exact StageAuthority signing
rotation set. Fleet copies only that public verifier, the generated launch
manifest, and no Stage Worker private key or Artifact credential into the
ModelRuntime private memory volume. The external digest-pinned H3 runtime image
must use `/usr/local/bin/vela-model-runtime` as its exact OCI entrypoint and must
also contain every driver binary named by the actuation manifest.

Each `ModelRuntimeProcess` in a WorkerBundle binds its own ModelResidency,
StageProfile, durable epoch floor, initialization timeout, and shutdown timeout.
The AUX shared-slot exception therefore launches Encoder and VAE as independent
resident processes with independent epochs while the supervisor enforces one
active StageLease across both routes. Model loading remains lifecycle-owned by
the Pod; per-Job load, unload, and model replacement commands are absent.
The authenticated registration path supports both standard single-member H3
and a multi-member WorkerInstance. Every member registers its independent
runtime epoch; only the deterministic smallest-UUID leader acquires a StageLease
and coordinates the complete gang through authenticated member Services. A
missing member or incomplete barrier fails closed. The ModelRuntime remains
local to its Worker Pod and is reached only through the protected UDS.

Render the static prerequisites with:

```sh
kubectl kustomize deploy/stage-worker
```

The render is a configuration contract, not a ResidencyPlan approval,
ModelResidency receipt, GPU isolation receipt, or Production Launch Receipt.
The canonical release bundle binds each external resource name and declared
revision, but does not bind a live Kubernetes UID or content digest. Fleet's
multi-member actuation path reads the named source Secret to validate and derive
member PKI; its Role cannot update or delete Secrets. Kubernetes admission
control validates exact creates and rejects mutation of Fleet-derived member
Services and Secrets. Secret-manager policy must separately prevent deletion
and same-name recreation of external immutable ConfigMaps and source Secrets
for the lifetime of a rollout; that receipt remains an external launch gate.
