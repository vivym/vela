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
actuation must reference their exact names. A root init container copies those
projected files to a memory-backed private volume, changes ownership to
UID/GID 10001, and applies mode `0400`; the Stage Worker process never mounts a
projected Secret directly.

Render the static prerequisites with:

```sh
kubectl kustomize deploy/stage-worker
```

The render is a configuration contract, not a ResidencyPlan approval,
ModelResidency receipt, GPU isolation receipt, or Production Launch Receipt.
The canonical release bundle binds each external resource name and declared
revision, but Fleet deliberately has no permission to read Secret values and
does not bind a live Kubernetes UID or content digest. Kubernetes admission
control and secret-manager policy must therefore prevent deletion and same-name
recreation of an immutable ConfigMap or Secret for the lifetime of a rollout;
that receipt remains an external launch gate.
