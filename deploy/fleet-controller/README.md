# Fleet Controller Deployment Contract

This Kustomize base installs the namespaced `WorkerPool` API, the exact Fleet
Controller service account, namespace-bound live-resource permissions, read-only
Node discovery, and the fail-closed validating webhook contract. The Fleet
Controller owns protected `WorkerPool`, `OnDelete` DaemonSet, and Worker Pod
lifecycle. Argo CD may install this base and versioned desired input, but this
base grants no Argo identity access to live protected resources.

The controller image, approved ResidencyPlan, Stage Worker Agent and H3
ModelRuntime images, configuration names, device identities, and webhook
`caBundle` are deliberately invalid placeholders. The ResidencyPlan BusyBox
init image is the repository-pinned shared `1.37.0` `linux/amd64` manifest.
Delivery must replace the immutable target-only rollout ConfigMap with an
approved release-bound plan and provision these independent trust materials:

- `vela-fleet-control-mtls`: Fleet Controller client certificate, private key,
  and CA for the Fleet maintenance gRPC service;
- `vela-fleet-admission-tls`: webhook serving certificate and private key for
  `vela-fleet-admission.vela-system.svc`; and
- `vela-kube-apiserver-webhook-client-ca`: the CA that verifies the
  kube-apiserver client certificate presented to `/validate`.

Delivery must also inject the serving-certificate CA into the webhook
`caBundle`. Applying the placeholders is not a successful deployment and cannot
produce a Launch Receipt.

The legacy `desired-revisions.yaml` remains in the repository only as an
explicit rollback-before-contraction input. It is not part of the default
Kustomize render and the production Deployment does not mount it. Re-enabling
that path requires a reviewed rollback overlay and is forbidden after the S49.12
contraction fence.

## Release bundle boundary

Production assembly must include the final `kubectl kustomize` output as the
exact `fleet-controller` render in the canonical Slice 40 release bundle. The
bundle rejects placeholder or mutable image references and binds the approved
ResidencyPlan, referenced configuration/Secret revisions, and complete
versioned resource inventory. A changed render, plan, image, actuation, or
external revision requires a new configuration revision and release digest.
Verification does not replace live admission, RBAC, rollout, or Launch Receipt
evidence.

## Admission Identity Contract

`/validate` trusts `AdmissionReview.userInfo` only after TLS has verified the
kube-apiserver peer. Configure the kube-apiserver `ValidatingAdmissionWebhook`
plugin in `AdmissionConfiguration` with a `kubeConfigFile`. The selected
kubeconfig user must present a client certificate and key whose certificate:

- chains to the CA stored in `vela-kube-apiserver-webhook-client-ca`; and
- has exactly one URI SAN,
  `spiffe://vela.internal/kube-apiserver/admission`, matching
  `VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID`.

The webhook serving `caBundle` does not provide this reverse client
authentication. A kubeconfig with only server CA data is incomplete and all
`/validate` calls will fail closed.

After peer authentication, protected mutations are accepted only from
`system:serviceaccount:vela-system:vela-fleet-controller`. The sole delegated
exception is protected Pod `CREATE` by `system:kube-controller-manager`, because
the DaemonSet controller materializes Pods from the Fleet-owned protected
`OnDelete` DaemonSet. The handler validates the Pod against its live protected
parent. That identity receives no protected update, delete, or finalizer-removal
authority.

## Network Boundary

This base deliberately does not include a `NetworkPolicy`: kube-apiserver source
ranges and kubelet health-probe paths differ by cluster and CNI. Each deployment
overlay must provide a default-deny-compatible policy for the Fleet Controller
Pods that limits admission-port ingress to the exact kube-apiserver/control-plane
sources and the minimum node sources required for `/healthz` and `/readyz`
probes. Rendering this base is not evidence that admission ingress is isolated.

## Retirement Completion

Admission authorization permits a protected Kubernetes mutation; it does not
prove deletion completion. The Fleet reconciler reports a resource retired only
after a live GET proves the exact Kubernetes UID absent and PostgreSQL records an
append-only completion receipt while revalidating the complete `DELETE` plus
`REMOVE_FINALIZER` authorization. A same-UID terminating resource or a resource
held by another finalizer remains Pending. On restart, a matching durable
completion receipt is replayed before any Kubernetes mutation is attempted.

Render the repository contract with:

```sh
kubectl kustomize deploy/fleet-controller
```

The two-replica controller Deployment runs the Fleet runtime and fail-closed
admission endpoint with independent readiness, TLS mounts, and a disruption
budget. Repository rendering does not prove live admission, RBAC, rollout, or
any Production Gate.
