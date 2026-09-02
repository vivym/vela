# Fleet Controller Deployment Contract

This Kustomize base installs the Fleet Controller service account,
namespace-bound live-resource permissions, read-only Node discovery, and the
fail-closed validating webhook contract. The Fleet Controller owns protected
WorkerInstance Pods and the per-member Services and Secrets derived from the
approved ResidencyPlan. Argo CD may install this base and versioned desired
input, but this base grants no Argo identity access to live protected resources.

The controller image, approved ResidencyPlan, Stage Worker Agent and H3
ModelRuntime images, configuration names, device identities, and webhook
`caBundle` are deliberately invalid placeholders. The default ResidencyPlan
materializes the complete H3 topology contract: one single-slot AUX
WorkerInstance with separate Encoder and VAE ModelRuntime routes plus seven
independently schedulable single-slot DiT WorkerInstances. The repeated `3` and
`4` image digests, `template-not-approved` identity, unconfigured region, and
template model/runtime revisions are non-production values. The ResidencyPlan
BusyBox init image is the repository-pinned shared `1.37.0` `linux/amd64`
manifest. Delivery must replace the immutable target-only rollout ConfigMap
with an approved release-bound plan, recompute its content and layout digests,
and provision these independent trust materials:

- `vela-fleet-control-mtls`: Fleet Controller client certificate, private key,
  and CA for the Fleet maintenance gRPC service;
- `vela-fleet-admission-tls`: webhook serving certificate and private key for
  `vela-fleet-admission.vela-system.svc`; and
- `vela-kube-apiserver-webhook-client-ca`: the CA that verifies the
  kube-apiserver client certificate presented to `/validate`.

Delivery must also inject the serving-certificate CA into the webhook
`caBundle`. Passing the repository deployment-contract test only proves that the
placeholder topology is complete and internally consistent. Applying the
placeholders is not a successful deployment, Production Gate, or Launch Receipt.

For a multi-member WorkerInstance, the approved WorkerBundle also names one
immutable aggregate member-PKI source Secret. The controller has namespaced
`get` and `create` authority for Secrets and Services so it can validate that
source and materialize one protected Service and one immutable derived Secret
per member. It has no update or delete authority for those resources. Standard
single-member H3 WorkerInstances do not use this permission path and expose no
member Service. The fail-closed webhook reconstructs exact member Services and
derived Secrets from the release-bound rollout and source PKI before allowing
Fleet `CREATE`; it rejects their `UPDATE` and `DELETE` until a separate cleanup
authority is implemented.

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

After peer authentication, protected Pod, Service, and Secret mutations are
accepted only from
`system:serviceaccount:vela-system:vela-fleet-controller`. There is no delegated
controller-manager mutation path.

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
