# Application publishing boundary

This package installs the first two application namespaces and a cluster observer.
The live validation is recorded in `docs/application-access-validation-2026-09-14.md`.
Argo CD and human OIDC group mapping are implemented in `argocd/`; Rancher is optional.
Current publishing evidence: `docs/platform-publishing-validation-2026-09-15.md`.

| Scope | Placement and authority |
| --- | --- |
| `llm-api` | Non-model API/aggregation on `.70/.71` CPU management nodes. Only the Argo controller can write application workloads in this namespace. |
| `llm-models` | Models and model-related CPU services, including codecs, on ordinary GPU workers. Only the Argo controller can write application workloads in this namespace. |
| `cluster-observer` | Read nodes, workloads, events, logs and resource metrics across the cluster; no Secret/ConfigMap read or mutation. |
| Platform operator | Existing root-protected management kubeconfig. Owns RBAC, network policy, quota, gateway routes and infrastructure. |

The shared `.66` control/GPU host is intentionally excluded from these new tenant
workloads by the `gpu-worker` selector. Existing approved infrastructure there is
unaffected. No host restart or GPU allocation is part of the access validation.

Initial tenant ceilings are 8 CPU/8Gi memory/8Gi ephemeral storage for `llm-api`,
and 32 CPU/64Gi memory/64Gi ephemeral storage/8 GPUs for `llm-models`. These are
aggregate limits, not resource reservations or a claim that a model fits. Both
namespaces currently have **zero PVC/storage allowance** while R5 capacity and
worker disks remain deferred. Changing these limits is a platform operation.

## Identities and credentials

Each tenant retains `-ci`, `-viewer`, and `-runtime` ServiceAccounts. The legacy
`-ci` account is now unbound and has no publishing authority. The namespace
`application-publisher` Role is bound only to the Argo application controller.
It manages Deployments, StatefulSets, ReplicaSets, Jobs/CronJobs, HPA, PDB,
application ConfigMaps and selector-backed ClusterIP Services. It cannot read or
write Secrets. Helm is rendered by Argo, without Helm release Secrets.
Viewers read workload state and logs, without Secret or ConfigMap access.
Runtime accounts have no RoleBinding and no mounted API token.

Each runtime account references `vela-release-pull-v1`, an immutable, platform
created Docker config Secret scoped to that tenant's repository prefix. It holds
read-only credentials for all three release-registry endpoints. Create it using
`hack/configure-registry-pull-secrets.py` after registry preparation; values stay
outside this package. Publishing uses a separate identity documented in
[`registry-access`](../registry-access/README.md).

The Argo target writer cannot change RBAC, ServiceAccounts, quotas, network policy, monitoring
configuration, Nodes, other namespaces, raw Pods, exec/portforward or ephemeral
containers. Platform-owned PodMonitors limit scrape size and select
`vela.ai/metrics: enabled`, port `metrics` (TCP 9090), path `/metrics`.

Install `issue-access.py` root-owned as `/usr/local/sbin/vela-issue-access` on a
management node. Issue credentials through an existing platform operator session:

```sh
sudo vela-issue-access llm-api-viewer --seconds 1800 \
  --output /root/llm-api-viewer.kubeconfig
sudo vela-issue-access cluster-observer --seconds 1800 \
  --output /root/cluster-observer.kubeconfig
```

The tool accepts 600–3600 seconds, writes mode 0600, refuses an existing output,
and prints only the file path/scope/expiry. Transfer the file securely to its
intended recipient and delete the operator copy afterward. It contains explicit
`.70` and `.71` contexts with the cluster CA. Choose the other context if an
endpoint is unavailable; do not automatically repeat failed writes. Tokens expire
and there are no persistent token Secrets. Removing a RoleBinding immediately
removes that role's authority; deleting the ServiceAccount revokes its tokens.
These are scoped automation credentials, not individual human attribution or SSO.
Human publishing access uses the separate Keycloak/Argo groups described in `argocd/README.md`;
no additional cluster-admin credentials are installed.
Anyone holding the existing SSH `user` account and sudo remains a platform
operator. Give application users personal Argo identities; limited observer kubeconfigs are
issued only when needed, without that shared host account; this package does not revoke existing host-level administrator access.

## Admission and network contract

Both namespaces enforce Pod Security Admission `restricted`, pinned to Kubernetes
v1.35. It covers regular/init/ephemeral containers. Additional admission policies:

- Require the namespace runtime account, explicit `automountServiceAccountToken:
  false`, Linux rules, digest-pinned images and `preemptionPolicy: Never`.
- Require the designated CPU/GPU node selectors, default scheduler, the platform-owned non-preempting `vela-application` PriorityClass,
  and only bounded node-not-ready/unreachable tolerations. Model Pods may use the
  `nvidia` RuntimeClass. A direct `nodeName` cannot bypass scheduling.
- Permit emptyDir/configMap/downwardAPI and equivalent projected volumes;
  disallow projected API tokens, host paths, CSI and persistent volumes. Runtime
  Secret references require the platform-owned namespace annotation
  `vela.ai/runtime-secret-names` (comma-separated, no spaces). Missing means none.
  This covers regular/init/ephemeral env and volume references. Image-pull
  credentials may be used to pull, but cannot be mounted by default. Application
  code can read an approved runtime Secret; API RBAC cannot conceal it from code.
- Require selector-backed ClusterIP Services without externalIPs/ExternalName;
  disallow ServiceAccount-token Secrets.

The Pod rules also apply when controllers create Pods. A Deployment object may
be accepted before its unsafe Pod template is rejected; inspect ReplicaSet events
and require successful rollout. Server dry-run a representative Pod before release.
Digest pinning alone does not verify image provenance. Registry publication and
canonical release verification remain separate R4 requirements.

Default ingress and egress are denied. Same-namespace traffic is permitted.
Cross-tenant model/API traffic is limited to TCP 8000/8080/8443. APISIX Pods can
reach those API ports; Prometheus Pods can reach metrics TCP 9090. Egress permits
CoreDNS TCP/UDP 53 and the OTel Collector TCP 4317/4318. Kubernetes API, gateway
Admin API, host SSH, external internet and arbitrary management services have no
allow rule. Add specific dependencies through a platform-reviewed NetworkPolicy.
Container image pulls use node networking, outside Pod egress policy.

Only the platform operator publishes APISIX routes after validating authentication,
rate limits, request size, timeouts, TLS and telemetry. No ingress controller or
NodePort authority is granted to application publishers.

## Apply and validate

```sh
kubectl kustomize deploy/application-platform > /tmp/application-platform.yaml
# On the first install, create the three Namespace objects before server dry-run.
kubectl apply --server-side --field-manager=vela-application-platform \
  --dry-run=server -f /tmp/application-platform.yaml
kubectl apply --server-side --field-manager=vela-application-platform \
  -f /tmp/application-platform.yaml
sudo python3 deploy/application-platform/argocd/install.py
python3 hack/verify-application-boundary.py \
  --receipt /root/application-boundary.json
```

The verifier uses 10-minute tokens in memory, including the Argo controller identity
and negative checks for the retired CI identities. Publisher kubeconfig issuance
is removed from both CPU management nodes. It exercises actual API requests,
restricted/host-access negatives, controller-created Pods, placement, token absence,
DNS, cross-namespace APIs, telemetry and blocked management paths. Temporary CPU
probes use no GPUs or PVCs and are cleaned up. It does not restart services or
perform physical node failure tests. `--api-only` and `--network-only` allow focused
reruns after a relevant change; the separate receipts retain their actual scope.

A security-boundary rollback must remove Argo target writer bindings first, then confirm that tenant Pods are
stopped before loosening admission or network policy. Removing only the policies
would leave already-issued publisher credentials without their safety boundary.
