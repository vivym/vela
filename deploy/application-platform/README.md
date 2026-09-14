# LLM API application publishing boundary (design draft)

> **Status: design only. Do not apply this directory.**
>
> Rancher, Argo CD, an identity-provider group mapping, and a tested admission
> policy package are not installed/validated in the current cluster. The YAML
> files below are placeholders for review and are not a security boundary.

This describes the intended first application tenant boundary for the cluster. It is separate
from `vela-system`, `monitoring`, `apisix`, `longhorn-system`, `object-store`,
and `kube-system`.

## What an application team can do

Members of the external identity group `llm-api-publishers` can use Helm in the
`llm-api` namespace. They can install, upgrade, roll back, and uninstall
namespaced application releases, read Pod logs, and create application
ServiceMonitors/PrometheusRules. The `llm-api-ci` ServiceAccount is bound to the
same Role for CI; issue short-lived tokens with the Kubernetes TokenRequest API,
never a long-lived token Secret.

The publisher Role deliberately has no access to Nodes, namespaces,
PersistentVolumes, StorageClasses, Roles, RoleBindings, NetworkPolicies,
ResourceQuotas, LimitRanges, admission webhooks, HelmChart resources, APISIX
Admin credentials, or any management namespace. It also has no `pods/exec` or
`pods/portforward` permission. The viewer group is read-only.

## Intended enforcement (not live)

The final package must bind quotas to an approved capacity budget, use guarded
admission expressions, cover init/ephemeral containers and all host access
fields, and allow the CPU aggregation tier to select the management nodes. A
default-deny policy must include approved in-namespace dependencies and
Prometheus/OTel scraping paths. These conditions are still open design work.

The APISIX namespace must carry the gateway label after that package is tested.
A public route is a platform-owned
change: an operator reviews the application Service, authentication, rate
limit, timeout, request size, upstream TLS and observability settings, then
creates the APISIX route through the protected Admin API. Application teams
never receive the APISIX Admin credential and cannot create a NodePort.

## Helm release procedure

For a chart in a Git repository or OCI registry, a publisher runs:

```sh
helm lint ./charts/llm-api
helm template llm-api ./charts/llm-api --namespace llm-api \
  --values env/validation.yaml > /tmp/llm-api.yaml
kubectl auth can-i --list --namespace llm-api
kubectl apply --dry-run=server -f /tmp/llm-api.yaml
helm upgrade --install llm-api ./charts/llm-api --namespace llm-api \
  --values env/validation.yaml --atomic --timeout 15m \
  --history-max 10
kubectl -n llm-api rollout status deployment/llm-api --timeout=15m
```

Use immutable image digests, explicit CPU/memory/GPU requests, `ClusterIP`
Services, `serviceAccountName: llm-api-runtime`, readiness/startup probes,
PodDisruptionBudget and a ServiceMonitor. Keep secrets out of values files;
materialize them through the approved Secret/PKI workflow. `--atomic` rolls back
an unsuccessful upgrade, while the Helm release Secret remains namespaced.

For production, store the chart and values in Git and let Argo CD reconcile a
dedicated `llm-api` Application with `CreateNamespace=false`, a pinned chart
revision and an allow-list of this namespace. Argo CD is not installed in the
current cluster; installing it is a separate platform change requiring its
repository, SSO group mapping, HA/storage choices and image mirror promotion.
Do not give Argo or application users cluster-admin as a shortcut.

## Gateway publication request

Submit the release name, Service name/port, external hostname and path, auth
policy, rate limit, timeout, maximum body size, upstream TLS/SNI, and rollback
revision to the platform owner. The owner verifies that the Service is
`ClusterIP`, checks endpoints and health probes, then applies the APISIX route
and tests HTTP status, auth rejection, trace propagation and rollback. This
keeps every externally reachable API behind APISIX while preserving a separate
approval boundary from Helm publishing.
