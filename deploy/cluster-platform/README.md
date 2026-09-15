# MarsLab cluster platform rollout

The RKE2 control plane remains on `10.1.201.70`, `10.1.201.71`, and the shared `10.1.201.66`. The 51 hosts in `gpu-workers.ini` are RKE2 agents with `vela.ai/node-role=gpu-worker` and `vela.ai/gpu-runtime=true`; `rke2-agent` is enabled for boot.

`install-gpu-toolkit.yml` installs NVIDIA Container Toolkit 1.19.1 from the internally staged packages and configures the RKE2 containerd NVIDIA runtime. Credentials and the RKE2 join token are intentionally supplied at runtime and are not stored here.

The `monitoring` Helm release is kube-prometheus-stack 91.1.0 with Longhorn persistence (Prometheus 50Gi/15d, Grafana 20Gi, Alertmanager 10Gi), node-exporter, kube-state-metrics, Prometheus, Alertmanager and Grafana pinned to nodes labelled `vela.ai/management=true`. Grafana is intentionally one replica because its 20Gi Longhorn claim is `ReadWriteOnce`; its deployment uses `Recreate` to avoid a PVC attach deadlock during upgrades. Grafana is exposed through the APISIX gateway after a route is added. GPU nodes run the host `nvidia-smi-exporter` service on port 9835; Prometheus scrapes all 51 requested GPU hosts plus the shared control-plane GPU host, and the `Vela GPU Fleet` dashboard is provisioned in Grafana.

The `apisix` Helm release is APISIX 3.18 with two gateway replicas spread across the two management nodes and a three-member embedded etcd StatefulSet using Longhorn. The gateway is exposed as NodePort 30080 (HTTP) and 30443 (HTTPS); the admin service is ClusterIP and restricted to the two management IPs. The APISIX default plugin set is explicitly preserved and includes the `opentelemetry` plugin; the global rule sends sampled traces to the in-cluster OTel Collector. Add external routes through the APISIX Admin API or APISIX Ingress Controller after defining the upstream service and authentication policy.

Current validation snapshot: 54 total nodes, 53 Ready. All 51 requested GPU IPs are registered and labelled; 49 report 8 GPUs, `10.1.201.59` reports 7 GPUs, and `10.1.201.19` is currently unreachable (`server-36`) and therefore not Ready. The NVIDIA device plugin and Toolkit are enabled as boot services/DaemonSets; image pulls use the three internal caches on `.70`, `.71`, and `.66` (`:5000` through `:5004`), with the release registry mirrored on `:5005`, followed by the configured public China mirrors. Grafana remains `ClusterIP` and is published only through APISIX; the generated admin credential is stored only in the `monitoring/grafana-admin` Secret on the cluster. APISIX responds on `10.1.201.70:30080` and `:30443` (and `.71`), with admin kept as ClusterIP.

The APISIX Admin API routes `/grafana/*` and `/api/*` proxy to the internal Grafana and `vela-api` Services. The API route is recorded in `vela-api-route.json`, rewrites `/api/...` to the backend path, applies a local 120 requests/minute limit, and relies on the Vela bearer-token check; unauthenticated probes return HTTP 401 on both gateway nodes. `vela-api` remains `ClusterIP`, so external access follows the gateway. The APISIX namespace must carry `vela.ai/network-role=api-ingress`. The MarsLab `vela-control-allow-api` overlay matches the chart-native Pod labels `app.kubernetes.io/name=apisix` and `app.kubernetes.io/instance=apisix`; chart 2.17.0 ignores `podLabels`, so a manual custom label is no longer required. The generic Vela base retains its platform-independent gateway-role selector. Namespace labels are declared in `apisix-namespace-label.yaml`. HTTPS on `30443` uses the cert-manager-managed private certificate published to `vela-gateway-default` by a five-minute CronJob. It accepts SNI such as `apisix-gateway` or bare IP clients via the configured fallback; clients must trust the private gateway CA. See `gateway-tls/README.md`; public DNS/PKI remains a separate publication step. The dead node `server-36` has been cordoned while awaiting host recovery; its pending node-exporter pod is expected until the node returns or is removed.

The later HTTPS policy enables `ssl.fallbackSNI=apisix-gateway`, so verified
bare-IP HTTPS also works on `.70/.71:30443` without client SNI. The global
redirect plugin upgrades matched HTTP routes to HTTPS with status 308 and
preserves the method, path and query. Its filter uses the actual connection
scheme and cannot be bypassed using `X-Forwarded-Proto`. The observability Job
preserves this policy alongside Prometheus and OpenTelemetry. Clients still
need the private CA; Grafana is at `https://10.1.201.70:30443/grafana/` or the
same path on `.71`. See `docs/gateway-validation-2026-09-14.md`.

Operational boundary: `10.1.201.44`, `10.1.201.56`, `10.1.201.57`, and
`10.1.201.66` are protected no-reboot hosts. They are recorded in the
`protected_no_reboot` inventory group; automation must exclude that group from
reboot, power-cycle, driver reload, and remediation actions. The GPU toolkit
playbook also fails closed if a protected host is ever targeted.

## Observability validation

The full signal-path snapshot is recorded in
`docs/observability-live-evidence-2026-09-14.md`. Grafana exposes Prometheus,
Loki and Tempo datasources and the `Vela Observability Overview` dashboard.
Alloy tails Kubernetes pod logs with secret-pattern redaction; OTel Collector
accepts OTLP on the management ClusterIP; APISIX metrics are scraped from the
internal `apisix-metrics` Service. External Alertmanager receivers remain
unconfigured until an operator supplies a real destination.

## Current reconciliation and finite acceptance

See `docs/cluster-production-readiness-2026-09-14.md` for the remaining six work
items. The monitoring overlay adds authenticated node-local RKE2 and Alloy
metrics plus Longhorn replica-policy visibility. Read `deploy/observability/README.md`
for dependency/install order; do not apply the Alloy DaemonSet without its
Kustomize-generated configuration.

`disable-default-ingress.yaml` keeps the packaged RKE2 chart managed with zero
NGINX replicas, host ports/service/default class/admission webhook disabled.
Apply the HelmChartConfig only after checking no Ingress resources depend on it.
The live cluster passed that preflight and all 53 Ready nodes no longer accept
TCP80/443. APISIX remains the intended ingress for new externally exposed APIs.

The existing worker local load balancer caches all three control backends. The
restricted `.11` agent-restart drill passed with `.70` blocked; bootstrapping a
new worker was separately verified using a fresh KVM guest with `.70:6443/9345`
blocked. `configure-rke2-bootstrap-ha.yml` installs the CA-verified supervisor
selector on ordinary workers without restarting them. The guest registered via
`.71` and reached Ready. `configure-management-api-ha.yml` installs
`sudo vela-kubectl` on the CPU management nodes; it probes authenticated readyz
and executes the requested command once against the selected API. It does not
retry a failed write against another API. See
`docs/rke2-bootstrap-ha-validation-2026-09-14.md` for deployment and cleanup evidence.
Control application replicas now run on the CPU nodes with no Pod fsGroup;
the materializer owns 0700 scratch directories and the live remount test proves
kubelet no longer widens them to 02770. Application release gates remain open.

Longhorn now requires an explicit default-disk opt-in label; only `llmpool01`,
`llmpool02`, and `marslab-gpu-01` carry it. Existing disk specs were unchanged.
Run `deploy/management-cluster/configure-longhorn-disk-opt-in.py` before adding
future nodes. This prevents newly enrolled GPU workers from automatically
becoming storage replica targets.

The historical all-nodes TCP80/443 closure statement above is superseded for
`.70:80`: a host `nginx.service` is currently listening there and remains intact.
The packaged RKE2 Ingress remains disabled; new APIs use APISIX.

Grafana advertises `root_url=/grafana/` and keeps `serve_from_sub_path=false`, since APISIX strips that prefix before forwarding. This preserves direct backend health/provisioning APIs while all browser assets and redirects use the public subpath. The chart 91.1.0 upgrade changed only the Grafana ConfigMap and Deployment; its existing PVC identity was verified unchanged. Both gateway nodes served all nine login-page assets, authenticated user/search APIs (31 dashboards), and healthy Prometheus/Loki/Tempo datasources on 2026-09-14. Evidence is in `docs/evidence/gateway-grafana-path-validation-2026-09-14.json`.
