# Vela observability bundle

This bundle installs infrastructure/application monitors and rules, Loki, Tempo,
two central OTel Collectors, node-local authenticated RKE2 metrics, Alloy logs,
and Grafana datasources/dashboards. It preserves each resource's explicit
namespace. Generated ConfigMaps explicitly belong to `monitoring`; the additive
Longhorn metrics policy belongs to `longhorn-system`.

## Application release ownership

For a schema-3 release plan using `render_contract: kubernetes-v2`, render the
application-owned resources separately:

```sh
python3 hack/render-release-observability.py --output /tmp/release-observability.yaml
```

The destination must not already exist. This local command uses the same rules,
dashboard and contract sources as the full platform render. It produces exactly
three hashed ConfigMaps, the `vela-control` PodMonitor, and the `vela-slo-rules`
PrometheusRule, all in `monitoring`. Include this output as the release's
`observability` artifact. Existing Prometheus, Grafana, Collector, Loki and Tempo
remain platform dependencies and are not redeployed by an application release.
The five resources passed live server-side dry-run on 2026-09-15; this does not
constitute production application tracing or SLO acceptance.

## Installation order

1. Install cert-manager and the `vela-identity-selfsigned-bootstrap` ClusterIssuer,
   Longhorn with its chart-managed NetworkPolicies, and kube-prometheus-stack
   91.1.0. Existing Secret values stay outside the repository.
2. Reconcile the existing monitoring release using the pinned chart and
   `deploy/cluster-platform/monitor-observability-reconcile-values.yaml`. Use
   `helm upgrade monitoring <pinned-chart> -n monitoring --reuse-values -f <overlay>`
   for this existing cluster. The overlay adds the rotating-token scrape class
   and read-only Longhorn replica-policy metrics; fresh installations also need
   `monitor-values.yaml` and their existing credential/storage configuration.
3. Preflight node ports 19105–19108 and apply `vela.ai/node-metrics=enabled` only
   to verified, in-scope nodes. `.19` has not passed; `.44/.56/.57` are excluded.
   The registry probes also require `monitoring/vela-registry-prober`: a private
   Secret with the `node-pull` password (`password`) and registry CA (`ca.crt`).
   It is provisioned from the root-owned registry state, never committed here.
4. Render the bundle and run server-side dry-run before applying it:

   ```sh
   kubectl kustomize deploy/observability > /tmp/vela-observability.yaml
   kubectl apply --dry-run=server -f /tmp/vela-observability.yaml
   kubectl apply -f /tmp/vela-observability.yaml
   kubectl apply -f deploy/observability/apisix-observability-service.yaml
   kubectl apply -f deploy/observability/apisix-observability-job.yaml
   ```

Alloy requires the Kustomize-generated, hashed ConfigMap. Applying `alloy.yaml`
alone does not resolve this dependency. Wait for `node-metrics-server` to become
Ready before treating authenticated scrape targets as healthy. The APISIX Job
uses the existing admin Secret through the ClusterIP API; no credentials are
embedded in its manifest. Those APISIX resources remain separate so the Job can
be intentionally rerun when reconciling plugin policy.
When changing this one-shot Job's template, recreate the completed Job before
applying it; Kubernetes does not update an existing Job's Pod template. Its
global rule now includes the HTTP-to-HTTPS 308 policy, so a telemetry policy
reconciliation preserves the gateway's HTTPS requirement.

The node-exporter chart values set `priorityClassName: system-node-critical`.
This keeps host filesystem telemetry available while kubelet reports
`DiskPressure`; it does not remove the pressure taint or grant the exporter
write access. On `.66`, the ZFS dataset rules become actionable only while this
telemetry Pod is running.

The Longhorn metrics policy adds Pod-CIDR TCP9500 access. It depends on the
chart's separate manager, webhook9501/9502, recovery9503 and data-plane policies;
it must never replace them. The old misplaced `monitoring` duplicate was removed
on 2026-09-14 after confirming it selected no Pods.

When adopting a legacy NATS exporter Deployment, capture its existing container
list before applying. Client-side apply can retain a container that was never
managed by this manifest. The final list must contain exactly `nats-0`, `nats-1`
and `nats-2`; a retained `exporter` container can conflict on port 7777. Remove
only the legacy container after matching it to the captured original, then
verify the rollout and six current active Prometheus targets. Do not count
historical `up` series from superseded Pods as current targets. The 2026-09-15
migration is complete; ordinary reconciliation does not need this repair.

## Validation and operating boundary

- `hack/test-infrastructure-rules.py`: promtool failure, disappearance, partial
  coverage, replica-policy, log/trace delivery and MinIO quorum fixtures.
- `hack/verify-grafana-alert-flow.py`: disposable internal alert/fire/silence/resolve
  cycle; refuses a drill if external integrations are configured.
- `hack/verify-alloy-log-pipeline.py`: synthetic secret redaction, useful labels,
  ordinary log preservation and one collector per node.
- `hack/collect-cluster-readiness.py`: repeatable read-only current-state snapshot.
- `hack/test-nats-rules.py`: 27 scenarios for per-member coverage, duplicate
  exporters, stream/consumer absence, quota limits, metadata leader disagreement
  and backlog. NATS uses six fixed targets on two CPU exporter Pods; see the
  `vela-nats` dashboard and messaging runbook.
- `hack/test-registry-rules.py`: registry outage, partial/missing probe coverage
  and certificate expiry scenarios. `registry-probes.yaml` checks authentication,
  anonymous denial and the current Control digest at all three endpoints;
  update that digest when promoting a new Control release.
- `docs/cluster-production-readiness-2026-09-14.md`: finite outstanding work and
  acceptance conditions, including the application release boundary.

Prometheus has 50Gi/15d with a 40Gi retention cap; Loki 100Gi/7d; Tempo 30Gi/48h.
These are capacity bounds, not measured retention guarantees. Loki, Tempo,
Prometheus and Grafana each have one application process with two Longhorn data
copies; a process restart or RWO reattachment can cause an outage. Measure actual
write rate before extending retention or admitting customer telemetry.
The [2026-09-15 measurement](../../docs/telemetry-retention-capacity-2026-09-15.md)
found that the current Prometheus block densities project 44.94–64.28GiB for
15 days, before head/WAL overhead. Its 40GiB cap may therefore trim data before
15 days. This projection is not a proven retention period. Loki/Tempo have low
current usage but insufficient history and business load for full-period proof.

The existing `.70` Docker Compose monitoring is preserved. Its IPMI metrics are
federated read-only; retiring it requires replacing that source and preserving
its historical data. External paging is deferred by the user. Replication across
`.70/.71/.66` without an independent physical failure domain is accepted;
independent-site recovery is not a prerequisite added to this deployment scope.
Application OTLP propagation and real business SLO/residency metrics still require
application integration and evidence.
