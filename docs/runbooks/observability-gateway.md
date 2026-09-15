# Gateway observability

Check `up{job="apisix-metrics"}` and the `apisix-metrics` Service endpoints. The
metrics endpoint is an internal ClusterIP on port 9091; the public NodePorts
remain 30080/30443. If the target is down, inspect APISIX pod logs for
`plugin_attr` parse failures, then verify `/apisix/prometheus/metrics` from an
in-cluster curl probe. APISIX admin operations use the existing Secret and are
never exposed through the gateway.

Grafana uses `root_url=/grafana/` with `serve_from_sub_path=false`; APISIX strips
the prefix. Validate all referenced assets and authenticated datasource health,
not just `/grafana/login`. The MarsLab API NetworkPolicy matches chart-native
APISIX name/instance labels because chart 2.17.0 does not render `podLabels`.

Certificate `apisix/vela-gateway` is renewed by cert-manager and CronJob
`vela-gateway-tls-sync` publishes it every five minutes. Check Certificate Ready,
renewalTime, CronJob lastSuccessfulTime and the served leaf fingerprints on
both specific Pods. `hack/verify-gateway-live.py` checks the CA and fingerprints
without printing credentials. Its optional `--exercise-limit` consumes 125
unauthenticated requests per Pod from the management host; allow that source's
60-second counter window to reset before subsequent authenticated API probes.

The `VelaGatewayCertificateExpiring`, `VelaGatewayCertificateNotReady` and
`VelaGatewayCertificateSyncStale` alerts cover expiry, missing issuance telemetry
and stopped synchronization. Do not print APISIX SSL response bodies: they
contain the private key. Suspend the sync CronJob before manually restoring a
root-only SSL backup. See [the live record](../gateway-validation-2026-09-14.md)
and `deploy/cluster-platform/gateway-tls/README.md` for the private CA boundary.
