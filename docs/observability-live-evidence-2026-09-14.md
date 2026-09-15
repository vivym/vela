# Observability live evidence — 2026-09-14

## Current follow-up — 17:01 CST

The original Compose monitor is preserved: its Prometheus API reports 2.55.1,
172 targets/170 up, and 55 BMCs. Six BMCs have at least one failed IPMI collector;
18 failed collector series must not be reported as 18 failed machines. GPU/IPMI
dashboard reuse and Grafana's internal-only alert workflow remain in place.

New MinIO member-loss tests exposed a timeout in full admin inventory and v2
metrics when two peers are blackholed. A dedicated v3 erasure-set ServiceMonitor
now supplies the quorum alerts, with separate missing-endpoint coverage and
namespace/service/pool/set grouping. 133 fixtures pass, including isolation of
two distinct MinIO clusters. See `docs/minio-ha-validation-2026-09-14.md` for the
source trace, successful member-level S3 checks and recovered cleanup failure.

The 16:48 initial retention-rate snapshot is
`docs/evidence/retention-initial-rates-2026-09-14.json`: approximately 865,751
head series, 27,774 float samples/s, 1.82 GB of compacted Prometheus blocks and
only 15.6 hours of history. Loki receives about 14.1 kB/s; Tempo's recent incoming
rate is zero. These are preliminary measurements, not 15d/7d/48h workload-retention
proof. The complete 17:01 inventory is
`docs/evidence/cluster-readiness-2026-09-14-followup.json` (29 healthy volumes).

## Historical reconciliation — 16:04 CST

The machine-readable snapshot and scoped validation receipts are in
`docs/evidence/cluster-readiness-2026-09-14.json`. The complete outstanding-work
list is `docs/cluster-production-readiness-2026-09-14.md`; it supersedes earlier
open-item lists. No protected host was rebooted or had a GPU driver reloaded.

| Signal | Current evidence | Retention/availability boundary |
| --- | --- | --- |
| Node/control metrics | 53 kube-proxy and 53 authenticated relays; scheduler, controller-manager and etcd each 3; 23 Longhorn replica-policy metrics | `.19` is offline; source metrics stay loopback |
| Logs | 53 online Alloy agents use local Pod discovery and useful labels; both CPU-node synthetic log drills passed | Loki 100Gi/7d, one process, two CPU-host Longhorn copies |
| Traces | Both OTel collectors expose application 8889 and internal 8888 metrics; APISIX sampled traces reached Tempo | Tempo 30Gi/48h, one process, two data copies; business propagation pending |
| Dashboards | Two imported GPU dashboards (25 panels), IPMI hardware and 16-panel control/telemetry dashboard | IPMI federation depends on old Compose Prometheus |
| Alerts | Grafana fire/silence/unsilence/automatic-resolve passed; 127 promtool 3.14.0 cases pass | External receivers deferred by user |
| Gateway | Both `.70/.71:30080/grafana/login` return 200; default RKE2 ingress disabled | Internal HTTPS 30443 uses interim certificate/SNI `apisix-gateway` |

### Authenticated RKE2 metrics

`node-local-metrics-control-storage` is 3/3; the worker DaemonSet is 50/50.
After port preflight 53 nodes carry `vela.ai/node-metrics=enabled`; `.19` has
not passed preflight. OTel reads kube-proxy 10249 and, on control nodes,
scheduler 10259/controller 10257/etcd 2381. The node listener 19105 requires TLS/RBAC;
no credentials and an unprivileged ServiceAccount returned 401/403, while the
Prometheus ServiceAccount returned 200. No hostPath or widened source listener
is used. cert-manager renews the dedicated leaf certificate.

Monitoring Helm revision 12 adds the projected-token `rke2-node-metrics` scrape
class and read-only kube-state-metrics Longhorn configuration metrics. The
chart's direct kube-proxy scraper is deliberately replaced by per-node relays
and coverage rules; it is no longer an unresolved monitoring gap.

### Logs and collector health

The original Alloy setup discovered all cluster Pods on every node, lacked
namespace/pod/container/node labels, and tried to redact a nonexistent extracted
`message` field. A six-line baseline actually reproduced unredacted synthetic
credentials. The corrected configuration uses `spec.nodeName=NODE_NAME`, original
line capture redaction, explicit labels, and `collector_node` ownership.

Native Alloy 1.10.2 validation passed. CPU-node markers `8a5f459e489f` and
`7a916da109fa` each yielded six distinct lines: all five credential formats
masked, normal text preserved, exactly the local collector identified. Old
`alloy-config` is retained for rollback/offline-node cleanup; new Pods use hashed
`alloy-config-4d478tbk6d`. The DaemonSet is 54 desired/53 Ready/53 updated because
`.19` remains unreachable. Both delivery-drop and retry counters were 0 at the
recorded check, and 53 authenticated Alloy self-metrics targets were healthy.

Alloy drops replayed lines older than 1h; Loki has bounded 24h chunk age. These
settings and asynchronous bounded exporters do not prove zero loss during
extended outages. Application code must still prevent customer content and
secrets entering logs or spans.

### Alert behavior and Compose reuse

Two legacy MarsLab GPU dashboards were adapted to `vela-prometheus` and
`job="nvidia-smi"`; unsupported exporter metrics were replaced with accurate
collection/scrape signals. IPMI federation accepts upstream `ipmi`/`ipmi-*` job
names and monitors both HTTP `up` and actual `ipmi_up`. At 14:41 CST, 33 panels and
38 queries across the imported/hardware dashboards returned data; 55 BMCs were
present and 6 had actual collector failures. See
`docs/evidence/observability-compose-reuse-2026-09-14.json` for that timestamped
snapshot. The original Docker Compose services and data were preserved.

The duplicate-default Grafana datasource provisioning error was fixed, and
`vela-alertmanager` supports the internal alert workflow. Drill `1daf02b2a1c7`
passed fire → Grafana visibility → silence → unsilence → automatic resolve;
the temporary rule was deleted and silence expired. No external integrations
were configured. The newer control/telemetry dashboard has 16 panels and all 16
queries returned data in its initial validation.

The current 127-case rule suite covers failed/vanished/partial targets, storage
replica-policy exemptions, control/etcd health, OTel refusal/export failures,
Alloy retries/drops, hardware collection and MinIO quorum. The last 17 fixtures
specifically distinguish duplicate MinIO scrapes, write/read loss, zero margin
and partial missing telemetry. The test Pod exited 0 and was removed before the
rules were applied.

### Storage and namespace reconciliation

Loki has exactly two healthy RW data copies on `.70/.71`. The former scheduling
failure was caused by provisioned commitments plus 30% CPU-disk reservations;
these became 20%, keeping overprovisioning 100% and minimum physical free 10%.
`.66:/srv/models` is a different filesystem and was not that failure's cause.
No historical data was deleted. A surplus `.66` copy was removed only after
both intended CPU copies were RW. Replica-policy alerts exclude only the two
disposable Control validation claims, never other single-copy persistent data.

The global Kustomize namespace transformer had relocated the Longhorn metrics
policy into `monitoring`. It now preserves explicit namespaces and assigns
`monitoring` to each ConfigMap generator. Rendering changed only that policy's
namespace; the bundle passed server dry-run. The correct Longhorn namespace
already contained its additive metrics policy and chart-managed manager,
webhook and recovery policies. Those were preserved; the unused misplaced
monitoring duplicate was backed up and removed.53 Longhorn scrapes remained up.

### Remaining real conditions

- `.19` is NotReady/cordoned; `.59` enumerates seven physical GPUs.
- `.66:/srv/models` has approximately 9.6% free in the earlier filesystem probe;
  retain its low-space signal and resolve capacity without deleting user data.
- MinIO is four members with read quorum 2/write quorum 3. Two members share `.66`;
  loss of that host interrupts writes until recovery. Current HTTP 200 and four
  healthy members do not prove single-host write availability.
- Business SLI/SLO and Stage-residency series are not yet backed by real
  application traffic/reports. Their missing-data alerts remain real release
  integration work; nothing was fabricated to suppress them.
- Loki, Tempo, Prometheus, Grafana and Alertmanager each have a single application
  process. Two Longhorn copies support data recovery, not continuous service
  through process restart or volume reattachment. Actual retention duration and
  recovery times still need workload-based measurements.

External paging is intentionally deferred. The shared physical failure domain
is explicitly accepted. Independent-site infrastructure is not an added blocker
for the current deployment; these boundaries also do not establish customer
release readiness.
