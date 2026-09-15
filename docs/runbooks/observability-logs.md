# Log pipeline

Grafana uses `Vela Loki`. Each Alloy agent discovers only Pods on its own
`NODE_NAME`, adds namespace/pod/container/node labels, and publishes its own
`collector_node`. Query both `node` and `collector_node` when investigating
cross-node duplicate streams. Keep cardinality bounded; do not add customer
identifiers or request bodies as labels.

The hashed `alloy-config-*` ConfigMap must be deployed through Kustomize together
with the downward-API environment. The previous config remains available until
all old Pods, including the offline `.19` Pod, have gone away. Applying a new
configuration to old agents without `NODE_NAME` is not safe.

Redaction operates on the original log line, covering bearer/basic credentials,
unquoted key/value pairs and quoted/escaped JSON or logfmt values. It does not
replace application-side prevention of customer-content or secret logging.
`hack/verify-alloy-log-pipeline.py` emits six synthetic lines and checks that all
five credential formats are masked, normal content survives, useful labels are
present and only the local collector supplies the stream. No real credentials
are used by the test.

Check `up{job="alloy"}`, `loki_write_dropped_entries_total` and
`loki_write_batch_retries_total` before interpreting an empty query as no events.
The Alloy UI stays on loopback 12345; Prometheus scrapes a TLS/RBAC proxy. For TLS
or authorization errors follow `observability-control-plane.md`.

Loki has one application process and a 100Gi Longhorn PVC with two healthy data
copies on the CPU nodes. Retention is seven days, ingestion 16MiB/s and burst 32MiB/s.
Two storage copies do not eliminate process-restart/volume-reattachment downtime.
Alloy intentionally drops replayed lines older than one hour; Loki allows a
bounded 24-hour chunk age. Check rejection/drop counters when investigating gaps;
no lossless delivery claim is made across extended outages.

The [2026-09-15 capacity measurement](../telemetry-retention-capacity-2026-09-15.md)
found about 443MiB in use and 14.82 hours of queryable history. Successful
compactor cycles and low compressed chunk throughput do not prove a full seven
days of customer logs. Include unflushed chunks, index, WAL and workload growth
when sizing; raw ingestion bytes are not compressed disk bytes.

The user accepts the cluster's shared physical failure domain. Independent-site
backup is unavailable, and external notifications are deferred. Neither changes
the obligation to preserve real failure and missing-data signals in Grafana.
