# NATS and event delivery

Two CPU-host exporter Pods each run three collectors, one for each fixed NATS
monitoring endpoint. Prometheus must see six targets and three distinct
`nats_member` values. The `vela-nats` Grafana dashboard displays per-member file
quotas, business stream/consumer presence and backlog. Check the exporter Pods
and all three NATS endpoints. Inspect
JetStream replica and consumer lag before changing cluster membership. Keep
the exporter and alert path healthy for a full observation window after repair.

Before replacing a member, require every stream and consumer replica to be
current. `hack/nats-live-recovery` prepares an isolated bounded file stream;
`hack/verify-nats-live-recovery.py` uses PDB-protected sequential Pod evictions,
checks stable PVC names and message hashes, and verifies durable redelivery.
Inspect the existing runner PID and incremental receipt before any retry.
The [2026-09-14 record](../stateful-recovery-validation-2026-09-14.md) passed,
but does not test a whole-cluster loss or the PostgreSQL Outbox replay path.

The business stream's 64GiB contract currently exceeds each server's automatic
36.7GiB quota on a 50Gi PVC. Explicit server memory and file limits and adequate
storage must be established before business bootstrap. Do not lower the
stream contract or delete historical data to make capacity checks pass.


The `VelaNATS*` business alerts distinguish exporter/member coverage, reduced
exporter redundancy, file quota below the 64GiB contract, missing/unobservable
`VELA_EVENTS` or `VELA_SCHEDULER`, incomplete stream replica coverage, conflicting
metadata leaders, and sustained consumer backlog. Two exporter copies of the
same NATS member count as one member. Zero messages in an existing stream is
healthy for the presence check; a missing series is not treated as zero messages.
Stream metrics alone do not validate the complete stream configuration or prove
PostgreSQL Outbox replay.

`hack/test-nats-rules.py` generates 27 promtool cases. Use a disposable CPU Job
with the deployed Prometheus image, read-only fixtures and a bounded writable
`/tmp`; distroless images have no shell utilities and promtool needs temporary
storage. Keep failed test receipts and clean Job/ConfigMap/NetworkPolicy resources.

Before expanding NATS PVCs, use the measured allocation report in
[`nats-capacity-and-observability-2026-09-15.md`](../nats-capacity-and-observability-2026-09-15.md).
The two CPU nodes lack safe capacity for all volume commitments plus 80Gi NATS
claims. Merely lowering Longhorn reservations, increasing overprovisioning or
retiring the old MinIO claims does not resolve all physical headroom constraints.
No protected host reboot, GPU driver operation or deferred disk initialization
is part of this monitoring change.

The 2026-09-15 exporter migration passed 13 live checks and a full alert waiting
window: six active targets are healthy, while three quota alerts and the missing
business stream/consumer alerts fire. These remain actionable R3/R5 items.
Before applying over an older unmanaged Deployment, capture its containers and
check the final list is exactly `nats-0`, `nats-1`, `nats-2`. A legacy `exporter`
can survive client-side apply and conflict on port 7777; remove only that captured
legacy container. Preserve failed receipts and verify active target membership
rather than stale historical `up` series. See the linked report for receipts.
