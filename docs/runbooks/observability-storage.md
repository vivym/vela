# Longhorn observability and replica policy

Check `up{job="longhorn-backend"}` and the `longhorn-manager` ServiceMonitor in
`monitoring`. The target Service and additive metrics NetworkPolicy live in
`longhorn-system`. Preserve the Longhorn chart's manager/webhook/recovery/data-plane
policies; a TCP 9500-only rule is not a replacement for those service paths.

Inspect Longhorn volume robustness and actual replica `RW` states before repair.
Also inspect `kube_customresource_longhorn_volume_replicas`: a one-copy volume can
report `healthy` while violating the accepted two-copy policy. Only disposable
`vela-system/vela-control-.*-artifact-validation` claims on `longhorn-wffc` are
exempt. A missing configured-replica metric must remain visible as a coverage gap.

The accepted storage nodes are `.70/.71/.66`; ordinary GPU workers cannot receive
persistent data replicas. Loki is additionally pinned to the two CPU hosts.
`deploy/management-cluster/reconcile-observability-storage.py` records the bounded
reservation/placement repair. CPU reservations are 20%, overprovisioning 100%,
minimum physical free 10%. Evaluate provisioned commitments separately from actual
filesystem use. `.66:/srv/models` is a different filesystem from Longhorn's root
volume and did not cause the former Loki allocation failure.

Remove a surplus replica only through the native Longhorn API after both intended
copies are healthy `RW`. Never delete historical model data to make a telemetry
volume schedulable. Keep two independent questions explicit: data copies and
application availability during process or volume recovery.

Three hosts share one physical failure domain by user decision. The existing
snapshot/restore drill proves an in-cluster recovery path; it does not prove
survival of a shared power, rack or site failure.
