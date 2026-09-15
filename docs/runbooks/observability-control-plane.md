# Control-plane and node-local metrics

The `node-local-metrics-*` DaemonSets read RKE2's original loopback endpoints:
`10249` for kube-proxy on each node, and `10257`, `10259`, `2381` for
controller-manager, scheduler and etcd on the three control nodes. No RKE2
listener is widened. OTel exports only on loopback `19106`; the node IP exposes
TLS/RBAC proxy port `19105`. Unauthenticated requests receive 401, unauthorized
ServiceAccounts 403, and the Prometheus ServiceAccount receives 200.

Before adding a recovered/new node, check ports `19105`–`19108`, then enable
the `vela.ai/node-metrics=enabled` node label. `.19` is still offline and has
not passed this preflight. `.44/.56/.57` remain outside the deployment scope;
`.66` must not be rebooted or have its driver reloaded.

For a missing node signal, inspect node readiness and the two containers in its
metrics Pod. Inspect the node-metrics Certificate, Issuer and Secret if the
relay is running but Prometheus receives a TLS or authentication error. Never
disable verification or grant cluster-admin to make a scrape pass. The native
receiver ServiceAccount has GET `/metrics` and delegated authentication only.

For control component failure, check all three individual `up` series. Etcd
members must each see a leader; sustained WAL fsync p99 above 250 ms requires
checking disk contention. Avoid restarting another control node while quorum
or leader visibility is impaired.

The Prometheus `rke2-node-metrics` scrape class reads its projected, rotating
ServiceAccount token. It is installed by
`deploy/cluster-platform/monitor-observability-reconcile-values.yaml`; install
that overlay before the PodMonitor. cert-manager renews the proxy certificate.
The dedicated CA has a ten-year lifetime; plan an overlapping CA trust rollover
before its renewal window, rather than replacing the only trusted CA in place.
