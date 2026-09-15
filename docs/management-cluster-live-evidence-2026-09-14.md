# Management cluster live evidence — 2026-09-14

## Latest reconciliation — 17:01 CST and targeted follow-up

The new snapshot is `docs/evidence/cluster-readiness-2026-09-14-followup.json`;
the 16:04 file below remains a historical checkpoint. There are still 54 nodes,
53 Ready and 407 allocatable GPUs. All 29 Longhorn volumes are healthy; six new
two-replica MinIO staging claims account for the increase from 23.

The generic Control base and MarsLab validation overlay are reconciled. The full
deployment-contract suite passes, including rendered Barman supply-chain/RBAC
checks, legal listener addresses, exact Secret/ConfigMap references and explicit
unmet storage guarantees. Runtime ConfigMap h is live, both CPU Control replicas
are Ready with zero restarts, and the `.70` container runtime's expanded Stage
Finalizer ID matches its Pod UID. The site overlay is not canonical release closure.

All three new MinIO host-group member-loss cases passed synthetic read/write
checks. Final bucket cleanup initially failed and was recovered separately; the
nonzero runner exit is preserved in `docs/evidence/minio-ha-quorum-2026-09-14.json`.
Six new members and their two-copy volumes are healthy; no drill resources remain.
Original MinIO clients/data have not been cut over. See
`docs/minio-ha-validation-2026-09-14.md` for the admin-info timeout finding and
dedicated v3 quorum collector. 133 promtool fixtures pass and the collector/rules
are deployed. The accepted shared failure domain and R1–R6 scope remain unchanged.

## Historical reconciliation — 16:04 CST

The finite acceptance list is `docs/cluster-production-readiness-2026-09-14.md`,
with current JSON in `docs/evidence/cluster-readiness-2026-09-14.json`. The older
sections below are historical checkpoints, not additional open requirements.

- 54 nodes remain registered, 53 Ready, 407 allocatable GPUs; only `.19` is cordoned.
- Node-local authenticated control metrics, OTel internal metrics and Alloy local
  discovery/redaction are deployed and verified; monitoring Helm is revision 12.
  The old kube-proxy coverage gap is closed. 127 alert fixtures now pass.
- `.11` agent restart with `.70:6443/9345` blocked reached alternate control
  backends and `/readyz=ok` in 22.11 seconds. All 49 other reachable ordinary
  workers retain all three backend/cache endpoints and enabled/active agents.
  Cached-worker failover is proven; first bootstrap and external admin access
  remain separate checks. No new VIP is required merely to restate this proof.
- Default RKE2 NGINX ingress is disabled (Helm revision 15, Deployment 0/0), with
  no IngressClass/webhook. 53 Ready nodes had no reachable TCP 80/443; both APISIX
  gateways still return 200. No RKE2 or host restart was needed for this change.
- A Control replica crashed because its remounted sandbox became 02770 under
  `fsGroup`, violating the application's no-group-write guard. A disposable
  Longhorn test reproduced 02770 and then preserved 0700 without `fsGroup`.
  Removed the conflicting group policy, pinned the current image digest, and
  moved both Control replicas to the CPU nodes with a feasible 0-surge/1-unavailable
  rollout. Both are Ready with 0 restarts after the rollout.
- 23 Longhorn volumes are present; persistent data requires two copies, with
  exemptions only for the two disposable Control scratch claims. Loki has two
  healthy CPU copies. The misplaced monitoring-namespace Longhorn policy was
  removed while preserving all correct chart policies; metrics remain healthy.
- MinIO reports one four-drive set, read quorum 2/write quorum 3, with two members
  on `.66`. This exposes a real write-availability limitation on loss of that
  host. It is recorded for topology/recovery work, not falsely covered by the
  user's acceptance of a shared physical failure domain.
- The deployment-contract suite has seven pre-existing categories of failure;
  fixing the image pin exposed an existing port-name contract mismatch. Source
  base/overlay consistency, capacity guarantees and actual application release
  evidence remain unfinished; the cluster is not declared fully production-ready.

## Continuation reconciliation — 2026-09-14 14:00 CST

- APISIX was reconciled from the local pinned chart `/opt/vela-cluster/apisix-2.17.0.tgz` to Helm revision 14. The release now owns the complete default plugin list plus `opentelemetry`, the Prometheus plugin and its `plugin_attr` export on `0.0.0.0:9091`.
- The APISIX deployment uses `maxSurge=0` and `maxUnavailable=1`, so two replicas can roll safely across the two CPU management nodes. The three etcd members use only `vela.ai/node-role=control-storage`, allowing the accepted shared `.66` member without requiring the CPU-only management label.
- Prometheus reports both `apisix-metrics` targets `up=1`; the gateway `/grafana/login` smoke request returned HTTP 200 after the Helm reconciliation.
- The kube-prometheus-stack release is revision 11. Its kube-proxy scraper/rule is disabled, but live inspection confirmed metrics at `127.0.0.1:10249/metrics`. This historical target-path gap was subsequently closed by authenticated node-local relays (see latest reconciliation).
- Corrected cert-manager PDB selectors (`cainjector` and `webhook`) and operator rolling strategies were applied. The controller, cainjector and webhook each have two Ready replicas distributed across `.70` and `.71`, with one allowed disruption.
- Longhorn CSI recovery drill: a 1Gi `longhorn-wffc` PVC was written with marker `vela-longhorn-drill-20260914`, snapshotted through the `longhorn` VolumeSnapshotClass, restored to a new PVC, and read back successfully. The temporary namespace and volumes were deleted after verification.
- Loki's temporary single-copy configuration was corrected. The blocked second replica was caused by CPU-disk provisioned commitments and 30% Longhorn reservations; `.66:/srv/models` is a separate filesystem and was not the cause. CPU reservations are now 20% with 100% overprovisioning retained. Loki is pinned to `.70/.71` and both copies were verified running `RW`; the extra `.66` copy was removed through Longhorn only after both CPU copies were healthy. See the observability evidence for the reproducible repair and tests.

Source: direct checks through `marslab@100.111.196.116` and Ansible on
`10.1.201.70`, with `KUBECONFIG=/etc/rancher/rke2/rke2.yaml`.

## Earlier validation pass (2026-09-14)

- Registry TLS was reissued from the existing internal CA with SANs for
  `10.1.201.70`, `10.1.201.71`, `10.1.201.66`, `registry.vela.local`, and
  `registry`. From each management node, all 18 registry endpoints
  (`5000`--`5005` on all three hosts) returned HTTP 200 with the RKE2 CA.
- RKE2 `registries.yaml` now lists all three internal registry endpoints before
  the public China mirrors for Docker Hub, NVCR, GHCR, Quay, and
  `registry.k8s.io`; the release registry also has all three endpoints. The
  generated containerd `hosts.toml` on a worker contains the three internal
  endpoints and the CA path.
- The configuration was rolled to every reachable worker and the RKE2 agent
  was restarted with Ansible. The status audit reports 50 reachable workers
  `rke2-agent enabled/active`; `10.1.201.19` remains the sole unreachable,
  cordoned worker.
- A real worker pull using
  `/var/lib/rancher/rke2/bin/crictl --config /var/lib/rancher/rke2/agent/etc/crictl.yaml pull docker.io/library/busybox:1.36`
  completed successfully and resolved the expected image digest. This
  validates the live containerd mirror path rather than only the YAML files.
- Docker mirror configuration was installed on every reachable node that has
  Docker, with CA directories for all three registry hosts. Docker is
  `enabled/active` on all three control/storage nodes and reports the mirror
  configuration after restart. GPU workers use RKE2 containerd and do not have
  a Docker systemd service; their containerd mirror path is the authoritative
  runtime path.
- The cluster remains at 54 nodes: 53 `Ready`, one expected `NotReady` for
  `.19`; the NVIDIA device-plugin is `51/51 Ready`, allocatable NVIDIA GPU
  capacity is 407 across 51 nodes, and the only non-running platform Pod is
  the `.19` node-exporter Pending Pod. APISIX remains the only NodePort
  service (`30080`/`30443`), and Grafana returns 302/200 over HTTP and 200
  over HTTPS with the validation SNI.
- Protected hosts `10.1.201.56`, `10.1.201.57`, and `10.1.201.44` are outside
  the deployment inventory and were not rebooted. Future automation must
  retain an explicit exclusion for them. `10.1.201.66` is the accepted shared
  control/storage node and is excluded from further routine restarts.

- RKE2: 54 Kubernetes nodes total, 53 `Ready`; the 51 requested GPU worker
  addresses are registered and labelled `vela.ai/node-role=gpu-worker`.
- GPU inventory: 49 workers expose 8 GPUs, `10.1.201.59` exposes 7 GPUs, and
  `10.1.201.19` is unreachable and exposes no allocatable GPU resource.
- Boot persistence: reachable workers report `rke2-agent enabled/active`; the
  NVIDIA Toolkit and device-plugin DaemonSet are enabled and healthy.
- Registry: `.70`, `.71`, and `.66` each run the six restart-on-boot Docker
  Registry caches on ports `5000`–`5005` for Docker Hub, NVCR, GHCR, Quay,
  `registry.k8s.io`, and the internal release image repository. The mirror
  configuration was rolled to reachable workers with Ansible.
- Monitoring: Prometheus (50Gi, 15d), Alertmanager (10Gi), Grafana (20Gi),
  node-exporter, kube-state-metrics, and the GPU `nvidia-smi-exporter` scrape
  are healthy. Prometheus reports 52 GPU targets, 51 up and the unreachable
  `.19` target down. The `Vela GPU Fleet` Grafana dashboard is provisioned.
- Gateway: APISIX 3.18 runs two replicas on `.70/.71`, with a three-member
  Longhorn-backed etcd. NodePorts are HTTP `30080` and HTTPS `30443`; the
  `/grafana/*` route returns HTTP 200. HTTPS currently uses the interim
  `vela-gateway-default` certificate and requires an SNI hostname.
- Storage: 21 Longhorn volumes are healthy. All active replicas are now on
  `llmpool01`, `llmpool02`, or `marslab-gpu-01`; GPU-worker Longhorn disks are
  unschedulable with eviction requested. MinIO has four healthy members on the
  same three-node storage group.

## Earlier recorded physical and endpoint boundaries

- `10.1.201.19` (`server-36`) has no route/ARP from the jump and management
  nodes; it remains cordoned. Recovery requires host, NIC, switch, power, or
  BMC access, after which the RKE2 agent and GPU device-plugin state must be
  rechecked.
- `10.1.201.59` (`server-124`) has exactly seven NVIDIA PCI devices. Software
  reports the physical inventory correctly; the eighth GPU requires a hardware,
  slot, power, or BIOS investigation.
- Replace the interim APISIX certificate with the production PKI certificate
  before exposing a public DNS name.
- RKE2 agents currently use `https://10.1.201.70:9345` as their server
  endpoint for bootstrap. Later restart testing proved that joined workers use
  cached `.70/.71/.66` backends when `.70` is unavailable. First bootstrap and
  external administration remain separate checks; no unused VIP was invented.
- The three storage nodes share one physical failure domain. Replication inside
  Longhorn/MinIO does not survive a common rack, power, or network failure.

## Final validation pass (2026-09-13 21:10 UTC)

- The historical `vela-outbox` NATS JWT name was diagnosed and handled by the
  compatibility logic in `internal/natsauth/connector.go`; a rebuilt image was
  published as `10.1.201.70:5005/vela-control:compat-vela-outbox`. Both
  `vela-control` replicas are `1/1 Running` on `llmpool01` and `llmpool02`.
- Artifact-validation scratch volumes now use the `longhorn-wffc` StorageClass
  with `WaitForFirstConsumer` and one replica. Twenty-one active Longhorn
  volumes report `healthy`; disposable stale validation volumes are deleting.
- After restarting `llmpool02` and clearing only rebuildable pip cache on the
  shared GPU/control host, all three control-plane nodes are `Ready`. NATS is
  `3/3 Running` on the two CPU management nodes and the explicitly shared
  `marslab-gpu-01`; MinIO is `4/4 Running` across `marslab-gpu-01`, `llmpool02`,
  and `llmpool01`.
- The NATS StatefulSet now has `vela.ai/node-role=control-storage`, so a
  control-plane failure cannot place NATS on an ordinary GPU worker. The shared
  `.66` node is intentionally tolerated as the third control/storage member.
- APISIX `/grafana/login` returns HTTP `200` on both HTTP `30080` and HTTPS
  `30443` (with the validation SNI/certificate). The only active non-running
  platform pod is the expected node-exporter on unreachable `server-36`.
- Current inventory is 54 nodes, 53 `Ready`, 51 labelled GPU workers; 50 have
  allocatable NVIDIA resources. `server-124` exposes seven physical GPUs and
  `server-36` remains unreachable/cordoned.

## Final operational checks (2026-09-14)

- All reachable GPU workers, including `10.1.201.11`, report `rke2-agent`
  `enabled/active`; all three control/storage nodes report `rke2-server`
  `enabled/active`. The Ansible inventory on `10.1.201.70` now includes the
  `.11` worker as well.
- The NVIDIA device-plugin DaemonSet is `51/51` desired and ready. The only
  Pending platform pod is the node-exporter pod for unreachable
  `server-36`; there are no Failed pods.
- Grafana dashboard UID `vela-gpu-fleet` returns HTTP 200 from Grafana's API.
  `/grafana/` returns HTTP 302 and `/grafana/login` returns HTTP 200 through
  APISIX on both management nodes over ports `30080` and `30443`.
- The only Kubernetes NodePort is `apisix/apisix-gateway` (`30080`, `30443`);
  Grafana and all platform services remain `ClusterIP`.
- Longhorn currently reports 21 attached volumes as `healthy`; its backup
  target is `available=true`. MinIO remains `4/4 Running`.
- `marslab-gpu-01` has 133G free on the root/Longhorn filesystem (85% used);
  approximately 140G of historical experiment data remains under `/tmp` and
  was intentionally left untouched pending an owner-approved cleanup list.

## Final closure recheck (2026-09-13 22:56 UTC)

- CNPG reports `Cluster in healthy state`, `readyInstances=3`, and
  `currentPrimary=vela-postgres-2`. The three instances are on distinct hosts:
  `vela-postgres-1` on `llmpool02`, `vela-postgres-2` on `llmpool01`, and
  `vela-postgres-3` on `marslab-gpu-01`. The primary's `pg_stat_replication`
  reports both standbys as `streaming` with `sync_state=quorum`.
- APISIX has two gateway Pods and three healthy embedded-etcd members, one on
  each control/storage node. HTTP `/grafana/` returns `302`; HTTPS
  `/grafana/login` returns `200` when addressed with the validation SNI
  `apisix-gateway` on both `10.1.201.70:30443` and `10.1.201.71:30443`.
- NATS is `3/3 Running`. MinIO is `4/4 Running` and is distributed across all
  three storage nodes (`minio-0` on `.71`, `minio-1` and `minio-3` on `.70`,
  `minio-2` on `.66`). Longhorn reports 21 volumes with `robustness=healthy`
  and backup target `default available=true`.
- The GPU device-plugin DaemonSet is `51/51 Ready`. The only non-running
  workload Pod is the expected node-exporter Pod for unreachable `server-36`
  (`10.1.201.19`); it remains cordoned. No other Failed or Unknown workload
  Pods remain after deleting the stale `.66` `vela-control` Pod and its
  orphaned disposable artifact-validation volume.
- After restarting `.66`'s `rke2-server`, the service is `enabled` and
  `active`, and `lsof +L1 | grep rke2.old` is empty. The `.66` root filesystem
  remains at 77% used with approximately 194G free; historical `/tmp` data was
  not removed.

## Rolling restart and boot-path hardening (2026-09-13 23:13 UTC)

- The RKE2 conditional image-import cache is enabled on all three server nodes
  at `/var/lib/rancher/rke2/agent/images/.cache.json` (`0600`, root-owned).
  A subsequent `.70` restart imported only the small per-component image
  references; the previous 1m28s bulk `rke2-images.linux-amd64.tar.zst` import
  was skipped. The node returned to `Ready` about 94 seconds after the restart
  began. The later whole-host reboot re-imported the 1m30s archive during the
  early boot path despite the cache, so the measured full-reboot recovery bound
  remains below five minutes but the cache should not be treated as a guarantee
  for power-cycle startup. The cache is an optimization only; if an image is
  manually pruned, it must be re-imported or the cache invalidated before
  restarting.
- `.70`, `.71`, and `.66` were restarted one at a time after enabling the
  cache. Each `rke2-server` is `enabled/active`, all three control-plane nodes
  are `Ready`, and the final PostgreSQL, NATS, MinIO, APISIX/etcd, and Longhorn
  checks remained healthy.
- `.70` had three unused NetworkManager Ethernet profiles that caused
  `NetworkManager-wait-online.service` to fail. Their autoconnect flag is now
  disabled by UUID; the active `eno1` profile is unchanged and
  `NetworkManager-wait-online.service` is `active`.
- A historical failed state for `.66`'s `systemd-networkd-wait-online.service`
  was reset after confirming `enp34s0f0` is routable; the service is now
  `active`. A three-node systemd audit reports no failed units. Current root
  filesystem use is approximately 12% on `.70`, 20% on `.71`, and 77% on
  `.66`.

## Whole-host failover drill (2026-09-13 23:16--23:25 UTC)

- A real `.70` host reboot was issued through the jump host. During the outage
  `llmpool01` became `NotReady` while the other two control-plane members
  remained available. CloudNativePG entered failover and selected
  `vela-postgres-1` on `llmpool02` as the new primary; after `.70` returned,
  `vela-postgres-2` was recreated and the cluster returned to `3/3` healthy.
- After recovery, the primary's `pg_stat_replication` showed both
  `vela-postgres-2` and `vela-postgres-3` in `streaming` with
  `sync_state=quorum`. APISIX etcd returned to three running members, NATS
  returned to three running members, and MinIO returned to four running
  members distributed across `.70/.71/.66`.
- MinIO's `/minio/health/live` and `/minio/health/cluster` endpoints both
  returned HTTP `200` after the host recovered. No `Unknown`, `CrashLoopBackOff`,
  `Error`, or `Failed` workload Pods remained. This is evidence for a single
  host loss and recovery within the accepted shared failure domain; it does not
  remove the separate-site backup and physical-domain limitations recorded
  above.

## Post-recovery audit (2026-09-13 23:38 UTC)

- The three control/storage nodes are `Ready`, with `DiskPressure=False`; the
  live CNPG primary is now `vela-postgres-1` on `llmpool02`, with both other
  instances healthy. APISIX gateway/etcd, NATS, MinIO, and all 21 Longhorn
  volumes are running/healthy.
- Monitoring remains deployed with Prometheus, Alertmanager, Grafana, and 31
  PrometheusRule objects. The sole Pending monitoring Pod is the expected
  node-exporter on the unreachable, cordoned `server-36` (`10.1.201.19`).
- A fresh three-node systemd audit reports no failed units. Root filesystem
  usage is approximately 12% (`.70`), 20% (`.71`), and 77% (`.66`); inode use
  remains below 6% on all three nodes.

## Continuation audit (2026-09-14 01:44 UTC)

- After the worker rollout, the cluster still reports 54 nodes, 53 `Ready`,
  and only the expected `.19` `NotReady,SchedulingDisabled` node. No workload
  Pod is Failed, Unknown, or CrashLoopBackOff; the only non-running Pod is the
  `.19` node-exporter Pending Pod.
- CloudNativePG reports `Cluster in healthy state`, `readyInstances=3`, and
  `Continuous archiving is working`. The immediate scheduled backup resource
  `vela-postgres-daily-20260913133318` is `completed`.
- NATS is `3/3 Running`, MinIO is `4/4 Running`, APISIX and its three etcd
  members are running, and all 21 Longhorn volumes are `attached/healthy`.
  Longhorn's default backup target reports `available=true`.
- The three Registry hosts expose the reissued certificate through December
  2028 with all three IP SANs. Docker reports all three internal mirrors plus
  the configured public mirrors on `.70/.71/.66`; worker containerd generated
  matching three-endpoint `hosts.toml` files.
- APISIX Grafana login returns HTTP 200 over HTTPS with SNI `apisix-gateway`
  on both management nodes. The remaining warning events are transient APISIX
  etcd readiness retries and Kubernetes DNS nameserver truncation notices; the
  corresponding Pods are currently Ready.
- Current root filesystem capacity is approximately 737G free on `.70` (12%),
  695G free on `.71` (21%), and 190G free on `.66` (78% used); inode use is
  1%, 2%, and 6% respectively. The shared GPU/control node remains the storage
  capacity bottleneck, so new Longhorn replicas should continue to prefer the
  two CPU management nodes.
- Kubernetes `/readyz?verbose` passes, all three control/storage nodes carry
  `vela.ai/node-role=control-storage`, and all 51 requested worker IPs carry
  `vela.ai/node-role=gpu-worker`. The shared `.66` control-plane taint remains
  `PreferNoSchedule`, while the two CPU management nodes remain preferred for
  control services.
- Current control-plane workloads are confined to the accepted control/storage
  group: Vela Control runs on `.71` and shared `.66`, NATS and PostgreSQL span
  `.70/.71/.66`, and no ordinary GPU worker hosts management workloads.
- A fresh read-only probe from both the jump host and `.70` routes `.19` via
  the expected interface but gets no ICMP response and an `INCOMPLETE` ARP
  neighbor. This narrows the remaining worker issue to host/NIC/switch/power
  or BMC access; no Kubernetes-side repair can make that node Ready.
- Protected `.44`, `.56`, `.57`, and shared `.66` were only queried for
  hostname/uptime in this pass; no reboot or service restart was issued to
  those hosts.

## Protected-host boundary reaffirmed

The operator has reaffirmed that `10.1.201.44`, `10.1.201.56`,
`10.1.201.57`, and `10.1.201.66` must never be rebooted. The Ansible inventory
now records all four under `protected_no_reboot`, and the GPU toolkit playbook
fails closed if a protected address is ever included in its target set. The
current live read-only check shows `.66` is `Ready`, `rke2-server` and Docker
are `enabled/active`, with no failed systemd units; its root filesystem is
approximately 78% used with 187G available. No operation in this update
restarted or rebooted any protected host.

## Continuation audit

- A fresh Kubernetes audit still reports 54 nodes, 53 `Ready`, and the single
  expected `Pending` node-exporter Pod on cordoned `server-36`
  (`10.1.201.19`). All Deployments, StatefulSets, and the NVIDIA
  device-plugin DaemonSet report their desired replicas ready; no Failed or
  Unknown workload Pods were found.
- Kubernetes `/readyz?verbose` passes. Longhorn reports 21 attached volumes as
  `healthy`, and the `default` backup target reports `available=true`.
- A new SSH attempt to `10.1.201.19` through the jump host fails with
  `No route to host`; the management node also reports an `INCOMPLETE` ARP
  neighbor and `Destination Host Unreachable`. This remains a physical network
  or host availability issue, outside Kubernetes remediation.
- APISIX Grafana login returned HTTP `200` from both management nodes over
  ports `30080` and `30443` using the validation SNI. Registry API probes on
  `10.1.201.70:5000` and `:5005` also returned HTTP `200`.
- The APISIX etcd container healthcheck was run directly on `apisix-etcd-1`
  five times over 14 seconds; all five checks returned `rc=0` with commit
  latency between 2.7ms and 4.3ms. Earlier readiness warnings were transient
  startup probes; the current three-member etcd cluster is healthy.
- A throttled direct SSH audit of the worker inventory found `rke2-agent`
  `active` on all 50 reachable workers; the only failed connection was the
  already isolated `.19` host. Parallel SSH was deliberately avoided because
  the jump host rate-limits bursts.
- The same throttled audit found `rke2-agent` `enabled` on those same 50
  reachable workers, confirming boot persistence without touching the
  protected hosts.
- A read-only privileged probe of each reachable worker found the generated
  Docker Hub containerd `hosts.toml` and the `10.1.201.70:5000` internal mirror
  entry on all 50 hosts. The unreachable `.19` was excluded from the probe and
  remains the sole node requiring physical recovery.
- The NVIDIA containerd runtime drop-in (`99-nvidia.toml`, including
  `BinaryName`) is present on all 50 reachable workers. This confirms the GPU
  runtime configuration reached every currently accessible worker.
