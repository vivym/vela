# Three-node management cluster

This bundle describes the accepted three-node management topology. The nodes
share one physical failure domain; Longhorn and MinIO provide replication
inside this cluster only.

| Node | Role | Scheduling intent |
| --- | --- | --- |
| `llmpool01` (`10.1.201.70`) | RKE2 server + etcd | Preferred management/control services |
| `llmpool02` (`10.1.201.71`) | RKE2 server + etcd | Preferred management/control services |
| `marslab-gpu-01` (`10.1.201.66`) | RKE2 server + etcd + GPU worker | Control-plane failover and GPU workloads |

The third server preserves an etcd majority when either CPU management node is
unavailable. The shared host retains a control-plane taint; intentional shared workloads
use explicit selectors/tolerations. Ordinary management applications prefer the
two CPU nodes; cluster quorum/storage members may use the accepted shared host. This shared GPU/control-plane role is an
accepted capacity constraint because no third CPU-only host is available.

`nvidia-device-plugin.yaml` is restricted to nodes labelled
`vela.ai/gpu-role=worker`, uses the RKE2 `nvidia` RuntimeClass, and pins the
image digest served by the internal NVCR pull-through cache. A one-GPU smoke
Pod produced `nvidia-smi` output on `marslab-gpu-01` on 2026-09-13.

`registries.yaml` is the RKE2 mirror configuration installed on all control
and worker nodes. Each mirror lists the three internal cache endpoints on
`.70`, `.71`, and `.66` before the public fallbacks: Docker Hub (`:5000`),
NVCR (`:5001`), GHCR (`:5002`), Quay (`:5003`), Kubernetes registry (`:5004`),
and the internal release registry (`:5005`). The CA file referenced by it is
installed at `/etc/rancher/rke2/registry-ca.crt` before restarting RKE2. The
three registry caches share the accepted physical failure domain. The release
endpoints on port 5005 now enforce scoped authentication, and actual Kubernetes
mirror fallback with imagePullSecrets has passed. See
[`registry-access`](../registry-access/README.md) for credentials, promotion and
rollback. Future image replication, backup and garbage collection remain explicit
release operations. The absence of an independent physical failure domain is already
accepted by the user, not a request for additional infrastructure.

Longhorn is the storage class for PostgreSQL, NATS, and MinIO. Its data path is
the host root filesystem (`/var/lib/longhorn`), separate from the GPU model
mount at `/srv/models`; the default two replicas are spread across eligible
nodes. This is cluster-internal replication only: a rack, power, network, or
site failure affecting all three hosts loses both copies. The GPU host is the
capacity bottleneck, so do not treat `/srv/models` or the Longhorn pool as
unbounded storage. Longhorn disk scheduling is restricted to the three accepted
storage nodes by `configure-longhorn-placement.sh`; GPU workers are excluded
from control-plane data placement.

`local-path-storage.yaml` installs a narrowly mapped `local-path` StorageClass
for validation and small control-plane data. It is not a production storage
class: it has no replication, capacity enforcement, independent failure domain,
or backup target.

`install-platform-operators.sh` is the repeatable operator bootstrap. It
downloads and verifies the pinned cert-manager and CNPG manifests, applies the
RBAC-hardened Barman overlay, installs the validation StorageClass, and applies
the CPU-node placement and digest image policies. It requires an already
configured `KUBECONFIG` and internal registry CA; it does not create Secrets.

The live RKE2 configuration reserves `8 CPU/32Gi + 4 CPU/16Gi` on the GPU
host and `4 CPU/8Gi + 2 CPU/4Gi` on each CPU node for host and Kubernetes
services respectively. Keep these reservations when reproducing the cluster;
they are required for shared GPU/control-plane operation.

`configure-longhorn-placement.sh` restricts Longhorn disk scheduling to the
accepted `llmpool01`, `llmpool02`, and `marslab-gpu-01` group and requests
replica migration from GPU workers. Keep the generated node and replica backup
files with the rollout evidence. The shared GPU host is currently near its
Longhorn free-space guard, so new replicas will normally land on the two
CPU management nodes.
