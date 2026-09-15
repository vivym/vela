# Management cluster live evidence

Date: 2026-09-13. The evidence below was collected through the approved
`marslab` jump host after the GPU host was reused as the third RKE2 server.

## Kubernetes foundation

All three nodes report `Ready`, `control-plane,etcd`, and
`v1.35.7+rke2r1`:

```text
llmpool01        Ready   control-plane,etcd   10.1.201.70
llmpool02        Ready   control-plane,etcd   10.1.201.71
marslab-gpu-01   Ready   control-plane,etcd   10.1.201.66
```

The three etcd Pods are `Running`. The API `/readyz?verbose` check passed,
including `etcd`, `etcd-readiness`, and the encryption-provider checks.
After restarting the `.71` Canal Pod to clear a stale duplicate Felix health
listener, all three Canal Pods report `2/2 Running` and all nodes are `Ready`.

`cert-manager v1.21.1` (`sha256:5f6a499b8c1857d57f560f536e0dcc830914b45c420899fe7ad0692c8624e408`),
CloudNativePG `v1.30.0`
(`sha256:f8bede43fe4ee0d478c2355b204a36876b2ae4faac60f2a9452280b293da3b88`),
and the Barman Cloud Plugin `v0.14.0`
(`sha256:8d4f1719cc54891ddffd7633279ec93b5d2cc547df8684c3b84f3b156a615e7c`)
are installed from SHA-256-verified upstream manifests. Their
operator images use the recorded digests. cert-manager, CNPG, and Barman each
run two replicas across `llmpool01` and `llmpool02`; Barman's generated client
and server Certificates are `Ready`. The Barman source manifest is retained at
`deploy/control-storage/barman-cloud-plugin-install/manifest.yaml` and its
RBAC-hardening kustomization renders successfully. The Barman sidecar image is
also pinned to
`sha256:9880817c285c7afa4d195da2145064d21907405489ed6ec39abe59b1feb558a4`;
both Barman replicas were restarted and report the digest at runtime.
PodDisruptionBudgets with `minAvailable: 1` cover all five operator
Deployments.

The internal registry host now exposes Docker Hub, NVCR, GHCR, and Quay
pull-through caches on ports `5000` through `5003`; the GHCR and Quay endpoints
returned HTTP 200 for `/v2/` after their RKE2 configuration was installed on all
nodes. These are
single-host caches and still require authentication, backup, and failover work
before they can be treated as production supply-chain infrastructure.

## GPU runtime

`nvidia-device-plugin-daemonset` is restricted to
`vela.ai/gpu-role=worker`, uses RuntimeClass `nvidia`, and runs from the
internal NVCR cache at `10.1.201.70:5001`. Its image is pinned to

```text
sha256:630596340f8e83aa10b0bc13a46db76772e31b7dccfc34d3a4e41ab7e0aa6117
```

The plugin registered `nvidia.com/gpu`; `marslab-gpu-01` reports 8 allocatable
GPUs. A one-GPU Pod completed `nvidia-smi` successfully and reported driver
`580.159.03` and CUDA `13.0`. The smoke Pod was deleted after verification.

The RKE2 kubelets now reserve resources for the host and Kubernetes control
plane. `marslab-gpu-01` reserves 8 CPU, 32Gi memory, and 20Gi ephemeral
storage for the host plus 4 CPU and 16Gi for Kubernetes; each CPU management
node reserves 4 CPU/8Gi plus 2 CPU/4Gi respectively. Memory and filesystem
eviction thresholds are configured. Each node returned `Ready` and retained a
healthy etcd Pod after its sequential restart.

An explicit etcd snapshot was also saved on each control-plane history path;
the GPU node now has
`vela-preprod-marslab-gpu-01-20260913-marslab-gpu-01-1789299545` (about 20MiB),
and the CPU nodes retain their scheduled/manual snapshots. These are local
snapshots only; an off-host encrypted copy and restore drill are still required.

## Storage and production boundary

The cluster now has a `local-path` StorageClass with `WaitForFirstConsumer`.
Its controller is pinned to `llmpool01`; node paths are explicitly mapped for
the two CPU nodes and the GPU host. A 1Gi PVC and Pod completed successfully on
`llmpool01`, then the smoke namespace was deleted. This proves provisioning and
mounting only. A second 1Gi PVC/Pod completed on `marslab-gpu-01` with its
control-plane toleration, proving the mapped GPU-host path is usable when a
workload explicitly opts into the shared node. Local host-path storage is not
replicated or independently durable. The provisioner image is pinned to
`sha256:e757967a5ec338f6a9b371c5a9688bedaa8c3578ea3dd4db329ea0084be0a86f`.

The GPU host has about 199G free on root and 179G free on `/srv/models`; it must
not be used as an unbounded PostgreSQL, NATS, or object-storage data disk. The
repository's production control/storage contract still requires independent
durable storage, external S3/PITR, immutable release Secrets, and live
failure/restore receipts.

On 2026-09-13, the GPU host's 8GiB `/swap.img` (about 26MiB used) was disabled
with `swapoff -a`; its fstab entry was commented and
`/etc/fstab.codex-pre-swapoff` was retained as a rollback copy. `rke2-server`
remained active and the three nodes remained `Ready` after the change.

The repository's disposable CNPG/Barman PITR conformance drill also completed
on 2026-09-13 using the pinned manifests and image digests. It created a
three-instance PostgreSQL source, completed a plugin base backup, restored to a
timestamp between marker writes (`before=1`, `after=0`), archived WAL, and
verified that the Barman operator and PostgreSQL service accounts could read
only `vela-backup-s3` and could not read Artifact credentials. The drill used
local MinIO in kind; the raw receipt is retained at
`docs/evidence/cnpg-barman-pitr-conformance-2026-09-13.log`. It is release-path
evidence, not evidence of the remote cluster's independent storage or backup
failure domain.

This evidence proves the three-node RKE2 foundation and GPU scheduling path. It
does not advance any of the nine Production Gates or constitute a production
Launch Receipt.

## Shared Longhorn and MinIO validation

Longhorn `v1.12.1` is healthy on all three nodes. Its default StorageClass is
configured with `numberOfReplicas: "2"`; the default backup target is the
internal MinIO service and reports `available: true`. The PostgreSQL, NATS, and
MinIO volumes observed during this validation are `attached` and `healthy` with
two replicas. This is cluster-internal replication across the three hosts; the
hosts share one failure domain, so it does not provide site or rack failure
independence. A MinIO PodDisruptionBudget requires at least three of four
instances during voluntary disruption.

Longhorn currently reports approximately 801Gi available on `llmpool01`,
756Gi on `llmpool02`, and 217Gi on `marslab-gpu-01` after its reserved-space
policy. The GPU host is therefore the capacity bottleneck; model data on
`/srv/models` remains outside the Longhorn data path and is already about 90%
full.

During the final host preflight, `cryptsetup` was installed on all three
nodes, `dm_crypt` was loaded and persisted in `/etc/modules`, and the GPU
host's unused `multipathd` service was stopped and masked. Longhorn manager was
then rolled out to refresh host probes. All three Longhorn Nodes now report
`Ready=True`, `Schedulable=True`, `RequiredPackages=True`,
`KernelModulesLoaded=True`, and `Multipathd=True`; all observed volumes remain
`attached/healthy`. The three `rke2-server` services remained active throughout.

The MinIO StatefulSet was then reconciled from the repository bundle with a
hard topology spread constraint and the `control-storage` node selector. Its
RWO PVCs completed a sequential detach/attach rollout; the final placement is
`minio-0` on `llmpool01`, `minio-1` on `llmpool02`, and `minio-2`/`minio-3` on
`marslab-gpu-01`. The MinIO admin report shows `Network: 4/4 OK`, `Drives: 1/1
OK` for each member, `4 drives online`, and `EC:2`. A write/read checksum probe
through `mc` passed after the rollout.

The four-pod `object-store` MinIO StatefulSet is Ready. Its hard topology
spread constraint currently places one pod on each CPU node and two on the
shared GPU/control-plane node. `/minio/health/live` and
`/minio/health/cluster` both returned HTTP 200 after the rollout, and a
write/read checksum probe through the MinIO client passed. The live PostgreSQL
Barman ObjectStore uses `http://minio.object-store.svc.cluster.local:9000`; its
immediate backup completed (`backupId=20260913T134150`) and WAL archive entries
were written successfully.

## Control data plane

NATS has three Ready pods, one per node, with JetStream storage on 50Gi
Longhorn PVCs. Its monitoring endpoint reports a three-member `VELA` metadata
cluster with a leader and zero pending peers. The live StatefulSet now requires
TLS on client and route connections and loads operator/account JWT resolver
data plus separate Outbox, Scheduler, and bootstrap NKey credentials from
operator-managed Secrets. The server certificate covers all three headless Pod
DNS names and has server/client authentication usage. A coordinated full
rollout was used so no old plaintext route peer remained. The application client
certificate is held separately in `nats-client-tls`.
The NATS, MinIO root, PostgreSQL backup, and Longhorn backup credential Secrets
are marked immutable; rotation therefore requires a new versioned Secret and a
coordinated rollout.

CloudNativePG `vela-postgres` has three Ready instances, each with 50Gi data
and 20Gi WAL Longhorn PVCs, distributed across the three nodes. The primary
reports both standbys as `streaming` and `sync_state=quorum`. A write on the
primary was read back from a standby. The smoke table was removed after the
check. These checks demonstrate the replicated validation setup, while a
separate off-cluster backup and restore receipt remains outstanding.

## Recovery checks

Deleting `vela-postgres-2` caused CNPG to recreate it on the same eligible
node; the cluster returned to three Ready instances and both standbys again
reported `streaming / sync_state=quorum`. Deleting `nats-1` likewise recreated
the Pod and returned the JetStream metadata cluster to three current peers with
`pending=0`. These are single-Pod recovery checks; they do not prove recovery
from simultaneous host loss or loss of the shared failure domain.

## Production closure blockers

The infrastructure layer is ready for controlled application deployment, but
the repository's nine Production Gates remain `0/9`. Closing the remaining
gates still requires a real customer/platform OIDC issuer, a published
digest-pinned `vela-control` release, versioned application Secrets and PKI,
an API VIP or equivalent managed load balancer, authenticated registry
operations with backup/garbage-collection policy, and an external backup
domain. Real H3/GPU soak, remediation, commercial integrations, and paging
delivery also require their external owners and acceptance thresholds. These
inputs are intentionally absent from the repository and cannot be safely
invented on the three-node validation cluster.
