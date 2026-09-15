# Node storage pressure

Use `node_filesystem_avail_bytes`, Longhorn volume state and the node's local
disk report to identify the filesystem. Stop image churn and temporary artifact
creation first, then reclaim only approved data. Never delete Longhorn data or
model files manually. Verify free space and healthy replicas before uncordoning.

`VelaNodeRootFilesystemHeadroomLow` warns below 20% available for five minutes.
The general 10% filesystem warning is too late when imagefs shares root and
kubelet uses its default 15% imagefs eviction threshold. `VelaNodeDiskPressure`
pages after one minute of the actual kubelet condition; inode pressure and
non-root image filesystems also require investigation.

Compare `/api/v1/nodes/<node>/proxy/stats/summary` node fs and runtime imageFs
with `statvfs`/`df`, and inspect kubelet eviction events. Check process cwd,
executables, open files, mappings, environment path references and container
bind mounts before reclaiming build caches. Keep a manifest and preserve
source, validation evidence, model data and active jobs. Do not remove the
disk-pressure taint or lower eviction thresholds to force scheduling. Wait for
the configured eviction minimum reclaim target and kubelet's pressure transition
period, then verify NATS/PostgreSQL/etcd members,
MinIO quorum and Longhorn attachment/replica health. A return above 15% is
incident recovery only; maintain at least 20% root headroom before planned
control-plane maintenance.

## ZFS datasets and pools

`VelaZFSFilesystemLow` warns when a ZFS filesystem has less than 15% available;
`VelaZFSFilesystemCritical` pages below 10%. These alerts use the node-exporter
`fstype="zfs"` filesystem series and identify the affected `instance` and
`mountpoint`. Correlate them with `zpool list -p`, `zfs list -o
name,used,avail,refer,mountpoint,quota`, and the node's disk inventory before
taking action.

On `.66`, the `data` pool is shared by `/srv/models`, `/data`, and other
datasets. Its measured free space is about 181 GiB (roughly 9%), which is below
the critical threshold. Do not move `/tmp`, containerd, kubelet, or model data
into this pool as a quick fix: that would trade root pressure for a full pool
and could make model and stateful workloads fail together. Keep at least 15–20%
pool headroom before adding new replicas, caches, Longhorn, or MinIO data.

For recovery, stop new cache/model writes, determine the owning dataset and
retention policy, and reclaim only explicitly approved temporary artifacts.
Never run `zpool destroy`, `zfs destroy`, `zpool set`, or format a backing disk
as part of alert handling. Verify `zpool status` is `ONLINE`, dataset mounts are
present, and stateful service quorum is healthy before resuming writes. A
filesystem alert clearing does not prove pool-wide capacity or data safety; the
pool-level `zpool list` result remains the authority for the operating margin.

On `.66`, the 2026-09-15 effective config has 15% imagefs eviction plus 10%
minimum reclaim, so recovery after triggering requires about 25% free (219.42
GiB on this root filesystem), followed by five minutes without pressure. Merely
returning above 15% does not clear the latched reclaim requirement. Read configz
for the current values; do not assume defaults from an earlier configuration.
