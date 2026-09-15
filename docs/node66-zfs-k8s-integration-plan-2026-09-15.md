# `.66` ZFS 与 Kubernetes 集成方案（验证阶段）

日期：2026-09-15  
节点：`10.1.201.66`（`marslab-gpu-01`，GPU worker 与控制面共用）

## 判断

可以使用 ZFS 数据盘，但用途应限定为 `.66` 的节点本地模型、可重建缓存和验证临时目录。当前不适合把它注册成 Kubernetes 的通用持久化存储，也不应把 Longhorn、MinIO、PostgreSQL、NATS 或 APISIX etcd 的副本迁到这个 pool。

现场只读核对（2026-09-15 10:17 CST）显示：`data` 的物理 vdev 是单块 `/dev/disk/by-id/nvme-ZHITAI_TiPlus7100s_2TB_ZTA82T0AB261025GP-part1`，没有 mirror/RAIDZ 冗余；pool `SIZE=2,044,404,432,896`、`ALLOC=1,799,969,121,792`、`FREE=244,435,311,104`、`CAP=88%`，状态 `ONLINE`，最近 scrub 无错误。dataset `data` 的可用量约 181,084,879,360 字节（约 168.6 GiB），`/srv/models` 约 169 GiB 可用（约 91% 已用）。`data/models` 与 `/data` 共享同一个 pool。ZFS pool 低于 10% 时，写放大、快照保留和后台重平衡都会迅速压缩可用空间。单盘故障会同时影响该节点上的所有 ZFS 数据，ZFS 本身没有提供数据副本。

现场登录后报告的主机名为 `marslab-server`，而 Kubernetes/现有文档把该地址称为 `marslab-gpu-01`；在创建 node-local PV 前必须核对并统一节点名、hostname 和 RKE2 `node-name`，避免 PV 的 `nodeAffinity` 绑定到错误身份。

根文件系统现场为约 111 GiB 可用（约 87% 已用），这是当前 `DiskPressure` 的主要来源。`data/docker` 是单独的 196 GiB zvol，已挂载到 `/var/lib/containerd`，该挂载点约 125 GiB 可用；把 Kubernetes 临时目录再搬进 ZFS 不会解决根盘压力，反而会让模型和平台状态争用同一个 pool。

## 目标布局

保持现有 `data/models` 不动，先只读使用。容量恢复后，再创建有边界的子 dataset：

```text
data/models       -> /srv/models       # 已有模型，业务清单管理
data/cache        -> /srv/vela/cache   # 可删除缓存，设置 quota 和 TTL
data/work         -> /srv/vela/work    # 验证/编解码临时产物，设置 quota 和 TTL
data/registry-cache -> /srv/vela/registry-cache  # 可选，限制为小容量
```

新 dataset 建议使用 `compression=lz4`、`atime=off`、`dedup=off`、`sync=standard`。不要为缓存设置无限 quota，也不要为模型目录设置会导致写入失败的临时 quota；模型删除必须通过模型清单和业务审批完成。

建议的初始配额（仅在 pool 恢复到至少 20% 空闲后采用）：

| dataset | quota | 用途 |
| --- | ---: | --- |
| `data/cache` | 80 GiB | Hugging Face、镜像层和编译缓存 |
| `data/work` | 40 GiB | 视频编解码和验证临时文件 |
| `data/registry-cache` | 20 GiB | 可选的节点本地镜像缓存 |

这些是上限，不是预留容量。达到 80% quota 时触发 warning，达到 95% 时暂停写入并清理带 TTL 的对象。

## Kubernetes 接入方式

验证阶段采用静态 `local` PV，避免在压力状态下引入新的 ZFS CSI 控制器：

1. 为 `.66` 打上 `storage.vela.ai/zfs-models=true` 和 `storage.vela.ai/zfs-cache=true` 标签。
2. 为 `/srv/models`、`/srv/vela/cache`、`/srv/vela/work` 分别创建带 `nodeAffinity` 的 `PersistentVolume`，`storageClassName=zfs-local`，`volumeBindingMode=WaitForFirstConsumer`，`reclaimPolicy=Retain`。
3. 业务容器将模型 PVC 以 `readOnly: true` 挂载；缓存和 work PVC 使用 `ReadWriteOnce`，并在 Pod 级别加 `.66` 的节点亲和性和 GPU/模型工作负载所需的 toleration。
4. 普通平台有状态服务继续使用 Longhorn 的控制节点策略；不能通过 toleration 让它们绕过 `.66` 的 `DiskPressure`。

只有在静态 PV 方案完成故障、重启和挂载缺失演练后，才考虑 OpenEBS ZFS LocalPV 或 democratic-csi。即便采用 CSI，它仍是单节点本地卷，不能提供跨节点高可用。

## 不能做的事情

- 不把 `/tmp`、`/var/lib/kubelet`、RKE2 数据目录或 containerd 临时目录 bind mount 到 `data`。
- 不把 Longhorn replica、MinIO erasure set、PostgreSQL/NATS/APISIX etcd 数据目录放进 `data/models` 或同一 pool 的新目录。
- 不执行 `zpool add`、`zpool destroy`、`zfs destroy`、格式化或无快照迁移。
- 不把 `DiskPressure` taint 删除或降低 kubelet eviction 阈值来强行调度。

## 分阶段执行门槛

### 阶段 0：只读盘点（已完成，可复查）

以下命令已在 `.66` 以只读方式执行；结果确认当前 pool 为单盘 vdev。后续扩容或迁移前仍需保存完整输出和设备序列号：

```bash
zpool status -P data
zpool list -p -v data
zfs list -r -o name,used,avail,refer,logicalused,compressratio,mountpoint,quota,reservation data
zfs get -r compression,recordsize,atime,dedup,sync data
findmnt -T /srv/models -T /data -T /var/lib/containerd
```

### 阶段 1：容量恢复

在 pool 达到至少 15% 空闲前，暂停新增模型、缓存和本地 registry 写入；目标是 20% 空闲再创建新 dataset。按现场 `zpool list` 的物理容量计算，达到 15% 需要约 306.7 GB free，当前约 244.4 GB，至少要增加约 62 GB 物理空闲；达到 20% 需要约 408.9 GB，至少要增加约 164.5 GB。dataset 可用量还要扣除 ZFS 保留空间，因此以 `zpool list` 为准。只清理有清单、有 TTL、明确获批的临时产物；不得删除模型、Longhorn 或容器运行时数据。

根盘应单独治理：优先扩大系统盘/LVM，或在获批后清理已确认的系统日志和构建缓存，使根盘恢复到至少 20% 可用。ZFS 方案不能替代这一步。

### 阶段 2：接入和演练

创建 dataset、PV/PVC 后，执行挂载缺失、节点重启后的自动挂载（不在 `.66` 上强制重启）、只读模型校验、quota 触发和 Pod 重新调度演练。模型目录必须能从原始制品仓库按 checksum 重建；缓存和 work 目录必须能安全删除。

### 阶段 3：扩容或迁移

长期应更换为 mirror/RAIDZ 拓扑或迁移到具备冗余的存储，并使 pool 保持 15–20% 以上空闲。当前单盘 pool 不应通过盲目 `zpool add` 再添加单个 vdev 来“扩容”，因为布局和故障容忍度不可逆。扩容或迁移前先确认硬件拓扑和备份；迁移使用快照、`zfs send/receive`、校验和比对和回滚点，完成后再调整挂载点。

## 可观测性要求

现有 `VelaZFSFilesystemLow`（15%）和 `VelaZFSFilesystemCritical`（10%）已覆盖 `/srv/models`、`/data` 文件系统，但还不能代表 pool 健康。应补充受限的 pool exporter 或 node-exporter textfile collector，仅允许读取：

- `zpool health`、`zpool size/alloc/free`、fragmentation；
- 每个 dataset 的 used/available/quota；
- 快照数量和最老快照时间（如启用快照）。

告警建议：pool free <20% warning、<15% page、<10% critical；dataset quota 使用率 >80% warning、>95% page。采集器不得拥有 `destroy`、`set` 或格式化权限。

## 验证边界

本方案只定义布局、Kubernetes 接入和容量门槛；现场复查执行了只读 `zpool status/list`、`zfs list` 和 `df`，没有执行删除、格式化、ZFS 属性变更、扩容、迁移或主机重启。当前仍需要的是根盘可回收空间清单，以及模型制品仓库/备份的可恢复性证明。
