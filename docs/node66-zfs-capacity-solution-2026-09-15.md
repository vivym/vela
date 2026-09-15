# `.66` ZFS 数据盘方案（验证阶段）

日期：2026-09-15  
节点：`10.1.201.66`（`marslab-gpu-01`，GPU 与管理服务共享）

## 已确认的现场状态

| 项目 | 现场值 | 结论 |
| --- | ---: | --- |
| 根文件系统 `/` | 约 877.7 GiB，总可用约 121.8 GiB，使用率约 87% | 当前 DiskPressure 的主要来源；需要独立治理 |
| ZFS pool `data` | 约 2.044 TB，总可用约 181 GiB，约 91% 已分配 | 低于安全运行余量，不能继续无上限承载新数据 |
| `data/models` → `/srv/models` | 已用约 1.70 TiB | 模型数据与 pool 共享容量，不能误删或在线搬迁 |
| `data` → `/data` | 已用约 14 GiB | 可承载少量平台数据，但受同一 pool 余量约束 |
| `data/docker` | ZFS zvol，ext4 挂载到 `/var/lib/containerd`，可用约 133 GiB | 已与根盘隔离；迁移它不会明显缓解 `/` 压力 |
| pool 状态 | `ONLINE`，`lz4`，模型 dataset `recordsize=1M`，无 quota | 健康但缺少容量边界；新增 dataset 目前不会自动获得配额 |

## 方案结论

短期不做 ZFS 重构。不要把 `/tmp`、kubelet、RKE2 或 containerd 通过 symlink、bind mount 或改挂载点搬进 `data`：这会把根盘告警转换成 pool 满盘，且模型、MinIO、Longhorn 和管理服务会共享同一个故障容量域。

中期只为新的工作负载创建单独 dataset，并设置配额、保留策略和告警。例如：

```text
data/work                 # 临时构建与验证产物，设置明确 quota
data/cache                # 可删除缓存，设置 quota 与 TTL
data/platform-backup      # 仅短期备份，设置 quota 与过期清理
```

现有 `data/models` 保持只读治理：模型删除、迁移或压缩必须经过模型清单和业务确认，不能由磁盘告警自动触发。

长期根治有两个方向，优先级相同：

1. 扩大 `.66` 系统盘或 LVM，使根盘长期保持至少 20% 可用，目标是满足 kubelet 约 25% 恢复线并保留维护余量。
2. 扩大 ZFS vdev/pool 或迁移一部分模型，使 pool 回到至少 15–20% 空闲，再评估是否承载更多缓存、Longhorn 或 MinIO 副本。

在 pool 恢复到该余量前，`.66` 只承担已有控制面/存储副本和 GPU 模型任务，不再增加新的高写入状态服务。

## 监控与告警

`deploy/observability/infrastructure-rules.yaml` 已增加：

- `vela:zfs_filesystem_avail_ratio` recording rule；
- `VelaZFSFilesystemLow`：ZFS filesystem 可用低于 15%，`warning`；
- `VelaZFSFilesystemCritical`：低于 10%，`page`。

这些规则监控 dataset 挂载点（例如 `/srv/models`），不能替代 pool 级别的 `zpool list`。生产接入时应再提供一个受限的 ZFS pool exporter 或 node-exporter textfile collector，输出 pool 的 `size/alloc/free/health`；采集器只允许执行只读的 `zpool list`、`zpool status` 和 `zfs list`，不得授予 destroy、set 或 format 权限。

## 处理流程

1. 收到 ZFS 告警后暂停新增模型/缓存写入，读取 `zpool status`、`zpool list -p`、`zfs list -o name,used,avail,refer,mountpoint,quota`。
2. 判断是 pool 余量还是单个 dataset 配额触发；保留模型文件、Longhorn 数据、验证证据和运行中任务。
3. 只清理有清单、有 TTL、明确获批的临时目录；禁止直接删除 `/srv/models`、Longhorn 路径、`/var/lib/kubelet` 或 `/var/lib/containerd` 内容。
4. 恢复后验证 pool 为 `ONLINE`、挂载存在、MinIO/PostgreSQL/NATS/etcd 与 Longhorn 副本健康，再解除维护动作。

## 当前验证边界

本方案没有执行删除、格式化、`zpool set/destroy`、扩容、迁移或重启。告警规则已通过 `promtool check rules` 及 145 个基础设施场景测试；现场仍需补 pool-level exporter 的真实采集验证，才能把 pool 健康纳入生产告警闭环。

2026-09-15 09:59 CST 已将 node-exporter 与 control-storage 的 node-local-metrics
提升为 `system-node-critical`，`.66` 的 ZFS 指标已真实进入 Prometheus；
`/srv/models` 的 9.59% 可用率触发了 warning/page 两级告警。该优先级变更只
保护只读遥测进程，未移除 `DiskPressure` taint，也没有改变业务副本的调度规则。
