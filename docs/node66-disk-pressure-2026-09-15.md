# .66 磁盘压力事件 — 2026-09-15

05:16 CST 起，`.66` (`marslab-gpu-01`) 的根文件系统压力触发 kubelet 驱逐。
05:56 CST 仍未恢复；Fleet 安装器在健康预检中停止，未改动运行中的 Control。
本事件归入既有 R3/R5，不新增验收编号。

06:39 CST [最新复核](evidence/cluster-readiness-after-fleet-preparation-2026-09-15.json)
仍未恢复，PostgreSQL/NATS/APISIX etcd 各 2/3、MinIO 5/6；Longhorn 当前
18 attached/healthy、2 attached/degraded、7 detached/unknown、2 detaching。
根卷可用降至 125.45GiB，按之前核定的 94.00GiB 清理量仅比 219.42GiB 恢复线
高约 0.03GiB，已没有可靠操作余量。原清理清单仍待授权，实际执行前必须重新
测量可用量和目录状态，不能沿用 06:07 的 25.8% 估算或扩大删除范围。

## 影响与证据

| 项目 | 05:56 CST 实测 |
| --- | --- |
| 节点 | 54 注册、53 Ready、407 GPU；`.19` 仍离线，`.66` Ready 但 DiskPressure=True |
| PostgreSQL | 2/3 Ready，主库 `vela-postgres-1` |
| NATS / APISIX etcd | 均为 2/3 Ready |
| MinIO | 5/6 Ready；读写健康接口均 200，但健康成员分布为 .70 两个、.71 三个 |
| Longhorn | 20 attached、7 detached、2 detaching；含保留的四个旧 MinIO 卷，不能全部计作新故障 |
| 监控 | Alloy/node-exporter 各 52/54，额外缺口来自 .66 |
| 发布与 API | Argo 工作负载仍健康，Control 2/2，两台网关 Grafana HTTPS 均 200 |

原状态采集器遗漏了 Ready 节点的 pressure condition 和 CNPG 自定义资源副本数，
MinIO 也只有“已调度成员”分布。现补充 `nodes.pressured`、`postgresql`、
`minio.members` 与 `ready_members_per_host`；保留原字段供已有消费者使用。
新的现场快照将这些退化状态明确列出，不能只看 Ready 节点总数或 HTTP 200。

## 根因与恢复门槛

`/` 与 kubelet imagefs 是同一 ext4 根卷，容量 `942396350464 B`（877.67 GiB）。
实际 configz：

```yaml
evictionHard:
  imagefs.available: 15%
  nodefs.available: 10%
  memory.available: 2Gi
evictionMinimumReclaim:
  imagefs.available: 10%
  nodefs.available: 10%
evictionPressureTransitionPeriod: 5m0s
imageGCHighThresholdPercent: 85
imageGCLowThresholdPercent: 80
```

因此在触发 imagefs 驱逐后，恢复目标约为 25% 可用，即 219.42 GiB，随后还需
经过五分钟压力转换期。回到略高于 15% 仍不够，这解释了此前持续 Pending。
RKE2 自身数据约 4.4 GiB，根卷 `/home` 约 548 GiB、`/tmp` 约 108 GiB；
大量其他计算项目的编译与仿真文件共享根卷。没有历史连续目录快照，不能把
全部新增占用归因于某一个进程。Vela Fleet 新镜像仅约 25MB，事件早于其复制。

## 已做的处理

确认文件格式、超过 48 小时未修改、无进程 cwd/exe/fd/maps/environ 引用、
无容器挂载后，清理三份独立 Vela Go cache：`/tmp/vela-current-cache`、
`/tmp/vela-current-cache-v2`、`/home/marslab/vela-cache`。每份保留文件元数据
清单与 hash；没有删除源码、测试证据或模型。共回收约 2.91 GiB，05:45
根卷可用约 137.23 GiB（15.64%），距压力恢复仍差约 82.19 GiB。

最初的 cache 校验因复制缓存不带 README 而在删除前停止；随后改为验证全部
Go action cache 元数据格式并重新预检，通过后才清理。boot ID 一直为
`e7cb0945-b805-4dad-9f2e-b3d36c71bb5f`；未重启主机、驱动或 RKE2。

新增两条告警且保留原 36 条规则：根卷可用低于 20% 持续 5 分钟预警、
实际 DiskPressure 持续 1 分钟 page。145 个 promtool 场景通过，现场两个规则
health=ok，`.66` DiskPressure 告警 firing。根卷预警当时 inactive 是因为
`.66` node-exporter 被驱逐、序列缺失，不能解释为它的容量健康；实际节点
condition 告警与已有采集缺失告警覆盖这一阶段。

## 后续动作

[70 个临时目录回收清单](node66-temporary-artifact-reclamation-plan-2026-09-15.md)
共 94.00 GiB：首批 67 个超过 24 小时未修改，补充 3 个超过 18 小时未修改，
均未发现运行引用。两批在 `.70` 的归档与内容比对已通过，06:07 的回收前
只读预检也通过；原目录未删除。补充的原因是归档期间其他任务继续写入，
根卷可用又降至约 132.28 GiB，原 84 GiB 已不足以恢复到 25%。这些目录多数属于其他计算项目，需要明确同意
归档后回收，不从 Vela 缓存清理授权推断可以删除其他项目源码与结果。

回收后须等待 kubelet 自然解除压力，确认 Longhorn 的 instance manager、CSI、
manager 恢复及卷正常挂载，再验证各有状态服务仲裁。MinIO 必须恢复三台主机
各两个健康成员后再做其他维护；不能把当前 CPU 两台上的分布视为原三主机
部署已恢复。之后继续 [Fleet 准备记录](fleet-controller-deployment-preparation-2026-09-15.md)
中的发布步骤。仍按原固定 R1–R6 清单收口。

## 现场证据

- [压力配置与受影响 Pod](evidence/node66-disk-pressure-2026-09-15.json)
- [本任务 Go cache 回收](evidence/node66-cache-reclamation-2026-09-15.json)
- [修正后的完整状态快照](evidence/cluster-readiness-disk-pressure-2026-09-15.json)
- [告警部署](evidence/disk-pressure-alert-deployment-2026-09-15.json)及[实际 firing](evidence/disk-pressure-alert-live-2026-09-15.json)
- [145 个规则测试](evidence/disk-pressure-rule-tests-2026-09-15.json)
- [两批完整归档和无删除预检](evidence/node66-archive-reclamation-preflight-2026-09-15.json)
