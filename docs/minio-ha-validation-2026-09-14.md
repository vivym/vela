# MinIO 六成员验证与监控修正

原 `object-store/minio` 四成员集群已在 2026-09-14 完成冻结；稳定 `minio` Service 现在选择新的六成员集群，旧 StatefulSet 为 0，四个源 PVC 保留。新的
`minio-ha` 使用六成员、每主机两个、STANDARD/RRS 均为 EC:3；read quorum
为 3，write quorum 为 4。暂存 overlay 为 6×8Gi，最终基线为 6×16Gi，
每卷有两份 Longhorn 副本。

## 已完成的成员失效验证

在 `.70`、`.71`、`.66` 各进行一组测试：只给该主机的两个新 MinIO Pod
施加 NetworkPolicy，再正常重建这两个 Pod 以断开旧连接。策略不作用于
原 MinIO、主机网络服务或 GPU 驱动，未重启任何主机。

每组都确认剩余四个成员可用、存储层报告四盘在线及 3/4 读写仲裁；然后
写入三个合成对象，逐字节读回三个新对象，并读回先前对象三次。三组均
输出 `S3 READ_WRITE_PASS`，验证后再次确认仍为四盘，防止自动回滚提前
恢复六成员而产生假通过。

最后一次删除测试桶失败，脚本因此以 1 退出，未写出原计划的完整 receipt。
后续只读检查发现桶内已无对象，再次删除成功；最终确认六成员 Ready、
六卷均为两副本且 healthy，没有测试桶、临时策略或回滚定时器残留。
[恢复后的证据记录](evidence/minio-ha-quorum-2026-09-14.json)保留了原始
非零退出和清理恢复事实。各组耗时与 payload hash 当时未落盘，未补造。
脚本现已增加逐组 checkpoint 和有界清理重试；这版清理改进未再整轮重跑。

这证明上述成员失效条件下的合成 S3 读写，不证明物理主机掉电、Longhorn
卷故障、真实业务负载下的 RTO/RPO，亦不等于迁移完成。

## 两个导致早期验收失败的观测问题

1. NetworkPolicy 生效后，新连接被阻断，但既有 MinIO peer 连接继续通行，
   六成员仍 Ready。增加对选定暂存 Pod 的正常重建，并等待 UID 更换，才
   确认原进程和旧连接确实退出；仅观察 Terminating 导致的 Ready 变化不够。
2. `mc admin info` 在两 peer 被丢包时曾报告仅一盘在线，而四个剩余成员
   的 cluster health 均返回 200、write quorum 4。v2 全量指标也超过 5 秒
   抓取期限。固定版本源码中，`getLocalServerProperty` 串行调用每 peer
   最多 5 秒的 `isServerResolvable`，外围 `ServerInfo` RPC 仅限 10 秒；
   两个超时即可把其他健康成员的 RPC 也判为失败。延长客户端重试没有解决。

改用 `/minio/metrics/v3/cluster/erasure-set` 后，同一两成员失效场景报告
四盘在线且三组 S3 读写都通过。该窄接口读取 `objectAPI.Health` 的
erasure-set 状态，避开上述完整 inventory 路径。原 MinIO 已新增专用
`minio-quorum` ServiceMonitor；它只选客户端 Service，避免 headless
Service 重复抓取。仲裁告警按 namespace/service/pool_id/set_id 分组，
不将不同 MinIO 集群合并计算。全量指标失败仍保留自己的告警。

133 个 promtool 场景通过，包含新 endpoint 故障/消失、v2 失败而 v3 正常、
不同集群相同 pool/set 编号的隔离检查。部署契约全套测试通过，完整监控
render 另做服务器 dry-run。

源码依据均固定到正在运行的 RELEASE.2025-04-22T22-12-26Z：

- [admin-server-info.go](https://github.com/minio/minio/blob/RELEASE.2025-04-22T22-12-26Z/cmd/admin-server-info.go#L38)
- [notification.go](https://github.com/minio/minio/blob/RELEASE.2025-04-22T22-12-26Z/cmd/notification.go#L1132)
- [prepare-storage.go](https://github.com/minio/minio/blob/RELEASE.2025-04-22T22-12-26Z/cmd/prepare-storage.go#L124)
- [metrics-v3-cluster-erasure-set.go](https://github.com/minio/minio/blob/RELEASE.2025-04-22T22-12-26Z/cmd/metrics-v3-cluster-erasure-set.go#L83)

## 迁移仍需完成

08:40 UTC 的[源库存](evidence/minio-source-inventory-2026-09-14.json)记录了
五个桶。两个 Artifact 桶已启用版本控制但当前为空；其余三个桶的版本
控制未启用，备份桶仍在增加 WAL 对象。若干 retention/replication/ILM
查询返回错误，尚未分类，不能把 rc=1 直接解释成“没有配置”。IAM 用户
列表为空，但 service account、STS 及外部身份尚未完整盘点。

切换前还要完成元数据、IAM、版本/删除标记、持续备份增量及逐对象校验，
并解决从 8Gi 到 16Gi 的容量承诺和 PVC/StatefulSet 不可变字段处理。
不要直接对既有四成员 erasure set 原地扩成六成员，也不要为释放配额
提前删除原数据。后续任务归入统一验收清单 R3/R5。
