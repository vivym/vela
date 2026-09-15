# 有状态恢复现场记录 — 2026-09-14

本次验证只覆盖以下明确故障范围；没有主机重启或 GPU 驱动操作，没有操作
GPU worker 的数据盘。数据规模很小，耗时是本次实测，不是生产规模 RTO 承诺。

## PostgreSQL 时间点恢复

[原始 receipt](evidence/cnpg-live-pitr-2026-09-14.json) 的结果为
`LIVE_PITR_PASS`。源为 `vela-system/vela-postgres`，数据库总大小
61,557,147 bytes，备份 `20260914T130306` 于 13:03:20Z 完成。
目标时间为 `2026-09-14T13:03:26.066672Z`。

临时唯一 schema 中，base 标记在备份前写入；before_target 在备份完成后写入，
after_target 在目标时间后写入。恢复结果为 `base=1, before_target=1,
after_target=0, in_recovery=false`，因此包含实际 WAL replay 证据。
恢复创建到标记验证耗时 145.8 秒；从演练启动到验证完成 172.11 秒。

恢复实例的 NetworkPolicy 起初阻断了 CNPG operator 的 TCP8000 状态访问，
本次现场增加来自 `cnpg-system`、`app.kubernetes.io/name=cloudnative-pg`
的许可后通过。修正已回写 `hack/verify-cnpg-live-pitr.py`；不能将本次运行
描述为未修正脚本一次性通过。业务数据库 ingress 仍关闭。

临时 Cluster、Pod/PVC、NetworkPolicy、Backup CR 和源 marker schema 已清理，
源集群保持 3/3 Ready。S3 中备份/WAL 由原 retention policy 管理。
`pg_switch_wal()` 必须在 marker INSERT 已提交后的独立 psql 调用中执行。
Backup 资源使用完整类型 `backups.postgresql.cnpg.io`，避免与 Longhorn 混淆。

## NATS 持久消息和 consumer 恢复

[原始 receipt](evidence/nats-live-recovery-2026-09-14.json) 的结果为
`NATS_ROLLING_PERSISTENCE_PASS`。使用独立的 `VELA_DRILL_8c5b3c5ee9fc0bbe`
stream、三副本 file storage 和三副本 durable consumer `DRILL`。
测试限制为 1MiB/100 messages/1 小时；发布一个消息，首次消费故意不 Ack。

通过 Eviction API、UID 前置条件和 PDB，依次替换 nats-0、nats-1、nats-2。
每次确认新 Pod Ready、UID 改变、PVC 名不变、stream/consumer 副本全部 current、
消息 SHA256 不变后才替换下一成员。实测分别为 13.14、6.88、13.18 秒。
最终同一 sequence=1 消息的 delivery_count=2；测试 stream 清理成功，
StatefulSet 保持 3/3 Ready。

这证明逐成员 Pod 替换时的持久消息与 consumer 恢复，不证明全 NATS 集群同时
丢失、PostgreSQL Outbox replay 或跨站点恢复。实现见
`hack/nats-live-recovery/main.go` 与 `hack/verify-nats-live-recovery.py`。

## 尚未解决的容量前提

业务 `VELA_EVENTS` 要求 64GiB、三副本；当前三个 50Gi PVC 的 server 自动
`max_storage` 为 39,378,487,296 bytes，约 36.7GiB，无法兑现该契约。
自动 `max_memory` 又按主机内存计算，超过 Pod 的 8Gi 限额。
不得以独立小 stream 演练代替业务 stream 的容量/bootstrap 验收。
两个 CPU 节点的 Longhorn 预约已接近保留后预算，不能盲扩或降低保护余量。
按用户要求，当前硬盘初始化/扩容工作暂缓，以上继续归入 R3/R5。
