# NATS 容器内存配额修复

三个 NATS 2.10.22 成员原先按宿主内存自动分配 JetStream 上限，分别为
50,332,197,888、405,620,880,384 和 50,321,866,752 bytes，均超过 Pod 的 8Gi
限额。现场确认没有 stream、消息或已预留的 JetStream 内存。

现已明确设置 `max_memory_store: 2147483648`，即每成员 2GiB，为 8Gi Pod 中
的其他缓冲区和进程开销保留空间。这是 JetStream 内存存储上限，不是整个 NATS
进程的总内存保证。参数记录于
`deploy/control-storage/nats-resource-limits.json`。

原 `vela-nats-auth` 是不可变 Secret，因此新建
`vela-nats-auth-memory-d74221ecd013`，仅修改其中的 `nats.conf`。其他凭据字节
和原 Secret 均保持不变，客户端继续引用原 Secret。新配置先由运行中的实际
NATS 二进制执行 `--test` 验证；未将 Secret 内容写入本仓库。

NATS v2.10.22 的 `reload.go` 明确拒绝从动态内存限额热切换到显式限额。
更新 StatefulSet 时先设置 partition=3，再按 nats-2、nats-1、nats-0 逐级降到 0。
每步检查三个成员 Ready、对同一 leader 达成一致，以及 leader 报告的两个副本
current/无 lag。该版本的 follower `/jsz` 不含副本 current 列表，不能把字段省略
当成仲裁故障，也不能据此忽略 leader 的副本状态。

三个成员分别在 13.18、6.94、6.97 秒内替换并恢复，最终均报告 2GiB 上限。
PVC 引用、容量、stream contract 均未改变；没有重启主机、RKE2 或 GPU 驱动。
最终运行引用保存在 `deploy/environments/marslab/nats-memory.patch.json`，后续
发布须先提供同名版本化 Secret，再合并该站点 patch；首次限额变更应使用
`hack/configure-nats-memory-limit.py` 的分阶段流程。

[完整证据](evidence/nats-memory-limit-2026-09-14.json)包含新旧 Pod UID、配置摘要、
内存/磁盘配额与仲裁结果。

本次修复没有解决 64GiB 业务 stream 契约与 50Gi PVC、约 36.7GiB 自动文件配额
之间的容量差距。`VELA_EVENTS` 仍未 bootstrap，R3/R5 的容量验收仍然开放；
不能通过降低业务契约或创建过量承诺的 stream 宣称完成。
