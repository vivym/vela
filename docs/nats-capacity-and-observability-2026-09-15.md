# NATS 容量与可观测性收口 — 2026-09-15

本轮核验 R3/R5 的真实容量，修复 NATS 只观察一个成员的监控盲区。
业务 `VELA_EVENTS` 仍未 bootstrap；没有降低 64GiB 契约、调整存储预留、
删除旧卷或操作用户暂缓的数据盘。容量问题不能通过把 NATS Pod 标为 Ready 解决。

## 容量结论

三个 NATS 2.10.22 成员都使用 50Gi PVC、2GiB JetStream 内存上限，自动文件
配额均为 39,378,487,296 bytes，约 36.67GiB。三个成员的业务 stream 数、
文件占用和已预留文件容量均为 0，元数据 leader 为 nats-2，两个 follower
current，未观察到 lag。服务健康与业务流已配置是两个不同条件。

三个 NATS PVC 各有两份 Longhorn 副本，六份副本都在 `.70/.71`。
每成员扩到 80Gi 会增加每台 CPU 节点 90Gi 的逻辑承诺，总共 180Gi。
这尚未包含文件系统、索引、快照及其他服务增长余量，不能直接把 80Gi PVC
容量当成 80Gi 可用消息空间。

以下为 09-15 本轮只读盘点，GiB=2^30 bytes：

| 项目 | .70 / llmpool01 | .71 / llmpool02 |
| --- | ---: | ---: |
| 根文件系统总量 | 878.11 | 915.32 |
| 普通进程可用空间（f_bavail） | 697.06 | 646.04 |
| Longhorn 实际占用（du） | 49.46 | 48.79 |
| 非 Longhorn 已用空间 | 86.92 | 173.93 |
| Longhorn 已分配副本逻辑容量 | 690 | 708 |
| Longhorn 调度预留 | 175.62 | 183.06 |
| 当前还能承诺的副本容量 | 12.49 | 24.26 |
| 仅扩 NATS 到 80Gi 后，全部卷写满时理论剩余 | 11.19 | **-56.60** |

最后一行是 `文件系统总量 - 非 Longhorn 当前占用 - 全部副本逻辑容量 - 90Gi`。
它还没扣未来快照/额外增长，已经说明 `.71` 无法兑现承诺。Longhorn 的
`storageAvailable` 使用包含 ext4 保留块的 free 值；不能与普通进程 `f_bavail`
混为一谈。降低 Longhorn reservation 或提高 over-provisioning 不创造物理容量。

已停用的四个旧 MinIO PVC 各 20Gi、两份副本，仍占 `.70/.71` 各 80Gi
逻辑容量，总计 160Gi。即使未来经过归档、恢复验证后退役它们，按当前非
Longhorn 占用计算，扩容后的 `.71` 理论余量也只有 23.40Gi，不满足现有 10%
空闲下限，更没有快照和系统增长余量。**退役旧卷本身不足以完成容量收口**。

`.66` 的 Longhorn 根盘总量 877.67Gi、free 约 175.59Gi、已有 98Gi 副本
承诺，另保留 263.30Gi 调度空间。它不是可以无条件接收全部新副本的空盘。
GPU worker 数据盘继续只用于模型、缓存和临时数据，不作为本次补容量来源。

证据：[Longhorn/PVC](evidence/nats-capacity-preflight-2026-09-15.json)、
[逐副本与 NATS 状态](evidence/nats-capacity-replicas-2026-09-15.json)、
[文件系统与实际使用](evidence/nats-capacity-host-filesystems-2026-09-15.json)。
远程完整原始记录保存在 `.70` root 私有目录
`/opt/vela-cluster/nats-capacity-20260915/`。

R3/R5 下一次容量实施前，必须形成可兑现的三节点分配计划，同时纳入 NATS、
MinIO、PostgreSQL、Control scratch 与遥测留存。现有旧数据只做只读盘点，
没有默认授权删除它们；硬盘初始化遵守用户的暂缓决定。

## 修复的监控盲区

旧 exporter 仅访问共享 `nats.vela-system.svc:8222`，实际一次只观察一个
随机成员。`up=1` 只能证明 exporter HTTP 成功，不能证明三个成员或业务流健康。
本轮改成两台 CPU 管理节点各一个 exporter Pod，每 Pod 三个独立进程，分别
绑定 `nats-0/1/2.nats.vela-system.svc`，共六个 Prometheus targets。

各进程仅采集一个 NATS 成员。ServiceMonitor 增加固定 `nats_member` 标签，
告警按成员去重。exporter readiness/liveness 使用 TCP listener，避免一个
NATS 上游失败使同 Pod 中其他两个成员的采集端点一起退出 Service。
镜像固定到已运行的 v0.15.0 digest；无 Kubernetes token、Secret、PVC 或 GPU。

新增 `vela-nats-rules` 包含一条记录规则与八类告警：成员覆盖不足、采集副本
不足、文件配额低于 64GiB、业务 stream 缺失、stream 副本覆盖不足、Scheduler
consumer 缺失、metadata leader 不一致、持续调度 backlog。
新增 Grafana `vela-nats` 八面板，复用现有认证入口、Prometheus 和 Grafana。

27 个 promtool 场景验证缺失、恢复、重复采集、部分故障、配额边界，以及
空 stream/consumer 与缺失的区别。测试使用集群实际 Prometheus v3.14.0
distroless digest，在无网络/无 token/无 PVC 的 CPU Job 中执行。
最初尝试不能向只读 distroless 容器创建临时目录；独立 Job 首次也因缺少
可写 `/tmp` 而失败。最终增加有上限的 emptyDir 临时目录后全部通过，测试
Job、ConfigMap、NetworkPolicy 均已清理，失败记录单独保留。

这些告警明确暴露当前未完成的业务事件配置，不是把开放项伪装成通过。
stream/consumer 指标存在也不能单独证明完整配置与 release 契约一致；实际
bootstrap 仍须使用 `internal/eventstream` 的精确验证并完成 PostgreSQL Outbox replay。

## 现场验收与归档

09-15 04:58 CST，六个当前采集目标全部 up；三条成员文件配额不足告警、
一条业务 stream 缺失告警和一条 Scheduler consumer 缺失告警已进入 firing。
成员覆盖、采集冗余、metadata leader 与 backlog 告警 inactive，九条规则
health 均为 ok。上述五条业务/容量告警是当前 R3/R5 的真实缺口。

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| 原生 Prometheus 规则测试 | 27 场景通过，测试资源已清理 | [测试回执](evidence/nats-observability-rule-tests-2026-09-15.json) |
| 部署准备 | 5 个变更对象 server dry-run 通过 | [准备回执](evidence/nats-observability-prepare-2026-09-15.json) |
| 旧容器迁移 | 核对原始清单后仅移除遗留 exporter 容器 | [修复回执](evidence/nats-observability-legacy-repair-2026-09-15.json) |
| 最终现场检查 | 13 项通过；两个 CPU Pod、六个 Ready 容器、六个当前采集目标、八面板看板 | [最终回执](evidence/nats-observability-postcheck-2026-09-15.json) |
| 告警观察窗口 | 5 条预期告警 firing，采集和冗余告警 inactive | [告警快照](evidence/nats-observability-alerts-2026-09-15.json) |

看板：<https://10.1.201.70:30443/grafana/d/vela-nats/>，复用既有 Grafana 登录。
最终检查确认 NATS Pod UID、NATS PVC 配置与 Longhorn 分配策略未变。
`go test ./internal/deploymentcontract ./internal/eventstream -count=1` 和完整
observability Kustomize 渲染通过。

首次 apply 保留了原先不受同一 apply 清单管理的 `exporter` 容器，造成端口
7777 冲突。核对部署前捕获的容器配置后仅删除该遗留容器，随后 rollout 成功。
首次检查还包含已退出 Pod 的旧 `up` series；最终检查以当前 active targets
及其 instance 集合判定。Grafana 凭据引用则从当前 Deployment 解析，未把
凭据值写入回执。历史超时、只读临时目录失败和两次未通过的检查均保留在
`.70:/opt/vela-cluster/nats-capacity-20260915/observability/`，没有覆盖成成功记录。

24 份源码、清单、文档和回执的审计包已逐文件核对 SHA256，保存到
`.70:/opt/vela-cluster/nats-capacity-20260915/audit/source.tar.gz`。
见 [归档回执](evidence/nats-observability-archive-2026-09-15.json)。归档包含实际
执行的无凭据测试/部署/校验脚本，不包含 SSH 辅助凭据或 Secret 载荷。
