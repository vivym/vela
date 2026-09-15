# 遥测留存容量核查 — 2026-09-15

05:05–05:09 CST 只读检查了当前 Prometheus、Loki、Tempo 的有效配置、PVC
占用、原生数据块元数据和近期写入指标。Prometheus 的 40Gi 上限存在先于
15 天触发清理的风险；Loki/Tempo 当前占用低，但尚无完整保留周期的业务证据。
这补充既有 R5，不修改保留目标、采集频率、PVC、主机磁盘或业务数据。

## 现场结果

| 服务 | 配置 | 当前 PVC 使用量 | 实测数据边界 |
| --- | --- | --- | --- |
| Prometheus | 50Gi PVC；15d；40GiB 大小上限 | 4.49GiB | 最旧指标约 27.94 小时；约 79.1 万 head series，28,537 samples/s |
| Loki | 100Gi PVC；168h；retention_enabled=true | 443.46MiB | 七天范围内向前查询的最旧结果约 14.82 小时；仅保留时间戳，不导出日志正文 |
| Tempo | 30Gi PVC；48h | 2.57MiB | 原生 block 元数据从 09-14 05:21 UTC 开始；主要是既有有限验证流量 |

Prometheus 的 15d/40GiB 同时由运行中的 `/api/v1/status/flags` 确认；大小或时间
约束先到者会触发清理。当前 size/time retention 计数均为 0，尚未发生相应删除。
PVC 使用量来自 kubelet 文件系统统计，不等同于 Longhorn 两副本的物理总占用。

## Prometheus 的容量推算

通过当前容器 PID 的 `/proc/<pid>/root/prometheus` 只读访问实际挂载：仅对目录
执行 `du`、读取 `meta.json`，没有扫描时间序列载荷或修改数据块。下面使用完整
18h/6h/2h 数据块，排除安装期不足一小时的小块。

| 数据块时长 / compaction level | 目录实际分配 | 按同样密度推算 GiB/天 | 15 天数据块推算 | 40Gi 对应时长 |
| --- | --- | --- | --- | --- |
| 18h / level 3 | 2,412,548,096 bytes | 3.00 | 44.94GiB | 13.35 天 |
| 6h / level 2 | 967,503,872 bytes | 3.60 | 54.06GiB | 11.10 天 |
| 2h / level 1 | 383,418,368 bytes | 4.29 | 64.28GiB | 9.33 天 |

计算为 `数据块目录 bytes / (maxTime - minTime) × 86400`，时长单位先转换为秒。
不同压缩层级、采集集合变化和 series churn 都会改变密度，因此 **9.33–13.35 天
是这些样本的情景推算，不是已兑现的保留期或保证的上下界**。较早 18 小时内
也包含部署变化，不能将其当作未来稳定负载。

当前另有 WAL 768,651,264 bytes、head 166,481,920 bytes；推算表未加 WAL/head、
compaction 临时空间及未来业务增长。仅调高 40Gi 上限不能证明 50Gi PVC 足够。
当前主要 series 来源包括 kubelet、node-exporter、Longhorn 与 API server；
不是 NATS 新六个 targets 导致的整体基数。此次未删除指标或降低采集频率。

## Loki 与 Tempo

Loki 最近六小时原始接收约 12,032 bytes/s，已 flush 的压缩 chunk 约
640 bytes/s；按后者简单外推七天约 0.36GiB，仅作当前验证负载的参考。
实际 chunk 目录 406,736,896 bytes、WAL 41,091,072 bytes。两种速率不能互换，
也不能将原始接收 bytes 直接视为磁盘需求。24 小时窗口包含较早 rollout、
重试和验证流量，`increase` 还包含 Prometheus 边界外推，不能称作精确磁盘增量。

最近六小时，Loki 已暴露的丢弃计数与 flush 失败增量为 0；compactor 最近一次
成功约在 13 分钟前，并有持续的成功计数。但当前尚无七天历史，不能据此证明
完整七天的保留和删除行为。24 小时的 chunk age、索引/WAL、未来客户日志量
也不包含在 0.36GiB 的简单推算中。

Tempo blocks 目录约 1.86MiB，generator 约 0.67MiB，最近六小时已暴露的丢弃
与 flush 失败增量为 0。当前流量主要是追踪验证，不能代表真实模型/API 的
48 小时需求。现有容量尚无紧急压力，真实业务速率和完整周期验收仍开放。

## 证据与后续条件

- [配置前检查与原始指标](evidence/telemetry-retention-preflight-2026-09-15.json)
- [实际配置、分组采集频率与原生块元数据](evidence/telemetry-retention-metadata-2026-09-15.json)
- [六小时速率、失败计数与日志最旧时间](evidence/telemetry-retention-rates-2026-09-15.json)
- [容量计算输入和结果](evidence/telemetry-retention-analysis-2026-09-15.json)
- [05:09 CST 集群快照](evidence/cluster-readiness-after-nats-observability-2026-09-15.json)
- [14 份文档、脚本和回执的 SHA256 归档记录](evidence/telemetry-retention-archive-2026-09-15.json)

原始未合并的 target 清单与执行结果保留在
`.70:/opt/vela-cluster/telemetry-retention-20260915/`。首次指标名称查询漏了 API
路径前缀，返回 404；修正为 `/api/v1/label/__name__/values` 后采集成功。
所有操作只读，没有为核查启动新 Pod、重启服务或操作原 Compose 监控。

R5 的关闭仍需统一核算 NATS、Control scratch、MinIO 与监控的管理盘容量；
在验证负载下的空余不能全部重复分配给多个服务。应先确定能兑现的空间方案，
再在真实采集/业务负载下验证留存。当前核查不授权初始化暂缓的 worker 数据盘，
也不把采购独立故障域作为新前置条件。
