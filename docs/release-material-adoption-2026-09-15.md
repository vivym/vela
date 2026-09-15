# Control/NATS 材料与数据库基线采用 — 2026-09-15

04:12 CST 完成新 Control 镜像与不可变发布材料切换，Control 2/2、NATS 3/3
Ready，25 项切换检查通过。本次收口固定清单 R4 的当前 Control/NATS 输入与
schema 来源问题；完整 canonical release、业务容量和真实模型验收仍未完成。

## 当前结果

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| 材料固化 | 2 个 ConfigMap、14 个 Secret；双读验证 immutable、精确键集、UID 和 canonical digest | [创建](evidence/external-material-apply-2026-09-15.json) |
| 受控切换 | Control 新镜像与键引用、ServiceAccount pull Secret、NATS server auth/TLS 引用；25 项通过 | [切换](evidence/external-material-rollout-2026-09-15.json) |
| 数据库最终状态 | 3,004 个 schema 定义一致；Goose 0–96 基线与两个 Fleet EXECUTE 权限已生效 | [最终数据库](evidence/schema-baseline-current-2026-09-15.json) |
| 真实角色校验 | 完整权限接受；额外 jobs SELECT 和缺失必需函数 EXECUTE 拒绝；恢复后接受 | [原生 PostgreSQL](evidence/schema96-native-role-check-2026-09-15.json) |
| 镜像副本 | .71/.66 各 1 manifest、6 blobs、44,722,820 bytes 逐项 SHA256 校验 | [复制](evidence/control-schema96-replicas-2026-09-15.json) |
| 源码归档 | 235 个实际编译/嵌入/模块文件逐项校验，.70 root 私有 archive 为 0600 | [归档](evidence/control-schema96-archive-2026-09-15.json) |
| 现场与清单最终核对 | 52 项通过：ConfigMap 字节、键引用、镜像、三端新 digest 指标及证书有效期 | [最终检查](evidence/external-material-postcheck-2026-09-15.json) |

新镜像：

```text
10.1.201.70:5005/vela-control@sha256:9cd421866b4526d60b693cd161d3e1a2d92314fa5966e4504e9f0a0e24884d64
```

构建来源为明确标记的 working-tree snapshot，源码摘要
`62911d49776c2b9b0187ca7d7341f5ed06119f5d853a833378273a71853bf275`；二进制摘要
`e2a70cab354c03cd7b3076f50ba8c5431fafd6037cc06f76d36deefc8de7b85b`。
以先前固定 digest 镜像为基础，仅替换 Control 二进制，保留 ffprobe、validator、
CA、UID/GID 和 entrypoint。源码清单见
[source](evidence/control-schema96-source-2026-09-15.json)，构建见
[image](evidence/control-schema96-image-2026-09-15.json)。这不是 clean HEAD 或
完整全组件 release 的证明。材料切换时 tracing 尚未启用；04:27 CST 的独立
配置切换随后启用了 Control OTLP，并通过 46 项实际 Pod/双网关追踪检查，见
[追踪验收](application-tracing-validation-2026-09-15.md)。52 项材料 postcheck
记录的是该后续配置切换之前的模板，保留为历史证据。

## 数据库基线如何建立

原 `app` 已有 214 个 public 表，包含 migration 96 的表，但 Goose ledger
只有 `version_id`、`is_applied` 和 `(0,true)`。在 CPU 节点上使用与生产相同
PostgreSQL 镜像，创建无网络、无 PVC 的一次性参考库，实际运行全部 96 个迁移。
初次比对有 2,998 个相同定义、六个差异：四个 Goose 结构/ACL 差异与两个 Fleet
函数授权缺失。九个非空迁移种子表在明确处理时间戳和数据库独立 identity 后匹配。

经独立事务演练后，给现有 Goose 表增加标准 identity `id` 和 `tstamp`，将主键
迁到 `id` 并保留表 OID，登记 1–96 基线及原有版本 0，恢复 `vela_internal` 的
预期 ledger 权限。登记时间是本次采用时间，**不是历史迁移执行时间**。
没有向业务库重放迁移，也没有修改业务表数据。

第一次严格文本 postcheck 在事务提交后发现物理列顺序与 ACL 输出顺序不同，
原失败记录保留。随后逐项比较列属性、schema 对象集合及授权集合，证明这两处
只是输出顺序差异。最终再验 213 个业务表行数、九个种子表 hash 均保持不变，
数据库独立 identity 没有替换。见 [初次比对](evidence/schema-reference-comparison-2026-09-15.json)、
[基线采用](evidence/schema-baseline-adoption-2026-09-15.json) 与上述最终证据。

## 两次失败与恢复

1. 第一次恢复 migration-96 两项函数授权后，旧 Control 二进制的 Fleet 精确
   allowlist 拒绝启动。源代码也遗漏这两个函数，已仅补齐
   `vela_store_runtime_startup_authorization(uuid,bytea)` 和
   `vela_get_runtime_startup_authorization(uuid)`；未放宽一般权限边界。
   临时撤销这两项授权，旧 Control 恢复 2/2 Ready。
   见 [第一次切换](evidence/external-material-attempt1-2026-09-15.json) 和
   [恢复](evidence/external-material-recovery-2026-09-15.json)。
2. 第二次新 Control 已 2/2 Ready，但检查脚本假定 NATS 镜像有 `wget`，
   后续检查失败并自动撤销授权、恢复旧 Control/ServiceAccount 模板。
   见 [第二次切换](evidence/external-material-attempt2-2026-09-15.json)。
   最终改为管理节点通过仅监听 loopback 的临时 port-forward 访问 NATS
   `/healthz?js-enabled-only=true`，并在任何授权/模板变更前验证三成员探测。
   每个临时转发进程均在 finally 中终止。

第三次切换完整通过。各尝试使用独立目录和 PID/start-time 记录；旧材料、原始
失败和回退证据均保留。今后回退旧 Control 镜像时必须同步处理这两项权限，
不能只做 `rollout undo` 而留下不兼容授权。

首次监控收口检查遇到 Prometheus 查询仍保留旧 digest 的历史序列，记录为
[初次 postcheck](evidence/external-material-postcheck-initial-2026-09-15.json)。
三个实际 active targets 已全为新 digest 且 up；最终检查同时核对 active targets
和对应新 digest 的三个 HTTP 200 / probe_success=1，允许旧历史序列自然过期。
没有删除监控历史或改动探针认证规则。

## 可重建输入与后续边界

Control site overlay 已采用本次镜像与不可变引用；NATS 的 site 材料记录位于
`deploy/environments/marslab/nats-memory.patch.json`，仅针对现有
`StatefulSet/vela-system/nats` 的配置/TLS projection 与更新策略。NATS 镜像、
数据卷和副本数保持本次预检值；不能用这份 patch 代替完整存储发布输入。

材料工具为 `hack/release-external-inputs`。NATS server auth 副本只含 `nats.conf`，
原 bootstrap/outbox/scheduler 凭据保留原用途。证书与凭据轮换需要新的不可变
副本和消费者切换；原 Secret 更新不会自动更新副本。

定向验证通过 `go test ./internal/deploymentcontract ./internal/releasebundle -count=1`；
真实渲染测试用生产 canonical 摘要函数核对两个 ConfigMap 的内容注解。
更早的材料工具 race、database/Fleet/Control 测试和原生权限正反例均已通过。

远程材料、脚本和证据在 `.70` root 私有目录
`/opt/vela-cluster/release-inputs-20260915/`；实际源码 archive 位于
`control-schema96-image/source.tar.gz`。未提交或推送；受保护主机和暂缓的数据盘
保持用户确定的操作边界。完整 release 仍需 Fleet/Stage、Host packages、
模型/ResidencyPlan 与完整 PKI；R3/R5 的容量及 R6 的真实业务验收保持开放。
