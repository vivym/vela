# minimax-h3 独立部署与验收（2026-09-17）

本次新增 `minimax-h3`，目标为 1 Encoder、8 DiT、2 Decoder；保留
`minimax-h3-live-validation` 的独立路由和现有用户。11 个实例已部署并通过真实 API、
完整音视频、唯一计费与执行后容量恢复验收，中转项目已授权。
本部署仍处于 INTERNAL 发布范围，不代表九项
Production Gates、吞吐 SLA 或故障注入验证全部完成。

2026-09-17 22:32 更新：19:15 核查发现的修复差异已通过 V10 发布闭合。
11 个 GPU 实例已应用最新修复，正式模型取消恢复、后续完整音视频、幂等和计费
验收通过，详见[最新修复版部署与验收](h3-minimax-v10-latest-fix-deployment-2026-09-17.md)。
当前中转队列 / 并发为 64 / 8、阶段队列 128。下文保留初次独立部署的 V9 镜像、
路由和额度记录；当前运行版本及回执以 V10 报告为准。

## 实例与本地缓存

| 角色 | Kubernetes Node | IP |
| --- | --- | --- |
| Encoder × 1 | server-31 | 10.1.201.13 |
| DiT × 8 | server-41 / server-34 / server-39 / server-10 / server-38 / server-42 / server-14 / server-28 | 10.1.201.14 / .15 / .16 / .17 / .18 / .20 / .21 / .22 |
| Decoder × 2 | server-33 / server-29 | 10.1.201.23 / .24 |

这 11 台均为 256 GB 内存节点，每个实例独占本机 GPU 0。Encoder 与 Decoder
独立；Decoder 对应执行图的 `vae` stage / `VAE_DECODER` component。
缩略图复用原有、运行在 GPU 节点的 CPU Worker，不计入 11 个 GPU 实例。
没有重启主机，包括明确禁止重启的 `.44/.56/.57/.66`。

所有目标节点均已有完整本地权重与 native snapshotter 镜像缓存。
权重路径为：

```
/var/lib/vela/models/dad0bd33673dee603d107fda712ee22e1e6dca2268748f2b3d723f93c74d5aa1
```

权重内容为 59,510,503,579 字节，manifest SHA256 为
`6eac1f04b601ebfd5d9cf54a130d9c3b7e5a13549fa1a8517bea2730657a28c0`。
推理直接读各节点缓存；阶段间 conditioning/latent 和最终产物仍按执行图传输。

批量缓存最终为 **44 台 256GB 节点全部 VERIFIED**：首批 43 台，以及补齐的
`.19/server-36`。`.19` 原因是批处理假设免密 sudo，与该节点要求密码不符；
使用已有授权凭据恢复后，于 2026-09-17 06:26 UTC 校验全部 59,510,503,579 字节
通过，后台服务正常退出，临时 sudo 输入文件已删除，未修改 sudoers。
这 44 台缓存节点包含本次全部 11 台实例，但不表示在 44 台上都启动了模型。

2026-09-17 06:19 UTC 逐台核验：11 台的 `vela-minimax-h3.service` 均为
`enabled/active`，已配置开机自启。重启恢复需要重新核验 Runtime epoch、
Node journal 和 DRA 授权，不能把旧 READY 记录当成重启后的证据。本次未通过重启
生产主机验证该过程。

## 发布材料与身份

材料由平台侧生成，不需要业务调用者提供：

- `.70:/opt/vela-cluster/h3-minimax-20260917/release-v9`：11 个实例的 approved plan、
  WorkerBundle、ConfigMap、DRA、逐节点权限与启动输入。
- 同目录 `business-v9/publish.sql`：精确镜像对应的 graph、profile、Rate Card
  与版本化路由发布事务。发布前执行同一事务并回滚，随后正式提交。
- 同目录 `qualification-v8`：11 个实例的实际 native 预热资格证据。
- 同目录 `stage-profiles-v9`：三个 GPU StageProfile 的映射与认证回执。
- 同目录 `model-routes`：数据库 109 迁移、原函数备份及迁移回执。

每个 Worker member 使用单独的 `vela-worker-tls-<memberUUID>` Secret，
SPIFFE 为 `spiffe://vela.internal/stage-worker/<memberUUID>`。签发私钥仅在
管理节点，未分发到 Worker。节点 Kubernetes 身份使用 `vela-node-<node>`，
绑定对应 Pod/ConfigMap/Secret 名称的窄权限。

本任务 release-v9 发布使用 Fleet ConfigMap `vela-fleet-minimax-v9-publish-be6fa81a`。
后续按当前 Deployment 的 ConfigMap 指针做 CAS 更新，不能覆盖其他维护任务的 rollout。

| 组件 | 固定镜像 digest |
| --- | --- |
| Worker Agent | `10.1.201.70:5005/vela-stage-worker-agent@sha256:2422e76ac483182a12a2a6161fb15157e1e1f564b5eb8d08e818e742156a3ac8` |
| H3 Runtime | `10.1.201.70:5005/vela-h3-stage-runtime@sha256:5bbb3a6306849da82f649703b241b757207b393dc558e09ef035ac0f8ad4e6ff` |
| Fleet Controller | `10.1.201.70:5005/vela-fleet-controller@sha256:dbb6f6ddf8751101cb4bc4c0c384c7e9df3e3c82c8e57cff2e31b36f143a9af4` |

这些是本次发布所核验的镜像；后续其他维护任务发布新镜像时应以现场为准。
Secret 内容、API Key、客户端私钥和模型权重不提交 Git。

本次发布事务因全局 cutover 被共享维护更新，首次 dry-run 正常拒绝
`cutover changed`。读取最新状态后重新生成、dry-run 回滚与正式提交均通过；
结果同时保留两个 ModelRevision 的路由，没有覆盖旧模型发布。

## 多模型路由与权限

原实现只用 `stage_cutover_control.current_revision_id` 解析模型路由，发布另一个
模型会导致原模型提交返回 `capacity_unavailable`。数据库迁移 109 新增
`model_stage_routes`，按 model revision 保存当前 cutover revision：

- 激活仍调用原 `vela_activate_stage_cutover`，保留审批、摘要和生产门禁。
- 同模型更新替换自己的指针；其他模型指针继续有效。
- INTERNAL 项目授权仍绑定精确 cutover revision，不自动继承到新版本或另一模型。
- `vela_authorize_stage_cutover_internal_project` 可以授权任何仍在使用的模型路由，
  已被替换的版本不能继续授权。
- 全局非 `STAGE_ONLY` 模式清空所有路由；重新开放必须逐模型显式发布。
- Up/Down 在操作前锁住 cutover control，避免并发发布穿过迁移窗口。
  Down 遇到额外模型路由会拒绝，防止静默丢失服务。

新模型 revision：`217c75f1-2f5e-547f-be1d-4e117da9e7e2`；
Graph：`50c61fd1-2ce9-5c08-ac4b-ed7622da979e`；
ExecutionProfile：`fe5e0591-a10f-5ecd-bcca-b352684e1d65`；
Cutover：`9f979515-1341-5cd2-99eb-479ee595069c`。

不建议用全局关闭回退单个模型。业务需要临时禁用新模型时，应通过 Catalog
受控管理禁用其模型/preset/profile，并核对在途任务与计费；不要删除 journal，
不要直接修改 `model_stage_routes`。旧模型无须切换指针即可继续使用。

## 本次修复

1. Worker 对已 CLOSED 但未 RETIRED 的持久化 assignment，先完成经过认证的
   terminal retirement，再恢复 Discover/Acquire，避免取消确认被误当成彻底 drain。
2. Fleet 在 Registry 仍授权退休、且 Pod 已有 deletion timestamp 时，接受移除
   protection finalizer 后的重试 DELETE；非终止 Pod 的 finalizer 漂移仍拒绝。
3. Bootstrap 在写 journal 前核对 `max-records=32`，与 Fleet 渲染共用常量。
   不通过改写已有 journal 来更换身份或补救不匹配的配置。
4. Broker 示例与 release validator 统一为 `/run/vela-pidfd-broker/broker.sock`。
5. 为独立组件生成逐 member TLS Secret 和正确的 `vela-node-<node>` 权限输入。

新实例在 Node Registry 中 READY，并不等于已经有可调度容量。实际注册还要求
StageProfile 为 CERTIFIED/ACTIVE；本次先采集每个实例的 native 预热证据，
再将三个 WorkerProfile 与三个 StageProfile 从 CANARY 认证为 CERTIFIED，
由 Worker 自行发布非零容量，未手工制造容量观测。

## API 使用与验收

入口：`https://vela.marslab.ic/api`。支持的组合沿用已验证原生规格：

```json
{
  "model": "minimax-h3",
  "generation_preset": "fast",
  "service_class": "standard",
  "output_spec": "h3-native-av-1344x768-5s-24fps",
  "generation_count": 1,
  "prompt": "A red sailboat glides across a calm lake at sunrise, soft water sounds.",
  "h3": {
    "task": "t2va",
    "target": {"short_edge": 768, "aspect_ratio": "16:9", "duration_seconds": 5},
    "sampling": {"num_inference_steps": 20, "quality": "lossless"}
  }
}
```

计费单价沿用现有验证配置：100 minor CNY，即 ¥1.00/条。没有设置新的商用价格。
API 调用者继续使用现有项目 API Key；服务是否可用取决于该项目对模型路由的授权。

本机 `.70` 验收时无法通过默认 DNS 解析域名，因此验收程序仅对
`vela.marslab.ic` 指定连接地址 `10.1.201.70`，仍保留原域名 TLS SNI 和 CA 校验。
未改系统 DNS/hosts；这不能证明所有客户端的 DNS 已配置。

完整验收使用 `hack/verify-h3-api-flow.py`，验证匿名拒绝、受理、轮询、
完整音视频及缩略图下载、对象版本/大小/SHA256、ffprobe、唯一 Charge、
报价一致和幂等重放。不会裁剪原生输出。

旧模型恢复后真实验收 Job：`4d1bc058-c0ee-4b75-b060-75fbc805ee46`，
回执在 `.70:/opt/vela-cluster/h3-api-acceptance-20260917-stop-recovery/run/receipt.json`，
`passed=true`。新模型修复后验收目录：
`.70:/opt/vela-cluster/h3-api-acceptance-20260917-minimax-v8`（测试目录保留历史名称，
实际执行 release-v9），Job：`8957b56d-3495-47eb-b5be-55fadd091c03`。

原共享项目并发额度被另一验收占满，调度器明确报告
`PROJECT_CAPACITY_EXHAUSTED`。因此创建独立验收项目
`eb8f032a-1228-5dc6-8ea6-bd0c6d75ddb7`（队列 16、并发 2），
签发仅该项目可用、7 天有效的临时服务 Key。旧共享项目中的本次排队任务
`ff1c0ef2-46aa-4f5e-a765-14f8d29c9b38` 经 API 取消，
返回 `billable=false`；未修改其他验收的项目额度或任务。

## 代码验证

- `go test ./...` 通过。
- PostgreSQL 多模型升级、同时受理、替换、权限负例、维护模式、Down/Up 与并发
  发布/降级测试通过；相关 StageCutover/StageAdmission 测试通过。
- H3 release generator 与 API flow 共 13 项 Python 测试通过。
- Linux root bootstrap 预检和 journal custody 两项定向测试通过。
- Worker 停止恢复与 Fleet 删除重试定向/race 测试通过。
- 多模型迁移由 Spec/Standards 两个维度复审；发现的迁移竞态和授权负例缺口已修复。

## 边界

11 实例部署和单次业务成功不代表 8 DiT 满载吞吐已经测出。1:8:2 是初始配置，
后续应按阶段排队、服务时间、GPU 利用率、显存和失败率调整。磁盘为集群内复制、
无独立故障域的既有方案；外部告警通知仍待配置。证据中没有捏造 Production Gate、
容灾测试或商业质量认证。

## 终止恢复修复与发布结果

首次真实新模型 Job `dac13805-1337-471f-9047-b755d8794335` 的 Encoder、DiT、
Decoder 均成功，但缩略图依赖阻塞；其 Encoder 和执行过的 DiT 在终止恢复中反复
报告 `StageAuthority is stale`。不能把 READY/CONNECTED 或 GPU 阶段成功当作完整
交付与可持续接单的证据。

本轮采用已通过定向和完整 Go 检查的时钟偏差与 floor 重试修复。修复镜像引用必须
使用具体 Linux/amd64 manifest；带 provenance 的 OCI index 被 Node Agent 预检
拒绝。其次，StageProfile 的 `runtime_image_digest` 是不可变的，镜像更换后必须
建立对应的新 StageProfile 和新执行配置；不能修改旧认证记录来强行注册。

修复版 GPU Runtime 的精确 digest 为
`sha256:5bbb3a6306849da82f649703b241b757207b393dc558e09ef035ac0f8ad4e6ff`，
Worker Agent 为
`sha256:2422e76ac483182a12a2a6161fb15157e1e1f564b5eb8d08e818e742156a3ac8`，
Node Agent 二进制 SHA256 为
`064d64b9d74d6b1299d02418c632bb200a5e345a0ea09bf005871dfb331f911c`。
11 台节点以该精确镜像完成了 native 预热，回执位于
`.70:/opt/vela-cluster/h3-minimax-20260917/qualification-v8/receipt.json`，SHA256：
`e0b109425f46544ef4c428af785347879812f1a455a0d33875d091a9080d00e9`。
该回执只证明组件初始化与预热；独立的完整 API 验收结果见下文。

对应的三个新 StageProfile：

| 阶段 | StageProfile |
| --- | --- |
| Encoder | `b7887377-9bad-5416-8707-7204aa90d12f` |
| DiT | `a3a1b20b-0ba3-5cf8-a4bb-9234ef787472` |
| Decoder | `b7397370-9030-57a5-9619-a684974c207f` |

新发布材料在 `.70:/opt/vela-cluster/h3-minimax-20260917/release-v9`，Plan 为
`c9000cff-03af-5120-95ba-886a1da2a404`。所有旧实例先核验没有活动分配，再 fencing、
停止服务、确认容器与 containerd task 退出，最后按授权清理 Pod 和 GPU claim 模板。
没有删除或改写已有 journal，没有重启主机。

v7 中 10 个从未 bootstrap 的候选 Pod 因没有 journal receipt，平台拒绝其普通退休；
它们已撤回并 FENCED，保留 runtime-startup scheduling gate，未运行任何容器。
核验其 ResourceClaim 无 allocation 后，仅回收阻碍后续发布的旧 GPU claim 模板。
Pod 记录与拒绝证据保留，未绕过 admission webhook；这批历史资源清理仍是维护尾项。

新的业务路由发布被显式限制为：GPU StageProfile 和共享缩略图 StageProfile 必须
都绑定修复后的精确镜像且经真实认证，之后才运行新 API、完整音视频、唯一计费与
执行后容量恢复验证。上述验收已通过，中转站授权和并发提升已执行。

早期隔离验收 Job `dac13805-1337-471f-9047-b755d8794335` 的执行快照绑定已退休的
缩略图 Profile，不能直接替换。在 2026-09-17 06:17 UTC 通过正常 API 取消该内部
测试任务，终态 CANCELED。因已到 Billable Start，产生唯一 100 minor CNY 的
CUSTOMER_CANCELLATION Charge，未产生 VisibleCompletion；此费用属于内部验收
项目，不属于中转站。原始失败、取消和账本证据全部保留，没有将其算作成功验收。

## 最终验收与中转开放

2026-09-17 06:23:58 UTC，Job `8957b56d-3495-47eb-b5be-55fadd091c03`
成功，四个 StageRun 均 SUCCEEDED。从提交至完成约 7 分 43 秒；这是一次真实任务
的观测，不代表并发吞吐或延迟 SLA。

| 检查 | 实测结果 |
| --- | --- |
| HTTPS API、匿名拒绝、项目鉴权 | 通过 |
| 相同幂等键重放、不同请求冲突 | 同一 Job；不同 payload 返回 409 |
| 完整视频 | 1344×768、124 帧、24fps，2,162,953 字节 |
| 音轨 | AAC、双声道、32kHz、5.175 秒 |
| 缩略图 | WebP，7,844 字节 |
| 下载版本、大小、SHA256 | 与已提交 Artifact 元数据一致 |
| 计费 | 1 VisibleCompletion、1 POSTED Charge、100 minor CNY |
| 终态重放 | 没有重复费用或 ArtifactSet |
| 执行后恢复 | 4 个 allocation 全部 RELEASED；12 个相关实例 fresh capacity ≥ 1 |

06:25 UTC 中转项目已显式授权新 Cutover。项目排队上限保持 10，运行上限由 2
提高到 8。永久 Key 未更换，通过 HTTPS 只读鉴权探测；本次开放未创建中转 Job
或中转 Charge。旧模型路由保留。调用者使用 `minimax-h3` 和本文的精确 SKU。

完整视频 SHA256：
`82f08199265524770843617513d937b521b3cd78f694a9c983435b989115220a`。
视频及缩略图位于 `.70` 验收目录的 `run/complete-video.mp4` 和 `run/thumbnail.webp`。

脱敏证据：[deployment-receipt.json](evidence/minimax-h3-independent-20260917/deployment-receipt.json)。
包含预热资格、镜像/Profile 闭合、开机自启、业务发布前后路由、API 验收、ffprobe、
唯一费用、容量恢复、中转授权和早期测试取消。运行时 Secret 与签名下载 URL 不入库。

剩余边界：尚未测量 8 DiT 满载吞吐；没有通过重启验证恢复；历史 v7 的 10 个未启动
Pod 留待受控清理，不占 GPU；九项 Production Gates、独立故障域及外部告警通知
不在本次成功回执的证明范围内。
