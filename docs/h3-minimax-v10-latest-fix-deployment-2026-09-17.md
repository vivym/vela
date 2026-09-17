# `minimax-h3` 最新修复版部署与验收（2026-09-17）

已将 `minimax-h3-live-validation` 后续修复发布到正式 `minimax-h3`，并完成真实 API 闭环验收。
旧 `minimax-h3-live-validation` 路由保持不变。

## 发布结果

- 11 个正式 GPU Worker：1 Encoder、8 DiT、2 Decoder。
- 全部 Worker `READY/CONNECTED`，Pod `Running`，新 Runtime 与 Worker Agent 的
  `restartCount` 均为 0。
- 新 Runtime：`vela-h3-stage-runtime@sha256:71bd111548526028064af17ed575b04dfb4a8b733a0306ec433208e52f6b3f95`。
- 新 Worker Agent：`vela-stage-worker-agent@sha256:a4896ac14b7280bd2bb203e915e66c9708f10eb6e8ac261ea3aed22dc56834f2`。
- 实际导入的 `fast_h3.vela.driver` 包含取消收尾方法 `_finish_cancelled_execution`，并与修复版摘要一致。
- 3 个新 GPU StageProfile 已升级为 `CERTIFIED`，`minimax-h3` 新 cutover 为
  `150668d8-0575-556c-ac8a-94e241833664`；`live-validation` 仍为原 cutover。
- 阶段队列上限保持此前配置的 128；中转站项目排队 / 运行上限保持 64 / 8。
- 11 台节点均已预拉取镜像和本地权重；最少可用空间约 585 GiB。没有重启主机，
  `.44/.56/.57/.66` 没有改动。

## 版本与发布记录

发布前已提交 `b19dafa`（取消恢复、忙时受理修复）及 `7d49b18`（运行记录与版本差异）。
六个受影响 Go 包的定向测试通过。共享 Control 已包含修复，本次更新正式模型的 GPU
Runtime 和 Worker Agent；Node Agent 二进制保持已验证版本。

发布材料位于 `.70:/opt/vela-cluster/h3-minimax-20260917/release-v10`，
Plan 为 `3c1181d1-e09a-5780-864b-7bacb42a8cd4`，ExecutionProfile 为
`4286d19d-f531-512c-a433-bc32f5e036f7`。Fleet 发布合并当时最新 ConfigMap，并以
Deployment resourceVersion 做 CAS；旧实例在无非终态 Job、无活动分配后退休。
这次包含正式模型旧池撤回和新池启动窗口，不是零停机滚动发布。

初版生成输入错误地映射了硬件身份且缺少预热证据模板，预检失败。修正时保留实际
GPU/PCI、WorkerProfile、设备及 membership 摘要，在 bootstrap 前核对新配置，
没有用无效输入启动 Runtime 或改写旧 journal。初版材料保留在远端
`release-v10-incomplete-preflight`，Git 中记录的是修正后的发布输入。

实际时间顺序（UTC）：13:38 更新 Catalog 路由时三个新 GPU StageProfile 尚为 CANARY；
约 13:48 完成 CERTIFIED，13:50 再核对实际 native residency、非空 warmup/canary
摘要及版本绑定，13:53 才开始真实 API 验收。不能将此记录表述为先认证后切换路由；
后续发布应把认证就绪检查前置。最终三个 Profile 全部 CERTIFIED，原项目授权和价格
均已核验保留。模型仍属于 INTERNAL 发布范围。

## 真实 API 验收

后台验收目录：`.70:/opt/vela-cluster/minimax-h3-busy-cancel-v10-20260917/`。

通过的检查：

1. 第一个任务在 DiT 持续运行超过空闲容量观测 TTL 后，第二个任务仍返回 HTTP 202。
2. 取消运行中的第一个任务后，原先承载它的精确 DiT Worker 恢复为 `READY/CONNECTED`，
   生成了取消后的新鲜正容量观测，且没有活动分配。
3. 取消后的后续任务返回 HTTP 202，不需要人工重启。
4. 两个成功任务各返回完整视频、AAC 音轨和缩略图；两个成功任务各只有一笔
   `VISIBLE_COMPLETION` Charge。取消任务只有一笔 `CUSTOMER_CANCELLATION` Charge。
5. 同一幂等键重放没有重复 Charge，修改请求体复用幂等键返回冲突。
6. 11 个正式 GPU Worker 加上共用缩略图 Worker，其 Pod UID、容器 ID 和 restart count
   在取消恢复前后完全一致。

取消 Job 为 `a84e1c6d-7642-449d-aece-012cf4f35069`；成功 Job 为
`9c17441f-61a9-4232-8443-9983857b827e` 和 `b0bbc56f-129e-4ac1-a098-92b42c277495`。
每个成功任务一笔 ¥1 完成费用；取消任务按既有策略产生一笔 ¥1 取消费用，属于独立
验收项目，没有向中转站项目发起测试任务。原 DiT Worker 在取消决定后约 37.8 秒
产生新鲜正容量观测；这是本次观测，不是恢复时间 SLA。

媒体证据为 1344×768、124 帧、24 fps、约 5.167 秒视频，立体 AAC 32 kHz，容器约 5.175 秒。

## 证据与边界

- [API 取消恢复回执](evidence/h3-minimax-v10-20260917/acceptance.json)
- [11 个实例就绪回执](evidence/h3-minimax-v10-20260917/all-ready.json)
- [最终镜像、运行代码、自启动、队列与路由核验](evidence/h3-minimax-v10-20260917/publication-verification.json)
- [实际 native 预热与 Registry 证据](evidence/h3-minimax-v10-20260917/native-qualification.json)
- [Catalog 价格、授权、认证闭合](evidence/h3-minimax-v10-20260917/catalog-verification.json)
- [发布事务回执](evidence/h3-minimax-v10-20260917/publication-receipt.json)
- [幂等与费用补充检查](evidence/h3-minimax-v10-20260917/supplemental-verification.json)
- [第二任务媒体探测](evidence/h3-minimax-v10-20260917/second-ffprobe.json) /
  [第三任务媒体探测](evidence/h3-minimax-v10-20260917/third-ffprobe.json)
- [批准计划](evidence/h3-minimax-v10-20260917/approved-plan.json) /
  [部署输入](evidence/h3-minimax-v10-20260917/rollout-input.json) /
  [发布 SQL](evidence/h3-minimax-v10-20260917/publish.sql)
- [最终 Catalog / Route / Profile 状态](evidence/h3-minimax-v10-20260917/catalog-status-final.txt)

完整视频与缩略图保留在上述后台验收目录的 `second/`、`third/` 下，文件名为
`complete-video.mp4` 和 `thumbnail.webp`。Git 保存媒体探测及校验回执，不包含
API Key、签名下载链接或私钥。native 资格原始回执 SHA256 为
`a30a96238ab24970120dd5652c603412434f214b4cde568a0b7cd974c4def9fa`。

这次证明了正式模型的修复版 API、取消恢复、完整音视频和计费闭环。它没有完成持续满载
吞吐压测、节点断电故障注入或全部 Production Gates；这些仍需单独安排。
