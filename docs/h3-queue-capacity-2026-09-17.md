# H3 排队容量扩容（2026-09-17）

按用户“现在排队有多少容量？我希望容量大一些”的要求，已在生产数据库调整中转站
项目与当前 H3 路由的队列上限。2026-09-17 19:09:31（Asia/Shanghai）复核时，
全平台非终态任务为 0，中转站排队 0、运行 0，可用排队额度为 64。

## 实际配置

| 配置 | 调整前 | 调整后 |
| --- | ---: | ---: |
| `relay-station` Project 普通排队上限 | 10 | 64 |
| `relay-station` Project 运行上限 | 8 | 8 |
| 当前两个 H3 路由关联的各 CapacityPool READY 阶段队列上限 | 32 | 128 |
| 旧模型验收 Project 排队 / 运行上限 | 4 / 1 | 4 / 1 |
| 独立模型验收 Project 排队 / 运行上限 | 16 / 2 | 16 / 2 |

Project 排队额度按 `queued_count - retry_wait_count` 判断，是项目内所有 Key 和模型共享的
普通排队额度。64 个排队名额与 8 个运行名额分开计算；实际受理还要求可用的完整阶段路径、
未满的阶段队列、有效 SKU 和足够的合同额度。128 是每个阶段池的 StageRun 数量上限，
不是任务并发数，也不能将四个阶段的额度相加。

现场路由关联 11 个 ACTIVE 池，其中有 7 个池具有 READY/CONNECTED Worker，另 4 个是
同一缩略图 StageProfile 的旧池投影。队列投影仍可能涉及这些池，因此将全部 11 个关联池
一并调整，而不是只修改有 Worker 的池。其余历史版本的池没有修改。

`minimax-h3` 现场为 1 Encoder、8 DiT、2 Decoder，就绪缩略图 Worker 另计；
`minimax-h3-live-validation` 为 1 DiT，Encoder 与 Decoder 共享 AUX Worker。
两个模型共享缩略图阶段。这些是现场就绪数量，不代表已经完成满载吞吐验收。

## 时间与容量边界

当前 `standard` 的排队/重试 allowance 为 3,600 秒，加上现有计算预算 3,600 秒与
finalization 预算 600 秒，Job 从受理起总有效期为 7,800 秒（2 小时 10 分钟）。
已受理任务固定自己的过期时间，不会因队列扩大而自动续期。

本次 64 个排队名额用于正式模型吸收短时批量请求。没有把容量直接提高到几百或几千，
也没有承诺 64 个任务一定在有效期内完成；稳定吞吐还受 Encoder、Decoder、共享缩略图、
对象存储及其他项目负载影响。旧模型只有一个 DiT，批量请求应使用 `minimax-h3`。
若需要持续大批量积压，下一步应实测完整流水线吞吐，再制定更长等待时间的版本化服务
策略及对应存储预算，不能只改队列数字。

## 持久化与发布范围

变更只更新 PostgreSQL 的 `projects.queued_limit` 和 `capacity_pools.max_ready_queue_depth`。
数据库是运行期队列上限的持久权威；核对现场 `vela_apply_residency_plan_v63(jsonb)` 后确认，
已有 ResidencyPlan 重放会直接返回，不会将已有池的上限恢复成 bootstrap 的 32。
因此不需要重启主机、Worker 或 Control，也不需要改 Fleet ConfigMap 或旧计划的签名摘要。

既有 Job、定价/计费、执行身份、两条模型路由均未修改。仓库中固定历史身份的
`build-h3-live-workers.py` / `build-h3-minimax-bundle-plan.py` 仍保留历史 bootstrap 值；
以后新建池的发布计划应明确采用 128，并生成新的 plan identity / approval digest，
不能编辑已经批准的旧计划后用原身份重新发布。数据库恢复到本次扩容之前的备份时，
也需要核对并重新执行容量调整。

可审查的配置源与现场证据：

- [应用 SQL](../deploy/operations/h3-queue-capacity-20260917.sql)：固定 Project、两个 route revision 和 11 个池；行锁、旧值比较及完整池集合校验，漂移即失败。
- [回滚 SQL](../deploy/operations/h3-queue-capacity-20260917-rollback.sql)：恢复 10 / 32；现有队列高于目标时拒绝回滚，不删除已受理任务。
- [变更前清单](evidence/h3-queue-capacity-20260917/before.json)、[应用回执](evidence/h3-queue-capacity-20260917/applied.json)、[验证回执](evidence/h3-queue-capacity-20260917/verification.json)。

现场完整操作材料位于 `.70:/opt/vela-cluster/queue-capacity-20260917/`。平台管理员通过
当前 PostgreSQL primary 执行 SQL；若模型路由、池集合或项目额度已有后续变更，应重新
检查并生成新的变更，不能去掉 SQL 的漂移检查强行重放。

## 验证

1. 提交事务后回读中转站 64 / 8、全部 11 个目标池 128，以及两条模型路由保持原 revision。
2. 在仅持有目标 Project 行锁的独立事务中，将普通排队计数临时置于 63；运行 Admission
   使用的原子增量条件，第 64 个名额更新 1 行，第 65 个名额更新 0 行。检查数据库约束后
   整个事务回滚，其他连接看不到临时计数，也未创建 Job、Credit Reservation 或 Charge。
3. 完整回滚 SQL 以 `ROLLBACK` 结尾演练通过，随后再次确认线上仍为 64 / 128。
4. 使用中转站现有永久 Key，经 `.70`、`.71` 的 HTTPS 域名入口查询 active Jobs，均为 HTTP 200。

这验证了配置生效、数据库额度边界与入口可用性；没有提交 64 个真实付费任务，
不构成满队列压测或 8 并发吞吐证明。已有真实音视频与计费验收见
[独立模型验收](minimax-h3-independent-deployment-2026-09-17.md)和
[旧模型恢复验收](h3-live-admission-cancellation-repair-2026-09-17.md)。
