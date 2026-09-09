# Assignment history reclamation design

当前 Worker assignment journal 在 `MaxRecords` 达到上限时返回
`ErrAdmissionCapacity`。这是正确的 fail-closed 行为：`watermark`、原始
authority、renewal 链、input-drain 证据和 execution floor 仍然可被重放验证。
直接删除最旧记录会制造一个无法证明的 sequence gap，因此不能作为 sustained
arrivals 的实现。

## 目标不变量

回收后仍必须满足：

1. `watermark` 单调递增，且所有低于 durable cutoff 的 sequence 都有一份不可变
   的 aggregate proof；
2. 任何 renewal、duplicate Acquire、Stop、input drain 和 execution floor 查询都
   能从保留记录或 cutoff proof 得到同样的结论；
3. cutoff proof 与 Worker scope、instance epoch、member、profile binding、history
   digest 和前一个 cutoff proof 链接，不能跨 Worker 或跨 epoch 借用；
4. 未关闭的 assignment、未完成 input drain、未完成 materialization 或未完成
   terminal retirement 不能进入 cutoff；
5. 重启、替换 Runtime 和 control-session reattach 只允许读取 cutoff proof，不能
   重新发放已被 cutoff 覆盖的 authority；
6. proof 写入、cutoff 提升和记录删除必须是同一持久事务，任一步失败都保持旧
   cutoff 和原始记录可恢复。

## 建议数据模型

保留现有逐条 journal，增加一个 append-only `history_cutoffs` 记录：

- `scope_digest`、Worker instance/member/epoch 和 history schema；
- `from_sequence`、`through_sequence`、`through_digest`；
- 逐条记录集合的 Merkle/累积 digest 及 proof revision；
- proof 中每个 assignment 的 terminal disposition、input-drain、renewal
  watermark 和 execution-floor摘要；
- `previous_cutoff_digest`、签名、created-at 和 committed transaction id。

本地文件 journal 可以用 `cutoff-N.json` + 原子 rename/fsync 表示；数据库侧使用
同一 cutoff digest 和 transaction receipt。两侧 digest 不一致时必须拒绝准入并
重新对账，不得自动重建。

## 回收协议

1. 停止接收新的 Acquire，或将 scope 置于短暂 `CUTOVER`，固定当前
   `candidate_through_sequence`；
2. 读取完整 retained history，验证所有 authority、renewal、input-drain、floor
   和 materialization 状态；
3. 生成并持久化 cutoff proof，包含前一个 cutoff digest；
4. fsync proof 后再在同一 owner 事务中提升 cutoff；
5. 只删除 `sequence <= through_sequence` 且已被 proof 覆盖的逐条记录；
6. 重新打开 Acquire，第一条新 assignment 必须严格大于 cutoff sequence；
7. 重启和故障恢复优先读取 cutoff proof。发现 proof 已提交但删除未完成时，重复
   删除是幂等的；发现删除已发生但 proof 未提交时，必须 fail closed。

## 最低验收矩阵

- cutoff 前最后一个 assignment、cutoff 后第一个 assignment 的顺序和 digest；
- proof 写入前、写入后、删除前、删除后各个 crash/response-loss 边界；
- 重启读取 cutoff 后拒绝旧 authority、接受新 sequence；
- renewal 链跨 cutoff 仍能验证，错误 profile/member/epoch 不能借用 proof；
- 一个 pending input、materialization journal 或 terminal retirement 存在时回收被
  拒绝；
- cutoff 重放两次不产生新的 watermark、Charge、allocation 或 materialization；
- 超过当前 `MaxRecords` 后的长期 CPU/mock arrivals 能继续执行，且 journal 大小
  由 cutoff proof 而不是无界历史增长控制。

在这组验收通过前，`MaxRecords` 满后拒绝准入仍是正确行为；当前有限 campaign
不能被解释为 sustained throughput 或长期资源上界证明。

## 当前实现暴露的空间上界问题

当前实现确实删除逐条 assignment，但永久保留每一条 `history_cutoffs`。实测
40 条连续 cutoff 令 state 文件从 `540` 增长到 `41,398` bytes，增长来自 proof
chain 本身，而不是 goroutine 或 heap 泄漏。因此“删除逐条历史后空间由 proof
控制”的原目标尚未成立：proof chain 仍然是随 arrival 线性增长的历史。

下一版应引入可验证的 **checkpoint compaction**，而不是直接截断数组：

1. 在固定 `through_sequence` 生成 `HistoryCheckpoint`，包含 scope/Worker identity、
   `from_sequence`、`through_sequence`、累计 history digest、terminal/input/materialization
   汇总 digest、最后 cutoff digest、schema/revision 和 owner signature；
2. checkpoint 先独立 fsync，再由 journal 原子提交 `checkpoint_digest` 与新的
   `HistoryBase`；提交记录同时声明被覆盖的 cutoff digest 区间；
3. 只保留 checkpoint 加最近有限数量的 cutoff，后续 cutoff 的
   `PreviousCutoffDigest` 绑定 checkpoint digest；旧 cutoff 被压缩后不能再被静默
   伪造或跳过；
4. 恢复时必须验证 checkpoint 与保留 cutoff 的连续范围、identity、累计 digest 和
   schema revision。checkpoint 缺失、范围重叠、digest 不匹配或 signature 失效都
   fail closed；
5. compaction 失败时允许旧链继续运行，不能先删除旧 proof 再等待新 checkpoint。

验收标准是 cutoff 数量和 state 字节数在固定窗口后趋于上界，同时历史 digest 对账、
跨 checkpoint renewal/拒绝旧 authority、重启和 response-loss 语义保持不变。在
该协议实现并通过故障矩阵前，不能把当前 bounded campaign 宣称为长期 sustained
arrival 证明。

当前已先落地 `AssignmentHistoryCheckpoint` 的 canonical JSON、identity、范围、
proof digest、compacted-count 和 revision 校验层，并接入 journal recovery 的
边界校验：`HistoryBase` 与 checkpoint 必须一致，保留 cutoff 必须由 checkpoint
digest 锚定。它尚未执行旧 proof 删除或改变现有 schema version，因此不会提前
宣称 compaction 已完成。当前还已实现单 checkpoint 的原子 compaction：只有在
prefix range、cutoff count、last cutoff digest 和 reclaimed `HistoryBase` 全部匹配
时才替换旧 cutoff 前缀；提交后恢复会验证 checkpoint-only 状态。多 checkpoint、
目录同步失败后的两种落盘结果、checkpoint 前缀加 retained suffix 的完整 digest
重锚定已在 bounded campaign 中验证；多 checkpoint、
重复 checkpoint compaction 已在同一 bounded campaign 中验证，第二个 checkpoint
能够继续覆盖 retained suffix 并将 cutoff 数量重新压到零；掉电级别故障和外部签名
仍未闭合。
