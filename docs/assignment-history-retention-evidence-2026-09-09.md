# Assignment history retention boundary

日期：2026-09-09。验证命令：

```bash
go test -race ./internal/stageworkeragent -run \
  'TestAssignmentAdmission(CrossProfileWatermarkAndBoundedBacklog|ConcurrentBeginHasOneWriter)|TestAssignmentJournalPreparation' \
  -count=1
```

结果：通过，耗时约 3.3 秒。

验证结果：

- `MaxRecords=3` 时，第四次准入返回 `ErrAdmissionCapacity`；旧执行记录不会被
  静默删除或覆盖；
- journal close/reopen 后，watermark=3、pending history、原始 StageProfile
  绑定以及 `CLOSED` phase 均保持；
- 旧 profile、旧 authority 或旧 acquire identity 不能绕过 watermark 重新准入；
- 并发 Begin 仍只有一个 active writer，其他请求不会创建第二个 execution history。

这证明当前策略的安全底线是“历史满时 fail closed”。它没有关闭长期运行所需的
持久 cutoff、历史精确证明、重启后的 cutoff 语义或安全 reclamation；不能通过简单
删除旧记录来解决容量问题。`MaxRecords` 上限仍是后续 sustained-arrivals 验证的
前置约束。

相关实现：[assignment_admission.go](../internal/stageworkeragent/assignment_admission.go)。
