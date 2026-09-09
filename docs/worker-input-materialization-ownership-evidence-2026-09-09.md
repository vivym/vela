# Worker input / transfer / materialization ownership review

日期：2026-09-09。范围是当前 CPU/mock Stage Worker 代码；这份报告记录已验证
的本地边界和仍缺少的生产装配证据。

## 已确认的 owner 边界

| 对象 | 当前写入 owner | 保护与证据 |
| --- | --- | --- |
| root input | `HTTPSRootInputResolver` | 通过 `securefile.OpenTrustedRoot` 打开受信任目录；先写 `.partial.<uuid>`，校验 size/SHA-256、`fsync` 后原子发布；取消或失败删除 partial。 |
| TransferTicket input | `AssignmentInputResolver` + `FilesystemInputTransferTarget` | ticket、destination、artifact、版本、digest、size 全绑定；目标只允许一个 pending writer，`Commit` 前必须关闭 writer 并重新校验内容；`Close`/`Abort` 清理未提交内容。 |
| transfer consume journal | `InputTransferJournal` | 先写 pending intent，再执行 Control consume，最后标记 consumed；重启按 token digest、command、destination 和已提交文件重新核对，不能仅凭 receipt 重放。 |
| assignment admission | `FileAssignmentAdmission` | admission 持有 in-process input writer slot；`Stop` 关闭执行入口但等待 resolver/后代 writer 退出后才写入 `InputDrain`，关闭期间不得重新准入。 |
| materialization | `StreamAgent` 的 `MaterializationConfig` | `MaterializationJournal` 记录 pending/committed/source-lost；输出需满足 `AttemptOwnedFilesystemScratchV1`，提交 authority 和本地 receipt 后才允许 scratch retirement。 |
| terminal scratch | `TerminalScratchRetirement` / `FilesystemScratchRetirer` | 共享 admission、Runtime、output contract；重新绑定目录并校验 inode/type/manifest/Artifact bytes，未完成 drain 或 proof 时拒绝删除。 |

关键实现位置：

- [input_resolver.go](../internal/stageworkeragent/input_resolver.go)
- [root_input_resolver.go](../internal/stageworkeragent/root_input_resolver.go)
- [input_transfer_target.go](../internal/stageworkeragent/input_transfer_target.go)
- [input_transfer_journal.go](../internal/stageworkeragent/input_transfer_journal.go)
- [assignment_admission.go](../internal/stageworkeragent/assignment_admission.go)
- [materialization.go](../internal/stageworkeragent/materialization.go)
- [terminal_materialization.go](../internal/stageworkeragent/terminal_materialization.go)

## 本轮验证

```bash
go test -race ./internal/stageworkeragent -run \
  'Test(StreamAgentStopCancelsPendingRootInput|DurableStreamStopCancelsPendingRootInput|DurableStreamPendingInputStopIdentityAndLateReturn|DurableStreamMaterialization|TerminalMaterialization|FilesystemInputTransfer)' \
  -count=1
```

结果：通过，`internal/stageworkeragent` 用时约 5.7 秒。覆盖了下载取消、late
resolver writer、durable input drain、materialization replay/terminal proof、
scratch retirement 以及 transfer target 的 exact integrity 检查。

## 尚未关闭的边界

这些测试证明的是同一进程内的 owner 协作和文件级 fail-closed 行为，仍不能证明：

1. 真实 Node/Runtime/ProductionAgent 同一次 remote-owner Job 装配中，所有 input、
   transfer、materialization journal 都由生产角色持有；
2. resolver 或其真实子进程在不同 UID、mount namespace、后代进程下无法直接打开
   input/output root 并篡改已提交内容；
3. Stop 后已经获得的外部文件描述符、fork 后代或重启实例无法重新注入内容；
4. PostgreSQL durable journal、对象存储回包丢失和进程替换同时发生时，不确定的
   transfer/materialization 写不会被重复执行；
5. root input、TransferTicket input、materialization output 与 terminal cleanup
   在同一 remote-owner Job 上完成可审计的 owner 对账。

因此第 2 项仍是未完成的架构验证项。下一步应把这些已有 owner API 接到一次真实
CPU remote-owner 装配，再注入 resolver/child-process 直接写入、Stop 后 late
write、journal 回包丢失和 replacement epoch 四类故障；不能把本报告的组件测试
写成生产隔离已经成立。
