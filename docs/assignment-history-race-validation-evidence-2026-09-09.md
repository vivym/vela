# Assignment history race validation evidence

日期：2026-09-09  
分支：`feature/vela-mock-hardening`  
范围：CPU/local `stageworkeragent` assignment history 与 terminal recovery 定向验证。

## 执行命令

```bash
go test -race ./internal/stageworkeragent \
  -run 'AssignmentHistory|History' -count=1 -v
```

## 结果

命令通过，退出码为 `0`，耗时约 `10.4s`。本轮实际覆盖：

- `AssignmentHistoryCutoff` canonical JSON、digest chain、初始 sequence 与 gap/proof 拒绝；
- 损坏或不完整 admission state、scope/history 重验证；
- cutoff reclaim 的持久 `HistoryBase`、重启恢复和继续接纳；
- 40 条 bounded arrival campaign，逐条 drain、关闭、记录 cutoff、回收；
- 完整 history preflight，确保 terminal drain、execution floor、exclusion 和 recovery 在 RPC 前拒绝不可信 history；
- 未知 input/runtime history 在 retirement 时保留并施加 backpressure；
- `-race` 下的 terminal recovery、lost proof 和不可用 session 场景。
- 两个连续 cutoff 的目录同步故障注入；第二次回收在重开后允许且区分两种
  正确结果：rename 已落盘则恢复到新的 `HistoryBase`，否则保留旧前缀并可安全重试。
- 最终持久文件的独立 JSON 对账：`HistoryBase`、两条 cutoff、第二条的
  `PreviousCutoffDigest` 与内存中期望 proof 逐项匹配。
- 40 条连续回收的 state 文件从 `540` 增长到 `41,398` bytes（约 `40,858`
  bytes 增量），说明当前实现保留完整 cutoff proof chain，增长主要来自持久
  proof 历史，而不是 goroutine/heap 泄漏。

campaign 日志报告 goroutine `2 -> 2`，heap allocation 增量约 `118,568` bytes。该值只支持本次有限 campaign 没有观察到明显泄漏；它不构成 open-loop 长期吞吐、journal/history 空间上界、掉电 durability 或真实多进程生产证明。

## 边界

本证据不关闭 cutoff proof chain 的长期空间上界；当前测量暴露了需要 checkpoint/
压缩协议的架构缺口。它也不关闭真实 Node/Fleet/CRI 装配、power-loss durability
或 GPU 验证。Production Gates 仍为 `0/9`。
