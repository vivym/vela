# Integration full-suite timeout evidence

日期：2026-09-09  
范围：当前 `feature/vela-mock-hardening` 分支，checkpoint/repeated-compaction 改动之后。

## Full-suite run

```bash
go test -tags=integration ./internal/integration -count=1 -timeout=20m
```

结果不是通过：20 分钟后 timeout。堆栈显示运行停在
`TestStageMaterializationCannotStartAfterTerminalCancellation/SOURCE_LOST` 的
fixture database seed 写入，同时已有多个 PostgreSQL testcontainers 被顺序创建。
因此不能把这次运行记为 full integration pass，也不能直接把 timeout 归因于
checkpoint 代码。

## Isolated reproduction

```bash
go test -tags=integration ./internal/integration \
  -run '^TestStageMaterializationCannotStartAfterTerminalCancellation$' \
  -count=1 -timeout=5m -v
```

该隔离测试通过，耗时约 `6.6s`，`COMMIT` 和 `SOURCE_LOST` 两个子场景均通过。
这说明测试本身在干净进程/独立 fixture 下可完成；full-suite timeout 更像是
长时间顺序运行的 fixture/container/数据库资源累积或测试总时长问题，仍需单独
建立 suite-level 分片与资源趋势证据。

## 边界

本轮不能声明 `505/505` 全套当前通过。默认 `go test ./...`、`go vet ./...`、
`internal/stageworkeragent` race package 和上述隔离 integration test 仍是独立
通过证据；Production Gates 保持 `0/9`。
