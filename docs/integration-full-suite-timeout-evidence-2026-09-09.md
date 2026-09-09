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

## Two-shard acceleration experiment

为验证执行层面的加速，而不改变测试的数据库隔离边界，使用
`hack/run-integration-shards.sh 2` 将 `go test -list '^Test'` 发现的 505 个测试
按 round-robin 分到两个独立进程；每个进程自行创建 PostgreSQL testcontainers。
实跑约 `942.876s`（约 15.7 分钟）后，shard 1 通过，shard 2 失败，故这次实验
不能算完整 integration pass，也不能只凭失败结果断言业务代码错误。它证明了
分片可以在相同隔离模型下并发推进，但当前 Docker/fixture 成本仍然很高，且至少
有一组分片需要单独收敛失败原因。

脚本随后补充了两个执行保护：请求的 shard 数超过发现的测试数时自动收敛，避免
空 shard 的正则意外匹配全部测试；失败时保留 shard 日志目录，便于定位具体测试
和资源问题。推荐先用 `VELA_INTEGRATION_SHARDS_DRY_RUN=1` 检查分配，再运行 2
shards；4 shards 需要重新观察 Docker 资源压力。

## Re-run after runner hardening

在 runner 增加失败日志保留和空 shard 收敛后，用相同的 2-shard 分配再次执行：

```bash
VELA_INTEGRATION_TIMEOUT=20m hack/run-integration-shards.sh 2
```

结果为 `shard 1 passed`、`shard 2 passed`。这证明 505 个 integration tests
可以在两个相互隔离的 PostgreSQL fixture 进程中完成；它仍不改变单进程全套运行
曾经超过 20 分钟的事实，也不把两进程分片结果提升为生产 readiness 或
Production Gates 证据。若再次出现单 shard 失败，应优先读取 runner 保留的日志，
区分具体测试失败、Docker 资源压力和 fixture 启动失败。

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
