# Integration runner 证据审计

范围：本地 CPU/mock 验证入口；不部署、不使用 GPU，Production Gates 仍为 0/9。
基线：`502279a`。本轮修改验证脚本和报告，业务执行代码未变。

## 已复现与修复

`go test ./hack -run '^TestLocalIntegrationShardEvidence$' -count=1` 在修复前失败。
用 PATH 中的假 Go 驱动真实 `run-integration-shards.sh`，观察到以下结果：

- 发现命令输出部分测试名称后非零退出，runner 仍运行分片并返回成功。
- 执行只返回一个测试结果，另一个缺失，runner 仍返回成功。
- 同一个测试有重复结果，runner 仍返回成功。
- SKIP 与 PASS 都只显示 `shard passed`，成功运行的日志随后被删除。

根因：`mapfile < <(go test ... | awk ...)` 未传播 process substitution 的失败；
执行时只检查 `go test` exit code，没有与发现清单核对，并且成功分支删除日志。

修复后发现命令须成功才分配测试，且每个分片核对顶层 `--- PASS/SKIP/FAIL:`
结果。嵌套 subtest 不混入顶层计数；缺失、重复、额外结果和任何非零 Go 退出均
拒绝成功。逐测试 TSV、发现输出、source revision/status/diff 与原始日志在私有
目录保留。SKIP 允许普通 integration 运行成功退出，但单独列出，不能视为 PASS。
本地入口只覆盖 `internal/integration`，CI 的另一入口还会发现其他 tagged 包。

回归覆盖正常结果、SKIP 保留、缺失、重复、额外测试、Go 非零退出及发现失败。
七个场景通过；`go test ./hack -count=1`、golangci-lint v2.13.1 对 `./hack`
检查、shell syntax 与 diff whitespace 检查通过。

## Campaign 开关核对

显式清空 `VELA_RUN_CPU_MOCK_CAMPAIGN` 后，用 `-v` 重跑此前四条 campaign，
结果为四项 SKIP、package PASS。这重现了“绿色包结果不证明 campaign 执行”。
此前 0.862s 的命令没有显式启用开关，不能作为已执行 campaign 的证据。

本轮显式使用 `VELA_RUN_CPU_MOCK_CAMPAIGN=1`、`-race -v -count=1 -timeout=5m`，
按精确名称重新执行四项，全部实际 PASS，包总耗时 77.088s，无 race 报告：

| 测试 | 结果 | 测试报告耗时 |
| --- | --- | --- |
| TestCPUMockDurableStreamExactCacheCampaign | PASS | 13.40s |
| TestCPUMockExactCacheSourceTargetCampaign | PASS | 8.14s |
| TestCPUMockExactCacheProductionLoopCampaign | PASS | 11.88s |
| TestCPUMockProductionLoopClockOffsetCampaign | PASS | 38.99s |

原始日志：`/tmp/vela-four-campaign-audit.log`。耗时受同期分片运行影响，不能当作
独立性能基准。这四项通过仍不代表受保护 Node/CRI/Fleet 的完整 Job 装配已成立。

## 单进程 deadline 的解释修正

最近一次全量命令使用 `-timeout=15m`。超时栈显示当前测试只运行了 `0s`，日志
显示前一容器刚完成 migration 和 cleanup。栈中的 `ContainerStart` 是全包计时器
触发时的位置，不能单凭这一帧证明该调用已长时间卡死。可以确认套件未完成；
不能确认 Docker 死锁、磁盘阻塞或业务 assertion failure。后续需要完整逐测试
耗时和错误记录，以区分总时限不足、稳定慢测试与基础设施异常，不能再由瞬时栈
推断根因。本轮没有重复这一 15 分钟无完整日志的运行。

## 当前完整分片结果

`VELA_INTEGRATION_CONCURRENCY=4 ./hack/run-integration-shards.sh 20` 正常退出；
20 组结果与发现清单逐项核对，505 个名称一一匹配，无重复、缺失或额外结果：

| 顶层结果 | 数量 |
| --- | ---: |
| PASS | 496 |
| SKIP | 9 |
| FAIL | 0 |
| MISSING | 0 |

九个 SKIP 包含七条 opt-in CPU campaigns，以及
`TestProtectedProvisioningCommandPostgres`、`TestRuntimeStartupNodeProcessPostgresTLS`。
四条 CPU campaigns 已在上面的显式开关运行中另行 PASS。这里不能宣称“505 全部
实际通过”，也不能宣称其他 integration-tag 包均已验证。

机器可读摘要与原始文件 SHA256：
[validation-runner-audit-2026-09-09.json](validation-runner-audit-2026-09-09.json)。
原始日志、逐测试 TSV 与发现列表保留于摘要中的私有 evidence directory。
本轮不是 GPU 验证，也没有提升 Production Gates 或真实启动装配的证据等级。

## 全部 CI 分片审计（未闭合）

按 CI 入口以并发 4 运行 20 个分片时，19 个分片完成，`shard 19` 的
`TestConcurrentCredentialIssueAndServicePrincipalDisableLeaveNoActiveCredential`
在 PostgreSQL container ready 后，Testcontainers 的 Docker `inspect` mapped-port
请求以 `context deadline exceeded` 失败（约 54 秒）；没有业务断言失败。分片摘要
为 `FAIL=1`，所以这次整体不能记为通过。原始目录为
`/tmp/vela-ci-shards-audit.4o5ic3`。

随后单独重跑该精确测试：

```text
go test -v -tags=integration ./internal/integration -run '^TestConcurrentCredentialIssueAndServicePrincipalDisableLeaveNoActiveCredential$' -count=1 -timeout=5m
```

结果通过，约 3.766s。该对照支持“并发 Docker 宿主压力下的 Testcontainers
inspect 超时”解释，但不证明整体 CI 分片已通过；仍需在稳定宿主上重新完成全部
20 个 CI 分片。

## CI 入口回归

同一终态校验已接入 `hack/test-integration-shard.sh`，它覆盖 CI 发现的多个
tagged 包。真实执行：

```text
bash hack/test-integration-shard.sh 0 20
```

结果通过；包级摘要为：`cmd/vela-lab-bootstrap` 1 PASS、
`internal/fleetcontroller` 1 PASS、`internal/integration` 24 PASS/1 SKIP、
`internal/modelruntime` 14 PASS、`internal/nodeagent` 2 PASS、
`internal/workerbootstrap` 1 PASS，所有包 `FAIL=0 MISSING=0`。原始输出保留于
`/tmp/vela-ci-shard0-audit.log`。该单分片不能替代 CI 的其余分片或生产环境。
