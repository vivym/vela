# 受保护 Runtime 与实际 Node journal pair 的 Fleet 验证

基线：`8032afe`。本次验证前轮 Linux dumpability 保护与 Node 身份检查的
兼容性，同时补齐此前 Node/Fleet 同链测试中 Worker journal 来源的缺口。
范围仍是 CPU 组件装配，不发放 backend grant、不运行 GPU 或完整 Job。

## 实际 journal pair

root Node 通过真实 mTLS 取得 PostgreSQL 中的首次 bootstrap claim 后，
使用 `ExecutionJournalOwner` 创建并锁定 Runtime journal；再由
`workerjournal.AssignmentConfig` 推导同一批准 manifest 的 Worker topology，
调用 `WithPreparedAssignmentJournal` 创建 Worker journal。两者均位于容器内
root 创建的 0700 目录；Worker 的 input/output roots 也在容器内创建。

Worker journal 的初始化显式指定 `MaxRecords=32`，延迟 Runtime routes，
不准入任何 assignment。其 callback 持锁覆盖 Registry receipt、签名 binding
读取、Runtime caller 认证、预留与 ledger 重开检查。callback 内第二个
`PrepareAssignmentJournal` 无法取得该 journal，验证排他锁仍持有；退出时
既有实现再次校验文件与目录状态。Runtime owner 同样保留至本次操作结束。

Registry pair 使用这两份实际 journal 的 ID/scope。host 再经 TLS 读取
bootstrap history，与 native report 中 Worker journal 的 ID/scope 精确
匹配，并要求 schema 5 与有效 storage identity。此前虚构 Worker UUID/scope
已从这条测试路径移除。此处证明的是**初始化与持锁来源**，尚未实现 Node
代 Worker 执行输入/materialization journal 的完整业务 API。

## 受保护原始 Runtime

独立非 root PID-1 caller 在发送 challenge-bound 请求前执行并确认
`PR_SET_DUMPABLE=0`。它复用了实际内核属性，未伪造 `/proc` 内容；实际 CLI
保护入口及同 UID 访问对照由前轮 [Runtime process protection](runtime-process-protection-evidence-2026-09-09.md)
独立验证。本轮 caller 仍为测试 helper，不声称已运行 production Runtime CLI。

| 场景 | Node SYS_PTRACE | 预留 RPC | 数据库预留 | Node 回执 |
| --- | --- | --- | --- | --- |
| 正常 | 有 | 1 | 1 | 有 |
| handler 提交后丢回包 | 有 | 1 | 1 | 无，保留 intent |
| 缺少检查权限 | 显式 drop | 0 | 0 | 无，原始 caller 认证拒绝 |

正常与丢回包场景继续匹配完整 Node record 的 SHA256、Fleet owner 摘要、
原始请求和只读数据库历史；live 重复调用、关闭重开后调用都不产生新预留，
也不能从历史重建原 pidfd。连接确认使用 TLS 1.3。权限缺失场景仍可能已
记录首次 bootstrap claim/pair；零 Runtime reservation 不意味着可以静默
重新初始化或自动归还 Worker 容量。

Docker 测试容器只在正向场景增加 `SYS_PTRACE`，负向场景显式删除它。
没有修改真实部署权限；未来 Node 部署需独立声明并验证此检查能力，不能
靠放宽 Runtime dumpability 使身份检查通过。

## 验证、复现与边界

完整 native race runner 与三种 PostgreSQL/TLS 情景均通过。普通 tests/vet、
普通/Linux lint 与 Linux compile-only 通过；integration-tag lint 仍有 82
项既有诊断，本次文件无新增诊断。原始命令终态、日志、source/image/binary
摘要见[receipt](node-protected-pair-evidence-2026-09-09.json)。

复现：先运行 `hack/run-journal-owner-native.sh`，再将其 `image.txt` 精确
image ID 作为 `VELA_RUNTIME_STARTUP_NODE_IMAGE`，运行：

```sh
go test -race -tags=integration ./internal/integration \
  -run '^TestRuntimeStartupNodeProcessPostgresTLS$' -count=1 -v -timeout=3m
```

Pod/CRI 仍为 fixture，未运行 Kubernetes/containerd workload；input/output
roots 没有发生真实 Worker 下载或 materialization。没有新增 effective OCI
批准、一次性 Node grant、生产挂载/命令或后代 containment。完整 remote-owner
CPU Job、cache/transfer/Charge/cleanup、替换和持续运行仍未闭环。
Production Gates 保持 **0/9**。
