# Journal grant transition

更新：2026-09-09，基线 `535ea5d` 加本轮 grant 验证增量。Production Gates 保持 **0/9**。

## 修复与证据校正

`JournalWriteGrant` 是绑定具体 endpoint 的内存 capability，不可序列化，不可从
历史回执恢复。激活后 endpoint 只允许一次转换，并继续按原始进程区分 Runtime
和 Worker 写入角色；多个并发激活最多一个成功。

此前 `TestJournalReadOnlyGrantTransition` 直接构造内部 grant，再直接改写
`readOnly`/`grantUsed`，没有调用公开的 `IssueJournalWriteGrant`，也没有验证
实际 journal 写入。旧报告关于过期、取消和失效 pidfd 的描述是代码预期，不能
当作这些反例已经执行的证据。原生 runner 当时也未选择该测试。

本轮用真实 Linux 原始进程与保留的 pidfd 通过公开 API 签发 grant，并将测试加入
`hack/run-journal-owner-native.sh`。激活对已关闭 endpoint 显式拒绝，期限比较采用
严格的 `now < expires`；修正了只读构造器注释中与 grant 转换矛盾的“immutable”表述。

## 新增验证

- 激活前，Runtime admit 和 Worker floor 均拒绝；仅签发 grant 仍不能写入。
- 激活后，两类有效签名命令通过真实进程 RPC 持久写入，journal 的 `Highest=1`、
  `Floor=1`；错误角色与同 UID sibling 仍拒绝。
- 签发后关闭 enrollment observations，endpoint 保留的独立原始句柄仍可用于激活。
- 签发拒绝取消/缺失 context、缺失 owner、角色交换、同 UID sibling、零及超限期限；
  拒绝后正确签发仍可完成。
- 激活拒绝过期、同 peers/journal 的另一个 endpoint、取消/缺失 context、endpoint
  关闭、Runtime 或 Worker 已实际退出；失败不改变只读状态。
- 同一 grant 的八个并发激活只有一个成功；已激活 endpoint 拒绝再次签发和激活。

## 当前运行结果与复现

最终 Linux/arm64 静态 race 镜像运行通过：ModelRuntime 59 个、Node 30 个顶层
测试（包含 helper），所选测试无 SKIP、无 race 报告。上述四个 grant 主测试全部
实际执行通过。源码摘要、image ID、二进制 hash、测试名/耗时与原始日志位置见
[机器可读记录](journal-grant-native-evidence-2026-09-09.json)。

首次扩展 runner 时，原有 `TestJournalServer.*` 还选中了
`TestJournalServerActualRemoteCLI` 和 `TestJournalServerExecObservedRemoteCLI`，
因镜像不含 CLI/observer 而跳过，runner 以非零状态退出。本轮将 server 选择器
限定为当前镜像支持的八项，保留任何意外 SKIP 都失败的检查。那两项的专用入口
仍是 `hack/run-remote-runtime-cli-native.sh`，本轮不声称重新验证了它们。

```sh
VELA_OWNER_EVIDENCE=/tmp/vela-grant-native-repro hack/run-journal-owner-native.sh
VELA_RUNTIME_STARTUP_NODE_IMAGE="$(cat /tmp/vela-grant-native-repro/image.txt)" \
  go test -race -tags=integration ./internal/integration \
  -run '^TestRuntimeStartupNodeProcessPostgresTLS$' -count=1 -v -timeout=3m
```

最终镜像的 integration package 耗时 `17.003s`，三个子场景均 PASS。宿主与原生
Node helper 均开启 race；PostgreSQL 本身不在 Go race 检测范围内。
`go test ./internal/nodeagent ./internal/modelruntime`、Linux/arm64 Node `go vet`、
Node/runtimechannel 的 golangci-lint v2.13.1（`0 issues`）、shell 语法检查与
`git diff --check` 均通过。Darwin 默认测试不覆盖 Linux-only 文件，不能替代原生结果。

## 与真实 Fleet 的关系

本轮也显式执行 `TestRuntimeStartupNodeProcessPostgresTLS` 的正常预留、数据库
提交后丢回包、缺少 ptrace 权限三个场景。测试使用当前源码构建的 Linux image，
临时 PostgreSQL 17、schema 95、真实 Fleet/mTLS 和实际 root-held Runtime/Worker
journal；不依赖默认 integration 运行时未提供 image 导致的 skip。

这条预留链和 grant 转换仍是两套独立验证。Pod/CRI inventory 与 startup approval
仍有 fixture；Fleet reservation 不是 backend Permit，也不能直接变成可写授权。
生产 Node 必须把有效启动批准、执行连续性、同次 Fleet 预留、一次性 grant 与
角色路由连接起来，且在任何不确定结果下保持只读。持久 grant 恢复、真实 CLI/CRI
同链与完整 remote-owner Job 仍未闭合。本轮没有添加依据 reservation receipt
自动授权的路径。
