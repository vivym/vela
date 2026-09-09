# 持久消费与 journal 激活的同次调用

日期：2026-09-09。基线 `c07a3f9`。本轮连接同一个活 Node ledger 中的预留、独立
内存 grant、持久消费和 endpoint 激活；没有实现生产授权签发器或 backend Permit。

## 接口与信任边界

`IssueReservedJournalWriteGrant` 是 trusted Node 的签发边界：调用者必须已经完成
独立启动批准与执行连续性检查。它将具体 endpoint、原始 Runtime/Worker 句柄、
operation、证据摘要及有限期限绑定到不可序列化的 grant。摘要本身不是授权。
这种 grant 不能通过普通 `ActivateJournalWriteGrant` 激活。

`RuntimeStartupLedger.ActivateReservedJournalWriteGrant` 不接收查询得到的消费记录。
它要求活 ledger 的原始 Runtime 与 endpoint 的 Runtime 是同一个进程，并核对
计划、operation、journal ID/scope/storage、startup snapshot、incarnation 和
完整 runtime routes。随后在同一次方法调用中：

1. 将 endpoint 标为正在协调，仍只读，阻止其他已签发 grant 绕过此流程。
2. 持久消费本次机会，完成 append、fsync 与读回核对。
3. 重新检查 ledger 的 owner custody、journal snapshot、期限、上下文和两个
   原始进程，随后激活角色写入。
4. 将激活 endpoint 关联到该活 ledger；关闭 ledger 会关闭这些路由。

endpoint 锁不跨越 fsync，故存储阻塞时读取、拒绝写入和 endpoint Close 仍能推进。
锁顺序为 ledger → activation → endpoint。独立 activation guard 使 ledger Close
先封禁新激活并撤销现有路由，再等待 ledger I/O；不会因另一启动的 append/fsync
阻塞而延迟这些路由的撤销。这不承诺 endpoint 自身其他 I/O 的停止时限。
协调 claim 之后的任何错误都会清除输入 grant 的
nonce，并让该 endpoint 保持只读/关闭且不能再签发或激活其他 grant；消费记录
已落盘时仍保留。claim 前的无效参数拒绝不会被描述为已经持久消费。

历史查询没有升级为授权接口：提前单独 `ConsumeJournalGrantAttempt` 后，不能
把该记录交给新方法继续激活。重开 ledger 即使仍持有旧 grant 对象，也会因为缺少
原始 owner 或机会已消费而拒绝。恢复不重建 endpoint 或进程句柄。

## 验证内容

五组新增原生 race 测试覆盖：

- 激活前 Worker RPC 写入拒绝，fsync 后但激活前仍拒绝；正常激活后同一有效签名
  floor 命令持久生效为 `Floor=1`；ledger Close 后再次写入拒绝。
- operation、Runtime、journal、plan 不匹配，普通 grant、过期 grant、空 grant、
  提前消费记录和重开 ledger 均不能产生激活。
- append 前/后、fsync 后的错误，以及持久化期间取消、endpoint 关闭、Worker
  退出、ledger 原始 custody 丢失、journal 改写和授权过期均保持只读。
  journal 改写使用可成功执行的独立 owner 写入，以排除无效注入造成的假阳性。
- 八个并发调用只激活一次；持久化期间预先签发的普通 grant 不能绕过协调状态。
- 同 ledger 第一条路由已激活、第二条启动阻塞在 append 前时，Close 在解除阻塞
  之前撤销第一条路由；解除阻塞后第二条启动也不能激活。

真实 PostgreSQL/TLS 组合测试的正常分支也使用此入口，并通过实际非 root Worker
RPC 完成一次 floor 写入。grant issuer、Worker enrollment 和 Pod/CRI inventory
仍为 fixture；没有启动实际 backend factory，也没有发出 Permit。

首次组合运行中，正常分支的旧重试断言失败：floor 写入后 journal 不再具备首次
启动资格，因此返回 `ErrBackendStartupDenied`，比 `ErrRuntimeStartupRecorded`
检查更早。测试现按是否发生激活分别断言具体错误，仍要求真实 Fleet RPC 次数
和数据库预留行数均为一；不是将任意错误放宽成通过。

## 最终同源结果与复现

最终源码的 Linux/arm64 原生 runner 退出 `0`，ModelRuntime **59** 个、Node
**41** 个顶层 race 测试通过（包含 helper），无 SKIP、FAIL 或 DATA RACE。
新增五组激活测试全部通过；原有 grant 消费和实际 SIGKILL 恢复测试也在本次
选择范围内。SIGKILL 不构成掉电 durability 证明。

同一镜像的真实 PostgreSQL/TLS 三场景全部通过，integration package 耗时
`23.615s`（不是完整 runner 的墙钟耗时）：

| 场景 | 实际观察 |
| --- | --- |
| normal | 一次 Fleet RPC、一行 reservation；持久消费并激活；Worker RPC 写入 `Floor=1`；ledger Close 后写入拒绝 |
| committed-response-lost | 一次 Fleet RPC、一行 reservation；缺本地 receipt，消费拒绝；从 held journal 实测 `Floor=0` |
| missing-ptrace | 原始 caller 检查在预留前拒绝 |

独立 Linux Node `go vet`、golangci-lint v2.13.1（`0 issues`）通过；Darwin
Node/ModelRuntime 默认回归通过（缓存结果）。最终证据目录为
`/tmp/vela-startup-activation-closure-20260909`，镜像为
`sha256:dc10e758d33033a5a03996922eba1076cd1d34fae2ca5e85268293b23db37dd6`。
完整源码/patch/二进制/日志摘要和逐测试结果见
[机器可读证据](runtime-startup-activation-evidence-2026-09-09.json)。源码摘要
不包含文档；本报告引用最终运行，不沿用较早 `verified`/`observed` 目录的结果。

```sh
VELA_OWNER_EVIDENCE=/tmp/vela-startup-activation-repro \
  hack/run-journal-owner-native.sh
VELA_RUNTIME_STARTUP_NODE_IMAGE="$(cat /tmp/vela-startup-activation-repro/image.txt)" \
  go test -race -tags=integration ./internal/integration \
  -run '^TestRuntimeStartupNodeProcessPostgresTLS$' -count=1 -v -timeout=3m
```

## 尚未完成

这关闭的是“活内存 grant → 持久消费 → 角色激活”的事务边界。生产独立批准、
observer 可信创建/连续检查、真实 CLI/CRI 与本次 Fleet 调用的统一入口仍需装配。
普通组件构造器和 grant API 仍是独立的 trusted-Node 接口，生产入口必须限制
其调用路径，不能据本轮测试声称所有路径都强制经过 ledger。

方法返回后不自动监控 observer 或未来请求的丢回包。运行服务的取消、observer
失联及不确定 Permit 回包仍需由上层关闭 endpoint；不能把本方法中的上下文检查
当作持续 watchdog。消费记录也不证明 backend 已启动或已停止。Production Gates
保持 **0/9**。
