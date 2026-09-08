# 启动阶段 journal 只读通道

日期：2026-09-09。基线：`8f4db39`。范围：本地 Linux/arm64 CPU/mock、
真实非 root 进程与 CLI、containerd 2.3.1 / runc 1.4.2；无部署、GPU 或
Production Gate 放行。Production Gates 仍为 **0/9**。

## 问题与改动

Runtime 必须先读取 Node journal 才能组装启动请求。此前实际 CLI 测试在
这个阶段使用了 `NewJournalEndpoint`，而该可写构造器的前置条件是 Node
已经独立批准两个原始进程的相应写入角色。单纯登记 pidfd/CRI 身份不足以
满足这个前置条件；也不能因为需要提供启动快照，就提前开放 journal 写入。

新增 `NewReadOnlyJournalEndpoint`，复用原有 root custody、原始 pidfd、
不同 Runtime/Worker 角色和请求验证，固定其生命周期内的只读权限：

- 原始 Runtime 和 Worker 可以读取 journal 快照；相同 UID 的其他进程不能
  借此获得读取角色。
- typed command 解析后，任何非 Read 操作都在 `ExecutionJournalOwner.Apply`
  前返回与请求摘要绑定的 `REJECTED`，不返回 mutation receipt。包括 Worker
  的 restrictive floor，也不设例外。
- 没有模式 setter、自动升级、receipt→write 或 caller Permit→write 路径。
  关闭 endpoint 撤销两个通道，但不关闭 Node 自己持有的 journal owner。
- Node 的独立受信任 owner 操作不受这个 RPC 只读限制。grant 后的写路由需要
  独立授权和显式装配，本轮没有提供或伪造该授权转换。

实际远程 CLI 测试、Node publication 的实际 CLI 测试，以及实际 CLI/CRI
同次预留测试均改用这个只读构造器。已有 `NewJournalEndpoint` 保留已授权
阶段的可写契约；它不是启动阶段的默认放行或独立 grant 验证器。

## 验证结果

| 层次 | 实测结果 |
| --- | --- |
| 原生只读 RPC | Runtime、Worker 两个角色分别通过；每个角色覆盖八种 mutation selector 拒绝，原始角色可读、同 UID sibling 不可读、拒绝后仍可读、关闭后不可读 |
| 状态未变 | 拒绝前后的完整 journal Document 与 LockDocument 字节相同；没有 mutation receipt，没有让 owner 进入 recovery/poison 状态 |
| 有效写命令对照 | 同一份正确签名 admission/floor 在只读 RPC 中两次被拒绝；endpoint 关闭后，独立 trusted owner Apply 对相同字节成功，Highest 或 Floor 实际变为 1 |
| 实际 CLI/CRI | 11 个场景全部通过，新增 pregrant-floor；正常路径仍读取快照、检查真实 image/task/只读 mount、单次预留，并由显式测试 Permit 完成 backend initialize/shutdown |
| 原生 CLI/发布 runner | 11 个选定主测试全部通过，包括实际 CLI 文件/消费反例、发布并发与进程崩溃，以及原有独立远程 Worker 执行组件 |
| 已授权可写路径 | 原有 6 个 endpoint/server 主测试通过：正确角色写入、丢回包重放、退出与关闭、过载恢复、超时和 listener 生命周期 |
| 仓库与平台检查 | 全库 test/vet/lint、Linux integration-tag nodeagent lint/vet、Linux/amd64 交叉编译通过；未声称 amd64 原生运行 |

八种 selector 是 Admit、Candidates、Seal、Drain、Health、Floor、NonAdmission
和 TerminalNonAdmission。这组测试证明它们都被只读入口拦截；其中独立验证
业务上可成功的反例为 admission 和 floor，没有把其余命令的结构合法性
宣称为全部业务前置状态已经成立。

`pregrant-floor` 使用真实 CLI/CRI 与真实 Node journal server。已登记 Worker
发送正确签名的 floor，第一次在 startup reservation 前，第二次在显式测试
Permit 已初始化 backend 后；两次均为 `REJECTED`，owner status 完全不变。
该 case 正常保留 `1 intent / 1 Fleet fixture call`。server 停止后，独立
Node owner 对原 floor 字节执行 Apply 成功。这也证明测试 Permit 不会让
只读 endpoint 静默变为可写。

所有正向原生 campaign 无 skip/race。完整原始日志、两个 runner 的源码补丁、
binary/image 摘要与检查命令见
[机器证据清单](journal-readonly-startup-evidence-2026-09-09.json)。

## 独立退化对照

Go overlay 仅关闭 `endpoint.readOnly` 的 mutation 拒绝分支，不更改工作区
源码、请求签名、原始角色认证或 owner 状态机。它使 Runtime admission、
Worker floor 和实际 CLI/CRI 的 pregrant floor 被错误写入，三个测试均按
预期失败；正常实际 CLI case 继续通过。对应正式源码全部通过。

这是具体 RPC 写权限边界的对照证据，不是生产 grant、完整 Job 或所有
恶意 Runtime 行为已经被排除的证明。

## 尚需完成

只读构造器和上述测试装配已经闭合；生产 Node daemon 尚未装配这个启动流程。
真实 Registry/Fleet/TLS/PostgreSQL、生产 Pod→CRI 环境与挂载仍需接入。
可写构造器要求独立 authority，当前没有不可伪造的 once-only grant 对象或
grant 后路由转换；不能用手工选择可写构造器来替代该验证。

下一步仍需绑定执行连续性与 once-only grant，并明确失败、Node 重启、旧
pidfd 丢失和回包丢失时的权限状态。grant 后写通道应由这一状态转换建立；
现有只读 endpoint 本身不会升级，所以还不能承载完整 Job 的 Prepare/Seal/
Drain 写入。

Worker input/transfer/materialization journal 独立保管、同一装配的四 Stage
Job、exact-cache/transfer/一次 Charge/cleanup、真实后代停止、多成员失败、
历史安全回收及持续到达验证继续按
[剩余验证顺序](remaining-validation-2026-09-09.md) 推进。
