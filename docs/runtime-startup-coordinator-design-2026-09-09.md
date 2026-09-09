# Node 统一启动协调入口：授权条件与恢复规则

日期：2026-09-09。基线 `7b91188`。本轮实施的是统一入口的持久 grant 消费边界，
不是完整启动授权器。没有新增 backend Permit、生产路由或部署。

## 入口的职责与授权来源

统一入口最终应当拥有一次启动的全过程，而不是由调用者拼接可重放回执。
只有以下条件在同次调用成立，才能进入正向激活：

1. Registry 签名的计划与当前 Fleet 批准匹配，绑定 Node、Worker member、runtime
   incarnation、journal pair、scope、配置与完整 epoch 向量。
2. Node 持有实际 Runtime/Worker journal 和独立原始进程句柄；只读 endpoint
   绑定的 journal/进程必须就是本次检查的对象，不能接受调用者提供的数字 PID。
3. 实际 CLI 消费的配置、有效 argv/env、镜像入口与只读 mount 通过当前检查。
   这些采样必须与可信创建的 observer 连续性约束结合；采样本身不能证明未 exec。
4. 同一次调用完成 Fresh Fleet 预留，并在返回后重新验证以上事实。
   预留回执只证明预留，没有 backend Permit 的含义。
5. 独立启动授权策略批准此次操作。策略实现仍需落实；非零摘要、回调成功、旧
   reservation 或测试 fixture 均不能替代生产授权来源。
6. 激活机会已经持久消费，且调用仍持有原始进程与同次检查状态，才允许单次
   journal grant 激活。Permit 回包必须排在其后。

本轮没有用宽泛的 `Authorize() == nil` 回调伪装这些条件已经成立。

## 状态与恢复

| 状态 | 持久内容 | 允许行为 | Node 重开后的行为 |
| --- | --- | --- | --- |
| 启动前只读 | 原有 journal | 认证读取，拒绝写入 | 不从记录重建原始句柄 |
| Startup intent | 原始进程观察、request、journal、epoch 等 | 当前活调用最多发起一次 Fleet 预留 | 已存在 intent 阻止再次预留 |
| Reserved | 精确 operation/request 的本地 reservation 回执 | 完成独立启动授权与最后检查 | 只能读取历史，不能据此激活 |
| Grant attempt consumed | startup/reservation/授权证据摘要及记录时间 | 仅原活调用未来可能继续激活 | 只查询；相同或不同摘要都不能重新消费 |
| Grant activated（未来统一入口） | 消费记录仍在；激活状态在内存 | 按角色写 journal，再回复 caller | 不推断旧 backend 是否已启动，不重发许可 |

Grant 消费记录不能区分“尚未激活”和“已经激活但回包丢失”。这正是保守恢复
必须保留的不确定性。生产恢复需先取得旧进程及后代的可靠停止/隔离证据，再走
独立的新 incarnation 协议；本轮未实现该替换协议。

## 本轮实现

`RuntimeStartupLedger.ConsumeJournalGrantAttempt` 是限制性入口：

- 在现有 root-only、持锁的 append ledger 中增加 `grant_attempt` 条目。
- 从 ledger 派生完整 startup/reservation 摘要，绑定明确的 `OperationID` 和
  `JournalID`；调用者不能替换这两个历史对象。
- `AuthorizationDigest` 只用于记录独立证据的身份。本方法只检查非零，不验证
  证据真实性，因此返回值永远不是许可；没有 grant nonce、endpoint 或 Permit。
- 需要本 ledger 当前持有的原始 Runtime owner 存活，拒绝无 reservation、已 exit、
  重复消费及取消请求。没有记录也不代表重启后可以消费：重开不会重建 owner。
- 沿用 append → fsync → 全文件/摘要核对；不确定 append 使当前 handle 失效。
  调用失败时返回空记录。记录已落盘时恢复为已消费，未落盘时恢复仍因无 owner 拒绝。
- 每次 append 仍保留原有 exit 记录空间；空间不足时拒绝消费，不挤占退出记录预算。
- 新建 ledger 为 schema 3；schema 1/2 仍可按原语义读取，不能写 grant 条目，
  不静默升级。旧程序拒绝 schema 3，不能直接回退到旧二进制继续写入。

`InspectJournalGrantAttempt` 仅返回历史。现有 `IssueJournalWriteGrant` 仍是独立
的 trusted-Node API；本轮尚未让它强制消费这个记录，也没有将消费方法接入生产
启动处理器。因此这是一条已实现的前置边界，不能宣称统一协调入口已经闭合。

## 验证边界与后续实施

当前运行通过：ModelRuntime 59 个、Node 36 个顶层原生 race 测试（包含 helper），
所选测试无 SKIP。其中新增六组 grant 消费测试覆盖正常/重复/并发、错误绑定、
缺失证据、取消、失效 owner、旧 schema、损坏/乱序记录及不确定 append 恢复。
原有进程崩溃 runner 新增 `grant-before-append`、`grant-after-append`、
`grant-after-sync` 三个外层实际 SIGKILL 场景，恢复均不重新消费。

同一源码 race image 的 PostgreSQL/TLS 三个场景也通过，integration package
耗时 `22.731s`：正常场景记录消费，提交后丢回包场景虽然数据库有记录、只读
history RPC 成功，仍因缺少本地 reservation 回执而拒绝消费；缺少 ptrace 权限
在预留前拒绝。两侧分别检查 operation、journal、完整 startup 摘要和 fixture
证据摘要。没有实际激活 journal 或发送 Permit。

```sh
VELA_OWNER_EVIDENCE=/tmp/vela-startup-grant-repro hack/run-journal-owner-native.sh
VELA_RUNTIME_STARTUP_NODE_IMAGE="$(cat /tmp/vela-startup-grant-repro/image.txt)" \
  go test -race -tags=integration ./internal/integration \
  -run '^TestRuntimeStartupNodeProcessPostgresTLS$' -count=1 -v -timeout=3m
```

Linux/arm64 Node `go vet`、golangci-lint v2.13.1（`0 issues`）、Darwin Node/
ModelRuntime 默认回归、integration 编译检查、shell 语法与 diff 检查均通过。
源码/镜像/二进制摘要、测试清单和原始日志位置见
[机器可读记录](runtime-startup-grant-consumption-evidence-2026-09-09.json)。

本轮验证覆盖真实 Linux root Node、非 root 原始 PID-1、pidfd、文件锁与 fsync，
并复用真实 PostgreSQL/TLS 预留测试。批准证据摘要、Pod/CRI inventory 仍为 fixture；
记录 fixture 摘要只验证拒绝/消费逻辑，不是发放测试后门 Permit。

下一增量需实现同次调用的协调对象，将具体 endpoint/Worker owner、可信 observer
创建与检查、实际 CLI/CRI 批准、Fresh reservation 及独立授权策略绑定，消费后
立即复核并激活。丢回包、observer 失联和取消时必须撤销角色路由，不得保持无限期
可写；不得跨网络检查持有阻塞 watchdog 的共享锁。这个对象不能从历史或 JSON
构造，生产 CLI 只能通过该入口获得许可。随后才接完整 remote-owner CPU Job。

进程 SIGKILL 不等于断电测试；摘要校验不构成恶意 root 攻击下的真实性证明；当前
任何结果都不提升 Production Gates，仍为 **0/9**。
