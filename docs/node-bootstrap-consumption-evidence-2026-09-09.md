# Runtime 实际配置快照与 Node 发布记录匹配

日期：2026-09-09。基线：`cd34dbe`。范围：本地 CPU/mock，Linux/arm64
实际非 root CLI、真实 containerd/CRI 与持锁 Node journal。没有部署或 GPU
验证，Production Gates 保持 **0/9**。

## 改动与不变量

此前 Node 能检查 caller 当前视图中的发布文件，却无法将其与 Runtime 先前
解析的配置关联。本轮增加 `BackendStartupRequest` schema 2：携带 CLI
解析时的 canonical bootstrap SHA256 和实际 `--bootstrap-file` 路径。
`RemoteRuntimeServerConfig(wire, bootstrapPath)` 在解析时按值冻结摘要，实际
Node gate 使用该快照，不保留可变输入切片，也不在发送前重新读取路径。

`ReservePublishedImageRemote` 从 verified plan、当前持锁 journal 和 owner
实际公钥重新生成发布配置，要求 request 的版本、摘要、路径、incarnation
全部匹配，然后继续执行原有原始 pidfd、实际只读 mount、image/task 及 Fleet
预留前后检查。Node 私有发布记录和原始 caller 视图仍是独立检查来源。

schema 1 的 canonical 编码保持兼容，但不能用于要求消费声明的发布接口。
schema 2 不能通过未绑定 publication 的旧 reservation/record API 写入
intent；恢复读取也要求 request 与持久 bootstrap 观察的摘要和路径一致。
最终编码长度限制为 4096 字节，包含 JSON 转义膨胀；路径必须为规范绝对路径，
非根目录，最长 2048 字节。缺失、重复、未知、混合版本和非规范编码均拒绝。

这些字段对任意 caller 仍只是声明。实际 CLI 测试证明了受测实现的行为；生产
路径还须批准有效 argv/env 并确保可执行代码连续性，才能信任其消费声明。
本轮没有增加 Permit、一次性 grant 或历史转授权接口。

## 验证结果

两个 runner 的最终 source.patch 一致，且能反向应用到当前源码；文件清单、
镜像和 binary SHA256、原始命令和日志见
[机器证据清单](node-bootstrap-consumption-evidence-2026-09-09.json)。

| 验证 | 结果与实际边界 |
| --- | --- |
| 启动信封及纯读超时重试 | focused 测试重复 10 次通过；信封含 16 个反例，保持 schema 1 编码兼容 |
| 真实 UID/GID 10001 CLI | 3 个 publication 子场景通过：正常初始化/关闭、解析后配置替换、相同内容不同路径；CLI runner 全部 10 个选定主测试通过，无 skip/race |
| 实际 CRI 发布预留 | 17 个场景全部通过，包括新增错误消费摘要、错误路径、旧版本、旧 reservation API、旧 record API；新增 5 项均为 0 intent / 0 Fleet 调用 |
| 原生 ledger/startup | 11 个主测试通过，覆盖已有进程崩溃、恢复、不确定写入、退出和单次预留语义；无 skip/race |
| 仓库回归 | `go test ./...`、`go vet ./...`、golangci-lint v2.13.1 全库通过 |
| Linux 专项 | nodeagent/modelruntime/实际 CLI 三包 integration-tag lint/vet 通过；Linux/amd64 交叉编译通过，未声明 amd64 原生运行 |

配置替换反例在实际 CLI **解析配置后的第一次 journal RPC** 处暂停。Node
parent 认证该原始 Runtime caller，发布仅 `ShutdownTimeout` 不同的合法新
配置并替换目录，再经实际 `JournalEndpoint.Handle/Reply` 放行。CLI 随后的
启动请求必须仍携带原摘要；Node 对照新发布记录拒绝它。此时 backend 尚未
初始化、Runtime socket 未出现，原 journal 的 startup/highest 没有变化。
这是实际协议边界，不是产品中的测试暂停钩子。

路径别名反例给实际 CLI 一个 root-owned 只读文件，字节与原发布文件相同。
Node 要求原计划路径，因此拒绝相同内容的不同路径。正常场景仍由显式测试
Permit 放行真实 CPU process backend，观察 initialize/shutdown、实际 Worker
发现 epoch=2，以及无本地 epoch/journal。

独立 Go overlay 故意把实际 CLI 改为 gate 发送前重读文件并覆盖摘要，未改
工作区产品代码。它在 `changed-after-read` 处按预期失败，正常和路径别名
场景继续通过。对照证明上述时间边界能检测“重读当前文件冒充已解析快照”的
具体退化；不是对所有恶意 executable 的证明。

## 本轮失败与修正

第一次全库测试在已有 `TestJournalRemoteReadTimeoutCanRetry` 的成功重试处
返回 `STALE`、`requires state recovery` 和 `context deadline exceeded`。
当时该测试对每次 journal I/O 都设置 1 秒预算，重试还包含实际 fsync。
日志确认的是重试发生超时与恢复隔离，不能据此声称定位了宿主机耗时来源。

测试现在仅为故意阻塞的纯读调用设置 50ms caller deadline，正常 owner I/O
预算为 30 秒；原先“失败前零写入、零 backend 调用，之后允许正常重试”的
断言保留。产品超时/不确定写恢复语义未放宽。focused 10 次与最终全库回归
均通过。首次失败日志单独留存；它不是历史 `11ce026` STALE 失败的解释。

首轮 CLI/CRI 成功日志也单独保存：CLI 当时在收到 startup 请求后才替换
配置，之后已加强为首次 journal RPC 边界。最终结果以 `cli/`、`cri/`
子目录为准，不能用首轮日志替代最终源码证据。

## 下一步与未闭环项

实际 CLI 场景使用真实 journal/Node 比较器和 fixture Permit，没有实际 CRI
镜像/挂载预留；CRI 场景使用测试 probe 和 fixture Registry/Pod/Fleet。
两组证据不能合并声称已完成同一次启动的 CLI + mount/image + 真实 Fleet
授权，更不能声称完整受保护 Job 已通过。

下一增量应把 CLI 的有效 argv/env、代码连续性、镜像与发布挂载、真实 Fleet
预留和一次性 grant 接到同一次操作，再完成生产 Node/Fleet 入口装配。其后
依赖仍包括 Worker input/transfer/materialization 独立保管、同一装配下四
Stage Job 与 cache/Charge/cleanup、进程和后代替换、停止时限、多成员失败、
32 条历史上限后的安全回收和持续资源趋势。实施顺序继续由
[剩余验证清单](remaining-validation-2026-09-09.md) 管理。
