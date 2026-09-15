# Runtime startup validation closure audit (2026-09-13)

当前目标是完成 validation 阶段的可重复 Node composition 证据，不是发布生产版本。
`Production Gates` 保持 `0/9`。

## 已经闭合的代码边界

- Linux 6.8 没有 `pidfs` 时，Node 通过 root-owned `pidfd` broker 交接原始 descriptor；代码不从 numeric PID 重开 pidfd。
- production launcher 的四 descriptor handoff、observer custody、UID/GID、ptrace、签名 Pod、digest、资源和安全字段校验已经有 focused/native 测试。
- 启动失败回执支持 reservation 前失败；回执使用 `O_EXCL`、`fsync`，并校验 phase 与 authority 顺序。
- CRI cleanup 只有在精确 target 的 container 查询为空，且同一 sandbox 明确返回 `codes.NotFound` 时才会标记为 verified；容器查询的明确 `NotFound` 也视为 absent，其他错误均失败。
- 已创建 grant 的 failure receipt 必须包含 reservation、authorization 和 grant attempt digest；这避免把不完整的失败记录误当成 authority 证据。
- backend 请求到达前的 caller/observer 失败会从同一 Node ledger 恢复 reservation digest，仍按严格 receipt 规则落盘，不把 `pending` 误记为 Permit。

验证命令：

```text
go test ./...
make lint
GOOS=linux GOARCH=amd64 go vet ./cmd/vela-node-agent ./internal/nodeagent
git diff --check
```

## 仍未闭合的 validation 工作

本轮已修复 validation fixture 对 `/run` 的硬编码、过长 Unix socket 路径，以及把
Worker journal 的“grant 前拒绝”检查放在 Permit 之后的问题。修复后的
`TestRuntimeStartupCompositionDriver` 和 `TestRuntimeNamespaceOwnerFromPIDFDRejectsWrongCredentials`
已在 `marslab` 的 source-matched Linux amd64 容器中通过；这只改变 validation fixture
的可重复性和凭据回归覆盖，不改变下面列出的 command-level 和生产输入边界。

另外修正了 command composition 的 Pod 身份绑定：签名 plan 中的 Pod 是模板，通常没有
`metadata.uid`；Node 现在只从 plan 绑定 namespace/name/container，要求 launcher 提供非空
live Pod UID，并交由后续 CRI/Kubernetes observation 校验。此前的模板 UID 硬比较会把正常
Pod 创建全部错误拒绝。

本轮新增的共享 internal composition driver 已闭合 validation fixture 层：它在同一套
driver 中执行成功交换、Worker journal mutation、receipt 独立回放、重复启动拒绝，以及
policy response loss、caller replacement、observer loss。`marslab` 回归证据见
[`runtime-startup-composition-driver-2026-09-13`](evidence/runtime-startup-composition-driver-2026-09-13/README.md)。

本轮已新增并在 `marslab` Linux amd64 root 环境通过 internal composition v2：成功路径
包含 fake backend Permit 和 receipt replay；Node restart、policy response loss、caller
replacement、observer loss 也在同一测试组执行。回放摘要来自独立 backend request 与
durable reservation JSON。该证据见
[`runtime-startup-composition-2026-09-13-v2`](evidence/runtime-startup-composition-2026-09-13-v2/README.md)。

现有 `TestRuntimeStartupCommandCompositionHarness` 仍是显式 opt-in、缺输入即 skip 的入口。
它尚未自动生成并销毁一套完整 fixture，也没有在同一命令进程中把以下对象串成一个
`operation_id`：

本轮已把 `loadRuntimeStartupResources` 拆为生产默认 wrapper 和
`loadRuntimeStartupResourcesWithFactory`。这解决了验证夹具无法替换外部 Kubernetes/CRI/
Fleet 构造器的代码耦合，但不改变上述结论：成功路径和故障矩阵仍未生成完整 receipt。

```text
signed plan -> CRI Runtime/Worker -> observer custody -> Fleet reservation
-> Node journal -> Worker journal mutation -> startup grant -> CPU ModelRuntime Permit
```

因此仍需要 command-level validation harness，至少完成：

1. 通过 `loadRuntimeStartupResourcesWithFactory` 装配临时 Ed25519 plan、Fleet authorization key、issuer reply key 和 root-owned 目录；
2. 让 fake Fleet、Node/Worker journal、policy issuer、CPU ModelRuntime backend 接入同一命令边界；
3. 真实调用 `loadRuntimeStartupResources` 与 `composeRuntimeStartupAuthority`；launcher double 只交接真实 pidfd/socketpair，不直接制造 grant/Permit；
4. 成功路径写出可回放、可 `VerifyBinding` 的 composition receipt；
5. 将 helper timeout/crash、Node restart、broker restart 也接入同一 command driver；
6. 每个场景验证 reservation revoke/seal、journal seal、grant/Permit 回收、CRI workload absent 和重复启动拒绝。

Installer 的本地验证已补齐：systemd 失败回执、单次 rollback 和 symlink parent
防护均有临时 root 测试；仍未在目标机执行真实 install/reload。

Command gate 现在也有成功路径回归：Permit receipt 在进入长生命周期 wait 前落盘，
随后重新读取并执行 `Verify()`；这证明了持久化边界，但输入仍是 command test fixture，
不等同于目标机真实 composition。

## 不能由代码单独解决的外部前置

目标机 `marslab` 已证明 Ubuntu 24.04 / kernel `6.8.0-137-generic` 的 pidfd 兼容路径可运行，
但当前没有可用于真实 composition 的完整输入：

- signed Pod/Runtime launch plan、批准的 digest-pinned Runtime/Worker image；
- Fleet authorization provisioning、issuer private/reply key 和 authorization directory；
- 已应用 migration 00095/00096 且可回放 reservation/authorization 的 Fleet PostgreSQL；
- 能消费真实 startup grant 并返回 Permit 的 CPU ModelRuntime；
- RKE2/etcd 地址漂移修复后的稳定 CRI/Kubernetes control plane。

这些条件未提供前，不能把 synthetic launcher smoke、validation matrix 或 policy round-trip
receipt 解释成真实 Launch Receipt。

## 发布前最后一项

完成 validation composition 和联合故障矩阵后，才执行 canonical bundle 的
`install -> verify digests -> enable broker/issuer -> reload Node -> composition -> rollback`。
该回放必须留下每一步 receipt，并证明 rollback 后没有 socket、pidfd offer、journal grant
或 CRI workload 残留。

## 停止条件

在上面两段 validation 工作和目标机外部输入均完成前：

- 不修改 `Production Gates`；
- 不重启 RKE2；
- 不清理既有业务 workload；
- 不提交或 push 当前 dirty worktree；
- 不把 focused/native 测试或 synthetic smoke 宣称为生产闭环。

## 2026-09-13 late validation recheck

本轮重新执行了当前工作树的验证：`make test`（Go 全量与 16 项 Python
validation）、`make lint`、`go test -race ./cmd/vela-node-agent ./internal/nodeagent`、
`git diff --check` 均通过；Linux amd64 的 `cmd/vela-node-agent` test binary 也成功交叉
构建，SHA-256 为
`abb70e62e2ca4bf994e3b78c647ef2d86e9eaf39708263820dc50fd12dee7af2`。

安装前对 `marslab@100.111.196.116` 的只读复核结果为：Ubuntu 24.04、kernel
`6.8.0-137-generic`，`pidfd_open` 可用，`/sys/fs/pidfs` 不存在，
`vela-pidfd-broker.service` 为 `active`；policy issuer 和 Node Agent 仍未安装，
`/etc/vela/runtime-policy-issuer`、`/etc/vela/node-agent` 当时不存在，
Docker mirror 为 `https://docker.1ms.run/`。因此下一步仍是 command-level fixture 和
真实授权输入 provisioning；本次复核没有修改目标机服务、RKE2 或业务 workload。

同一轮还在仓库内临时目录完成了 runtime-startup package 的实际构建与独立验证：
`build-runtime-startup-packages` 成功生成完整 artifact inventory，随后
`verify-runtime-startup-packages validation-2026-09-13` 返回
`PASS runtime-startup-packages`。临时目录在进程退出时销毁；该结果证明 release artifact
graph 可生成和校验，但仍不等价于目标机 system path install 或 service enable。

随后用同一份临时 package 在 confined temporary root 执行了 installer 的真实
`install --apply` 与 `rollback --apply`，并检查 receipt 最终为 `status=rolled-back`、
`rolled_back=true`，且目标 root 下的五个安装目标全部被移除。该结果闭合了 installer 的
临时 root 回放；由于没有启用 systemd，它仍不代表 `marslab` 的真实 service reload 或
Node composition。

随后按已获授权的 validation 安装边界，将同一 source-matched package 安装到 `marslab`
的 system paths（installer receipt operation
`8d5d5ae4-aed7-45e2-a0e0-2683375c94ec`）。安装前 verifier 通过，三份 helper binary 和两份
unit 均按 receipt 中的 SHA-256 写入；随后仅重启了已经存在的
`vela-pidfd-broker.service`，确认服务回到 `active` 且 socket 存在，没有 enable 或 restart
issuer/Node Agent。没有重启 RKE2 或业务 workload。详细 hash 和 receipt 位置见
[`runtime-startup-packages-2026-09-13`](evidence/runtime-startup-packages-2026-09-13/README.md)。

重启后又用 host broker socket 做了 source-matched Linux amd64 外部回归：
`TestPIDFDBrokerExternalSocket` 在 root、无网络容器中四个场景（`same`、`different`、
`nested-same`、`wrong-gid`）全部通过，证明新安装的 broker 仍保持 pidfd 身份和凭据拒绝
语义。

另外新增的 `TestLoadRuntimeStartupResourcesWithFactoryAssemblesAndClosesDependencies` 已
用 source-matched Linux amd64 test binary 在 `marslab` 的 root、无网络容器中通过。该测试
走完整 command resource-loader 构造顺序，覆盖临时 Node ledger、Kubernetes/Pod reader、
CRI observer、Fleet registry 和受保护 startup socket，并在 close 后重新打开 ledger 验证
锁已释放、检查 socket 已删除以及 Fleet close 回调已执行。它闭合了 command resource
assembly 的验证边界，但仍不产生 Permit；Permit 仍需要下一层真实 launcher handoff、
observer custody、CRI task 和 ModelRuntime 输入。
