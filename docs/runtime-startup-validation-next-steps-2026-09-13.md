# Runtime startup validation: next implementation slice

## 2026-09-13 composition-driver closure

The command composition target check now treats the signed Pod as a template:
it binds namespace/name and container identities from the plan, while the live
Pod UID comes from the launcher and is verified by the CRI/Kubernetes
observation. This avoids rejecting every normal Pod creation whose template
has no `metadata.uid`.

The internal Linux validation slice now has one shared composition driver in
`internal/nodeagent/runtime_startup_composition_matrix_linux_test.go`. Its
success path performs the authenticated exchange, operation-bound grant,
paired Worker journal mutation, independent receipt replay and duplicate-start
rejection. The same driver covers policy response loss, caller replacement and
observer loss. The source-matched run on `marslab` passed; evidence is in
[`runtime-startup-composition-driver-2026-09-13`](evidence/runtime-startup-composition-driver-2026-09-13/README.md).

This closes the internal composition/failure-matrix slice. The remaining code
boundary is the command resource-loader harness, which still needs to run the
same fixture through `loadRuntimeStartupResourcesWithFactory` and
`composeRuntimeStartupAuthority`; production gates remain unchanged until that
boundary and the real host inputs are available.

当前仍是 validation 阶段，`Production Gates=0/9`。现有证据已经覆盖
pidfs 不可用时的 root-owned broker、launcher v2 的四个 descriptor、CRI 精确清理和
helper 级异常矩阵；它们没有覆盖 Node composition root 的完整成功路径。

## 2026-09-13 增量

production launcher 的 pidfd wrapper 已修复 `SIGCONT` lost-wakeup 竞态：释放信号在
发送 pidfd 前注册并缓冲，等待具有两分钟上限。`marslab` Linux amd64 root 回归
`TestProductionPIDFDOfferBindsKernelSenderToTask/self` 通过。internal composition 的
receipt replay 现在使用独立 durable reservation JSON 和 backend request 重新计算摘要；
成功、Node restart、policy response loss、caller replacement、observer loss 以及
command gate helper timeout/crash 回执测试均已通过。证据见
[`runtime-startup-pidfd-release-race-2026-09-13.md`](evidence/runtime-startup-pidfd-release-race-2026-09-13.md)。

## 需要补齐的 command-level 验证闭环

内部 composition driver 已完成；剩余 command-level harness 仍需把以下输入在一次测试运行中生成并在结束时销毁：

1. 生成临时 Ed25519 plan、Fleet authorization key 和 policy issuer reply key。
2. 启动 fake Fleet、fake journal/Worker journal 和 CPU ModelRuntime backend；每个 backend
   只接受带 operation/request/reservation digest 的请求。
3. 使用 `composeRuntimeStartupAuthority` 真实装配 Node authority。launcher double 只负责
   交接真实 pidfd/socketpair，不能直接返回 grant 或 Permit。
4. 让 caller 通过 startup socket 完成握手，依次观察 Fleet reservation、Node journal、Worker
   journal mutation、startup grant 和 ModelRuntime Permit，输出一个 typed receipt；receipt
   必须能用 digest 串回同一个 operation。
5. 在同一个 harness 重放 caller replacement、observer loss、policy response loss、helper
   timeout、helper crash 和 Node restart。每个场景都检查 reservation revoke、journal seal、
   grant/Permit 回收、CRI workload 清理和重复启动拒绝。

Composition preflight 现在还会单独报告 `pidfd_open` 和 `pidfs`：`pidfd_open` 缺失会
fail-closed；`pidfs` 缺失只记录为 `absent-broker-fallback`，因为目标机使用
root-owned pidfd broker。这项检查仍只验证内核能力和输入完整性，不启动 Node 或修改 CRI。

Node 侧现已提供 `RuntimeStartupOrchestration.CompositionReceipt`，以
`operation_id`、`request_digest`、`reservation_digest`、authorization digest、grant-attempt
digest 和 Permit outcome 生成可自校验的 typed receipt。命令级 harness 仍需把 launcher
handoff、observer custody 和该 receipt 在一次运行中接起来。

命令层资源工厂现在同时允许验证夹具替换 plan、StageAuthority validator、journal owner、
ledger、Pod reader、CRI observer、Fleet registry 和 startup socket 的构造器；生产默认构造
路径保持不变。这样下一步可以在同一 composition root 注入一次性本地资源并验证完整清理，
而不必把测试状态导出为生产 API。

Receipt 校验现在还要求 `request_digest`、`reservation_digest` 和
`authorization_digest` 均为非空值；缺少任一 authority digest 的“permitted”记录会被
拒绝，避免把不完整的 grant 记录误当成组合闭环。

## 现有测试为何还不够

`hack/run-runtime-startup-validation-matrix.py` 直接驱动 validation helper，未经过 Node
adapter；其正常 policy reply 使用 synthetic `journal_id` 和 `reservation_digest`。
`internal/integration/runtime_startup_node_process_test.go` 已包含更接近目标的 Fleet/TLS/
PostgreSQL 流程，但没有设置 `VELA_RUNTIME_STARTUP_NODE_IMAGE` 时会 `SKIP`，且当前 fixture
issuer 明确不提供 backend Permit。因此这些测试可以作为 harness 的部件，不能作为组合闭环
receipt。

## Installer gate now covered

The release installer now has temporary-root tests for systemd failure
transitions, single-use rollback receipts, and symlink-safe target parents. A
failed service enable/reload restores files and persists `status=failed`;
successful rollback persists `status=rolled-back` and rejects replay. This
closes installer bookkeeping for validation, but a real host install still
requires the signed plan, images, keys and Permit-capable ModelRuntime listed
below.

The preflight also rejects broken symlinks and existing state directories that
are not root-owned and private, while retaining the explicit `pidfs` optional
broker-fallback result.

## 依赖和停止条件

composition harness 完成前不修改生产 gate 状态，也不把目标机的 synthetic launcher smoke
当成 Launch Receipt。完成 harness 后，再在 `marslab` 使用 source-matched image 运行 native
receipt；目标机的 RKE2/etcd 地址漂移必须由运维修复，测试不得重启现有业务 workload。

## 本轮额外收紧

attached observer 在 custody acknowledgement 后、释放 ptrace/pidfd gate 前再次核对目标
进程的 effective UID/GID，避免握手期间身份变化。该检查有 Linux 单测覆盖。

## 本轮实现与验证

production launcher 已补齐批准 init container 的执行边界：两个 init container 按签名顺序
创建、启动并等待成功退出；其 digest-pinned 镜像在 PodSandbox 建立前完成预检。CPU/memory
资源、只读 rootfs、NoNewPrivs、RuntimeDefault seccomp、FSGroup 和支持的 volume source
会映射到 CRI，无法复现的字段继续 fail-closed。fake CRI 生命周期测试覆盖成功、非零退出、
启动失败、状态失败、上下文超时和 cleanup；Linux amd64 测试二进制已在 `marslab` Ubuntu
24.04 / kernel `6.8.0-137-generic` 通过。该证据只覆盖 launcher contract，仍不能替代
command-level composition harness。

此外，`marslab` 已在临时 root-owned `exec-observer`/`exec-probe` 条件下通过 Node
authority、coordinator、orchestration 和 observer-custody 的 native Linux 测试组；证据
位于 `docs/evidence/runtime-startup-nodeagent-native-2026-09-13/`。这扩大了 Node 侧原生
覆盖，但没有改变 command-level composition harness 仍待完成的结论。

本轮还修复了 Fleet Pod 与 launcher 的 bootstrap argv 契约：Fleet 现在显式生成
`Command=["/usr/local/bin/vela-model-runtime"]` 和
`Args=["serve-remote","--bootstrap-file","/run/vela-model-runtime-bootstrap/bootstrap.json"]`；
Node/launcher 校验时按 OCI 规则合并两者，避免把 argv[0] 错放进 `Args`。Fleet、Node 和
launcher 相关测试以及全仓 `go test ./...` 均通过。

本轮又把 native Fleet/Node process helper 的成功分支接入
`RuntimeStartupCompositionReceipt.VerifyBinding`：Permit 返回后会读取同一 ledger 的
grant attempt，以 `operation_id`、`request_digest`、reservation digest 和 authorization
digest 做外部绑定校验，并把 receipt 写入 helper report。该改动增强了已有 process-level
fixture 的证据关联，但它仍不等同于 `cmd/vela-node-agent` 的真实资源装配；后者需要在
具备 signed plan、CRI observer 和 production launcher 的环境中单独运行。

随后，internal composition 的成功测试增加了临时文件持久化回放：receipt JSON 会在关闭
orchestration 前写入、重新读取，并执行 `Verify` 与 `VerifyBinding`。这闭合了 validation
fixture 的可回放要求，但仍不替代 command-level 的真实资源装配。

此外，`internal/nodeagent/runtime_startup_authority_composition_linux_test.go` 已增加同一
Node composition fixture 的 policy response loss、caller replacement 和 observer loss
分支，分别检查 reservation/grant 边界及 failed-closed receipt；Node/broker restart 的
进程级恢复测试仍由现有 ledger/broker suites 覆盖，尚未与 command-level launcher 入口
合并成一次目标机回放。

command-level harness 还增加了 policy issuer loss 和 helper timeout/crash 的注入测试；
注入只替换 validation bootstrap/policy seam，生产默认仍调用真实 bootstrap publication
和外部 issuer，避免测试绕过生产路径后被误标为成功。

这些边界已在 `marslab` 的 source-matched Linux amd64 测试二进制上实际通过，证据见
`docs/evidence/runtime-startup-command-composition-failclosed-2026-09-13/README.md`；该次
运行没有启动 Node/Fleet/issuer 服务，也没有创建 CRI workload。

## Harness implementation decision

The remaining command-level harness cannot be completed by adding more fakes in
`cmd/vela-node-agent` alone. `RuntimeContainerObserver` and
`RuntimeNamespaceOwner` intentionally keep their readers, task client, boot
identity and pidfd ownership private. A command-package test therefore cannot
construct a valid observer/owner or safely manufacture the process handles that
`composeRuntimeStartupAuthority` requires. Expanding those internals into a
production-facing fake API would weaken the trust boundary being tested.

The implementation path is split into two layers:

1. Put the full fixture and failure matrix in an `internal/nodeagent` Linux test
   package, where the existing observer, namespace-owner and launch-plan
   fixtures are available. The fixture must still call the production
   reservation/orchestration methods and use real subprocess pidfds and
   socketpairs; it may not construct grants or Permits directly.
2. Keep `cmd/vela-node-agent` as a thin composition-root test. It invokes the
   same resource factory and `serveRuntimeStartupComposition` boundary against
   the fixture's exported driver, persists the receipt, and replays
   `VerifyBinding`. This proves command wiring without exporting private owner
   state.

This split is a code prerequisite, separate from deployment. The external
blockers remain the signed production plan/images, Fleet PostgreSQL migrations
and keys, a Permit-capable CPU ModelRuntime, and a stable RKE2/CRI control plane
on `marslab`. Until both layers and those inputs exist, `Production Gates=0/9`
remains the only valid status.
