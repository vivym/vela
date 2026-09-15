# Runtime startup validation execution plan

当前阶段是 validation，`Production Gates=0/9`。本文件把剩余工作收敛成有限的
验收门槛，避免把同一个 helper smoke 反复当成新的闭环证据。

## 结论先行

`pidfs` 不可用不会阻止运行。目标机的内核已经提供 `pidfd`（`anon_inode:[pidfd]`），
但没有可见的 `pidfs` 文件系统接口；因此兼容路径是 root-owned `vela-pidfd-broker`。
Node、Worker journal 和 ModelRuntime 只交接本次创建得到的原始 pidfd，跨 namespace 的
身份比较由 broker 完成。任何地方都不能从 numeric PID 重新 `pidfd_open`，也不能把
`/proc` 观察结果当成句柄等价物。

当前协议、broker、CRI 精确清理、observer custody、Worker journal RPC 和 release
artifact graph 已有 focused/native 证据。剩余问题集中在一次真实 Node composition 是否
能把这些部件串成同一个 `operation_id`，以及生产输入是否已经被实际安装和回放。

## 四个剩余门槛

### 1. Command-level composition harness（可在 validation 阶段完成）

`cmd/vela-node-agent/runtime_startup_composition_linux_test.go` 已提供可重复的 command-level
harness 入口，直接调用 `composeRuntimeStartupAuthority`。测试通过
`VELA_RUNTIME_STARTUP_COMPOSITION_HARNESS=1` 显式开启；没有完整输入时保持 skip，避免
把伪造资源写成成功证据。当前入口消费已 provisioned 的 signed plan、CRI、Fleet、issuer
和 ModelRuntime；临时 fixture 生成仍需在 validation 环境补齐：

本轮已将资源装配提取为 `loadRuntimeStartupResourcesWithFactory`，生产入口仍使用完整的
默认构造器；validation harness 可以替换 Kubernetes、CRI、Fleet 和 socket 构造器，同时
继续执行相同的 plan/validator/journal/ledger 顺序和失败清理。该 seam 本身已由单测验证，
但还没有借此完成成功路径 receipt。成功边界也已提取为
`serveRuntimeStartupComposition`，供 harness 和 daemon 共同调用，避免测试只覆盖旁路逻辑。
其消费的是最小的 orchestration lifecycle interface，validation 可以验证成功/失败边界，
并会把成功 receipt 写入临时目录后重新读取、`Verify` 和 `VerifyBinding`；它不会伪造
authority、grant 或 Permit。

- signed Runtime launch plan、Pod digest 和临时 Ed25519 issuer/Fleet key；
- fake Fleet reservation、fake Node/Worker journal 和 CPU ModelRuntime backend；
- launcher double 交接真实 pidfd、observer pidfd 和 socketpair，不制造 grant/Permit。

正常路径必须在一个 operation 内观察到：

`caller → launcher/CRI → observer custody → Fleet reservation → Node journal →
Worker journal mutation → startup grant → ModelRuntime Permit`。

测试最终写出并 `Verify()` 一个 composition receipt，至少包含
`operation_id`、`request_digest`、`reservation_digest`、authorization digest、grant
attempt digest 和 Permit outcome。receipt 中的每个 digest 必须来自真实 wire 或 durable
record，不能使用 synthetic zero value。

### 2. 同一 harness 的故障矩阵（可在 validation 阶段完成）

在同一个 composition harness 中重放以下场景，并对每个场景检查相同的后置条件：

| 场景 | 必须证明 |
| --- | --- |
| caller replacement | 原始 pidfd 身份不匹配即拒绝；reservation、grant、Permit 均不存在 |
| observer loss | custody 检查失败；reservation revoke、journal seal、CRI cleanup |
| policy response loss | 超时后 fail-closed；没有 startup grant 或 Permit |
| helper timeout/crash | Node 取消后仍完成 CRI cleanup；receipt 为 failed |
| Node restart | 只读 ledger 恢复；不重建 owner、grant 或 Permit；重复 startup 被拒绝 |
| broker restart | broker 重启期间请求失败并回收；恢复后只能接受新的 operation |

每个场景都要保存同范围 typed receipt。单独的 helper receipt、旧三 descriptor receipt
或只证明 `StopContainer/RemoveContainer` 的结果，不能替代这一门槛。

### 3. 目标机真实输入（需要运维/部署资料）

完成前两项后，才能在 `marslab` 做一次 source-matched run。所需输入是：

1. signed plan、approved digest-pinned Runtime/Worker images、CRI kubeconfig 和 observer
   executable；
2. policy issuer reply key、Fleet authorization public key、authorization directory；
3. Fleet PostgreSQL 已应用 migration 00095/00096，并能回放 reservation/authorization；
4. 一个 CPU ModelRuntime backend，能消费真实 startup grant 并返回 Permit；
5. 目标机当前 RKE2/etcd 地址漂移（manifest `10.1.200.17`，网卡 `10.1.201.66`）由运维
   在变更窗口修复。测试不得自动改 manifest、重启 RKE2 或清理业务 workload。

validation 阶段的合格结果是 `validation_only=true` 的完整、可回放 composition receipt；
只有后续真实生产输入的 source-matched run 才能生成 `validation_only=false` 的 Launch
Receipt。Synthetic Pod 的 production launcher smoke 继续作为 helper 证据。

### 4. Release 安装回放（需要部署窗口）

把 launcher、pidfd broker、policy issuer、systemd units、migration 和 provisioning
contract 放进同一个 canonical bundle，在目标机执行：

`install → verify source/image/config digests → enable broker → enable issuer → reload
Node → run composition → rollback check`。

每一步都要有 receipt，且 rollback 后不能留下 socket、pidfd offer、journal grant 或 CRI
容器。完成前不改变 `Production Gates`。

## 执行顺序和停止条件

执行顺序固定为：

1. 先完成 command-level harness 的正常路径和 receipt 自校验；
2. 在同一 harness 完成六类故障和 Node/broker restart；
3. 再准备目标机的真实签名计划、key、PostgreSQL 和 CPU backend，做一次 source-matched
   composition run；
4. 最后做 release install/reload/rollback 回放；
5. 只有四项都有同范围 receipt，才重新评估九个 Production Gates。

以下结果明确不算闭环：`go test`、交叉编译、pidfd broker 单测、validation matrix、
synthetic launcher smoke、只含 policy round-trip 的 receipt，或从 CRI numeric PID 重开
的 pidfd。

## 当前状态

- 本地全仓 `go test ./...`、Linux `go vet`、race/focused tests 已通过。
- `marslab` 已证明 Ubuntu 24.04 / kernel 6.8 的 pidfd broker、四 descriptor handoff、
  observer ancestry 和 CRI cleanup；不需要升级系统。
- 2026-09-13 已用 source-matched protocol-v2 launcher 在 `marslab` 重跑 synthetic
  smoke：request/reply `version=2`、四 descriptor、observer handshake、`TracerPid`
  和 CRI cleanup 全部通过；临时 root-owned binary 和 bootstrap 目录已清理。详见
  [`runtime-startup-production-launcher-smoke-2026-09-13-v2.md`](evidence/runtime-startup-production-launcher-smoke-2026-09-13-v2.md)。
- `marslab` 当前 `vela-pidfd-broker.service` 为 `active`，但
  `vela-runtime-policy-issuer.service` 和 `vela-node-agent.service` 均为 `inactive`；
  issuer env 中的 private key、Fleet public key、authorization/reply-cache directory
  和 socket 路径也都不存在；因此真实 issuer/Node composition 仍没有可执行的生产输入。
- Node composition 正常路径尚未由 command-level harness 跑通；资源构造 seam 已完成，下一段
  需要用它生成完整 fixture 并跑通成功 receipt。
- broker restart 和 Node-side composition restart 现已有 Linux amd64 进程级回归证据，分别见
  [`runtime-startup-broker-restart-2026-09-13.md`](evidence/runtime-startup-broker-restart-2026-09-13.md)
  和 [`runtime-startup-composition-node-restart-2026-09-13.md`](evidence/runtime-startup-composition-node-restart-2026-09-13.md)。
  这些证据关闭了各自的局部恢复语义，但仍未替代一次完整 command-level composition。
- 真实 issuer/Fleet/ModelRuntime、RKE2 地址修复和 release install 属于外部输入，不能用
  synthetic fixture 填充。
- 当前源码的 runtime-startup 三件套已由 `vela-release-artifacts` 构建并在 `marslab` 做过
  一次不落盘的 hash/目录复核；传输后已清理临时包。该证据见
  [`runtime-startup-packages-2026-09-13`](evidence/runtime-startup-packages-2026-09-13/README.md)，
  不改变 release install 仍未执行的状态。

因此当前最短路径是先完成第 1、2 项并生成一套可回放 receipt，然后一次性列出第 3、4 项
的部署前置；在此之前保持 `Production Gates=0/9` 是正确且可解释的状态。
