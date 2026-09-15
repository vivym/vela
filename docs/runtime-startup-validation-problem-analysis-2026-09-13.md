# Runtime startup validation 问题分析（2026-09-13）

当前问题已经收敛为两个不同层次：Linux 的 `pidfs` 兼容路径基本成立，Node
composition 的可回放闭环仍未成立。继续重复 `pidfd` broker、launcher 或 CRI cleanup
smoke 不会关闭第二个问题。

## 已确认

- 目标机 Ubuntu 24.04、kernel `6.8.0-137-generic` 没有可用的 `pidfs` 接口，但支持
  `pidfd_open`。root-owned `vela-pidfd-broker` 交接原始 descriptor 的路径可运行；代码中
  没有从 numeric PID 重新打开 pidfd 的回退。
- Node 资源装配已经有 production factory 和 validation seam，失败时按逆序清理。
- `serveRuntimeStartupComposition` 现在要求真实 orchestration 产生 permitted receipt，并且
  `runRuntimeStartupGate` 会在进入长生命周期等待前持久化并校验该 receipt。
- 等待或 shutdown 失败时，Node 会在撤销 live grant 前先快照 composition receipt；因此失败
  回执仍能准确记录此前已经发出的 `Permit`、grant 和 authorization digest。
- broker downtime/restart 与 Node-side composition restart 已在 `marslab` 的 Linux amd64
  测试二进制中通过：旧 socket 请求 fail-closed，重启后只接受新的 descriptor；重启 Node
  只读恢复原 reservation/grant attempt，重复 startup 被拒绝。
- command gate 现在在 shutdown 前快照 composition receipt；专门的 Linux 测试证明 wait
  失败时 failure receipt 仍保留 `PermitIssued`、grant 和 authorization digest。
- 全仓 Go 测试、race/focused 测试、lint、Linux build 和 validation preflight 均通过；这些
  结果仍属于 validation evidence，`Production Gates` 保持 `0/9`。
- composition preflight 现在实际调用 `pidfd_open(getpid())` 并记录 `pidfs` 是否存在；
  `pidfs` 缺失只选择 broker fallback，不会被误判为内核不支持。
- composition preflight 对延迟创建的 socket/state directory 现在还验证父目录已经存在、
  为 root-owned directory 且没有 group/other write；父目录缺失或不受信任会 fail-closed，
  不再把这类环境标记为 `deferred`。

## 真正剩余的代码问题

现有 opt-in 的 `TestRuntimeStartupCommandCompositionHarness` 依赖外部已 provisioned 的
plan、CRI、Fleet、issuer 和 ModelRuntime，缺输入时直接 `SKIP`。它没有自动生成一套可销毁
的完整 fixture，也没有把以下对象在一次进程中绑定到同一个 `operation_id`：

```text
signed plan → CRI Runtime/Worker → observer custody → Fleet reservation
→ Node journal → Worker journal mutation → startup grant → ModelRuntime Permit
```

直接在 `cmd/vela-node-agent` 中继续增加 fake 不可行：`RuntimeContainerObserver` 和
`RuntimeNamespaceOwner` 的 reader、task client、boot identity、pidfd ownership 都是
private；伪造 grant/Permit 又会绕过要验证的信任边界。

## 采用的实现方案

1. 在 `internal/nodeagent` Linux 测试包内实现完整 fixture。这里可以复用现有 signed
   launch-plan、observer、namespace-owner、journal 和 reservation fixture，同时仍调用生产
   reservation/orchestration 方法；launcher 必须交接真实 subprocess pidfd、observer pidfd
   和 socketpair。
2. 在同一 fixture 中实现正常路径和六类故障回放：caller replacement、observer loss、
   policy response loss、helper timeout/crash、Node restart、broker restart。每个场景都
   检查 reservation/journal seal、grant/Permit 回收、CRI workload absent 和重复启动拒绝。
3. 保留 `cmd/vela-node-agent` 为薄 wrapper 测试：调用同一 composition boundary，落盘 typed
   receipt，再读取并执行 `Verify`/`VerifyBinding`。这样可以证明 command wiring，不需要把
   private owner state 变成生产 API。

命令层资源工厂的 Pod reader seam 现在使用公开的
`nodeagent.RuntimeLaunchPodReader` 接口，而不是绑定到 Kubernetes 具体实现；生产默认仍
使用 `KubernetesRuntimeLaunchPodReader`。这样验证夹具可以替换 Pod 读取器，同时保留
authority、ledger 和 receipt 的生产调用顺序。

## 外部前置

代码 fixture 完成后，仍必须在 `marslab` 提供 source-matched signed plan、digest-pinned
Runtime/Worker image、Fleet PostgreSQL migration 00095/00096、issuer/Fleet key、可返回
Permit 的 CPU ModelRuntime，以及稳定的 RKE2/CRI control plane。当前 issuer/Node 服务和
这些授权输入尚未 provisioned；因此暂时不能运行真实 composition，也不能升级系统、重启
RKE2 或清理业务 workload。

## 2026-09-13 目标机复核

通过 `ssh -o BatchMode=yes marslab@100.111.196.116` 复核到：目标机为
`marslab-server`，Ubuntu 24.04，kernel `6.8.0-137-generic`，`pidfd_open` 可用且
`pidfs` 不存在；`vela-pidfd-broker.service` 为 `active`。Docker Server 为 `29.1.3`，
镜像 mirror 为 `https://docker.1ms.run/`，中国镜像链路可用于后续验证镜像拉取。

当前 `vela-runtime-policy-issuer.service` 和 `vela-node-agent.service` 均未安装/启用，
`/etc/vela/runtime-policy-issuer`、`/etc/vela/node-agent` 以及 `/var/lib/vela` 尚不存在。
这属于 release install 和授权输入 provisioning 尚未完成，不是 `pidfs` 兼容代码失败；在
补齐 signed plan、digest-pinned images、Fleet/issuer keys、migrations 00095/00096 与
Permit-capable ModelRuntime 前，不应启动真实 composition。

当前源码构建的 runtime-startup package 已在 `marslab` 做过一次临时传输与 hash 复核，
但没有解包到系统目录；该证据只证明 artifact graph 和传输完整性，不能替代 install、
enable、composition 或 rollback。详见
[`runtime-startup-packages-2026-09-13`](evidence/runtime-startup-packages-2026-09-13/README.md)。

## 验收顺序

先完成 internal fixture 和 typed receipt replay，再做同一 fixture 的故障矩阵；之后才在
`marslab` 做一次 native composition，最后执行 canonical bundle 的 install、enable、
reload 和 rollback 回放。四层都产生同范围 receipt 后，才重新评估九个 Production Gates。
