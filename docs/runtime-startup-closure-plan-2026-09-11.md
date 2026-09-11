# Runtime startup 闭环收口清单

本清单基于当前分支 `8cf46be` 及之后的本地变更，目的是真实区分代码已经完成的部分、仍缺的代码，以及必须由部署环境提供的输入。当前 Production Gates 仍为 `0/9`。

## 已完成的代码

| 能力 | 当前实现 | 证据边界 |
| --- | --- | --- |
| signed launch source | `loadRuntimeStartupPlan`、`VerifyRuntimeLaunchPlan` | 验证 signed binding、bundle、Pod 计划和 caller UID/GID；不代表当前进程已启动 |
| journal owner | `loadRuntimeJournalOwner` | 只打开 Node 持有的 execution journal；不创建 Runtime/Worker 进程 |
| Kubernetes/CRI/Fleet source | `loadRuntimeKubernetesCore`、`loadRuntimeContainerObserver`、独立 startup Fleet client | 显式凭据、inode/权限和 SPIFFE 校验；不复用 WorkerInstance evidence client |
| protected listener | `listenRuntimeStartupSocket` | root-owned、`0600`、canonical path、identity-aware cleanup |
| caller pre-auth | `receiveRuntimeStartupCaller` | 使用 verified plan UID/GID；同一个 caller/pidfd 可传给 reservation 与 `ServeCaller` |
| startup evidence | `RuntimeStartupAuthorizationPolicy` | evidence 绑定 operation/request digest，UTC，最长 5 分钟；裸 `AuthorizationHash` fail-closed |
| shutdown owner | `runtimeStartupLifecycle` | orchestration 先 revoke/stop，再释放 listener、registry、observer、journal |
| pidfs 兼容 | `RuntimeCaller`/observer 使用 pidfd API | 目标 Ubuntu 24.04 / kernel 6.8 已验证；不要求升级目标系统 |

## 仍缺的代码（按依赖顺序）

1. **Node process launcher contract**：定义并实现 Node 自己创建 Runtime 与 Worker 的 launcher。它必须返回原始 Runtime/Worker pidfd、observer socketpair 两端及关闭责任；不能从 PID、receipt 或历史 reservation 重建。
2. **Worker owner/custody assembly**：用 launcher 返回的 Worker pidfd 调用 `RetainNamespaceOwner`，用 Node 创建的 observer socketpair 和原始 observer pidfd 调用 `ReceiveRuntimeObserverCustody`，并在失败路径 revoke/close。
3. **Runtime startup ledger source**：为命令配置增加独立 ledger directory，调用 `OpenRuntimeStartupLedger(..., initialize=true)`；execution journal state directory 不能被默认当作 ledger directory。
4. **Concrete policy adapter**：把实际策略输入（当前 plan、Node identity、runtime incarnation、Fleet reservation record 和有效 launch/continuity evidence）实现为 `RuntimeStartupAuthorizationPolicy`。测试 fixture 不能接入生产入口。
5. **Composition root**：`runRuntimeStartupGate` 按顺序加载 ledger、plan、journal、Kubernetes、CRI、Fleet、listener，启动 launcher，预认证 caller，组装 authority，调用 `Prepare`，再调用 `ServeCaller`。
6. **Signal lifecycle**：将 `SIGTERM/SIGINT/context cancellation` 接到同一个 lifecycle；超时必须保持 revoked/fail-closed，并等待后台 cleanup，不得直接关闭 pidfd 让 coordinator 失去观察。
7. **ModelRuntime server handoff**：确认 caller 的 request/reply 与 ModelRuntime remote-startup server 的协议、版本和 timeout 完全一致；不能把现有 `Serve(listener)` adapter 当作生产闭环。

## 验收顺序

每一项完成后都要有独立 receipt：

1. Linux native 单 caller：正常、同 UID 冒充、caller/pidfd 变化、observer 失联、回包丢失、ledger append 阻塞。
2. Node crash/restart：只读取历史，不重建 owner、custody、grant 或 permission。
3. 目标主机 native race：在 `marslab` 的 source-matched checkout 运行，保留原有 dirty 文件，不覆盖部署 workload。
4. 真实 CPU Job：同一 composition root 贯穿 Pod/CRI/Fleet/journal/ModelRuntime；完成前不得提升 Production Gates。

## 当前唯一硬阻塞

仓库目前没有生产 Runtime/Worker launcher，也没有配置来源能提供 Worker pidfd 与 observer custody。继续实现这两项之前，任何把 `runRuntimeStartupGate` 改成成功启动的代码都会制造伪 authority。因此当前入口必须继续返回 fail-closed 错误；这不是测试未跑完，而是所需 authority source 尚未存在。
