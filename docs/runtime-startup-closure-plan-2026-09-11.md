# Runtime startup 闭环收口清单

本清单基于当前分支 `8cf46be` 及之后的本地变更，目的是真实区分代码已经完成的部分、仍缺的代码，以及必须由部署环境提供的输入。当前 Production Gates 仍为 `0/9`。

## 已完成的代码

| 能力 | 当前实现 | 证据边界 |
| --- | --- | --- |
| signed launch source | `loadRuntimeStartupPlan`、`VerifyRuntimeLaunchPlan` | 验证 signed binding、bundle、Pod 计划和 caller UID/GID；不代表当前进程已启动 |
| journal owner | `loadRuntimeJournalOwner` | 只打开 Node 持有的 execution journal；不创建 Runtime/Worker 进程 |
| Kubernetes/CRI/Fleet source | `loadRuntimeKubernetesCore`、`loadRuntimeContainerObserver`、独立 startup Fleet client | 显式凭据、inode/权限和 SPIFFE 校验；不复用 WorkerInstance evidence client |
| protected listener | `listenRuntimeStartupSocketWithGID` | root-owned、verified Runtime GID、production `0660`、canonical path、identity-aware cleanup |
| caller pre-auth | `receiveRuntimeStartupCaller` | 使用 verified plan UID/GID；同一个 caller/pidfd 可传给 reservation 与 `ServeCaller` |
| startup evidence | `RuntimeStartupAuthorizationPolicy` | evidence 绑定 operation/request digest，UTC，最长 5 分钟；裸 `AuthorizationHash` fail-closed |
| startup ledger | `VELA_NODE_AGENT_RUNTIME_STARTUP_LEDGER_DIRECTORY` + `OpenRuntimeStartupLedger` | 独立目录、Node-owned lock、逆序关闭；不再与 execution journal state 混用 |
| shutdown owner | `runtimeStartupLifecycle` | orchestration 先 revoke/stop，再释放 listener、registry、observer、journal |
| pidfs 兼容 | `RuntimeCaller`/observer 使用 pidfd API；legacy invisible pidfd 通过 root-owned host broker 比较 | 目标 Ubuntu 24.04 / kernel 6.8 已验证；broker 已部署并通过权限、restart、replacement 和跨 namespace receipt；仍需接入真实 Node composition |

## 现在的剩余项（按依赖顺序）

1. **验证 helper 的负面场景与生产 helper 实现**：Node 已提供 `RuntimeStartupLauncher` 的受保护
   `SOCK_SEQPACKET + SCM_RIGHTS` adapter；平台仍必须部署一个 root-owned helper，按
   `docs/runtime-startup-launcher-contract-2026-09-11.md` 创建 Runtime/Worker、按首帧携带的
   `expected_pod` 与 `expected_pod_digest` 创建并核对本次 Pod、返回本次
   启动的原始 pidfd、observer endpoint 和 CRI target，并在 reservation 后提供 policy evidence。
   低层 orchestration 已移除裸 `AuthorizationDigest` 兼容入口，任何启动都必须经过该
   operation-bound policy。
   validation-only 的五类 helper fault 现在由
   `hack/run-runtime-startup-validation-matrix.py` 统一驱动，并按实际 CRI namespace
   核对精确 workload 回收；2026-09-12 已在加入 Worker journal socket parent read-only
   mount 后重新完成六场景矩阵；随后使用真实部署 broker 重跑并保存 receipt，但这仍不等于
   Node/Fleet 生产 receipt。
2. **真实端到端验证**：validation-only helper 已能在目标机 CRI namespace 创建独立
   `model-runtime`/`stage-worker-agent`、回传两个 target 和四个 descriptor（含 Runtime
   pidfd），并在退出时
   清理精确 workload；仍需用 Node composition root 贯穿 Pod/CRI/Fleet/journal/ModelRuntime
   CPU Job，覆盖 caller replacement、observer loss、回包丢失和 restart。测试 fixture 不能
   接入生产入口。

以下部分已经在本轮 composition root 中闭合：Worker owner/custody assembly、独立 startup ledger source、`Prepare`/`ServeCaller` 顺序、`SIGTERM`/`SIGINT`/context cancellation 生命周期，以及失败路径的 revoke/close。

## 验收顺序

每一项完成后都要有独立 receipt：

1. Linux native 单 caller：正常、同 UID 冒充、caller/pidfd 变化、observer 失联、回包丢失、ledger append 阻塞。
   validation helper 的正常与五类故障由 matrix driver 生成；Node composition 仍需独立
   运行同名场景并绑定真实 caller/observer。
2. Node crash/restart：只读取历史，不重建 owner、custody、grant 或 permission。
3. 目标主机 native race：在 `marslab` 的 source-matched checkout 运行，保留原有 dirty 文件，不覆盖部署 workload。
4. 真实 CPU Job：同一 composition root 贯穿 Pod/CRI/Fleet/journal/ModelRuntime；完成前不得提升 Production Gates。

Node startup ledger 的十个 crash boundary 已在目标机 native 通过；这只是 ledger recovery
证据，仍要在真实 composition 中重放并绑定 CRI、observer 和 Worker journal 生命周期。

## 当前外部阻塞

仓库内 composition root、Node helper adapter 和 validation-only CRI helper 已完成；部署环境还没有可交付的生产
Runtime/Worker helper。`VELA_NODE_AGENT_RUNTIME_LAUNCHER_PATH` 指向缺失或不满足 wire
contract 时，入口会 fail-closed。Kubernetes/CRI 观察器、历史 receipt、numeric PID 和
WorkerInstance reporter 都不能替代该 helper。接入 helper 后还必须完成真实
Node→CRI→observer→Fleet→journal→ModelRuntime 验证，以及生产 listener GID 发布、policy
issuer、故障 receipt 和镜像/任务连续性检查；在这些 receipt 完整前不能提升 Production Gates。
具体输入、句柄和生命周期要求见
`docs/runtime-startup-launcher-contract-2026-09-11.md`。
