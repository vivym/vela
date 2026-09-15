# Runtime startup validation matrix receipt

2026-09-12 在 `marslab@100.111.196.116` 的 Ubuntu 24.04 / kernel
`6.8.0-137-generic` 上运行了 validation-only 矩阵。目标机使用 k3s
containerd socket `/run/k3s/containerd/containerd.sock`、固定本地镜像和
`/tmp/vela-target-run/exec-observer-fixed`。结果目录导回本仓库后不再修改。
当前协议版本的主证据为
[`runtime-startup-validation-matrix-2026-09-13-v2`](evidence/runtime-startup-validation-matrix-2026-09-13-v2/)；
下文早期目录仅作为协议演进历史保留。

历史矩阵结果见 [`final9/matrix.json`](evidence/runtime-startup-validation-matrix-2026-09-12-final9/matrix.json)；当前矩阵结果见
[`v2/matrix.json`](evidence/runtime-startup-validation-matrix-2026-09-13-v2/matrix.json)，
逐场景原子 receipt 也在同一目录。矩阵驱动按 helper 返回的本场景
`Runtime container`、`Worker container` 和 `PodSandbox` identity 判定 cleanup；CRI
列表中同时出现的其他 Kubernetes 容器不会被误计为泄漏：

| 场景 | receipt outcome | cleanup | 关键结果 |
| --- | --- | --- | --- |
| `normal` | `completed` | `true` | 两个 CRI target、四个 descriptor、同一 operation/request digest 的 validation policy round-trip |
| `caller-replacement` | `failed` | `true` | 替换 Worker owner descriptor 后 fail-closed |
| `observer-channel-loss` | `failed` | `true` | observer endpoint 丢失后 fail-closed |
| `policy-response-loss` | `failed` | `true` | policy response 被抑制后 fail-closed |
| `helper-timeout` | `failed` | `true` | 控制上下文终止后 receipt 明确为 failed |
| `helper-crash` | `failed` | `true` | helper panic 先清理 CRI workload，再写 failed receipt |

所有场景都验证了本次创建的 Runtime/Worker container 和 sandbox 在退出后从 CRI
namespace 中消失。`Node restart` 没有由 helper 伪造，仍标记为
`external-driver-required`。driver 直接启动 validation helper，没有运行 Node adapter；这组 receipt
只证明 helper wire protocol、validation policy round-trip 和真实 CRI cleanup 的边界，不证明
Node adapter 已在同一次 composition 中运行，也不证明 signed plan、Fleet reservation、journal owner、
ModelRuntime Permit 或生产 helper，因此 `Production Gates` 继续为 `0/9`。

其中 observer pidfd 来自 observer 创建时的 `PidFD`。本报告所记录的旧矩阵版本中，Worker
pidfd 曾受 CRI task API 限制而从 numeric PID 重开，因此该旧 receipt 不能证明原始句柄
交接。2026-09-12 后续修正的 launcher 已在容器内 wrapper 的创建边界取得自身 pidfd，
通过 `SCM_RIGHTS` 交给宿主再 `exec` 目标命令；该修正版需要重新生成完整矩阵 receipt，
且仍然只属于 validation-only。生产 helper 仍必须在受信任的创建边界取得原始 Worker pidfd。

修正版完整六场景 receipt 已保存于
[`runtime-startup-validation-matrix-2026-09-12-pidfd-corrected`](evidence/runtime-startup-validation-matrix-2026-09-12-pidfd-corrected/)。
该版本还将 offer descriptor 与 Unix peer pidfd 及本次 CRI task 做了 live binding，并通过
wrong-task、wrong-UID、regular-file、different-process native 反例；它仍然是
validation-only evidence。

随后按 Worker journal socket contract 重新运行六场景矩阵。新的 receipt 保存在
[`runtime-startup-validation-matrix-2026-09-12-worker-journal`](evidence/runtime-startup-validation-matrix-2026-09-12-worker-journal/)。
本次 signed Pod 同时含有 `model-runtime` 与 `stage-worker-agent`，后者声明
`VELA_WORKER_JOURNAL_SOCKET`；launcher 为 socket parent directory 建立 read-only
host mount，并在 Worker 中注入相同路径。六个场景均完成精确 CRI cleanup：正常场景
`completed`，五个故障场景 `failed`。这仍只覆盖 validation-only launcher 和 mount
边界，没有接入真实 Node journal server、Fleet 或 ModelRuntime，因此
`Production Gates` 继续为 `0/9`。

随后在目标机安装并启用 root-owned pidfd broker，以同一六场景矩阵重新运行；结果见
[`runtime-startup-validation-matrix-2026-09-12-broker`](evidence/runtime-startup-validation-matrix-2026-09-12-broker/)。
signed Pod 同时声明 `VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET`，launcher 将 broker
parent directory 只读挂载给 Worker。六个场景仍分别得到
`completed/failed` 与 `cleanup_verified=true`，并证明不支持 `pidfs` 的 kernel 也能走
真实 broker transport；这仍不产生生产 Launch Receipt 或 Production Gate。

随后重新构建 launcher，增加 signed Pod broker path 与 helper configuration 的逐字核对；
该次旧 v2 目录仍保留作历史记录。使用当前 launcher protocol v2（四 descriptor）重新运行的
六场景 receipt 见
[`runtime-startup-validation-matrix-2026-09-13-v2`](evidence/runtime-startup-validation-matrix-2026-09-13-v2/)。
新矩阵逐场景检查 `reply.version=2`、`fd_count=4`、精确 CRI cleanup 和
`validation_errors=[]`；正常场景为 `completed`，五个受控故障场景为 `failed`。
