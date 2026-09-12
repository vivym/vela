# Runtime startup validation matrix receipt

2026-09-12 在 `marslab@100.111.196.116` 的 Ubuntu 24.04 / kernel
`6.8.0-137-generic` 上运行了 validation-only 矩阵。目标机使用 k3s
containerd socket `/run/k3s/containerd/containerd.sock`、固定本地镜像和
`/tmp/vela-target-run/exec-observer-fixed`。结果目录导回本仓库后不再修改。

矩阵结果见 [`matrix.json`](evidence/runtime-startup-validation-matrix-2026-09-12-final9/matrix.json)，
逐场景原子 receipt 也在同一目录。矩阵驱动按 helper 返回的本场景
`Runtime container`、`Worker container` 和 `PodSandbox` identity 判定 cleanup；CRI
列表中同时出现的其他 Kubernetes 容器不会被误计为泄漏：

| 场景 | receipt outcome | cleanup | 关键结果 |
| --- | --- | --- | --- |
| `normal` | `completed` | `true` | 两个 CRI target、三个 descriptor、同一 operation/request digest 的 policy round-trip |
| `caller-replacement` | `failed` | `true` | 替换 Worker owner descriptor 后 fail-closed |
| `observer-channel-loss` | `failed` | `true` | observer endpoint 丢失后 fail-closed |
| `policy-response-loss` | `failed` | `true` | policy response 被抑制后 fail-closed |
| `helper-timeout` | `failed` | `true` | 控制上下文终止后 receipt 明确为 failed |
| `helper-crash` | `failed` | `true` | helper panic 先清理 CRI workload，再写 failed receipt |

所有场景都验证了本次创建的 Runtime/Worker container 和 sandbox 在退出后从 CRI
namespace 中消失。`Node restart` 没有由 helper 伪造，仍标记为
`external-driver-required`。这组 receipt 只证明 validation helper、Node wire adapter
和真实 CRI cleanup 的边界，不证明 signed plan、Fleet reservation、journal owner、
ModelRuntime Permit 或生产 helper，因此 `Production Gates` 继续为 `0/9`。

其中 observer pidfd 来自 observer 创建时的 `PidFD`，Worker pidfd 受 CRI task API 限制，
由 validation-only 路径从当前 task PID 调用 `pidfd_open` 得到；这条路径明确不能进入生产。
生产 helper 仍必须在受信任的创建边界取得原始 Worker pidfd。
