# Runtime startup validation gap analysis

当前仍是 validation 阶段。`Production Gates` 仍为 `0/9`，原因是启动链的
authority 证据尚未在真实生产装配中形成完整、可回放的 Launch Receipt。

## 已经验证

| 层 | 结果 | 证据边界 |
| --- | --- | --- |
| pidfd 兼容 | 目标机 Ubuntu 24.04 / kernel 6.8 使用 `anon_inode:[pidfd]` + `fdinfo Pid/NSpid` 可运行 | 只证明句柄识别与存活检查，不证明生产启动 |
| Node adapter | `SOCK_SEQPACKET`、严格 JSON、`SCM_RIGHTS` 三 descriptor、target digest、operation-bound policy 回包 | helper 不满足 contract 时 fail-closed |
| CRI validation helper | 目标机 k3s containerd 创建 `model-runtime` 与独立 `stage-worker-agent`，返回两个 target 和三个 descriptor | 使用 validation-only policy；observer 仍是实验 custody 进程 |
| cleanup | helper 退出后精确 container/sandbox 在 `/run/k3s/containerd/containerd.sock` 对应 namespace 中消失 | 不等于 Node revoke、journal seal 或 Permit 回收 |
| 本地/交叉构建 | `go test ./...`、`go vet ./...`、Linux amd64 launcher/nodeagent test compile、`git diff --check` 通过 | 交叉编译不替代目标机 native race |

验证 helper 现在支持受控故障注入（`VELA_VALIDATION_SCENARIO`）：

| 场景 | 注入点 | 预期结果 |
| --- | --- | --- |
| `caller-replacement` | 把返回的 Worker owner descriptor 替换为另一 live pidfd | Node 以 kernel identity 拒绝，CRI workload 仍清理 |
| `observer-channel-loss` | descriptor 交接后关闭 observer endpoint | custody/observer 检查失败，receipt 为 failed |
| `policy-response-loss` | 收到 operation-bound policy request 后不回包 | Node 等待失败，receipt 为 failed |
| `helper-timeout` | 控制上下文取消时不把退出伪装成 completed | receipt 明确记录 failed |
| `helper-crash` | 交接后触发可回收 panic | cleanup defer 先执行，receipt 为 failed |

这些开关只用于 validation-only helper。它们验证 fail-closed 和精确 CRI 回收，不构成
生产 helper、Fleet issuer 或九个 Production Gate 的证据。

矩阵 driver 为
[`hack/run-runtime-startup-validation-matrix.py`](../hack/run-runtime-startup-validation-matrix.py)。
它逐场景启动 helper、收取 `SCM_RIGHTS`、执行 policy round-trip（正常场景），然后通过
同一个 `ctr -a <CRI socket> -n k8s.io` 查询验证新建的 Runtime/Worker/sandbox 全部消失，
并把每个 receipt 保存到独立目录。`Node restart` 不在 helper 内伪造，driver 会在总 receipt
中明确标记为 `external-driver-required`。

2026-09-12 已在目标机实际运行完整六场景矩阵；结果和逐场景 receipt 见
[`runtime-startup-validation-matrix-evidence-2026-09-12.md`](runtime-startup-validation-matrix-evidence-2026-09-12.md)。
正常场景为 `completed`，五个受控故障场景均为 `failed` 且 `cleanup_verified=true`。

目标 Linux 主机上的调用形态如下（镜像和 helper 路径必须使用该主机已验证的固定值）：

```bash
python3 hack/run-runtime-startup-validation-matrix.py \
  --launcher /tmp/vela-validation-/validation-launcher-final \
  --observer /tmp/vela-validation-/exec-observer \
  --cri-socket /run/k3s/containerd/containerd.sock \
  --sandbox-image <local-sandbox-image> \
  --runtime-image <runtime-image> \
  --worker-image <worker-image> \
  --output /tmp/vela-runtime-startup-matrix
```

## 还缺什么

1. validation helper 的六类故障 receipt：正常、caller replacement、observer channel loss、
   policy response loss、helper timeout/crash 已在目标机完成，逐场景 receipt 均证明拒绝或
   完成、精确 CRI cleanup 和无 Permit。剩余的 `Node restart` 仍需外部 driver 执行，不能由
   helper 自己模拟。
2. validation helper 尚未绑定到真实 Runtime 的连续执行链；当前 observer 是外部实验进程，
   不能声称 `CRI task → observer → Fleet → journal → ModelRuntime` 连续性。
3. Node adapter 尚未在真实 signed plan、Kubernetes reader、Fleet reservation、journal owner
   和 ModelRuntime CPU Job 的同一次 composition root 中完成端到端 receipt。
4. 生产 helper 仍缺少：root ownership、Runtime/Worker 创建、精确 Pod digest、原始 pidfd、
   observer endpoint、计划 Runtime GID 发布和真实 policy issuer。validation policy 不能进入生产。
5. Node restart 的恢复语义仍需证明“只读历史，不重建 owner/custody/grant/permission”；
   numeric PID、旧 receipt、WorkerInstance reporter 都不能替代原始句柄。

## 推荐收口顺序

先把六类 validation 负面场景和 cleanup receipt 做完，再把同一 helper 接到 Node composition
root 的真实 signed-plan/Fleet/journal/ModelRuntime CPU Job；随后实现生产 helper、listener GID
安全发布和 policy issuer，最后逐个生成九类 Production Gate receipt。任何一步缺 receipt 都保持
`0/9`，避免把 focused smoke test 当成生产闭环。
