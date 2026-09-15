# Production launcher smoke evidence (2026-09-13, protocol v2)

这是目标机上的 synthetic CRI helper smoke，不是 Node/Fleet/ModelRuntime
Launch Receipt，也不提升 `Production Gates`。

- Host: `marslab@100.111.196.116`
- OS/kernel: Ubuntu 24.04, `6.8.0-137-generic`
- CRI socket: `/run/k3s/containerd/containerd.sock`
- Request/reply protocol: version `2`
- Handoff descriptors: `4`（Runtime pidfd、Worker pidfd、observer pidfd、observer socket）
- Launcher/observer SHA-256: `a0db97ccd5ee8e9f98bf972a628fe161758eeb7d9a67c098de4fff899f54fbd3`
- Result: `passed=true`, `handoff_verified=true`, `observer_handshake_verified=true`,
  `cleanup_verified=true`
- Elapsed: `6.721s`

本次使用独立 root-owned launcher 路径和临时 bootstrap 目录。Runtime synthetic
command 满足 production `serve-remote --bootstrap-file` 参数契约，同时由 busybox
shell 保持进程存活。smoke 完成 observer ancestry、Runtime `TracerPid`、四个
descriptor 类型和 close-on-exec、CRI running 状态及退出后的 container/sandbox
精确清理检查。

本次运行修正并验证了两个 driver 漂移：request/reply 必须都是 protocol v2；
Runtime Pod 必须提供 bootstrap 参数和已发布的 bootstrap 目录。launcher 的严格
校验保持不变。另修正 observer 释放 gate 时 `PTRACE_CONT=ESRCH` 与异步
`SIGCONT` 的竞态：只有原始 pidfd 同时失效才判定目标死亡。

完整 JSON receipt：
[`runtime-startup-production-launcher-smoke-2026-09-13-v2.json`](runtime-startup-production-launcher-smoke-2026-09-13-v2.json)

同一 source-matched binary 随后连续两次重复运行均通过，分别耗时 `6.699s` 和
`6.605s`；汇总 receipt 见
[`runtime-startup-production-launcher-smoke-2026-09-13-v2-repeat.json`](runtime-startup-production-launcher-smoke-2026-09-13-v2-repeat.json)。

该证据仍只覆盖 production launcher 的 CRI/observer helper 边界；command-level
Node composition、真实 Fleet authorization、ModelRuntime Permit 和 release
install 仍未闭合。
