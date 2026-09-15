# Runtime startup validation matrix v2 rerun

这组 receipt 于 2026-09-12 在 `marslab@100.111.196.116` 的 Ubuntu 24.04、kernel
`6.8.0-137-generic` 上生成。使用 k3s CRI socket
`/run/k3s/containerd/containerd.sock`、已部署的 `/run/vela/pidfd-broker.sock`，以及
固定的本地 `busybox` validation image。

当前 launcher handoff wire version 为 `2`，每次 `SCM_RIGHTS` 交接包含四个 descriptor：
Runtime pidfd、Worker pidfd、observer pidfd 和 observer socket。policy channel 仍使用
wire version `1`。六个场景全部通过：`normal=completed`，其余五个受控故障为
`failed`，每个场景均 `cleanup_verified=true` 且 `validation_errors=[]`。

这仍然是 validation-only 证据，不能证明真实 signed Node composition、Fleet reservation、
ModelRuntime Permit、Node restart 或 Production Gates；当前 `production_gates` 仍为 `0/9`。

`matrix.json` SHA-256：
`166b99195d8806b606076804a5efc39434097efe1a4bdc1ff9ed6cccb9f264ca`。
