# Runtime startup target host status

在 `marslab@100.111.196.116` 上进行了只读 inventory，并在 `/tmp` 临时目录准备了当前
source-matched tree（没有修改 `/home/marslab/vela-test-24a2c31`、systemd、Kubernetes
workload 或 CRI 状态）。

主机为 `marslab-server`。可执行文件中只发现历史 `exec-observer`/`exec-probe` 和实验
产物，没有 Node→Runtime/Worker production launcher。`vela-evidence` 下的 observer 是
实验 fixture，不能作为生产 authority source。

截至 2026-09-12，主机上的 Docker/containerd 服务正常，Docker registry mirror 为
`https://docker.1ms.run/`，但没有安装 `vela-node-agent`、Kubernetes CLI 或生产 launcher
helper；当前运行容器为空。因此无法在该主机上生成真实 Pod/CRI/Fleet/ModelRuntime
startup receipt。

临时 checkout 的 focused command 为：

```text
go test ./cmd/vela-node-agent ./internal/nodeagent
```

测试未执行，因为目标机 `/usr/bin/go` 为 `go1.22.2`，而当前 `go.mod` 要求 Go `1.26.0`：

```text
go: go.mod requires go >= 1.26.0 (running go 1.22.2; GOTOOLCHAIN=local)
```

该结果是工具链前置条件不足，不是 pidfd 或 startup 代码失败。目标机没有被升级，也没有
下载或安装新的 Go toolchain。

为覆盖目标 kernel，随后在本地交叉编译了 `GOOS=linux GOARCH=amd64 CGO_ENABLED=0`
的 `internal/nodeagent` 测试二进制，并通过压缩标准输入流临时传到目标机执行。以下测试
实际通过：

```text
TestRuntimeCallerProcessParser
TestRuntimeStartupAuthorizationEvidenceIsOperationBound
TestRuntimeStartupOrchestrationRequiresEveryAuthorityInput
```

`TestRuntimeStartupLedgerRestartDoesNotReconstructOwner` 在目标机 root 执行上下文中按测试
设计跳过（需要不同的 non-root UID），不是失败。二进制和临时目录已清理。

2026-09-12 追加执行 helper adapter 的 `SCM_RIGHTS`、严格 JSON 和 root-owned helper
边界测试，目标机结果为 `PASS`。这只验证 Node 侧 wire adapter 和 kernel 能力；因为目标机
仍无生产 helper、Kubernetes/Fleet 环境，未生成真实 startup receipt，Production Gates
仍为 `0/9`。

同日 validation-only helper 又通过真实 CRI 创建/回收了一次临时 sandbox/container；控制
通道取消后 helper 正常退出，精确 workload ID 已从 containerd 消失，临时目录和 socket
已清理。该结果见 `docs/evidence/runtime-startup-composition-2026-09-12/cri-cleanup.log`，
不改变“目标机没有生产 helper、Production Gates 为 `0/9`”的结论。
