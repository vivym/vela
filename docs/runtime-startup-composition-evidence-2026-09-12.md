# Runtime startup composition native evidence

本 receipt 记录当前工作树在 `marslab@100.111.196.116` 的隔离 native runner 结果。
没有修改目标机原有 checkout、systemd、Kubernetes workload 或 CRI 状态；runner 使用
临时 `/tmp` source tree、固定 builder 和一次性 Docker image。

环境：

```text
host: marslab-server
kernel: 6.8.0-137-generic
architecture: linux/amd64
Docker: 29.1.3
containerd: 2.2.1
runc: 1.3.4
image: sha256:7888a156d3cc2990d871ab6be1784d362953974b72e6fff89ec3cde802a1c007
```

执行入口为当前仓库的 `hack/run-remote-runtime-cli-native.sh`，临时关闭 Go VCS
stamping 以适配 Docker 挂载的临时 Git 工作树；编译仍使用固定 `golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1`，并启用
`-race`、静态链接和 `CGO_ENABLED=1`。

结果：

```text
top-level PASS: 44
FAIL: 0
SKIP: 0
WARNING: DATA RACE: 0
runner result: Actual remote Runtime CLI checks passed
```

选择集覆盖 startup authority source validation、reservation/orchestration、server/
coordinator、retained caller、observer custody、journal observation、grant activation、
remote CLI 和 bootstrap publication。原始日志在同目录的 `native.log`，本次 source patch
SHA-256 为：

```text
64c14e67a141fed04b0f2fa46ae9445ba15117597a6fbad2d9d33ce5193a8ac4
```

该 receipt 证明当前 startup composition 的 Linux native focused 选择集通过；它不把
实验 `exec-observer` 提升为 production launcher，也不提升九个 Production Gates。

本轮新增 Node 内置 helper adapter：启动时校验 root-owned helper，使用 inherited FD 3
的 `SOCK_SEQPACKET` 控制通道，严格解析首帧和历史 `SCM_RIGHTS` 中的两个 pidfd 与 observer
endpoint，并在 Fleet reservation 后发送 operation/request-bound policy 请求。当前协议已升级为
四个 descriptor；目标机尚未
安装实现该 wire contract 的生产 helper，因此这部分只完成了本地构建与 fail-closed 路径
验证，Production Gates 仍保持 `0/9`。

## validation-only CRI cleanup

随后在同一目标机以 `VELA_VALIDATION_ONLY=1` 重跑一次真实 CRI helper。helper 创建了实际
的 containerd sandbox/container，并返回历史三个 `SCM_RIGHTS` descriptor；Node 侧控制通道
收到 `SIGTERM` 后，context-aware receive 正常退出，helper 使用独立 cleanup context 完成
`StopContainer`、`RemoveContainer`、`StopPodSandbox` 和 `RemovePodSandbox`。退出码为 `0`，
精确的 container/sandbox ID 在 containerd 中均已不存在，任务列表也不再包含该 workload。
原始短 receipt 见同目录的 [cri-cleanup.log](evidence/runtime-startup-composition-2026-09-12/cri-cleanup.log)。

这关闭了 validation helper 的 CRI 生命周期回收缺口，但仍只证明 validation-only helper
和 Node wire adapter 的组合。它没有接入 signed plan/Kubernetes reader/Fleet reservation/
Node journal/ModelRuntime 的同一生产装配，因此 Production Gates 仍为 `0/9`。

## validation-only Runtime/Worker handoff

修复 Worker 镜像/命令配置后，在同一目标机的 k3s CRI namespace 中导入固定测试镜像，
重新运行 helper。请求返回了两个不同的 CRI target：`model-runtime` 和
`stage-worker-agent`，并通过单个 `SCM_RIGHTS` 消息返回三个 descriptor；随后用同一
`operation_id`/`request_digest` 完成 policy round-trip。helper 正常退出并原子写出
[validation-normal-receipt.json](evidence/runtime-startup-composition-2026-09-12/validation-normal-receipt.json)，
其中 `validation_only=true`、`outcome=completed`，且同时记录 Runtime/Worker target。
原始交互输出见
[validation-normal.log](evidence/runtime-startup-composition-2026-09-12/validation-normal.log)。

退出后用 CRI 对应的 containerd 地址 `/run/k3s/containerd/containerd.sock` 查询，两个
container ID 和 sandbox ID 均不存在。此次运行仍然是 validation-only：observer 是外部
实验 custody 进程，未证明 signed plan、Fleet reservation、Node journal 或
ModelRuntime Permit 的同一生产链路；完整故障矩阵 receipt 另见
`runtime-startup-validation-matrix-evidence-2026-09-12.md`，其中 `Node restart` 仍为
`external-driver-required`。

代码层面已为 driver 增加 `VELA_VALIDATION_SCENARIO` 故障注入：
`caller-replacement`、`observer-channel-loss`、`policy-response-loss`、
`helper-timeout` 和 `helper-crash`。注入路径均保持独立 cleanup，并将 receipt 标成
`outcome=failed`。完整目标机矩阵已经执行，结果见
[runtime-startup-validation-matrix-evidence-2026-09-12.md](runtime-startup-validation-matrix-evidence-2026-09-12.md)。
这仍只是 validation harness，不能提升本 receipt 的证据等级；`Node restart` 仍必须由外部
driver 执行，不能由 helper 环境变量替代。
