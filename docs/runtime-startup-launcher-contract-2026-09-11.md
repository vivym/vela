# Runtime startup launcher contract

`nodeagent.RuntimeStartupLauncher` 是 Node Agent 与平台进程启动器之间的唯一交接点。它不是
Kubernetes/CRI observer，也不是 WorkerInstance reporter。后两者只能观察已经存在的
工作负载，不能产生本次启动所需的 owner/custody。

## 调用

```go
Launch(context.Context, *nodeagent.RuntimeLaunchPlan, startupSocketPath) (nodeagent.RuntimeStartupLaunch, error)
```

Node 在调用前已经验证 signed launch plan、journal、Kubernetes、CRI、Fleet、startup
listener 和独立 startup ledger。launcher 必须使用这次调用的 plan 和 socket path；不能
读取历史 reservation、receipt、PID 或另一个配置文件来替代它们。

## 成功返回

返回值必须同时包含：

- `WorkerOwnerPIDFD`：Node 本次创建/取得的 Worker namespace owner 原始 pidfd，设置
  `FD_CLOEXEC`，且在返回前仍 live；Node 会把它与已认证 caller pidfd 做 kernel identity
  比较。
- `ObserverPIDFD`：Node 本次创建的 observer 原始 pidfd；不能从 numeric PID 重开。
- `ObserverConn`：Node 在 observer 启动前创建的 Unix socketpair endpoint；observer
  必须在目标 executable 运行前关闭其 endpoint。
- `Target`：本次启动对应的完整 CRI container/sandbox/Pod target；字段必须来自本次
  launcher 创建结果，不能由 caller payload 提供。
- `Policy`：独立的 `RuntimeStartupAuthorizationPolicy`，在 Fleet reservation 后
  根据 operation、request digest、runtime incarnation 和 continuity evidence 签发
  operation-bound evidence。
- `Close`：释放 launcher 自己持有的状态，并终止尚未转移给 Node custody 的进程；每次
  启动失败和 Node shutdown 恰好调用一次。

任何一个字段缺失、重复使用或由历史状态重建，Node 都会拒绝启动并关闭所有句柄。

## 生命周期

launcher 返回后，Node 先接收并预认证 caller，再验证 `WorkerOwnerPIDFD`，然后调用
`RetainNamespaceOwner` 和 `ReceiveRuntimeObserverCustody`。成功转移后，Node 负责关闭
所有句柄；launcher 不得在返回后悄悄替换进程、socketpair 或 pidfd。Node shutdown 的
顺序是 revoke/stop admission、停止 listener、关闭 custody/owner、关闭 observer/Fleet/
journal/ledger。

启动失败、caller 超时、observer handshake 失败、reservation response 丢失、policy
拒绝或 signal cancellation 都必须返回错误并保持 fail-closed。不得把 `Target`、PID、
digest 或 receipt 序列化后作为下一次启动的替代 authority。

## Node 内置 helper adapter

Linux Node 现在通过 `VELA_NODE_AGENT_RUNTIME_LAUNCHER_PATH` 启动一个 root-owned
helper，并把控制 socket 作为 inherited FD 3 传入。控制通道是
`AF_UNIX/SOCK_SEQPACKET`，所有 frame 都是最多 64 KiB 的严格 JSON，未知字段和尾随
数据都会拒绝。首个 frame 为
`{version, manifest, expected_pod, expected_pod_digest, startup_socket}`；其中
`expected_pod` 是 verified plan 的 canonical Kubernetes Pod JSON，digest 是其
SHA-256。helper 必须逐字段核对 Pod 与自身 CRI 创建请求，不能让环境变量或 caller
payload 覆盖这些字段。helper 必须回传
`{version, target, fd_count:3}`，并在同一个 `SCM_RIGHTS` 消息中按顺序附带
`WorkerOwnerPIDFD`、`ObserverPIDFD`、observer socket endpoint。前两个 descriptor 必须
通过 Node 的 pidfd identity/liveness 校验，第三个必须是 Unix socket。
Node 在接收后为三个 descriptor 强制设置 `FD_CLOEXEC`，无法设置时立即拒绝。

Node 同时保留启动 helper 自己的原始 pidfd。observer 必须是该 helper 的直接子进程；
Node 在接收 custody 时将 observer 的 `PPid` 与 helper pidfd 的内核 `Pid` 绑定。旧的
直系 Node 子进程测试入口仍只允许 `PPid == Node`，不能用孙进程绕过这个检查。

Fleet reservation 成功后，Node 在同一控制通道发送
`{version, operation_id, request_digest}`。helper 必须返回相同 operation/request 绑定的
`evidence_digest`、UTC `issued_at`/`expires_at`；Node 再执行统一的 5 分钟窗口和时间顺序
校验。helper 退出、通道关闭、descriptor 数量不符、target 不合法或 policy 回包不匹配
都会 fail-closed，并终止本次 helper 及其未转移的进程。
Node 不再接受仅由调用方提供的裸 authorization digest；没有 helper policy 回包就不会
创建 journal write grant，也不会向 ModelRuntime 返回 Permit。

## 当前实现状态

仓库已经实现 contract 的 Node 侧 composition、受保护 helper adapter 和失败清理。生产
部署仍必须提供实现上述 wire contract 的 helper；目标主机现有 `exec-observer` 属于实验
fixture，不能满足该 contract，也不能把它配置为生产 helper。

## Validation-only helper boundary

验证阶段可以提供单独的 `validation-only` helper，但它必须使用独立的配置和状态目录，
并在 receipt 中标记 `validation_only=true`。它可以使用目标机现有 CRI socket 创建一次性
PodSandbox 和 `model-runtime` container，创建 observer socketpair，返回本次创建产生的
真实 target、observer creation pidfd 和 observer endpoint；Node shutdown 时必须停止并
删除它创建的精确 workload。生产 helper 不能读取历史 target、numeric PID、旧 receipt
或以 policy response 代替 CRI/进程证据，也不能被配置到生产默认路径。

当前 validation helper 的 CRI task API 只暴露 numeric task PID，因此 Worker descriptor
由 `pidfd_open(task_pid)` 得到，receipt 和代码注释明确标记为 validation-only。这个路径
不能满足生产 contract；生产 helper 必须在受信任的创建边界取得并交接原始 pidfd，禁止从
numeric PID 重开句柄。

validation helper 的 Worker 必须显式使用 `VELA_VALIDATION_WORKER_IMAGE` 和
`VELA_VALIDATION_WORKER_COMMAND`（简单 argv，禁止 shell 展开）；默认值不能假设 runtime
镜像内存在 `/pause`。receipt 必须同时记录 Runtime 与 Worker target，且在 helper 退出后
用实际 CRI socket 对应的 containerd namespace 核对两个 container 和 sandbox 均已消失。

为验证 Node 的拒绝和回收路径，validation-only helper 支持以下受控
`VELA_VALIDATION_SCENARIO`：`caller-replacement`、`observer-channel-loss`、
`policy-response-loss`、`helper-timeout`、`helper-crash`。这些场景只改变一次性 helper 的
实验行为，并要求 receipt 使用 `outcome=failed`；它们不能被配置到生产 launcher，也不能
把 validation policy 当成 Fleet policy issuer。`Node restart` 必须由外部 driver 在 helper
之外执行，不能通过这个环境变量伪造。

验证闭环至少要分别记录：正常启动、caller replacement、observer channel loss、policy
response loss、Node restart、helper timeout/crash。任一场景若没有明确的 fail-closed 结果，
该 helper 只能用于 adapter smoke test，不能证明 Node→CRI→observer→Fleet→journal→
ModelRuntime 的完整启动链。
