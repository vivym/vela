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
`{version, validation_only:false, target, worker_target, fd_count:4}`，并在同一个 `SCM_RIGHTS` 消息中按顺序附带
Runtime pidfd、Worker owner pidfd、observer pidfd 和 observer socket endpoint。
`RuntimePIDFD`、`WorkerOwnerPIDFD`、`ObserverPIDFD`、observer socket endpoint。前三个 descriptor 必须
通过 Node 的 pidfd identity/liveness 校验，第四个必须是 Unix socket。
Node 在接收后为四个 descriptor 强制设置 `FD_CLOEXEC`，无法设置时立即拒绝。
validation helper 必须回传 `validation_only:true`；生产 Node 收到该标志时立即拒绝并
关闭本次句柄，避免 root-owned 验证程序因错误配置进入生产启动链。

Node 同时保留启动 helper 自己的原始 pidfd。direct-child observer 必须是该 helper 的直接
子进程，Node 在接收 custody 时将 observer 的 `PPid` 与 helper pidfd 的内核 `Pid` 绑定。
production attached observer 使用同一 helper 创建的 tracer；Runtime 必须处于 ptrace stop，且
`/proc/<runtime>/status` 的 `TracerPid` 必须等于 observer pidfd 对应的 pid，此模式不要求
Runtime 的 `PPid` 等于 observer。两种模式都只使用内核 pidfd 和 `/proc` 身份字段，不从
numeric PID 重开句柄。

Fleet reservation 成功后，Node 通过独立的 root-owned
`VELA_NODE_AGENT_RUNTIME_POLICY_ISSUER_SOCKET` 请求外部 issuer。请求绑定
`operation_id`、`journal_id`、`request_digest` 和完整 `reservation_digest`；issuer 只能在
消费 Fleet 预先签名且一次性的 authorization 文件后返回同样绑定的
`evidence_digest`、UTC `issued_at`/`expires_at` 和 Ed25519 签名。Node 执行统一的五分钟
窗口、时间顺序和公钥校验。issuer 退出、通道关闭、target 不合法或 policy 回包不匹配
都会 fail-closed，并终止本次 helper 及其未转移的进程。
Node 不接受仅由调用方提供的裸 authorization digest；没有外部 issuer 的有效回包就不会
创建 journal write grant，也不会向 ModelRuntime 返回 Permit。旧的 helper 内置 policy
frame 仅保留给历史测试适配器，不能作为生产 authority。

## 当前实现状态

仓库已经实现 contract 的 Node 侧 composition、受保护 helper adapter、失败清理，以及
production launcher 内置的 attached observer mode。生产部署可以将
`VELA_RUNTIME_LAUNCHER_OBSERVER_PATH` 指向同一份 source-matched launcher binary；该 mode
通过继承的 Runtime 原始 pidfd attach 到本次 CRI Runtime，再完成 custody handshake。目标主机
现有 `exec-observer` 仍属于实验 fixture，不能继续配置为生产 observer。

## Validation-only helper boundary

验证阶段可以提供单独的 `validation-only` helper，但它必须使用独立的配置和状态目录，
并在 receipt 中标记 `validation_only=true`。它可以使用目标机现有 CRI socket 创建一次性
PodSandbox 和 `model-runtime` container，创建 observer socketpair，返回本次创建产生的
真实 target、observer creation pidfd 和 observer endpoint；Node shutdown 时必须停止并
删除它创建的精确 workload。生产 helper 不能读取历史 target、numeric PID、旧 receipt
或以 policy response 代替 CRI/进程证据，也不能被配置到生产默认路径。

当前 validation helper 在容器内 wrapper 启动边界调用 `pidfd_open(getpid())`，通过
`SCM_RIGHTS` 把原始句柄交给宿主，再 `exec` 实际命令；宿主只用 CRI task PID 做关联
核对，不从 numeric PID 重开句柄。这关闭了旧 validation helper 的句柄交接偏差，但
仍未证明 production observer、policy issuer 或完整 Node composition。

validation helper 的 Worker 必须显式使用 `VELA_VALIDATION_WORKER_IMAGE` 和
`VELA_VALIDATION_WORKER_COMMAND`（简单 argv，禁止 shell 展开）；默认值不能假设 runtime
镜像内存在 `/pause`。receipt 必须同时记录 Runtime 与 Worker target，且在 helper 退出后
用实际 CRI socket 对应的 containerd namespace 核对两个 container 和 sandbox 均已消失。

Worker journal socket 的 validation mount contract 也必须闭合：签名 Pod 的
`stage-worker-agent` 必须提供 canonical absolute
`VELA_WORKER_JOURNAL_SOCKET`；helper 将该路径的 parent directory 以 read-only host
mount 映射到同一 container path，并把相同值注入 Worker。Node 在 launcher 返回后才在
该 host directory 创建 root-owned `0660` socket，因此不能把 socket 文件本身作为
启动前的 bind mount；目录挂载保证 Worker 能看到随后创建和替换的 socket，同时不暴露
Node journal 文件目录。缺少该环境、路径非 canonical、或 mount 与 signed Pod 不一致时，
validation helper 必须在创建 Worker 前 fail-closed。

目标机不支持 `pidfs` 时，签名 Pod 还必须声明 canonical
`VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET`。helper 将 broker socket 的 parent directory
以 read-only host mount 映射到 Worker，并注入相同路径；broker socket 本身不能在启动前
作为单文件 bind mount。缺少 broker、目录不可信或 Pod 声明不一致时，必须在创建 Worker
前 fail-closed，不能退回 numeric PID。

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
