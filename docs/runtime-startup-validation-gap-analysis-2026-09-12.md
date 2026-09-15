# Runtime startup validation gap analysis

一次性闭环状态与验收判据见
[`runtime-startup-closure-status-2026-09-12.md`](runtime-startup-closure-status-2026-09-12.md)。

当前仍是 validation 阶段。`Production Gates` 仍为 `0/9`，原因是启动链的
authority 证据尚未在真实生产装配中形成完整、可回放的 Launch Receipt。

## 已经验证

| 层 | 结果 | 证据边界 |
| --- | --- | --- |
| pidfd 兼容 | 目标机 Ubuntu 24.04 / kernel 6.8 使用 `anon_inode:[pidfd]`；不可见 legacy identity 由 root-owned host broker 比较 | broker 已安装并 enabled，socket 权限、restart、replacement-safe cleanup 和真实跨 namespace `same/different/nested-same/wrong-gid` 均通过；仍未形成真实 Node startup receipt |
| Node adapter | 本地 focused tests 覆盖 `SOCK_SEQPACKET`、严格 JSON、`SCM_RIGHTS` 四 descriptor、target digest 和 operation-bound policy 解析 | v2 矩阵 driver 直接启动 helper，未运行 Node adapter；必须由 composition harness 补齐 live handoff |
| CRI validation helper | 目标机 k3s containerd 创建 `model-runtime` 与独立 `stage-worker-agent`，返回两个 target、Runtime/Worker 原始 pidfd 和 observer endpoint | 使用 validation-only policy；observer 仍是实验 custody 进程 |
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
正常场景为 `completed`，五个受控故障场景均为 `failed` 且 `cleanup_verified=true`。正常场景的
policy reply 仍是 synthetic round-trip，`journal_id`/`reservation_digest` 不绑定 Fleet
reservation 或 Node journal grant。
在部署 broker 后又以相同 socket mount contract 重跑了一次，receipt 见
[`runtime-startup-validation-matrix-2026-09-12-broker`](evidence/runtime-startup-validation-matrix-2026-09-12-broker/)。

随后使用 launcher protocol v2（四 descriptor）在同一目标机重跑，最新 receipt 见
[`runtime-startup-validation-matrix-2026-09-13-v2`](evidence/runtime-startup-validation-matrix-2026-09-13-v2/)。
六个场景的 `reply.version=2`、`fd_count=4`、精确 cleanup 和空
`validation_errors` 均通过；policy channel 仍为 wire version `1`。

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

legacy invisible pidfd 的兼容 broker 由 `cmd/vela-pidfd-broker` 提供。部署时必须由 root
启动，并设置 `VELA_PIDFD_BROKER_SOCKET=/run/vela/pidfd-broker.sock` 与
`VELA_PIDFD_BROKER_RUNTIME_GID=<Runtime GID>`；它只接收两个 `SCM_RIGHTS` pidfd，socket
为 root-owned `0660`、group 为 Runtime GID，父目录链必须 root-owned 且不可写。Runtime 的
bootstrap 通过 `pidfd_broker_socket` 字段把路径冻结给 Journal transport 和 Backend gate。
Worker remote journal 已将 broker path 设为强制配置；未配置 broker 时在 Node/Worker 配置
阶段拒绝，运行中 broker 不可用时保持严格 fail-closed，不能用 numeric PID 补救。

## 还缺什么

### 一次性剩余问题清单（2026-09-13）

当前代码与 validation 证据已经把协议、pidfd 兼容、broker、CRI cleanup、journal
mount、授权持久化和 release artifact graph 这些内部问题收敛完。剩余项都是需要外部
生产输入或真实环境回执的闭环门槛：

| 阻塞项 | 当前状态 | 完成判据 |
| --- | --- | --- |
| 真实 signed plan 与 Pod | 代码已校验签名、digest、Runtime/Worker identity；目标机没有可用的 production plan、approved image entrypoint 和 kubeconfig | `vela-runtime-launcher` 在目标机以 `validation_only=false` 创建真实 Pod，并生成可回放 target/pidfd receipt |
| 生产 authority 链 | issuer、Fleet reservation、authorization replay 和 Node 校验已实现；缺 production issuer key、Fleet authorization public key、PostgreSQL migration receipt | 同一 `operation_id + request_digest + runtime incarnation` 串联 reservation、authorization、journal grant 和 Permit |
| Node composition root | 启动前 journal/bootstrap、launcher handoff、owner/custody、reservation 顺序已接线；尚未由真实 Node service 运行一遍 | 单次 Launch Receipt 同时包含 caller、CRI、observer、Fleet、journal、Worker mutation、ModelRuntime Permit |
| 联合恢复与长期边界 | 各模块已有 focused/native tests；尚无真实 composition 的故障恢复和 sustained resource receipt | replacement、observer/policy loss、timeout/crash、Node/broker restart、revoke/cleanup 及资源阈值全部有 typed receipt |
| release 安装闭环 | `runtime-startup-provisioning.json` 已生成并可校验；尚未接入真实 production BuildPlan 并在目标机安装/reload | canonical bundle 的 source/image/config digest、migration、systemd enable 和 rollback receipt |

这些门槛依赖目标机和受信任部署资料，不能用 validation helper 或 synthetic Pod 填充。
在它们全部有回执前，`Production Gates` 保持 `0/9`；本轮 v2 矩阵只证明 validation
边界，不改变这个结论。

本轮已补齐四处验证实现接线：Fleet reservation 可以使用配置的 Ed25519 private key
生成 authorization，Node 会用 trusted public key 校验并原子发布 authorization，issuer
会在发送 reply 前写入 durable reply cache，Fleet authorization 现在也有不可变的
`runtime_startup_authorizations` 表和 replay lookup；reply cache、authorization signer
和 reservation replay 已覆盖 issuer restart、request digest 变化、expiry 拒绝和字节恒等
的 focused tests。数据库迁移仍需在真实 Fleet PostgreSQL 部署上执行并纳入 release
migration receipt。

验证阶段的边界已经固定：不把 validation helper 复制改名后接入 Node，也不生成伪造的
production policy receipt。当前保留独立的 `cmd/vela-runtime-launcher`，它已经实现生产
wire contract 的校验和 CRI 创建代码，但在目标机缺少真实 signed Pod、trusted kubeconfig
和 observer provisioning，因此仍不能被称为 production closure。

1. validation helper 的六类故障 receipt：正常、caller replacement、observer channel loss、
   policy response loss、helper timeout/crash 已在目标机完成，逐场景 receipt 均证明拒绝或
   完成、精确 CRI cleanup 和无 Permit。剩余的 `Node restart` 仍需外部 driver 执行，不能由
   helper 自己模拟。
2. validation helper 尚未绑定到真实 Runtime 的连续执行链；当前 observer 是外部实验进程，
   不能声称 `CRI task → observer → Fleet → journal → ModelRuntime` 连续性。
3. Node adapter 尚未在真实 signed plan、Kubernetes reader、Fleet reservation、journal owner
   和 ModelRuntime CPU Job 的同一次 composition root 中完成端到端 receipt。
4. 生产 helper 的代码路径已具备 root ownership、Runtime/Worker 创建、精确 Pod digest、
   原始 pidfd 和 observer endpoint 校验；launcher 内置 attached observer 会使用本次
   Runtime 原始 pidfd 做 ptrace custody，并在确认 attach 后释放 Runtime wrapper。仍缺目标机
   的真实 provisioning、OCI image config / approved entrypoint 证明、计划 Runtime GID 发布
   和同一次启动中的真实 policy issuer。validation policy 不能进入生产。
5. pidfd broker 的部署、Runtime GID/socket 权限、socket replacement、restart 和跨 namespace
   receipt 已完成；剩余是把同一 broker 接入真实 Node composition，并证明 Node/Worker journal
   mutation receipt 与 startup grant 的连续性。
6. Node restart 的 ledger 语义已在目标机十个 crash boundary 通过，证明只读历史、不重建
   owner/grant/permission；仍需把同一语义放进真实 Node composition，并覆盖 observer/journal
   socket 与 CRI workload 的联合恢复。numeric PID、旧 receipt、WorkerInstance reporter
   仍不能替代原始句柄。
7. Node-owned Worker journal 的 typed RPC、12 KiB 分片、分页 snapshot digest 和
   signed Pod 的 socket-parent read-only mount 已实现；带该 mount 的完整六场景
   validation matrix 已在目标机通过并保存 receipt。目标机另已通过 root-native
   `TestWorkerJournalRPC`，证明 retained original pidfd、pre-activation rejection 和
   activation 后 mutation 的真实 Unix transport；仍未生成真实 Node composition 的
   journal mutation receipt，也不等于 signed plan/Fleet/CRI/ModelRuntime composition。
8. Release bundle 现在提供显式 `runtime_startup` artifact graph，且新增
   `build-runtime-startup-packages` 可生成 launcher、broker、issuer、两个 systemd units
   和 `runtime-startup-provisioning.json`。provisioning contract 已可被 bundle 引用并校验
   socket parent、bootstrap directory、key files 和必需 env keys；仍需把它接入真实
   production build plan，并完成 canonical bundle reload 和目标机 source/image/config digest receipt。

## 推荐收口顺序

六类 helper receipt 和 package publication checks 已完成。下一步应先实现 validation
composition harness，把临时 signed fixture、fake issuer/Fleet/journal、CPU ModelRuntime 和
真实 Node adapter 放进同一个 operation；然后在该 harness 中重放故障和 Node restart。之后再
进入 production helper provisioning、canonical bundle install 和九类 Production Gate receipt。
任何一步缺少同范围 typed receipt，都保持 `0/9`。

### 2026-09-12 launcher contract correction

validation-only launcher 已修正两个会污染验证结论的偏差：observer socketpair 在
`CreateContainer`/`StartContainer` 之前建立；Runtime/Worker 容器由同一 launcher 二进制中的
wrapper 在目标进程自身启动边界调用 `pidfd_open(getpid())`，经挂载的 `SOCK_SEQPACKET`
offer socket 用 `SCM_RIGHTS` 把原始 pidfd 交给宿主，再 `exec` 实际命令。宿主不再从 CRI
numeric PID 调用 `pidfd_open`。这验证了“创建边界取得并交接句柄”的时序，但 wrapper、挂载
和 policy issuer 仍是 validation-only，不能代替生产 helper、Fleet authority、Node restart
或完整 CPU Job。

目标机已用修正后二进制完成完整六场景矩阵：normal 为 `completed`，五个受控故障为
`failed`，每个场景均收取两个进程 pidfd 和 observer endpoint，并验证退出后的精确
container/sandbox cleanup。offer receiver 的 native 负面测试同时拒绝 wrong task、wrong
UID、regular-file 和 different-process descriptor。该矩阵仍只覆盖 validation helper
边界，不能替代 Node composition 或 Production Gate。

### 2026-09-12 production launcher smoke result

修复 cgroup parent、pidfd-offer wrapper dispatch、`SO_PASSCRED`/`SO_PASSPIDFD`、启动
deadline 和失败回收后，独立 production launcher 已在目标机 RKE2 CRI 完成协议 smoke：
`handoff_verified=true`、`cleanup_verified=true`、`elapsed=11.525s`。该 run 使用
synthetic Pod/manifest，只证明 helper 的真实 CRI 创建、原始 descriptor 交接和 cleanup；
不证明 Node/Fleet/ModelRuntime composition。早先 Docker/RKE2 image store 隔离和 RKE2
重启期间的 `error reading from server: EOF` 已记录为部署诊断，不能作为最终 launcher
结论。详细证据见
[`runtime-startup-production-launcher-smoke-2026-09-12.md`](evidence/runtime-startup-production-launcher-smoke-2026-09-12.md)。

本轮在同一目标机重新执行 source-matched launcher，连续 3 次通过
`handoff_verified=true`、`observer_handshake_verified=true`、`cleanup_verified=true`；
launcher/observer SHA-256 为
`a621a2c774996d4d85695d6bd3093e23616a9410c0462cb0c7d9bdee7243ea66`。该结果仍是
synthetic CRI smoke，不能替代 Node/Fleet composition receipt。
