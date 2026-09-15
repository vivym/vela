# Runtime startup closure status

当前仍是 validation 阶段，`Production Gates=0/9` 是正确状态。目标机
`marslab@100.111.196.116` 已证明 Linux 6.8 上的 pidfd 兼容路径可用，但它没有提供
生产级 Kubernetes signed plan、真实 Fleet authorization provisioning 或 ModelRuntime
Permit，因此不能从现有 validation receipt 推导生产闭环。工作区已经有独立的
`cmd/vela-runtime-launcher` 实现和 focused tests，但尚未在目标机完成真实 CRI
composition。

## 已完成并有证据的部分

### Policy transport boundary (2026-09-12)

Node now requires `VELA_NODE_AGENT_RUNTIME_POLICY_ISSUER_SOCKET` and
`VELA_NODE_AGENT_RUNTIME_POLICY_PUBLIC_KEY_FILE` plus the distinct
`VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE` when runtime
startup is enabled. The Linux client authenticates a root peer over `SOCK_SEQPACKET`,
rejects duplicate/unknown/trailing JSON and ancillary descriptors, and
verifies an Ed25519 reply bound to `operation_id`, `journal_id`,
`request_digest`, and the complete reservation digest. The standalone issuer
consumes a Fleet-signed, root-owned authorization file exactly once and caps
the reply lifetime by the authorization expiry. Native tests passed on the
target host's Ubuntu 24.04 / kernel `6.8.0-137-generic`.

This does not create Fleet authorization records. The issuer still requires
an independently provisioned authorization directory and key; no Fleet-side
production provisioning is implied by this transport test.

The validation implementation now has the missing wiring seams: Fleet signs
the reservation authorization with a configured Ed25519 private key, Node
verifies and atomically publishes the signed authorization, and the issuer
persists an exact reply cache before sending a reply. These are implementation
and focused-test results only; they do not constitute a production Launch
Receipt because the target host still lacks a provisioned signed plan and
issuer. The command-level composition now publishes the schema-2 bootstrap
and serves the same read-only Runtime journal endpoint before the startup
handshake; Fleet reservation activates that endpoint in place.

| 边界 | 当前结论 | 证据 |
| --- | --- | --- |
| pidfd 兼容 | `pidfs` 不可用时由 root-owned host broker 接收两个原始 pidfd，比较 kernel identity；不回退 numeric PID | `docs/evidence/pidfd-broker-deployment-2026-09-12/`，目标机 `active/enabled`、`0660`、跨 namespace `same/different/nested-same/wrong-gid` |
| broker 安全 | restart 可恢复；旧 listener 不会删除 pathname replacement | broker replacement/restart tests and deployment receipt |
| Node adapter | `SOCK_SEQPACKET`、strict JSON、Pod digest、四个 `SCM_RIGHTS` descriptor、operation/request-bound policy | `cmd/vela-node-agent/runtime_startup_launcher_linux.go`；现在明确拒绝 `validation_only=true` |
| validation CRI | Runtime/Worker 两个 container、同一 sandbox、创建边界 pidfd offer、精确 cleanup | `docs/evidence/runtime-startup-validation-matrix-2026-09-13-v2/` |
| failure handling | normal=`completed`；replacement、observer loss、policy loss、timeout、crash=`failed` | v2 matrix；driver 现在要求场景与 outcome 一一对应，缺 handoff/descriptor/cleanup/policy 会失败 |
| Node ledger | restart 后只读历史，不重建 owner/grant/permission；十个 crash boundary 已 native 通过 | `docs/evidence/runtime-startup-node-restart-2026-09-12.md` |
| local quality | Go tests、vet、Linux amd64 compile、Python verdict counterexamples、diff check | 当前工作树检查结果 |

## 还缺的生产闭环

### 验证阶段仍需完成的代码闭环

在 `marslab` 临时安装 root-owned `exec-observer`/`exec-probe` 后，Node authority、
coordinator、orchestration 和 observer-custody native 测试组已通过，记录在
`docs/evidence/runtime-startup-nodeagent-native-2026-09-13/`。该证据补足了原先因缺少
observer fixture 而 skip 的原生覆盖，但仍不等于命令级资源装配或生产 Permit。

最新 v2 helper 矩阵没有运行 Node adapter，且正常场景的 policy reply 使用 synthetic
`journal_id`/`reservation_digest`；因此它不能证明同一 operation 已穿过 Node composition
root。当前仍需完成一条可重复的 validation composition harness：使用
临时 signed fixture、临时 Ed25519 issuer、fake journal/Fleet 和 CPU ModelRuntime，在一次
operation 中生成 caller、CRI、observer、journal、grant 和 Permit 的关联 receipt，并在
同一 harness 中重放五类故障及 Node restart。command-level opt-in 入口已加入，但未设置
`VELA_RUNTIME_STARTUP_NODE_IMAGE` 时，现有 `runtime_startup_node_process_test.go` 会
`SKIP`，不能算作该闭环证据。

runtime startup package builder 的发布前检查已经补齐精确 inventory、manifest 引用、
strict JSON、ELF64 amd64、`0755`、unit/env/provisioning contract，并有 mutation negative
tests；这部分不再是剩余阻塞项。

验证阶段不会把 validation helper 配置成生产 launcher。工作区现在包含独立的
`cmd/vela-runtime-launcher`，它发送 `validation_only=false`，并按生产 contract 校验
signed Pod、digest-pinned images、CRI targets、observer custody 和原始 pidfd；
`cmd/vela-validation-runtime-launcher` 仍明确标注 `validation-only`，Node 在协议层拒绝
它进入生产启动链。

这些是有依赖顺序的工作，不能用同一个 validation helper 伪造：

1. **真实 launcher composition**：root-owned helper 和 attached observer mode 已实现，但必须在目标机按
   `docs/runtime-startup-production-deployment-2026-09-12.md` 提供 kubeconfig、observer、
   sandbox image 和 signed Pod，实际创建 Runtime/Worker，核对 signed Pod digest，在创建
   边界取得原始 pidfd，并保持 helper 直接子进程关系。
2. **真实 policy issuer**：Fleet reservation 成功后，必须由受信任 issuer 根据
   `operation_id + request_digest + runtime incarnation + continuity evidence` 签发有效期
   不超过五分钟的 evidence。validation helper 的 hash 回包不能替代它。
3. **同一 composition root**：signed plan、Kubernetes Pod reader、CRI observer、Fleet
   reservation、Node-owned journals、Worker remote journal、ModelRuntime CPU Job 和
   Permit 必须在同一次启动中产生可回放 Launch Receipt。
4. **联合故障与恢复**：在真实 composition 中重放 caller replacement、observer loss、
   policy loss、helper timeout/crash、Node restart；restart 必须同时覆盖 CRI workload、
   observer custody、journal socket 和 ledger，而不是只测 ledger。
5. **长期边界**：持续生命周期期间测量 RSS、FD、goroutine、journal/scratch 增长、重复
   grant、broker restart 和 cleanup 延迟，并固定阈值。当前六场景矩阵是一次性验证，不是
   sustained resource evidence。
6. **release wiring**：broker binary、issuer、production launcher、systemd unit 和两份
   `env.example` 已进入 `runtime_startup` artifact graph；env 中的 Runtime GID、socket
   parent、key、authorization、reply-cache 路径会被 digest 绑定并做必需键校验。当前仍
   没有用真实 production plan 构建并验证一份 canonical bundle，也没有在目标机完成
   bundle reload/install receipt。

## 收口判据

只有以下条件全部满足，才可以开始生成 Launch Receipt 并考虑 Gate：

- 真实 helper 回包 `validation_only=false`，且通过 root/权限、Pod digest、原始 pidfd、
  observer ancestry 和 CRI target 检查；
- 同一 operation 在 Fleet reservation、policy evidence、journal grant、Worker mutation
  和 ModelRuntime Permit 中可通过 digest 串联；
- 六类故障和 Node restart 在真实 composition 中都有明确 failed/failed-closed receipt；
- cleanup、revoke、journal seal、Permit 回收和重复启动防护都有 postcondition；
- 长时间资源边界和目标机 native race 通过；
- release bundle 含 broker 与生产 helper，且验收环境保存 source/image/config digest。

在这些条件完成前，继续保持 `Production Gates=0/9`。九个 Gate（preset certification、
real H3 soak、state/event fault injection、GPU remediation、organization isolation/content
safety、data disaster recovery、release rollback、commercial lifecycle、observability
on-call）还各自需要独立 typed receipt；Runtime startup 证据本身不能替代它们。

## 目标机 production launcher smoke（2026-09-12）

后续 launcher contract 补强已覆盖两类此前未实现的签名 Pod 内容：批准的两个 init
container，以及 CPU/memory 与受限安全上下文。init 镜像在 sandbox 前预检，容器按顺序
运行并要求成功退出；fake CRI 生命周期测试和 source-matched Linux amd64 二进制已在
`marslab` 通过。该变化只扩大了可验证的 Pod 子集，不改变 Production Gates 或真实
composition 的缺口。

修复 cgroup parent、wrapper dispatch、pidfd ancillary socket options、startup timeout 和
partial-start cleanup 后，production launcher 在目标机真实 RKE2 CRI 完成一次协议 smoke（本轮 source-matched binary）：
`handoff_verified=true`、`observer_handshake_verified=true`、`cleanup_verified=true`、`elapsed=6.526s`。该结果使用
synthetic Pod/manifest，证明 helper/CRI/observer/pidfd 边界可运行，但尚未形成真实
Node/Fleet/Worker journal/ModelRuntime Permit 的 Launch Receipt。早先 CRI 重启期间的
`error reading from server: EOF` 已作为诊断历史保留。

`Production Gates` 仍为 `0/9`。

## Native composition validation (2026-09-13)

新增 `internal/nodeagent/runtime_startup_authority_composition_linux_test.go`，在
`marslab` 以 root Node、非 root Runtime fixture 运行成功路径和重复启动保护。该测试真实调用
`NewRuntimeStartupAuthority -> Prepare`，并在同一 operation 中经过 Fleet reservation、临时
authorization policy 和 startup grant。receipt 位于
`docs/evidence/runtime-startup-composition-2026-09-13/`，明确标记
`validation_only=true`；它仍未覆盖命令级 `composeRuntimeStartupAuthority`、生产 Fleet、真实
ModelRuntime Permit 或生产 Launch Receipt。

同日还完成了 source-matched `internal/nodeagent` Docker image 的
`TestRuntimeStartupNodeProcessPostgresTLS`：normal、committed-response-lost 和 missing-ptrace
三种场景均通过，receipt 位于
`docs/evidence/runtime-startup-node-process-2026-09-13/`。该测试补强了 Node/Fleet/TLS/
PostgreSQL 进程级证据，但仍不等于 `cmd/vela-node-agent` daemon composition。

当前 Node orchestration 还提供 `RuntimeStartupOrchestration.CompositionReceipt`：它从 durable
grant attempt 和一次性 coordinator 状态生成自校验 typed receipt，串联
`operation_id`、`request_digest`、`reservation_digest`、authorization digest、grant-attempt
digest 和 Permit outcome。进程级 Fleet helper 已改为调用 `ServeCaller`，由真实 startup
socket 回复 Permit；该证据仍使用 validation fixture，尚未替代命令级 daemon composition
或生产 issuer。

本轮代码又收紧了 launcher wire contract：生产路径现在必须通过同一
`SCM_RIGHTS` 消息交接四个 descriptor（Runtime pidfd、Worker pidfd、observer pidfd、observer
socket），以便 Node 在 Runtime 首次读取 journal 之前建立 owner 和只读 endpoint。此前
记录 `fd_count=3` 的历史 smoke 仍保留为历史证据，不能用于证明当前协议版本；目标机已
使用 source-matched helper 重新生成四-descriptor v2 receipt，见
`docs/evidence/runtime-startup-validation-matrix-2026-09-13-v2/`。

命令级装配也已完成关键时序：Node 在调用 launcher 前创建 Runtime journal listener、记录
unresolved startup intent 并发布 bootstrap；launcher 将 bootstrap 目录作为只读 bind mount
交给 Runtime。launcher 返回 Runtime pidfd 后，Node 才建立 owner、observer custody 和
Worker owner，随后由同一个 journal endpoint 在 reservation 后激活 grant。这样 Runtime
首次 journal read、startup handshake、Fleet reservation 和 Permit 不再依赖不同的 endpoint
或不同的 pidfd。

## 本轮收敛结果（2026-09-12）

Node 现在从独立的
`VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE` 读取 Fleet
authorization 公钥；它与 issuer reply 公钥分离，避免用错验证根。launcher 在
pidfd offer socket inode 被替换时会跳过递归删除 workload volume root，并把该情况
作为失败结果返回；CRI `StopContainer`/`StopPodSandbox` 错误也会进入 cleanup 结果，
不会再生成过强的清理成功结论。attached observer 的 direct-child 与 ptrace
`TracerPid` 两种 contract 已同步到代码注释和部署文档。
Runtime 启动 socket 现在按 Node 请求的 basename 映射到容器，并拒绝签名 Pod
中的额外容器及 launcher 尚未实现的容器字段，避免 synthetic smoke 在配置不等价时误报成功。

目标机原生检查确认：Ubuntu 24.04、kernel `6.8.0-137-generic`、`pidfs=absent`，
`vela-pidfd-broker.service` 为 `active`，socket 为 root-owned `0660`；
`vela-runtime-policy-issuer.service` 当前为 `inactive`，且未配置 issuer/Fleet key
文件。这是验证环境尚未完成 production provisioning 的直接证据，不能据此宣称
Node/Fleet/issuer/ModelRuntime Permit 闭环已经完成。
本轮只读预检（2026-09-12）显示 `rke2-server` 仍为 `activating`，日志持续报告
etcd/API readiness timeout；本轮仍通过直接可用的 CRI socket 重跑了 launcher smoke，
但尚未执行 Node composition，且没有执行 RKE2 重启或 workload 清理。
进一步只读诊断显示 etcd manifest 仍配置 peer 地址 `10.1.200.17`，而目标机当前网卡地址为
`10.1.201.66`，etcd 因 `bind: cannot assign requested address` 反复退出；这是 RKE2 主机网络配置漂移，
需要运维在变更窗口修正 `node-ip`/peer 地址后再重启 RKE2。

本轮已将 source-matched `vela-runtime-launcher` 和
`vela-runtime-policy-issuer` 安装到目标机 `/usr/local/bin`，并安装 issuer
unit 与 root-owned `0600` env 文件；issuer 因缺少真实 key 保持 `disabled/inactive`。
当前 launcher SHA-256 为 `a621a2c774996d4d85695d6bd3093e23616a9410c0462cb0c7d9bdee7243ea66`，
issuer SHA-256 为 `bc54c6a92ac6d64b1fb1f5bb73c2cd52d782e64b09b804e2536efa4385f4f9f4`。

本轮还补齐了 release graph 的配置输入：`runtime-startup-packages.json` 现在携带
`pidfd-broker.env.example` 与 `runtime-policy-issuer.env.example`，schema-3
`RuntimeStartupPlan/Manifest` 记录并验证这两个 artifact。该改动只收紧发布图，未改变
目标机服务状态或 Production Gates。
