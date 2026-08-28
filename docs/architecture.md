# Vela 架构设计

| 属性 | 内容 |
| --- | --- |
| 状态 | Accepted architecture baseline |
| 实现状态 | Repository implementation in progress；Production Gates 仍为 0/9 PASS，未通过前不得承载正式流量 |
| 日期 | 2026-08-20 |
| 首个工作负载 | MiniMax H3 文生视频 |
| 首发客户 | 通过邀请和线下合同接入的 Customer Organization |
| 长期定位 | 通用 AI 推理集群控制面 |

## 1. 摘要

Vela 是面向大规模 AI 推理集群的正式 B2B 控制面。它接收耗时从数分钟到数十分钟的异步推理任务，根据模型、生成质量档位、Service Class、执行拓扑、机器健康度和队列负载选择 Worker，并负责重试、产物发布、合同信用计费和故障恢复。首发客户范围受控，但其请求、数据和账单适用完整生产可靠性、隔离和审计要求，不视为 beta 或测试流量。

Vela 不实现模型内部的张量并行或流水线执行。具体推理由 SGLang fork、vLLM 或其他 Inference Backend 完成；Vela 决定什么任务应当放在哪个 Worker 上、以哪个 ExecutionProfileRevision 执行，以及执行失败后如何恢复。

MiniMax H3 是第一个 Model / Workload，SGLang fork 是它的第一种 Inference Backend。每台 H3 机器有 8 张 GPU，其中 1 张负责 Encoder 和 VAE Decoder，另外 7 张负责 DiT。由于 PCIe 带宽低，8 张卡对 serving plane 是不可拆分的整体。Kubernetes 管理长期运行并已预热的 Worker，Vela Scheduler 管理单个推理 Job，SGLang fork 管理节点内部的 8 卡执行。

系统的核心可靠性语义是：

> At-least-once execution, exactly-once visible completion.

物理计算可能因网络分区或 Worker 丢失而重复发生，但只有一个 Attempt 可以原子形成 Visible Completion：Job 成功、获胜 ArtifactSet、Charge 和 Artifact 访问资格同时生效。

领域词汇以仓库根目录 `CONTEXT.md` 为准，关键取舍记录在 `docs/adr/`。本文描述整合后的可实现架构；两者发生冲突时必须先修正文档，不允许由实现自行选择语义。

## 2. 目标与非目标

### 2.1 目标

- 提供异步 AI 推理接口，支持最长 40 到 50 分钟或更久的任务。
- 支持 Customer Organization 下的多个 Project、Human Principal、Service Principal、固定 RBAC 和强制 Organization Isolation。
- 将一台机器或多机拓扑抽象成一个可调度的 Worker / ExecutionProfileRevision。
- 根据 Dynamic ETA、ServiceClassRevision、Organization / Project Capacity Share、模型预热状态和硬件风险进行调度。
- 在 Worker、GPU、驱动或网络故障后自动重试，并避免重复发布和重复计费。
- 将视频、缩略图和 checkpoint 等 Artifact 可靠写入对象存储。
- 支持不同质量和有损加速档位，并为每种用户可见 GenerationPresetRevision 配置独立计费。
- 使用合同信用额度、离散 OutputSpec SKU 和月度 Invoice 导出完成可审计的 B2B 计费闭环。
- 以 99.9% 月度控制面 API 可用性和各 Preset 的统计型端到端 p95 / 成功率作为首发 SLO，不销售未经 CapacityReservation 支撑的 Hard Deadline。
- 对异常 GPU 执行摘流量、恢复、验证和重新入池。
- 为未来的 LLM、图像、多模态 Model 和其他 Inference Backend 保留扩展能力。

### 2.2 非目标

- Vela 不替代 SGLang、vLLM 或模型专用推理引擎。
- Kubernetes 不感知 Encoder、DiT、VAE 等模型内部阶段。
- 首发不支持任意 DiT step 的跨 Worker Durable Checkpoint；节点丢失后从头重算。
- 首发不承诺跨地域强一致调度、自动 failover 或 Artifact 同步复制。
- 首发不要求构建通用 GPU 云、训练调度平台、支付网关或发票系统。

## 3. 已知约束与假设

### 3.1 H3 硬件约束

```text
8-GPU H3 Worker

GPU-E:   Encoder + VAE Decoder
           |
           | embedding / latent
           v
GPU-D0 --+
GPU-D1   |
GPU-D2   |
GPU-D3   +-- 7-GPU DiT
GPU-D4   |
GPU-D5   |
GPU-D6 --+
```

- PCIe 带宽很低，必须尽量减少跨 GPU tensor movement。
- 一张 GPU 失效就会使整个 H3 Worker 无法继续 serving。
- GPU 可能需要 process restart、GPU reset、PCIe FLR、driver reload、reboot 或 BMC power cycle。
- GPU role 必须通过 GPU UUID 或 PCI BDF 绑定，不能依赖可能变化的 CUDA index。
- Host kernel、GPU driver、firmware/VBIOS 和 container toolkit 在早期应锁定版本。

### 3.2 工作负载约束

- 文生视频任务执行时间差异大，最长可达 40 到 50 分钟。
- 分辨率、帧数、denoise steps、ModelRevision、LoRA、GenerationPresetRevision 和 InferenceBackendRevision 都会影响成本。
- HTTP 请求不能与推理执行保持同生命周期。
- 用户需要可查询的进度、取消、失败原因、重试状态和 Artifact 下载入口。

### 3.3 需要校准的假设

以下参数不能仅凭架构讨论确定，必须由故障注入和生产数据校准：

- heartbeat 周期、Lease TTL 和 Worker Lost grace period。
- 各 ModelRevision、分辨率、时长和 GenerationPresetRevision 的运行时间预测模型。
- Preset SLO、heartbeat / Lease 时序、退避和 finalization budget 的生产校准值。
- DiT latent checkpoint 的大小、写入成本和恢复收益。
- GPU 错误分类与每一级 remediation 的成功率和安全条件。

## 4. 核心设计原则

1. **Worker 是 serving 资源。** H3 首发中，一台 8-GPU 机器等于一个 Worker。
2. **Kubernetes 只管理 Worker 生命周期。** 单个 Job 由 Vela Scheduler 调度。
3. **Inference Backend 封装节点内部执行。** Vela 不理解 Inference Backend 内部 tensor movement。
4. **Job、Attempt 和 Lease 分离。** Job 表示用户意图，Attempt 表示一次物理执行，Lease 表示限时执行权。
5. **状态存储是事实源。** 队列用于唤醒和传递事件，不能成为唯一事实源。
6. **计算允许重复，发布只能一次。** 使用 fencing token 和 compare-and-swap 选择唯一获胜 Attempt。
7. **Serving domain 与 fault domain 分离。** 单张 GPU 是 fault domain，整台 8-GPU 机器是 H3 serving domain。
8. **恢复逻辑不依赖被恢复对象。** 特权 Node Agent 运行在 host systemd 下，不依赖 GPU Pod 或 container runtime。
9. **GenerationPresetRevision 是用户承诺，ExecutionProfileRevision 是内部手段。** 重试不得静默降低用户购买的质量档位。
10. **用户计费与内部成本分离。** 平台重试增加内部 COGS，但不重复向用户收费。
11. **Organization 与 Project 分离。** 前者拥有合同、信用和结算关系，后者拥有 credential、运行额度、Artifact namespace 和审计范围。
12. **Generation Preset 与 Service Class 分离。** 前者定义生成质量和速度，后者定义 admission、并发、调度权重和统计型 SLO。
13. **`202 Accepted` 是持久承诺。** Admission 失败不创建 Job；成功后不能因普通拥塞将 Job 重新拒绝。
14. **容量保持 work-conserving。** READY Worker 不作为硬空闲备用；故障延迟进入实际 Preset SLO 与 error budget。

## 5. 系统上下文

```text
Human Principal / Service Principal
  |
  v
API Gateway
  |
  v
Organization / Project Access Control
  |
  v
Job Coordinator -------- PostgreSQL + Outbox
  |       |
  |       +------------- Billing Ledger -------- Monthly Invoice Export
  +<-------------------> Model Catalog
  ^
  | state-transition requests
Scheduler <------------- Model Catalog
  ^
  +--------------------- Worker Registry
                              ^
                              | heartbeat / health / warm state
                              |
H3 Worker Pod ----------------+-----------------------> Job Coordinator
  |                                   acquire / heartbeat / complete
  +-- Inference Backend --> 8-GPU H3 Worker
  |
  +-- Artifact Store -------- Object Storage
            ^
            |
     Artifact Validator / Reconciler -----------------> Job Coordinator

PostgreSQL Outbox ----> Outbox Dispatcher ----> NATS JetStream
                                                   |
                                                   +-- wake Scheduler / Billing / Fleet / Reconciler / Webhook

Webhook Dispatcher -------------------------------> Project endpoints

Fleet Controller ------> Kubernetes Worker Pool ------> H3 Worker Pod

Node Health Controller
  |
  v
vela-node-agent (host systemd)
  |
  +-- process restart
  +-- GPU reset / PCIe FLR
  +-- driver reload / reboot / BMC
```

## 6. 逻辑模块

### 6.1 API Gateway

负责认证、Organization / Project 识别、限流、请求大小限制和外部协议适配。API Gateway 不代理视频上传或下载，也不保存 Job 状态。

外部 Interface 固定为 REST / JSON，并由 OpenAPI 描述；Envoy Gateway 负责 TLS termination、基础限流和路由，`vela-control` 负责 Principal、Project scope、幂等和领域校验。首发客户端接口至少包括：

```text
POST   /v1/projects/{project_id}/jobs
GET    /v1/projects/{project_id}/jobs/{job_id}
POST   /v1/projects/{project_id}/jobs/{job_id}/cancel
GET    /v1/projects/{project_id}/jobs/{job_id}/artifacts
DELETE /v1/projects/{project_id}/jobs/{job_id}/content
POST   /v1/projects/{project_id}/webhook-subscriptions
```

Human Principal 使用 OIDC，Project-owned Service Principal 使用可轮换、可过期和可吊销的 scoped credential；服务端只保存 credential hash。`POST .../jobs` 必须接受 Project-scoped `Idempotency-Key`。Admission 成功返回 `202 Accepted`、`job_id`、`QUEUED`、PricingSnapshot 和 `job_expires_at`；信用不足返回 `402 credit_limit_exceeded`，Project 限额返回 `429`，容量不足返回 `503 capacity_unavailable`，后两者携带 `Retry-After` 且不创建 Job。

### 6.2 Job Coordinator

Job Coordinator 是异步执行语义的核心模块。它是唯一可以改变 Job、Attempt、Lease、RetryRuntimeState、CreditReservation、Charge 和获胜 ArtifactSet 指针的模块，并封装 Admission、取消竞争和 Visible Completion。Scheduler、Worker、Billing exporter 和 reconciler 只能向它提交带预期版本的命令，不能直接更新这些状态。

客户端接口保持较小：

```text
submit(project_id, principal_id, request, idempotency_key) -> JobHandle
get(project_id, job_id)                                  -> JobView
cancel(project_id, job_id)                               -> CancelResult
```

Worker 协议：

```text
acquire(worker_epoch, capacity)                 -> Assignment?
start(lease_credentials)                        -> StartGranted | Stop
heartbeat(lease_credentials, progress)          -> Continue | Stop
complete(lease_credentials, artifact_set_candidate) -> Accepted
                                                      | RejectedStaleLease
                                                      | RejectedJobTerminal
fail(lease_credentials, failure)                -> RetryDecision
```

首发 Worker transport adapter 固定为 Protobuf / gRPC 双向流。Worker 主动建立 mTLS 连接，只在本地 capacity 可用时调用 `acquire()`；服务端从 mTLS 身份解析 `worker_id`，请求显式携带在本进程生命周期固定的 `worker_epoch`。Coordinator 通过同一流或 heartbeat 响应返回 `Continue`、`Stop`、`Drain` 和 Lease 更新。`acquire()` 是 read-or-create 操作：同一 `(worker_id, worker_epoch)` 已有未终结 Assignment 时，必须先重放原 `attempt_id`、Lease token、fence 和原始 `expires_at`，不能创建第二个 Attempt 或因重放延长 Lease。Assignment 一旦在 PostgreSQL 提交即可确认传输事件，40 到 50 分钟的执行所有权由 Lease / fence 保证，不能依赖一条长期 unacked 的 NATS 消息。

Job Coordinator 对内暴露上述小 Interface，生产使用 gRPC adapter，状态机测试使用 in-memory adapter。HTTP、gRPC 和 NATS transport 都不能绕过 Job Coordinator 直接修改持久状态。

### 6.3 Scheduler

Scheduler 从持久状态中选择可运行 Job 和符合条件的 Worker。它不拥有 Job 状态，只能通过 Job Coordinator 的事务操作创建 Attempt 和 Lease。

Scheduler 负责：

- 有界 Admission 和 Organization / Project 限额。
- Service Class、hierarchical fairness、Protected Lane、retry lane 和 aging。
- ExecutionProfileRevision 与 Worker capability 匹配。
- 模型预热和数据 locality。
- 预计运行时间和队列完成时间计算。
- 重试时避开已知故障 Worker 或 fault domain。
- 防止失败任务引发 retry storm。

H3 首发使用中央队列，每个 Worker 同时最多运行一个 Job，BUSY Worker 不接受预派任务。Scheduler 不维护 per-Worker queue，也不保留硬空闲 Worker。

### 6.4 Worker Registry

Worker Registry 保存 Worker 的身份、epoch、capability、模型预热状态、当前 Assignment、GPU 拓扑、Lifecycle State 和 Reachability Condition。

Worker 重启后必须递增 `worker_epoch`。旧 epoch 签发的 Lease 不能在新进程中继续使用。

### 6.5 Inference Worker

Inference Worker 是长期运行并已预热的进程。H3 首发中，一个 Pod 独占整台 8-GPU 节点，并拆成两个职责明确的进程：Go `vela-worker-agent` 管理 Assignment、Lease、heartbeat、ArtifactUpload 和 finalization；Python H3 runner 封装 SGLang fork，并协调 Encoder、DiT 和 VAE 进程。

二者通过 Pod 内 Unix domain socket 上的 Protobuf / gRPC Interface 通信，最小方法为 `prepare()`、`start()`、`cancel()`、`status()` 和 `collect_outputs()`。未来的 LLM runner 是该 Interface 的另一个 adapter，不把 backend-specific tensor 或进程细节暴露给 Worker Agent。

Worker 负责：

- 验证 Assignment 与 ExecutionProfileRevision。
- 按 GPU UUID / PCI BDF 绑定角色。
- 周期性 heartbeat 并上报阶段进度。
- 将生成结果写入本地 NVMe scratch。
- 上传全部必需 Artifact 并提交 ArtifactSetCandidate。
- 在 Lease 被拒绝或收到 Stop 后终止执行并清理临时资源。

Coordinator 的 Assignment / heartbeat 响应除持久化的 `expires_at` 外，还必须携带按 PostgreSQL 当前时间计算的 `lease_valid_for`。Worker 在发出对应请求前记录 monotonic timestamp，并以 `request_started_monotonic + lease_valid_for` 作为本地 fail-closed deadline；网络往返时间因此会缩短而不会延长可执行窗口。Worker 不使用本地 wall clock 比较 `expires_at`，收不到续租响应时必须在本地 monotonic deadline 前停止推进和提交。

### 6.6 Node Health Controller 与 vela-node-agent

Node Health Controller 根据 Worker heartbeat、NVML/DCGM、PCIe AER 和主机状态决定摘流量和恢复意图。`vela-node-agent` 是 host systemd 下的特权执行模块，负责安全地执行具体动作。

Node Agent 必须：

- 验证恢复命令的身份、目标设备和前置状态。
- 在 reset 前确认新任务已停止进入，相关进程已退出或被终止。
- 对同一节点的恢复动作加互斥锁。
- 记录每一步动作、退出码、设备身份和健康验证结果。
- 失败时升级恢复等级，达到阈值后隔离节点。

### 6.7 Artifact Store

Artifact Store 封装对象存储的 multipart upload、校验、短期访问凭据、逻辑提交和生命周期策略。生产环境使用同地域 S3-compatible adapter，测试使用本地 adapter。

### 6.8 Billing Ledger

Billing Ledger 保存 Contract Credit Limit、CreditReservation、不可变 PricingSnapshot、Charge、settlement / credit-adjustment reference 和内部 UsageRecord。Admission 在同一 PostgreSQL 事务中检查组织可用信用并创建 CreditReservation；Visible Completion 或 Billable Start 后的 Customer Cancellation 将其原子转为一条 Charge，其他终态释放 reservation。Ledger 异步导出月度 Invoice line，但不处理外部收款，也不让 Invoice 状态推进 Job。

### 6.9 Model Catalog

Model Catalog 管理 ModelRevision、InferenceBackendRevision、ExecutionProfileRevision、GenerationPresetRevision、ServiceClassRevision、OutputSpec、ProfileCertification 和 RateCardRevision。它拥有不可变定义、兼容关系、认证状态和 revision retention；Scheduler 对普通流量只能选择 `ACTIVE` 且认证有效的 revision，对显式 canary 流量可以选择 `CANARY` revision。首发用户可见 Preset id 固定为 `quality`、`balanced` 和 `fast`，未取得 Launch Receipt 的 revision 不得 ACTIVE。

### 6.10 Organization、Project 与 Identity

该模块保存 Customer Organization、Project、Human Principal 的 OIDC binding、Service Principal、Credential hash、固定角色和 scope。Customer Organization 是合同、信用和 Organization Isolation 边界；Project 是 credential、运行限额、Artifact namespace 和审计边界。首发固定角色为 OrganizationOwner、BillingAdmin、OrganizationAuditor、ProjectAdmin、Developer 和 ProjectViewer，不支持客户自定义 RBAC。

Platform Operator 不属于客户角色。读取 Customer Content 必须通过限时、审批和全审计的 Break-glass Access，不能用共享组织 master key 或模拟客户 Principal。

### 6.10.1 Compliance 与 Legal Hold

Compliance Principal 独立于 Human Principal、Service Principal、Finance Principal 和 Platform Operator。它通过专用 PostgreSQL role 与 TLS 1.3 mutual-auth listener 提交不可变 Legal Hold event，只能为一个确切的 Organization、Project 或 Job 冻结 `METADATA`、`FINANCIAL` 或两者的正常到期。Hold 的 placement 与 release 使用各 Principal 独立且连续的 source sequence，release 单向且不能删除历史证据。

Legal Hold 不拥有 Prompt、输入、Artifact、debug dump、Worker scratch 或 Local Recovery State；这些 Customer Content 仍受 24 小时 Content Deletion 合同约束。metadata / financial expiry authority 必须在删除候选记录的同一 PostgreSQL 事务中锁定并检查匹配的 ACTIVE hold，不能用异步缓存或事后补偿替代。

### 6.11 Fleet Controller

Fleet Controller 将 Catalog 中的 ExecutionProfileRevision 实现为 Kubernetes Worker pool，并负责 warm-up、canary、planned drain、rollout 和 retirement。它只能通过 Job Coordinator 请求 drain/fence，不能直接终止仍拥有有效 Lease 的 Worker。

由于 Fleet Controller / Node Health Controller 与 `vela-control` 是不同进程，`vela-control` 提供仅供其 service identity 调用的 mTLS gRPC maintenance Interface：

```text
request_drain(operation_id, worker_id, expected_epoch, reason, deadline) -> DrainOperation
get_drain(operation_id)                                                  -> DrainStatus
request_fence(operation_id, worker_id, expected_epoch, reason)          -> FenceResult
```

`operation_id` 是幂等键；Job Coordinator 在 PostgreSQL 事务中完成 Lifecycle State 转换、停止新 Assignment 和 Lease fencing。Controller 只有得到持久化的完成状态后才能要求 Kubernetes 删除 Pod 或要求 Node Agent 执行恢复动作，不能直写 Job / Worker 表。

### 6.12 Artifact Validator / Reconciler

Artifact Validator 验证对象身份、checksum、媒体规格和完整结果集。Artifact Reconciler 修复 Worker 在 multipart upload 完成后、ArtifactSet commit 前失联留下的中间状态，并清理无法恢复的 upload session 和孤儿对象。二者通过 Artifact Store interface 工作，不直接依赖具体对象存储产品。

Artifact Reconciler 不持有 Worker 的执行凭据。Job Coordinator 只能在原 Attempt 尚未被替代、ArtifactUpload 可恢复且 finalization budget 未耗尽时，为它签发同一 fence 的 FINALIZATION Lease。该 Lease 只能上传、验证和提交既有结果，不能重新运行推理。

### 6.13 Webhook Dispatcher

Webhook Dispatcher 从 Outbox-backed delivery queue 向 Project Webhook Subscription 投递 `job.succeeded`、`job.failed` 和 `job.canceled`。它使用带时间戳的 HMAC、支持新旧 secret 重叠轮换，对非 2xx 响应指数退避最多 72 小时，随后进入可查看和人工重放的 dead-letter 状态。投递是 at-least-once，payload 只携带 `event_id`、aggregate version 和状态 metadata，不携带 Customer Content、signed URL，也不能修改 Job、Charge 或 ArtifactSet。

### 6.14 实现与部署单元

逻辑 Module 不等于独立网络进程。首发使用同一个 Go module / repository，并只在权限、生命周期或真实远程依赖不同处拆部署单元：

| 部署单元 | 语言 | 包含的 Module | 拆分原因 |
| --- | --- | --- | --- |
| `vela-control` | Go | HTTP adapter、Organization / Project / Identity、Compliance / Legal Hold、Job Coordinator、Scheduler、Worker Registry、Model Catalog、Billing Ledger、Artifact Validator / Reconciler、Webhook / Outbox dispatcher | 共享 PostgreSQL 事务和领域不变量，保持模块化单体；Compliance 使用独立 listener 与数据库 pool |
| `vela-fleet-controller` | Go | Fleet Controller、Node Health Controller | 独立 Kubernetes RBAC 与 rollout 生命周期 |
| `vela-worker-agent` | Go | Worker protocol、Lease client、Artifact upload / validation client | 与推理 runner 分离，保持长连接和恢复语义稳定 |
| H3 runner | Python | SGLang fork、GPU role binding、模型执行 | 保留 Python / CUDA 推理生态 |
| `vela-node-agent` | Go | allowlisted remediation executor | host systemd 高权限进程，不依赖 Kubernetes 或 container runtime |

`vela-control` 可以运行多个相同 replica；后台循环使用 row claim、advisory lock 或唯一约束竞争，不为同进程内的 Scheduler、Catalog 和 Billing 增加网络 Interface。对象存储、OIDC、Invoice export、Kubernetes、Worker、Inference Backend，以及跨进程的 Fleet / Node Health maintenance command 这些真实变化点定义 adapter seam。

`deploy/vela-control` 固定该模块化单体的 Kubernetes 边界：两个 replica 必须分布在不同 Control/Storage Node，Scheduler、各 Reconciler 和 Dispatcher 的 claimant identity 从不可变 Pod UID 派生；Remediation 使用与共享证书 URI `spiffe://vela.internal/controller/vela-control` 匹配的稳定 actor。数据库 DSN 和 credential pepper 仅来自 release-versioned 外部 Secret，文件型 mTLS / NATS / S3 / keyring material 逐 key 复制为 UID 10001 拥有的普通文件；轮换必须创建新 Secret 名称并滚动 Pod template，不支持原地 Secret 更新。public API、Worker、Fleet、Finance 和 Compliance 使用五个单用途 ClusterIP Service 与独立 ingress policy，`/healthz` 和 `/readyz` 只监听不经 Service 暴露的 Pod-private management port；其中 `vela-control.vela-system.svc:8444` 只保留 Fleet maintenance 兼容地址。每个 Pod 通过独立 ephemeral PVC 预留 Artifact validation scratch，并由专用 StorageClass 和 PriorityClass 隔离共享 Control/Storage Node 上的容量与 I/O 风险。环境相关 egress 由 release overlay 按实际 PostgreSQL、NATS、OIDC、对象存储、Webhook、Invoice 和 Node Agent 目标收敛，仓库 base 不以宽泛外网规则代替。该可渲染 contract 不是已部署证据，也不改变 Production Gate 结果。

## 7. 领域模型

| 概念 | 定义 | 关键不变量 |
| --- | --- | --- |
| CustomerOrganization | 合同、信用和结算主体 | 所有 Project 共享一个 Contract Credit Limit 和结算关系 |
| Project | Organization 内的操作空间 | credential、配额、Idempotency-Key、Artifact namespace 和审计均按 Project 隔离 |
| Principal / Credential | 行为主体及其可轮换身份凭据 | Human Principal 使用 OIDC；Service Principal 属于一个 Project；审计归因不随 credential 轮换丢失 |
| CompliancePrincipal / LegalHold | 独立合规主体及其非内容保留指令 | 只能覆盖确切 Organization / Project / Job 的 METADATA / FINANCIAL；不能保留 Customer Content |
| Job | 用户的一次推理意图 | 请求、报价和执行策略快照创建后不可变 |
| Attempt | Job 的一次物理执行 | 一个 Job 可有多个 Attempt |
| Lease | Worker 对 Attempt 的限时执行权 | 区分 EXECUTION / FINALIZATION phase，包含鉴权 token、单调 fence、owner epoch 和 expiry |
| Worker | 对外可调度的执行实体 | H3 中为完整 8-GPU appliance |
| ModelRevision | 确切的模型权重和配置版本 | 可复现，不使用浮动 latest |
| InferenceBackendRevision | 推理引擎及其适配代码版本 | 与 ModelRevision 的兼容性已验证 |
| ExecutionProfileRevision | 内部执行拓扑和加速方法 | 必须具有有效 ProfileCertification |
| GenerationPresetRevision | 用户可选择的生成质量和速度档位 | 独立版本化，不包含排队优先级，不暴露硬件细节 |
| ServiceClassRevision | 用户购买的队列服务等级 | 锁定 admission、并发、调度权重和统计型 SLO，不改变生成质量 |
| ProfileCertification | GenerationPresetRevision 与 ExecutionProfileRevision 的认证关系 | 有基准、指标、有效期和失效状态 |
| OutputSpec | 已认证的离散输出 SKU | 未被 ACTIVE RateCardRevision 覆盖的规格不能 Admission |
| RateCardRevision | Model、Preset、Service Class、OutputSpec 到固定单价的映射 | 不可变、有生效区间并生成 PricingSnapshot |
| PricingSnapshot | Admission 时锁定的 SKU 报价 | 排队期间调价不影响既有 Job，金额使用 integer minor unit / Decimal |
| CreditReservation | 从组织合同信用额度中为 Job 占用的金额 | 与 Accepted Job 同事务创建，只能转为 Charge 或释放 |
| ExecutionPolicySnapshot | Job 接纳时锁定的执行策略 | Retry Budget、Job Expiry、Service Class 和取消语义不随配置漂移 |
| RetryRuntimeState | Job 当前的动态重试状态 | 与 Attempt 终态在同一事务更新 |
| Artifact | 视频、缩略图、checkpoint 或 debug dump | 每个 Attempt 写独立不可变对象 |
| ArtifactSet | 一个成功 Job 对外发布的完整 Artifact manifest | 所有必需输出一起发布，不允许部分可见 |
| ArtifactUpload | 一次可恢复的对象上传会话 | 记录 multipart、校验和验证状态 |
| UsageRecord | 一次 Attempt 的实际资源消耗 | 用于内部 COGS，不等于用户 Charge |
| Charge | 进入月度结算的不可变应收记录 | Visible Completion 或 Billable Start 后取消最多生成一次；Failed Job 不收费 |
| WebhookSubscription / Delivery | Project 外部通知配置与一次投递 | at-least-once、可重放，不能作为 Job 状态事实源 |

### 7.1 Generation Preset、Service Class 与 Execution Profile

`ExecutionProfileRevision` 面向 Scheduler 和 Worker，例如：

```yaml
id: h3-lossy-fast-v3
revision: 1
model_revision: minimax-h3-2026-08
resource:
  nodes: 1
  gpus_per_node: 8
topology:
  encoder_vae: 1
  dit: 7
runtime:
  inference_backend_revision: sglang-vela@abc123
  precision: fp8
  acceleration_level: lossy-2
```

`GenerationPresetRevision` 面向用户并只定义生成质量 / 速度承诺，例如：

```yaml
id: fast
revision: 3
model_revision: minimax-h3-2026-08
quality_class: fast
quality_contract:
  benchmark_revision: h3-video-quality-v2
  min_quality_score: 0.82
generation_performance_envelope:
  max_certified_inference_p95_seconds: 1800
```

首发稳定 id 为 `quality`、`balanced` 和 `fast`；id 不随实现升级变化，所有行为变化通过 revision 发布。`quality` 使用参考质量路径，`balanced` 是默认认证档位，`fast` 使用更激进但仍满足独立质量阈值的有损加速。

`ServiceClassRevision` 独立定义 queue contract，例如：

```yaml
id: standard
revision: 1
admission:
  queue_budget_policy_revision: h3-standard-admission-v1
scheduling:
  organization_weight_policy_revision: b2b-fairness-v1
slo:
  measurement: queued_to_visible_completion
  target_matrix_revision: h3-standard-slo-v1
```

首发只开放 `standard`，但 PricingSnapshot 和 ExecutionPolicySnapshot 必须显式锁定 ServiceClassRevision，不能把未来的优先服务伪装成请求级 `priority` 参数。

`ProfileCertification` 建立 GenerationPresetRevision 与 ExecutionProfileRevision 的有效映射：

```yaml
generation_preset_revision: fast@3
execution_profile_revision: h3-lossy-fast-v3@1
inference_backend_revision: sglang-vela@abc123
output_spec: video-1080p-5s-24fps@1
hardware_baseline: h3-8gpu-driver-r1
benchmark_revision: h3-video-quality-v2
quality_score: 0.85
performance_receipt: benchmark://h3-fast-v3-20260820
status: active
certified_at: 2026-08-20T00:00:00Z
invalidated_at: null
```

一个 GenerationPresetRevision 可以认证多个满足同一质量承诺的 ExecutionProfileRevision。平台可以升级硬件或 Inference Backend，但不能在重试时把 `quality` Job 改为 `fast`、改变 ServiceClassRevision，也不能选择 ProfileCertification 已失效的 ExecutionProfileRevision。Preset SLO 使用 Service Class 的 target matrix 按 GenerationPresetRevision 和 OutputSpec 统计，Dynamic ETA 不构成 SLO。

## 8. 持久化与消息传递

### 8.1 权威状态

PostgreSQL 是以下数据的权威事实源：

- Customer Organization、Project、Principal binding、Credential hash、role 和 scope。
- Job、Attempt 和 Lease。
- RetryRuntimeState、Worker epoch 和 Assignment。
- Artifact、ArtifactSet、ArtifactUpload 和获胜 ArtifactSet 指针。
- Model Catalog revision、ServiceClassRevision、OutputSpec、RateCardRevision、ProfileCertification 和 rollout 状态。
- Contract Credit Limit、CreditReservation、PricingSnapshot、ExecutionPolicySnapshot、UsageRecord、Charge、Invoice export receipt 和 settlement / credit-adjustment reference。
- WebhookSubscription、WebhookDelivery、Outbox 事件、Retention / deletion 和恢复审计记录。

首发 Scheduler Job claim 使用按 Worker pool scoped 的 PostgreSQL advisory transaction lock 串行化短暂的公平性决策，并从一个有硬上限的 point-in-time candidate snapshot 创建 durable dispatch intent；partial unique index 禁止同一 Job 或 Worker 同时存在多个 live claim。Scheduler 不把该锁延伸到 Assignment，`Acquire` 在独立事务中锁定并重新检查 dispatch intent、Job、Worker epoch、认证、额度和重试 authority。每个业务 row 显式带 `organization_id`；Project-owned row 同时带 `project_id`，composite foreign key 禁止跨 Organization 关联。客户请求使用受限数据库 role、transaction-local identity context 和 `FORCE ROW LEVEL SECURITY`，Scheduler / Reconciler 使用独立内部 role 和连接池，客户请求路径不得借用该 privileged pool。

控制面所有 Lease expiry、Job Expiry 和重试时间比较以 PostgreSQL 时间为准，不能依赖各 Pod 的 wall clock。Worker 只使用服务端返回的 `lease_valid_for` 和本地 monotonic clock 做保守的 fail-closed 倒计时，不能自行延长 Lease。CloudNativePG 在三个 Control/Storage Node 上使用同步提交、跨节点副本和自动 failover，并通过 Barman Cloud Plugin 把 WAL 与 base backup 写入独立故障域以支持 PITR；数据库不可用时系统停止 Admission 和新 Assignment。

### 8.2 队列语义

首发可靠事件设施固定为 3-replica NATS JetStream。JetStream 使用 Control/Storage Node 的独立持久盘、跨节点 anti-affinity、durable consumer 和 explicit ack；它承载 Job ready、状态变化、Invoice export、Webhook、Fleet 和 reconciliation wakeup，但不保存唯一业务状态。消息只携带 `event_id`、aggregate type / id、aggregate version、event type 和 schema version。

所有关键状态变更与 outbox event 必须在同一 PostgreSQL 事务中提交。Dispatcher 以 `event_id` 作为 `Nats-Msg-Id` 发布，并且只有收到目标 replicated stream 的 quorum-committed `PubAck` 后，才能在独立 PostgreSQL 事务中标记 outbox row 为 published，同时记录 stream 和 sequence receipt。publish timeout、negative ack 或连接中断都视为未发布，以同一 `Nats-Msg-Id` 重试；JetStream duplicate window 必须覆盖 dispatcher 的最大重试间隔，但 broker 去重只用于减少重复，正确性仍由消费者幂等保证。若在 `PubAck` 成功、标记前崩溃，会产生重复消息而不是丢消息。消费者必须先读取 PostgreSQL 当前状态，再以 `event_id`、aggregate version、唯一约束或 compare-and-swap 幂等处理，并且只在本地事务提交后 ack。

Scheduler 和其他关键 consumer 必须有周期性 reconciliation scan，即使 JetStream 整体不可用、消息过期或 consumer state 丢失，也能从 PostgreSQL 重新发现可调度 Job、过期 Lease、待完成 ArtifactSet、待导出 Invoice line 和待投递 Webhook。JetStream 恢复后，outbox dispatcher 继续发布积压事件。

JetStream 只缩短发现延迟并隔离 consumer，不取代持久 Job 队列。Assignment 落库后即 ack 对应 wakeup；Worker 通过 gRPC pull 获取 Assignment，长时间执行由 Lease 续租，不在 Broker 中保留 40 到 50 分钟的 pending delivery。

### 8.3 事件故障语义

| 故障点 | 结果与恢复 |
| --- | --- |
| Outbox 事务提交后 dispatcher 崩溃 | row 仍为未发布，恢复后重发 |
| publish timeout、negative ack 或 `PubAck` 丢失 | row 保持未发布，以相同 `Nats-Msg-Id` 重试；可能重复但不能丢失 |
| `PubAck` 成功、outbox 标记前崩溃 | JetStream 可能收到重复 event，consumer 幂等处理 |
| consumer 提交本地事务、ack 前崩溃 | event 重投，aggregate version / CAS 拒绝重复转换 |
| JetStream 集群不可用 | outbox 积压，PostgreSQL reconciliation 维持最终恢复 |
| Scheduler 收到事件后崩溃 | durable consumer 重投或 reconciliation 重新 claim |
| PostgreSQL 不可用 | 停止新 Assignment；不能退化为仅凭 JetStream 消息推进状态 |

## 9. Job 与 Attempt 状态机

### 9.1 Job 状态

```text
QUEUED -> ASSIGNED -> RUNNING -> FINALIZING -> SUCCEEDED
   ^          |          |           |
   |          +----------+-----------+
   |                     |
   +------ RETRY_WAIT <---+

QUEUED / RETRY_WAIT -------------------------> CANCELED
ASSIGNED -- cancel wins CAS -----------------> CANCELED
RUNNING / FINALIZING -- cancel wins CAS -----> CANCELING -> CANCELED

QUEUED / RETRY_WAIT 超过 Retry Budget 或 Job Expiry -> FAILED
ASSIGNED / RUNNING / FINALIZING 超过预算或 Job Expiry -> fence Attempt -> FAILED
```

Admission 失败是 HTTP 层的 Capacity / Credit / Validation Rejection，不创建 Job；Accepted Job 的初始状态固定为 `QUEUED`。终态为 `SUCCEEDED`、`FAILED` 和 `CANCELED`，终态 Job 不得重新进入执行态。`CANCELING` 只表示 Billable Start 后的 Customer Cancellation 已持久化、Charge 已形成且执行权已 fenced，但 Worker 停止尚未确认。

### 9.2 Attempt 状态

```text
ASSIGNED -> RUNNING -> FINALIZING -> SUCCEEDED
    |          |           |
    +----------+-----------+--> FAILED
    +----------+-----------+--> LOST
    +----------+-----------+--> CANCELED
```

Attempt 终态不得相互转换。EXECUTION Lease 过期且超过 Worker Lost grace period 后，Attempt 持久化为 `LOST`；它随后提交完成时，`complete()` 返回 `RejectedStaleLease`，但 Attempt 仍保持 `LOST`。FINALIZATION Lease 的 owner 失联时，若 ArtifactUpload 可恢复且 finalization budget 未耗尽，Attempt 保持 `FINALIZING`，由 Artifact Reconciler 使用同一 fence 的新 Lease 接管；只有确认不可恢复或预算耗尽后才进入 `FAILED` 或 `LOST`。未来若启用 speculative execution，应新增明确的 `SUPERSEDED` 终态，而不是复用 stale 响应。

### 9.3 Worker 状态

Worker 状态分成两个正交维度，避免把运行阶段与网络可达性混为一个枚举：

```text
Lifecycle State:
REGISTERING -> WARMING -> READY <-> BUSY
                    ^       |       |
                    |       +---+---+
                    |           v
                    |       DRAINING
                    |           |
                    |           v
                    +------ RECOVERING -> QUARANTINED

Reachability Condition:
HEALTHY <-> SUSPECT <-> OFFLINE
```

短暂 heartbeat 缺失首先进入 `SUSPECT`；超过 grace period 后进入 `OFFLINE`。EXECUTION Lease 的 Attempt 随后标记为 `LOST`；FINALIZATION Lease 则先撤销失联 owner，由 Artifact Reconciler 在 finalization budget 内判断是否可接管，只有无法恢复时才结束 Attempt。BUSY Worker 可以同时是 SUSPECT。OFFLINE Worker 恢复 heartbeat 后先进入 `SUSPECT`，完成设备、Inference Backend 和 canary health check 后才回到 `HEALTHY`；Lifecycle State 不因网络恢复自动改变。

## 10. 提交、执行与完成流程

### 10.1 Job 提交

1. 客户端提交请求和 `Idempotency-Key`。
2. API Gateway 验证 Principal、Organization / Project ownership、scope、限流、请求大小和基础 schema。
3. Job Coordinator 解析 immutable ModelRevision、GenerationPresetRevision、ServiceClassRevision、OutputSpec、RateCardRevision、PricingSnapshot、ExecutionPolicySnapshot 和 `job_expires_at`。
4. Scheduler 基于 ACTIVE ProfileCertification、风险修正后的 pool queue budget、Artifact Store circuit、scratch 水位和预计排队时间形成 Admission candidate；这一步只做预测，不能占用额度或返回 `202`。
5. Job Coordinator 在一个 PostgreSQL 事务中锁定 Project / pool admission counter 和 Customer Organization credit row，重新检查 Catalog revision、Project queued / running limit、pool queue bound、circuit、Job Expiry policy 和可用 Contract Credit Limit。全部满足时才更新计数，并创建 `QUEUED` Job、CreditReservation、PricingSnapshot、ExecutionPolicySnapshot、空 RetryRuntimeState、Project-scoped idempotency result 和 Job-ready Outbox event。信用不足返回 `402 credit_limit_exceeded`，Project 限额返回 `429`，容量不足返回 `503 capacity_unavailable`；拒绝事务不更新计数，也不创建 Job 或 CreditReservation。
6. 事务提交后返回 `202 Accepted`、`job_id`、锁定报价、`QUEUED` 和 `job_expires_at`。普通队列拥塞不能再将 Accepted Job 改成 REJECTED。

同一 Project 下，相同 Idempotency-Key 和相同 request hash 在 Admission 成功后返回原 Accepted Job；相同 key 但 request hash 不同返回 `409 Conflict`。Admission 前的 402 / 429 / 503 拒绝不缓存为永久业务结果，条件变化后可以用同一 key 重新评估。

### 10.2 Assignment 与 Lease

1. Scheduler 从中央队列选择 Job，并选择具有有效 ProfileCertification 的 ExecutionProfileRevision。
2. Scheduler 只考虑 Lifecycle 为 READY、Reachability 为 HEALTHY 的 Worker，并过滤 capability、模型版本、拓扑、健康和维护状态不匹配的节点。
3. Job Coordinator 在一个事务中锁定 Job 和 Worker，并重新检查 Job version / QUEUED 状态、CreditReservation 为 RESERVED、Job Expiry、Worker epoch / READY capacity、ServiceClassRevision 和 ProfileCertification 有效性；全部满足时才创建 Attempt、占用 Worker capacity 并签发 Lease。唯一约束保证一个 H3 Worker 同时最多有一个 active Assignment，校验失败则不产生 Attempt并重新调度。
4. EXECUTION Lease 至少包含 `attempt_id`、`worker_id`、`worker_epoch`、不可伪造的 `lease_token`、对该 Job 单调递增的 `fence` 和 `expires_at`。
5. `acquire()` 在同一事务中先按 mTLS `worker_id` 和请求 `worker_epoch` 查找未终结 Assignment；存在时返回原 Assignment，不重新 claim Job。只有不存在时才执行步骤 3 的创建逻辑。响应丢失后，同一 Worker epoch 重连会得到完全相同的 `attempt_id`、Lease token、fence 和原始 `expires_at`；返回动作本身不续租。
6. Worker 完成 prepare 后调用 `start()`；Job Coordinator 以 Lease / Job version 执行 `ASSIGNED -> RUNNING` CAS。该持久转换是 Billable Start，Worker 只有收到 `StartGranted` 才开始模型计算；丢失响应时以同一 Lease 重放，不产生第二次转换。
7. Worker 执行并通过 heartbeat 续租。

同一 Job 在逻辑上只有一个有效执行权。网络分区时可能有多个物理计算，但只有当前 `fence` 和 Lease token 能推进 Job 和提交 Artifact。后创建 Attempt 的 fence 必须严格大于此前所有 Attempt。

### 10.3 Heartbeat 与进度

Worker heartbeat 至少上报：

```text
attempt_id
worker_epoch
lease_token
lease_fence
backend_stage
backend_stage_progress
estimated_remaining_seconds
gpu_health_summary
local_artifact_state
scratch_free_bytes
artifact_store_reachable
```

backend progress 是观测和预测信息，不作为恢复正确性的唯一依据。Job API 将其映射为 backend-neutral `phase`、attempt-scoped `phase_progress`、`attempts_started`、`next_retry_at`、`estimated_finish_at` 和 `progress_updated_at`。Phase Progress 可以在 retry 后重置，更新过期时返回 null；只有 Visible Completion 表示 100%。续租失败、Lease 被撤销或 Worker epoch 变化时，Worker 必须停止当前 Attempt。

### 10.4 完成与 ArtifactSet commit

1. Worker 在本地 NVMe 完成编码和封装。
2. Worker 调用 `begin_finalization()`。Job Coordinator 在一个数据库事务中将 Lease phase 原子切换为 FINALIZATION、固定 Attempt 的 `finalization_started_at` / `finalization_deadline_at`，并按 output spec 幂等创建所有必需的 `STAGING` Artifact 和 ArtifactUpload；唯一约束保证崩溃重放不会创建第二组记录。
3. 事务提交后，Artifact Store claim 对应 ArtifactUpload：已有 `multipart_upload_id` 时恢复 session；没有时创建 multipart session，再用 row version CAS 保存 upload id。若在外部创建成功、CAS 落库前崩溃，session 可能成为 orphan，由 Reconciler / bucket incomplete-multipart lifecycle 清理；不能假设 S3 `CreateMultipartUpload` 自带幂等语义。
4. Worker 完成或恢复每个必需 Artifact 的 multipart upload，ArtifactUpload 持久化 upload id、已完成 parts、object version 和 checksum。
5. Artifact Validator 验证每个对象的 object version、checksum、content type、duration、resolution、frame count 和 codec，并验证必需输出的 kind、ordinal 和数量符合 output spec / `generation_count`。
6. Worker 使用当前 FINALIZATION Lease 提交包含全部必需输出的 ArtifactSetCandidate。Worker 失联后，Artifact Reconciler 只能在 Job Coordinator 重新签发同一 fence 的 FINALIZATION Lease 后接管。
7. Job Coordinator 使用 Lease token、单调 fence 和 Job version 执行 compare-and-swap，在同一 PostgreSQL 事务中创建不可变 ArtifactSet manifest、标记其所有 Artifact 为 `COMMITTED`、更新 `jobs.result_artifact_set_id`、将 Job 置为 `SUCCEEDED`、把 CreditReservation 转为唯一 `POSTED` Charge，并开放 Artifact 访问资格。
8. 同一事务写入 Visible Completion、Invoice export 和 Webhook Outbox event；缺少任一必需输出时整个事务回滚，Job 保持 `FINALIZING`，ArtifactSet 不可见且不能计费。
9. Worker 收到 `Accepted` 后清理本地 scratch。

旧 Attempt 晚到时返回 `RejectedStaleLease`，其对象进入短期清理策略。对象已经上传但 Worker 在提交前失联时，Artifact Reconciler 根据 ArtifactUpload 和对象存储状态继续恢复已有必需输出的上传、验证、提交整个 ArtifactSet 或清理，不重新运行推理。只要可恢复的 FINALIZATION Lease 仍在预算内，Scheduler 就不能启动新的计算 Attempt。

### 10.5 取消与完成竞争

所有取消、完成和 Job Expiry 事件都通过 Job Coordinator 的 versioned compare-and-swap：

- `QUEUED` / `RETRY_WAIT` Job 取消时直接进入 `CANCELED` 并释放 CreditReservation。
- `ASSIGNED` Job 取消赢得 CAS 时，在同一事务中递增 fence、直接进入 `CANCELED`、释放 CreditReservation 并写 Stop / Webhook Outbox event；不等待 Worker 确认且不产生 Charge。
- `RUNNING` / `FINALIZING` Job 的 Customer Cancellation 赢得 CAS 时，在同一事务中递增 fence、进入 `CANCELING`、把 CreditReservation 转为完整报价 Charge，并写 Stop 与 Invoice export Outbox event；不等待 Worker 真正停机。
- Visible Completion CAS 先成功时，Job 进入 `SUCCEEDED`，随后 cancel 返回 `AlreadySucceeded` 并保留 Artifact 访问。
- cancel fencing 先成功时，complete 返回 `RejectedStaleLease`，Artifact 不得发布或生成第二个 Charge。
- `job_expires_at` 到期时，Job Coordinator 在一个事务中递增 fence、结束当前 Attempt、将 Job 置为 `FAILED` 并释放 CreditReservation；晚到的 heartbeat 或 complete 只能得到 stale 响应。Job Expiry 是系统停止上限，不是 Hard Deadline。

只有 Worker 停止确认或 Lease 过期使 `CANCELING -> CANCELED` 提交时，才写 `job.canceled` Webhook Outbox event；Webhook Dispatcher 不得把中间状态伪装成终态通知。

## 11. 调度策略

### 11.1 候选过滤

Scheduler 首先执行硬约束过滤：

- Worker Lifecycle 为 READY、Reachability 为 HEALTHY，且没有维护或隔离 condition。
- Worker capability 满足 ExecutionProfileRevision。
- ModelRevision 和 InferenceBackendRevision 已加载，或允许在 Assignment 前预热。
- GPU UUID / PCI BDF 拓扑符合要求。
- Customer Organization、Project、地域、数据驻留和安全策略允许。
- GenerationPresetRevision 与 ExecutionProfileRevision 之间存在当前有效的 ProfileCertification。

### 11.2 排序模型

H3 首发不向 BUSY Worker 预派任务，也不维护 per-Worker queue。选择顺序固定为 Customer Organization、Service Class、Project、Job、compatible Worker。Organization 和 Project 分别使用 weighted deficit fairness 与硬 queued / running limit；请求不能携带绕过 ServiceClassRevision 的任意 `priority`。

中央队列中的 Job 排序可以使用：

```text
job_order_score =
    predicted_runtime_seconds
  + bounded_retry_risk_penalty
  - bounded_expiry_urgency_credit
  - bounded_aging_credit
```

`job_order_score` 越小越先运行。`bounded_retry_risk_penalty` 是带 source revision 的 per-Job 调度预测，并受 ServiceClassRevision 的不可变上限约束；缺失预测时为 0。各 credit 必须有上限；Job Expiry 越近，`bounded_expiry_urgency_credit` 越大，避免把高风险 Job 反向排到后面。Organization / Project 公平性在前置层级执行，不依赖把所有因素压进一个全局分数。

为保证 aging 真正防止饥饿，等待超过 `max_queue_wait_before_protection` 的 Job 进入 Protected Lane 并按 Job Expiry / FIFO 排序，不再与持续到来的短 Job 竞争上述分数；Protected Lane 仍受 Organization / Project 并发配额约束。Retry 保留原 Job 等待年龄，但进入有独立并发上限的 retry lane，防止 retry storm。

从 READY Worker 中选择执行位置时使用：

```text
worker_score =
    model_cold_start_penalty
  + locality_penalty
  + worker_health_risk_penalty
```

`predicted_runtime_seconds` 可以由输出时长、分辨率、帧数、denoise steps、ModelRevision、GenerationPresetRevision 和历史 telemetry 拟合。模型必须持续用实际 Attempt 数据校准。

Scheduler 还要用 READY Worker 的即时容量和 BUSY Worker 上报的 `estimated_remaining_seconds` 构造全池容量时间线，计算 Job 的 `predicted_start_at`、`predicted_finish_at` 和 expiry urgency。该时间线只用于排序、Admission 和 Dynamic ETA，会在每次 heartbeat / Assignment 后重算，不形成对 BUSY Worker 的 CapacityReservation 或预派绑定。

### 11.3 公平性与 admission control

- Organization 按合同 Capacity Share 进行 weighted deficit fairness；Organization 内按 Project weight 和并发上限分配。
- aging 与 Protected Lane 防止长 Job 饥饿，retry lane 防止故障风暴吞没普通流量。
- Admission 的事务内 counter 锁定并重新检查 ProfileCertification、Project / Organization limit、pool queue bound、Artifact Store circuit 和 scratch 水位；事务外预测只用于快速拒绝，不能成为容量事实源。
- Project 限额返回 429；风险修正后的预测排队超过 Service Class admission budget 时返回 503；二者不创建 Job。
- 所有 READY compatible Worker 都可执行普通 Job，不保留硬空闲备用。Worker 丢失后立即收紧 Admission，retry lane 优先于尚未开始的普通 Job但不抢占运行中任务；故障等待计入真实 Preset SLO。
- 首发不销售 Hard Deadline。`job_expires_at` 只限制系统生命周期，Dynamic ETA 不作承诺；未来必须先实现持久 CapacityReservation 和专属容量再销售逐 Job deadline。
- Scheduler 使用 ServiceClassRevision 的队列约束，不读取价格，也不从 GenerationPresetRevision 推导 priority。

## 12. 重试与故障处理

### 12.1 Retry Budget

每个 Job 的 ExecutionPolicySnapshot 必须固化以下重试参数：

```text
max_attempts
max_total_compute_seconds
max_finalization_seconds_per_attempt
job_expires_at
retry_backoff_policy
retryable_failure_classes
circuit_breaker_policy
```

首发 ExecutionPolicy template 允许最多 3 个 compute Attempt，并将 `max_total_compute_seconds` 初始设为所选 Preset 认证 p95 inference runtime 的 2 倍；早期失败可能到达第三个 Attempt，晚期失败通常只剩一次重试机会。具体值必须在 ACTIVE 前由故障注入和实测 runtime 校准，策略更新只影响新 Job。

动态信息保存在 RetryRuntimeState：

```text
attempts_started
compute_seconds_consumed
finalization_seconds_consumed
finalization_retry_count
next_retry_at
excluded_workers_with_reason_and_expiry
failure_fingerprints
circuit_breaker_state
last_failure_class
```

Attempt 首次进入 FINALIZING 时，Job Coordinator 从 `max_finalization_seconds_per_attempt` 计算并持久化不可延后的 `finalization_deadline_at`；更换 Lease owner 或 Reconciler 接管不得重置该时间。ArtifactUpload 的 `expires_at` 不得晚于它。Attempt 进入任何终态时，Job Coordinator 在同一事务中累计实际或保守估算的 compute / finalization seconds、更新 exclusion 和 fingerprint，并据此选择 `RETRY_WAIT`、`FAILED` 或 circuit open。`next_retry_at` 是 Scheduler 重新取出 Job 的权威时间。

### 12.2 失败分类

| 失败类型 | Job 处理 | Worker 处理 | 用户计费 |
| --- | --- | --- | --- |
| 同步可判定的参数或 OutputSpec 非法 | Admission 拒绝，不创建 Job | 无 | 无 CreditReservation |
| 执行期才能判定的输入内容不支持 | FAILED | 无 | 释放 CreditReservation，不收费 |
| ModelRevision 或 GenerationPresetRevision 配置错误 | 当前 Job FAILED | 打开相关 revision circuit | 释放 CreditReservation，不收费 |
| Worker heartbeat 丢失 | RETRY_WAIT | SUSPECT，随后恢复 | 保留 CreditReservation，不重复计费 |
| GPU Xid / fallen off bus | RETRY_WAIT | DRAINING / RECOVERING | 保留 CreditReservation，不重复计费 |
| 临时进程崩溃 | 按下方规则 5 进入 RETRY_WAIT 或 FAILED | process restart | 最终失败释放 CreditReservation |
| 确定性 OOM | 按下方规则 3 进入 RETRY_WAIT 或 FAILED | 无或降载 | 最终失败释放 CreditReservation |
| Artifact upload 失败 | 保持 FINALIZING | resume multipart upload | 不重新计算或重复计费 |
| 多 Worker 相同错误 | 按下方规则 4 进入 RETRY_WAIT 或 FAILED | 打开 revision circuit 并调查 | 最终失败释放 CreditReservation |
| Customer Cancellation | CANCELING / CANCELED | Stop + cleanup | Billable Start 前释放，之后生成完整报价 Charge |

失败必须携带稳定的 `failure_class`、原始错误摘要、stage、Worker、GPU UUID、InferenceBackendRevision 和是否建议重试。不要让 Scheduler 解析自由文本日志决定重试。

Job 的唯一重试决策者仍是 Job Coordinator，决策顺序固定为：

1. 非 retryable failure、`job_expires_at` 到期或 Retry Budget 耗尽时进入 `FAILED` 并释放 CreditReservation。
2. ArtifactUpload 可恢复且 `finalization_deadline_at` 未到期时保持 `FINALIZING`，只重试上传、验证或 commit。
3. 确定性 OOM 只有在 Catalog 存在满足原 GenerationPresetRevision、资源更充足且认证有效的 ExecutionProfileRevision 时才进入 `RETRY_WAIT`，否则进入 `FAILED`。
4. 同一 revision 的 failure fingerprint 在配置阈值内跨多个健康 Worker 重现时，先使相关 ProfileCertification 失效并打开 revision circuit；当前 Job 只有存在其他合格 ExecutionProfileRevision 且预算允许时才重试，否则失败。
5. 其余 retryable failure 根据退避策略设置 `next_retry_at` 和动态 Worker exclusion 后进入 `RETRY_WAIT`。

### 12.3 重试放置

- 默认避开上一个失败 Worker。
- GPU 或 PCIe fault 后避开同一节点，直到恢复验证完成。
- 新 Attempt 必须继续满足原 GenerationPresetRevision，不能静默降级。
- 新 Attempt 必须保持原 ServiceClassRevision，不能因 retry 降低 Capacity Share 或 SLO 统计范围。
- 相同 failure fingerprint 在多个 Worker 重复出现时应触发 Job 或 ModelRevision circuit breaker。
- 默认不启用 speculative duplicate execution；只有明确的高价值 SLA 才允许 hedging。

### 12.4 Checkpoint

首发按阶段考虑恢复：

```text
Encoder -> DiT -> VAE -> Upload
```

Encoder / DiT / VAE 的 Local Recovery State 只保存在当前 Worker 的 NVMe，用于同节点进程重启。Artifact upload 失败只恢复上传；Worker 节点或其 NVMe 丢失时 Attempt 进入 LOST，新 Worker 从头重算。首发不创建跨 Worker Durable Checkpoint。只有故障阶段分布、latent 大小、I/O 开销和恢复成功率证明总成本低于重算时，才引入 DiT checkpoint。

## 13. Artifact 设计

### 13.1 存储职责

- PostgreSQL 保存 Artifact / ArtifactSet metadata 和 Job 的最终 ArtifactSet 指针。
- 对象存储保存视频、缩略图、checkpoint 和采样 debug dump。
- Worker 本地 NVMe 只作为有配额、可清理的 scratch 空间。
- API Gateway 不转发大文件内容。
- Artifact Validator 校验媒体内容是否符合 Job 的 output spec。
- Artifact Reconciler 恢复或清理中断的 multipart upload 和未完成 commit。

### 13.2 Object key

```text
artifacts/{organization_id}/{project_id}/{job_id}/{attempt_id}/{artifact_id}/video.mp4
artifacts/{organization_id}/{project_id}/{job_id}/{attempt_id}/{artifact_id}/thumbnail.webp
checkpoints/{organization_id}/{project_id}/{job_id}/{attempt_id}/{artifact_id}/dit-latent.bin
```

`artifact_id` 由控制面随机生成且永不复用。Object key 不包含 prompt、用户名或用户提供的原始文件名。

对象不可变性必须由存储机制强制保证，不能只依赖命名约定：

- 生产 bucket 启用 versioning，并在 metadata 中保存 `object_version_id`。
- upload credentials 只允许写一个确切 object key，不允许 list、overwrite 或 delete。
- adapter 在支持时使用 conditional create，例如 `If-None-Match: *`。
- 已 COMMITTED Artifact 的读取固定到 object version，而不只使用可变 key。
- 对需要更强保护的正式 Artifact，可以启用对象存储的 WORM / Object Lock 能力。
- 不支持 versioning 的 adapter 必须证明 key 永不复用、覆盖被 policy 拒绝，并通过 conformance test 后才可用于生产。

对象存储没有发布语义。Vela 不通过 copy + delete 模拟 rename，而是通过数据库中的不可变 ArtifactSet manifest 决定哪些对象作为一个完整结果正式可见：

```text
jobs.result_artifact_set_id          -> artifact_sets.id
artifact_set_items.artifact_set_id   -> artifact_sets.id
artifact_set_items.artifact_id       -> artifacts.id
```

### 13.3 Artifact metadata

```text
artifact_id
artifact_set_id (nullable until commit)
job_id
attempt_id
kind
ordinal
object_key
object_version_id
storage_region
size_bytes
sha256
content_type
validated_output_spec
status
retention_until
created_at
```

Artifact 状态至少包括 `STAGING`、`VERIFIED`、`COMMITTED`、`EXPIRED` 和 `DELETED`。

ArtifactSet 保存不可变发布 manifest：

```text
artifact_set_id
job_id
attempt_id
manifest_hash
generation_count
required_item_count
status
retention_until
committed_at
```

ArtifactSet item 固定每个 Artifact 的 `artifact_id`、kind、ordinal、object key、object version、size 和 checksum；manifest 内容在创建后不可变，只有生命周期 status 可以变化。只有 output spec 要求的所有 item 都达到 `VERIFIED`，Job Coordinator 才能在一个事务中创建 ArtifactSet、更新所有 item 的 `artifact_set_id` / `COMMITTED` 状态和 Job 结果指针。checkpoint 与 debug dump 默认不属于对外结果集，不阻塞正式视频发布。

ArtifactUpload 独立记录可恢复上传状态：

```text
artifact_upload_id
artifact_id
attempt_id
attempt_fence
multipart_upload_id
completed_parts
object_version_id
expected_size_bytes
expected_sha256
state
retry_count
next_retry_at
expires_at
updated_at
```

上传状态至少包括 `INITIATED`、`UPLOADING`、`UPLOADED`、`VERIFIED`、`ABORTED` 和 `EXPIRED`。Worker 进程重启可以基于本地文件和 ArtifactUpload 恢复 multipart；跨 Worker 接管只有在源文件或 checkpoint 已位于共享持久存储时才允许。finalization budget 到期且结果无法恢复时，Job Coordinator 才结束当前 Attempt 并依据 RetryRuntimeState 决定是否重新计算。

Artifact Validator 必须核对 object version、size、checksum 和 content type，并使用媒体探测验证 duration、resolution、frame count 和 codec。验证结果、必需 kind / ordinal 或 generation count 与 Job 的 output spec 不一致时，ArtifactSet 不得 COMMITTED 或触发 Charge。

### 13.4 访问与安全

- Bucket 保持 private。
- Worker 使用绑定当前 Artifact object key、method、size 和 checksum constraint 的短期 multipart upload 权限。
- Visible Completion 事务提交后即可按需签发 15 分钟 signed GET URL；外部 Invoice settlement 不阻塞访问。
- 对公网高流量下载可以在 COMMITTED Artifact 前增加 CDN。
- 对象存储与计算集群保持同地域；首发不提供 Vela 跨地域复制。
- Prompt、输入、输出、中间文件和 debug dump 均为 Customer Content，默认不得用于训练、benchmark 或人工质量分析；访问必须记录审计日志。

### 13.5 生命周期

| Artifact 类型 | 默认策略 |
| --- | --- |
| 正式视频 | 默认 30 天；合同可选 7、30 或 90 天 |
| 缩略图 / 预览 | 跟随正式视频 |
| Prompt / 请求正文 | 默认 30 天；随后只保留不可还原内容的规格、hash 和执行 metadata |
| 失败、LOST、未获胜 Attempt 输出和 incomplete multipart | 24 小时内清理 |
| Local Recovery State | 终态后立即清理，最迟 24 小时 |
| Debug dump | 默认关闭；客户授权时最多 72 小时 |
| Job / Attempt 非内容 metadata | 1 年 |
| Charge / CreditReservation / 结算审计 | 7 年或适用法律要求的更长期限 |
| Worker 本地 scratch | 终态后立即清理，最迟 24 小时 |

Retention Policy 版本锁定并由控制面与存储 lifecycle 共同执行，不应写死在 Worker 中。客户 Content Deletion 在 24 小时内异步删除 Prompt 与 Artifact，不撤销 Charge，也不删除法定保留的非内容审计；备份中的删除通过到期与恢复后 deletion replay 闭环。

非内容 Legal Hold 只能延长 Job / Attempt metadata 与财务记录的正常到期，不能改变上表任何 Customer Content 期限。未来 metadata / financial expiry Reconciler 必须先锁定候选记录，再在同一 PostgreSQL 事务中调用 active-hold lock contract；存在匹配 ACTIVE hold 时不得提交到期。Release 只允许后续正常到期，不追溯恢复已删除记录。

### 13.6 存储故障与背压

Worker heartbeat 上报 scratch 剩余容量和 Artifact Store 可达性。控制面维护 Artifact Store circuit，并采用 high / low watermark 避免抖动：

```text
Artifact Store unhealthy
  -> 停止受影响 pool 的新 Assignment
单个 Worker scratch 达到 high watermark
  -> 将该 Worker 移出 READY 候选集
pool 可用 scratch 容量低于 pool watermark
  -> 停止该 pool 的新 Assignment
  -> 允许运行中 Job 结束并优先恢复上传
  -> scratch 回落到 low watermark 且存储探测通过
  -> 恢复 Assignment
```

本地 Artifact 在 COMMITTED 或明确终止前不得因普通空间压力被静默删除。达到 critical watermark 时必须停止推理并触发运维告警；如何处置无法上传的结果属于显式恢复动作。

## 14. 计费设计

### 14.1 离散 SKU 与内部成本

客户按已认证的离散 OutputSpec SKU 计费，不按实际 GPU 时间，也不接受任意参数进入连续价格公式。RateCardRevision 的有效 line key 为：

```text
ModelRevision
GenerationPresetRevision
ServiceClassRevision
OutputSpec
currency
```

每个 line 定义一次生成的固定 integer minor-unit / Decimal 价格，`generation_count` 只做整数数量相乘。未命中 ACTIVE RateCardRevision 的组合在 Admission 前拒绝；未来 LoRA、长期存储等能力作为显式 line item，不隐藏在不可审计的 multiplier 中。

每个 Attempt 的 GPU 时间、能耗、重试和存储流量记录为 UsageRecord，用于内部 COGS。平台故障和自动重试只增加内部成本，不改变 PricingSnapshot 或生成额外 Charge。

### 14.2 PricingSnapshot

Job 创建时保存：

```text
model_revision
generation_preset_revision
service_class_revision
output_spec
rate_card_revision
rate_line_id
quantity
quoted_amount_minor
currency
```

`retry_policy_revision` 属于 ExecutionPolicySnapshot，而不是价格计算本身。ExecutionPolicySnapshot 和 PricingSnapshot 一起绑定到 Job，分别保证执行语义和报价不会在长时间排队期间漂移。

任务排队或执行期间发生调价、GenerationPresetRevision 更新或硬件升级，都不能改变既有 PricingSnapshot。

### 14.3 Contract Credit Ledger

Customer Organization 通过线下合同获得 Contract Credit Limit。可用信用按同一 PostgreSQL 一致性视图计算：

```text
available_credit =
    contract_credit_limit
  - unsettled_posted_charges
  - active_credit_reservations
```

Admission 锁定 Organization credit row，并与 Accepted Job 同事务创建覆盖完整 `quoted_amount_minor` 的 CreditReservation。该记录只属于内部 Contract Credit Ledger，没有独立到期或续期流程，在 Job 形成 Charge 或无费用终止前不能自行释放。信用不足返回 `402 credit_limit_exceeded` 且不创建 Job。

### 14.4 计费流程

```text
submit
  -> resolve discrete SKU quote
  -> reserve Contract Credit
  -> QUEUED + 202 Accepted
  -> execute and retry
  -> Visible Completion
  -> POSTED Charge + Artifact access

FAILED / pre-RUNNING cancellation
  -> RELEASED CreditReservation

RUNNING / FINALIZING Customer Cancellation
  -> POSTED full-quote Charge
```

```text
                              +-> CONSUMED
CreditReservation: RESERVED --|
                              +-> RELEASED

Charge: POSTED
```

Visible Completion 在同一 PostgreSQL 事务中提交获胜 ArtifactSet、SUCCEEDED、POSTED Charge 和 Artifact access，并将 CreditReservation 标记为 CONSUMED。Billable Start 后 Customer Cancellation 赢得 CAS 时，同样在取消事务中生成完整报价 Charge；其他最终 FAILED Job 均不收费。`charges(job_id)` 唯一约束保证每个 Job 最多一条 Charge，商业减免由外部 credit note 处理，不回写执行历史。

### 14.5 月度结算

Invoice 不属于 Vela Job 生命周期。POSTED Charge 与同事务的 `invoice.export_requested` 建立 PostgreSQL-authoritative export authority；Billing exporter 周期扫描 PostgreSQL，Outbox / JetStream 只提供可选 wakeup，不是待导出 line 的唯一事实源。外部财务流程按 Customer Organization 月度汇总，以 `charge_id` 作为幂等键；Vela 保存不可变外部 Invoice / line receipt。导出失败可重试但不能产生重复 line、阻塞 Job、Artifact access 或修改 Charge。外部财务流程在收款、credit note 或合同额度变更后，通过幂等 reconciliation 写入独立的 settlement / credit-adjustment record，只改变可用信用计算，不改写 Charge 金额、Job 或 Artifact 历史。

Contract Credit Limit 只能由服务方财务根据有效合同变更并完整审计。BillingAdmin 可查看信用使用、Charge 与 Invoice reference，并维护结算联系人，但不能自行提高额度，默认也不能读取 Prompt 或 Artifact。

## 15. Model Catalog 与 revision 生命周期

### 15.1 Catalog 状态

ModelRevision、InferenceBackendRevision、ExecutionProfileRevision、GenerationPresetRevision、ServiceClassRevision、OutputSpec 和 RateCardRevision 都必须使用不可变 revision。`quality`、`balanced`、`fast`、`standard` 等稳定 id 只能作为查询 alias；Admission 必须解析到具体 revision 并写入 Job snapshot。

ExecutionProfileRevision 的生命周期为：

```text
REGISTERED -> VALIDATING -> CERTIFIED -> CANARY -> ACTIVE -> DRAINING -> RETIRED

VALIDATING -> INVALID
CANARY     -> INVALID
```

只有 `ACTIVE` ExecutionProfileRevision 可以接收普通 Job；`CANARY` 只接收隔离的内部 canary 流量，不能靠首发客户承担验证。验证或 canary 失败时进入 `INVALID`。生产 telemetry 低于质量门槛时，Catalog 先使相关 ProfileCertification 失效，再将 `ACTIVE` ExecutionProfileRevision 转为 `DRAINING`，从而阻止新 Assignment；引用归零后才进入 `RETIRED`。

ProfileCertification 至少绑定 ModelRevision、InferenceBackendRevision、ExecutionProfileRevision、GenerationPresetRevision、OutputSpec、硬件 / driver 基线、benchmark corpus revision 和证据 digest。首发三个 Generation Preset 的每个可售 OutputSpec 都必须分别取得质量、成功率、端到端 p95、成本和统计置信证据；没有认证的组合不能出现在 ACTIVE RateCardRevision 中。首发 Service Class 固定为 `standard`，其 SLO matrix 同样按 GenerationPresetRevision 和 OutputSpec 版本化。

### 15.2 Rollout

```text
register immutable revisions
  -> verify ModelRevision / InferenceBackendRevision compatibility
  -> run quality and performance benchmark
  -> issue ProfileCertification
  -> Fleet Controller warm canary Worker pool
  -> admit bounded canary traffic
  -> promote ACTIVE
  -> drain old ExecutionProfileRevision
  -> retire after references reach zero
```

Fleet Controller 执行 planned rollout，并与硬件维护共用 DRAINING 语义：停止新 Assignment，等待当前 Job 完成或达到显式 grace period，再替换 Worker。不能依赖 Kubernetes 直接终止一个仍在执行 40 分钟 Job 的 Pod。

普通 release 不得中断 Accepted Job。控制面和 Worker protocol、Protobuf event、数据库 schema 必须支持 N/N-1 版本共存；数据库迁移采用 expand -> backfill -> switch -> contract，contract 只能在旧 binary、旧 event backlog 和旧 Worker 引用归零后执行。消费者必须忽略未知的可选字段，禁止在同一 field number 或 event type 上改变既有语义。若变化无法满足兼容窗口，必须建立显式 migration operation，先 drain 受影响 revision，再执行升级。

每次 release 先升级无状态 `vela-control` replica，再 canary 新 Worker pool。回滚不得回退已提交的数据库 schema，而是切回 N-1 binary / configuration；发布前必须用真实长任务验证升级、回滚、Worker drain 和旧 event backlog 消费。

`vela-control` 的 repository base 使用两个 replica、`maxUnavailable: 0`、`maxSurge: 1`、required hostname anti-affinity 和 `minAvailable: 1` PDB。它只保证 release manifest 不主动同时移除两个 replica；只有真实集群中的旧/新 binary coexistence、readiness、连接 drain、long-running Job 和 retained backlog receipt 才能证明 non-interrupting release。

### 15.3 Revision 保留

- QUEUED、ASSIGNED、RUNNING、FINALIZING 或 RETRY_WAIT Job 引用的 ModelRevision、GenerationPresetRevision、ExecutionPolicySnapshot 和 PricingSnapshot 必须保留；非终态 Attempt 引用的 InferenceBackendRevision、ExecutionProfileRevision 和 ProfileCertification 同样必须保留。
- Retry 可以选择另一个具有有效 ProfileCertification 的 ExecutionProfileRevision，但 ModelRevision 和 GenerationPresetRevision 不变。
- Model weights、Inference Backend image 和配置只有在引用计数为零且审计保留期满足后才能删除。
- 强制迁移必须创建显式 migration record，重新验证质量、价格和用户承诺，不能静默修改 Job snapshot。

## 16. GPU 健康与恢复

### 16.1 恢复阶梯

| Level | 动作 | 首发执行策略 | Lifecycle / Reachability |
| --- | --- | --- | --- |
| L0 | restart inference process | allowlist 内自动执行 | DRAINING |
| L1 | 清理 CUDA process / context | allowlist 内自动执行 | DRAINING |
| L2 | `nvidia-smi --gpu-reset` | 仅认证过的 GPU / topology 自动执行 | RECOVERING |
| L3 | PCIe FLR | 仅认证过且已 fence 的设备自动执行 | RECOVERING |
| L4 | unload / reload driver | 认证、fence、限频后自动执行 | RECOVERING |
| L5 | reboot node | 认证、fence、限频后自动执行 | RECOVERING / OFFLINE |
| L6 | BMC power cycle | 必须人工审批或双人确认 | RECOVERING / OFFLINE |
| L7 | quarantine | 身份不清、动作未认证或验证失败时自动 fail closed | QUARANTINED |

恢复流程：

```text
detect fault
  -> mark Worker DRAINING
  -> stop new Assignment
  -> wait grace period or hard-stop severe fault
  -> fence current Lease
  -> execute selected remediation
  -> run device and Inference Backend health tests
  -> warm model
  -> canary admission
  -> return READY or QUARANTINED
```

不能对所有错误无条件执行 FLR，也不能默认逐级尝试到最高等级。每次 Remediation Operation 必须持久化 `operation_id`，绑定 node identity、GPU UUID / PCI BDF、Worker epoch、故障证据、认证矩阵 revision、动作和结果。Node Agent 根据 error class、设备 reset capability、拓扑限制和当前使用状态选择已认证动作；身份不匹配、重复失败、超出限频或 post-check 失败直接 Quarantine。

### 16.2 Kubernetes 与 driver

首发使用三台非 GPU Control/Storage Node，统一运行 Ubuntu LTS、RKE2 和 containerd。每台节点都承载 RKE2 control plane / etcd、CloudNativePG replica、JetStream replica 和分布式 S3-compatible storage 的一个 failure-domain member；组件通过 pod anti-affinity、独立持久盘、I/O limit、priority class 和容量预留避免互相挤占。它们可以共享节点，但不能共享同一数据盘，也不能与不稳定 GPU Worker 共用生命周期。

对象存储只有在三节点磁盘拓扑、故障域和实测恢复能力满足 Production Gate 时才自托管；否则首发切换到已有的外部 S3-compatible store。无论哪种实现，接口都必须提供 private bucket、versioning、conditional create、固定 object version、checksum 和 off-cluster backup。

GPU Worker 由主机镜像、PXE 或 Ansible 管理固定 kernel、driver、firmware 和 container toolkit。若使用 NVIDIA GPU Operator，关闭其 driver 和 toolkit 生命周期管理，只启用经验证的 NVIDIA Device Plugin、DCGM Exporter 和必要的 metrics 能力。

H3 首发 Worker Pod 请求：

```yaml
resources:
  limits:
    nvidia.com/gpu: 8
```

同时使用专用 node label、taint/toleration 和 Worker pool，阻止普通 GPU workload 进入 H3 节点。`nvidia.com/gpu: 8` 保证 GPU 独占，但系统 DaemonSet 和 host `vela-node-agent` 仍可共存，因此不要把它误解为 CPU 和主机资源的完全独占。

不能单独使用下面的扩展资源代替实际 GPU claim：

```yaml
resources:
  limits:
    vela.ai/h3-worker: 1
```

Kubernetes 将 `vela.ai/h3-worker` 与 `nvidia.com/gpu` 视为独立资源；仅请求前者不会自动占用 8 张 GPU。长期若引入自定义资源，只能选择一个权威分配机制：

- 自定义 Device Plugin 或 DRA driver 真正声明整组设备、注入所需 device，并确保同一 GPU 不再由另一插件重复分配；或
- 继续请求 `nvidia.com/gpu: 8`，将 Vela profile 只作为 node label / scheduling metadata，而不是可独立消费的资源。

H3 首发采用第二种方案，不实现 `vela.ai/h3-worker` 扩展资源。

Fleet Controller 为每个 ACTIVE / CANARY ExecutionProfileRevision 管理带专用 node selector 的 Worker pool。H3 baseline 使用 `OnDelete` DaemonSet，但 `OnDelete` 本身不是安全边界。Live WorkerPool / DaemonSet / Worker Pod 由 Fleet Controller 独占管理，Argo CD 只交付 controller、CRD 和版本化期望配置，不 prune 或直接 patch live pool resource。Kubernetes RBAC 与 validating admission webhook / policy 拒绝其他 service account 删除 Worker Pod / pool、修改 DaemonSet selector / image 或移除保护 finalizer；Fleet Controller 只有携带 Job Coordinator 已完成的 `DrainOperation` 引用才能解除 finalizer并执行这些动作。节点突然失效仍由 Lease / fence 恢复，不能由 Kubernetes guard 保证。

Fleet Controller 只有在 Job Coordinator 确认 Worker 已 DRAINING、Lease 已结束或 fenced 后才删除 Pod；不能依赖默认滚动更新自动终止长任务。PodDisruptionBudget、较长 `terminationGracePeriodSeconds` 和 preStop drain 只能作为额外保护，不能代替 Vela Lease 语义。

每个 GPU 节点提供独立 NVMe scratch，使用 XFS project quota 或等价硬配额，并挂载到 H3 Worker Pod。Worker Agent 为每个 Attempt 创建独立目录；配额、high / low / critical watermark 和终态清理由 Vela 管理，不能把 Kubernetes ephemeral-storage eviction 当作 Artifact 恢复机制。

## 17. 一致性与高可用

### 17.1 Scheduler 高可用

可以运行多个 Scheduler replica，但 Assignment 必须通过数据库事务竞争。Scheduler 进程本身不持有不可恢复内存状态。

一个 Worker pool 的 claim、counter drift 或 Assignment 错误只结束该 pool 当前调度 tick，并携带 pool identity 返回告警；它不能阻止同一 cycle 中其他健康 pool 继续调度。共享 context 被取消或超时才停止整个 cycle。

Scheduler 崩溃后：

- 未提交事务不会产生 Assignment。
- Assignment 已提交但 gRPC acquire 响应未送达时，同一 mTLS `worker_id` 和 `worker_epoch` 重试 `acquire()` 会先读到并重放同一 active Assignment；该路径不创建新 Attempt、不签发新 fence，也不延长 `expires_at`。若 Worker 始终没有取得响应或续租，则 Lease 过期后由 reconciliation 结束 Attempt。新进程必须使用递增的 epoch，不能继承旧 Lease。Outbox 只重发状态事件，不传递执行所有权。
- Worker 未续租时 Lease 最终过期，Job 进入重试判断。

### 17.2 网络分区

Worker 与控制面失联时，旧 Worker 可能仍继续计算。控制面可以在 grace period 后签发具有更大 fence 的新 Attempt，但旧 Attempt 已失效，不能推进 Job、发布 Artifact 或触发 Charge。

### 17.3 数据与恢复目标

- 任一 Control/Storage Node、单盘或单个控制面实例失效时，已提交 PostgreSQL 事务的目标是 RPO 0，控制面恢复目标是 RTO 不超过 5 分钟；Admission 和新 Assignment 在无法证明 quorum 时 fail closed。
- 整个主站点或三节点集群丢失时，不做自动跨地域 failover。PostgreSQL WAL、Catalog / 配置和密钥恢复材料写入独立故障域，控制面 metadata 的站点恢复目标为 RPO 不超过 15 分钟、RTO 不超过 4 小时。Committed Artifact 另做 off-cluster backup 和抽样恢复验证，但全站点 Artifact 恢复与 GPU serving capacity 恢复不从 metadata 目标外推，必须在客户合同中单独披露。
- JetStream 是可重建的投递设施，不是灾备事实源。恢复顺序为 PostgreSQL / Catalog -> Artifact Store -> JetStream -> Outbox replay -> Reconciler；恢复不能制造第二个 Visible Completion、Charge 或 Webhook event id。
- PostgreSQL restore、JetStream rebuild、Outbox replay、Artifact 抽样恢复和 secret rotation 至少每季度实演一次。备份任务成功不等于恢复通过。

Slice 38 将仓库中的 CNPG native backup surface 迁移到 digest-pinned
Barman Cloud Plugin `ObjectStore`、WAL archiver 和 immediate/daily base backup，
并在 fresh four-node kind/MinIO 环境完成真实 base backup、目标 WAL 归档和
timestamp restore（`4f4bc2d`，credential-isolation review closure
`e8a4149`）。release-owned install render 将两个 Barman principal 限制为只能
访问精确的 `vela-backup-s3`，并拒绝 Artifact credential 读取。这只证明 local
plugin API/recovery path；生产
RKE2、独立 S3 故障域、provider/network failure、完整恢复顺序、季度演练与
Launch Receipt 仍是外部 Production Gate。

### 17.4 外部依赖故障

- PostgreSQL 不可用时停止新 Assignment，Worker 可以在有限 Lease 内继续当前 Attempt。
- 对象存储不可用时已完成推理停留在 FINALIZING，并保留本地 Artifact 后恢复上传；Artifact Store circuit 阻止受影响 pool 的新 Assignment，scratch high watermark 只阻止对应 Worker 或 pool 的新 Assignment。
- Invoice export 不可用时 PostgreSQL 保留由 POSTED Charge 和 canonical Outbox intent 建立的 export authority；周期 reconciliation 以同一 `charge_id` 重试，不阻塞 Visible Completion、Artifact access，也不重新执行 Job。
- JetStream 不可用时 outbox 保留事件，并依靠 PostgreSQL reconciliation 保证最终恢复。

Vela 不持有硬空闲故障 Worker。故障后立即收紧风险修正 Admission、保护 retry lane，并让所有 READY 且兼容的 Worker 保持 work-conserving；由此产生的排队延迟必须计入 Preset SLO 和 error budget。

## 18. 安全

- Human Principal 只通过企业 OIDC 登录；Service Principal 必须属于一个 Project，使用 scope 化、可过期、可轮换和可吊销的 Credential。Client、Worker、Scheduler、Node Agent、NATS 和 storage credential 使用独立身份，服务端只保存可验证 hash 或外部 subject。
- 首发 RBAC 固定为 OrganizationOwner、BillingAdmin、OrganizationAuditor、ProjectAdmin、Developer 和 ProjectViewer，不提供自定义角色。Organization 级角色不因可看账单或审计而默认获得 Customer Content 读取权。

| Principal / role | 默认授权边界 |
| --- | --- |
| OrganizationOwner | 管理 Organization 成员、Project 和组织策略；读取 Project Customer Content 仍需显式 Project role |
| BillingAdmin | 查看信用使用、Charge 与 Invoice reference，并维护结算联系人；不能修改 Contract Credit Limit，不读取 Customer Content |
| OrganizationAuditor | 只读查看组织审计与非内容用量；不读取 Customer Content |
| ProjectAdmin | 管理 Project 成员、Service Principal、Credential 和 Webhook Subscription |
| Developer | 在获授权 Project 提交、查询和取消 Job，并读取 Artifact |
| ProjectViewer | 在获授权 Project 查询 Job 和读取 Artifact，不能提交或取消 |
| Service Principal | 只执行 Credential 显式 scope 允许的 Project API，不继承 Human role |

- 每个组织域表都包含 `organization_id`，Project 域表同时包含 `project_id`；PostgreSQL RLS、复合外键、唯一约束和事务内 identity context 共同强制 Organization Isolation。应用层 filter 不是安全边界，跨组织负向测试属于 Production Gate。
- Platform Operator 默认不能读取 Customer Content。例外访问必须使用 Break-glass Access，绑定审批人、原因、scope、到期时间和不可变审计，不允许共享 master credential 或模拟客户 Principal。
- Envoy Gateway 使用 cert-manager 管理外部 TLS；Kubernetes workload 的内部 gRPC 使用 cert-manager 签发和轮换 mTLS certificate。host systemd `vela-node-agent` 使用 OS provisioning、Vault PKI 或等价私有 CA 签发的独立 host certificate。所有证书身份必须映射到 Worker / Controller / Node 注册身份。
- NATS listener 和 monitoring endpoint 只暴露在内部网络；客户端连接必须使用 TLS。NATS 使用 operator / account JWT 模式，为 outbox dispatcher、Scheduler、Billing、Fleet 和 Reconciler 分别签发可轮换的 NKey workload credential，并按 event subject 配置最小 publish / subscribe ACL；禁止共享全权 token，禁止业务 workload 使用 system account。
- 长期 secret 存放在 Vault 或现有企业 KMS / Secret Manager，通过 External Secrets 注入；对象存储上传和下载使用短期凭据，BMC credential 不写入普通 ConfigMap 或日志。
- Node Agent 具有高权限，其命令接口必须最小化并限制到已登记设备和动作。
- Node Agent 不接受任意 shell command，不把 PCI sysfs path 直接暴露给远端调用者。
- Worker 只能上传当前 Artifact 的确切 object key，不能覆盖、删除或列举其他对象。
- Artifact Validator 将媒体视为不可信输入；`ffprobe` 在无网络、非特权且有 CPU、内存、文件大小和超时限制的 sandbox 中运行。
- signed URL 具有短 TTL，并绑定 method、object key 和 content constraints。
- Customer Content 默认只用于执行和交付 Job，不用于训练、benchmark、共享质量数据集或例行人工抽检。日志禁止记录 prompt 正文、对象凭据和 signed URL。
- Retention Policy 默认执行成功 Artifact / prompt 30 天、失败对象 24 小时、scratch 24 小时以内、显式授权 debug dump 最多 72 小时、运行 metadata 1 年、财务与审计记录 7 年或法律允许的更长期限。Content Deletion 不撤销 Charge，也不删除法定非内容记录。
- 所有管理动作、Artifact 访问、计费变更和节点恢复均需审计。

## 19. 可观测性

所有 Go / Python 进程使用 OpenTelemetry SDK，将 trace、metric 和结构化日志发送到 OpenTelemetry Collector。首发 backend 使用 Prometheus + Alertmanager、Grafana、Loki 和 Tempo；GPU 指标来自 DCGM Exporter。生产告警必须覆盖 API 99.9% 月度 SLO、各 Preset p95 / success-rate error budget、PostgreSQL replication / failover、JetStream replica / consumer lag、outbox age、reconciliation backlog、Webhook backlog 和 object-store circuit。

### 19.1 Job 指标

- 接纳率、队列长度和 queue wait time。
- 按 ModelRevision、GenerationPresetRevision、ServiceClassRevision、OutputSpec 和受控 customer cohort 统计 queue wait、端到端完成时间与成功率；单个 `organization_id` 或 `project_id` 不作为常规 metric label。
- Job success、failure、cancel 和 retry rate。
- 每个 Job 的 Attempt 数量和累计 compute seconds。
- Artifact upload latency、失败率和 orphan bytes。
- Admission rejection reason、CreditReservation create / release / consume、Charge post 和 Invoice export 成功率。

### 19.2 Worker 与硬件指标

- 各 Lifecycle State 和 HEALTHY、SUSPECT、OFFLINE Reachability 的 Worker 数量。
- GPU utilization、memory、temperature、power 和 Xid。
- PCIe AER、fallen off bus 和 heartbeat loss。
- 各 remediation level 的执行次数、成功率和恢复耗时。
- ModelRevision / ExecutionProfileRevision warm 状态和 cold-start 时间。

### 19.3 追踪标识

所有结构化日志和 trace span 至少关联：

```text
organization_id
project_id
principal_id
job_id
attempt_id
worker_id
worker_epoch
model_revision
generation_preset_revision
execution_profile_revision
```

Prometheus metric 只使用数量受控的 label，例如 ModelRevision、GenerationPresetRevision、ServiceClassRevision、ExecutionProfileRevision、OutputSpec、failure class、phase 和 status。`organization_id`、`project_id`、`principal_id`、`job_id`、`attempt_id` 等高基数标识不能成为常规 label；需要从指标跳转到单次执行时使用 trace exemplar 或审计查询。

## 20. 技术选型

### 20.1 语言与 Interface

| 领域 | 选择 | 约束 |
| --- | --- | --- |
| 控制面、Worker Agent、Node Agent | Go | 使用同一 Go module；并发状态机、gRPC、Kubernetes controller 和静态 host binary 共用工具链 |
| Inference Backend | Python + SGLang fork | Python 只拥有模型和 GPU 执行，通过 runner Interface 与 Go Worker Agent 隔离 |
| 外部请求 | REST / JSON + OpenAPI | Envoy Gateway 路由；Go 使用 `oapi-codegen` 生成类型，不手写漂移的 request struct |
| 内部协议和事件 schema | Protobuf | gRPC 和 JetStream event 共用版本化 schema，使用 `buf` lint / breaking check |
| PostgreSQL access | `pgx` + `sqlc` | 保留显式 SQL、row lock、CAS 和数据库约束，不使用隐藏事务语义的重型 ORM |
| Schema migration | `goose` | migration 随 release 版本化，生产执行前备份并验证向前兼容 |
| Python environment | `uv` + lockfile | runner 依赖、CUDA / SGLang fork revision 和镜像 digest 一起固定 |

所有具体版本在实现 bootstrap 时锁定到当期稳定版本，并通过镜像 digest、Go/Python lockfile 和 SBOM 进入 release receipt；架构文档不跟随每次 patch version 更新。

### 20.2 数据与可靠事件

| 领域 | 选择 | 生产基线 |
| --- | --- | --- |
| 事实源 | CloudNativePG / PostgreSQL | 三台 Control/Storage Node 各运行一个 replica，同步提交、自动 failover；Barman Cloud Plugin 将 WAL 与 base backup 归档到独立故障域并提供 PITR |
| 事件设施 | NATS JetStream | 3 replicas、PVC-backed file storage、durable consumer、explicit ack、anti-affinity |
| 一致性 | Transactional outbox + idempotent consumer + reconciliation | 不做 PostgreSQL / NATS 双写，不依赖 Broker exactly-once 宣称 |
| Catalog 配置 | YAML authoring + JSON Schema + canonical JSON | 接纳时校验，入库保存 canonical JSON、schema revision 和 content hash，不执行任意模板代码 |
| Billing | PostgreSQL Contract Credit Ledger | 保存 CreditReservation、PricingSnapshot、Charge 和 UsageRecord；只向外部财务系统导出月度 Invoice line，不接支付网关 |
| 金额 | integer minor unit 或 Decimal | 禁止 binary floating point；PricingSnapshot 创建后不可变 |
| 时间 | PostgreSQL clock | Lease、Job Expiry、finalization 和 retry 时间不以 Pod 本地 wall clock 为准 |

### 20.3 平台与存储

| 领域 | 选择 | 生产基线 |
| --- | --- | --- |
| 集群 | Kubernetes + Vela Scheduler | Kubernetes 管 Worker 生命周期，Vela 管 Job placement；裸金属 baseline 为 Ubuntu LTS + RKE2 / containerd |
| GPU | NVIDIA Device Plugin + DCGM Exporter | H3 请求 `nvidia.com/gpu: 8`；host driver / toolkit 版本锁定，不由 Operator 自动升级 |
| Worker rollout | Fleet Controller + `OnDelete` DaemonSet | planned drain / fence 后才删除 Pod；profile 用 node label 和 pool 表达 |
| Artifact | 三节点分布式 S3-compatible store | 运行在 Control/Storage Node 的独立数据盘；private bucket、versioning、conditional create、固定 object version、checksum 和 off-cluster backup；磁盘拓扑不足时切换到已有外部 S3 store |
| 本地对象存储 | MinIO 或 local adapter | 只用于开发和 conformance test，不能把本地通过当作生产 durability / restore 证据 |
| Scratch | 本地 NVMe + XFS project quota | per-Attempt 目录、watermark 背压、明确终态后清理 |
| 镜像与模型 | OCI registry + S3 | 镜像固定 digest；模型权重固定 checksum，Catalog 只保存 revision 和位置 metadata |
| 媒体探测 | FFmpeg `ffprobe` | 固定版本和探测参数，输出解析为结构化 metadata 后再执行 output-spec validation |

### 20.4 安全、可观测性与交付

| 领域 | 选择 |
| --- | --- |
| Gateway | Kubernetes Gateway API + Envoy Gateway |
| 外部身份 | 企业 OIDC + Project Service Principal Credential；凭据只保存 hash / provider subject，支持 scope、过期、轮换和吊销 |
| TLS / mTLS | cert-manager；外部 TLS 和内部 workload certificate 分开签发与轮换；NATS 使用 TLS + operator/account JWT + per-workload NKey / subject ACL |
| Secret | Vault 或现有企业 KMS / Secret Manager + External Secrets |
| Telemetry | OpenTelemetry SDK / Collector + Prometheus / Alertmanager + Grafana + Loki + Tempo |
| GPU telemetry | DCGM Exporter + NVML / PCIe AER host probe |
| Deployment | Helm + Argo CD；GPU host image / driver 使用 Ansible 或 PXE 管理 |
| 本地集成测试 | `testcontainers-go` 启动 PostgreSQL、NATS、S3 fixture；fake Worker / OIDC / Invoice exporter；Python runner 使用 `pytest` |
| 硬件验收 | 独立 H3 staging pool 执行 process kill、网络分区、GPU fault、reboot 和对象存储故障注入 |

### 20.5 明确不采用

| 方案 | 首发决策 |
| --- | --- |
| Temporal | 不采用；无法替代 Vela placement、Lease fencing 和 PostgreSQL Artifact / Billing CAS，引入后会形成第二状态权威。团队已有成熟 Temporal 平台时才重新评估 |
| Kafka | 不采用；当前不需要长期事件历史和超高吞吐 replay，JetStream 运维面更小 |
| Redis Streams | 不承担关键事件或状态；只有出现有测量依据的缓存需求时再引入 Redis |
| K8s + Ray Serve | 不采用，避免 Kubernetes、Ray、Vela 和 SGLang 四层重复调度 |
| Kubernetes Job per inference | 不采用，避免每个请求重新调度、启动和加载 8-GPU 模型 |
| Slurm | 不作为互联网在线 serving 主框架 |
| 纯 systemd + 自研集群管理 | 仅适合很小规模，不能替代 Kubernetes rollout、service discovery 和 declarative lifecycle |
| Nomad | 可行，但当前没有足够收益替换 Kubernetes 生态 |

## 21. 正式生产首发范围

首发面向受邀 Customer Organization，但承载正式业务流量，必须交付以下完整闭环：

- 单地域、单 RKE2 集群；三台 Control/Storage Node 承载 etcd、CloudNativePG、3-replica JetStream 和分布式 S3-compatible storage，GPU Worker 独立部署。
- MiniMax H3 Model / Workload、SGLang fork Inference Backend，以及 `quality`、`balanced`、`fast` 三个已认证 Generation Preset；首发 Service Class 为 `standard`。
- Go 模块化 `vela-control`、Fleet Controller、Worker Agent、host Node Agent 与 Python H3 runner；一台 8-GPU 节点对应一个长期运行的 Worker Pod。
- Customer Organization、Project、企业 OIDC Human Principal、Project Service Principal、固定 RBAC、Organization Isolation、审计和 Break-glass Access。
- REST / OpenAPI 的异步 submit / get / cancel / Artifact / Content Deletion / Webhook Interface，Project-scoped Idempotency-Key 和明确的 402 / 429 / 503 Admission 结果。
- Job、Attempt、Lease、fencing、ExecutionPolicySnapshot、RetryRuntimeState、Job Expiry、Attempt-scoped phase progress、最多 3 次 Attempt 和累计 compute budget。
- 分层公平、Protected Lane、bounded retry lane、风险修正 Admission 和 work-conserving Worker capacity；不向 BUSY Worker 预派任务。
- PostgreSQL 唯一事实源、transactional outbox、JetStream at-least-once wakeup、幂等 consumer 和周期 reconciliation。
- S3 multipart upload、不可变 object version、完整 ArtifactSet validation、Visible Completion 原子提交、signed download、Retention Policy 和 Content Deletion。
- Contract Credit Limit、Admission 时 CreditReservation、离散 OutputSpec SKU、不可变 PricingSnapshot、POSTED Charge、UsageRecord 和月度 Invoice export。
- Project Webhook 的 HMAC 签名、secret 轮换、72 小时重试、dead-letter 可见性和人工重放；GET Job 始终是权威查询。
- Catalog benchmark / certification、N/N-1 兼容发布、warm canary、planned drain、rollback、revision retention，以及 L0-L5 自动恢复、L6 人工审批和自动 Quarantine。
- OpenTelemetry、Prometheus / Alertmanager、Grafana、Loki、Tempo、DCGM、Production Gate、Launch Receipt、告警、runbook、值班和故障注入环境。

正式首发明确不包含：

- 跨 Worker Durable Checkpoint；节点或 NVMe 丢失后从头重算。
- 自动跨地域 failover、跨地域 active-active 或 Vela 管理的跨地域 Artifact 复制。
- 多节点 LLM ExecutionProfileRevision。
- 无人审批的 BMC power cycle。
- 基于机器学习的复杂 ETA；首发使用可校准的统计模型。
- 严格单 Job Hard Deadline 或 CapacityReservation。
- 支付网关、银行卡预扣、钱包、自动收款和发票结算。
- 客户自定义 RBAC，以及默认使用 Customer Content 训练、benchmark 或人工质检。

## 22. 验收场景

1. 相同 Project 与 Idempotency-Key 的重复提交只原子创建一个 QUEUED Job、CreditReservation、PricingSnapshot、ExecutionPolicySnapshot 和 Outbox event；相同 key 搭配不同请求返回冲突。
2. 信用不足、Project 限额、容量不足分别返回 402、429、503；任何 Admission 失败都不创建 Job 或 CreditReservation。
3. 多个 Project 并发 Admission 锁定同一 Organization credit row，不能让 reserved 加 posted 超过 Contract Credit Limit。
4. QUEUED / ASSIGNED 取消赢得 CAS 时直接进入 CANCELED 并释放 CreditReservation；RUNNING / FINALIZING 取消赢得 CAS 时立即 fence、POSTED 全额 Charge 并进入 CANCELING，Worker 确认或 Lease 过期后进入 CANCELED。
5. complete 与 cancel 并发时只有一个 CAS 获胜；Job、ArtifactSet、CreditReservation 和 Charge 始终形成同一个业务结果。
6. 除 Billable Start 后 Customer Cancellation 外，所有 terminal FAILED Job 都释放 CreditReservation 且不形成 Charge。
7. Scheduler 在 Assignment 事务前后崩溃，Job 不会卡死；BUSY Worker 不接收预派任务，Organization -> Service Class -> Project -> Job 的公平性、aging、Protected Lane 和 retry lane 仍有界。
8. Worker 网络分区后旧 Attempt 完成返回 `RejectedStaleLease`；最多 3 个 Attempt、累计 compute / finalization budget 和 Job Expiry 中任一耗尽都会停止重试。
9. Progress 只描述当前 Attempt 的当前 Execution Phase；重试后允许重置，`job_expires_at` 和 Dynamic ETA 均不会被呈现为 Hard Deadline。统计 SLO 必须按 UTC 月度 Accepted/QUEUED cohort，对 exact ModelRevision、GenerationPresetRevision、ServiceClassRevision、OutputSpec 和 generation count 独立计算；p95 从 `jobs.created_at` 到 Visible Completion，FAILED/expiry 进入成功率分母，Customer Cancellation 单列，open/低样本/缺 target/mixed revision 一律不能 PASS。Slice 37 用 migration 00033、统一 Go evaluator、strict typed evidence、Pod-private metrics、burn-rate rules/dashboard/runbook 和复合迁移测试建立 repository conformance；真实月度流量、H3 结果、paging/P1 演练和 Launch Receipt 仍属于 Production Gate。
10. multipart upload 中断后，同一 Worker 节点且本地源仍存在时可继续上传；节点或 NVMe 丢失时不声称跨 Worker 恢复，而是按 Retry Budget 从头重算。Slice 30 已通过 production Worker Agent、mTLS WorkerControl、PostgreSQL 和 versioned MinIO 直接证明：同 Worker/epoch/Attempt/fence 的进程丢失只续传剩余 multipart parts，不重跑 Runner；首个 Worker 的本地恢复根不可访问并被判定为 `LOST` 后，不同 Worker 以 higher-fence Attempt 2 从空本地根重算，最终仍只有一个 Visible Completion、ArtifactSet 和 Charge（`864c134`，review closure `5a9bad6`）。真实 H3/XFS/NVMe 故障注入与 Launch Receipt 仍属于 Production Gate。
11. Worker 在对象上传完成、ArtifactSet commit 前失联时，Artifact Reconciler 可验证并提交已上传对象或安全清理；同一 object key 覆盖写被拒绝，COMMITTED Artifact 固定到不可变 object version。
12. duration、resolution、frame count、codec、generation count 或任一必需缩略图不符合 OutputSpec 时，整个 ArtifactSet 不能发布或计费。
13. 两个 Attempt 同时完成时，只有一个能原子形成 Visible Completion 和一条 Charge；未获胜 Attempt 的对象按 Retention Policy 清理。
14. RateCardRevision 在 Job 排队期间更新，既有 Job 仍使用原 PricingSnapshot；Retry 可以更换 ExecutionProfileRevision，但必须保持原 Model、Generation Preset、Service Class 和 OutputSpec，并持有有效 ProfileCertification。
15. ProfileCertification 因质量回归失效后立即阻止新 Assignment；旧 revision 在所有 Job、Attempt、Artifact 和审计引用归零前不会删除。
16. Artifact Store 故障停止受影响 pool 的新 Assignment；scratch high watermark 只停止对应 Worker / pool，存储探测和 low watermark 同时恢复后才重新接纳。
17. H3 Worker 请求 `nvidia.com/gpu: 8` 并受专用 taint / label 约束；Fleet Controller 之外的 Pod / pool delete、selector / image patch 和 Argo prune 被 RBAC、admission 和 finalizer 拒绝。
18. Remediation Operation 的 node identity、GPU UUID / PCI BDF 或 Worker epoch 不匹配时拒绝执行；L0-L5 只按认证矩阵自动执行，L6 没有人工审批不得执行，失败验证自动 Quarantine。
19. 失败对象 24 小时、scratch 最多 24 小时、授权 debug dump 最多 72 小时、成功 Artifact / prompt 30 天后按策略删除；Content Deletion 提前删除内容但保留法定 Charge 和审计记录。Slice 31 已用独立最小权限角色、versioned MinIO 双 bucket 和真实 PostgreSQL dump/restore 证明：只为 COMMITTED Artifact 创建 off-cluster backup target，删除覆盖 backup key 的全部 versions/delete markers，恢复点位于 deletion authority 之后时可重放 PRIMARY 与 OFF_CLUSTER_BACKUP targets 而不复活对象，并保留 prompt tombstone、Charge 与 actor attribution。Slice 32 为每个 COMMITTED Artifact 将冻结的 PRIMARY exact version 复制到 versioned backup，记录不可变证据，并通过同一 PostgreSQL row lock 串行化复制与删除。Slice 33 增加独立 Compliance Principal，以不可变事件在精确 Organization、Project 或 Job 范围放置/释放只覆盖 `METADATA`/`FINANCIAL` 的 Legal Hold，且不能保存 Customer Content。Slice 34 从 canonical terminal event 或 Finance Reconciliation `posted_at` 建立 365/2557 天时钟，与 active hold 串行化后物理删除 Job、Attempt 和 financial source row，并以最小 root 保留独立证据。Slice 38 用 Barman Cloud Plugin 在 fresh kind/MinIO 环境完成 local base backup、目标 WAL 归档和 timestamp restore。真实 provider/network 故障证据、早于 deletion authority 的恢复点、生产独立故障域 PITR、expiry/failover/observability 与 Launch Receipt 仍属于 Production Gate。
20. JetStream 短暂或整体不可用后，Outbox 保留发布意图，reconciliation 从 PostgreSQL 恢复待调度 Job；Invoice exporter 不可用不阻塞 Visible Completion 或 Artifact access，恢复重试以 `charge_id` 幂等且不产生重复 Invoice line。
21. `begin_finalization()` 在事务提交前后崩溃，重放仍只得到一组 Artifact / ArtifactUpload，并保留原 `finalization_deadline_at`。
22. OFFLINE Worker 恢复后必须经过身份、设备、Inference Backend、model warm-up 和 canary 检查，达到 HEALTHY + READY 才重新接收 Assignment。
23. Outbox dispatcher 只有收到 3-replica stream 的 quorum `PubAck` 才标记 published；在 `PubAck` 前失败保留 row，在 `PubAck` 后标记前崩溃以同一 `Nats-Msg-Id` 重试，consumer 通过 event id、aggregate version 和 CAS 幂等处理。
24. Assignment wakeup 被 ack 后 Worker 执行 40 到 50 分钟，JetStream 不保留长期 pending delivery；Assignment gRPC response 丢失时，同一 Worker epoch 重放原 Assignment，不产生第二个 Attempt、fence 或 Lease 延期。
25. PostgreSQL 或控制面失联时，Worker 按服务端 `lease_valid_for` 和本地 monotonic deadline fail closed；修改 wall clock 不能延长执行权。
26. PostgreSQL 单节点自动 failover 后，已接纳 Job、Outbox、Lease fence、CreditReservation 和 Charge 不丢失；数据库无 quorum 时不接受 Admission 或创建新 Assignment。
27. 使用无权限或其他 workload 的 NATS credential 访问受限 subject 被拒绝；Organization A 的任何角色、Project credential、signed URL 或复合外键都不能读取或引用 Organization B 的数据。
28. Webhook endpoint 超时、返回非 2xx 或 Dispatcher 崩溃时，同一 `event_id` 允许重复投递并在 72 小时后进入 dead letter；签名可验证，payload 不含 Customer Content，GET Job 返回最终权威状态。
29. Credential 轮换、撤销和 Break-glass 到期不会丢失 Principal 审计归因；BillingAdmin 和 OrganizationAuditor 默认不能读取 prompt 或 Artifact。
30. N 与 N-1 控制面、Worker、event 和 schema 在长任务 rollout 中共存；升级、回滚和 drain 不终止 Accepted Job，旧 event backlog 能由新 consumer 正确处理。Slice 29 已用 exact adjacent N-1 control、Admission、Outbox、Scheduler 与 Worker probe，在 schema 27 上直接证明 retained raw event 由 current Inbox/Scheduler 接收、active Lease 在 current Fleet drain 中继续、exact N-1 rollback writer 保持 authority，并在 CNPG 无同步 quorum 时由 current 与 N-1 writer 共同 fail closed（`21e0781`）；真实 Kubernetes 长任务 rollout 与 production backlog receipt 仍属于 Production Gate。

## 23. 已决事项、Production Gate 与 Future Work

### 23.1 正式首发已决事项

- 首发是受控客户范围内的正式 B2B production service，不把真实流量当 beta 或 MVP 流量。
- Customer Organization 拥有合同、信用与结算，Project 隔离 credential、额度、Artifact namespace 和审计；固定 RBAC、RLS 和复合外键强制 Organization Isolation。
- PostgreSQL 是唯一事实源；JetStream、Kubernetes、Worker 本地状态和 Webhook 都不能单独推进 Job。事件可靠性由 transactional outbox、幂等 consumer 和 reconciliation 共同保障。
- Billing 使用 Contract Credit Ledger 和月度 Invoice export，外部收款不属于 Job 生命周期。Admission、Visible Completion 和 Customer Cancellation 的计费边界均在单一数据库事务内确定。
- 三个 Generation Preset 与 `standard` Service Class 分离，价格由 certified OutputSpec SKU 决定；SLO 是统计承诺，Job Expiry 不是 Hard Deadline。
- Scheduler 使用分层公平、Protected Lane 和 bounded retry lane，同时保持 Worker capacity work-conserving，不保留硬空闲故障备用。
- Artifact 存入 S3-compatible store并以完整 ArtifactSet 原子发布；首发没有跨 Worker Durable Checkpoint，节点丢失后从头重算。
- 三台 Control/Storage Node 提供基本单节点容错；站点级恢复依赖 off-cluster backup 和人工 runbook，不建设跨地域 active-active。
- 普通 release 不中断 Accepted Job，使用 N/N-1 compatibility、expand / backfill / switch / contract、canary、drain 和可验证 rollback。
- 控制面使用 Go 模块化单体，Python 只保留在 Inference Backend runner；首发不引入 Temporal、Kafka、Redis 或 Ray 调度层。

### 23.2 硬 Production Gates

每个 Gate 必须产生绑定 release digest、configuration revision、验证环境、原始结果、owner、时间和阈值的 Launch Receipt。缺失或失败的 receipt 不能因客户熟悉、口头豁免或“上线后再修”而标记 PASS。

Slice 35 提供严格 manifest loader、evidence bytes SHA-256 复算、显式
`vela-verify-launch` release check，以及绑定 sealed receipt 的三 Preset、
RateCard 与 `ACTIVE` Catalog promotion authority。它只闭合 repository
enforcement。Slice 39 为八个非 observability Gate 增加固定 typed semantic
contract、typed artifact aggregate 和 Catalog plan/evidence 精确绑定；现有
observability schema 保持独立版本。Slice 40 再以一个 canonical release
bundle 从 exact final renders、host packages、Node Agent unit、per-Worker
materialization、logical WorkerPool 下 hostname-pinned placement、external
Secret/ConfigMap revisions 和 OCI manifest/config bytes 推导 release digest 与
configuration revision；`vela-verify-launch` 与
Catalog promotion 都必须重新验证该 bundle，并在数据库 transaction 前精确匹配
receipt 绑定。Registry publication、signature、SBOM、vulnerability approval、
真实 PKI/Secret、生产节点 materialization 与部署仍是外部 release responsibility。
仓库内 bundle、fixture 和测试不是 Launch Receipt，当前仍为 `0/9 PASS`。

| Gate | PASS 证据 | 未通过时行为 |
| --- | --- | --- |
| 三 Preset certification | `quality`、`balanced`、`fast` 对每个可售 OutputSpec 的版本化 corpus、质量阈值、p50 / p95、成功率、成本、统计置信和 ProfileCertification | 对应组合不能 ACTIVE 或进入 RateCard |
| 72 小时真实 H3 soak | 真实硬件、混合 OutputSpec、故障后排队、scratch / storage 压力和 N/N-1 版本共存；无丢失 Accepted Job、重复 Visible Completion 或重复 Charge，Preset SLO 达标 | 不开放正式流量 |
| 状态与事件 fault injection | process kill、网络分区、node reboot、Outbox commit / `PubAck` / consumer ack / Assignment response 各崩溃窗口，以及 retry budget / stale fence receipt | 不开放正式流量 |
| GPU remediation | 每个 GPU SKU、PCIe topology、kernel、driver、firmware 的 L0-L5 认证、限频、身份校验、post-check、canary、Quarantine 和 L6 审批 receipt | 未认证动作 fail closed；关键自动恢复缺失则不开放正式流量 |
| Organization Isolation 与内容安全 | 跨组织、跨 Project、各固定角色、Credential 撤销、signed URL、RLS / 复合外键负向测试，以及 Break-glass 和 Customer Content 不复用审计 | 不开放正式流量 |
| 数据与灾备 | 单节点 RPO 0 / RTO <= 5m、PostgreSQL PITR、站点 metadata RPO <= 15m / RTO <= 4h、JetStream rebuild、Outbox replay、Artifact backup 抽样恢复、secret rotation 和季度演练 receipt | 不开放正式流量 |
| Release 与 rollback | expand / backfill / switch / contract、N/N-1 REST / Protobuf / event / Worker compatibility、长任务 drain、旧 backlog 消费和 rollback receipt | 该 release 不得上线 |
| 商业与数据生命周期 | Admission 错误码、并发 credit、取消竞争、失败不计费、Invoice export、Webhook 72 小时重试、Retention Policy 和 Content Deletion 的端到端 receipt | 不开放正式流量 |
| Observability 与 on-call | API / Preset SLO dashboard、paging alert、runbook、备份恢复、GPU / storage / event 故障演练和一次完整 P1 response exercise，明确 24x7 owner | 不开放正式流量 |

### 23.3 Future Work

- H3 DiT Durable Checkpoint：只有故障阶段分布证明预期节省的重算时间和成本显著高于写入、存储、吞吐影响和恢复失败成本时才启用。
- 多节点 LLM ExecutionProfileRevision：单独设计 gang placement、跨节点 Lease、拓扑和部分节点失效语义，不扩大 H3 首发 Interface。
- Hard Deadline 产品：先实现持久 CapacityReservation 和可证明的 Admission，再销售单 Job 最晚完成时间。
- 自动跨地域 failover、跨地域 Artifact policy 和更复杂的 Dynamic ETA。

## 24. 参考资料

- [Kubernetes Device Plugins](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/)
- [Kubernetes Dynamic Resource Allocation](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
- [Kubernetes Node Problem Detector](https://github.com/kubernetes/node-problem-detector)
- [NVIDIA NVML Device Queries](https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html)
- [NVIDIA System Management Interface](https://docs.nvidia.com/deploy/nvidia-smi/)
- [Linux PCI Support Library](https://docs.kernel.org/driver-api/pci/pci.html)
- [NVIDIA GPU Operator Installation](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/getting-started.html)
- [Ray Serve on Kubernetes](https://docs.ray.io/en/latest/serve/production-guide/kubernetes.html)
- [Slurm Configuration](https://slurm.schedmd.com/slurm.conf.html)
- [Amazon S3 Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
- [Amazon S3 Conditional Writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Amazon S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
- [PostgreSQL Synchronous Replication](https://www.postgresql.org/docs/current/warm-standby.html#SYNCHRONOUS-REPLICATION)
- [CloudNativePG Documentation](https://cloudnative-pg.io/documentation/current/)
- [gRPC Documentation](https://grpc.io/docs/)
- [RKE2 Documentation](https://docs.rke2.io/)
- [Envoy Gateway](https://gateway.envoyproxy.io/)
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Argo CD Documentation](https://argo-cd.readthedocs.io/en/stable/)
- [cert-manager Documentation](https://cert-manager.io/docs/)
- [External Secrets Operator](https://external-secrets.io/latest/)
- [FFprobe Documentation](https://ffmpeg.org/ffprobe.html)
- [Testcontainers for Go](https://golang.testcontainers.org/)
