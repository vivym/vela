# Vela 架构设计

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft |
| 日期 | 2026-08-20 |
| 首个工作负载 | MiniMax H3 文生视频 |
| 长期定位 | 通用 AI 推理集群控制面 |

## 1. 摘要

Vela 是面向大规模 AI 推理集群的控制面。它接收耗时从数分钟到数十分钟的异步推理任务，根据模型、质量档位、执行拓扑、机器健康度和队列负载选择 Worker，并负责重试、产物发布、计费和故障恢复。

Vela 不实现模型内部的张量并行或流水线执行。具体推理由 SGLang fork、vLLM 或其他 Inference Backend 完成；Vela 决定什么任务应当放在哪个 Worker 上、以哪个 ExecutionProfileRevision 执行，以及执行失败后如何恢复。

MiniMax H3 是第一个 Model / Workload，SGLang fork 是它的第一种 Inference Backend。每台 H3 机器有 8 张 GPU，其中 1 张负责 Encoder 和 VAE Decoder，另外 7 张负责 DiT。由于 PCIe 带宽低，8 张卡对 serving plane 是不可拆分的整体。Kubernetes 管理长期运行并已预热的 Worker，Vela Scheduler 管理单个推理 Job，SGLang fork 管理节点内部的 8 卡执行。

系统的核心可靠性语义是：

> At-least-once execution, exactly-once visible completion.

物理计算可能因网络分区或 Worker 丢失而重复发生，但只有一个 Attempt 可以正式发布 Artifact 和触发用户计费。

## 2. 目标与非目标

### 2.1 目标

- 提供异步 AI 推理接口，支持最长 40 到 50 分钟或更久的任务。
- 将一台机器或多机拓扑抽象成一个可调度的 Worker / ExecutionProfileRevision。
- 根据预计完成时间、优先级、租户配额、模型预热状态和硬件风险进行调度。
- 在 Worker、GPU、驱动或网络故障后自动重试，并避免重复发布和重复计费。
- 将视频、缩略图和 checkpoint 等 Artifact 可靠写入对象存储。
- 支持不同质量和有损加速档位，并为每种用户可见 GenerationPresetRevision 配置独立计费。
- 对异常 GPU 执行摘流量、恢复、验证和重新入池。
- 为未来的 LLM、图像、多模态 Model 和其他 Inference Backend 保留扩展能力。

### 2.2 非目标

- Vela 不替代 SGLang、vLLM 或模型专用推理引擎。
- Kubernetes 不感知 Encoder、DiT、VAE 等模型内部阶段。
- 第一版不支持任意 DiT step 的透明 checkpoint/resume。
- 第一版不承诺跨地域强一致调度或 Artifact 同步复制。
- 第一版不要求构建通用 GPU 云或训练调度平台。

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
- 最大重试次数、总计算预算和退避参数。
- DiT latent checkpoint 的大小、写入成本和恢复收益。
- GPU 错误分类与每一级 remediation 的成功率和安全条件。

## 4. 核心设计原则

1. **Worker 是 serving 资源。** H3 第一版中，一台 8-GPU 机器等于一个 Worker。
2. **Kubernetes 只管理 Worker 生命周期。** 单个 Job 由 Vela Scheduler 调度。
3. **Inference Backend 封装节点内部执行。** Vela 不理解 Inference Backend 内部 tensor movement。
4. **Job、Attempt 和 Lease 分离。** Job 表示用户意图，Attempt 表示一次物理执行，Lease 表示限时执行权。
5. **状态存储是事实源。** 队列用于唤醒和传递事件，不能成为唯一事实源。
6. **计算允许重复，发布只能一次。** 使用 fencing token 和 compare-and-swap 选择唯一获胜 Attempt。
7. **Serving domain 与 fault domain 分离。** 单张 GPU 是 fault domain，整台 8-GPU 机器是 H3 serving domain。
8. **恢复逻辑不依赖被恢复对象。** 特权 Node Agent 运行在 host systemd 下，不依赖 GPU Pod 或 container runtime。
9. **GenerationPresetRevision 是用户承诺，ExecutionProfileRevision 是内部手段。** 重试不得静默降低用户购买的质量档位。
10. **用户计费与内部成本分离。** 平台重试增加内部 COGS，但不重复向用户收费。

## 5. 系统上下文

```text
Client
  |
  v
API Gateway
  |
  v
Job Coordinator -------- PostgreSQL + Outbox
  |       |
  |       +------------- Billing Ledger -------- External Billing System
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
                                                   +-- wake Scheduler / Billing / Fleet / Reconciler

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

负责认证、租户识别、限流、请求大小限制和外部协议适配。API Gateway 不代理视频上传或下载，也不保存 Job 状态。

外部 Interface 固定为 REST / JSON，并由 OpenAPI 描述；Envoy Gateway 负责 TLS termination、基础限流和路由，`vela-control` 负责租户认证、幂等和领域校验。第一版客户端接口：

```text
POST   /v1/jobs
GET    /v1/jobs/{job_id}
POST   /v1/jobs/{job_id}/cancel
GET    /v1/jobs/{job_id}/artifacts
```

`POST /v1/jobs` 必须接受 `Idempotency-Key`，成功接纳后返回 `202 Accepted` 和 `job_id`。

### 6.2 Job Coordinator

Job Coordinator 是异步执行语义的核心模块。它是唯一可以改变 Job、Attempt、Lease 和 RetryRuntimeState 的模块，并封装取消竞争、ArtifactSet commit 和计费事件。Scheduler、Worker、Billing consumer 和 reconciler 只能向它提交带预期版本的事件，不能直接更新这些状态。

客户端接口保持较小：

```text
submit(request, idempotency_key) -> JobHandle
get(job_id)                     -> JobView
cancel(job_id)                  -> CancelResult
```

Worker 协议：

```text
acquire(worker_epoch, capacity)                 -> Assignment?
heartbeat(lease_credentials, progress)          -> Continue | Stop
complete(lease_credentials, artifact_set_candidate) -> Accepted
                                                      | RejectedStaleLease
                                                      | RejectedJobTerminal
fail(lease_credentials, failure)                -> RetryDecision
```

MVP 的 Worker transport adapter 固定为 Protobuf / gRPC 双向流。Worker 主动建立 mTLS 连接，只在本地 capacity 可用时调用 `acquire()`；服务端从 mTLS 身份解析 `worker_id`，请求显式携带持久化到本进程状态的 `worker_epoch`。Coordinator 通过同一流或 heartbeat 响应返回 `Continue`、`Stop`、`Drain` 和 Lease 更新。`acquire()` 是 read-or-create 操作：同一 `(worker_id, worker_epoch)` 已有未终结 Assignment 时，必须先重放原 `attempt_id`、Lease token、fence 和原始 `expires_at`，不能创建第二个 Attempt 或因重放延长 Lease。Assignment 一旦在 PostgreSQL 提交即可确认传输事件，40 到 50 分钟的执行所有权由 Lease / fence 保证，不能依赖一条长期 unacked 的 NATS 消息。

Job Coordinator 对内暴露上述小 Interface，生产使用 gRPC adapter，状态机测试使用 in-memory adapter。HTTP、gRPC 和 NATS transport 都不能绕过 Job Coordinator 直接修改持久状态。

### 6.3 Scheduler

Scheduler 从持久状态中选择可运行 Job 和符合条件的 Worker。它不拥有 Job 状态，只能通过 Job Coordinator 的事务操作创建 Attempt 和 Lease。

Scheduler 负责：

- Admission control 和租户配额。
- Job 优先级、aging 和公平性。
- ExecutionProfileRevision 与 Worker capability 匹配。
- 模型预热和数据 locality。
- 预计运行时间和队列完成时间计算。
- 重试时避开已知故障 Worker 或 fault domain。
- 防止失败任务引发 retry storm。

H3 MVP 使用中央队列，每个 Worker 同时最多运行一个 Job，BUSY Worker 不接受预派任务。Scheduler 不维护 per-Worker queue。

### 6.4 Worker Registry

Worker Registry 保存 Worker 的身份、epoch、capability、模型预热状态、当前 Assignment、GPU 拓扑、Lifecycle State 和 Reachability Condition。

Worker 重启后必须递增 `worker_epoch`。旧 epoch 签发的 Lease 不能在新进程中继续使用。

### 6.5 Inference Worker

Inference Worker 是长期运行并已预热的进程。H3 第一版中，一个 Pod 独占整台 8-GPU 节点，并拆成两个职责明确的进程：Go `vela-worker-agent` 管理 Assignment、Lease、heartbeat、ArtifactUpload 和 finalization；Python H3 runner 封装 SGLang fork，并协调 Encoder、DiT 和 VAE 进程。

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

Billing Ledger 保存不可变报价快照、BillingAuthorization、用户 Charge 和内部 UsageRecord。BillingAuthorization 记录外部 authorization id、保留金额、有效期和续期状态。Ledger 通过 transactional outbox 接收 Job 状态事件；外部调用的幂等键为 `(job_id, operation_type, operation_generation)`，其中同一轮重试复用 generation，每次新的 renewal 先事务性递增 generation，避免多轮续期互相折叠。

### 6.9 Model Catalog

Model Catalog 管理 ModelRevision、InferenceBackendRevision、ExecutionProfileRevision、GenerationPresetRevision 和 ProfileCertification。它拥有不可变定义、兼容关系、认证状态和 revision retention；Scheduler 对普通流量只能选择 `ACTIVE` 且认证有效的 revision，对显式 canary 流量可以选择 `CANARY` revision。

### 6.10 Fleet Controller

Fleet Controller 将 Catalog 中的 ExecutionProfileRevision 实现为 Kubernetes Worker pool，并负责 warm-up、canary、planned drain、rollout 和 retirement。它只能通过 Job Coordinator 请求 drain/fence，不能直接终止仍拥有有效 Lease 的 Worker。

由于 Fleet Controller / Node Health Controller 与 `vela-control` 是不同进程，`vela-control` 提供仅供其 service identity 调用的 mTLS gRPC maintenance Interface：

```text
request_drain(operation_id, worker_id, expected_epoch, reason, deadline) -> DrainOperation
get_drain(operation_id)                                                  -> DrainStatus
request_fence(operation_id, worker_id, expected_epoch, reason)          -> FenceResult
```

`operation_id` 是幂等键；Job Coordinator 在 PostgreSQL 事务中完成 Lifecycle State 转换、停止新 Assignment 和 Lease fencing。Controller 只有得到持久化的完成状态后才能要求 Kubernetes 删除 Pod 或要求 Node Agent 执行恢复动作，不能直写 Job / Worker 表。

### 6.11 Artifact Validator / Reconciler

Artifact Validator 验证对象身份、checksum、媒体规格和完整结果集。Artifact Reconciler 修复 Worker 在 multipart upload 完成后、ArtifactSet commit 前失联留下的中间状态，并清理无法恢复的 upload session 和孤儿对象。二者通过 Artifact Store interface 工作，不直接依赖具体对象存储产品。

Artifact Reconciler 不持有 Worker 的执行凭据。Job Coordinator 只能在原 Attempt 尚未被替代、ArtifactUpload 可恢复且 finalization budget 未耗尽时，为它签发同一 fence 的 FINALIZATION Lease。该 Lease 只能上传、验证和提交既有结果，不能重新运行推理。

### 6.12 实现与部署单元

逻辑 Module 不等于独立网络进程。MVP 使用同一个 Go module / repository，并只在权限、生命周期或真实远程依赖不同处拆部署单元：

| 部署单元 | 语言 | 包含的 Module | 拆分原因 |
| --- | --- | --- | --- |
| `vela-control` | Go | HTTP adapter、Job Coordinator、Scheduler、Worker Registry、Model Catalog、Billing Ledger、Artifact Validator / Reconciler、outbox dispatcher | 共享 PostgreSQL 事务和领域不变量，保持模块化单体 |
| `vela-fleet-controller` | Go | Fleet Controller、Node Health Controller | 独立 Kubernetes RBAC 与 rollout 生命周期 |
| `vela-worker-agent` | Go | Worker protocol、Lease client、Artifact upload / validation client | 与推理 runner 分离，保持长连接和恢复语义稳定 |
| H3 runner | Python | SGLang fork、GPU role binding、模型执行 | 保留 Python / CUDA 推理生态 |
| `vela-node-agent` | Go | allowlisted remediation executor | host systemd 高权限进程，不依赖 Kubernetes 或 container runtime |

`vela-control` 可以运行多个相同 replica；后台循环使用 row claim、advisory lock 或唯一约束竞争，不为同进程内的 Scheduler、Catalog 和 Billing 增加网络 Interface。对象存储、支付、Kubernetes、Worker、Inference Backend，以及跨进程的 Fleet / Node Health maintenance command 这些真实变化点定义 adapter seam。

## 7. 领域模型

| 概念 | 定义 | 关键不变量 |
| --- | --- | --- |
| Job | 用户的一次推理意图 | 请求、报价和执行策略快照创建后不可变 |
| Attempt | Job 的一次物理执行 | 一个 Job 可有多个 Attempt |
| Lease | Worker 对 Attempt 的限时执行权 | 区分 EXECUTION / FINALIZATION phase，包含鉴权 token、单调 fence、owner epoch 和 expiry |
| Worker | 对外可调度的执行实体 | H3 中为完整 8-GPU appliance |
| ModelRevision | 确切的模型权重和配置版本 | 可复现，不使用浮动 latest |
| InferenceBackendRevision | 推理引擎及其适配代码版本 | 与 ModelRevision 的兼容性已验证 |
| ExecutionProfileRevision | 内部执行拓扑和加速方法 | 必须具有有效 ProfileCertification |
| GenerationPresetRevision | 用户可选择的质量、速度和 SLA 档位 | 独立版本化，不暴露硬件细节 |
| ProfileCertification | GenerationPresetRevision 与 ExecutionProfileRevision 的认证关系 | 有基准、指标、有效期和失效状态 |
| RateCard | GenerationPresetRevision 的价格规则 | 有生效区间并生成不可变快照 |
| PricingSnapshot | Job 接纳时锁定的报价依据 | 排队期间调价不影响既有 Job |
| BillingAuthorization | 为 Job 保留的余额或外部支付 hold | 覆盖金额、有效期和续期状态可审计 |
| ExecutionPolicySnapshot | Job 接纳时锁定的执行策略 | retry、deadline 和取消语义不随配置漂移 |
| RetryRuntimeState | Job 当前的动态重试状态 | 与 Attempt 终态在同一事务更新 |
| Artifact | 视频、缩略图、checkpoint 或 debug dump | 每个 Attempt 写独立不可变对象 |
| ArtifactSet | 一个成功 Job 对外发布的完整 Artifact manifest | 所有必需输出一起发布，不允许部分可见 |
| ArtifactUpload | 一次可恢复的对象上传会话 | 记录 multipart、校验和验证状态 |
| UsageRecord | 一次 Attempt 的实际资源消耗 | 用于内部 COGS，不等于用户 Charge |
| Charge | 用户需要支付的金额 | 每个成功 Job 最多 capture 一次 |

### 7.1 ExecutionProfileRevision 与 GenerationPresetRevision

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

`GenerationPresetRevision` 面向用户并定义可测量承诺，例如：

```yaml
id: fast
revision: 3
model_revision: minimax-h3-2026-08
quality_class: fast
quality_contract:
  benchmark_revision: h3-video-quality-v2
  min_quality_score: 0.82
  max_p95_runtime_seconds: 1800
```

`ProfileCertification` 建立 GenerationPresetRevision 与 ExecutionProfileRevision 的有效映射：

```yaml
generation_preset_revision: fast@3
execution_profile_revision: h3-lossy-fast-v3@1
benchmark_revision: h3-video-quality-v2
quality_score: 0.85
performance_receipt: benchmark://h3-fast-v3-20260820
status: active
certified_at: 2026-08-20T00:00:00Z
invalidated_at: null
```

一个 GenerationPresetRevision 可以认证多个满足同一质量承诺的 ExecutionProfileRevision。平台可以升级硬件或 Inference Backend，但不能在重试时把 `quality` Job 改为 `fast`，也不能选择 ProfileCertification 已失效的 ExecutionProfileRevision。

## 8. 持久化与消息传递

### 8.1 权威状态

PostgreSQL 是以下数据的权威事实源：

- Job、Attempt 和 Lease。
- RetryRuntimeState、Worker epoch 和 Assignment。
- Artifact、ArtifactSet、ArtifactUpload 和获胜 ArtifactSet 指针。
- Model Catalog revision、ProfileCertification 和 rollout 状态。
- PricingSnapshot、BillingAuthorization、ExecutionPolicySnapshot、UsageRecord 和 Charge 状态。
- Outbox 事件和恢复审计记录。

第一版使用数据库行锁和 `SELECT ... FOR UPDATE SKIP LOCKED` 实现 Job claim。控制面所有 Lease expiry、deadline 和重试时间比较以 PostgreSQL 时间为准，不能依赖各 Pod 的 wall clock。Worker 只使用服务端返回的 `lease_valid_for` 和本地 monotonic clock 做保守的 fail-closed 倒计时，不能自行延长 Lease。生产 PostgreSQL 必须启用同步提交、跨故障域同步副本、自动 failover、PITR 和定期恢复演练；数据库不可用时系统停止创建新 Assignment。

### 8.2 队列语义

MVP 的可靠事件设施固定为 3-replica NATS JetStream。JetStream 使用 file storage、跨节点 anti-affinity、durable consumer 和 explicit ack；它承载 Job ready、状态变化、Billing、Fleet 和 reconciliation wakeup，但不保存唯一业务状态。消息只携带 `event_id`、aggregate type / id、aggregate version、event type 和 schema version。

所有关键状态变更与 outbox event 必须在同一 PostgreSQL 事务中提交。Dispatcher 以 `event_id` 作为 `Nats-Msg-Id` 发布，并且只有收到目标 replicated stream 的 quorum-committed `PubAck` 后，才能在独立 PostgreSQL 事务中标记 outbox row 为 published，同时记录 stream 和 sequence receipt。publish timeout、negative ack 或连接中断都视为未发布，以同一 `Nats-Msg-Id` 重试；JetStream duplicate window 必须覆盖 dispatcher 的最大重试间隔，但 broker 去重只用于减少重复，正确性仍由消费者幂等保证。若在 `PubAck` 成功、标记前崩溃，会产生重复消息而不是丢消息。消费者必须先读取 PostgreSQL 当前状态，再以 `event_id`、aggregate version、唯一约束或 compare-and-swap 幂等处理，并且只在本地事务提交后 ack。

Scheduler 和其他关键 consumer 必须有周期性 reconciliation scan，即使 JetStream 整体不可用、消息过期或 consumer state 丢失，也能从 PostgreSQL 重新发现可调度 Job、过期 Lease、待完成 ArtifactSet 和待处理 Billing event。JetStream 恢复后，outbox dispatcher 继续发布积压事件。

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
PENDING_AUTH -> QUEUED -> ASSIGNED -> RUNNING -> FINALIZING -> SUCCEEDED
      |           ^          |          |           |
      |           |          +----------+-----------+
      |           |                     |
      |           +------ RETRY_WAIT <---+
      v
   REJECTED

PENDING_AUTH / QUEUED / RETRY_WAIT -> CANCELED
ASSIGNED / RUNNING / FINALIZING -> CANCEL_REQUESTED -> CANCELED
PENDING_AUTH / QUEUED -> REJECTED  (authorization 或动态 admission 在执行前失败)

PENDING_AUTH 超过 deadline -> REJECTED
QUEUED / RETRY_WAIT 超过 retry budget 或 deadline -> FAILED
ASSIGNED / RUNNING / FINALIZING 超过 retry budget 或 deadline -> fence Attempt -> FAILED
```

终态为 `REJECTED`、`SUCCEEDED`、`FAILED` 和 `CANCELED`。`REJECTED` 只适用于已经创建、但未通过余额预授权或授权后动态 admission 的 Job；hard admission 的同步拒绝不创建 Job。终态 Job 不得重新进入执行态。

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
2. API Gateway 完成认证、限流和基础校验。
3. Job Coordinator 解析 ModelRevision、GenerationPresetRevision、输出规格和 ExecutionPolicySnapshot。
4. Scheduler 执行 hard admission，检查 ExecutionProfileRevision 可用性、租户配额、队列上限、对象存储健康和 scratch 水位。失败时在短事务中持久化 `(tenant_id, idempotency_key, request_hash)` 和稳定的拒绝响应，不产生预授权或 Job；相同 key 的重复请求返回相同拒绝结果。
5. Billing Ledger 生成 PricingSnapshot。
6. 同一数据库事务写入 `PENDING_AUTH` Job、两个 snapshot、空的 RetryRuntimeState、idempotency record 和 authorization outbox event。
7. 返回 `202 Accepted`、`job_id`、锁定报价和 `PENDING_AUTH` 状态。
8. Billing consumer 使用 `job_id` 幂等调用余额或支付 adapter。
9. 预授权成功后持久化 authorization id、金额和 `authorization_expires_at`，再重新检查动态队列和配额。检查通过时在事务中将 BillingAuthorization 标记为 `AUTHORIZED`、Job 标记为 `QUEUED`；检查失败则 Job 进入 `REJECTED` 并写入 release authorization outbox event。
10. 预授权失败时 Job 进入 `REJECTED`。

外部支付或余额系统不参与 PostgreSQL 事务。预授权调用与状态落库之间的崩溃由幂等调用和 reconciliation 修复，不能依赖分布式事务。

相同租户下，相同 Idempotency-Key 和相同 request hash 返回原 Job 或原 hard-admission 拒绝；相同 key 但 request hash 不同返回冲突。

### 10.2 Assignment 与 Lease

1. Scheduler 从中央队列选择 Job，并选择具有有效 ProfileCertification 的 ExecutionProfileRevision。
2. Scheduler 只考虑 Lifecycle 为 READY、Reachability 为 HEALTHY 的 Worker，并过滤 capability、模型版本、拓扑、健康和维护状态不匹配的节点。
3. Billing Ledger 确认 BillingAuthorization 能覆盖预计完成时间和安全余量；不足时先续期或重新授权，未成功前不得创建 Attempt。
4. Job Coordinator 在一个事务中锁定 Job 和 Worker，并重新检查 Job version / QUEUED 状态、BillingAuthorization version / expiry、Worker epoch / READY capacity 和 ProfileCertification 有效性；全部满足时才创建 Attempt、占用 Worker capacity 并签发 Lease。唯一约束保证一个 H3 Worker 同时最多有一个 active Assignment，校验失败则不产生 Attempt 并重新调度或续期授权。
5. EXECUTION Lease 至少包含 `attempt_id`、`worker_id`、`worker_epoch`、不可伪造的 `lease_token`、对该 Job 单调递增的 `fence` 和 `expires_at`。
6. `acquire()` 在同一事务中先按 mTLS `worker_id` 和请求 `worker_epoch` 查找未终结 Assignment；存在时返回原 Assignment，不重新 claim Job。只有不存在时才执行步骤 4 的创建逻辑。响应丢失后，同一 Worker epoch 重连会得到完全相同的 `attempt_id`、Lease token、fence 和原始 `expires_at`；返回动作本身不续租。
7. Worker 接受 Assignment 后开始执行并通过 heartbeat 续租。

同一 Job 在逻辑上只有一个有效执行权。网络分区时可能有多个物理计算，但只有当前 `fence` 和 Lease token 能推进 Job 和提交 Artifact。后创建 Attempt 的 fence 必须严格大于此前所有 Attempt。

### 10.3 Heartbeat 与进度

Worker heartbeat 至少上报：

```text
attempt_id
worker_epoch
lease_token
lease_fence
stage
stage_progress
estimated_remaining_seconds
gpu_health_summary
local_artifact_state
scratch_free_bytes
artifact_store_reachable
```

`stage_progress` 是观测和预测信息，不作为恢复正确性的唯一依据。续租失败、Lease 被撤销或 Worker epoch 变化时，Worker 必须停止当前 Attempt。

### 10.4 完成与 ArtifactSet commit

1. Worker 在本地 NVMe 完成编码和封装。
2. Worker 调用 `begin_finalization()`。Job Coordinator 在一个数据库事务中将 Lease phase 原子切换为 FINALIZATION、固定 Attempt 的 `finalization_started_at` / `finalization_deadline_at`，并按 output spec 幂等创建所有必需的 `STAGING` Artifact 和 ArtifactUpload；唯一约束保证崩溃重放不会创建第二组记录。
3. 事务提交后，Artifact Store claim 对应 ArtifactUpload：已有 `multipart_upload_id` 时恢复 session；没有时创建 multipart session，再用 row version CAS 保存 upload id。若在外部创建成功、CAS 落库前崩溃，session 可能成为 orphan，由 Reconciler / bucket incomplete-multipart lifecycle 清理；不能假设 S3 `CreateMultipartUpload` 自带幂等语义。
4. Worker 完成或恢复每个必需 Artifact 的 multipart upload，ArtifactUpload 持久化 upload id、已完成 parts、object version 和 checksum。
5. Artifact Validator 验证每个对象的 object version、checksum、content type、duration、resolution、frame count 和 codec，并验证必需输出的 kind、ordinal 和数量符合 output spec / `generation_count`。
6. Worker 使用当前 FINALIZATION Lease 提交包含全部必需输出的 ArtifactSetCandidate。Worker 失联后，Artifact Reconciler 只能在 Job Coordinator 重新签发同一 fence 的 FINALIZATION Lease 后接管。
7. Job Coordinator 使用 Lease token、单调 fence 和 Job version 执行 compare-and-swap，在同一事务中创建不可变 ArtifactSet manifest、标记其所有 Artifact 为 `COMMITTED`、更新 `jobs.result_artifact_set_id` 并将 Job 置为 `SUCCEEDED`。
8. 同一事务写入 billing capture outbox event；缺少任一必需输出时整个 ArtifactSet 都不可见且不能计费。
9. Worker 收到 `Accepted` 后清理本地 scratch。

旧 Attempt 晚到时返回 `RejectedStaleLease`，其对象进入短期清理策略。对象已经上传但 Worker 在提交前失联时，Artifact Reconciler 根据 ArtifactUpload 和对象存储状态继续恢复已有必需输出的上传、验证、提交整个 ArtifactSet 或清理，不重新运行推理。只要可恢复的 FINALIZATION Lease 仍在预算内，Scheduler 就不能启动新的计算 Attempt。

### 10.5 取消与完成竞争

所有取消、授权回调和完成事件都通过 Job Coordinator 的 versioned compare-and-swap：

- `PENDING_AUTH` Job 被取消后进入 `CANCELED`；晚到的授权成功回调只能触发 release，不能把 Job 重新放回队列。
- `QUEUED` Job 被取消后直接进入 `CANCELED` 并释放预授权。
- `ASSIGNED`、`RUNNING` 或 `FINALIZING` Job 被取消时递增 fence，进入 `CANCEL_REQUESTED`，Worker 收到 Stop 或 Lease 过期后进入 `CANCELED`。
- complete CAS 先成功时，Job 进入 `SUCCEEDED`，随后 cancel 返回 `AlreadySucceeded`。
- cancel fencing 先成功时，complete 返回 `RejectedStaleLease`，Artifact 不得发布或触发 Charge。
- active Job 的 `deadline_at` 到期时，Job Coordinator 必须在一个事务中递增 fence、结束当前 Attempt 并将 Job 置为 `FAILED`；晚到的 heartbeat 或 complete 只能得到 stale 响应。没有有效 Lease 的 QUEUED / RETRY_WAIT Job 可以直接进入 `FAILED`。PENDING_AUTH 到期时进入 `REJECTED`；晚到的授权成功回调只能幂等 release，不能复活 Job。

## 11. 调度策略

### 11.1 候选过滤

Scheduler 首先执行硬约束过滤：

- Worker Lifecycle 为 READY、Reachability 为 HEALTHY，且没有维护或隔离 condition。
- Worker capability 满足 ExecutionProfileRevision。
- ModelRevision 和 InferenceBackendRevision 已加载，或允许在 Assignment 前预热。
- GPU UUID / PCI BDF 拓扑符合要求。
- 租户、地域、数据驻留和安全策略允许。
- GenerationPresetRevision 与 ExecutionProfileRevision 之间存在当前有效的 ProfileCertification。

### 11.2 排序模型

H3 MVP 不向 BUSY Worker 预派任务，也不维护 per-Worker queue。调度分为租户选择、中央 Job 排序和 READY Worker 选择三步：先通过 weighted fair queue / deficit 保证租户份额，再在该租户的 eligible Job 中排序，最后选择 Worker。

中央队列中的 Job 排序可以使用：

```text
job_order_score =
    predicted_runtime_seconds
  + retry_risk_penalty
  - bounded_deadline_urgency_credit
  - bounded_priority_credit
  - bounded_aging_credit
```

`job_order_score` 越小越先运行。各 credit 必须有上限；deadline 越近，`bounded_deadline_urgency_credit` 越大，避免把高风险 Job 反向排到后面。租户公平性在选租户阶段执行，不依赖把所有因素压进一个全局分数。

为保证 aging 真正防止饥饿，等待超过 `max_queue_wait_before_protection` 的 Job 进入受保护队列并按 deadline / FIFO 排序，不再与持续到来的短 Job 竞争上述分数；受保护队列仍受租户并发配额约束。

从 READY Worker 中选择执行位置时使用：

```text
worker_score =
    model_cold_start_penalty
  + locality_penalty
  + worker_health_risk_penalty
```

`predicted_runtime_seconds` 可以由输出时长、分辨率、帧数、denoise steps、ModelRevision、GenerationPresetRevision 和历史 telemetry 拟合。模型必须持续用实际 Attempt 数据校准。

Scheduler 还要用 READY Worker 的即时容量和 BUSY Worker 上报的 `estimated_remaining_seconds` 构造全池容量时间线，计算 Job 的 `predicted_start_at`、`predicted_finish_at` 和 deadline urgency。该时间线只用于排序、admission 和对外 ETA，会在每次 heartbeat / Assignment 后重算，不形成对 BUSY Worker 的 reservation 或预派绑定。

### 11.3 公平性与 admission control

- 按租户使用 weighted fair queue 或等价策略。
- aging 防止大任务长期饥饿。
- 每个租户限制 queued、running 和 retrying Job 数量。
- 重试保留原 Job 的等待年龄，但使用独立 retry lane 和预算，防止 retry storm。
- Hard admission 在预授权前检查不支持的 ExecutionProfileRevision、租户配额、队列上限、Artifact Store circuit 和 scratch 水位。
- 预授权成功后必须重新检查动态队列和配额；失败时拒绝 Job 并释放预授权。
- MVP 不提供严格的单 Job 完成期限保证；`deadline_at` 只限制排队、执行和重试预算，ETA 与 p95 SLO 均为 best effort。未来若提供严格 deadline GenerationPresetRevision，必须先引入可持久化、带有效期且参与 admission 的 CapacityReservation，再在预授权前取得 reservation；在该模型落地前不得销售严格 deadline。
- Scheduler 使用 GenerationPresetRevision 的优先级和 SLA 约束，不直接计算价格。

## 12. 重试与故障处理

### 12.1 Retry Budget

每个 Job 的 ExecutionPolicySnapshot 必须固化以下重试参数：

```text
max_attempts
max_total_compute_seconds
max_finalization_seconds_per_attempt
deadline_at
retry_backoff_policy
retryable_failure_classes
circuit_breaker_policy
```

不能只使用固定的 `max_retries = 3`。长任务的重试必须同时受次数、累计计算时间和用户 deadline 限制。

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
| 同步可判定的参数或输出规格非法 | hard admission 拒绝，不创建 Job | 无 | 不预授权 |
| 执行期才能判定的输入内容不支持 | FAILED | 无 | 释放预授权 |
| ModelRevision 或 GenerationPresetRevision 配置错误 | 当前 Job FAILED | 打开相关 revision circuit | 释放预授权 |
| Worker heartbeat 丢失 | RETRY_WAIT | SUSPECT，随后恢复 | 不重复计费 |
| GPU Xid / fallen off bus | RETRY_WAIT | DRAINING / RECOVERING | 不重复计费 |
| 临时进程崩溃 | 按下方规则 5 进入 RETRY_WAIT 或 FAILED | process restart | 不重复计费 |
| 确定性 OOM | 按下方规则 3 进入 RETRY_WAIT 或 FAILED | 无或降载 | 重试时保留 / 续期，最终失败时释放 |
| Artifact upload 失败 | 保持 FINALIZING | resume multipart upload | 不重新计算 |
| 多 Worker 相同错误 | 按下方规则 4 进入 RETRY_WAIT 或 FAILED | 打开 revision circuit 并调查 | 重试时保留 / 续期，最终失败时释放 |
| 用户取消 | CANCELED | Stop + cleanup | 按取消策略 |

失败必须携带稳定的 `failure_class`、原始错误摘要、stage、Worker、GPU UUID、InferenceBackendRevision 和是否建议重试。不要让 Scheduler 解析自由文本日志决定重试。

Job 的唯一重试决策者仍是 Job Coordinator，决策顺序固定为：

1. 非 retryable failure、`deadline_at` 到期或 retry budget 耗尽时进入 `FAILED`。
2. ArtifactUpload 可恢复且 `finalization_deadline_at` 未到期时保持 `FINALIZING`，只重试上传、验证或 commit。
3. 确定性 OOM 只有在 Catalog 存在满足原 GenerationPresetRevision、资源更充足且认证有效的 ExecutionProfileRevision 时才进入 `RETRY_WAIT`，否则进入 `FAILED`。
4. 同一 revision 的 failure fingerprint 在配置阈值内跨多个健康 Worker 重现时，先使相关 ProfileCertification 失效并打开 revision circuit；当前 Job 只有存在其他合格 ExecutionProfileRevision 且预算允许时才重试，否则失败。
5. 其余 retryable failure 根据退避策略设置 `next_retry_at` 和动态 Worker exclusion 后进入 `RETRY_WAIT`。

### 12.3 重试放置

- 默认避开上一个失败 Worker。
- GPU 或 PCIe fault 后避开同一节点，直到恢复验证完成。
- 新 Attempt 必须继续满足原 GenerationPresetRevision，不能静默降级。
- 相同 failure fingerprint 在多个 Worker 重复出现时应触发 Job 或 ModelRevision circuit breaker。
- 默认不启用 speculative duplicate execution；只有明确的高价值 SLA 才允许 hedging。

### 12.4 Checkpoint

第一版按阶段考虑恢复：

```text
Encoder -> DiT -> VAE -> Upload
```

Artifact upload 失败只恢复上传。VAE 失败时，可以在验证收益后复用已完成的 DiT latent。任意 DiT step checkpoint 需要 Inference Backend 明确支持，并证明保存开销显著小于预期重算成本后才启用。

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
artifacts/{tenant_id}/{job_id}/{attempt_id}/{artifact_id}/video.mp4
artifacts/{tenant_id}/{job_id}/{attempt_id}/{artifact_id}/thumbnail.webp
checkpoints/{tenant_id}/{job_id}/{attempt_id}/{artifact_id}/dit-latent.bin
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
- MVP 只有在 Charge 达到 `CAPTURED` 后才向客户端签发短期 signed GET URL；计费未完成时 API 返回明确的 billing pending 状态。
- 对公网高流量下载可以在 COMMITTED Artifact 前增加 CDN。
- 对象存储与计算集群保持同地域；跨地域复制异步进行。
- Prompt、输入和输出均视为租户敏感数据，访问必须记录审计日志。

### 13.5 生命周期

| Artifact 类型 | 默认策略 |
| --- | --- |
| 正式视频 | 按产品配置保留，例如 7 天或 30 天 |
| 缩略图 / 预览 | 跟随正式视频 |
| 失败、LOST 或未获胜 Attempt 输出 | 24 到 72 小时后清理 |
| DiT checkpoint | 短期保留，例如 24 小时 |
| Debug dump | 默认不保存，按故障采样 |
| Worker 本地 scratch | COMMITTED 后立即清理 |

具体期限属于产品和合规策略，不应写死在 Worker 中。

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

### 14.1 用户计费与内部成本

文生视频优先按用户可预测的输出规格计费，而不是按不稳定的实际 GPU 时间收费。示意公式为：

```text
billable_units =
    output_duration
  * resolution_factor
  * generation_count
  * preset_multiplier
  * model_multiplier
```

实际价格公式由版本化 RateCard 定义。金额必须使用整数最小货币单位或 Decimal，禁止使用浮点数。

每个 Attempt 的 GPU 时间、能耗、重试和存储流量记录为 UsageRecord，用于成本分析。平台故障导致的重试只增加内部成本，不生成额外用户 Charge。

### 14.2 PricingSnapshot

Job 创建时保存：

```text
model_revision
generation_preset_revision
output_spec
rate_card_revision
quoted_amount_minor
currency
billable_units
```

`retry_policy_revision` 属于 ExecutionPolicySnapshot，而不是价格计算本身。ExecutionPolicySnapshot 和 PricingSnapshot 一起绑定到 Job，分别保证执行语义和报价不会在长时间排队期间漂移。

任务排队或执行期间发生调价、GenerationPresetRevision 更新或硬件升级，都不能改变既有 PricingSnapshot。

### 14.3 长任务的预授权有效期

MVP 的资金适配器必须满足以下契约：

- 平台余额使用按 `job_id` 幂等、覆盖完整 quoted amount 的 reservation；在 Job 终态和结算完成前不得自行过期。
- 外部支付 hold 只有在 `authorization_expires_at >= deadline_at + capture_safety_margin`，或 adapter 支持幂等续期 / 重新授权时才可用于该 Job。每个 Job 都必须有平台生成的最大端到端 `deadline_at`，即使用户没有购买严格完成期限。
- Billing Ledger 在到期前触发续期；每一轮使用持久化的 operation generation，并由 reconciliation 以同一幂等键处理“外部成功、落库前崩溃”等不确定结果。
- QUEUED Job 的续期或重新授权失败时，在创建 Attempt 前进入 `REJECTED` 并释放旧 hold。
- Attempt 已开始后续期失败时，MVP 不重复计算也不把支付故障伪装成推理失败；平台承担本次信用风险，继续当前 Attempt，但在 Charge `CAPTURED` 前不签发 Artifact 下载 URL，并触发计费告警和人工追收流程。
- 不满足上述能力的外部支付 adapter 不得接纳可能超过其 hold 有效期的长任务。

### 14.4 计费流程

```text
submit
  -> quote
  -> authorize / reserve balance
  -> execute and retry
  -> ArtifactSet COMMITTED
  -> capture Charge

FAILED / CANCELED
  -> release authorization or apply cancellation policy
```

Billing 状态与 Job 状态分离。成功生成但计费 adapter 暂时不可用时，Job 仍保持 `SUCCEEDED`，Charge 进入 `CAPTURE_PENDING`；MVP 在 `CAPTURED` 前不允许下载 ArtifactSet。

```text
AUTH_PENDING -> AUTHORIZED -> CAPTURE_PENDING -> CAPTURED
      |             |
      v             +---------------------------> RELEASED
 AUTH_FAILED
```

Charge 使用 `job_id` 和 charge type 作为幂等键。只有获胜 Attempt 的 ArtifactSet commit 事务可以发出 capture event。

### 14.5 待确定的产品策略

- 用户在 QUEUED、RUNNING 和 FINALIZING 阶段取消时是否收费。
- 超出默认保留期后是否收取长期存储费用。
- GenerationPresetRevision 的价格是否包含优先级和完成时间 SLO。
- 企业客户是否支持按实际 compute usage 的独立计费模式。

## 15. Model Catalog 与 revision 生命周期

### 15.1 Catalog 状态

ModelRevision、InferenceBackendRevision 和 ExecutionProfileRevision 都必须使用不可变 revision。浮动名称只能作为查询 alias，不能写入 Job snapshot。

ExecutionProfileRevision 的生命周期为：

```text
REGISTERED -> VALIDATING -> CERTIFIED -> CANARY -> ACTIVE -> DRAINING -> RETIRED

VALIDATING -> INVALID
CANARY     -> INVALID
```

只有 `ACTIVE` ExecutionProfileRevision 可以接收普通 Job；`CANARY` 只接收显式 canary 流量。验证或 canary 失败时进入 `INVALID`。生产 telemetry 低于质量门槛时，Catalog 先使相关 ProfileCertification 失效，再将 `ACTIVE` ExecutionProfileRevision 转为 `DRAINING`，从而阻止新 Assignment；引用归零后才进入 `RETIRED`。

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

### 15.3 Revision 保留

- QUEUED、ASSIGNED、RUNNING、FINALIZING 或 RETRY_WAIT Job 引用的 ModelRevision、GenerationPresetRevision、ExecutionPolicySnapshot 和 PricingSnapshot 必须保留；非终态 Attempt 引用的 InferenceBackendRevision、ExecutionProfileRevision 和 ProfileCertification 同样必须保留。
- Retry 可以选择另一个具有有效 ProfileCertification 的 ExecutionProfileRevision，但 ModelRevision 和 GenerationPresetRevision 不变。
- Model weights、Inference Backend image 和配置只有在引用计数为零且审计保留期满足后才能删除。
- 强制迁移必须创建显式 migration record，重新验证质量、价格和用户承诺，不能静默修改 Job snapshot。

## 16. GPU 健康与恢复

### 16.1 恢复阶梯

| Level | 动作 | Lifecycle / Reachability |
| --- | --- | --- |
| L0 | restart inference process | DRAINING |
| L1 | 清理 CUDA process / context | DRAINING |
| L2 | `nvidia-smi --gpu-reset` | RECOVERING |
| L3 | PCIe FLR | RECOVERING |
| L4 | unload / reload driver | RECOVERING |
| L5 | reboot node | RECOVERING / OFFLINE |
| L6 | BMC power cycle | RECOVERING / OFFLINE |
| L7 | quarantine | QUARANTINED |

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

不能对所有错误无条件执行 FLR。Node Agent 必须根据 error class、设备 reset capability、拓扑限制和当前使用状态选择动作。

### 16.2 Kubernetes 与 driver

已有符合要求的 Kubernetes 集群可以直接复用。裸金属 greenfield baseline 使用 Ubuntu LTS、RKE2 和 containerd：至少 3 台非 GPU control-plane 节点承载 etcd / Kubernetes control plane，PostgreSQL、NATS 和对象存储运行在独立 CPU / storage fault domain，不能与不稳定 GPU Worker 共用生命周期。

GPU Worker 由主机镜像、PXE 或 Ansible 管理固定 kernel、driver、firmware 和 container toolkit。若使用 NVIDIA GPU Operator，关闭其 driver 和 toolkit 生命周期管理，只启用经验证的 NVIDIA Device Plugin、DCGM Exporter 和必要的 metrics 能力。

H3 MVP 可请求：

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

H3 MVP 采用第二种方案，不实现 `vela.ai/h3-worker` 扩展资源。

Fleet Controller 为每个 ACTIVE / CANARY ExecutionProfileRevision 管理带专用 node selector 的 Worker pool。H3 baseline 使用 `OnDelete` DaemonSet，但 `OnDelete` 本身不是安全边界。Live WorkerPool / DaemonSet / Worker Pod 由 Fleet Controller 独占管理，Argo CD 只交付 controller、CRD 和版本化期望配置，不 prune 或直接 patch live pool resource。Kubernetes RBAC 与 validating admission webhook / policy 拒绝其他 service account 删除 Worker Pod / pool、修改 DaemonSet selector / image 或移除保护 finalizer；Fleet Controller 只有携带 Job Coordinator 已完成的 `DrainOperation` 引用才能解除 finalizer并执行这些动作。节点突然失效仍由 Lease / fence 恢复，不能由 Kubernetes guard 保证。

Fleet Controller 只有在 Job Coordinator 确认 Worker 已 DRAINING、Lease 已结束或 fenced 后才删除 Pod；不能依赖默认滚动更新自动终止长任务。PodDisruptionBudget、较长 `terminationGracePeriodSeconds` 和 preStop drain 只能作为额外保护，不能代替 Vela Lease 语义。

每个 GPU 节点提供独立 NVMe scratch，使用 XFS project quota 或等价硬配额，并挂载到 H3 Worker Pod。Worker Agent 为每个 Attempt 创建独立目录；配额、high / low / critical watermark 和终态清理由 Vela 管理，不能把 Kubernetes ephemeral-storage eviction 当作 Artifact 恢复机制。

## 17. 一致性与高可用

### 17.1 Scheduler 高可用

可以运行多个 Scheduler replica，但 Assignment 必须通过数据库事务竞争。Scheduler 进程本身不持有不可恢复内存状态。

Scheduler 崩溃后：

- 未提交事务不会产生 Assignment。
- Assignment 已提交但 gRPC acquire 响应未送达时，同一 mTLS `worker_id` 和 `worker_epoch` 重试 `acquire()` 会先读到并重放同一 active Assignment；该路径不创建新 Attempt、不签发新 fence，也不延长 `expires_at`。若 Worker 始终没有取得响应或续租，则 Lease 过期后由 reconciliation 结束 Attempt。新进程必须使用递增的 epoch，不能继承旧 Lease。Outbox 只重发状态事件，不传递执行所有权。
- Worker 未续租时 Lease 最终过期，Job 进入重试判断。

### 17.2 网络分区

Worker 与控制面失联时，旧 Worker 可能仍继续计算。控制面可以在 grace period 后签发具有更大 fence 的新 Attempt，但旧 Attempt 已失效，不能推进 Job、发布 Artifact 或触发 Charge。

### 17.3 外部依赖故障

- PostgreSQL 不可用时停止新 Assignment，Worker 可以在有限 Lease 内继续当前 Attempt。
- 对象存储不可用时已完成推理停留在 FINALIZING，并保留本地 Artifact 后恢复上传；Artifact Store circuit 阻止受影响 pool 的新 Assignment，scratch high watermark 只阻止对应 Worker 或 pool 的新 Assignment。
- Billing adapter 不可用时使用 outbox 重试 capture，不重新执行 Job。
- JetStream 不可用时 outbox 保留事件，并依靠 PostgreSQL reconciliation 保证最终恢复。

## 18. 安全

- Client、Worker、Scheduler、Node Agent 和 storage credential 使用独立身份。
- Envoy Gateway 使用 cert-manager 管理外部 TLS；Kubernetes workload 的内部 gRPC 使用 cert-manager 签发和轮换 mTLS certificate。host systemd `vela-node-agent` 使用 OS provisioning、Vault PKI 或等价私有 CA 签发的独立 host certificate。所有证书身份必须映射到 Worker / Controller / Node 注册身份。
- NATS listener 和 monitoring endpoint 只暴露在内部网络；客户端连接必须使用 TLS。NATS 使用 operator / account JWT 模式，为 outbox dispatcher、Scheduler、Billing、Fleet 和 Reconciler 分别签发可轮换的 NKey workload credential，并按 event subject 配置最小 publish / subscribe ACL；禁止共享全权 token，禁止业务 workload 使用 system account。
- 长期 secret 存放在 cloud KMS / Secret Manager 或 Vault，通过 External Secrets 注入；对象存储上传和下载使用短期凭据，BMC / payment credential 不写入普通 ConfigMap 或日志。
- Node Agent 具有高权限，其命令接口必须最小化并限制到已登记设备和动作。
- Node Agent 不接受任意 shell command，不把 PCI sysfs path 直接暴露给远端调用者。
- Worker 只能上传当前 Artifact 的确切 object key，不能覆盖、删除或列举其他对象。
- Artifact Validator 将媒体视为不可信输入；`ffprobe` 在无网络、非特权且有 CPU、内存、文件大小和超时限制的 sandbox 中运行。
- signed URL 具有短 TTL，并绑定 method、object key 和 content constraints。
- 日志禁止记录 prompt 正文、对象凭据、signed URL 和支付凭据。
- 所有管理动作、Artifact 访问、计费变更和节点恢复均需审计。

## 19. 可观测性

所有 Go / Python 进程使用 OpenTelemetry SDK，将 trace、metric 和结构化日志发送到 OpenTelemetry Collector。MVP backend 使用 Prometheus + Alertmanager、Grafana、Loki 和 Tempo；GPU 指标来自 DCGM Exporter。生产告警必须覆盖 PostgreSQL replication / failover、JetStream replica / consumer lag、outbox age、reconciliation backlog 和 object-store circuit。

### 19.1 Job 指标

- 接纳率、队列长度和 queue wait time。
- 按 ModelRevision、GenerationPresetRevision、分辨率和受控 tenant tier 统计运行时间分布；单个 `tenant_id` 不作为常规 metric label。
- Job success、failure、cancel 和 retry rate。
- 每个 Job 的 Attempt 数量和累计 compute seconds。
- Artifact upload latency、失败率和 orphan bytes。
- Quote、authorization、capture 和 release 成功率。

### 19.2 Worker 与硬件指标

- 各 Lifecycle State 和 HEALTHY、SUSPECT、OFFLINE Reachability 的 Worker 数量。
- GPU utilization、memory、temperature、power 和 Xid。
- PCIe AER、fallen off bus 和 heartbeat loss。
- 各 remediation level 的执行次数、成功率和恢复耗时。
- ModelRevision / ExecutionProfileRevision warm 状态和 cold-start 时间。

### 19.3 追踪标识

所有结构化日志和 trace span 至少关联：

```text
tenant_id
job_id
attempt_id
worker_id
worker_epoch
model_revision
generation_preset_revision
execution_profile_revision
```

Prometheus metric 只使用数量受控的 label，例如 ModelRevision、GenerationPresetRevision、ExecutionProfileRevision、failure class、stage 和 status。`tenant_id`、`job_id`、`attempt_id` 等高基数标识不能成为常规 label；需要从指标跳转到单次执行时使用 trace exemplar 或审计查询。

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
| 事实源 | PostgreSQL HA | 优先使用托管 PostgreSQL；裸金属使用 CloudNativePG 部署在专用 CPU 节点，跨故障域同步副本、自动 failover、PITR |
| 事件设施 | NATS JetStream | 3 replicas、PVC-backed file storage、durable consumer、explicit ack、anti-affinity |
| 一致性 | Transactional outbox + idempotent consumer + reconciliation | 不做 PostgreSQL / NATS 双写，不依赖 Broker exactly-once 宣称 |
| Catalog 配置 | YAML authoring + JSON Schema + canonical JSON | 接纳时校验，入库保存 canonical JSON、schema revision 和 content hash，不执行任意模板代码 |
| Billing | PostgreSQL internal credit ledger 优先 | 外部 payment provider 通过注入 adapter 接入；测试使用 mock adapter |
| 金额 | integer minor unit 或 Decimal | 禁止 binary floating point；PricingSnapshot 创建后不可变 |
| 时间 | PostgreSQL clock | Lease、deadline、retry 和 authorization expiry 不以 Pod 本地时钟为准 |

### 20.3 平台与存储

| 领域 | 选择 | 生产基线 |
| --- | --- | --- |
| 集群 | Kubernetes + Vela Scheduler | Kubernetes 管 Worker 生命周期，Vela 管 Job placement；裸金属 baseline 为 Ubuntu LTS + RKE2 / containerd |
| GPU | NVIDIA Device Plugin + DCGM Exporter | H3 请求 `nvidia.com/gpu: 8`；host driver / toolkit 版本锁定，不由 Operator 自动升级 |
| Worker rollout | Fleet Controller + `OnDelete` DaemonSet | planned drain / fence 后才删除 Pod；profile 用 node label 和 pool 表达 |
| Artifact | 同地域 managed / existing S3-compatible store | private bucket、versioning、conditional write、固定 object version；不把新建分布式存储系统塞进 Vela MVP |
| 开发对象存储 | MinIO 或 local adapter | 只用于本地和 conformance test，生产选择必须单独通过 durability / restore 验证 |
| Scratch | 本地 NVMe + XFS project quota | per-Attempt 目录、watermark 背压、明确终态后清理 |
| 镜像与模型 | OCI registry + S3 | 镜像固定 digest；模型权重固定 checksum，Catalog 只保存 revision 和位置 metadata |
| 媒体探测 | FFmpeg `ffprobe` | 固定版本和探测参数，输出解析为结构化 metadata 后再执行 output-spec validation |

### 20.4 安全、可观测性与交付

| 领域 | 选择 |
| --- | --- |
| Gateway | Kubernetes Gateway API + Envoy Gateway |
| 外部身份 | scope 化 API key 用于机器客户端，OIDC 用于管理端；凭据只保存 hash / provider subject，支持轮换和吊销 |
| TLS / mTLS | cert-manager；外部 TLS 和内部 workload certificate 分开签发与轮换；NATS 使用 TLS + operator/account JWT + per-workload NKey / subject ACL |
| Secret | cloud KMS / Secret Manager 或 Vault + External Secrets |
| Telemetry | OpenTelemetry SDK / Collector + Prometheus / Alertmanager + Grafana + Loki + Tempo |
| GPU telemetry | DCGM Exporter + NVML / PCIe AER host probe |
| Deployment | Helm + Argo CD；GPU host image / driver 使用 Ansible 或 PXE 管理 |
| 本地集成测试 | `testcontainers-go` 启动 PostgreSQL、NATS、S3 fixture；fake Worker / payment adapter；Python runner 使用 `pytest` |
| 硬件验收 | 独立 H3 staging pool 执行 process kill、网络分区、GPU fault、reboot 和对象存储故障注入 |

### 20.5 明确不采用

| 方案 | MVP 决策 |
| --- | --- |
| Temporal | 不采用；无法替代 Vela placement、Lease fencing 和 PostgreSQL Artifact / Billing CAS，引入后会形成第二状态权威。团队已有成熟 Temporal 平台时才重新评估 |
| Kafka | 不采用；当前不需要长期事件历史和超高吞吐 replay，JetStream 运维面更小 |
| Redis Streams | 不承担关键事件或状态；只有出现有测量依据的缓存需求时再引入 Redis |
| K8s + Ray Serve | 不采用，避免 Kubernetes、Ray、Vela 和 SGLang 四层重复调度 |
| Kubernetes Job per inference | 不采用，避免每个请求重新调度、启动和加载 8-GPU 模型 |
| Slurm | 不作为互联网在线 serving 主框架 |
| 纯 systemd + 自研集群管理 | 仅适合很小规模，不能替代 Kubernetes rollout、service discovery 和 declarative lifecycle |
| Nomad | 可行，但当前没有足够收益替换 Kubernetes 生态 |

## 21. MVP 范围

第一阶段建议交付以下闭环：

- 单地域、单 Kubernetes 集群。
- MiniMax H3 单 Model / Workload 和 SGLang fork 单 Inference Backend。
- Go `vela-control` / Fleet Controller / Worker Agent / Node Agent 与 Python H3 runner。
- 一台 8-GPU 节点对应一个长期运行的 Worker Pod。
- PostgreSQL HA 事实源、transactional outbox、3-replica NATS JetStream 和周期性 reconciliation。
- REST / OpenAPI 异步 submit/get/cancel Interface 和 Idempotency-Key。
- Worker 发起的 mTLS gRPC pull / heartbeat stream，不向 BUSY Worker 预派任务。
- Job、Attempt、Lease、ExecutionPolicySnapshot、RetryRuntimeState 和 fencing token。
- 中央队列、hard admission、基于公平性与预计工作量的 Scheduler。
- Worker heartbeat、阶段进度和自动重试。
- S3-compatible ArtifactUpload 恢复、不可变 object version、完整 ArtifactSet 验证、CAS commit 和 signed download。
- ModelRevision、InferenceBackendRevision、ExecutionProfileRevision、GenerationPresetRevision、ProfileCertification 和 RateCard 快照。
- Model Catalog 的 benchmark、certification 和 revision retention。
- Fleet Controller 的 warm-up、canary、planned drain 和 rollout。
- Artifact Store circuit 和 scratch high / low watermark 背压。
- 可覆盖长任务的 BillingAuthorization、续期 reconciliation、成功 capture、失败 release 和内部 UsageRecord。
- host systemd `vela-node-agent`，先实现安全的 process restart、drain、quarantine 和人工审批的高等级恢复。
- OpenTelemetry、Prometheus / Alertmanager、Grafana、Loki、Tempo、审计和故障注入测试。

MVP 明确不包含：

- 任意 DiT step checkpoint。
- 自动跨地域 failover。
- 多节点 LLM ExecutionProfileRevision。
- 自动 driver reload 或 BMC power cycle 的无监督生产执行。
- 基于机器学习的复杂运行时间预测。
- 严格的单 Job deadline 和 CapacityReservation。

## 22. 验收场景

1. 相同 Idempotency-Key 重复提交只创建一个 Job、一个预授权和一个最终 Charge。
2. Hard admission 拒绝不会产生预授权；授权后的动态 admission 拒绝会可靠释放预授权。
3. `PENDING_AUTH` Job 被取消后，晚到授权回调不能把它重新放入队列。
4. complete 与 cancel 并发时只有一个 CAS 获胜，Job、ArtifactSet 和 Charge 结果一致。
5. Scheduler 在 Assignment 事务前后崩溃，Job 不会永久卡在不可恢复状态。
6. H3 Worker BUSY 时不接受预派任务，中央队列仍保持租户公平和 aging。
7. Worker 网络分区后旧 Attempt 完成时返回 `RejectedStaleLease`，持久状态仍为 LOST。
8. GPU fault 导致 Worker Lost，RetryRuntimeState 累计成本、动态排除故障节点并设置 `next_retry_at`。
9. multipart upload 中断后，同一 Worker 进程重启且本地源文件仍在，或接管 owner 可访问共享持久源文件时，可从 ArtifactUpload 恢复而不重新执行推理；Worker 丢失且源文件未共享时不承诺跨 Worker 恢复。
10. Worker 在 upload 完成、ArtifactSet commit 前失联时，Artifact Reconciler 可以完成验证或安全清理。
11. 同一 object key 的覆盖写被拒绝，COMMITTED Artifact 固定到不可变 object version。
12. duration、resolution、frame count、codec 或 generation count 不符合 output spec 的 ArtifactSet 无法发布或计费。
13. 两个 Attempt 同时完成时只有一个进入 SUCCEEDED 并触发 capture。
14. RateCard 在 Job 排队期间更新，既有 Job 仍使用原 PricingSnapshot。
15. Retry 可以更换 ExecutionProfileRevision，但必须具有原 GenerationPresetRevision 的有效 ProfileCertification。
16. ExecutionProfileRevision 质量回归后，其 ProfileCertification 失效且不能接收新 Job；旧 revision 在引用归零前不会删除。
17. Artifact Store 故障会停止受影响 pool 的新 Assignment；单个 Worker 或 pool 的 scratch 达到 high watermark 时只停止对应 Worker 或 pool，恢复到 low watermark 且存储探测通过后重新接纳。
18. H3 Worker 请求 `nvidia.com/gpu: 8` 并受专用 taint/label 约束，不与其他 GPU workload 重复分配。
19. GPU remediation 和 planned rollout 前 Worker 完成摘流量和 Lease fencing；绕过 Fleet Controller 的 Pod / pool delete、selector / image patch 或 Argo prune 被 RBAC + admission / finalizer 拒绝。
20. 失败或未获胜 Attempt、过期 checkpoint 和本地 scratch 按策略自动清理。
21. JetStream 短暂或整体不可用后，outbox 保留待发布事件，reconciliation 能从 PostgreSQL 恢复待调度 Job。
22. Billing adapter 短暂不可用不会导致重复推理或重复 Charge。
23. `generation_count > 1` 或包含缩略图时，缺少任一必需输出都不能发布部分 ArtifactSet 或触发 Charge。
24. `begin_finalization()` 在事务提交前后崩溃，重放仍只得到一组 Artifact / ArtifactUpload，并保留原 `finalization_deadline_at`。
25. 长任务的 payment hold 临近过期时能幂等续期；续期失败的 QUEUED Job 不会开始执行，已开始的 Job 不会重复计算且在 `CAPTURED` 前不可下载。
26. OFFLINE Worker 恢复后必须经过 SUSPECT、设备 / Inference Backend 检查和 canary，达到 HEALTHY + READY 才重新接收 Assignment。
27. Outbox dispatcher 只有收到 3-replica stream 的 quorum `PubAck` 才标记 published；在 `PubAck` 前失败会保留未发布 row，在 `PubAck` 后、标记前崩溃会以同一 `Nats-Msg-Id` 重试并产生可安全消费的重复 event，不会丢失状态变化。
28. Consumer 在 PostgreSQL 事务提交、JetStream ack 前崩溃时，重投 event 由 aggregate version / CAS 幂等拒绝。
29. Assignment wakeup 被 ack 后 Worker 执行 40 到 50 分钟，JetStream consumer 不保留长期 pending delivery，Lease / fence 仍能处理 Worker 丢失。
30. PostgreSQL 自动 failover 后，已接纳 Job、Outbox、Lease fence 和 Charge 幂等状态保持一致；数据库不可用窗口不创建新 Assignment。
31. Assignment 已提交但 gRPC response 丢失时，同一 `worker_id` 和 `worker_epoch` 重连会得到原 Assignment，不产生第二个 Attempt、fence 或 Lease 延期；超过原 `expires_at` 后按 Lease expiry 正常恢复。
32. PostgreSQL 或控制面失联时，Worker 依据上一次服务端 `lease_valid_for` 和请求起点的 monotonic deadline fail closed；修改本地 wall clock 不能延长执行权。
33. 使用无权限或其他 workload 的 NATS credential publish / subscribe 受限 subject 时被拒绝，credential 轮换不丢 outbox event。

## 23. 已决事项、上线门槛与 Future Work

### 23.1 MVP 已决事项

- 事件正常路径使用 3-replica NATS JetStream；PostgreSQL transactional outbox 保证不丢发布意图，reconciliation 保证最终恢复。
- Worker 使用 mTLS gRPC pull acquisition，只在 capacity 可用时请求 Assignment；控制命令通过同一 stream / heartbeat 返回。
- PostgreSQL 是唯一事实源，JetStream、Kubernetes 和 Worker 本地状态都不能单独推进 Job。
- 控制面使用 Go 模块化单体；Python 只保留在 Inference Backend runner；MVP 不引入 Temporal、Kafka、Redis 或 Ray 调度层。
- H3 使用 `nvidia.com/gpu: 8`、专用节点池和 Fleet Controller 管理的 `OnDelete` DaemonSet，不实现自定义扩展资源。
- Artifact 使用同地域 S3-compatible store；生产对象存储不作为 Vela 自建子项目。

### 23.2 生产上线门槛

以下项目不能在架构讨论中伪造固定答案，但必须有 owner、验证环境、receipt 和通过标准后才能上线：

| Gate | 必需产物 | 未通过时行为 |
| --- | --- | --- |
| heartbeat / Lease / Worker Lost / retry budget | 基于网络分区、进程 kill、节点重启和长任务故障注入的参数报告；staging 初始值可从 5 秒 heartbeat、15 秒 SUSPECT、60 秒 Lease / Lost 开始验证 | 保持人工配置，不宣称生产 SLA；重试受保守预算限制 |
| 事件与 Assignment 可靠交付 | 可复现的 fault-injection receipt：覆盖 outbox commit 后崩溃、publish timeout / `PubAck` 丢失、`PubAck` 后标记前崩溃、consumer commit 后 ack 前崩溃、Assignment commit 后 gRPC response 丢失；证明事件不丢、consumer 幂等且同一 Worker epoch 不产生第二个 Attempt / fence | 不进入生产流量；保留 reconciliation 并限制在 staging 测试租户 |
| GenerationPresetRevision certification | 版本化 benchmark corpus、基准 profile、质量阈值、p50 / p95 runtime、成功率、成本和统计置信 receipt | Preset / ProfileCertification 不得进入 ACTIVE |
| 用户取消计费 | 产品、财务和法务确认的 PENDING_AUTH / QUEUED / RUNNING / FINALIZING 逐状态规则及账单示例 | 公网接口不得承诺未定义的 refund；默认只在测试租户开放运行中取消 |
| Artifact retention | 正式结果、失败对象、checkpoint、debug dump 和本地 scratch 的默认期限、租户覆盖规则与删除审计 | 使用最短保守期限；debug dump 默认关闭 |
| GPU remediation capability | 按 GPU SKU、PCIe topology、kernel、driver、firmware 建立 reset / FLR / driver reload 认证矩阵和恢复 receipt | 未认证动作 fail closed，只允许 quarantine、reboot 或人工审批 |
| 基础设施灾备 | PostgreSQL failover / PITR、JetStream 节点丢失、对象存储不可用和 secret rotation 演练 | 不进入生产流量 |

### 23.3 Future Work

- H3 DiT latent checkpoint：只有故障阶段分布证明预期节省的重算时间和成本显著高于写入、存储和恢复成本时才启用。
- 多节点 LLM ExecutionProfileRevision：单独设计 gang placement、跨节点 Lease、拓扑和部分节点失效语义，不扩大 H3 MVP Interface。
- 严格单 Job deadline：先实现持久 CapacityReservation 和 admission 证明，再销售严格期限 GenerationPresetRevision。
- 自动跨地域 failover、复杂运行时间预测和跨地域 Artifact policy。

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
