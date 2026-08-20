# Vela 架构设计

| 属性 | 内容 |
| --- | --- |
| 状态 | Draft |
| 日期 | 2026-08-20 |
| 首个工作负载 | MiniMax H3 文生视频 |
| 长期定位 | 通用 AI 推理集群控制面 |

## 1. 摘要

Vela 是面向大规模 AI 推理集群的控制面。它接收耗时从数分钟到数十分钟的异步推理任务，根据模型、质量档位、执行拓扑、机器健康度和队列负载选择 Worker，并负责重试、产物发布、计费和故障恢复。

Vela 不实现模型内部的张量并行或流水线执行。具体推理由 SGLang fork、vLLM 或其他 backend 完成；Vela 决定什么任务应当放在哪个 Worker 上、以哪个 Execution Profile 执行，以及执行失败后如何恢复。

MiniMax H3 是第一种 backend。每台 H3 机器有 8 张 GPU，其中 1 张负责 Encoder 和 VAE Decoder，另外 7 张负责 DiT。由于 PCIe 带宽低，8 张卡对 serving plane 是不可拆分的整体。Kubernetes 管理长期运行并已预热的 Worker，Vela Scheduler 管理单个推理 Job，SGLang fork 管理节点内部的 8 卡执行。

系统的核心可靠性语义是：

> At-least-once execution, exactly-once visible completion.

物理计算可能因网络分区或 Worker 丢失而重复发生，但只有一个 Attempt 可以正式发布 Artifact 和触发用户计费。

## 2. 目标与非目标

### 2.1 目标

- 提供异步 AI 推理接口，支持最长 40 到 50 分钟或更久的任务。
- 将一台机器或多机拓扑抽象成一个可调度的 Worker / Execution Profile。
- 根据预计完成时间、优先级、租户配额、模型预热状态和硬件风险进行调度。
- 在 Worker、GPU、驱动或网络故障后自动重试，并避免重复发布和重复计费。
- 将视频、缩略图和 checkpoint 等 Artifact 可靠写入对象存储。
- 支持不同质量和有损加速档位，并为每种用户可见 Preset 配置独立计费。
- 对异常 GPU 执行摘流量、恢复、验证和重新入池。
- 为未来的 LLM、图像、多模态和其他推理 backend 保留扩展能力。

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
- 分辨率、帧数、denoise steps、模型、LoRA、Preset 和 backend 版本都会影响成本。
- HTTP 请求不能与推理执行保持同生命周期。
- 用户需要可查询的进度、取消、失败原因、重试状态和 Artifact 下载入口。

### 3.3 需要校准的假设

以下参数不能仅凭架构讨论确定，必须由故障注入和生产数据校准：

- heartbeat 周期、Lease TTL 和 Worker Lost grace period。
- 各模型、分辨率、时长和 Preset 的运行时间预测模型。
- 最大重试次数、总计算预算和退避参数。
- DiT latent checkpoint 的大小、写入成本和恢复收益。
- GPU 错误分类与每一级 remediation 的成功率和安全条件。

## 4. 核心设计原则

1. **Worker 是 serving 资源。** H3 第一版中，一台 8-GPU 机器等于一个 Worker。
2. **Kubernetes 只管理 Worker 生命周期。** 单个 Job 由 Vela Scheduler 调度。
3. **推理 backend 封装节点内部执行。** Vela 不理解 backend 内部 tensor movement。
4. **Job、Attempt 和 Lease 分离。** Job 表示用户意图，Attempt 表示一次物理执行，Lease 表示限时执行权。
5. **状态存储是事实源。** 队列用于唤醒和传递事件，不能成为唯一事实源。
6. **计算允许重复，发布只能一次。** 使用 fencing token 和 compare-and-swap 选择唯一获胜 Attempt。
7. **Serving domain 与 fault domain 分离。** 单张 GPU 是 fault domain，整台 8-GPU 机器是 H3 serving domain。
8. **恢复逻辑不依赖被恢复对象。** 特权 Node Agent 运行在 host systemd 下，不依赖 GPU Pod 或 container runtime。
9. **Preset 是用户承诺，Execution Profile 是内部手段。** 重试不得静默降低用户购买的质量档位。
10. **用户计费与内部成本分离。** 平台重试增加内部 COGS，但不重复向用户收费。

## 5. 系统上下文

```text
Client
  |
  v
API Gateway
  |
  v
Job Coordinator -------------------------- Billing Ledger
  |                                             |
  | PostgreSQL + Outbox                         | Payment / Credit Adapter
  v                                             v
Scheduler <---------- Worker Registry      External Billing System
  |
  | Assignment + Lease
  v
Kubernetes Worker Pod ----- Artifact Store ----- Object Storage
  |
  v
Inference Backend
  |
  v
8-GPU H3 Worker

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

第一版客户端接口：

```text
POST   /v1/jobs
GET    /v1/jobs/{job_id}
POST   /v1/jobs/{job_id}/cancel
GET    /v1/jobs/{job_id}/artifacts
```

`POST /v1/jobs` 必须接受 `Idempotency-Key`，成功接纳后返回 `202 Accepted` 和 `job_id`。

### 6.2 Job Coordinator

Job Coordinator 是异步执行语义的核心模块。它封装 Job 状态机、Attempt、Lease、重试、Artifact commit 和计费事件，不把这些规则分散到 API Gateway、Scheduler 和 Worker 中。

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
complete(lease_credentials, artifact_candidate) -> Accepted | Stale
fail(lease_credentials, failure)                -> RetryDecision
```

具体使用 gRPC、HTTP 或消息协议属于 transport adapter 的选择，不改变上述语义。

### 6.3 Scheduler

Scheduler 从持久状态中选择可运行 Job 和符合条件的 Worker。它不拥有 Job 状态，只能通过 Job Coordinator 的事务操作创建 Attempt 和 Lease。

Scheduler 负责：

- Admission control 和租户配额。
- Job 优先级、aging 和公平性。
- Execution Profile 与 Worker capability 匹配。
- 模型预热和数据 locality。
- 预计运行时间和队列完成时间计算。
- 重试时避开已知故障 Worker 或 fault domain。
- 防止失败任务引发 retry storm。

### 6.4 Worker Registry

Worker Registry 保存 Worker 的身份、epoch、capability、模型预热状态、当前 Assignment、GPU 拓扑和健康摘要。

Worker 重启后必须递增 `worker_epoch`。旧 epoch 签发的 Lease 不能在新进程中继续使用。

### 6.5 Inference Worker

Inference Worker 是长期运行并已预热的进程。H3 第一版中，一个 Pod 独占整台 8-GPU 节点，SGLang fork 在 Pod 内协调 Encoder、DiT 和 VAE 进程。

Worker 负责：

- 验证 Assignment 与 Execution Profile。
- 按 GPU UUID / PCI BDF 绑定角色。
- 周期性 heartbeat 并上报阶段进度。
- 将生成结果写入本地 NVMe scratch。
- 上传 Artifact 并提交 ArtifactCandidate。
- 在 Lease 被拒绝或收到 Stop 后终止执行并清理临时资源。

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

Billing Ledger 保存不可变报价快照、余额预授权、用户 Charge 和内部 UsageRecord。它通过 transactional outbox 接收 Job 终态事件，并使用 `job_id` 作为计费幂等键。

## 7. 领域模型

| 概念 | 定义 | 关键不变量 |
| --- | --- | --- |
| Job | 用户的一次推理意图 | 请求、报价和执行策略快照创建后不可变 |
| Attempt | Job 的一次物理执行 | 一个 Job 可有多个 Attempt |
| Lease | Worker 对 Attempt 的限时执行权 | 包含鉴权 token、单调 fence、worker epoch 和 expiry |
| Worker | 对外可调度的执行实体 | H3 中为完整 8-GPU appliance |
| ModelRevision | 确切的模型权重和配置版本 | 可复现，不使用浮动 latest |
| ExecutionProfile | 内部执行拓扑和加速方法 | 必须满足 Preset 承诺 |
| GenerationPreset | 用户可选择的质量、速度和 SLA 档位 | 独立版本化，不暴露硬件细节 |
| RateCard | Preset 的价格规则 | 有生效区间并生成不可变快照 |
| PricingSnapshot | Job 接纳时锁定的报价依据 | 排队期间调价不影响既有 Job |
| ExecutionPolicySnapshot | Job 接纳时锁定的执行策略 | retry、deadline 和取消语义不随配置漂移 |
| Artifact | 视频、缩略图、checkpoint 或 debug dump | 每个 Attempt 写独立不可变对象 |
| UsageRecord | 一次 Attempt 的实际资源消耗 | 用于内部 COGS，不等于用户 Charge |
| Charge | 用户需要支付的金额 | 每个成功 Job 最多 capture 一次 |

### 7.1 Profile 与 Preset

`ExecutionProfile` 面向 Scheduler 和 Worker，例如：

```yaml
id: h3-lossy-fast-v3
model_revision: minimax-h3-2026-08
resource:
  nodes: 1
  gpus_per_node: 8
topology:
  encoder_vae: 1
  dit: 7
runtime:
  engine: sglang-vela
  engine_revision: abc123
  precision: fp8
  acceleration_level: lossy-2
```

`GenerationPreset` 面向用户，例如：

```yaml
id: fast
revision: 3
model: minimax-h3
quality_class: fast
eligible_execution_profiles:
  - h3-lossy-fast-v3
  - h3-lossy-fast-v4
```

一个 Preset 可以映射到多个满足同一质量承诺的 Execution Profile。平台可以升级硬件或 backend，但不能在重试时把 `quality` Job 改为 `fast`。

## 8. 持久化与消息传递

### 8.1 权威状态

PostgreSQL 是以下数据的权威事实源：

- Job、Attempt 和 Lease。
- Worker epoch 和 Assignment。
- Artifact metadata 和获胜 Artifact 指针。
- PricingSnapshot、ExecutionPolicySnapshot、UsageRecord 和 Charge 状态。
- Outbox 事件和恢复审计记录。

第一版可以使用数据库行锁和 `SELECT ... FOR UPDATE SKIP LOCKED` 实现 Job claim。规模或吞吐增长后可以增加专用队列，但不能改变 PostgreSQL 的权威地位。

### 8.2 队列语义

Redis Streams、NATS JetStream 或 Kafka 可作为唤醒和事件传输设施，具体产品待选。队列消息只携带 `job_id`、事件类型和版本，不携带唯一状态。

所有关键状态变更与 outbox event 必须在同一数据库事务中提交。后台 dispatcher 发布成功后标记 outbox row，重复发布由消费者幂等处理。

Scheduler 必须有周期性 reconciliation scan，即使队列事件丢失也能重新发现可调度 Job、过期 Lease 和待处理计费事件。

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

PENDING_AUTH / QUEUED / ASSIGNED / RUNNING / FINALIZING / RETRY_WAIT
   -> CANCEL_REQUESTED -> CANCELED

任意可恢复状态超过 retry budget 或 deadline -> FAILED
```

终态为 `REJECTED`、`SUCCEEDED`、`FAILED` 和 `CANCELED`。`REJECTED` 表示未通过余额预授权或 admission control，尚未开始执行。终态 Job 不得重新进入执行态。

### 9.2 Attempt 状态

```text
ASSIGNED -> RUNNING -> FINALIZING -> SUCCEEDED
    |          |           |
    +----------+-----------+--> FAILED
    +----------+-----------+--> LOST
    +----------+-----------+--> CANCELED
    +----------+-----------+--> STALE
```

`STALE` 表示 Attempt 完成时 Lease 已失效或其他 Attempt 已获胜。STALE Attempt 不能发布 Artifact 或触发 Charge。

### 9.3 Worker 状态

```text
REGISTERING -> WARMING -> READY <-> BUSY
                    ^       |       |
                    |       +---+---+
                    |           v
                    |       DRAINING
                    |           |
                    |           v
                    +------ RECOVERING
                                |
                                v
                           QUARANTINED
```

`OFFLINE` 表示控制面无法确认节点状态。短暂 heartbeat 缺失首先作为 `SUSPECT` health condition 处理，超过 grace period 后才判定 Worker Lost。

## 10. 提交、执行与完成流程

### 10.1 Job 提交

1. 客户端提交请求和 `Idempotency-Key`。
2. API Gateway 完成认证、限流和基础校验。
3. Job Coordinator 解析 ModelRevision、GenerationPreset、输出规格和 ExecutionPolicySnapshot。
4. Billing Ledger 生成 PricingSnapshot。
5. 同一数据库事务写入 `PENDING_AUTH` Job、两个 snapshot、idempotency record 和 authorization outbox event。
6. 返回 `202 Accepted`、`job_id`、锁定报价和 `PENDING_AUTH` 状态。
7. Billing consumer 使用 `job_id` 幂等调用余额或支付 adapter。
8. 预授权成功后在事务中将 Billing 标记为 `AUTHORIZED`、Job 标记为 `QUEUED`；失败则 Job 进入 `REJECTED`。

外部支付或余额系统不参与 PostgreSQL 事务。预授权调用与状态落库之间的崩溃由幂等调用和 reconciliation 修复，不能依赖分布式事务。

相同租户下，相同 Idempotency-Key 和相同 request hash 返回原 Job；相同 key 但 request hash 不同返回冲突。

### 10.2 Assignment 与 Lease

1. Scheduler 选择 Job 和满足 Preset 的 Execution Profile。
2. Scheduler 过滤 capability、模型版本、拓扑、健康和维护状态不匹配的 Worker。
3. Job Coordinator 在事务中创建 Attempt 并签发 Lease。
4. Lease 至少包含 `attempt_id`、`worker_id`、`worker_epoch`、不可伪造的 `lease_token`、对该 Job 单调递增的 `fence` 和 `expires_at`。
5. Worker 接受 Assignment 后开始执行并续租。

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
```

`stage_progress` 是观测和预测信息，不作为恢复正确性的唯一依据。续租失败、Lease 被撤销或 Worker epoch 变化时，Worker 必须停止当前 Attempt。

### 10.4 完成与 Artifact commit

1. Worker 在本地 NVMe 完成编码和封装。
2. Worker 上传到 Attempt 独占 object key。
3. Worker 提交包含 size、checksum、content type 和 object key 的 ArtifactCandidate。
4. Artifact Store 验证对象存在且元数据匹配。
5. Job Coordinator 使用 Lease token、单调 fence 和 Job version 执行 compare-and-swap。
6. 获胜 Attempt 的 Artifact 标记为 `COMMITTED`，Job 进入 `SUCCEEDED`。
7. 同一事务写入 billing capture outbox event。
8. Worker 收到 `Accepted` 后清理本地 scratch。

旧 Attempt 晚到时返回 `Stale`，其对象进入短期清理策略。

## 11. 调度策略

### 11.1 候选过滤

Scheduler 首先执行硬约束过滤：

- Worker 状态为 READY 且没有维护或隔离 condition。
- Worker capability 满足 Execution Profile。
- ModelRevision 和 backend revision 可用或允许预热。
- GPU UUID / PCI BDF 拓扑符合要求。
- 租户、地域、数据驻留和安全策略允许。
- Preset 允许使用该 Execution Profile。

### 11.2 排序模型

通过硬约束过滤后，按预计完成时间和风险排序。初始模型可以是：

```text
score =
    queued_work_seconds
  + predicted_runtime_seconds
  + model_cold_start_penalty
  + locality_penalty
  + worker_health_risk_penalty
  + retry_risk_penalty
  - priority_credit
  - aging_credit
```

`predicted_runtime_seconds` 可以由输出时长、分辨率、帧数、denoise steps、ModelRevision、Preset 和历史 telemetry 拟合。模型必须持续用实际 Attempt 数据校准。

### 11.3 公平性与 admission control

- 按租户使用 weighted fair queue 或等价策略。
- aging 防止大任务长期饥饿。
- 每个租户限制 queued、running 和 retrying Job 数量。
- 重试保留原 Job 的等待年龄，但使用独立 retry lane 和预算，防止 retry storm。
- 无法满足 deadline 或配额时，应在接纳阶段返回明确结果，不能无限排队。
- Scheduler 使用 Preset 的优先级和 SLA 约束，不直接计算价格。

## 12. 重试与故障处理

### 12.1 Retry Budget

每个 Job 的 ExecutionPolicySnapshot 必须固化以下重试参数：

```text
max_attempts
max_total_compute_seconds
deadline_at
retry_backoff_policy
retryable_failure_classes
excluded_worker_ids
```

不能只使用固定的 `max_retries = 3`。长任务的重试必须同时受次数、累计计算时间和用户 deadline 限制。

### 12.2 失败分类

| 失败类型 | Job 处理 | Worker 处理 | 用户计费 |
| --- | --- | --- | --- |
| 参数非法、输入不支持 | FAILED | 无 | 释放预授权 |
| 模型或 Preset 配置错误 | FAILED 或熔断队列 | 标记 profile 不可用 | 释放预授权 |
| Worker heartbeat 丢失 | RETRY_WAIT | SUSPECT，随后恢复 | 不重复计费 |
| GPU Xid / fallen off bus | RETRY_WAIT | DRAINING / RECOVERING | 不重复计费 |
| 临时进程崩溃 | 按错误指纹重试 | process restart | 不重复计费 |
| 确定性 OOM | FAILED 或换兼容 profile | 无或降载 | 释放预授权 |
| Artifact upload 失败 | 保持 FINALIZING | retry upload | 不重新计算 |
| 多 Worker 相同错误 | 熔断 Job / revision | profile 调查 | 释放预授权 |
| 用户取消 | CANCELED | Stop + cleanup | 按取消策略 |

失败必须携带稳定的 `failure_class`、原始错误摘要、stage、Worker、GPU UUID、backend revision 和是否建议重试。不要让 Scheduler 解析自由文本日志决定重试。

### 12.3 重试放置

- 默认避开上一个失败 Worker。
- GPU 或 PCIe fault 后避开同一节点，直到恢复验证完成。
- 新 Attempt 必须继续满足原 GenerationPreset，不能静默降级。
- 相同 failure fingerprint 在多个 Worker 重复出现时应触发 Job 或 ModelRevision circuit breaker。
- 默认不启用 speculative duplicate execution；只有明确的高价值 SLA 才允许 hedging。

### 12.4 Checkpoint

第一版按阶段考虑恢复：

```text
Encoder -> DiT -> VAE -> Upload
```

Artifact upload 失败只重试上传。VAE 失败时，可以在验证收益后复用已完成的 DiT latent。任意 DiT step checkpoint 需要 backend 明确支持，并证明保存开销显著小于预期重算成本后才启用。

## 13. Artifact 设计

### 13.1 存储职责

- PostgreSQL 保存 Artifact metadata 和 Job 的最终 Artifact 指针。
- 对象存储保存视频、缩略图、checkpoint 和采样 debug dump。
- Worker 本地 NVMe 只作为有配额、可清理的 scratch 空间。
- API Gateway 不转发大文件内容。

### 13.2 Object key

```text
artifacts/{tenant_id}/{job_id}/{attempt_id}/video.mp4
artifacts/{tenant_id}/{job_id}/{attempt_id}/thumbnail.webp
checkpoints/{tenant_id}/{job_id}/{attempt_id}/dit-latent.bin
```

Object key 不包含 prompt、用户名或用户提供的原始文件名。对象默认不可变，每个 Attempt 使用独立前缀。

对象存储没有发布语义。Vela 不通过 copy + delete 模拟 rename，而是通过数据库指针决定哪个对象正式可见：

```text
jobs.result_artifact_id -> artifacts.id
```

### 13.3 Artifact metadata

```text
artifact_id
job_id
attempt_id
kind
object_key
storage_region
size_bytes
sha256
content_type
status
retention_until
created_at
```

状态至少包括 `STAGING`、`COMMITTED`、`EXPIRED` 和 `DELETED`。

### 13.4 访问与安全

- Bucket 保持 private。
- Worker 使用当前 Attempt 前缀的短期 multipart upload 权限。
- 客户端使用短期 signed GET URL 下载。
- 对公网高流量下载可以在 COMMITTED Artifact 前增加 CDN。
- 对象存储与计算集群保持同地域；跨地域复制异步进行。
- Prompt、输入和输出均视为租户敏感数据，访问必须记录审计日志。

### 13.5 生命周期

| Artifact 类型 | 默认策略 |
| --- | --- |
| 正式视频 | 按产品配置保留，例如 7 天或 30 天 |
| 缩略图 / 预览 | 跟随正式视频 |
| 失败或 STALE Attempt 输出 | 24 到 72 小时后清理 |
| DiT checkpoint | 短期保留，例如 24 小时 |
| Debug dump | 默认不保存，按故障采样 |
| Worker 本地 scratch | COMMITTED 后立即清理 |

具体期限属于产品和合规策略，不应写死在 Worker 中。

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
preset_revision
output_spec
rate_card_revision
quoted_amount_minor
currency
billable_units
```

`retry_policy_revision` 属于 ExecutionPolicySnapshot，而不是价格计算本身。ExecutionPolicySnapshot 和 PricingSnapshot 一起绑定到 Job，分别保证执行语义和报价不会在长时间排队期间漂移。

任务排队或执行期间发生调价、Preset 更新或硬件升级，都不能改变既有 PricingSnapshot。

### 14.3 计费流程

```text
submit
  -> quote
  -> authorize / reserve balance
  -> execute and retry
  -> Artifact COMMITTED
  -> capture Charge

FAILED / CANCELED
  -> release authorization or apply cancellation policy
```

Billing 状态与 Job 状态分离。成功生成但计费 adapter 暂时不可用时，Job 仍保持 `SUCCEEDED`，Charge 进入 `CAPTURE_PENDING`；Artifact 下载权限由明确的产品策略决定。

```text
AUTH_PENDING -> AUTHORIZED -> CAPTURE_PENDING -> CAPTURED
      |             |
      v             +---------------------------> RELEASED
 AUTH_FAILED
```

Charge 使用 `job_id` 和 charge type 作为幂等键。只有获胜 Attempt 的 Artifact commit 事务可以发出 capture event。

### 14.4 待确定的产品策略

- 用户在 QUEUED、RUNNING 和 FINALIZING 阶段取消时是否收费。
- 计费系统不可用时是否允许下载已生成 Artifact。
- 超出默认保留期后是否收取长期存储费用。
- Preset 的价格是否包含优先级和完成时间 SLO。
- 企业客户是否支持按实际 compute usage 的独立计费模式。

## 15. GPU 健康与恢复

### 15.1 恢复阶梯

| Level | 动作 | Worker 状态 |
| --- | --- | --- |
| L0 | restart inference process | DRAINING |
| L1 | 清理 CUDA process / context | DRAINING |
| L2 | `nvidia-smi --gpu-reset` | RECOVERING |
| L3 | PCIe FLR | RECOVERING |
| L4 | unload / reload driver | RECOVERING |
| L5 | reboot node | OFFLINE |
| L6 | BMC power cycle | OFFLINE |
| L7 | quarantine | QUARANTINED |

恢复流程：

```text
detect fault
  -> mark Worker DRAINING
  -> stop new Assignment
  -> wait grace period or hard-stop severe fault
  -> fence current Lease
  -> execute selected remediation
  -> run device and backend health tests
  -> warm model
  -> canary admission
  -> return READY or QUARANTINED
```

不能对所有错误无条件执行 FLR。Node Agent 必须根据 error class、设备 reset capability、拓扑限制和当前使用状态选择动作。

### 15.2 Kubernetes 与 driver

早期由主机镜像、PXE 或 Ansible 管理固定 kernel、driver、firmware 和 container toolkit。若使用 NVIDIA GPU Operator，建议先关闭其 driver 和 toolkit 生命周期管理，只使用经验证的 device plugin、DCGM 和 metrics 能力。

H3 MVP 可请求：

```yaml
resources:
  limits:
    nvidia.com/gpu: 8
```

长期可以通过 Device Plugin 暴露：

```yaml
resources:
  limits:
    vela.ai/h3-worker: 1
```

是否实现自定义扩展资源取决于多 backend 混部需求，不是 MVP 前置条件。

## 16. 一致性与高可用

### 16.1 Scheduler 高可用

可以运行多个 Scheduler replica，但 Assignment 必须通过数据库事务竞争。Scheduler 进程本身不持有不可恢复内存状态。

Scheduler 崩溃后：

- 未提交事务不会产生 Assignment。
- 已提交但未送达的 Assignment 由 outbox 重发或 reconciliation 发现。
- Worker 未续租时 Lease 最终过期，Job 进入重试判断。

### 16.2 网络分区

Worker 与控制面失联时，旧 Worker 可能仍继续计算。控制面可以在 grace period 后签发具有更大 fence 的新 Attempt，但旧 Attempt 已失效，不能推进 Job、发布 Artifact 或触发 Charge。

### 16.3 外部依赖故障

- PostgreSQL 不可用时停止新 Assignment，Worker 可以在有限 Lease 内继续当前 Attempt。
- 对象存储不可用时已完成推理停留在 FINALIZING，并保留本地 Artifact 后重试上传。
- Billing adapter 不可用时使用 outbox 重试 capture，不重新执行 Job。
- 队列不可用时依靠 PostgreSQL reconciliation 保证最终恢复。

## 17. 安全

- Client、Worker、Scheduler、Node Agent 和 storage credential 使用独立身份。
- 内部控制协议使用 mTLS 或等价的双向身份认证。
- Node Agent 具有高权限，其命令接口必须最小化并限制到已登记设备和动作。
- Node Agent 不接受任意 shell command，不把 PCI sysfs path 直接暴露给远端调用者。
- Worker 只能上传当前 Attempt 的对象前缀，不能列举其他租户对象。
- signed URL 具有短 TTL，并绑定 method、object key 和 content constraints。
- 日志禁止记录 prompt 正文、对象凭据、signed URL 和支付凭据。
- 所有管理动作、Artifact 访问、计费变更和节点恢复均需审计。

## 18. 可观测性

### 18.1 Job 指标

- 接纳率、队列长度和 queue wait time。
- 按模型、Preset、分辨率和租户统计运行时间分布。
- Job success、failure、cancel 和 retry rate。
- 每个 Job 的 Attempt 数量和累计 compute seconds。
- Artifact upload latency、失败率和 orphan bytes。
- Quote、authorization、capture 和 release 成功率。

### 18.2 Worker 与硬件指标

- READY、BUSY、DRAINING、RECOVERING 和 QUARANTINED Worker 数量。
- GPU utilization、memory、temperature、power 和 Xid。
- PCIe AER、fallen off bus 和 heartbeat loss。
- 各 remediation level 的执行次数、成功率和恢复耗时。
- ModelRevision / ExecutionProfile warm 状态和 cold-start 时间。

### 18.3 追踪标识

所有日志、指标和 trace 事件至少关联：

```text
tenant_id
job_id
attempt_id
worker_id
worker_epoch
model_revision
preset_revision
execution_profile_revision
```

## 19. 技术选型

| 方案 | 决策 |
| --- | --- |
| Kubernetes + Vela Scheduler | 推荐，分别承担 Worker 生命周期和 Job 调度 |
| K8s + Ray Serve | 第一版不采用，避免重复的资源和 replica 调度层 |
| Slurm | 不作为互联网在线 serving 主框架 |
| 纯 systemd + 自研调度 | 仅适合很小规模，集群增长后运维成本高 |
| Nomad | 可行，但当前没有足够收益替换 K8s 生态 |
| PostgreSQL | Job、Lease、Artifact metadata 和 Billing 的事实源 |
| S3-compatible object storage | 视频和 checkpoint 的持久存储 |
| Redis Streams / NATS / Kafka | 可选事件设施，具体产品待压测和运维评估 |

## 20. MVP 范围

第一阶段建议交付以下闭环：

- 单地域、单 Kubernetes 集群。
- MiniMax H3 单 backend。
- 一台 8-GPU 节点对应一个长期运行的 Worker Pod。
- PostgreSQL 事实源和 transactional outbox。
- 异步 submit/get/cancel 接口和 Idempotency-Key。
- Job、Attempt、Lease 和 fencing token。
- 基于硬约束与预计工作量的 Scheduler。
- Worker heartbeat、阶段进度和自动重试。
- S3-compatible Artifact multipart upload、CAS commit 和 signed download。
- ModelRevision、ExecutionProfile、GenerationPreset 和 RateCard 快照。
- 预授权、成功 capture、失败 release 和内部 UsageRecord。
- host systemd `vela-node-agent`，先实现安全的 process restart、drain、quarantine 和人工审批的高等级恢复。
- 基础 dashboard、审计和故障注入测试。

MVP 明确不包含：

- 任意 DiT step checkpoint。
- 自动跨地域 failover。
- 多节点 LLM Execution Profile。
- 自动 driver reload 或 BMC power cycle 的无监督生产执行。
- 基于机器学习的复杂运行时间预测。

## 21. 验收场景

1. 相同 Idempotency-Key 重复提交只创建一个 Job、一个预授权和一个最终 Charge。
2. Scheduler 在 Assignment 事务前后崩溃，Job 不会永久卡在不可恢复状态。
3. Worker 网络分区后旧 Attempt 完成，不能覆盖新 Attempt 的 Artifact。
4. GPU fault 导致 Worker Lost，Job 在 retry budget 内迁移到其他健康 Worker。
5. Artifact upload 失败只重试上传，不重新执行推理。
6. 两个 Attempt 同时完成时只有一个进入 SUCCEEDED 并触发 capture。
7. RateCard 在 Job 排队期间更新，既有 Job 仍使用原 PricingSnapshot。
8. Retry 可以更换 Execution Profile，但不能违反原 GenerationPreset。
9. GPU remediation 前 Worker 完成摘流量和 Lease fencing。
10. STALE Attempt、过期 checkpoint 和本地 scratch 按策略自动清理。
11. 队列短暂不可用后，reconciliation 能从 PostgreSQL 恢复待调度 Job。
12. Billing adapter 短暂不可用不会导致重复推理或重复 Charge。

## 22. 待决问题

- 第一版事件设施选择 PostgreSQL polling、Redis Streams、NATS JetStream 还是 Kafka。
- Worker acquisition 使用 pull、push 还是混合协议。
- heartbeat、Lease TTL、grace period 和每类 Job retry budget 的标定值。
- Preset 的质量承诺、加速方法和可测量验收指标。
- 用户取消运行中 Job 的收费规则。
- 预授权或支付 hold 的有效期是否覆盖最长排队和执行时间，以及过期后的重新授权策略。
- 计费失败时 Artifact 下载权限。
- 正式视频、checkpoint 和 debug Artifact 的默认保留期。
- H3 DiT latent checkpoint 是否具备正收益。
- 各 GPU 型号支持的 reset / FLR 能力和安全拓扑约束。
- 自定义 `vela.ai/h3-worker` Device Plugin 的启用时点。
- 未来 LLM 多节点 Worker 的 Lease 和 gang placement 模型。

## 23. 参考资料

- [Kubernetes Device Plugins](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/)
- [Kubernetes Node Problem Detector](https://github.com/kubernetes/node-problem-detector)
- [NVIDIA NVML Device Queries](https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html)
- [NVIDIA System Management Interface](https://docs.nvidia.com/deploy/nvidia-smi/)
- [Linux PCI Support Library](https://docs.kernel.org/driver-api/pci/pci.html)
- [NVIDIA GPU Operator Installation](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/getting-started.html)
- [Ray Serve on Kubernetes](https://docs.ray.io/en/latest/serve/production-guide/kubernetes.html)
- [Slurm Configuration](https://slurm.schedmd.com/slurm.conf.html)
