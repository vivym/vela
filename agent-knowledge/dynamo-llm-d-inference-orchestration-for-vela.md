# Dynamo、llm-d 与 Vela 推理编排研究

> 研究日期：2026-08-29
>
> 研究深度：deep
>
> 主题：Dynamo, llm-d, and inference orchestration patterns applicable to Vela
>
> 来源索引：`resources/dynamo-llm-d-inference-orchestration-for-vela-sources.json`
>
> 实现对齐：2026-09-03，Vela `b09867c8b431338a6e71c6bf2c766c5b0d905724`

## 1. 结论先行

Vela 不应把 NVIDIA Dynamo 或 llm-d 当成可替换自身控制面的成品。
它们解决的是不同层次的问题：

- Dynamo 和 llm-d 的核心是在线推理 serving data plane：请求进入后，在毫秒级完成
  endpoint 选择、排队、Prefill/Decode 协调、KV 传输和流式返回。
- Vela 的核心是持久化 B2B inference control plane：Admission、信用预留、Job / Attempt /
  Lease、数十分钟执行、重试、Artifact、Visible Completion、Charge、租户隔离和审计。
- 两者可以组合，但 authority 必须分层。PostgreSQL 继续拥有 Vela 的业务和执行真相；
  Router、KV index、engine queue 和 transfer rendezvous 是可重建的快状态。

最值得借鉴的不是某个部署 YAML，而是五组机制：

1. **快速决策与持久 authority 分离。** 借鉴 Dynamo Router 和 llm-d EPP 的
   `Filter -> Score -> Pick`，但 Assignment 最终仍由 Vela Coordinator 在 PostgreSQL
   中以 Job version、Worker epoch、Lease fence 原子确认。
2. **执行拓扑成为显式版本化合同。** 为未来 LLM 增加 Aggregated、P/D、E/PD、E/P/D
   等 `ExecutionGraphRevision`，不要把一个多阶段请求假装成单一 Worker 调用。
3. **KV 目录是高频、可重建的 serving-plane 状态。** 不进入 Vela transactional outbox，
   不成为 PostgreSQL 业务事实；只把阶段开始、阶段终止、Transfer 结果和最终 Attempt
   结果持久化。
4. **Planner 与 Scheduler 分成快慢两个闭环。** Scheduler 选择当前资源；Planner 根据
   workload、SLO、成本、最小副本和冷启动预算提出容量效果。先 advisory / shadow，
   再由独立 actuator 修改 Kubernetes desired state。
5. **把可解释性和 freshness 作为正确性条件。** 每次候选过滤、打分、选择、降级和拒绝
   都要带有有界 reason code；缺失或过期的 load / readiness / KV 信号必须有明确的
   fail-closed 或降级规则。

研究完成后的产品约束进一步明确：Encoder、DiT 和 VAE Decoder 都是长时间执行组件，
跨机 Artifact 传输不是当前吞吐主导项，因此原先“一台 8-GPU 主机等于一个不可拆
Worker”的建议已经失效。Vela 现已实现显式 Encoder -> DiT -> VAE/CPU Stage graph、
跨节点 StageArtifact、Encoder/DiT exact cache、常驻 ModelRuntime 和按 GPU 独占的
WorkerMember。当前 H3 DiT 仍是单 GPU、单进程；多 GPU/多节点 gang 保留给未来 LLM 或
其他经过认证的 profile。借鉴点仍来自 Dynamo/llm-d 的分层 authority、显式执行拓扑、
快慢调度闭环、freshness 和可重建 cache/index，但不直接引入其 request router 作为
Vela Job authority。[V09]

## 2. 证据边界

### 2.1 固定版本

| 项目 | 检查 commit | 日期边界 | 用途 |
| --- | --- | --- | --- |
| NVIDIA Dynamo | `4c7e98162232d24147daa701d6bfcf93f2fa4edf` | 2026-08-29 | Router、Planner、disaggregation、fault tolerance、observability |
| llm-d | `1f97eb0f928fd3c6509fce984ad67af83d65d3e8` | 2026-08-27 | Kubernetes composition、InferencePool、well-lit paths、autoscaling、batch |
| llm-d Router | `644a885639ac64ca09d6f35af3a67fe61bcc2e31` | 2026-08-29 | EPP、Flow Control、Filter/Score/Pick、Coordinator、metrics |
| llm-d KV Cache Manager | `8cf43067afb7fc9fefafc1b64de063c769f2c90f` | 2026-07-23 | KV events、dedup、index recovery |
| Vela research baseline | `bc590e20b3e81ee54651ac7766c8ecd82b394097` | 2026-08-29 | 原始研究和建议基线 |
| Vela implementation alignment | `b09867c8b431338a6e71c6bf2c766c5b0d905724` | 2026-09-03 | 分阶段 H3、常驻模型、exact cache、CPU Worker、单/多成员 gang |

所有 GitHub 代码引用都使用 commit-pinned permalink。`docs.nvidia.com`、`llm-d.ai`
等网页只作为入口；具体判断优先绑定到上述仓库文件。来源清单共 63 条，包含 40 条
官方或第一方文档、14 条第一方源码、9 条 Vela 代码/设计证据。V01-V08 保留原始研究
快照，V09 单独记录随后完成的实现对齐，避免用新代码倒推上游事实。

### 2.2 标签约定

- **确认事实**：在固定 commit 的文档或实现中直接观察到。
- **推导**：从多个确认事实得到的架构解释，不代表上游项目承诺。
- **建议**：针对 Vela 的设计选择，尚未实现。
- **未知**：需要 benchmark、故障注入、产品合同或生产数据回答。

上游 README 中的性能数字只记录为上游自述，不作为 Vela 容量或 SLO 结论。不同模型、
硬件、请求分布、并发、网络和 engine revision 下不能外推。[D01][L01]

## 3. 三个系统的边界

| 维度 | Dynamo | llm-d | Vela 当前基线 |
| --- | --- | --- | --- |
| 主要对象 | 在线 inference request / stream | 在线 inference request、InferencePool | Accepted Job、Attempt、Lease、ArtifactSet、Charge |
| 事实源 | runtime / discovery / router state；部署可用 Kubernetes native discovery | Kubernetes resources、EPP 内存状态、model-server metrics | PostgreSQL；JetStream 只负责可重放投递和唤醒 |
| 时间尺度 | token/request，毫秒到分钟 | token/request，毫秒到分钟 | Job，分钟到数十分钟 |
| 调度目标 | KV reuse、load、TTFT/ITL、拓扑 | Filter/Score/Pick、fairness、flow control、SLO | 合同容量、weighted deficit、aging、retry、认证 profile、Worker health |
| 执行拓扑 | aggregated、P/D、E/P/D、multi-tier KV | aggregated、P/D、E/P/D、InferencePool variants | 版本化 Stage graph；H3 Encoder/DiT/VAE 可跨节点，当前 DiT 单 GPU，未来 LLM 支持多成员 gang |
| 故障语义 | request cancellation、migration、drain | proxy retry、queue TTL、pod readiness、coordinator errors | at-least-once execution、exactly-once Visible Completion、fenced Lease |
| 多租户 | request priority / hints / policy class | Priority Band、FlowKey、fairness ID、usage limits | Organization / Project RLS、信用、运行/排队限额、ServiceClassRevision |
| 计费/审计 | 非核心 | 非核心 | 核心合同 |

**推导：** Dynamo/llm-d 更适合作为 Vela 管理的 `InferenceBackendRevision` 或执行平面
组件，而不是 Job authority。Vela 可以让一个 Worker 或 Worker group 内运行 Dynamo /
llm-d，但不能让其内存 queue 或 KV directory 决定 Job 是否存在、是否收费或哪个
Attempt 赢得 Visible Completion。

### 3.1 未来选栈规则

- **当前 H3：两者都不直接引入请求主路径。** 保持 Vela Scheduler + H3 backend；只吸收
  planner、drain、decision evidence 和阶段观测模式。
- **未来 Kubernetes 标准化在线 LLM 平台：优先评估 llm-d。** 它以 Gateway API、
  InferencePool、Envoy/ext-proc、KServe composition 为中心，更容易接入已有 Kubernetes
  traffic policy 和 CRD lifecycle。[L02][A01][A02]
- **未来需要一体化多 engine runtime、KV router、P/D、KVBM 和 SLA planner：优先评估
  Dynamo。** 代价是引入更完整的 Dynamo runtime/operator/observability contract。[D01]
- **一个 `ExecutionProfileRevision` 只选一个 request router/coordinator owner。** 初期不要
  让 Gateway EPP 再进入 Dynamo KV Router 做两次独立 endpoint 选择；如需组合，外层只能
  做 pool/tenant policy，内层才做 worker/KV selection，并写清唯一 retry owner。
- 两个主要仓库均以 Apache-2.0 发布，但实际复制源码仍需保留 notice、依赖许可证和
  supply-chain 审查；本研究不构成法律结论。[D01][L01]

## 4. NVIDIA Dynamo：可借鉴机制

### 4.1 Runtime、Frontend、Router 与 Worker

**确认事实：** Dynamo 明确定位为 inference engine 之上的 orchestration layer，不替代
SGLang、TensorRT-LLM 或 vLLM。当前 Kubernetes 模式可使用 CRD + EndpointSlice 做
discovery，TCP 承载 request plane；大多数 Kubernetes 部署不要求 etcd 或 NATS。
NATS/JetStream 只在选择相应分布式模式或事件能力时需要。[D01]

**可借鉴点：**

- Vela 不要让 JetStream 演化成 Worker inventory 或 Job state authority。
- 为 Worker protocol 保留直接、低延迟的 request/control transport；JetStream 继续做
  PostgreSQL outbox 的 durable wakeup。
- discovery 与 authority 分开：Kubernetes/EndpointSlice 描述可达 endpoint，Vela
  PostgreSQL 描述谁有执行权。

### 4.2 KV-aware routing 不是“命中率最大化”单目标

**确认事实：** Dynamo Router 同时处理 KV overlap、worker load、availability、overload、
allowed worker、pinned worker、DP rank 和 taint/routing constraints。`RoutingEligibility`
会在 affinity-derived pin 路径上仍然强制检查 hard availability；overload 与 unavailable
是不同原因。[D04][D05][D06][D16][D18]

**确认事实：** Dynamo 提供 FCFS、LCFS 和 WSPT queue policy。WSPT 用
`(1 + priority_jump) / new_tokens` 排序，其中 `new_tokens` 会扣除有效 KV overlap；
strict priority 仍是更高层的排序键。[D07][D17]

**推导：** KV affinity 只能是候选效用的一项，不能越过健康、租户、profile、拓扑和
fencing 约束。Vela 当前 scheduler 已经先做 Worker/Profile/Organization/Project 的硬
资格判断，再计算顺序；未来加入 cache/locality scorer 时应保留这个顺序。[V02][V03]

### 4.3 Disaggregated Prefill/Decode

**确认事实：** Dynamo 将 Prefill 和 Decode 建模为计算特征、内存占用和扩缩容需求
不同的 worker pool；远端 Prefill 生成的 KV 必须传给 Decode，传输拓扑可能用 required
或 preferred constraint。required 模式没有同域 Decode capacity 时可以合法失败，
preferred 模式允许在过载或不可用时跨域。[D02][D08]

**适配到 Vela：**

- `ExecutionProfileRevision` 需要声明 `aggregated`、`P/D`、`E/PD` 或 `E/P/D`，以及每个
  stage 的 engine revision、资源角色、最小/最大副本、transfer connector 和 topology
  policy。
- 一次 Vela Attempt 仍代表一次用户可见执行尝试；其内部可包含多个 `StageAttempt`。
- Stage 之间不能共享同一个模糊 Lease。建议每个 stage 有独立 `StageLease`/fence，
  Attempt Coordinator 持有整个 execution graph 的推进权。
- KV bytes 和 block membership 不进 PostgreSQL；只持久化 `TransferTicket` identity、
  connector revision、源/目的 stage、开始/完成/失败摘要和证据 digest。

### 4.4 Planner 是慢闭环，不是请求级 Scheduler

**确认事实：** Dynamo Planner 把环境 I/O、planner engine 和 actuation 拆开；
`PlannerScalingState` 有意不做 I/O，状态机根据 worker counts、traffic observations、
forward-pass metrics、SLA 和 GPU/power budgets 产生 effect。Planner 支持 advisory 模式，
minimum endpoint 还会在变更前校验 GPU/power budget。[D09][D10][D19][D20]

**可借鉴点：**

- Vela 新增 `CapacityPlanner`，输入必须是版本化 cohort 和有 freshness 的观测快照，输出
  是 `PlannerProposal`，不能直接在同一个函数里改 Kubernetes。
- `PlannerProposal` 至少包含 input digest、algorithm revision、current/desired capacity、
  reason codes、预算约束、stabilization window 和过期时间。
- `FleetActuator` 单独执行 proposal，并把 Kubernetes observed generation、ready capacity、
  actuation latency 和失败结果回写为可审计 evidence。
- 首先运行 advisory/shadow；只有离线 replay、线上 shadow 和 failure injection 都通过，
  才允许 auto-apply。

### 4.5 Fault tolerance 的适用边界

**确认事实：** Dynamo request migration 在 frontend pipeline 中累计已生成 token，连接
失败时把原 prompt + 已生成 token 重新发给健康 worker；这是 request/stream 级恢复，
由 frontend 的 migration limit 约束。[D12]

**确认事实：** cancellation 通过 `AsyncEngineContext` 父子链传播；runtime metric 只证明
worker 收到 cancellation signal，不证明 engine 已停止。[D14]

**确认事实：** graceful shutdown 先从 discovery 注销 endpoint，再等待 grace period，
随后 invalidates endpoint 并在有界 timeout 内等待 in-flight request；liveness 和 readiness
在 drain 期间刻意分离。[D11]

**对 Vela 的判断：**

- 借鉴 drain 顺序、父子 cancellation propagation 和“收到取消不等于实际停止”的观测
  区分。
- 不把 token migration 当成 Vela Attempt retry。对 H3 视频，若 engine 没有经过认证的
  durable checkpoint，Worker 丢失后仍应开始新 Attempt；不能把部分视频状态拼接成成功。
- 即使未来 LLM backend 内部完成 token migration，Vela 仍需用当前 Attempt/Lease fence
  约束最终结果，避免旧 stream 在迁移后形成可见完成。

### 4.6 Observability

**确认事实：** Dynamo 区分 Prometheus pull metrics 与 OTLP push trace/log；请求通过
`x-request-id`、`trace_id`、`span_id` 关联。Forward Pass Metrics 的本地 trace queue 是
bounded/nonblocking，落后时丢诊断记录而不阻塞 inference；文档明确这些文件不是
durable event log。[D15]

**可借鉴点：** Vela 应维持当前低基数 Prometheus label 纪律，把 Job/Attempt/Worker ID
放在 trace/log/evidence，而不是指标 label。[V08] 高频 engine telemetry 可以丢样或聚合；
Lease、Job transition、Charge、Visible Completion 不能丢，必须继续走 PostgreSQL。

## 5. llm-d：可借鉴机制

### 5.1 Kubernetes-native composition

**确认事实：** llm-d 由 Proxy + Endpoint Picker (EPP) 构成 Router；`InferencePool`
以 selector 表示一组 model-server endpoints，variant 通过 pod labels 区分 Prefill、Decode、
成本或性能角色。Proxy 使用 Gateway API Inference Extension 的 ext-proc protocol 向 EPP
询问 endpoint。[L01][L02][A01]

**可借鉴点：**

- 把 backend-specific request parsing、endpoint selection 和 proxy transport 与 Vela
  Coordinator 分开。
- 若未来提供同步 LLM API，可增加独立 `OnlineInferenceGateway`，通过 GAIE/EPP 或 Dynamo
  Router 进入 serving plane；不要把 HTTP streaming 生命周期塞进现有异步 Job API。
- Vela 的 WorkerPool/ProfileCertification 可向 InferencePool/variant 投影，但投影不是
  authority；从 Kubernetes 删除 Pod 不能直接删除 Vela Job/Attempt。

### 5.2 `Filter -> Score -> Pick` 插件管线

**确认事实：** llm-d scheduler 对每个 request 依次运行 Filters、Weighted Scorers 和一个
Picker；P/D 模式由 profile handler 分别执行 Prefill/Decode profile。现有插件覆盖 label、
SLO headroom、prefix affinity、KV utilization、queue depth、running requests、token load、
LoRA affinity、session affinity、no-hit LRU 等。[L03][R02]

**建议：** Vela 不直接加载任意第三方 Go plugin。先定义静态编译、版本化、纯函数式的
决策接口：

```text
CandidateFilter(snapshot, job, worker) -> eligible | reason_code
CandidateScorer(snapshot, job, worker) -> bounded_score + evidence
CandidatePicker(scored_candidates)     -> selection + tie_break
```

每个决策写入有 TTL 的 `SchedulerDecisionEvidence` 或外部 evidence store，包括 policy
revision、input digest、候选数量、过滤 reason 计数、winner 和 tie-break。Job aggregate
不需要保存全部候选列表。

### 5.3 Flow Control 的强项和局限

**确认事实：** llm-d Flow Control 实现严格三层：Priority Band -> Fairness Flow ->
Ordering Item。flow control 开启后，饱和请求在 EPP 内存排队，受 per-band/global
`maxRequests`、`maxBytes`、TTL 约束；无 endpoint 可以使用单独的 scale-from-zero TTL。
metrics 过期时 endpoint 按 fully saturated 处理，属于 fail-closed。[L04][L09][R05][R06]

**确认事实：** program-aware fairness 以 header 中的 program ID 识别 flow，使用输入/
输出 token 加权的 attained service；它带半衰期和 idle eviction。该实现也公开承认
abandoned request 可能让 in-flight 计数无法归零，高 churn ID 会造成 TTL 范围内内存和
Prometheus series 增长。[R06]

**确认事实：** llm-d Router 的 flow-control queue/fairness state 是 per EPP replica，
Active-Active 时不共享；per-band 容量也按 replica 生效。approximate prefix state 也会
因 replica 分片而降低命中。[R12]

**对 Vela 的判断：**

- 借鉴 Priority/Fairness/Ordering 的分层概念和 attained-service 校正。
- 不采用 EPP 内存 queue 作为 Accepted Job 队列。Vela `202 Accepted` 是持久承诺，队列
  必须可重启恢复，当前 PostgreSQL scheduler authority 更合适。[V01][V02][V03]
- Vela 已有 weighted deficit、Protected Lane、retry lane 和 immutable p95 runtime。
  可以在同一层增加“已获得服务量”校正，但单位应是认证/校准后的 predicted GPU-seconds
  或 profile cost，不是 LLM token；H3 视频没有可比 token 单位。
- fairness identity 必须来自已认证 Organization/Project/ServiceClass，不接受用户任意
  header 直接创建高基数 flow。

### 5.4 KV event 与 index

**确认事实：** llm-d precise prefix cache 路由由 model server 发出 KV events，indexer
维护 block 到 endpoint 的映射；事件路径包含 dedup，index 允许 in-memory backend 并以
snapshot/recovery 修复。approximate 与 precise 路由是不同成熟度/成本的选择。
[L06][K01][K02][K03][K04]

**建议：** 为未来 LLM 建一个独立 `ServingStatePlane`：

```text
Engine KV events -> per-pool ingest -> dedup/sequence check -> in-memory/sharded index
                                          |                    |
                                          +-> lag/freshness     +-> Router lookup
                                          +-> snapshot/rebuild  +-> no business authority
```

不要把每个 KV block event 放进 `VELA_EVENTS`。Vela stream 当前有 1 MiB message、7 天、
64 GiB、业务 envelope 和 durable consumer contract；把高频 cache event 混入会污染
outbox recovery、拉长 RPO/RTO，并让 JetStream 容量与模型 token rate 耦合。[V06][V07]

### 5.5 Coordinator 与阶段编排

**确认事实：** llm-d coordinator 把请求构造成 E/P/D pipeline steps，connector 负责
NIXL、shared storage 或 engine-specific KV transfer。Gateway 选 endpoint，Coordinator
推进 encode/prefill/decode；实现和测试围绕 pipeline、step、connector 分层。[R03][R04]

**适配到 Vela：** 借用 step/connector 分层，但把持久状态提升到 Vela：

- `ExecutionGraphRevision`：不可变 topology 与 connector contract。
- `StageAttempt`：某 Attempt 内一次 stage 物理执行。
- `StageLease`：stage-specific authority；至少包含 parent Attempt fence。
- `TransferTicket`：源/目的、connector revision、token/embedding/KV 类型、deadline、
  integrity digest、状态摘要。
- `AttemptCoordinator`：只有它可推进 durable stage 状态；backend coordinator 只返回
  observation/result。

### 5.6 Autoscaling 与 batch

**确认事实：** llm-d 同时支持 HPA/KEDA 类局部扩缩容和 WVA 类跨 variant 优化；batch
由 Batch Gateway 管理 Job，Async Processor 从消息队列取单个 inference request，并根据
系统指标节流，以保护 interactive traffic。[L07][L08][L11]

**适配到 Vela：**

- Vela 当前就是 durable async workload，不需要复制 llm-d Batch Gateway。
- 可借鉴“interactive 保留 headroom、batch 吃剩余容量”的策略，但要表达为
  ServiceClassRevision/CapacityShare，而不是两套互不知情的 queue。
- KEDA/HPA 适合 stateless gateway/router；昂贵、长冷启动、gang-scheduled 的 H3/LLM
  worker 更适合 Vela Planner 给出 desired capacity，再由 Fleet Controller 执行。
- KEDA 的 min/max/cooldown/fallback 是可参考的 actuator guard，不是 SLO planner。[A06]

## 6. 当前 Vela：已经具备什么

以下均以 `bc590e20b3e81ee54651ac7766c8ecd82b394097` 为准。

### 6.1 已确认实现

1. **PostgreSQL authority。** Admission 原子完成 idempotency、SKU/profile 解析、Project /
   pool queue limit、credit reservation、Job 和 outbox。[V01]
2. **Job / Attempt / Lease 分离。** `workercontrol.Service` 使用 Worker mTLS identity、epoch、
   signed Lease token、fence 和 PostgreSQL time；Start/Heartbeat/Completion 都重新验证
   authority。[V04]
3. **Exactly-once Visible Completion。** Artifact manifest 完整性、CreditReservation、
   Charge、ArtifactSet 和 Job 成功在数据库事务中形成唯一业务结果。[V05]
4. **分层 scheduler。** migration `00008_hierarchical_scheduler.sql` 实现 retry、protected、
   normal lanes，weighted deficit，aging，certified p95 runtime，worker tail projection，
   dispatch claim 和 deterministic tie-break。[V02][V03]
5. **Outbox/Inbox + JetStream。** Outbox 先 claim 后 publish，要求 durable PubAck；
   `Nats-Msg-Id` 绑定 event ID，Inbox transaction 去重；Scheduler consumer 只把
   `job.ready` 当 wakeup，随后回 PostgreSQL 取 authority。[V06][V07]
6. **Fleet lifecycle。** 已有 readiness cycle、capacity observation、drain、worker epoch、
   node remediation 和 profile readiness，不只是 Kubernetes Deployment 的简单 ready 位。
[V01]
7. **多租户和可观测性边界。** Organization/Project/RLS、信用和 capacity share 已持久；
   HTTP metrics 使用 bounded method/route/status labels，SLO 以 immutable cohort 计算，
   不把 Job/Principal/Worker ID 放进 Prometheus label。[V08]

### 6.2 仍不是 production-ready 的证据

`docs/architecture.md` 明确记录 Production Gates 仍为 `0/9 PASS`。因此“代码路径
存在”和“生产闭环已验证”必须分开；本研究不能把 48 个 slice 或通过的单元/集成测试
写成正式流量 readiness。[V01]

### 6.3 面向通用 LLM 的真实缺口

| 缺口 | 当前状态 | 影响 |
| --- | --- | --- |
| 在线 streaming aggregate | 只有异步 Job 契约 | 不能直接承载 token-stream lifecycle |
| 多阶段 execution graph | Attempt 绑定一个 Worker/Profile | P/D、E/P/D 的多 endpoint authority 不明确 |
| Stage Lease/fence | 只有 Attempt execution/finalization Lease | 某 stage 迁移或重试可能与旧 stage 竞态 |
| KV directory | 无 | 不能精确 cache-aware route；也没有 freshness/rebuild contract |
| KV/embedding transfer | 无通用 connector contract | 无法认证 P/D 或 E/P/D profile |
| 慢闭环 planner/actuator | 有 ETA/capacity prediction 和 Fleet Controller，但没有通用 SLO/cost planner proposal | Scheduler 只能选已有容量 |
| Router decision framework | SQL 内有强调度逻辑，但缺少通用 versioned filter/score evidence ABI | 加新信号容易侵入 authority SQL |
| Serving-plane replay benchmark | 有集成/故障测试，但没有上游同类 request trace/route replay 闭环 | 无法量化 KV/latency/公平性收益 |

### 6.4 代码级借鉴地图

| 上游/现有代码路径 | 直接观察 | Vela 中的落点 |
| --- | --- | --- |
| Dynamo `lib/kv-router/src/scheduling/filter.rs` | hard availability、allowed set、overload、taint 分开判定 | 在 scorer 之前固定 `CandidateFilter` reason taxonomy；Coordinator 仍事务内 recheck |
| Dynamo `lib/kv-router/src/scheduling/policy.rs` | FCFS/LCFS/WSPT，WSPT 扣除有效 cache overlap | shadow evaluator 中实验，不直接替换 Vela weighted deficit/Protected Lane |
| Dynamo `lib/kv-router/src/scheduling/selector/default.rs` | 选择器组合 locality 与 load | 未来 LLM backend 的 serving-plane picker，不进入 billing/Job SQL |
| Dynamo `components/src/dynamo/planner/core/state_machine.py` | planner state 无 I/O | 新 `CapacityPlannerCore` 保持纯决策，可做 deterministic replay |
| Dynamo `components/src/dynamo/planner/core/base.py` | environment、tick、diagnostics、advisory、budget 校验 | `PlannerRunner` + `PlannerProposal` + 独立 `FleetActuator` |
| llm-d Router `pkg/epp/flowcontrol/registry/registry.go` | per-flow queue/lifecycle/occupancy 为内存状态 | 借鉴 bounded occupancy；拒绝将其作为 Accepted Job authority |
| llm-d Router `pkg/epp/flowcontrol/controller/controller.go` | dispatch、backpressure、eviction outcome 分层 | 为 Vela pre-admission/online gateway 定义有界 outcome，不改变 durable Job queue |
| llm-d Router `pkg/epp/framework/plugins/flowcontrol/fairness/program-aware/README.md` | least attained service、decay、idle eviction | 用 authenticated tenant + calibrated GPU-seconds 适配，并避免 program ID 指标爆炸 |
| llm-d Router `pkg/coordinator/pipeline/pipeline.go` | stage pipeline 与 step registry | `ExecutionGraphRevision`/`StageAttempt`，由 Vela Attempt Coordinator 持久推进 |
| llm-d KV Manager `pkg/kvevents/event_dedup_filter.go` | KV event dedup | 独立 ServingStatePlane ingest，不复用业务 Inbox |
| llm-d KV Manager `pkg/kvcache/indexer.go` | prefix-chain lookup 与 backend abstraction | per-pool ephemeral KV index，带 lag/snapshot/rebuild contract |
| Vela `db/migrations/00008_hierarchical_scheduler.sql` | durable hierarchy、projection、claim、deterministic tie-break | 保持主 authority；先增加 evidence，不先重写算法 |
| Vela `internal/scheduler/service.go` | claim 后调用 Coordinator Acquire，失败后 abandon | 接入 shadow decision/proposal 的最窄边界 |
| Vela `internal/workercontrol/service.go` | Worker epoch、signed Lease token、fence | 扩展 parent Attempt 与 stage authority，不绕过现有认证链 |
| Vela `internal/workercontrol/visible_completion.go` | Artifact/credit/Charge/Job 原子完成 | 无论 backend 如何迁移/分解，唯一用户可见完成仍在这里收口 |
| Vela `internal/outbox/publisher.go` | durable PubAck、claim token、失败重试 | 只发布低频 durable domain event；KV block event 留在 serving plane |

## 7. Borrow / Adapt / Reject 排名

### 7.1 P0：直接借鉴，保持 Vela authority

| 排名 | 模式 | 采取方式 | 依赖 | 验收 |
| --- | --- | --- | --- | --- |
| P0-1 | Scheduler decision explainability | 为现有 SQL claim 输出有界 filter/reason/score evidence；不改变选中语义 | 固定 reason taxonomy、evidence retention | replay 相同 snapshot 得到相同 winner/digest |
| P0-2 | freshness fail-closed | 所有 readiness/load/capacity 观测携带 epoch、sequence、observed_at、expires_at | PostgreSQL/K8s time contract | stale/missing/mismatched epoch 不可分配且有独立指标 |
| P0-3 | drain 顺序 | 先停止新 Assignment，再 drain active Lease，最后缩容/替换 Pod | Fleet drain、K8s finalizer、Lease TTL | rollout 期间无新任务进 draining worker，旧任务按 policy 结束 |
| P0-4 | planner advisory/shadow | 先只生成 `PlannerProposal` 与对照指标，不自动执行 | workload trace、cost/SLO targets | proposal 可重放，误差与振荡预算可量化 |
| P0-5 | bounded observability | metrics 低基数；ID 进 trace/evidence；高频诊断可丢、业务事件不可丢 | telemetry schema | cardinality budget test 与 dropped-diagnostic metric |

### 7.2 P1：需要 Vela 语义适配

| 排名 | 模式 | 适配设计 | 主要风险 |
| --- | --- | --- | --- |
| P1-1 | `Filter -> Score -> Pick` | 静态编译、版本化纯函数；Coordinator 事务内 recheck | scorer 与 authority snapshot 漂移 |
| P1-2 | execution graph | `ExecutionGraphRevision` + `StageAttempt` + `StageLease` | 状态爆炸、跨 stage 取消/重试不清 |
| P1-3 | KV state plane | 独立事件/索引/快照/重建；不进入业务 outbox | 丢事件、乱序、stale hit、跨 replica 一致性 |
| P1-4 | topology-aware transfer | required/preferred domain + connector certification | 网络 topology 不是 transport health |
| P1-5 | program-aware fairness | 用 authenticated tenant + calibrated GPU-seconds；保留 Vela deficit/aging | cost model 偏差造成租户不公平 |
| P1-6 | SLO/cost planner | proposal/actuator 分离，min capacity、budget、cooldown、hysteresis | 冷启动、gang scheduling、观测延迟造成振荡 |
| P1-7 | backend migration | 只在认证 engine/profile 内部启用，最终仍受 Attempt fence | 部分输出重复、随机采样不可复现 |

### 7.3 Reject：当前不要采用

| 模式 | 拒绝原因 | 可重新评估条件 |
| --- | --- | --- |
| EPP 内存 queue 作为 Accepted Job queue | 重启丢失、per-replica fairness、不能支持 `202 Accepted` 持久承诺 | 只用于独立同步 Online API 的 pre-admission queue |
| JetStream 作为 Job/KV authority | 与 PostgreSQL authority 冲突；高频 KV event 会污染业务 RPO/RTO | 不重新评估 Job authority；KV 可用独立非业务 stream |
| 把 Dynamo/llm-d 的 P/D 或 E/P/D 拓扑原样当作 H3 graph | H3 的 Encoder/DiT/VAE、Artifact、cache 与完成语义不同 | 仅对语义匹配且完成 failure/SLO certification 的未来 backend profile 评估 |
| 连接断开直接等于 Job cancellation | 异步 Job 与 HTTP 生命周期不同；客户需显式取消并有计费边界 | 仅同步 Online API 可使用 connection-scoped cancel |
| 把 token migration 当 exactly-once execution | migration 是 stream 恢复，不解决旧 Worker finalization 竞态 | 始终保留 Vela fence/Visible Completion |
| 任意动态第三方 scheduler plugin | authority、确定性、供应链和 replay 风险过高 | 有签名 ABI、resource limits、determinism test 和 rollback |
| 在 Prometheus label 放 tenant/job/program 任意 ID | cardinality 和隐私风险 | ID 只进入 trace/log/evidence，指标用有界 cohort |

## 8. 建议目标架构

### 8.1 两层编排

```text
Durable Control Plane (Vela)
  API / Admission / Credit / Job / Attempt / Lease / Billing / Artifact
  PostgreSQL authority + transactional outbox + JetStream wakeup
                         |
                         | Assignment + immutable ExecutionGraphRevision
                         v
Serving Execution Plane (per backend/profile)
  Gateway/Router -> Aggregated or E/P/D Coordinator -> Engine workers
        |                       |                         |
        +-- load/KV index ------+-- transfer connector ---+
        +-- bounded telemetry / trace / result evidence --+
```

### 8.2 状态所有权

| 状态 | Owner | 持久性 | 原因 |
| --- | --- | --- | --- |
| Organization/Project/credit/Job | PostgreSQL | durable | 业务 authority |
| Attempt/Lease/fence/Visible Completion | PostgreSQL | durable | exactly-once visible result |
| ExecutionGraphRevision/ProfileCertification | PostgreSQL/catalog | immutable | 可审计部署合同 |
| Stage terminal result/transfer digest | PostgreSQL 或 evidence store + digest | durable summary | 恢复与审计需要 |
| Worker desired topology | Fleet Controller/Kubernetes spec + Vela revision | durable desired state | actuator contract |
| Worker readiness/capacity | Vela observation with TTL | bounded durable | Assignment recheck |
| KV block membership/prefix index | ServingStatePlane | ephemeral + snapshot | 高频、可重建 |
| Engine queue/token progress | engine/runtime | ephemeral | engine-local execution detail |
| Scheduler/Planner trace | evidence store | TTL/partitioned | 调试与 replay，不是 Job authority |

### 8.3 不要强行统一异步和在线 API

建议保留两个外部产品面：

- `Async Job API`：现有 Vela contract，适合视频、离线生成、长任务和 durable result。
- `Online Inference API`：未来独立 aggregate，适合 OpenAI-compatible streaming、连接级
  cancellation、router queue 和 token metrics。

两者共享 Organization、Project、Principal、ModelCatalog、ProfileCertification、容量和
账务基础设施，但 Job 状态机、SLO denominator、取消和计费边界不能复用同一套隐式语义。

## 9. 建议实现顺序

### Phase 0：先冻结合同，不改请求路径

1. 写 ADR：`Durable control plane vs serving state plane`。
2. 写 ADR：`ExecutionGraphRevision and stage authority`。
3. 为 scheduler claim 定义 bounded reason taxonomy 和 `DecisionEvidenceV1`。
4. 固定 workload trace schema：arrival、cohort、predicted/actual runtime、queue wait、worker
   state、decision、outcome；不得包含 prompt/artifact content。

完成标准：同一 PostgreSQL snapshot 的 scheduler replay 产生同一 decision digest。

### Phase 1：现有 H3 上做 shadow scheduling

1. 不改变 SQL winner，旁路运行 `Filter -> Score -> Pick` shadow evaluator。
2. 对比 current winner、shadow winner、ETA error、queue p95、tenant service share、retry rate。
3. 加入 stale signal、worker churn、长短 Job 混合、Protected Lane 和 retry lane 场景。
4. 只有 shadow 连续窗口无 authority divergence，才允许某个 scorer 进入实际路径。

完成标准：任何 scorer 失效只降低优化，不破坏 Assignment；Coordinator recheck 能拒绝
stale winner。

### Phase 2：CapacityPlanner advisory

1. 先只支持 Aggregated H3 profile，输入 certified runtime + arrival forecast + ready capacity。
2. 输出 min/desired/max、cooldown、hysteresis、budget、reason 和 expiry。
3. 与当前人工容量和实际完成数据做离线 replay。
4. 再接 FleetActuator，但默认 `auto_apply=false`。

完成标准：覆盖 step load、burst、missing metrics、cold start、failed scale-up、partial ready
和 capacity oscillation；必须报告 SLO、cost、waste、actuation lag，不能只报告 replica count。

### Phase 3：Execution graph 最小骨架

1. 先用单 stage `AGGREGATED` 表达现有 H3，不改变实际执行。
2. 引入 stage identity、parent Attempt fence、stage terminal evidence。
3. 证明旧 Worker/旧 stage Lease 不能在 graph 推进后提交结果。
4. 扩展 cancellation：Job cancel -> Attempt Coordinator -> 所有 active stage -> engine；
   分别记录 signal accepted、engine stopped、Lease fenced。

完成标准：现有 H3 行为不变，数据库 migration 可回滚，Job/Attempt/Charge invariant 全通过。

### Phase 4：未来 LLM P/D 实验 profile

1. 选择单一 engine/connector revision，不同时支持多 backend。
2. 建立 Prefill/Decode role pools 和 required/preferred topology。
3. `TransferTicket` 带 deadline、source/destination epoch、connector revision、integrity digest。
4. 故障注入：Prefill crash、transfer timeout、Decode crash、重复 completion、stale ticket、
   drain、network partition。

完成标准：与 aggregated baseline 比较 TTFT、TPOT、throughput/GPU、p95/p99、transfer GB/s、
GPU utilization、失败恢复和总成本；没有显著且稳定收益则保持 aggregated。

### Phase 5：KV-aware routing

1. 先 approximate/estimated prefix affinity，再上 precise KV events。
2. index 必须支持 event sequence、dedup、source restart、snapshot、full rebuild、lag metric。
3. stale index 降级到 load-aware，不允许 stale cache hit 越过 availability/profile constraints。
4. Active-Active router 必须说明 index 是共享、复制还是分片；不能假装 per-replica state
   等同全局一致。

完成标准：报告真实 hit ratio、recomputed prefill tokens、router CPU/memory、index lag、
错误命中和 tail latency；不得只报告平均 TTFT。

## 10. 测试与观测清单

### 10.1 Scheduler / fairness

- tenant 数、Project 数、Service Class、long/short Job、retry 混合矩阵。
- weighted share、attained calibrated GPU-seconds、max starvation、Protected Lane activation。
- stale readiness/load、Worker epoch change、claim expiry、Coordinator CAS reject。
- deterministic replay 与 decision evidence digest。

### 10.2 Planner

- prediction error：arrival、runtime、cold start、ready capacity。
- scale-up/down actuation latency、failed actuation、partial gang readiness。
- hysteresis、cooldown、min capacity、budget clamp、manual override。
- SLO breach、cost、idle GPU-hours、queue growth 同时报告。

### 10.3 Disaggregation / KV

- TTFT、TPOT、end-to-end latency、goodput/GPU、transfer latency/bandwidth。
- Prefill/Decode 独立 queue、KV utilization、cache hit、eviction、index lag。
- topology required 无同域 capacity、preferred fallback、transport unhealthy。
- event drop/duplicate/reorder、index rebuild、router restart、source epoch rollover。

### 10.4 Failure semantics

- cancellation signal accepted 不等于 engine stopped。
- migration 后旧 stream/old Lease completion 必须被 fence。
- Transfer 成功但 Decode 未开始；Decode 开始但 frontend 断开；stage terminal 但 Job 未终态。
- PostgreSQL failover、JetStream unavailable、Kubernetes API unavailable 互相独立注入。

## 11. 关键未知

1. **产品边界未知：** Vela 是否真的要提供同步 streaming LLM API，还是只管理异步 LLM
   Job。两者会决定是否需要 GAIE/EPP 类入口。
2. **H3 stage ROI 未知：** Encoder/DiT/VAE 的阶段耗时、tensor/latent 大小、PCIe traffic、
   可重叠程度和故障恢复收益仍需实测；当前不能据 LLM P/D 经验推断。
3. **LLM workload contract 未知：** 目标模型、context/output 分布、并发、SLO、GPU/NIC、
   engine revision 和 connector 都未冻结。
4. **KV state scale 未知：** block event rate、index working set、snapshot time、最大可接受
   lag 和 Active-Active consistency 目标未定义。
5. **公平性单位未知：** 视频与 LLM 共享集群时，GPU-seconds、cost units、memory-seconds
   或 profile-specific normalized work 哪个是可销售且可校准的共同单位。
6. **Planner authority 未知：** 谁批准 proposal、谁可暂停自动扩缩容、manual override 的
   TTL/审计/回滚如何定义。
7. **生产成熟度未知：** Vela 仍为 `0/9 PASS`，任何新 serving feature 都不能替代现有
   launch gate 的关闭。

## 12. 最终建议清单

短期只做三件事：

1. 在现有 scheduler 上增加 deterministic decision evidence 和 shadow scorer。
2. 建立 advisory-only CapacityPlanner proposal/actuator contract。
3. 用单 stage graph 表达现有 H3，先验证 stage authority/fencing，不拆 Worker。

中期再做两件事：

1. 为未来 LLM 选一个固定 engine + connector 做 P/D profile，先 benchmark 后产品化。
2. 建独立 KV state plane，从 approximate routing 演进到 precise event index。

明确不做：不替换 PostgreSQL authority，不把 Accepted Job 放进内存 queue，不把 KV block
event 混入业务 outbox，不因上游 benchmark 宣称就拆 H3，不让 connection cancellation
绕过 Vela 的 Job/Charge/Visible Completion 语义。

## 13. 来源键速查

- `[D01..D20]`：NVIDIA Dynamo 官方仓库文档与源码。
- `[L01..L12]`：llm-d 官方仓库架构与 guide。
- `[R01..R12]`：llm-d Router 官方源码与设计。
- `[K01..K04]`：llm-d KV Cache Manager 官方源码与设计。
- `[A01..A06]`：GAIE、KServe、vLLM、Ray Serve、KEDA 第一方材料。
- `[V01..V08]`：Vela `bc590e2` 当前代码与文档证据。

完整 URL、commit、path、置信度与用途见来源 JSON。来源 ID 是本指南的稳定 RAG 键，
并不表示来源间存在相同质量；具体判断仍应回到 `confidenceReason` 和固定 permalink。
