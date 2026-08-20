# Vela Domain Language

Vela 面向 Customer Organization 提供正式生产级 AI 生成服务。首发客户范围受控，但适用完整的生产可靠性、数据保护和审计要求。

## Commercial

**Customer Organization**:
与服务提供方建立商业关系并获准使用 Vela 的公司，是合同、额度和结算关系的主体。
_Avoid_: Tenant, client, company, account

**Launch Customer**:
通过邀请和线下商业关系接入 Vela 首批正式生产服务的 Customer Organization；其流量和数据不是测试流量或测试数据。
_Avoid_: Beta user, test tenant, MVP customer

**Contract Credit Limit**:
Customer Organization 在月度结算前允许累积的最大未结金额，由线下商业合同授予。
_Avoid_: Wallet balance, prepaid balance, payment hold

**Credit Reservation**:
Admission 时从 Contract Credit Limit 中为一个 Accepted Job 占用的额度，在 Job 形成 Charge 或无费用终止时解除，不依赖外部支付授权或续期。
_Avoid_: Card authorization, payment preauthorization, balance reservation

**Charge**:
Vela 对 Visible Completion 或 Billable Start 后 Customer Cancellation 确认的不可变应收记录，进入 Customer Organization 的月度结算范围。
_Avoid_: Capture, payment transaction, invoice

**OutputSpec**:
客户为 Job 选择的已认证离散输出规格，包含定价和结果验证所需的分辨率、帧率、时长与结果数量。
_Avoid_: Arbitrary generation parameters, Artifact metadata, ExecutionProfileRevision

**RateCardRevision**:
把 Model、GenerationPresetRevision、ServiceClassRevision 和 OutputSpec 的有效组合映射为固定单价的不可变价格表版本。
_Avoid_: PricingSnapshot, billing formula, discount code

**PricingSnapshot**:
Admission 时为一个 Job 固定的报价事实，记录命中的 RateCardRevision line、数量、币种和最终金额。
_Avoid_: RateCardRevision, estimate, Invoice

**Invoice**:
外部财务流程按结算周期向 Customer Organization 出具的付款请求；它汇总 Charge，但不属于 Vela 的 Job 生命周期。
_Avoid_: Charge, receipt

**Billable Start**:
Job 首次进入 RUNNING 的时刻；Customer Organization 在此之前取消不产生 Charge，在此之后主动取消产生完整报价的 Charge。
_Avoid_: Assignment time, dispatch time, first GPU second

**Customer Cancellation**:
Customer Organization 授权主体主动终止 Job 的请求，其计费结果由是否已达到 Billable Start 决定。
_Avoid_: Platform failure, operator abort

**Canceling Job**:
Billable Start 后 Customer Cancellation 已赢得持久竞争、Charge 已形成且执行权已 fenced，但 Worker 停止尚未确认的 Job。
_Avoid_: Canceled Job, failed Job, cancellation request

**Project**:
Customer Organization 内隔离 API credential、运行额度、Artifact namespace 和审计记录的操作空间；一个 Customer Organization 可以拥有多个 Project。
_Avoid_: Tenant, workspace, account

## Identity And Access

**Human Principal**:
通过外部 OIDC 身份登录 Vela，并以明确角色代表 Customer Organization 或 Project 行事的人类主体。
_Avoid_: User account, local user, API user

**Service Principal**:
隶属于一个 Project、代表程序调用 Vela API 的非人类主体；每个 Job 都归因到一个 Service Principal。
_Avoid_: API key, bot user, shared account

**Credential**:
Principal 用于证明身份的可轮换秘密或外部身份绑定；credential 可以过期或吊销，但不改变 Principal 的身份和审计历史。
_Avoid_: Principal, account, permanent key

**OrganizationOwner**:
管理 Customer Organization 成员、Project 和组织策略的 Human Principal。
_Avoid_: Super admin, tenant admin

**BillingAdmin**:
查看组织信用使用、Charge 与 Invoice reference，并维护结算联系人，但不能修改 Contract Credit Limit，默认也无权读取 prompt 和 Artifact 的 Human Principal。
_Avoid_: OrganizationOwner, finance viewer

**OrganizationAuditor**:
只读查看组织审计和用量记录、默认无权读取生成内容的 Human Principal。
_Avoid_: BillingAdmin, ProjectViewer

**ProjectAdmin**:
管理一个 Project 的成员、Service Principal 和 Credential 的 Human Principal。
_Avoid_: OrganizationOwner, project owner

**Developer**:
可以在一个 Project 中提交、查询和取消 Job，并读取其 Artifact 的 Human Principal。
_Avoid_: ProjectAdmin, API user

**ProjectViewer**:
可以读取一个 Project 的 Job 和 Artifact、但不能提交或取消 Job 的 Human Principal。
_Avoid_: OrganizationAuditor, Developer

**Break-glass Access**:
Platform Operator 在限时审批和完整审计下获得的例外客户数据访问，不属于任何客户角色。
_Avoid_: Support login, admin override, impersonation

**Organization Isolation**:
阻止一个 Customer Organization 的 Principal、Job、Artifact、Charge 和审计数据被另一个组织读取或修改的强制安全边界，不表示默认独占物理基础设施。
_Avoid_: Project isolation, dedicated deployment, tenant filter

**Dedicated Deployment**:
因合同要求为一个 Customer Organization 提供独立数据库、对象存储或密钥边界的交付档位。
_Avoid_: Organization Isolation, default tenant

**Production Gate**:
正式生产流量开放前必须以版本化证据通过的质量、可靠性、隔离、恢复和运维条件，不能由口头豁免改写结果。
_Avoid_: Checklist, future work, launch recommendation

**Launch Receipt**:
绑定 release digest、配置 revision、验证环境和结果的不可变 Production Gate 证据。
_Avoid_: Test log, dashboard screenshot, verbal approval

**Control/Storage Node**:
独立于 GPU Worker、共同承载 Vela 控制面和持久状态组件的自托管节点；首发以三节点复制提供基本单节点容错。
_Avoid_: GPU Worker, managed service, dedicated customer deployment

**Customer Content**:
Customer Organization 提交的 prompt、输入素材，以及 Vela 由其产生的 Artifact、中间文件和 debug dump；默认不得用于训练、benchmark 或人工质量分析。
_Avoid_: Telemetry, public data, platform dataset

**Retention Policy**:
规定一类 Customer Content、运行元数据或财务记录在终态后保留和删除期限的版本化规则。
_Avoid_: Object-store lifecycle, cache TTL, signed URL expiry

**Content Deletion**:
Customer Organization 要求在 Retention Policy 到期前删除 prompt 或 Artifact 的操作；它不撤销 Charge，也不删除必须保留的非内容审计记录。
_Avoid_: Job cancellation, refund, account deletion

## Execution And Delivery

**ArtifactSet**:
一个 Job 承诺交付的完整且不可分割的结果集合；缺少任一必需 Artifact 时不能作为成功结果交付。
_Avoid_: Output files, partial result, upload batch

**Local Recovery State**:
仅在原 Worker 节点仍可访问时用于进程级恢复的阶段数据，不承诺跨 Worker 或节点丢失后的恢复。
_Avoid_: Durable checkpoint, Artifact, shared cache

**Durable Checkpoint**:
可由另一个兼容 Worker 验证并恢复执行的持久中间结果；首发不提供。
_Avoid_: Local Recovery State, incomplete upload, debug dump

**Visible Completion**:
Job 成功、获胜 ArtifactSet、Charge 和 Artifact 访问资格共同生效的单一业务结果。
_Avoid_: Upload complete, inference finished, eventual success

**Preset SLO**:
GenerationPresetRevision 在约定统计窗口内对端到端完成时间分位数和成功率作出的服务目标，不保证任一 Job 的完成时刻。
_Avoid_: Hard deadline, ETA, runtime estimate

**Dynamic ETA**:
Vela 根据当前队列和 Worker 状态计算的非承诺完成时间预测，会随系统状态变化。
_Avoid_: Preset SLO, deadline, reservation

**Execution Phase**:
客户可见的 QUEUED、PREPARING、GENERATING、FINALIZING 或 RETRY_WAIT 阶段，不暴露 Inference Backend 内部拓扑。
_Avoid_: Job state, backend stage, GPU role

**Phase Progress**:
当前 Attempt 在当前 Execution Phase 中的可选观测值，允许在重试后重置且不构成完成承诺。
_Avoid_: Global progress, Preset SLO, completion percentage

**Hard Deadline**:
由持久 CapacityReservation 支撑、对单个 Job 作出的最晚完成时间承诺。
_Avoid_: Preset SLO, Dynamic ETA, timeout

**Job Expiry**:
Accepted Job 不得继续排队、执行、重试或 finalization 的系统生命周期上限；它不承诺 Job 会在该时刻前成功。
_Avoid_: Hard Deadline, Preset SLO, Dynamic ETA

**Retry Budget**:
ExecutionPolicySnapshot 为一个 Job 固定的计算 Attempt 数、累计计算时间和 finalization 恢复时间上限，并受 Job Expiry 约束。
_Avoid_: Retry count, customer quota, Job Expiry

**Capacity Share**:
Customer Organization 或 Project 在一个 Worker pool 中获得调度机会的合同权重与并发上限，不是对具体 Worker 的预留。
_Avoid_: CapacityReservation, priority, quota balance

**Work-conserving Capacity**:
所有 READY 且兼容的 Worker 都可执行普通 Job，不为故障恢复保留硬空闲设备。
_Avoid_: CapacityReservation, idle spare, overcommit

**Soft Failure Reserve**:
通过风险修正 admission、故障后收紧接纳和有界 retry lane 保护恢复能力的策略，不代表立即可用的空闲 Worker。
_Avoid_: Idle Worker, Hard Deadline, CapacityReservation

**Protected Lane**:
为等待超过保护阈值的 Job 提供的有界调度通道，防止长 Job 被持续到来的短 Job 饿死。
_Avoid_: Priority queue, Retry lane, CapacityReservation

**Remediation Operation**:
针对确切节点、设备身份和 Worker epoch 执行的一次幂等故障恢复意图，具有认证前置条件、动作级别和审计结果。
_Avoid_: Shell command, maintenance task, retry

**Quarantine**:
Worker 因身份、健康或恢复结果无法证明安全而被禁止接收 Assignment 的生命周期状态。
_Avoid_: Offline, draining, failed Job

**Admission**:
Vela 原子确认请求满足身份、规格、信用和容量约束，并创建 Accepted Job 的决定。
_Avoid_: Validation, enqueue, scheduling

**Accepted Job**:
已完成 Admission 并由 `202 Accepted` 确认的持久 Job；普通队列拥塞不能再将其拒绝。
_Avoid_: Request, rejected submission, tentative Job

**Capacity Rejection**:
Admission 前因 Project 限额或可用容量不足而返回的暂时拒绝，不创建 Job 或 Credit Reservation。
_Avoid_: Rejected Job, failed Job, queue timeout

**GenerationPresetRevision**:
面向客户的版本化生成质量与速度承诺，定义结果质量阈值和生成方式，但不定义排队优先级。
_Avoid_: ServiceClassRevision, ExecutionProfileRevision, pricing tier

**ServiceClassRevision**:
面向客户的版本化队列服务承诺，定义 admission、并发、调度权重和 Preset SLO，但不改变生成质量。
_Avoid_: GenerationPresetRevision, priority flag, ExecutionProfileRevision

**ExecutionProfileRevision**:
Vela 用于满足 GenerationPresetRevision 的内部版本化执行方法、资源拓扑和加速配置，不直接作为客户购买项。
_Avoid_: GenerationPresetRevision, ServiceClassRevision, Worker type

**Failed Job**:
无法形成 Visible Completion 且不再重试的终态 Job；除 Billable Start 后的 Customer Cancellation 外，不产生 Charge。
_Avoid_: Capacity Rejection, Customer Cancellation, unsuccessful Attempt

**Webhook Subscription**:
Project 为接收 Job 终态通知登记的外部 endpoint 和事件范围；通知不构成 Job 状态的事实源。
_Avoid_: Callback URL, event consumer, status stream

**Webhook Delivery**:
Vela 对一个领域事件向 Webhook Subscription 发起的一次可重试投递，允许重复且不改变 Job、Charge 或 ArtifactSet。
_Avoid_: Domain event, Visible Completion, exactly-once callback
