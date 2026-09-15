# Vela 开发与运维交接说明

> 适用仓库：/Users/viv/projs/vela
> 编写日期：2026-09-15
> 当前检查分支：feature/vela-mock-hardening
> 当前 HEAD：92199f2 (docs: document application publishing platform)

这份文档给第一次接手 Vela 的开发或平台同事使用。它描述当前仓库和现场的真实边界，不把“代码已经存在”“测试通过”“Pod Running”或“一次 smoke 成功”写成生产就绪。若本文件与代码、配置或最新证据冲突，以当前代码和最新可复核证据为准，并应在同一个变更中修正文档。

## 先读这几份文件

按以下顺序阅读，通常可以在半小时内建立正确的心智模型：

1. [CONTEXT.md](../CONTEXT.md)：领域词汇和禁止混用的术语。
2. [docs/architecture.md](architecture.md)：商业、身份、留存、灾备和 Visible Completion 基线。
3. [docs/h3-stage-execution-architecture.md](h3-stage-execution-architecture.md)：当前 H3 stage-disaggregated 执行架构，覆盖旧 H3 执行假设。
4. [docs/implementation-status.md](implementation-status.md)：实现状态和历史证据索引；它是证据索引，不是上线声明。
5. [docs/cluster-production-readiness-2026-09-14.md](cluster-production-readiness-2026-09-14.md)：当前现场收口清单和剩余工作，优先于旧的状态摘要。
6. 部署契约：deploy/vela-control/README.md、deploy/control-storage/README.md、deploy/fleet-controller/README.md、deploy/stage-worker/README.md。
7. 运维手册目录：[docs/runbooks/](runbooks/)。

## 接手后的第一个小时

先保存现场，不要马上整理、回滚或提交：

    cd /Users/viv/projs/vela
    git status --short --branch
    git log --oneline --decorate -15
    git diff --stat
    git diff --check

当前工作树有大量未提交的 Go、SQL、Proto、Kustomize、脚本和证据文件。它们可能来自不同验证批次，不能整体当作一个已经审核的 release。接手时应先把当前状态复制到交接记录，再按功能范围划分；不要执行 git reset --hard、批量清理未跟踪文件或删除 docs/evidence。

快速确认代码可读和生成链路：

    go version                         # 仓库要求 Go 1.26.x，toolchain 为 1.26.7
    make generate
    make verify-generated
    go test ./internal/executiongraph ./internal/admission ./internal/stageworkeragent

若只是了解系统，不需要先跑完整集成测试。若要验证完整本地闭环，再按“开发验证”一节执行；集成测试会启动 PostgreSQL 17 等 Testcontainers，可能需要 Docker 和较长时间。

## 当前状态和不可忽略的边界

### Git 和代码状态

- 当前分支是 feature/vela-mock-hardening，不是 main。
- 工作树有大量修改和未跟踪文件，包含 runtime startup、trace、release、observability、Fleet 和 worker 相关增量。
- 当前 HEAD 只代表已提交历史；未提交文件不能被默认为已 review、已部署或可回滚。
- 生成文件必须由源文件重新生成：api/openapi/vela.yaml、proto/、db/queries/ 是源；api/gen/、proto/gen/、internal/store/sqlc/ 是生成结果。

### 现场生产边界

截至 2026-09-15 的最新现场记录：

- 集群约 54 个注册节点、53 个 Ready、407 个 GPU；.19 仍不可达并 cordon，.66 有 DiskPressure。
- PostgreSQL、NATS、APISIX/etcd 的部分成员仍是 2/3；MinIO 现场记录为 5/6，不能按完整 HA 宣称。
- Control 已有两副本运行，NATS/Control 的一批不可变材料和 schema-96 基线已采用；生产数据库现场仍以 schema 96 为边界。
- Fleet Controller 和 Stage Worker 尚未完成生产部署；当前仓库中的候选清单和准备记录不等于 live deployment。
- 完整 canonical release bundle、所有组件的 OCI descriptor/config、Host package、批准的 Model/ResidencyPlan、完整 PKI 和 Launch Receipt 仍未齐全。
- 追踪相关代码和 00097/00098 迁移已经出现在当前工作树，但生产尚未采用；带 trace 的 Worker journal schema 6 需要先完成读写者升级和采用验证。
- 架构文档中的 Production Gate 基线仍未闭合；没有完整 Launch Receipt 前，不得承载正式客户流量。

现场状态以 docs/cluster-production-readiness-2026-09-14.md 和其链接的 docs/evidence/*.json 为准。现场证据里的地址、证书路径和凭据引用都属于环境输入，不能复制进代码或聊天记录。

## Vela 是什么

Vela 是异步 AI 推理控制面。它接收可能运行数分钟到数十分钟的 Job，完成身份和项目隔离、Admission、额度预留、执行图调度、Worker/Fleet 生命周期、Artifact 发布、取消/重试、计费和审计。模型推理本身由 SGLang fork、vLLM 或模型专用 backend 完成；Vela 不实现 backend 内部的 tensor parallel、pipeline parallel 或 token loop。

系统最重要的可见性语义是：

> At-least-once execution, exactly-once visible completion.

物理计算可以重复，只有一个 Attempt 能原子地形成 Visible Completion。Job 成功、获胜 ArtifactSet、Charge 和 Artifact 访问资格必须在同一个权威状态转移中成立。

### 当前 H3 执行拓扑

    8-GPU node

    GPU-0  AUX Worker
           ├─ long-lived Encoder process
           └─ long-lived VAE Decoder process
              （共享一个 active StageLease）

    GPU-1..GPU-7  七个独立的 single-GPU DiT WorkerInstance

Encoder、DiT、VAE 和 CPU media 是可独立调度的 CapacityPool。Encoder 输出和最终 DiT latent 在下游执行前必须成为 durable StageArtifact；直接传输只能作为优化，不能替代 durable copy。Exact cache 默认 Project-scoped、按精确输入和版本绑定，禁止跨 Organization 的 Customer Content 复用。

模型权重本地化是部署硬约束，不是性能优化。内网带宽有限，模型通常很大；Worker/ModelRuntime 启动、重启、扩容和重新调度时都不能从 Hugging Face、对象存储或其他网络地址临时下载权重。

- Runtime image 只负责代码、driver 和启动入口；权重必须在目标 GPU worker 的本地模型盘上提前准备。当前模型盘计划使用 XFS 和 `/srv/vela/models/weights/`，但现场已有文档也出现 `/srv/models`，实际采用路径必须在 release-bound ResidencyPlan、launch manifest 和挂载检查中统一，不能靠默认路径猜测。
- 每个 ModelResidency 必须绑定明确的 model revision、权重目录、内容 digest/manifest、文件权限和本地盘容量；校验不通过时 Worker 不得 Ready，也不得把权重写入系统盘、容器 writable layer 或临时目录。
- 严禁在 backend 中保留隐式网络 fallback，例如 `from_pretrained` 自动下载、启动脚本中的 `wget`/`curl`、运行时 registry/Hugging Face 拉取或“缺文件就下载”。缺权重应 fail closed，进入预热失败、drain 或 Fleet remediation。
- 准备流程应是“受控传输一次 -> 本地临时目录校验 -> 原子 rename 到 revision 目录 -> 重新计算 manifest/digest -> warmup -> 写入 readiness/evidence”。不要在正在服务的 revision 目录中覆盖文件。
- 本地模型盘没有自动复制副本。原始权重制品、版本清单和校验值必须另存于受控 artifact/备份位置；本地盘丢失时从原始制品重新布置，而不是依赖跨节点实时下载或把另一台 Worker 当主副本。
- 扩容和升级按节点逐台滚动：先确认磁盘挂载、可用容量、inode、读权限和 checksum，再拉入新 ModelResidency；旧 revision 在 drain 和证据留存完成前不能删除。

模型盘计划见 [docs/worker-model-storage-plan-2026-09-14.md](worker-model-storage-plan-2026-09-14.md)。该计划记录的 14 块 NVMe 尚未格式化或挂载，且没有授权清空潜在旧数据；“已准备初始化脚本”不等于模型盘已可用。

## 组件和职责

| 组件 | 代码入口 | 主要职责 | 权威边界 |
| --- | --- | --- | --- |
| vela-control | cmd/vela-control | REST API、Admission、Job/Attempt、计费、Outbox、Fleet maintenance、管理和指标端口 | PostgreSQL command transaction；不能由 HTTP/NATS handler 直接改领域表 |
| Attempt/Stage Coordinator | internal/attemptcoordinator、internal/executiongraph | ExecutionGraph、StageRun、StageAttempt、Lease、retry、materialization 和 finalization | PostgreSQL 是事实源；JetStream 只负责唤醒 |
| Stage Scheduler | internal/stagescheduler 等 scheduler 代码 | Filter -> Fairness -> Score -> Pick，重新校验后 claim | 只通过带 expected version/fence 的 command 修改状态 |
| Stage Worker Agent | cmd/vela-stage-worker-agent、internal/stageworkeragent | 接收 Assignment、heartbeat、执行 Stage、seal/materialize、scratch journal | StageAuthority、StageLease 和 durable journal；不能绕过 Control |
| Model Runtime | cmd/vela-model-runtime、internal/modelruntime | 长驻 backend driver、UDS command、execution floor、drain、inspection | Runtime epoch、signed floor 和进程生命周期 |
| Fleet Controller | cmd/vela-fleet-controller、internal/fleetcontroller | 将批准的 ResidencyPlan 变成 WorkerBundle/Pod/Service/Secret，提供 validating webhook | Kubernetes 是 actuator；Registry/Fleet authority 在 Vela/PostgreSQL |
| Node Agent | cmd/vela-node-agent、internal/nodeagent | host systemd 下的 GPU/PCI/驱动证据、fence、remediation、runtime startup | 主机特权边界；必须 fail closed，不能依赖待恢复的 GPU Pod |
| Release tools | cmd/vela-release-artifacts、cmd/vela-release-bundle、cmd/vela-verify-launch | 构建镜像/host package/runtime package、组装和校验 release bundle | digest、配置 revision、证据和资源清单必须绑定 |
| Lab/validation tools | cmd/vela-lab-*、cmd/vela-h3-*、hack/ | 本地 CPU mock、H3 mock、现场采集和定向验证 | 只能产生对应证据类别，不能自动晋级 Production Gate |

### 数据和消息层

- PostgreSQL：Job、Attempt、StageRun、Lease、Worker、Artifact、cache、billing、审计、迁移和所有 fencing 的事实源。
- NATS JetStream：Outbox、scheduler、Fleet、billing、webhook 和 reconciliation 的唤醒/传输；消息丢失后可从 PostgreSQL 重建，不是唯一业务状态。
- Object Store / S3 API：StageArtifact、客户 Artifact 和备份对象；版本、ETag、删除标记和 pin 由 Artifact 模块管理。
- Kubernetes：运行 Control/Fleet/Stage Worker/observability，并 actuation WorkerBundle；Pod readiness 不是 Vela readiness。
- Node systemd：Node Agent、PIDFD broker、runtime policy issuer 等主机级安全组件。

## 领域不变量

修改代码前先确认是否触及以下不变量：

1. Customer Organization、Project、Human Principal、Service Principal 和 Credential 不是同义词；API scope 和审计归属必须明确。
2. 202 Accepted 表示 Admission 已持久化承诺。普通拥塞不能把已 Accepted Job 重新拒绝。
3. CreditReservation、固定价 Charge 和 Usage/Cost Ledger 分离。平台重试增加内部成本，不能重复向客户收费。
4. Job、Attempt、StageRun、StageAttempt、StageLease 必须分层；阶段重试不得伪造新的端到端 Attempt。
5. WorkerInstance 独占 DeviceSet；epoch、runtime epoch、member identity 或 device subset 变化会 fencing 旧 Lease。
6. 调度顺序是 Filter -> Fairness -> Score -> Pick；cache affinity、locality、负载不能越过硬资格和租户公平。
7. Durable StageArtifact 先于下游 Stage；中间结果不能只存在本地 NVMe 或 NATS message。
8. Exact cache 需要精确输入版本、Model/StageProfile/Connector revision、Project scope、pin 和不可变 admission receipt。
9. Kubernetes、NATS、内存 queue、长期 unacked message 都不能成为 Job 或 Stage authority。
10. Node/Runtime/Worker journal 是执行历史和恢复边界；丢失、替换、sequence gap、身份漂移或 proof 不完整都应 fail closed。
11. Release 的镜像、ConfigMap、Secret、PKI、ResidencyPlan、渲染结果和 Launch Receipt 必须绑定同一个 release revision/digest。

## 代码导航和修改规则

### API、Proto、SQL

- OpenAPI 源：api/openapi/vela.yaml；生成：make generate-openapi。
- Protobuf 源：proto/vela/v1；生成：make generate-proto。
- SQL 查询源：db/queries；生成：make generate-sql。
- Schema migration：db/migrations，当前仓库最高为 00098；生产现场边界与本地分支不一定相同。
- 任何 API/Proto/SQL 修改必须同时检查兼容窗口、旧 consumer、generated diff、权限 role 和 migration 的 expand/backfill/switch/contract 顺序。

### Go module

go.mod 声明 Go 1.26.0，toolchain go1.26.7。版本化工具在 Makefile 顶部集中声明：oapi-codegen、sqlc、buf、protoc plugins 和 golangci-lint。不要用本机随意版本覆盖生成结果。

### 部署清单

deploy/* 多数是部署契约或 base，不是可直接上线的 production overlay。出现 .invalid endpoint、r0-placeholder Secret、全零 digest、placeholder PKI、template-not-approved 或文档地址时，必须停止发布并补齐 release 输入。每次改变镜像、配置、Secret、PKI、ResidencyPlan 或资源列表，都应重新生成 canonical release bundle。

## 开发验证

### 常规检查

    make generate
    make verify-generated
    make lint                       # go vet + golangci-lint
    make test                       # go test ./... + runtime startup Python checks
    make test-cross                 # linux/amd64 CGO_ENABLED=0 编译边界
    make validate-deployment        # Kustomize 和 deployment-contract tests
    make verify                     # 上述完整组合，耗时较长

make verify 只证明仓库层面的测试和契约，不证明现场节点、PKI、对象存储、CNI、真实模型、SLO 或 Launch Receipt。

### 集成和 CPU mock

集成测试通常需要 Docker：

    make test-integration
    # 或分片
    INTEGRATION_TEST_SHARD_INDEX=0 INTEGRATION_TEST_SHARD_TOTAL=4 make test-integration-shard

本地 CPU mock 只在明确设置变量时运行：

    VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
    go test -race -tags=integration ./internal/integration \
      -run '^TestCPUMockConcurrentAdmissionRuntimeCampaign$' -count=1 -timeout=3m -json

它验证真实 Admission、调度、authority、native mock runtime、transfer、materialization 和 Visible Completion 路径，但不是真实 GPU/H3 throughput、真实生产 stream handler、Linux sandbox 或远端 S3。扩展 512-Job 和 exact-cache 命令见 docs/runbooks/cpu-mock-load-campaign.md。

### 生成和 migration 调试

    go run ./cmd/vela-lab-bootstrap --help
    go run ./cmd/vela-lab-smoke --help
    go run ./cmd/vela-assignment-history-migrate --help

vela-lab-bootstrap 使用 Goose 应用 db/migrations。生产迁移前必须完成备份、兼容窗口、quorum/lock 预检和回滚边界；不要在现场直接编辑表或手工插入 authority 记录。涉及旧 Worker/Runtime journal 时，优先使用仓库提供的 recover/upgrade-* 路径；缺失历史 proof 时保留文件并进入人工 reconciliation，不能用 initialize 绕过。

## 发布和部署流程

### 预发布顺序

1. 固定 Git revision，检查工作树和 source digest。
2. 生成 API、Proto、SQL，并运行 make verify-generated。
3. 构建并验证 host/runtime/image artifacts：make build-host-packages、make build-runtime-startup-packages、make build-vela-image-artifacts。
4. 生成 release bundle：make build-release-bundle，随后 make verify-release-bundle。
5. 校验所有 deployment render：

       kubectl kustomize deploy/control-storage >/dev/null
       kubectl kustomize deploy/vela-control >/dev/null
       kubectl kustomize deploy/fleet-controller >/dev/null
       kubectl kustomize deploy/stage-worker >/dev/null
       kubectl kustomize deploy/observability >/dev/null

6. 将最终 render、镜像 descriptor/config、Secret/PKI 引用、ResidencyPlan、外部输入和 evidence 一起放入 canonical release bundle。
7. 先在 Argo 的审批/回滚流程中以候选 revision 验证，再按现场 runbook 采用；不要直接 apply placeholder base。
8. 部署后验证 Pod、Service、NetworkPolicy、TLS、数据库迁移、NATS stream/consumer、对象存储和 Launch Receipt；只要一个关键前置条件缺失，就保持不承载正式流量。

### 重要部署契约

- vela-control 默认 base 含无效镜像和 placeholder 材料；健康检查在 Pod-private management port 8081：/healthz、/readyz、/metrics。
- vela-control 对外 API、management、Fleet gRPC、Finance reconciliation、Compliance legal-hold 是不同接口，NetworkPolicy 不能把管理端口暴露给公共入口。
- control-storage 使用 CNPG PostgreSQL、三副本 NATS JetStream 和 MinIO/Object Store；备份凭据由独立 Secret Manager 提供，清单本身不含凭据。
- Fleet Controller 需要 kube-apiserver webhook client CA、serving cert、Fleet mTLS 以及 release-bound ResidencyPlan；base 本身不提供真实准入证明。
- Stage Worker 由 Fleet 按 WorkerMember Actuation 创建，deploy/stage-worker 没有静态 Deployment/DaemonSet；每个 Pod 只挂自己的控制身份和 Artifact 凭据。
- Node Agent 运行在 host systemd，必须使用实际 node source /32、mTLS 和已登记 Node identity；CNI 来源放行不等于应用授权。

## APISIX 网关边界和适配性

### 当前真实部署方式

Vela 当前已经把 APISIX 用作外部 HTTP/API 入口。`vela-api` 在 `vela-system` 中保持 `ClusterIP`，APISIX 通过 `/api/*` 路由转发到 `vela-api.vela-system.svc.cluster.local:80`，并把 `/api/...` 重写为后端的 `/...`。Grafana 也通过 `/grafana/*` 发布；Control 的管理端口和内部控制协议不经过公共网关。

现场平台使用两个 CPU 管理节点上的 APISIX gateway Pod，并使用三成员 etcd 保存网关配置；入口是 NodePort `30080`/`30443`。当前私有 CA、TLS、HTTP 到 HTTPS 的 `308` 跳转、APISIX Prometheus/OpenTelemetry 观测和 API 未认证拒绝都已有现场证据。证据和配置入口：

- 路由：[deploy/cluster-platform/vela-api-route.json](../deploy/cluster-platform/vela-api-route.json)
- 平台说明：[deploy/cluster-platform/README.md](../deploy/cluster-platform/README.md)
- Vela Service 边界：[deploy/vela-control/README.md](../deploy/vela-control/README.md)
- TLS/证书同步：[deploy/cluster-platform/gateway-tls/README.md](../deploy/cluster-platform/gateway-tls/README.md)
- 现场验收：[docs/gateway-validation-2026-09-14.md](gateway-validation-2026-09-14.md)

平台文档里的 “APISIX 3.18” 是服务版本，网关修复记录里的 “chart 2.17.0” 是 Helm chart 版本；发布清单必须分别记录并固定这两个版本，不能把它们混成一个版本号。
部署协议的硬约束是：**对外和跨进程承载 API、凭据、Customer Content 或控制命令的服务必须使用 HTTPS；已有内部服务使用 mTLS；证书链统一使用现有私有 CA。** 不得为了省事新增临时自签名 CA、关闭证书校验、使用 `curl -k`，或把 HTTPS 服务降级成明文 TCP。

- 公共 API 使用 APISIX HTTPS `30443`；HTTP `30080` 只用于受控跳转到 HTTPS `308`，不承载业务请求。客户端必须信任现有 gateway private CA，并校验 hostname/SAN。
- `vela-control` 的 Fleet、Finance、Compliance、Stage Worker control 等内部接口使用各自的 TLS/mTLS 材料和既有 CA；这些端口不应通过公共 APISIX route 发布。
- RKE2/containerd 拉取本地 registry 时使用内部 registry CA（节点上的 `/etc/rancher/rke2/registry-ca.crt`）和只读认证；不能把 registry 改成 HTTP 或跳过证书校验。
- cert-manager/APISIX gateway 的现有私有 CA 和证书同步机制属于部署材料的一部分。证书轮换要生成新版本、验证两端实际 served fingerprint，再滚动消费者；不能原地替换 Secret 后假定进程已经读取新证书。
- `/healthz`、`/readyz` 等仅限 Pod-private management listener 的明文探针是明确的本地例外：它们不得绑定公共 Service、不得被 APISIX 暴露，也不能承载客户请求或凭据。新建的跨 Pod 服务不能照搬这个例外。
- 每个新服务在发布前必须验证：TLS 版本、证书链、SAN/SNI、私有 CA 信任、过期时间、轮换路径、错误凭据拒绝和明文入口关闭/只跳转；证据写入 release bundle。


### Vela 是否适合引入 APISIX

适合，而且当前架构已经为这种边界留出了位置。Vela 的外部接口是异步 REST/JSON，需要 TLS termination、路径路由、基础限流、统一入口和 HTTP/OTLP 观测；这些正是 APISIX 的合适职责。现有 `vela-api` 保持 ClusterIP、默认拒绝入站、只允许带 `api-gateway` 身份的网关 Pod 访问，说明网络边界也按这个模型设计。

APISIX 的职责应保持在边缘层：

- TLS/SNI、HTTP 到 HTTPS redirect、公共路径路由、请求大小/超时/基础连接保护和网关指标。
- 将 `/api/*` 转到 `vela-api`，将 `/grafana/*` 转到 Grafana；每条 route 的 upstream、rewrite、插件和 release revision 都要可审计。
- 作为认证前的入口保护层，但不能代替 Vela 对 bearer credential、Organization、Project、Principal、RBAC、幂等 key、信用额度和 Job 状态的校验。当前未认证请求得到 `401`，真正的认证和授权仍由 Vela 完成。

以下边界不能交给 APISIX：

- 不要让 APISIX 直接访问 PostgreSQL、NATS、Object Store、Worker、ModelRuntime、Node Agent 或内部 mTLS 服务。
- 不要把 `vela-control` 的管理 `/healthz`、`/readyz`、`/metrics`、Fleet gRPC `8444`、Finance `8445`、Compliance `8446` 或 Stage Worker control `8447` 发布到公共路由。
- 不要把 APISIX 的限流结果当作租户配额或 Vela Service Class。当前 `limit-count` 是每个 APISIX Pod、按来源 IP 的本地 `120/60s`，不是全局限流，也不是按 Organization/Project 的公平配额；业务配额必须仍由 Vela/PostgreSQL 执行。
- 不要用 APISIX route 成功、Grafana 200 或未认证 401 代替认证成功的业务 Job、Stage、Artifact、SLO 或 Launch Receipt 验收。

### 采用时的硬检查

1. APISIX gateway 至少两个副本并跨管理节点；etcd 成员、PVC、备份、leader 和配置恢复需要单独验证。
2. Admin API 只能在集群内由受控 Job/Controller 调用；不要把 admin Service、admin key 或完整 SSL 对象暴露给业务入口。
3. Route、upstream、rewrite、plugin、TLS 证书和 NetworkPolicy 必须进入 canonical release bundle；不要把现场手工 Admin API 修改当成永久配置。
4. 对每个新 API 做两侧验证：APISIX 侧路由/TLS/超时/限流，Vela 侧 bearer authentication、scope、幂等、错误码和审计。
5. 保留 HTTPS 证书到期、证书同步失败、APISIX metrics 缺失、etcd/quorum、upstream 5xx、429 和 route drift 告警。
6. APISIX 只发布 API metadata 和请求，不承载模型权重。模型权重仍必须预置在 GPU Worker 本地模型盘，不能因为网关存在就把大文件或权重下载改成运行时网络路径。
- APISIX route、registry、Control 和内部 mTLS 服务都必须使用已有私有 CA；新服务不得提交明文 endpoint 或临时 CA 作为 production 默认。

结论：Vela 适合继续使用 APISIX 作为“外部 API edge gateway + Grafana publishing gateway”。它不适合作为 Vela 的业务 authority、内部服务网格、模型分发层或租户计费限流器。

## 镜像仓库和 Worker 拉取边界

生产部署的默认规则是：**先把所有要部署的 OCI 镜像复制并校验到本地镜像仓库，再让 Worker/节点优先从本地仓库拉取**。这条规则适用于 Control、Fleet、Stage Worker、ModelRuntime、Node Agent 相关镜像、NVIDIA device plugin、观测组件、init/sidecar、BusyBox/curl 等辅助镜像；不能只复制主应用镜像而遗漏 init 或 operator 镜像。它与模型权重本地化是两条不同的约束：镜像从本地 registry 拉取，权重从 Worker 本地模型盘读取。

- 当前 RKE2 节点配置在 `deploy/management-cluster/registries.yaml`：Docker Hub、NVCR、GHCR、Quay 和 `registry.k8s.io` 分别优先访问 `.70/.71/.66` 的 `:5000`–`:5004`；生产 release registry 使用 `:5005`。Worker 上的 `registry-ca.crt`、TLS 和认证必须先就绪。
- `:5000`–`:5004` 是上游缓存，`:5005` 是受认证和仓库前缀控制的 release registry。上游 fallback 只能作为受控 bootstrap/应急路径，不能把“公网 fallback 可用”当成“本地镜像已准备完成”。
- 每个 release 必须按 digest 复制 manifest、OCI index 和所有 blobs，逐 digest/SHA256 校验，并记录源、目标、平台架构、大小和复制结果。多架构镜像必须确认目标节点实际的 `linux/amd64` 或 `linux/arm64` manifest 存在，不能只复制一个不匹配的平台变体。
- Kubernetes 使用 namespace 级只读 `imagePullSecret` 和内部 CA；不要设置一个全局共享密码，也不要把 publisher 凭据放进 Worker。新 namespace、Fleet-created Worker Pod 和 operator Pod 都要检查自己的 Secret 引用。
- 部署清单尽量使用内部 registry 的 immutable digest，而不是公共 registry tag。改变镜像 digest、registry host、imagePullSecret、CA 或镜像清单时，都要产生新的 release revision 和 canonical bundle。
- 发布前先做“本地仓库存在性 -> 认证拉取 -> 节点实际 containerd 拉取 -> Pod 启动”的闭环验证；不要只在 registry API 上看到 manifest 就认为 Worker 能拉取。
- 本地三台 registry 没有自动同步能力。未来 release 必须显式执行复制和三端校验；当前 Control 镜像复制证据只证明该批 Control 镜像，不代表所有历史或未来镜像都已具备本地副本。
- 如果本地 registry 缺少镜像或证书/凭据失败，应在发布前 fail closed，修复复制、认证或 CA；不要让大量 Worker 在启动时同时访问公网，造成内网拥塞或产生不可重复的缓存状态。
- 镜像拉取、API 调用和内部控制 RPC 的证书验证失败时应 fail closed；不要用 `--insecure`、`-k`、明文 endpoint 或临时 CA 绕过。

相关入口和证据：

- 节点 mirror 配置：[deploy/management-cluster/registries.yaml](../deploy/management-cluster/registries.yaml)
- 管理集群说明：[deploy/management-cluster/README.md](../deploy/management-cluster/README.md)
- release registry 访问契约：[deploy/registry-access/README.md](../deploy/registry-access/README.md)
- 按 digest 复制工具：[hack/replicate-registry-image.py](../hack/replicate-registry-image.py)
- 最近认证和实际 Kubernetes 拉取验收：[docs/registry-access-validation-2026-09-15.md](registry-access-validation-2026-09-15.md)

## 日常运维入口

### 第一轮只读检查

    kubectl get nodes -o wide
    kubectl get pods -A
    kubectl get pvc -A
    kubectl -n vela-system get deploy,sts,svc
    kubectl -n vela-system get events --sort-by=.lastTimestamp | tail -80

    # Control
    kubectl -n vela-system port-forward svc/vela-control-management 18081:8081
    curl -fsS http://127.0.0.1:18081/healthz
    curl -fsS http://127.0.0.1:18081/readyz
    curl -fsS http://127.0.0.1:18081/metrics >/tmp/vela-control.metrics

实际 Service 名称以当前 overlay render 为准；先 kubectl get svc -n vela-system，不要根据旧截图猜端口。集群快照使用 hack/collect-cluster-readiness.py。

### 按故障域定位

| 症状 | 先看 | 再看 | 不要直接做 |
| --- | --- | --- | --- |
| API 401/403/5xx | APISIX route、Control /healthz、OIDC/credential、Control logs | trace、DB role、NetworkPolicy | 不要绕过 API 直接改 DB |
| /readyz 失败 | Control logs、PostgreSQL、NATS、Object Store、ConfigMap/Secret revision | docs/runbooks/observability-control-plane.md | 不要用重启代替依赖诊断 |
| Job 卡在 QUEUED | Job/StageRun 状态、容量 observation、pool certification、scheduler wakeup | NATS consumer/backlog、StageScheduler logs | 不要手动插入 assignment/lease |
| Stage 心跳停止 | Worker Pod、control mTLS、StageLease/fence、Runtime epoch | Worker journal、UDS、Node Agent evidence | 不要删除 journal 或 scratch 让它“重新初始化” |
| Artifact/materialization 失败 | object version/ETag、materialization journal、pin、S3/MinIO | docs/runbooks/stage-worker-scratch-retirement.md | 不要先删除本地输出或对象版本 |
| GPU/节点异常 | node condition、DiskPressure、nvidia-smi、PCI/driver、Node Agent receipt | Fleet WorkerInstance epoch、remediation operation | 不要在未 fencing 时重启或 reset GPU |
| 模型加载失败或启动变慢 | findmnt/挂载 UUID、df -h/inode、权重 manifest/digest、ModelResidency revision、runtime 日志 | 本地模型盘读错误、warmup receipt、是否错误触发网络 fallback | 不要让 Runtime 现场下载权重或把系统盘当模型盘 |
| Pod ImagePullBackOff | Pod 事件、imagePullSecret、节点 `/etc/rancher/rke2/registries.yaml`、registry CA、目标 digest | 三个本地 registry 的 manifest/blob、containerd 实际拉取和镜像架构 | 不要先放开公网拉取或改成 mutable tag |
| NATS 异常 | 三成员 Ready、stream/consumer、JetStream memory/disk quota、replica leader | docs/runbooks/observability-messaging.md、容量记录 | 不要清空 stream 代替 replay |
| PostgreSQL 异常 | CNPG primary/replicas、backup/WAL、migration ledger、连接 role | docs/runbooks/observability-database.md | 不要手工删除迁移记录 |
| 磁盘压力 | .66 根卷、Longhorn、MinIO/PVC、临时目录清单 | node66-* 现场记录、storage runbooks | 不要按 glob 批量删除别人的目录 |

模型盘日常检查至少应确认：findmnt /srv/vela/models（或 release 明确的实际路径）、挂载 UUID 与清单一致、剩余空间/inode 足够、权重目录只读可读、manifest digest 匹配当前 ModelResidency，并且节点网络流量没有出现启动期的大规模权重传输。

### 现有运维手册

- 控制面、数据库、消息、对象存储、日志、追踪、GPU、节点和留存：docs/runbooks/。
- CPU mock campaign：docs/runbooks/cpu-mock-load-campaign.md。
- Stage scratch 和 journal：docs/runbooks/stage-worker-scratch-retirement.md。
- 统计 SLO：docs/runbooks/statistical-slo-breach.md。
- 平台发布和 Argo：docs/platform-publishing/。

## 安全、恢复和不可逆操作

- 不要把 Secret、私钥、数据库 URL、NATS credentials、OIDC client secret、Artifact key 或现场主机凭据写入 Git、issue、聊天或证据 JSON。
- PostgreSQL 是授权边界的一部分；迁移、role grant、SECURITY DEFINER 函数和 RLS 变更必须做最小权限审查。
- Worker/Runtime journal、launch manifest、binding、verifier keyring、startup ledger 和 PIDFD broker 是安全边界，不是普通缓存。
- 任何 fence、GPU reset、driver reload、reboot、BMC power cycle、PVC 删除、MinIO 旧卷删除、Longhorn replica 移动和数据库 contract migration 都必须先做当前 inventory、命名影响对象、记录 preflight，并在动作后验证 postcondition。
- 模型盘初始化、格式化、挂载切换和权重删除同样是受控操作；必须按节点、设备序列号和批准清单执行，先保留原始权重制品和 manifest，再做变更。
- 生产恢复优先使用 CNPG PITR、NATS durable replay、MinIO versioned object 和 Vela reconciliation；不要直接删除状态让 controller 自己猜。
- 迁移采用 expand -> backfill -> switch -> contract。旧 binary、旧 event backlog、旧 Stage authority 和旧 journal 未清空前，不要做 contract/不可逆收缩。

## 当前优先级和建议接手顺序

### P0：先恢复可发布边界

1. 重新确认 .19 不可达、.66 DiskPressure、Longhorn/MinIO/PostgreSQL/NATS 当前成员和容量。
2. 完成所有组件的 canonical release bundle：固定 digest、ConfigMap/Secret/PKI、Registry 本地复制和三端校验、Host package、ResidencyPlan、渲染结果和 evidence。
3. 明确生产数据库 schema 96 与本地 97/98 候选迁移的差异；在独立参考库跑迁移、权限和回滚/兼容检查，未经采用不要把本地代码当现场代码。
4. 只有在 bundle 和现场前置条件完整后，才继续 Fleet Controller、Stage Worker、真实模型和 Launch Receipt 验证。

### P1：完成真实执行闭环

- 部署并验证 Fleet webhook、WorkerBundle、Stage Worker、ModelRuntime 和 Node Agent 的真实 mTLS/identity registration。
- 用批准的 H3 model/runtime image 做同节点、跨节点、节点丢失、Stage retry、materialization replay、exact cache 和 finalization 验收。
- 形成真实业务 API、SLO cohort、错误预算、备份恢复和故障演练证据。

### P2：采用追踪和新 runtime startup 增量

- 先升级所有 journal/reader 到兼容 schema，再采用 00097/00098 和 trace header/Outbox/NATS/Stage lineage。
- 重新跑 unit、race、integration、deployment contract 和现场 Collector/Tempo 验证。
- 把“代码里存在 trace”与“生产 trace 可查询、可采样、可留存、不会泄露 Customer Content”分开验收。

## 推荐的日常开发流程

    1. 记录 git status、分支、HEAD、现场 evidence 和目标 claim
    2. 先改 domain contract / OpenAPI / Proto / SQL source
    3. 运行 make generate 与 verify-generated
    4. 为状态机、fence、权限和恢复路径补 focused tests
    5. 运行 gofmt、go vet、相关 package test、race 或 integration
    6. 运行 deployment-contract / kustomize 检查
    7. 更新 evidence-bound 文档，明确事实、推导、假设和未知
    8. 只 stage 请求范围；检查 git diff --cached 和 git status
    9. 需要发布时再单独组装 release bundle；不要把现场临时证据顺手混入代码提交

提交前尤其检查：生成文件是否同步、migration 是否有 Down/兼容边界、错误路径是否保留 journal/scratch、日志是否泄露 prompt/Artifact/secret、测试是否真的覆盖 authoritative adapter 而不是只测 mock。

## 词汇速查

| 词 | 含义 |
| --- | --- |
| Job | 客户可见的一次异步请求和固定价格边界 |
| Attempt | 一个端到端执行图 epoch；可包含多个 Stage retry |
| StageRun | ExecutionGraph 中一个逻辑 stage 的 durable 运行节点 |
| StageAttempt | 一次物理 stage try |
| StageLease | 某个 Worker 对某个 StageAttempt 的有限执行权 |
| WorkerInstance | 独占 DeviceSet 的 serving 资源和调度单位 |
| WorkerMember | 多成员 WorkerInstance 的单个成员；由确定性 leader 协调 |
| ModelResidency | 长驻模型、runtime epoch、profile 和 capacity route |
| StageArtifact | 已 materialize、版本化、可被下游或 exact cache 使用的中间结果 |
| Visible Completion | 客户可见的唯一完成结果、ArtifactSet、Charge 和访问资格 |
| Launch Receipt | 绑定 release/config/evidence/environment 的 Production Gate 证据 |

## 交接完成标准

接手人完成以下事项后，才算真正接住系统：

- 能指出当前 branch、HEAD、dirty worktree 和生产现场边界。
- 能解释 PostgreSQL authority、NATS wakeup、Kubernetes actuator、Node Agent host boundary 的关系。
- 能从一次 Job 的 Admission 追到 Attempt、StageRun、StageLease、StageArtifact、finalization 和 Charge。
- 能在不删除状态的前提下完成一次只读健康检查，并按故障域找到对应 runbook。
- 能生成并验证 API/Proto/SQL，知道哪些文件是 generated output。
- 能说明当前为什么不能宣称 production ready，以及下一项需要什么证据。
- 能在提交或发布前区分已提交基线、当前未提交实验、现场已采用 revision 和待采用候选 revision。
