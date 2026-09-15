# 集群生产就绪验收清单 — 2026-09-14

这是后续工作的唯一收口清单。目标是控制节点、GPU worker 和可观测性达到
生产可用；“Pod Running”“一次 smoke 成功”和“完整 Vela release 可发布”
分别记录。当前不能宣告整体完成，也不再把已经通过的检查当成下一轮任务重跑。

09-15 07:58 CST [现场快照](evidence/cluster-readiness-after-worker-network-2026-09-15.json)
仍为 54 注册、53 Ready、407 GPU；`.66` DiskPressure、三个有状态服务各 2/3、
MinIO 5/6。R4 已补齐 51 个在线 worker 到 Control 的精确网络入口：102 次
双 CPU 节点来源测量、667 项连通/拒绝检查通过，临时资源清理完成，主机与
Control Pod 身份不变。仅开放各实测 `/32` 的 TCP8444，未认证 TLS 全部拒绝；
应用 Node Agent 身份注册与完整发布仍开放。详见
[worker 网络专项](worker-control-network-validation-2026-09-15.md)。

09-15 07:15 CST [现场复核](evidence/cluster-readiness-before-stage-tracing-2026-09-15.json)
仍为 54 注册、53 Ready、407 GPU，`.66` DiskPressure；三个有状态服务各
2/3、MinIO 5/6，Argo 六个 Pod Ready、双管理节点 HTTPS 可用。
07:28 [权限复核](evidence/platform-publishing-readonly-followup-2026-09-15.json)
确认 Argo 无集群角色绑定，旧 CI 无直接写入，Secret/RBAC/网络边界仍符合预期。

R6 本轮已实现 StageAssignment → worker → Runtime 的逐任务追踪，并通过
真实数据库回放、文件日志重新打开后的重连/旧任务落盘、实际 gRPC 关联和
watchdog 取消测试。候选需要数据库 97/98 迁移；带 trace 的 worker 入场日志
使用 schema 6，须先升级 journal 读写者。**生产仍未采用**，真实模型业务、
实际 Collector/Tempo 链路、SLO/Launch Receipt 继续开放。详见
[Stage/Runtime 追踪](stage-runtime-tracing-validation-2026-09-15.md)。

09-15 07:00 CST [只读预检](evidence/async-tracing-production-preflight-2026-09-15.json)：
`.66` 仍有 DiskPressure，PostgreSQL/NATS/APISIX etcd 各 2/3、MinIO 5/6，
Control 2/2。根卷可用约 122.10GiB，即使按原清单回收 94.00GiB，距 25% 恢复线
仍差约 3.32GiB，不能承诺原方案单独足以恢复；原目录未删除。
06:45 `.19` SSH 仍为 No route to host，保持离线/cordon。

R6 新增实际实现：Job 起始 trace 随事务保存，Outbox 重试、NATS header 和
Inbox 重投递关联已在独立 PostgreSQL/三 NATS 实例中通过故障窗口验证。
97 迁移、Linux Control 二进制与角色/部署契约检查已准备；生产实际仍为
`app` 数据库/schema 96 和原镜像，尚未采用。详见
[异步消息追踪](async-message-tracing-validation-2026-09-15.md)。这条为 07:00
阶段记录；后续 StageRun/runtime 实现见上方增量，生产采用与真实业务验收
继续开放，不关闭 R6。

09-15 06:39 CST [最新现场复核](evidence/cluster-readiness-after-fleet-preparation-2026-09-15.json)：
仍为 54 注册、53 Ready、407 GPU，`.66` DiskPressure=True；PostgreSQL/NATS/
APISIX etcd 各 2/3，MinIO 5/6，健康成员仍为 `.70:2/.71:3`。Longhorn 为
18 attached/healthy、2 attached/degraded、7 detached/unknown 和 2 detaching。
Argo、Control 与双网关仍可用。`.66` 根卷可用降至 125.45GiB，回收待授权的
94.00GiB 后仅比 25% 恢复线高约 0.03GiB，持续写入使这不再构成可靠余量。
原 70 个目录仍未删除；不扩大清理范围，实际执行前须重新核对容量和全部路径。

R4 增量：三台 API Server 的 Fleet 候选配置已暂存，每台原生 Kubernetes
检查 14 项通过；重复准备均 `created:false`，boot ID、RKE2 InvocationID 和
static Pod manifest hash 不变。候选未采用，Fleet 和 webhook 仍未部署。
这完成配置准备，不关闭真实准入和续期采用验收。

09-15 05:56 CST **磁盘事件早期快照，不能作为最新健康状态**：
`.66` Ready 但 DiskPressure=True，NATS/APISIX etcd/PostgreSQL 均 2/3 Ready，
MinIO 5/6 Ready、读写 200；Longhorn 有两个卷仍 detaching，Alloy/node-exporter
各 52/54。Control 2/2、Argo 与两台网关入口仍可用。kubelet 的 15% imagefs
驱逐加 10% minimum reclaim 使本次恢复需要约 25% 根卷可用。已安全回收
2.91GiB Vela Go cache，但其他作业仍继续写入。06:07 已将 70 个其他项目
临时目录（94.00GiB）完整归档并比对通过，回收前预检通过；原文件未删除，
需要对清单中其他项目的源码/结果路径取得回收授权。详见 [磁盘事件](node66-disk-pressure-2026-09-15.md)
和 [回收清单](node66-temporary-artifact-reclamation-plan-2026-09-15.md)。
新增两条预警/实际压力告警，145 个场景测试通过。状态采集器已补充压力条件、
CNPG 副本和 MinIO 实际健康成员分布。此事件归 R3/R5。

R4 Fleet 的镜像、PKI、不可变材料与主机候选已准备，但部署在健康预检处停止；Control
候选 trust 引用尚未采用、webhook 未注册。详见
[Fleet 准备记录](fleet-controller-deployment-preparation-2026-09-15.md)。
发布平台的管理/发布/审批/只读隔离已完成，GitLab 继续复用并暂缓对接。

09-15 04:58 CST，NATS 可观测增量已完成：两个 CPU exporter Pod 固定采集
三个成员，共六个健康目标；27 个原生 promtool 场景、13 项现场检查通过。
三条文件配额告警及缺失 stream/consumer 的两条告警按预期 firing，采集与
冗余告警 inactive。容量核算确认三个 NATS PVC 扩到 80Gi 将在 `.71` 兑现
全部现有卷承诺时出现约 56.60Gi 缺口；即使删除四个旧 MinIO 卷，剩余
23.40Gi 仍低于 10% 余量要求。未调整预留或删除数据。详见
[NATS 容量与监控](nats-capacity-and-observability-2026-09-15.md)。本项补齐
R3/R5 的容量证据与监控，业务 bootstrap、Outbox replay 和物理容量仍开放。

09-15 05:09 CST [当前集群快照](evidence/cluster-readiness-after-nats-observability-2026-09-15.json)
仍为 54 注册、53 Ready、407 GPU；Argo 六副本全部 Ready，Control 两 CPU 副本
Ready/零重启，MinIO 读写均 200。05:05–05:08 的
[留存核查](telemetry-retention-capacity-2026-09-15.md) 确认 Prometheus 仅有
约 27.94 小时历史；完整数据块按各自密度外推 15 天约需 44.94–64.28GiB，
存在早于 15 天触及 40Gi 上限的风险。Loki/Tempo 当前占用低，尚未具备七天/
48 小时真实业务验收。上述为 R5 容量证据，不修改保留目标或新增验收编号。

09-15 04:12 CST 最新增量：Control schema-96 修复镜像 2/2 Ready，NATS 3/3
Ready，当前 16 份材料已改为不可变版本引用，25 项切换检查通过。数据库与独立
96 次真实迁移参考库比对后完成 Goose 基线登记；最终 3,004 个 schema 定义
一致，业务表行数和种子 hash 不变。两次失败与恢复均保留独立记录。详见
[材料采用专项](release-material-adoption-2026-09-15.md)。这完成当前 Control/NATS
输入的增量，完整 canonical release 仍开放。04:27 CST 已另行启用 Control
OTLP，46 项实际 Pod/双网关追踪检查通过；真实业务和异步 lineage 仍属 R6。

04:28 CST [最新现场快照](evidence/cluster-readiness-after-material-tracing-2026-09-15.json)：
54 注册、53 Ready、407 GPU；`.19`（server-36）继续 Unknown/cordoned。
Argo 四个工作负载共 6 副本全部 Ready，Control 2 副本零重启，NATS 3/3；
25 个 Longhorn 卷 healthy，四个旧 MinIO 卷仍保留，MinIO 读写健康接口均 200。
剩余 DaemonSet 缺口对应离线节点，没有把硬件异常计为已修复。

09-15 仓库进展：三台 release registry 已启用身份/仓库前缀权限，当前 Control
镜像的两个缺失副本已补齐；两份真实 Kubernetes 探针完成错误凭据拒绝和
`.70` → `.71/.66` 认证回退拉取，清理通过。两个 Control Pod 已携带只读
imagePullSecret 且 Ready。详见 [仓库专项验收](registry-access-validation-2026-09-15.md)。
仓库持续观测已接入现有 Grafana：9/9 实际探针通过、2/2 探针 Pod Ready，
新六面板看板网关认证访问 200，9 个告警场景通过，当前无活动仓库告警。
公共 Docker Hub 首次下载仍可能较慢，国内备用源本次 Blob 请求返回 403；
当前 release 镜像及其三端认证回退已在内网实测通过，不能混同两者。

历史完整现场快照为 2026-09-14 20:41 CST，原始数据见
[evidence/cluster-readiness-2026-09-14-followup.json](evidence/cluster-readiness-2026-09-14-followup.json)。
本轮刷新快照见 [evidence/cluster-readiness-live-2026-09-14.json](evidence/cluster-readiness-live-2026-09-14.json)，APISIX API 路由证据见
[evidence/apisix-vela-api-route-2026-09-14.json](evidence/apisix-vela-api-route-2026-09-14.json)。
最终 21:49 CST 为 54 注册、53 Ready、407 GPU，`.19` 再次 NodeStatusUnknown/SSH No route to host，仍 cordoned。见 [最终节点状态](evidence/cluster-final-node-state-2026-09-14.json)。
21:39 CST 刷新见 [网关修复后快照](evidence/cluster-readiness-after-gateway-2026-09-14.json)：曾为 54/54 Ready、407 GPU，但 `.19` 在后续 SSH 盘点中再次不可达，持续 cordon；不能作为稳定恢复证据。详见 [节点异常记录](node19-recovery-findings-2026-09-14.md)。
可用 `hack/collect-cluster-readiness.py` 在管理节点只读更新快照。
22:37 CST 专项刷新仍为 54 注册、53 Ready、407 GPU。R2 的真实首次入群和管理
API 切换已通过，50 个在线普通 worker 完成无重启下发，两个 CPU 节点安装
`vela-kubectl`；临时 VM、磁盘、凭据及集群资源清理通过。
见 [R2 记录](rke2-bootstrap-ha-validation-2026-09-14.md) 与
[专项证据](evidence/rke2-bootstrap-ha-2026-09-14.json)。
另确认 `.70:80` 当前由主机级 `nginx.service` 监听，非 RKE2 Ingress；保留该现有
服务。因此历史“全部 TCP80/443 关闭”不能继续作为当前端口结论。
后续收口快照见
[22:54 CST 最终现场检查](evidence/cluster-readiness-after-bootstrap-https-2026-09-14.json)：
54 注册、53 Ready、407 GPU；两个网关 HTTP 308、HTTPS Grafana 200；
25 个 Longhorn 卷 healthy，4 个保留的旧 MinIO 卷仍为 detached/unknown。
NATS 三成员已完成显式 2GiB JetStream 内存上限配置与逐个 Pod 替换，仲裁通过，
凭据及 PVC 不变。见 [NATS 配额记录](nats-memory-limit-validation-2026-09-14.md)。
后续专项进展：部署契约全部通过；Grafana `/grafana/` 资源路径已修复并经 Helm 91.1.0 发布，PVC 保持不变；APISIX 2.17.0 不支持 `podLabels`，已将现场 NetworkPolicy 改为匹配 chart 原生 name/instance 标签，消除对手工 Pod 标签的依赖。Control 现场 overlay 使用 runtime ConfigMap
`vela-control-runtime-v-ebd9cc4d0cb4h`；六成员 MinIO 已完成对象/IAM/内容校验并切换稳定 `minio` Service，原四成员 StatefulSet 已缩容到 0、四个 PVC 保留。六个新 PVC 仍为迁移阶段 8Gi 配置；扩容到 16Gi 因容量和用户要求暂缓。29 个 Longhorn 卷
在 21:39 快照中为 25 个 attached/healthy、4 个旧 MinIO retained 卷 detached/unknown；后者未被删除。16:04 的 23 卷文件保留为历史快照。新 MinIO 三组成员失效读写
检查通过，清理失败及恢复单独留痕；仲裁指标改为专用 v3 接口，133 个告警
测试通过。详见 [MinIO 专项记录](minio-ha-validation-2026-09-14.md)。

真实恢复证据及边界见 [有状态恢复记录](stateful-recovery-validation-2026-09-14.md)。
Grafana、网关 TLS/限流和证书自动化见 [网关验收记录](gateway-validation-2026-09-14.md)。

## 已完成并有证据的范围

| 项目 | 已验证结果 | 证据边界 |
| --- | --- | --- |
| 节点与 GPU runtime | 54 注册、53 Ready；50 个在线普通 worker 加共享 `.66` 共 407 GPU | `.19` 未恢复；`.59` 只有 7 个物理 NVIDIA PCI 设备 |
| 自启动与镜像 | 50 个在线 worker 的 rke2-agent enabled/active；三端点缓存和后端列表已检查；真实 containerd pull 成功 | enabled 是配置证据；没有对受保护主机做重启验收 |
| 已加入 worker 的控制面切换 | `.11` 阻断 `.70:6443/9345` 后重启 agent，22.11 秒通过；连接 `.71/.66`，恢复 Ready | 不是物理重启，也不证明第一次加入集群时 `.70` 不可达的行为 |
| 管理服务与网关 | APISIX 两个 CPU 副本、三 etcd；Grafana 经 `.70/.71:30080` 的登录页、9 个静态资源、认证后 31 个看板列表均 200，Prometheus/Loki/Tempo 数据源健康；`/api/*` 已接入 `vela-api`，未带 bearer token 的探测在两入口均 HTTP 401 | 内网私有 CA、证书自动签发与五分钟同步已部署；两入口及两 Pod 的 TLS1.3/指纹通过；本地限流实测通过。HTTP 已统一 308 到 HTTPS，裸 IP 证书校验已通过；认证成功业务 SLO 仍待真实业务流量 |
| 默认入口关闭 | RKE2 NGINX Deployment 0/0，无默认 IngressClass/webhook；历史 53 节点端口关闭探测已被新现场事实修正：`.70:80` 有主机级 nginx | 保留现有 nginx 与原 Compose 监控；新部署 API 使用 APISIX，不擅自退役其他人的服务 |
| 基础设施指标 | kube-proxy 53、scheduler/controller-manager/etcd 各 3；TLS/RBAC 实测 401/403/200；Longhorn 策略指标覆盖 29 卷 | 离线 `.19` 保持真实缺失；节点恢复后必须先做端口预检再启用采集 |
| 日志和链路 | Alloy 53 在线采集器按本机发现；两 CPU 节点实测五类合成秘密脱敏、标签和单一采集归属；APISIX → OTel → Tempo 实测通过；09-15 新应用 tracing 包通过两个非特权 Linux 进程 HTTP → gRPC → Collector → Tempo 的 39 项检查 | Control 生产 HTTP 追踪已启用且通过 46 项检查；其他组件及异步 Outbox/Stage 关联未完成；不声明长期故障下日志零丢失 |
| 看板和告警 | 复用 GPU 看板 25 panels，IPMI 只读 federation；16-panel 控制/遥测看板；Grafana 告警触发、静默、解除、自动恢复通过；133 个 promtool 场景通过；MinIO 专用 v3 仲裁采集部署并验证 4 个唯一目标 | IPMI 依赖旧 `.70` Prometheus；外部通知按用户决定暂缓 |
| Longhorn 数据副本 | 持久卷要求 2 份；Loki 两份均在 CPU 节点 RW；快照/恢复临时 PVC 实测通过 | 两个 disposable Control scratch 为 1 份；复制不等于应用零中断 |
| Control 本次故障 | 重现 fsGroup 重挂载将 sandbox 0700 扩为 2770；取消 fsGroup 后重挂载仍 0700；两个 Control 副本在 `.70/.71` Ready；各有 1/2 次历史启动重试（09:40Z 数据库端口尚未恢复），此后持续运行 | 镜像已固定当前运行 digest；尚未取得完整业务发布验收 |
| 清单可重放修复 | Kustomize 保留显式 namespace，Longhorn 策略只在 longhorn-system；完整观测 bundle server dry-run 通过 | 本次没有盲目重放其他未审计的运行时配置 |

既有测试记录来自本任务较早的现场操作；最新 JSON 中 `live` 是本轮重新采集的
状态，`prior_validation` 明确标记继承记录，避免把历史演练伪装成本轮重测。

## 固定六项验收及剩余工作

09-15 R4 增量：显式 `kubernetes-v2` 契约已纳入 NATS Secret、既有
`monitoring` 内的 5 个应用监控资源与 Node Agent 精确入口。先完成 `.66` 的实测
CNI 来源 `10.42.2.0/32`，07:58 增量已扩展为全部 51 个在线 worker 的实测
`/32`，仍只放行 TCP8444；来源、切换与匿名 TLS 拒绝探针已通过。
Control 的 33 个 Secret 环境变量、9 组文件键引用及 NATS 材料已完成切换，
2 个 ConfigMap/14 个 Secret 都有不可变版本名和 canonical revision。
数据库 ledger 原为 0；现已与独立 schema-96 参考库核验后登记标准基线，
没有重放生产迁移，登记时间不冒充历史执行时间。新 Control 同步修复了两个
Fleet 函数的精确权限 allowlist。见 [发布输入](release-input-validation-2026-09-15.md)
与 [材料采用](release-material-adoption-2026-09-15.md)。以上仍归入既有 R4。

| ID | 尚未完成 | 完成条件与下一步 |
| --- | --- | --- |
| R1 | 两处物理异常 | `.19` 曾短暂恢复：8 张物理 GPU、驱动/Toolkit/自启动正常；设备插件解包文件为 0 字节，启动时报 exec format error，同时记录 MCE 硬件错误。继续盘点时又不可达，尚未执行缓存修复。须先稳定主机再校验/重建该镜像缓存并启用节点监控、解除 cordon；`.59` 调查缺失的第八张卡。可恢复的 Kubernetes 配置不反复重装；不得替用户删除旧数据 |
| R2 | 已收口；离线 `.19` 的补装并入 R1 | fresh KVM guest 没有 agent 目录/缓存，屏蔽 `.70:6443/9345` 后通过 `.71` 首次注册并 Ready；guest 原生 kubeproxy 凭据通过 `.71:6443` 读取 API。临时资源及凭据清理通过；50 个在线普通 worker 安装启动选择器并保持 boot ID/agent invocation 不变，`.70/.71` 安装 `vela-kubectl` 并通过认证 readyz。管理命令仅在执行前选择端点，不自动重放失败写入。无 VIP 依赖；该 NAT VM 不代表跨节点 CNI 生产验收 |
| R3 | 有状态服务恢复和 MinIO 写入可用性 | 六成员 EC:3 已完成 259 个对象（约 138MB）的 SHA256、对象元数据/标签、17 个桶元数据条目和 7 个 IAM 条目校验；在暂停验证写入后完成最终增量并切换稳定 `minio` Service，原四成员 StatefulSet 为 0、PVC 保留。真实 PostgreSQL PITR 已通过：恢复备份完成后、目标时间之前的 WAL 标记，排除目标之后的标记，恢复 145.8 秒，临时资源清理完成；本次修正恢复 NetworkPolicy 后通过。NATS 独立 R3 file stream/consumer 经三个 Pod 逐个替换后消息 SHA256 不变，未 Ack 消息重投递次数为 2，单成员恢复 6.88–13.18 秒，已清理测试 stream。业务 `VELA_EVENTS` 尚未配置：64GiB 契约大于 50Gi PVC 的自动 server 配额约 36.7GiB；须先解决配额/容量和 bootstrap。六个 MinIO PVC 从 8Gi 扩到 16Gi 暂缓。成员测试不等于整集群/物理主机丢失，也不证明 PostgreSQL Outbox replay |
| R4 | 发布权限、可重建 release 和部署契约一致性 | 原 7 类失败已完成 base/现场 overlay 分离及 Kubernetes 语义校正，全部 deployment-contract 测试通过；新增 Barman 渲染后的镜像/RBAC/可用性检查、监听地址和 UID 展开顺序回归、现场引用及容量边界检查。Control runtime 使用 h 的不可变快照，schema-96 修复镜像两 CPU 副本 Ready，20Gi 仍显式标为验证配置。应用权限边界已部署：两个 namespace 的 PSA/RBAC/准入/网络策略、全局 observer、两 CPU 管理节点短期凭据签发通过 106 项 API/准入和 29 项运行/网络检查；详见应用权限记录。Registry 三生产入口已启用身份/前缀权限；候选 45 项、生产 44 项检查通过，Control 镜像完整复制到两备用端点，两个应用命名空间实际认证回退拉取通过。09-15 已部署 Keycloak/Argo OIDC+MFA 与项目组映射，旧 CI 直接写权限撤销；现有 GitLab 对接按用户要求暂缓。尚需产出覆盖所有应用组件、固定 digest、配置版本和 Secret/PKI 引用的 canonical release bundle，不能把当前历史 release label 当成完成凭证 |
| R5 | 存储/留存容量兑现 | 现场 Control scratch 每卷 20Gi，contract 每卷 110Gi/总池 440Gi/专用及 I/O 隔离尚未兑现。默认允许 100Gi 输入，Inspector 完整 spool 后 sandbox 再复制一份，峰值可达约 200Gi 加开销，因此 110Gi 不能直接证明足够。GPU worker 的 14 块 4TiB NVMe 已完成只读盘点和 prepare，用户要求暂缓清空/初始化，未格式化。MinIO 六个新 PVC 当前为 8Gi；扩容到 16Gi 的容量计划因物理余量和用户要求暂缓。另处理 `.66:/srv/models` 约 9.6% 可用空间；NATS 内存缺口已修复为每成员 2GiB/Pod 8Gi，但 64GiB stream 与 50Gi PVC/约 36.7GiB server 文件配额仍不匹配；核验 Prometheus/Loki/Tempo 实际留存，禁止删除历史模型制造空余 |
| R6 | 业务 API 与可观测性最终验收 | 完成 Vela 应用 OTLP 传播、真实 API SLI/SLO 和 Stage residency 指标；用实际 API/模型任务贯穿认证、调度、产物校验与存储。所有新对外 API 通过 APISIX；内网 TLS、证书同步、告警、未认证拒绝及局部限流已通过。后续完成无 SNI 的裸 IP TLS 与 HTTP→HTTPS 308 全局策略，12 个方法/伪造转发头组合通过，Grafana 跳转及 HTTPS 200/API 401 通过，遥测插件保留。仍需认证成功业务与完整业务路由验收；未产生的 SLO/Launch Receipt 不伪造为通过 |

R1 需要现场机器恢复；R2 已收口，当前仍可推进 R3–R6，不应把整个任务判为阻塞。
R3–R6 彼此有关联，但每项只以表中的产物和验收结果收口。任何新增问题必须
归入这六项并注明证据，避免一边开发一边增加未定义的“生产要求”。

## R4/R6 发布与权限专项调查

最初调查确认应用发布准入未落地，随后已部署并完成专项验收，见
[应用权限隔离记录](application-access-validation-2026-09-14.md)。`llm-api` /
`llm-models`、全局 observer 和双管理节点签发工具已生效；临时探针与凭据文件
清理通过。内部 release registry 的三个 `:5005` 入口已经关闭匿名访问，按平台、
只读节点和两个应用前缀认证；详见 [仓库验收](registry-access-validation-2026-09-15.md)。
以下工作归入 R4，不另增验收编号：

- **发布平台已实施**：Argo CD v3.5.3、独立 Keycloak realm、PKCE/TOTP、发布/审批/只读隔离、Secret 引用白名单、失败同步和回滚、Argo 看板/告警。详见 [09-15 发布平台验收](platform-publishing-validation-2026-09-15.md)。GitLab 已存在且暂缓对接，Project 仓库白名单关闭；不新增 Rancher/门户作为前置条件。

- **已完成 Kubernetes 凭据边界**：管理/发布/只读/runtime 独立，短期 TokenRequest，
  真实 API 拒绝测试覆盖 Secret、RBAC、主机权限、调度与网关绕行；不把已有
  SSH/sudo 管理员误当成受限发布人员。
- **已完成命名空间边界**：PSA restricted、准入、资源上限、默认拒绝网络策略
  已生效；非模型 API 限 CPU 管理节点，模型相关任务限普通 GPU worker。
  Prometheus 真实采集、APISIX 请求、OTLP 端口和管理接口阻断均已验证。
- **已完成 release registry 权限和认证回退**：保留镜像内容，补齐当前 Control
  完整 OCI 副本；Control/应用 namespace 使用各自只读 imagePullSecret，真实
  containerd 错误凭据拒绝及 `.71/.66` 回退已通过。今后版本仍须按 digest
  推广到两备用仓库，尚未部署后台自动复制。
- 现场尚未部署 Fleet Controller / Stage Worker，未找到 canonical release
  bundle。schema 3 已通过显式 `kubernetes-v2` 资源契约修正 NATS Secret 和
  monitoring namespace 布局；当前 Control/NATS 材料、schema-96 基线已验证，
  仍须准备其他组件的真实发布输入。

R6 的 HTTP/gRPC 追踪已完成源码接入，实际 Linux 双进程经现有 Collector/Tempo
验证 39 项通过；04:27 CST 实际 Control 两副本的 HTTP 追踪已启用，双网关和
每个 Pod 的 46 项检查通过，临时转发进程已清理。详见
[应用传输层追踪验收](application-tracing-validation-2026-09-15.md)。其他组件、
Outbox/Stage 异步关联、host/isolated runtime 环境注入和真实业务验收仍待完成。
APISIX 自身 trace 或这次探针不能替代应用完整业务链路。当前脏工作树不能直接
当作 release bundle 所要求的 clean HEAD，不批量提交或清理既有工作。

## 用户已确定的边界

- 控制/存储使用 `.70/.71/.66`，接受集群内复制、无独立物理故障域；不把
  新机房、第三个 CPU 节点或异地备份采购列为当前未授权的前置条件。
- `.44/.56/.57/.66` 不得重启、重载 GPU 驱动或做破坏性主机修复；前三台
  不在 Kubernetes 部署清单内。本次仅替换 `.66` 上失败的应用 Pod，没有重启主机服务。
- 外部通知暂缓，以 Grafana/Alertmanager 看板、留存和静默流程为当前目标。
- 原 Compose Prometheus/Alertmanager/Grafana 和历史数据保留；当前 IPMI
  federation 的依赖必须明确，退役不是本次默认操作。
- 私有内网验证与正式客户业务发布分别验收；未完成的 release/容量/恢复
  要求保留，不用空白指标、降低预期 GPU 数量或取消测试来掩盖。

09-15 08:20 CST 增量：全体 54 节点运行时盘点完成，均为 amd64、containerd
`2.2.6-k3s1`；53 台 Ubuntu 24.04，1 台 Ubuntu 22.04，48 台内核
`6.8.0-139-generic`，其余版本已记录。51 个在线 GPU worker 的 Control
8444 精确来源规则已采用；runtime-startup 三个 helper 的 12 文件包已按当前
源码 revision 构建并通过 verifier，但生产密钥、授权证明、Node Agent 发行包
和真实身份尚未具备，未向 worker 下发。详见
[evidence/runtime-startup-package-build-2026-09-15.json](evidence/runtime-startup-package-build-2026-09-15.json)
和
[evidence/worker-runtime-version-inventory-2026-09-15.json](evidence/worker-runtime-version-inventory-2026-09-15.json)。

09-15 08:30 CST 增量：当前源码 revision 已构建 Node Agent Linux amd64 主机包，
构建器自带完整清单与合同校验通过。该包尚未安装到 GPU worker：Node Agent
运行需要每台机器独立的 TLS/身份、授权目录、Worker journal 和 quota 配置，
直接启用会因缺少这些输入而 fail closed。证据见
[evidence/host-package-build-2026-09-15.json](evidence/host-package-build-2026-09-15.json)。

09-15 08:45 CST 增量：对 51 个在线 GPU worker 完成主机服务盘点。除 `.66`
仍保留历史安装的 `vela-pidfd-broker` 外，其余节点均没有 Node Agent、broker、
policy issuer 二进制或环境文件；因此当前 worker 只是 RKE2 节点，不应误称为
Vela Node Agent 已部署。已生成不含密钥的候选身份映射，等待每节点证书、Fleet
授权和 root-owned 环境材料后才能安装启用。证据见
[evidence/worker-service-inventory-2026-09-15.json](evidence/worker-service-inventory-2026-09-15.json)
和
[evidence/worker-identity-plan-2026-09-15.json](evidence/worker-identity-plan-2026-09-15.json)。
