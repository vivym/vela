# RAGFlow Kubernetes 验证部署方案

2026-09-15。状态：验证版 RAGFlow 工作负载、PVC、私有镜像、CPU/GPU 调度、APISIX hostname/TLS 路由和两台主机的 80/443 入口已配置；企业 DNS、客户端 CA 信任、账号初始化与实际模型端点仍待落实。部署授权已经给出，实施沿用本方案，无须再次要求 GitLab/CI 输入。

## 决定

用官方 RAGFlow **v0.27.2** 的 Helm 目录，加站点补丁，由平台在 `.70` 手动运行 Helm。独立 namespace 为 `ragflow-lab`；首版跑登录、知识库上传、排队解析、检索、问答和 API。此轮不建设通用发布门户、GitLab、CI/CD 或新的 Argo 发布通道。

上游源码固定到 `a024bea0cd93f39e6652a42bf84dd20c55bc560b`，不能直接使用移动的 `main/nightly`。Chart 的 `appVersion: dev` 不是应用版本，镜像另外明确锁定，再核对实际 linux/amd64 digest。来源及细节见 [上游研究](ragflow-upstream-deployment-research-2026-09-15.md)。

此前讨论的 `vela-app validate/deploy/promote` 是设想，仓库中没有可用实现。本次直接使用现成的 Helm 和一个仅服务 RAGFlow 的薄封装，不以实现通用发布工具为前置条件。

## 集群现场结论

2026-09-15 13:30–13:38 CST，经 marslab 跳板读取 Kubernetes API、Longhorn API 和两台管理节点的 `df/free/lscpu`；未重启主机或修改运行配置。

| 项目 | `.70 / llmpool01` | `.71 / llmpool02` |
| --- | --- | --- |
| Ready / DiskPressure | True / False | True / False |
| allocatable CPU / memory | 74 / 约 49.5 GiB | 74 / 约 49.5 GiB |
| Pod CPU requests | 16.005 | 15.630 |
| Pod memory requests | 17,660 MiB | 17,004 MiB |
| `kubectl top` memory | 18,096 MiB | 14,682 MiB |
| Pod memory limits 合计 | 61,704 MiB，121% | 59,976 MiB，118% |
| 根盘非 root 可用字节（df） | 734,078,099,456，约 683.6 GiB | 686,700,978,176，约 639.5 GiB |
| 根盘使用率 | 18% | 27% |
| CPU | Xeon Gold 6138，x86_64，AVX2/FMA/BMI 等标志存在 | 相同 |

足够做有明确资源上限的小规模验证，但现有内存 limits 已超额承诺，不能把平均空闲量理解为高负载生产保证。新增组件先单副本、小并发；每个组件设置 requests/limits，长任务限制并发。

`.66` 实时 `DiskPressure=True`，本部署全部排除。`.19` 离线/cordon；`.59` 曾有 GPU 异常，不作为第一批模型节点。`.44/.56/.57/.66` 不执行任何重启操作，所有已有未初始化数据盘保持原样。

### 存储为什么不用默认 Longhorn

- 默认 StorageClass 是 Longhorn。`.71` 的 Longhorn 磁盘 `Schedulable=False`：`storageScheduled=794568949760`，大于日志中的 `ProvisionedLimit=786255039693`；这是容量承诺上限，不是根盘已满。
- `.66` 的 Longhorn 磁盘仍不可用，历史 diskStatus.available 不能作为当前空闲空间。
- `local-path` 已安装，`WaitForFirstConsumer`，管理节点映射到 `/var/lib/vela-storage`。它与 Longhorn 共享根文件系统，实际写入仍需计入同一容量预算；不能把它当作新增容量。
- 验证版专门使用本地持久卷，初始合计约 **55 GiB**：MySQL 10、Valkey 5、Infinity 20、对象存储 20。建立仅 `.70/.71` 可选的验证 StorageClass/绑定规则，使用 `Retain`，明确保留 PVC。不得顺带修改默认 SC 或其他卷。
- local-path 的容量声明不是硬磁盘限额；PVC 配额限制的是申请量。必须同时限制文件数量/大小、监控真实占用，并保留镜像、日志、临时文件与现有 Longhorn 增长空间。两管理节点各预留至少 30 GiB 镜像和临时预算，实际镜像大小在拉取前确认。
- 数据可跨 Pod 重建及同主机重启保留；节点故障时不会自动跨主机恢复。第一轮使用样本文档，不宣称存储高可用；后续备份恢复和迁移独立实施。

## 组件、落点和初始预算

表中是小规模验证的建议预算，不是官方性能指标，也不是已经分配的资源。管理节点新工作负载先采用 memory request=limit，控制新增负载；CPU limits 可略高于 requests。

| 组件 | 初始落点 | CPU request | memory request/limit | 持久存储 |
| --- | --- | --- | --- | --- |
| Web + Python API | `.70` 优先，允许 `.71` | 2 | 4 GiB | 业务数据走后端服务 |
| MySQL 8.0.40 | `.70` | 1 | 2 GiB | 10 GiB 本地 PVC |
| Valkey / Redis 协议 | `.70` | 0.5 | 1 GiB | 5 GiB 本地 PVC |
| Infinity 0.7.3 | `.71` | 2 | 4 GiB | 20 GiB 本地 PVC |
| MinIO 兼容对象存储 | `.71` | 0.5 | 1 GiB | 20 GiB 本地 PVC |
| 文档解析 task executor | 健康普通 GPU worker | 4 | 初始 8–16 GiB | 有上限的本地临时/模型缓存 |
| Embedding / Chat 推理 | 健康普通 GPU worker 或已核实的模型端点 | 按选定模型配置 | 按实际模型和显存预检 | 仅模型权重、缓存 |

管理侧初始合计 6 CPU requests、12 GiB memory；模型侧另算。解析 worker 可以先用 GPU 主机的 CPU 执行 OCR，开启加速时明确申请 `nvidia.com/gpu` 并设置 RuntimeClass，不依靠 `DEVICE=gpu` 决定调度。

管理组件必须同时匹配 `vela.ai/management=true` 与 `vela.ai/control-plane-tier=cpu`。解析与推理组件匹配 `vela.ai/node-role=gpu-worker`；具体主机在实施时重新检查 Ready、GPU 显存、已有进程和缓存路径。普通 GPU 节点不承载 MySQL、对象数据或向量数据库副本。

独立 MySQL/对象存储让这次练习不依赖现有 Vela PostgreSQL、MinIO 的降级状态。官方 Python Helm 路径使用 Redis Streams 队列，因此不新增 NATS。Infinity 是此版 chart 默认引擎，可避免 ES/OpenSearch 的 privileged sysctl init；以后切换检索引擎需迁移/重建索引，不能当无损配置变更。

## 必须做的少量适配

1. **拆开 API 与 worker。** 两者都设置 `API_PROXY_SCHEME=python`。API 使用 `--disable-taskexecutor --disable-datasync`；task executor 使用 `--disable-webserver --workers=1`。官方 chart 没有完整 args/调度接口，需要补丁。只给对外 API Pod 配置 Web Service selector，不能误把无 Web 的 worker 加进后端。
2. **配置与镜像固定。** Chart 缺省没有设置上述 API 模式；源码显示空模式可能只启动 nginx、不启动后端。MinIO 的服务用户名与应用 `MINIO_USER` 同步。所有组件镜像取固定版本/digest，经已有镜像缓存或内部 release registry 使用；本轮不往 `.66` 写新的 RAGFlow 镜像副本。
3. **本地持久化。** 四类数据使用独立 PVC 与正确 nodeAffinity，依赖启动探针通过后才启动 API/worker。升级保留 PVC，不自动卸载数据库或执行清卷命令。
4. **队列不丢任务。** Valkey 开启持久化，设置合理 maxmemory 和 `noeviction`，避免官方 chart 的 128 MiB/allkeys-lru 在压力下逐出队列；队列满报错也必须监控。
5. **模型单独配置。** 新版镜像不包含 Embedding 模型，自 v0.22 起 tag 也不再带 `-slim`。本轮按服务名称筛查没有找到现成的模型 Service，这不排除主机进程或其他名称的服务。实施先核实可复用 Chat/Embedding 端点；若没有，在 GPU worker 部署小模型供验证，不需要用户提供商业 API key。API 侧只调用远程模型，避免在管理节点加载本地 embedding/reranker。
6. **收敛首轮业务路径。** 标准知识库上传后排队解析可以拆分；源码中仍存在 API 内同步解析/缩略图生成路径。首轮验证普通知识库的队列流程，直接聊天附件、Agent 文档解析等路径需禁用或转交模型 worker 后再开放，不能声称仅凭两个启动参数就隔离了所有计算。

## 入口、权限与监控

- APISIX 已配置为统一入口：企业 DNS 需将 `ragflow.marslab.ic` 指向 `.70/.71` 的本地 Nginx 80/443；Nginx 再转发到本机 APISIX `30443`，使用现有私有 CA。HTTP 自动 308 到 HTTPS；根路径代理 `ragflow-lab` Web/API，上传上限 128 MiB，流式读写超时 3600 秒，阻断 `/api/v1/admin`。现有 APISIX 限流为每个 APISIX 实例按其看到的 `remote_addr` 计数 120 requests/min；经过 Nginx/NodePort 后可能是共享的节点来源，不能等同于每个终端用户的独立额度。客户端仍需配置企业内部 DNS/hosts 并信任 `vela-gateway-ca`。
- 独立 hostname 的 `/` 同时承载静态文件、`/v1`、`/api`，避免未经验证的 `/ragflow` 子路径改写。TLS 证书加入该 hostname，由客户端信任私有 CA。
- 只公开 Web/业务 API，MySQL、Valkey、Infinity、S3 和管理接口均为 ClusterIP。上游 nginx 会代理 `/api/v1/admin`，即使不暴露 9381 也必须阻断或单独限制该路径。
- 默认关闭代码沙箱：官方 Compose 沙箱需要 privileged 和 Docker socket，与当前集群边界不符。MCP/管理端按需要另开，首轮不纳入验收。
- 平台生成随机 Secret，只提供 RAGFlow 应用账号给团队；初始化后关闭自由注册。不提供 SSH/sudo/cluster-admin 或 APISIX Admin 凭据。应用进程获得的运行时凭据不能向代码执行者保密，权限隔离不能作这种保证。
- 独立 namespace 加网络策略、资源配额和无 API token 的运行账号；只开放实际依赖、DNS、网关、监控、模型服务。现有针对 llm-api/llm-models 的策略不会自动覆盖新 namespace。
- 此次由平台手动 Helm 管理，不与 Argo 同时管理相同对象。团队获得应用 UI/API 访问与必要只读日志入口。Helm 发布身份由平台持有，其 release Secret 不发给团队。
- 复用 Grafana/Prometheus/Alloy/Loki：Pod/依赖健康、内存、PVC 实际用量、排队任务、解析失败、API 错误率和延迟、网关可达性。检查应用文件日志采集，不能把 stdout 采集等同全部日志。告警按既有偏好先在 Grafana 查看；完整 trace 只在确认 RAGFlow 实际支持/输出后列为已完成。

## 实施顺序与完成标准

1. 固定 chart/镜像并生成站点 values/补丁；核对镜像体积和模型下载来源，渲染、检查准入，记录部署版本。
2. 创建专用 namespace、Secret、网络/配额及本地 PVC；先启动 MySQL、Valkey、Infinity、对象存储，检查读写和真实落点。
3. 初始化数据库与账号；启动 Web/API 和独立 task executor；接入实际可用的 Chat/Embedding 服务。初次初始化串行，避免多个角色竞争迁移。
4. 添加 APISIX hostname/TLS 路由、上传限制与流式超时，接入现有看板和日志。
5. 用一份中文样本文档完成登录→上传→解析完成→向量索引→检索→带来源回答→API 流式返回；检查任务确实在 worker 执行。重建 RAGFlow Pod 检查文档与账号仍在；只演练应用版本回退，数据库 schema 升级需先备份且不保证由 `helm rollback` 回退。记录 namespace、入口、凭据文件位置、镜像版本和删除保护。

完成意味着真实文档/模型流程通过及可重建恢复，而不只是 Pod Running 或首页能打开。本轮不以生产高可用、灾备切换或全量 Agent 功能作为前置条件；这些限制明确保留给后续工作。

## 官方依据

- [v0.27.2 发布](https://github.com/infiniflow/ragflow/releases/tag/v0.27.2)
- [固定版本 Helm README](https://github.com/infiniflow/ragflow/blob/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/README.md)
- [固定版本 chart values](https://github.com/infiniflow/ragflow/blob/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/values.yaml)
- [官方 entrypoint 与角色开关](https://github.com/infiniflow/ragflow/blob/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/entrypoint.sh)
- [详细源码事实和限制](ragflow-upstream-deployment-research-2026-09-15.md)
