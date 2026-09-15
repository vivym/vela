# 70 节点 Docker Compose 监控迁移评估

## 现场状态

`10.1.201.70` 上的 `/data/Prometheus/docker-compose.yml` 仍由 Docker
运行，包含 Prometheus、Alertmanager 和 Grafana。它与 Kubernetes
`monitoring` 命名空间中的 kube-prometheus-stack 同时存在，属于两套独立
的采集、规则和告警链路。

复核时 Compose 定义了 6 类抓取任务：Prometheus、node-exporter、
`nvidia-smi-exporter`、Alertmanager、Grafana 和 IPMI。原维护者正在更新
硬件采集配置，IPMI 任务名已由 `ipmi` 改成 `ipmi-supermicro`。
规则文件包含主机不可达、
GPU exporter 不可达、CPU/内存过高、根文件系统空间不足、GPU 温度过高和
GPU 显存使用率过高，以及 GPU 数量和功耗等告警。Grafana provisioning 下有 GPU、节点和
系统总览 dashboard 文件。

## 可借鉴内容

- 15 秒采集/评估周期可作为高频 GPU 运维指标的参考；实际周期应按指标量
  和 Prometheus 资源重新压测。
- 主机和 GPU exporter 的基础告警语义应迁入 `PrometheusRule`，但要统一
  告警名称、`for` 时长、严重级别和 runbook，避免同一故障产生两条告警。
- GPU、节点和系统 dashboard 的 panel/PromQL 可作为补充素材。导入前需
  将 datasource UID 从旧的 `PBFA97CFB590B2093` 改为集群的 `vela-prometheus`，并
  检查 job label 与当前 ServiceMonitor 的实际值。
- 30 天保留策略可作为容量规划上限参考；当前 Kubernetes Prometheus 已
  使用 Longhorn 50Gi、15 天和 40GB retention cap，应以实测样本写入率
  决定是否延长，而不是直接复制 30 天。

## 不应直接复制的部分

- `network_mode: host`、单机本地目录和固定 `127.0.0.1` 目标只适用于
  `.70`，不具备管理节点故障切换能力。
- `latest` 镜像标签不可用于可审计发布；Kubernetes 镜像必须固定版本并
  优先走内部 registry。
- Compose 文件内的 Grafana 管理员密码是明文；集群使用 Secret，密码不
  应回填到仓库或 Compose 文件。
- 静态 IP 列表包含已不可达的 `.19`，并对 `.56` 使用了 `9101` 这一特殊
  端口。Kubernetes 侧应以节点/ServiceMonitor 发现为准，并保留 `.19`
  的真实缺失告警。
- 不能把每台机器的 GPU 数量都硬编码为 8。`.70/.71` 是 CPU 节点，
  `.59` 当前只枚举到 7 个 NVIDIA PCI 设备；预期数量与实际数量应分别
  维护，保留缺卡问题，避免通过改预期值消除告警。
- 模型服务常驻显存、满负荷推理时接近功率上限都可能是正常行为。
  显存使用率超过 95% 或功耗超过额定值的 95% 应先作为容量/负载信号，
  不能直接照搬成需要处置的故障告警。

## 已完成的复用

- 新 Grafana 已接入原 GPU overview/detail 两张看板，共 25 个面板。
  datasource UID、GPU job 标签和三个不兼容的 exporter 内部指标已适配；
  全部 GPU 面板查询已返回真实集群数据。
- 新 Prometheus 通过 `.70:9090/federate` 只读接入原 IPMI 指标，新增
  `Vela · IPMI Hardware` 看板。没有复制 BMC 凭据、增加直接 BMC 探测或
  修改原 Compose。这个过渡方案依赖旧 Prometheus 继续运行。
- IPMI 告警同时检查采集接口 `up` 与实际硬件读取 `ipmi_up`，避免把
  “exporter HTTP 正常、BMC 读取失败”误报为健康；数据缺失显示为空。
- 新监控的节点、GPU、存储和中间件缺失规则已补齐 `up=0` 与无序列两类
  故障，并保留故障 instance。53 个基础设施场景和 12 个硬件采集场景，
  共 65 个 promtool 测试在该复用检查点通过；后续控制组件、日志/链路和 MinIO quorum 场景扩展后的总计为 127 个，见最新验收清单。
- Grafana 中补充了显式 Alertmanager 数据源；修复了重复默认数据源导致
  自动 provisioning 重载失败的问题。外部通知按用户决定暂不配置。

截至 2026-09-14 14:41 CST，三张看板共 33 个面板、38 个查询均返回
真实数据。IPMI 覆盖当前上游配置中的 55 个 BMC，其中 6 个有采集失败；
这些不是健康节点，保留失败信号。具体原始结果见
`docs/evidence/observability-compose-reuse-2026-09-14.json`。

一次临时告警已走完 Prometheus 触发 → Grafana 查看 → 静默 → 解除静默
→ Prometheus 条件恢复 → Grafana 告警消失的流程。临时规则已删除，测试
静默已过期；演练前验证了新 Alertmanager 没有外部 receiver integration。

新看板入口均经过 APISIX：

- `http://10.1.201.70:30080/grafana/d/vela-marslab-gpu-overview`
- `http://10.1.201.70:30080/grafana/d/vela-marslab-gpu-detail`
- `http://10.1.201.70:30080/grafana/d/vela-hardware-ipmi`

## 切换条件

目前应继续保留原 Compose。退役前还需迁移 IPMI 直接采集、核对旧看板
和规则差异、确定历史 Prometheus/Grafana 数据保留方式，并与原维护者
协调停用。当前只获授权复用/搭建新监控，未执行旧容器停用。

## 16:38 CST 只读复核

三只旧容器均为 Running、`restartPolicy=unless-stopped`。运行中的
Prometheus 镜像是 `prom/prometheus:v2.55.1`，API 也报告 2.55.1；这与用户
展示的 Compose 中 `local/prometheus:ubuntu24` 不同，评估实际版本应以
容器和 API 为准，不能仅从镜像别名推断。

旧 Prometheus 有 172 个目标，170 个 `up`。55 个 IPMI 抓取接口全部正常，
但 `ipmi_up` 的 165 条 collector 序列中 18 条为 0；这不是 18 台故障机器，
需要按 instance 去重。旧 TSDB 约 4.6G、Grafana 数据约 84M，当前约 28.1 万
head series、1.73 万 samples/s。该快照不足以证明 30 天留存空间；还要结合
完整块覆盖时长、压缩后的日增长和磁盘配额。新 Grafana 经 APISIX 返回 200。
