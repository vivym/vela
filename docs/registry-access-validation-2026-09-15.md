# 内部镜像仓库认证与回退验证 — 2026-09-15

三台生产 release registry 的 `:5005` 已启用认证，所有匿名访问返回 401。
平台、节点只读和两个应用租户的发布/只读身份已分离。Docker Hub 等
`:5000–5004` 上游缓存配置保持有效。本项归入固定清单 R4。

## 本次结果

| 范围 | 已验证结果 | 证据 |
| --- | --- | --- |
| 配置候选 | 45 项身份、路径、跨 repository blob mount、上传、读取和删除检查通过；候选容器及测试清单已移除 | [候选记录](evidence/registry-access-candidate-2026-09-14.json) |
| 当前 Control 副本 | 原 `.71/.66` 缺失的 OCI index 已补齐；每端 3 份清单、7 个 Blob、25,099,492 字节，逐 Blob GET/SHA256 校验 | [完整镜像复制](evidence/registry-control-replicas-2026-09-14.json) |
| 生产认证切换 | `.70/.71/.66` 各验证匿名 401、只读认证 200、Control digest HEAD 200、只读写入拒绝、仅 loopback backend；新容器 restart=always | [三端切换](evidence/registry-access-cutover-2026-09-15.json) |
| Kubernetes 凭据 | 三份 immutable imagePullSecret；Control 使用 node-pull，两个应用 namespace 各使用本租户 pull；既有 SA 引用保留 | [凭据与滚动更新](evidence/registry-pull-secrets-2026-09-15.json) |
| 真正认证拉取与回退 | 44 项生产检查；两个独有测试镜像仅放 `.71`、`.66`，均以 `.70:5005` 镜像地址通过 Kubernetes/containerd 拉取并运行；先实测错误密码失败 | [实际拉取](evidence/registry-kubernetes-pulls-2026-09-15.json) |
| 后置检查 | 54 注册、53 Ready、407 allocatable GPU；两个 Control 副本 Ready、零次重启，新 Pod 携带只读凭据；无遗留 registry 测试 Pod | [00:53 CST 状态](evidence/registry-postcheck-2026-09-15.json) |
| 持续观测 | Prometheus 实际采到 9/9 个通过的检查、2/2 个在线探针及三端证书到期时间；没有活动仓库告警；Grafana 网关认证访问新看板返回 200 | [持续采集](evidence/registry-observability-2026-09-15.json) |
| 告警与配置 | 9 个 promtool 场景通过；deployment-contract Go 测试、三组 Kustomize 渲染、JSON/Python 语法和文档链接检查通过 | [告警测试](evidence/registry-alert-tests-2026-09-15.log) |

候选和镜像复制来自 09-14 UTC 的既有执行；正式切换发生在 09-15 00:48 CST。
它们是不同执行阶段，不能合并成一次测试。时间字段保留原始 UTC。

实际回退分别发生在 `.70/llmpool01` 的 `llm-api` Pod 和 `.11/server-22`
的 `llm-models` Pod。镜像包含独有 config/digest，首先确认它只存在于指定的
备用入口，再进行错误凭据和正确凭据拉取；成功 Pod 的日志和 imageID 留痕。
这些是 CPU 探针，没有请求 GPU，不代表模型或 DeviceSet 隔离验收。

## 访问和运行边界

`platform-publisher` 可管理全仓库。`node-pull` 只读所有仓库；`llm-api-*`
及 `llm-models-*` 只能读各自前缀，只有 publisher 可以上传，catalog/delete
仍归平台。凭据只通过 TLS 使用。backend 绑定 `127.0.0.1:5007`，由原
IP 的 `:5005` Nginx TLS 入口代理，并验证 backend 证书。
从 `.70` 对三个主机 IP 的 TCP5007 连接均被拒绝，见
[backend 端口检查](evidence/registry-backend-ports-2026-09-15.json)。

生产凭据保存在 `.70` 的 root 私有目录
`/opt/vela-cluster/registry-access-20260914/live/`。每台主机的恢复配置在
`/opt/vela-registry-access/`。这些路径中的 credentials、Docker auth、input
和原 Docker 配置均不纳入仓库。操作入口、发布副本及恢复命令见
[registry-access README](../deploy/registry-access/README.md)。

没有设置集群级通用镜像密码；Kubernetes 按 namespace 使用只读 Secret。
Control 通过既有 `maxUnavailable:1/maxSurge:0` 策略逐个替换 Pod，使用
已有的 disposable scratch 配置；未修改容量、模型盘或 GPU 驱动。
三台主机 boot ID 与 RKE2 invocation ID 均保持不变。

原 registry 容器已停止并保留，restart 关闭；新 gateway/backend 均为
restart=always。没有重启主机、Docker daemon 或 RKE2。已启用自启动是
配置证据；本次没有为了验收而重启受保护主机。

## 持续观测和剩余边界

仓库探针配置为每分钟检查三入口的匿名拒绝、只读认证和当前 Control digest，
并观测 TLS 证书到期时间。两份探针部署只使用 CPU 管理节点；NetworkPolicy
只允许 Prometheus 访问探针，以及探针访问三台仓库的 TCP5005。
部署源见 [registry-probes.yaml](../deploy/observability/registry-probes.yaml)，
看板为 [Vela · Internal Registry](https://10.1.201.70:30443/grafana/d/vela-registry/vela-c2b7-internal-registry)，
六个面板覆盖匿名拒绝、认证、Control 清单、检查覆盖数、证书和请求延迟。
9 个告警场景包括健康、持续失败、短暂失败后恢复、部分/完全缺失和证书到期；
使用主机已安装的 promtool 2.45.3 测试，现场 Prometheus 已加载这三条规则并
采到预期结果。本次没有通过停止生产仓库重复制造故障。

探针镜像首次下载约 10 分钟，主要等待 Docker Hub 的一个 15,503,411 字节层；
完成后两个探针正常运行。对国内备用源的只读取样中，1ms/DaoCloud 在公共
token 握手后的 Blob 请求返回 403，NJU 返回 403。现有 mirror 列表不能保证
所有首次下载都快；已缓存的内部 release 镜像认证/回退不依赖这次公共源。
未修改公共 mirror 配置或为掩盖下载等待删除已有缓存。

三台仓库仍是独立的数据目录，没有后台自动同步。每个未来 release 必须先
运行按 digest 复制和校验，再更新应用镜像及 Control manifest 探针的 digest。
不能将本次 Control 副本证明扩大为所有历史/未来镜像均已有三份。

测试 Pod、错误密码 Secret 和测试 manifest 均已删除，删除后的 GET 为 404；
Distribution 可能留下少量无引用测试 Blob，本次没有做全局 GC。
共享 registry 身份不等于个人 OIDC/SSO；repository 认证也不等于镜像来源签名。
canonical release bundle、实际 GPU/DeviceSet 边界和完整业务链路仍在 R4/R6。
worker NVMe 初始化继续按用户要求暂缓。
