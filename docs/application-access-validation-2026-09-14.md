# 应用发布与管理权限隔离验收 — 2026-09-14

应用侧 Kubernetes 权限边界已部署并通过现场验证，归入 R4。此记录不代表
完整 Vela release、镜像仓库权限或所有节点已达到生产验收条件。
GPU 数据盘仍暂缓；本次没有重启主机、重启 RKE2、重载驱动或申请 GPU。

## 已部署的边界

| 对象 | 当前行为 |
| --- | --- |
| `llm-api` | 非模型 API / 聚合服务只选 `.70/.71` 的 CPU 管理标签。总额度 8 CPU、8Gi memory、8Gi ephemeral，GPU/PVC 配额为 0 |
| `llm-models` | 模型和模型相关 CPU 服务只选普通 `gpu-worker`，本次不向共享 `.66` 新增租户负载。总额度 32 CPU、64Gi memory、64Gi ephemeral、8 GPU，PVC 配额为 0 |
| 每个 namespace 的 `-ci` | 发布应用 workload、ClusterIP Service、ConfigMap/Secret；不能越权到另一 namespace 或控制面 |
| 每个 namespace 的 `-viewer` | 只读应用状态、事件与日志，无 Secret/ConfigMap 读取权限 |
| 每个 namespace 的 `-runtime` | 无 RoleBinding；禁止自动挂载或投射 Kubernetes token |
| `vela-access/cluster-observer` | 查看全局节点、workload、事件、日志与资源指标；无 Secret/ConfigMap 读取或写权限 |
| `vela-application` PriorityClass | value 0，`preemptionPolicy: Never`，由平台管理；租户不能使用更高优先级抢占其他服务 |

上述额度是 Kubernetes resource 配额而非资源预留。本轮不申请 GPU，尚未验证
实际 GPU 可见设备与配额/DeviceSet 一致性；这仍属于 R4/R6 的真实模型发布验收。
真实模型 sizing、持久存储和管理服务容量仍归 R5。
已有 SSH `user` / sudo 持有者仍具有平台管理权，不能把此共享主机身份当成
受限发布身份下发。人员 OIDC 映射尚未配置；当前使用有期限的独立用途 SA 凭据。

35 个资源通过 Kubernetes server dry-run 后应用。两个业务 namespace 均使用
PSA `restricted` v1.35，三个附加 ValidatingAdmissionPolicy 及其 Deny binding
覆盖 Pod/ephemeralcontainers、Service、Secret；CEL typeChecking 无 warning。
Controller 创建的 Pod 同样受限制。Deployment 可先被接受，非法模板会在创建
Pod 时被拒绝，故发布必须检查 rollout/ReplicaSet 事件。

网络策略默认拒绝 ingress/egress，允许 namespace 内依赖、两个业务 namespace
间 TCP 8000/8080/8443、APISIX 到这些 API 端口、Prometheus 到 metrics 9090、
CoreDNS TCP/UDP 53，以及 OTel Collector 4317/4318。API server、APISIX Admin
和主机 SSH 没有出站允许规则。指标采集配置由平台持有，租户不能改监控 CR。

## 现场验收

- **106 项 API / 准入检查通过**：使用短期 SA token 发起真实请求，覆盖发布/
  只读/runtime 身份，跨 namespace Secret、Nodes、RBAC、SA/TokenRequest、
  NetworkPolicy、监控配置、直接 Pod、NodePort/LB/externalIPs/ExternalName、
  长期 token Secret、调度绕行、Windows、host namespaces/hostPath、投射 token，
  以及普通/init 容器的特权、权限提升、capabilities、hostPort、非 digest 镜像。
- **29 项运行 / 网络检查通过**：真实 publisher 创建两份 Deployment；CPU 探针
  在 `llmpool02`，模型侧 CPU 探针在 `.11/server-22`，不请求 GPU。检查 runtime
  token 不存在、真实 ephemeralcontainers 准入、无关 namespace ingress 拒绝、
  DNS/业务互通/OTLP 放行、管理接口阻断，以及 APISIX 到 Pod 的 HTTP 请求。
  Prometheus 实际采到两个 Pod 的 `boundary_ok=1`，不是只检查端口连通。
- **签发工具通过双管理入口验证**：`.70` 签发 5 类身份，在 `.70/.71:6443`
  各验证允许读取和控制 Secret 拒绝；`.71` 安装同一 SHA256 的签发工具，
  签发 cluster observer 并通过两个端点读取 54 个注册 Node。
- 临时 Deployment、Pod、测试 kubeconfig 已删除；后置扫描无 `boundary-*` Pod，
  证据目录无残留 kubeconfig。没有持久 token Secret，短期 token 在内存中使用并
  按服务器记录的 expiry 失效。

API 子集与运行/网络测试来自两个明示的 run，不拼成一次执行。早期探针工具
在 APISIX/Prometheus 镜像中不可用；最终改为 APISIX 内置 cosocket 和 Prometheus
实际 query 验收。临时 HTTP 指标端点补全了 Prometheus 3 所需 Content-Type。
这些修正没有放宽准入规则、网络策略或 Prometheus 解析要求。

## 使用与证据

`.70/.71` 均已安装 `/usr/local/sbin/vela-issue-access`，root-owned 0755。工具
允许 600–3600 秒 TokenRequest，输出文件 0600，拒绝覆盖已有文件；stdout
只含 scope、路径、expiry。示例：

```sh
sudo vela-issue-access llm-api-publisher --seconds 1800 \
  --output /root/llm-api-publisher.kubeconfig
sudo vela-issue-access cluster-observer --seconds 1800 \
  --output /root/cluster-observer.kubeconfig
```

通过安全渠道交付给对应使用人，随后删除管理节点的临时副本。文件含显式的
`.70/.71` context 与 CA，不自动重放失败写入。不提供额外 cluster-admin 凭据。

- [部署包与详细使用说明](../deploy/application-platform/README.md)
- [API/准入证据](evidence/application-api-boundary-2026-09-14.json)
- [运行/网络/实际采集证据](evidence/application-network-boundary-2026-09-14.json)
- [主节点签发与双入口验证](evidence/access-issuer-2026-09-14.json)
- [另一管理节点签发验证](evidence/access-issuer-peer-2026-09-14.json)
- [live binding/PSA 与清理后检查](evidence/application-access-live-2026-09-14.json)
- [可重放验证器](../hack/verify-application-boundary.py)

后续 registry 生产入口权限及认证回退已经完成，见
[仓库专项验收](registry-access-validation-2026-09-15.md)。canonical release bundle、应用 OTLP 与真实模型
链路仍未在本记录中关闭，继续按 [R1–R6 唯一清单](cluster-production-readiness-2026-09-14.md) 执行。
