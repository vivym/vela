# Kubernetes 隔离与准入

`deploy/application-platform` 是已实施并通过现场拒绝用例验证的包。
两个应用 namespace 组合使用 RBAC、Quota、LimitRange、Pod Security restricted、
NetworkPolicy 和 ValidatingAdmissionPolicy。namespace 不提供内核级隔离；
已有 SSH/sudo/cluster-admin 仍属于平台信任边界。

## 工作负载

- llm-api 选择 CPU 管理节点 .70/.71；llm-models 选择普通 gpu-worker，排除共享 .66。
- 禁止 hostNetwork/hostPID/hostIPC/hostPath/hostPort、privileged、提权 capability、
  自选节点、任意调度器和自动挂载 API token；覆盖普通、init 和 ephemeral 容器。
- 只允许指定 runtime ServiceAccount、镜像 digest、受限 toleration 和非抢占 PriorityClass。
- 只允许带 selector 的 ClusterIP Service；拒绝 NodePort、LoadBalancer、externalIPs、ExternalName。
- 初始配额：llm-api 8 CPU / 8 GiB；llm-models 32 CPU / 64 GiB / 8 GPU。
  这些是上限，不是资源预留。当前两者 PVC/storage 配额为零，数据盘仍按用户要求暂缓。

Deployment 可能被接收，但其违规 Pod 在后续创建时被拒绝。发布必须检查完整
rollout 和 ReplicaSet events，不能只看 API 提交成功。

## Secret

应用人员及 Argo 目标写入角色都不能通过 Kubernetes Secret API 读写 Secret。
平台用 namespace annotation `vela.ai/runtime-secret-names` 批准允许交给应用的
Secret 名称，格式为逗号分隔、无空格；缺失时全部拒绝。覆盖 env、envFrom、
普通/init/ephemeral 容器和 secret/projected volumes。

`imagePullSecrets` 可以用于拉镜像，但其中凭据默认不允许挂载进容器。
应用代码执行者可以读取平台已批准交给该工作负载的 Secret；禁止 Secret API
读取不能对应用代码拥有者隐藏运行时明文凭据。因此同 namespace 内的批准列表
是同一团队的信任范围，不能混放需要相互保密的团队。

## 网络和管理

默认拒绝 ingress/egress；同 namespace 通信、限定端口的 API/模型互访、APISIX
入站、DNS、Prometheus 和 OTLP 按已有规则放行。应用 Pod 无法访问 Kubernetes API、
APISIX Admin、主机 SSH 或任意外部 provider。依赖新增由平台精确放行。

Argo repo-server 不挂载 API token，默认无外部仓库网络权限。controller/server
仅可访问自身 Redis、repo-server、Kubernetes API 和必要的 SSO 网关。
Secret、Quota、NetworkPolicy、节点、存储、监控和网关对象均不在团队写权限内。

现场验证脚本和证据见 [验收记录](../platform-publishing-validation-2026-09-15.md)。
