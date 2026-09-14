# 应用发布与权限隔离方案

本文回答“新任务如何部署、如何经 APISIX 对外提供、如何让其他人管理”的当前方案。它描述目标流程和已知缺口；不会把尚未安装或验证的管理组件写成现状。

## 当前状态

现有 RKE2 集群版本为 `v1.35.7+rke2r1`，Helm 可用，APISIX 3.18 已运行两副本并通过 NodePort `30080/30443` 提供网关，Prometheus/Grafana 和内部镜像缓存已运行。应用管理层目前没有 Rancher、Argo CD 或 APISIX Ingress Controller，也没有已经绑定到身份提供商的团队 RBAC。APISIX Admin API 仍由平台管理员持有。

因此现在可以由平台管理员用 Helm 发布 namespaced 应用；普通开发者还不能安全地直接发布。`deploy/application-platform/` 是设计草案，未经验证不得 `kubectl apply -k`。

## 推荐的职责分工

| 层 | 推荐组件 | 所有权 |
|---|---|---|
| 打包 | Helm chart，OCI registry | 应用团队；镜像使用 digest |
| 发布记录与回滚 | Argo CD，Git 保护分支 | 平台维护，发布人提 PR |
| 集群/项目/用户界面 | Rancher（先核对对 `v1.35` 的支持矩阵） | 平台管理员 |
| 身份 | 现有 Keycloak，SSO/MFA/群组 | 身份管理员 |
| 外部流量 | APISIX；路由配置由平台审批 | 网关管理员 |
| 指标/日志/追踪 | Prometheus、Grafana、Loki、Tempo | 平台维护；租户视图按标签限制 |

Rancher 和 Argo CD 解决不同问题：Rancher 是用户、项目、集群和 RBAC 的 UI；Argo CD 是 Git 中声明的发布、同步、审计和回滚。二者不能与手工 Helm 同时拥有同一资源的写权限。采用 Argo 后，生产回滚应通过 Git revert/Argo sync 完成，不再对同一 release 直接运行 `helm upgrade`。

## 发布流程

1. 团队提交 chart、镜像 digest、资源请求、探针、PDB、ServiceMonitor 和环境 values。Secret/PKI 只引用外部 Secret 管理流程，不进入 Git。
2. CI 执行 `helm lint`、`helm template`、镜像扫描和 `kubectl apply --dry-run=server`，并检查 Service 必须是 `ClusterIP`、没有 host namespace/hostPath/hostPort/privileged、资源请求落在配额内。
3. 受保护环境仓库通过评审后，Argo CD 的 `Application` 只允许同步到该团队的 namespace；应用 ServiceAccount 只授予运行时需要的权限。
4. 模型推理和模型相关的视频编解码等 CPU 服务使用 GPU worker 节点选择器/污点；聚合 API、鉴权、后台任务和管理 UI 使用 `.70/.71` 的 CPU 管理节点选择器。需要共享的存储只使用已经批准的 Longhorn/MinIO StorageClass。
5. 平台管理员确认 endpoints、健康探针、SSE 流式响应、超时、取消、限流、并发上限、认证插件、上游 TLS/SNI、trace header 和日志脱敏后，才在 APISIX 创建外部路由。
6. 上线后通过 Grafana 查看请求量、错误率、延迟、GPU 利用率、队列/并发、token 用量、SSE 中断率、Pod 重启、PVC/Longhorn/MinIO 状态。原始 prompt、token 和 provider key 不写入日志。

临时验证可以使用下面的 Helm 命令；它只适用于平台管理员，并且不会自动创建 APISIX 路由：

```sh
helm lint ./charts/llm-api
helm template llm-api ./charts/llm-api -n llm-api -f env/validation.yaml >/tmp/llm-api.yaml
kubectl apply --dry-run=server -f /tmp/llm-api.yaml
helm upgrade --install llm-api ./charts/llm-api -n llm-api \
  -f env/validation.yaml --atomic --timeout 15m --history-max 10
```

## 权限模型

至少分为四类身份：

- **平台管理员**：节点、CRD、StorageClass、APISIX Admin、Argo/Rancher 配置；人数最少，使用 MFA 和短期凭据。
- **发布审批人**：批准生产环境 Git PR、触发 Argo 同步和回滚；不能修改节点或集群 RBAC。
- **应用发布人**：只拥有自己的 namespace 内 Deployment/StatefulSet/Job/Service/ConfigMap/PVC 等 namespaced 权限；不能读取 Secret、创建 Role/RoleBinding、修改 NetworkPolicy/Quota、访问 `pods/exec`、Node、PV、Ingress 网关凭据或其他 namespace。
- **只读/值班人员**：只读工作负载和受限的 Grafana 文件夹/数据源。

每个团队和环境使用独立 namespace，例如 `llm-team-a-dev`、`llm-team-a-prod`；生产 namespace 只由 Argo 的受限 ServiceAccount 写入。Quota、LimitRange、Pod Security Admission、ValidatingAdmissionPolicy、NetworkPolicy 和审计日志由平台拥有。APISIX 路由也由平台拥有，应用团队提交路由申请而不接触 Admin Secret。

Namespace 和 RBAC 只能隔离 Kubernetes API。拥有节点 SSH/sudo 或集群管理员 kubeconfig 的人仍可绕过它们；普通发布者不能共享这些凭据。Grafana 文件夹权限也不能自动隔离共享 Prometheus 数据源，租户指标必须在采集/查询层加标签过滤。

## LLM API 的网关约束

APISIX 路由应固定 hostname/path 到指定 ClusterIP Service，启用 OIDC/JWT、限流和并发控制，设置足够的连接/读写超时，关闭代理缓冲或按 SSE 要求配置，限制请求体大小，并保留 trace/span header。模型服务的 provider key 放在 Secret 管理系统，由工作负载以最小权限读取；token 计费和配额在聚合服务中完成。禁止把 APISIX Admin、任意 Lua、任意 upstream 或管理端口开放给租户。

## 还需完成的安装/验证工作

1. 选定并核对 Rancher 对当前 RKE2/Kubernetes 版本的支持矩阵，确定其在 `.70/.71` 的高可用、数据库和入口方式。
2. 安装 Argo CD 到独立的管理 namespace，配置内部镜像、Keycloak OIDC、Git 仓库、项目 allow-list 和生产审批；选择唯一资源所有者。
3. 安装或实现 APISIX Ingress/Gateway API 控制器前，继续由平台通过 Admin API 管理路由；安装后必须限制 hostname/path/upstream/plugin 字段。
4. 将团队身份组映射到 namespace Role/RoleBinding，部署经过测试的 admission/policy 包，并用 `auth can-i`、拒绝用例和审计记录验收。
5. 为第一个 LLM API 建立 chart、values、Secret 引用、路由申请、SSE/鉴权/回滚验收清单，再复制给其他团队。

这些步骤完成前，不能宣称普通用户已经具备安全的自助发布能力。
