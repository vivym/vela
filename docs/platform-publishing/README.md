# 应用平台文档集

这组文档定义 LLM API 和其他应用在现有 RKE2 集群中的发布、网关、权限和运维边界。

## 文档状态

当前已验证的基础设施是 RKE2、Helm、APISIX 3.18、Prometheus/Grafana、Loki、Tempo、Longhorn、MinIO 和内部镜像缓存。Argo CD、Rancher、APISIX Ingress Controller 以及团队身份映射尚未安装或验收。文档中的“目标”“待安装”“示例”不能当作已部署能力。

## 阅读顺序

1. [Git、Helm 与 Argo CD 发布流程](01-git-helm-argocd.md)
2. [APISIX 路由管理与审批](02-apisix-route-management.md)
3. [Rancher、Keycloak 与角色权限](03-rancher-identity-rbac.md)
4. [Kubernetes 隔离与准入策略](04-kubernetes-isolation.md)
5. [LLM API 发布验收清单](05-llm-api-release-checklist.md)
6. [分阶段实施路线图](06-implementation-roadmap.md)

现有概览见 [application-publishing-and-access-control.md](../application-publishing-and-access-control.md)。`deploy/application-platform/` 仍是未验证设计草案，禁止直接应用。
