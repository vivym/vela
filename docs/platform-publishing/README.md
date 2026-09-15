# 应用发布平台

现有 RKE2 上已经部署应用隔离、Argo CD v3.5.3、Keycloak 发布身份与 APISIX 入口。
管理和发布权限分别维护；应用工作负载由 Argo controller 写入。
用户已有 GitLab，本次不部署或对接第二套 Git 服务。

| 能力 | 当前方式 |
| --- | --- |
| 人员登录 | 复用 Keycloak，独立 vela-platform realm，个人账号、改密、TOTP |
| 发布/审批/只读隔离 | 两个 Argo Project，各自独立组；审批人可同步/回滚 |
| Kubernetes 写入 | Argo controller 仅能写 llm-api / llm-models 的白名单资源 |
| 网关、Secret、RBAC、存储 | 平台管理员管理；应用人员没有修改权限 |
| 可观测 | 复用 Prometheus/Grafana、Alloy/Loki；新增 Argo 看板与告警 |
| GitLab 仓库/CI/MR 保护 | 用户暂缓；Project sourceRepos 默认关闭 |
| Rancher / 自助门户 | 可选，未增装；不影响当前 Argo 发布权限隔离 |

发布入口：<https://10.1.201.70:30443/argocd/>。
监控：<https://10.1.201.70:30443/grafana/d/vela-platform-publishing/>。
浏览器需信任集群私有 CA；.71 同路径可访问，OIDC 的固定 issuer 当前仍依赖 .70。

真实人员账号由平台管理员通过 `create-user.py` 创建，工具和命令见
[部署说明](../../deploy/application-platform/argocd/README.md)。不会为尚未指定的人员
创建共享永久账号。个人账号名单及 GitLab 仓库接入属于使用时输入。

## 文档

1. [Git、Helm 与 Argo CD](01-git-helm-argocd.md)
2. [APISIX 路由管理](02-apisix-route-management.md)
3. [身份和角色](03-rancher-identity-rbac.md)
4. [Kubernetes 隔离](04-kubernetes-isolation.md)
5. [具体业务发布验收](05-llm-api-release-checklist.md)
6. [实施状态](06-implementation-roadmap.md)

本次现场证据见 [2026-09-15 验收记录](../platform-publishing-validation-2026-09-15.md)。
这里验证的是发布平台能力，不等同于具体模型/API 业务或 GitLab 审批门禁已经上线。
