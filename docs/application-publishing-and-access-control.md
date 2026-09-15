# 应用发布与权限隔离

当前实现以 [platform-publishing 文档集](platform-publishing/README.md) 为准。
现场证据见 [2026-09-15 发布平台验收](platform-publishing-validation-2026-09-15.md)。

Argo CD v3.5.3 已部署到 .70/.71，复用现有 Keycloak 和 APISIX。
`llm-api` 的非模型服务落 CPU 管理节点；`llm-models` 的模型及相关 CPU 服务落
普通 GPU worker。应用人员通过个人 OIDC/MFA 身份访问本项目；发布者提案，
审批人同步/回滚，只读用户查看。只有 Argo controller 持有应用 namespace 的
工作负载写权限。平台管理员负责 Secret、RBAC、网络、网关、存储和监控配置。

两层限制一起生效：Argo Project/角色约束人能同步什么，Kubernetes RBAC、准入、
配额和网络约束控制器实际能创建什么。运行时 Secret 引用需要平台批准，既有
registry pull Secret 不可被挂载读取。拥有 SSH/sudo 的人仍是平台管理员。

GitLab 已有，按用户要求暂缓对接；不重复部署 Git 服务。当前 Project 的仓库
白名单为空，待接入真实仓库后才能发布真实应用。Argo 的同步权限本身不构成
GitLab MR 审批门禁。Rancher、APISIX Ingress Controller 和自助门户没有增装；
业务路由沿用现有平台管理员审批和 Admin API 管理方式。

Grafana 的发布平台看板和 Argo 日志已接入现有可观测体系；Grafana 仍为平台
管理员视图，不向租户宣称共享数据源已经隔离。具体 API 的 SSE、鉴权、限流、
业务遥测、数据库/PVC 迁移仍按 [业务验收清单](platform-publishing/05-llm-api-release-checklist.md) 验证。
