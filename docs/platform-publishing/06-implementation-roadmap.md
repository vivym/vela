# 平台实施路线图

## 阶段 0：验证期（当前）

由平台管理员使用 Helm 部署第一个 namespaced LLM API，手工通过 APISIX Admin API 创建测试路由。完成 SSE、鉴权、限流、观测和回滚验证。不要把 `deploy/application-platform/` 草案直接应用。

## 阶段 1：Git 与 Argo CD

选定 GitLab、GitHub、Gitea 或 Forgejo；建立 protected branches、PR 审批和 CI。安装 Argo CD 到独立 namespace，接入 Keycloak OIDC，配置 Argo Project 的 namespace/resource allow-list 和内部镜像。选定 Argo 为生产工作负载的唯一写入者。

## 阶段 2：身份和隔离

建立团队/环境 namespace，绑定 Keycloak 组，部署经过拒绝用例验证的 RBAC、Quota、LimitRange、Pod Security、NetworkPolicy 和准入策略。用普通发布者、审批人、只读用户分别验收 `auth can-i`、审计和 Grafana 数据范围。

## 阶段 3：网关声明式管理

先核对 APISIX Ingress Controller 版本与现有 APISIX 3.18 的兼容性，在验证 namespace 安装 CRD 和控制器。限制 hostname、path、upstream、plugin、TLS Secret 和管理端点；通过 Git PR 申请路由，Argo 同步后执行自动 smoke test。

## 阶段 4：Rancher（可选）

核对 Rancher 对当前 Kubernetes 版本的支持矩阵，再决定是否安装。Rancher 用于集群/项目/用户 UI 和运维查看；Argo 继续负责生产应用发布，避免两套系统同时写同一资源。

## 阶段 5：自助门户（可选）

当团队数量增加后，再提供基于模板的应用和路由申请门户。门户只能生成受限 Git PR，不能持有 cluster-admin 或 APISIX Admin credential。所有生产变更仍经过 CI、审批、Argo 和 Grafana 验收。

## 完成定义

只有在 Git 保护分支、Keycloak 群组、Argo 同步、路由字段限制、拒绝用例、SSE smoke test、告警流程和回滚证据全部通过后，才能把“普通用户可自助发布”标记为完成。
