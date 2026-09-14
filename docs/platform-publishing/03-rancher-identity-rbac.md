# Rancher、Keycloak 与角色权限

## 组件关系

Rancher 提供集群、项目、用户和 RBAC 的 UI；Keycloak 提供 OIDC、MFA 和用户组；Kubernetes API Server 执行最终授权；Argo CD 负责应用同步。Rancher 不会自动管理 APISIX 路由，也不能代替 Git 审批。

安装 Rancher 前必须核对其版本对当前 `v1.35.7+rke2r1` 的支持矩阵，并确定 `.70/.71` 的入口、证书、备份和数据库方案。不能因为有 UI 就绕过 Kubernetes RBAC。

## 角色模型

| 角色 | 允许 | 禁止 |
|---|---|---|
| 平台管理员 | 节点、CRD、存储、Argo、Rancher、APISIX | 共享管理员账号 |
| 发布审批人 | 批准生产 PR、触发同步和回滚 | 修改节点和集群 RBAC |
| 应用发布人 | 自有 namespace 的 namespaced 工作负载 | Secret 读取、RBAC、Node、PV、`exec`、其他 namespace |
| 只读/值班 | 受限工作负载和 Grafana 视图 | 写入集群资源 |

Keycloak 组映射到 namespace RoleBinding。每个团队和环境使用独立 namespace；生产 namespace 的写权限最好只给 Argo ServiceAccount，人工发布只保留紧急 break-glass 流程，并要求事后审计。

## 凭据要求

使用短期 OIDC token 或 TokenRequest；禁止长期共享 kubeconfig、ServiceAccount token Secret 和 APISIX Admin key。节点 SSH/sudo 与 Kubernetes RBAC 是两套权限，普通发布者不能拥有前者，否则可以绕过集群授权。

## 观测权限

Grafana 文件夹权限不能自动隔离共享 Prometheus 数据源。租户指标必须通过独立数据源、查询代理或强制标签过滤隔离；日志和 trace 同样要按 tenant/team 标签限制查询范围。
