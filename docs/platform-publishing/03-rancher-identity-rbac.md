# 身份与角色权限

复用 Keycloak，新增独立 `vela-platform` realm 和 `argocd` OIDC client。
保留既有 `platform`、`customer`、`master` realm 和全局 hostname。
Argo 使用 Authorization Code + PKCE S256；个人账号首次改密并设置 TOTP。
内置 Argo admin 和匿名访问关闭，未匹配组的用户默认无权限。

| 角色/组 | 允许 | 无权操作 |
| --- | --- | --- |
| 平台管理员 / platform-admins | Argo 配置与项目管理；受控 SSH/Kubernetes 入口另行管理基础设施 | 不把共享 sudo 账户发给应用人员 |
| llm-api-approvers / llm-models-approvers | 本项目查看、日志、同步、回滚 | 修改 Application 源/路径/目的地、跨项目访问、exec、直接删除资源 |
| llm-api-publishers / llm-models-publishers | 本项目查看和日志、在 Git 提案 | 直接同步、Kubernetes 写入、Secret、RBAC、网关管理 |
| llm-api-viewers / llm-models-viewers | 本项目查看和日志 | 同步、所有写入和跨项目访问 |

平台管理员使用 [create-user.py](../../deploy/application-platform/argocd/create-user.py)
为真实人员创建一个角色组的个人账号。输出是 0600 一次性密码文件，首次登录强制
改密和绑定 TOTP；拒绝覆盖已有用户。可登录账号与操作说明见
[部署说明](../../deploy/application-platform/argocd/README.md)。尚未提供人员名单，
因此不创建假个人账号或共享发布账户。

角色变更/停用后，已签发的 Argo ID token 最长仍有 5 分钟有效期。
先在 Keycloak 停用账号并撤销会话；紧急情况另撤销该组的 Argo 策略。
登录与管理事件保留 30 天，事件详情不保存请求体。

Kubernetes 中只有 Argo controller 持有应用写入 RoleBinding；旧 `*-ci` 账户没有
绑定，签发工具也不再接受 `*-publisher` scope。观察凭据仍是平台签发的短期 token，
不用于替代个人 SSO 审计。运行时 SA 无 API 权限且禁止自动挂载 token。

Rancher 是可选集群管理 UI，本次没有增装。它不是发布权限隔离的必要组件。

Grafana 当前是平台管理员视图。文件夹权限不隔离共享 Prometheus/Loki/Tempo
数据源；不要把该管理员入口宣称为租户隔离观测。应用人员可用 Argo 查看本项目日志。
