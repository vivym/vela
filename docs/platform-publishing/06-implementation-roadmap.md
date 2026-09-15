# 实施状态

这份表记录固定范围，避免把可选组件不断加成新的发布前置条件。

| 范围 | 状态与边界 |
| --- | --- |
| 应用 namespace/RBAC/Quota/准入/网络 | 已部署；CPU 管理和 GPU 应用调度分开 |
| Secret 权限 | 禁止发布角色读写，运行时引用必须在平台白名单 |
| Argo CD | 已部署 v3.5.3；server/repo 双副本，controller 只写两个应用 namespace |
| 个人身份和角色 | Keycloak PKCE/TOTP、三类团队角色与管理员隔离；人员创建工具已提供 |
| 发布与回滚 | 临时 Git fixture 实际验证正常同步、失败升级和恢复 |
| APISIX | 已提供 Argo/登录入口；业务路由仍由平台管理员审批维护 |
| 监控 | 新增 Argo ServiceMonitor、Grafana 看板和健康/同步告警 |
| 既有 GitLab 对接 | 用户暂缓；不重复安装；sourceRepos 暂时关闭 |
| APISIX Ingress Controller | 本次不增加第二个路由写入者，沿用管理员 Admin API 模式 |
| Rancher / 自助门户 | 可选、未安装 |

使用阶段只需要按真实人员创建账号，以及在恢复 GitLab 对接时提供确切仓库和应用
配置。这些都不能通过创建假人员或假仓库来“完成”。GitLab CI/MR 审批门禁、真实
模型/API 的业务验收未被本次测试代替。

本项归入现有 [R4 发布边界](../cluster-production-readiness-2026-09-14.md)。
其余 R1–R6 的存储容量、离线节点、业务遥测和 canonical release 问题继续遵守
原清单，不因本次发布平台工作自动关闭，也不增加新的强制平台组件。
