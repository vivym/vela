# 发布平台现场验收 — 2026-09-15

本次完成 GitLab 之外的应用发布与管理权限隔离。复用现有 Keycloak、APISIX、
Prometheus/Grafana 和日志链路，不重复部署 GitLab。最后专项检查为
2026-09-15 01:58 CST；证据 JSON 使用 UTC 时间。

09-15 07:28 CST [只读权限复核](evidence/platform-publishing-readonly-followup-2026-09-15.json)
确认 Argo 无 ClusterRoleBinding，两个应用 namespace 的 controller 可 patch
Deployment，但不能读取 Secret 或创建 Role/NetworkPolicy；两个旧 CI 身份
仍不能写 Deployment。组策略与仓库默认关闭状态保持。
同期 [基础设施快照](evidence/cluster-readiness-before-stage-tracing-2026-09-15.json)
显示 Argo 六个 Pod Ready。该复核未重复创建身份或发布测试工作负载。

## 已部署

| 项目 | 现场结果 |
| --- | --- |
| Argo CD | v3.5.3，全部 6 个 Pod Ready，均在 .70/.71 CPU 管理节点 |
| UI/repo-server | 各 2 副本，分别落两台 CPU 节点；Redis/controller 各 1 副本 |
| Kubernetes 权限 | 没有 Argo ClusterRoleBinding；controller 仅绑定两个应用 namespace 的应用写入 Role |
| 旧 CI | 两个 application-publisher 旧 RoleBinding 已删除；*-ci 无应用写权限；.70/.71 签发工具不再提供 publisher scope |
| 人员身份 | 复用 Keycloak，新 vela-platform realm；生产 Argo client 使用 PKCE S256，禁用 direct grants，强制个人账号改密/TOTP |
| Argo 角色 | 两个项目分别有 publisher/approver/viewer；审批人可同步和回滚，普通发布者/只读用户不可同步 |
| Secret | 目标 namespace Secret API 读写禁止；运行时引用需要平台维护的 namespace 白名单 |
| 网关 | Argo 与指定 realm 登录走现有 APISIX；Admin 和其他 realm 路径不公开 |
| 观测 | 5/5 Argo scrape targets up，7-panel Grafana 看板已 provision，4 条告警规则健康，Loki 查到 Argo 日志 |

发布入口：<https://10.1.201.70:30443/argocd/>。
监控：<https://10.1.201.70:30443/grafana/d/vela-platform-publishing/>。
.71 同路径也提供服务；浏览器需信任现有网关私有 CA。

## 验收证据

| 验证 | 结果 | 文件 |
| --- | --- | --- |
| Argo 真实身份/发布/回滚 | 91 项通过 | [publishing](evidence/platform-publishing-2026-09-15.json) |
| 切换旧 CI 写入者 | 6 项身份/namespace 写权限符合预期 | [cutover](evidence/platform-publishing-cutover-2026-09-15.json) |
| 切换后 Kubernetes API/准入 | 112 项通过 | [API boundary](evidence/platform-publishing-api-boundary-2026-09-15.json) |
| Secret 引用白名单 | 36 项通过；ephemeral 正反例另包含在 Argo 91 项中 | [Secret references](evidence/application-secret-references-2026-09-15.json) |
| OIDC/组/入口配置 | issuer、PKCE、独立 realm、既有 realm 保留 | [SSO](evidence/platform-publishing-sso-2026-09-15.json) |
| 实际监控与日志 | 5 个 targets、4 条健康规则、看板、Loki | [observability](evidence/platform-publishing-observability-2026-09-15.json) |
| 告警失败/缺失/恢复 | 12 个 promtool 场景通过 | [alert tests](evidence/platform-publishing-alert-tests-2026-09-15.log) |
| 最终清理、入口、账号工具 | 28 项通过 | [postcheck](evidence/platform-publishing-postcheck-2026-09-15.json) |

Argo 检查使用 7 个临时 Keycloak 身份，完整经过生产 client 的浏览器
Authorization Code + PKCE 和 TOTP 回调，随后调用真实 Argo API。验证了本项目
可见、跨项目 403、发布者/只读用户不能同步、审批人不能修改源/目的地或直接删除
工作负载。同步测试实际创建 CPU Pod：llm-api 在 llmpool02，llm-models 在
server-22，零 GPU 申请、零 PVC。

测试使用的 revision 属于临时 fixture，具体 SHA 见 JSON 的 `revision`。
验收先同步健康版本，再同步主动退出的 Deployment，
观察 Degraded，最后由审批人回滚，恢复原 revision 和 Healthy。没有用 API
接受请求或一次 Pod Running 代替这个过程。

Secret fixture 同步进入 Error，真实原因是 controller 在读取目标 Secret live
state 时被 Kubernetes RBAC 拒绝，尚未到 AppProject 资源校验阶段；Secret
未被创建。AppProject 同时配置资源白名单，但不能把此次更早的 RBAC 拒绝说成
单独证明了 Project validator 的执行顺序。

原 SSO 登录回调可成功，但 API token 校验没有信任私有 CA。已同时配置 OIDC
rootCA 和 Argo server 的系统 CA 信任目录，再完成上述全部身份验证。未关闭
issuer 的证书校验；APISIX 到 Argo 的内部 HTTP 由 NetworkPolicy 限制来源。

`go test ./internal/deploymentcontract -count=1` 通过；Python 语法检查和定向
`git diff --check` 通过。未提交或推送，也未清理既有无关修改。

## 清理与使用

所有临时用户、Application、Deployment、Service、ConfigMap、Git 服务进程和
测试网络放行规则均已移除；两个 Project 仓库白名单恢复为空，Secret 引用
annotation 恢复原状。个人账号工具另使用一次临时账号验证创建、改密/TOTP
要求、0600 凭据输出及拒绝覆盖，随后清理账号和凭据文件。

创建真实人员账号和部署/恢复操作见
[Argo 部署说明](../deploy/application-platform/argocd/README.md)。
.70 的受控工具目录为 `/opt/vela-cluster/platform-publishing-20260915/operator-tools/`。
临时验证源代码/日志留在 root 私有证据目录，未部署为长期 Git 服务。

## 明确边界

- GitLab 已有且用户暂缓对接；没有配置真实 repoURL、保护分支、CI 或 MR 审批。
  Argo 的 sync 权限允许选择仓库 revision，不自动证明该 revision 已获 GitLab 批准。
- 未给出真实人员名单，未创建共享永久发布账号；个人开户工具和组模型已经可用。
- OIDC issuer 目前固定 .70；即使 .71 UI 可访问，.70 整机故障仍会影响新登录。
  当前不宣称已经实现独立于 .70 的身份入口容灾。
- Redis/controller 单副本由 Kubernetes 重建，恢复期间暂停同步；不是发布控制器零中断 HA。
- 运行时 Secret 白名单把 Secret 交给指定团队的代码执行范围。代码拥有者能读取
  已批准的 runtime Secret；API 禁读无法对此保密。
- Grafana 是平台管理员视图，共享数据源尚未成为租户隔离查询服务；外部通知继续暂缓。
- 受保护主机未重启，未操作 GPU 驱动或暂缓的 worker 数据盘；原 Compose 监控保留。
- 本次归入 R4；canonical release、真实业务/SSE/OTLP、原 R1/R3/R5 等未完成项
  继续按固定清单记录，不用此次权限验收替代。
