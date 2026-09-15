# Argo CD 发布平台

固定版本 `v3.5.3`，上游支持当前 Kubernetes 1.35。`render.py` 校验上游
SHA256、剔除暂不用的组件并固定镜像 digest，生成 `install.yaml`。
Keycloak、APISIX 和 Prometheus/Grafana 复用现有部署。没有第二套 GitLab。

## 组件和权限

| 组件 | 副本/位置 | 权限 |
| --- | --- | --- |
| Argo server | 2，分别在 .70/.71 | Argo 配置与目标应用读取；不写业务工作负载 |
| repo-server | 2，分别在 .70/.71 | 渲染 Git/Helm；不挂载 Kubernetes token |
| application-controller | 1，CPU 管理节点 | 仅 llm-api/llm-models 的白名单资源写入 |
| Redis | 1，CPU 管理节点 | 无持久业务数据、无 Kubernetes token；使用随机密码 |

Redis/controller 故障时由 Kubernetes 重建，期间暂停同步；已运行的应用不依赖
它们处理请求。没有 Argo ClusterRoleBinding、cluster-admin token 或持久 SA token。
`in-cluster-applications` Secret 只有服务器、namespace 和空连接配置，不含 bearerToken。

`default` Project 拒绝全部资源/目的地；另外两个 Project 限定各自 namespace，
允许 Deployment/StatefulSet/ReplicaSet、Job/CronJob、ConfigMap、ClusterIP Service、HPA/PDB。
Secret、RBAC、存储、Namespace、监控和网关对象由平台管理。

## 身份

- `platform-admins`：Argo 平台管理。Kubernetes/SSH 平台管理仍使用现有受控管理员入口。
- `llm-api-publishers` / `llm-models-publishers`：只看自己的应用、日志，在 Git 提案。
- `*-approvers`：另可同步和回滚本项目应用，不能改 Application 源、路径、目的地或策略。
- `*-viewers`：只看自己的应用和日志。

默认角色无权限，匿名访问和内置 admin 关闭；无 exec、manifest override、直接资源删除权限。
新人员在 `vela-platform` realm 使用个人账号、首次改密和 TOTP。
没有把既有 `platform`/`customer` realm 或共享 SSH 账户改造成普通发布账号。

入口：`https://10.1.201.70:30443/argocd/`，.71 同路径也提供 UI/API。
OIDC issuer 目前以 .70 为固定地址；.70 整机失联时 .71 的新登录也依赖其恢复。
正式域名/VIP 接入属于入口地址迁移，需要同步调整 issuer、client 和 TLS。
浏览器需信任已有网关私有 CA；禁止通过关闭 TLS 验证解决证书问题。

管理员在 .70 创建真实人员账号：

```sh
sudo python3 /opt/vela-cluster/platform-publishing-20260915/operator-tools/create-user.py \
  --username alice --group llm-api-publishers --output /root/alice-platform.json
```

这里的 `alice` 是示例，命令要求使用实际个人标识。工具拒绝覆盖用户及凭据文件，
仅输出文件路径；凭据文件为 0600，首次登录要求改密和绑定 TOTP。
审批人使用 `llm-api-approvers` 等组；不应让同一个人同时承担自己的发布提案和审批职责。
用户停用/移组后，已有 Argo ID token 最长仍有 5 分钟有效期；先禁用 Keycloak
账号并撤销其会话，必要时撤销该组的 Argo 策略。不要将 SSH/sudo 给应用人员。

## 部署与恢复

`install.yaml` 独立于上层 namespace/RBAC 包，CRD 必须 server-side apply。
`install.py` 自动建立 argocd namespace 和 CRD，并生成 `argocd-redis` 随机密码 Secret。
应用主清单后运行 `configure-sso.py --run-directory /root/platform-sso`；该工具从
现有 Kubernetes Secret 读取管理员凭据，在内存中完成 SSO/APISIX 配置，
创建 CA ConfigMap，随后 Argo server 可以启动。重新渲染主清单不会覆盖动态 OIDC Secret。

```sh
sudo python3 deploy/application-platform/argocd/install.py
```

运行本目录工具需要 root 权限和已有管理员 KUBECONFIG。备份沿用集群 etcd 快照、
Keycloak 所在 PostgreSQL 备份；repository 凭据、argocd-secret、Redis 密码和 OIDC client
Secret 都不入 Git。恢复验证不能仅靠这些组件 Running 判定。

## GitLab 暂缓边界

两个 Project 的 `sourceRepos` 当前为空，repo-server 默认不能访问外部仓库。
接入已有 GitLab 时由平台批准确切仓库、网络目标和只读凭据，并创建固定
`repoURL/path/targetRevision/destination` 的 Application。AppProject 不限制 Git path；
路径边界由不可被团队修改的 Application 维持。

审批人可以选仓库里的 revision 同步；Argo sync 权限本身不证明该 revision 已经过
GitLab MR 审批。本次不会冒称保护分支、CI 扫描、生产提交批准门禁已经完成。
验证脚本的短时只读 Git fixture 在结束时关闭，不承担生产 Git 服务。

## 验证与监控

`hack/verify-platform-publishing.py` 覆盖真实 PKCE+TOTP 浏览器登录、三类租户身份、
跨租户读写拒绝、正常同步、失败 Deployment、回滚、Secret 同步拒绝和清理。
`hack/verify-application-secrets.py` 覆盖容器/init/volume/projected Secret 引用白名单；
完整 Argo 验证另覆盖 ephemeral container。

Grafana：`https://10.1.201.70:30443/grafana/d/vela-platform-publishing/`
监控包含应用健康/同步状态、同步失败、Argo 组件、内存和告警。
登录/管理事件在独立 Keycloak realm 保留 30 天；Argo/Kubernetes 操作日志沿用现有
Alloy/Loki 与 Kubernetes 审计链路。平台 Grafana 数据源仍是管理员视图，不能把它
当作租户隔离的数据查询入口。外部通知继续保持待配置。
