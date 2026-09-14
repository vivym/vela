# APISIX 路由管理与审批

## 当前方式

现有 APISIX 3.18 通过 `30080/30443` 提供网关，Admin Service 为 ClusterIP。当前没有 APISIX Ingress Controller 或 `ApisixRoute` CRD，所以路由由平台管理员使用 Admin API 管理。Admin Secret 不得发给应用团队，也不得放入 Git 或 CI 日志。

## 手工路由操作

管理员从受控主机建立短时 `port-forward`，从 Secret 注入凭据，创建并验证 upstream、route、认证、限流和超时配置。命令中的 token 使用环境变量占位，不能记录真实值：

```sh
kubectl -n apisix port-forward svc/apisix-admin 9180:9180
curl -fsS -H "X-API-KEY: ${APISIX_ADMIN_TOKEN}" \
  http://127.0.0.1:9180/apisix/admin/routes
```

实际变更前先确认目标 Service 是 `ClusterIP`、endpoints Ready、端口和协议正确；完成后验证 HTTP/HTTPS、鉴权拒绝、限流、SSE、超时和 trace header。

## 推荐声明式方式

安装并验证 APISIX Ingress Controller 后，应用路由可以作为 `ApisixRoute` 或 Gateway API 资源进入 Git，由 Argo CD 同步。平台必须限制：hostname 后缀、path 前缀、upstream namespace、插件白名单、请求体大小、超时、TLS Secret 和管理端点。应用团队不能创建任意 upstream、Lua 插件、NodePort 或 Admin 路由。

## 路由申请字段

申请至少包含：外部 hostname、path、Service 名称和端口、认证方式、限流/并发上限、连接/读写超时、最大请求体、SSE 要求、上游 TLS/SNI、owner、回滚 revision 和 Grafana dashboard。

## 审批顺序

应用团队提交申请；CI 校验字段；业务负责人确认 API 合约；平台/安全人员确认认证、限流和数据脱敏；发布审批人批准生产分支；Argo 同步；平台执行网关 smoke test。任何路由不得直接指向管理服务、APISIX Admin、Grafana 或 Kubernetes API。

## Dashboard 的定位

APISIX Dashboard 可作为管理员查看和操作界面，但它不是租户隔离方案。若启用，必须放在管理网络，接入 SSO/MFA，限制管理员人数，并保留 Admin API 审计。普通应用发布者继续使用 Git 申请路由。
