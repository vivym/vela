# 网关与 Grafana 现场修复 — 2026-09-14

本次已完成 Grafana 子路径修复、APISIX 网络策略可重放修复、内网 TLS 与
每副本限流验证，以及证书签发/同步自动化。没有操作 GPU 数据盘或重启主机。

## Grafana 和路由

原登录页虽返回 200，HTML 的 base 却为 `/`，导致浏览器请求的静态资源
绕开 `/grafana/*` 路由。现在配置 `root_url=/grafana/` 和
`serve_from_sub_path=false`，由 APISIX 去掉转发前缀。
Helm chart 91.1.0 的渲染差异仅有 Grafana ConfigMap 和 Deployment；
现存 PVC UID 和 PV 绑定保持不变。

`.70/.71:30080` 均验证了登录页、全部 9 个引用资源（JS/CSS/图标）返回 200，
认证后的用户接口、31 个 dashboard 列表可读，Prometheus/Loki/Tempo
datasource health 均为 OK。这是 HTTP/API 资源验证，不是浏览器交互验收。
[原始证据](evidence/gateway-grafana-path-validation-2026-09-14.json)。

APISIX chart 2.17.0 不支持 `podLabels`。现场 NetworkPolicy 改为匹配
chart 原生的 `app.kubernetes.io/name=apisix` 和
`app.kubernetes.io/instance=apisix`，同时保留 namespace 的
`vela.ai/network-role=api-ingress` 条件和仅 TCP8080 的许可。
该变化位于 MarsLab overlay；通用 base 的角色标签契约不变。

## TLS 与限流

两个 APISIX Pod 分别接收 125 次未认证 API GET，在 0.254/0.113 秒内分别
得到 119/120 个 401 和 6/5 个 429。已有探测会消耗同来源计数，所以第一个
Pod 在本组第 120 次请求前已达限额。结果证明当前每 Pod、每来源 IP 的
120 请求/60 秒限流生效，不是全局或租户配额。
[证据](evidence/gateway-tls-limit-validation-2026-09-14.json) 中的证书属于
替换前的临时证书，其限流结果仍有效。

原临时自签名叶证书将于 2026-10-13 到期，且没有自动续期。本次部署
`deploy/cluster-platform/gateway-tls`：私有 CA、90 天叶证书、提前 30 天续期，
以及每 5 分钟同步唯一 APISIX SSL 对象的 CronJob。新证书有效期至
2026-12-13T13:33:34Z，计划续期时间 2026-11-13T13:33:34Z。
首次 Job 和 13:35Z 的定时 Job 均成功，未重启 APISIX。
自然到期续期尚未发生，本次证明的是签发、首次同步和定时同步执行。

两个具体 Pod 的 9443 端口和 `.70/.71:30443` 都使用 TLS1.3，证书指纹与
cert-manager Secret 一致。验证使用私有 CA，核验主机名/SNI `apisix-gateway`，
没有跳过 TLS 校验。[最终证据](evidence/gateway-managed-tls-validation-2026-09-14.json)。
客户端可安装[CA 证书（不含私钥）](evidence/vela-gateway-ca.crt)并将
`apisix-gateway` 映射到任一 CPU 管理地址。该 CA 不具备公共浏览器默认信任；
不声称无 SNI 的裸 IP HTTPS 已可用。

## 可观测性和边界

证书到期、Certificate NotReady/指标消失、CronJob 同步过期/未成功三类告警
已经部署，9 个 promtool 场景通过。两个 cert-manager 指标端点 `up=1`，
证书 Ready=1，CronJob 最近成功时间可查询，三条规则 health=ok、state=inactive。
[现场证据](evidence/gateway-certificate-renewal-2026-09-14.json)及
[规则测试](evidence/gateway-certificate-alert-tests-2026-09-14.log)。

后续已完成 HTTP→HTTPS：APISIX chart 2.17.0 配置 `ssl.fallbackSNI=apisix-gateway`，
使不发送 SNI 的 IP 客户端也能取得现有包含 `.70/.71` SAN 的证书。server dry-run
确认只改变 APISIX ConfigMap/Deployment，随后 Helm 滚动发布成功。

全局 `redirect` 插件仅在真实 `scheme=http` 时执行，统一返回 308 到同主机
30443，保留方法、路径和查询参数。使用真实 scheme 过滤，避免依赖可被客户端
伪造的 `X-Forwarded-Proto`。原 Prometheus/OpenTelemetry 插件完整保留，重放
`apisix-observability-policy` Job 也保留该 HTTPS 策略。

两入口裸 IP HTTPS 均通过私有 CA 及主机名校验，无 SNI、无跳过校验；Grafana
登录页保持 200、正确子路径，未认证 API 保持 401。HTTP 的 GET/HEAD/POST 与
伪造转发协议头组合共 12 个检查均为 308，Grafana 跳转也通过，HTTPS 无跳转循环。
当前可用入口为 `https://10.1.201.70:30443/grafana/` 和
`https://10.1.201.71:30443/grafana/`；客户端仍须信任本项目提供的私有 CA。
见 [HTTPS 策略证据](evidence/gateway-https-policy-2026-09-14.json)。

用户认证成功的业务任务、全局/租户限流、应用跨服务 trace、真实业务 SLI/SLO
与 Launch Receipt 仍属于 R6。本次不以网关探测替代这些验收。
