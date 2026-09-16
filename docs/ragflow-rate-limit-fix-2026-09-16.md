# RAGFlow 全站限流修复（2026-09-16）

2026-09-16 15:54（Asia/Shanghai）已从线上 APISIX 路由 `ragflow-lab` 移除
`limit-count` 插件。变更通过 Admin API 动态生效，没有重启 Nginx、APISIX、
RAGFlow 或主机。

## 原因与修复范围

原规则对 `ragflow.marslab.ic` 的 `/*` 应用 `count=120`、`time_window=60`、
`key=remote_addr`、`policy=local`。静态资源和业务 API 共用额度，各 APISIX
实例独立计数。

`.71` 的 Nginx 日志中，2026-09-16 15:45 这一分钟，按 Referer 识别出的
RAGFlow 请求有 100 条、一个客户端来源，其中 45 条返回 429：33 条静态资源、
11 条 API、1 条页面请求。静态资源涉及 41 个不同路径。此处是按自然分钟提取的
日志样本，不等于 APISIX 各自计数窗口内的全部请求数。

两个 APISIX Pod 的日志分别显示 `10.42.1.0` 和 `10.1.201.71` 等来源。
Service 使用 `externalTrafficPolicy: Cluster`，Nginx 虽然覆盖了转发 IP 头，
APISIX 当前只信任 loopback/unix 来源，因此 `remote_addr` 不是终端用户地址。
多人可能共用代理节点的额度，不能将原规则称为每用户限流。

本次只删除 RAGFlow 路由上的 `limit-count`，不设置替代的全站次数限制。
Admin API 回读确认除该插件及更新时间外，路由内容与原值一致，其他六条路由未变。
保留的插件是：

- `client-control`：请求体上限 134217728 字节（128 MiB）。
- `uri-blocker`：阻断 `^/api/v1/admin`，返回 403。
- `prometheus`：网关指标。
- `request-id`：响应包含 `X-Request-ID`。

上游、超时、Host 匹配、请求方法、TLS、应用认证均沿用原配置。
全局插件为 `redirect`、`prometheus`、`opentelemetry`，没有额外的全局 `limit-count`。
同步更新了 [部署路由](../deploy/cluster-platform/ragflow-route.json)、
[入口说明](../deploy/cluster-platform/ragflow-nginx/README.md) 和
[部署方案](ragflow-k8s-deployment-plan-2026-09-15.md)。

## 验证

直接连接每个 APISIX Pod 的 9443，使用 `ragflow.marslab.ic` SNI/Host 和 Vela CA
验证 TLS，分别对故障涉及的 `/chunk/js/index-BoM_I577.js` 发起 130 次 HEAD。
使用固定 Pod IP，避免 NodePort 将请求分散到不同实例而掩盖旧规则；每个连接在请求间
间隔 50 ms，测试只读取静态资源。

| APISIX Pod | 节点 | 请求数 | 耗时 | HTTP 200 | HTTP 429 |
| --- | --- | ---: | ---: | ---: | ---: |
| `apisix-9c6c9c5c8-t66cm` | `llmpool01` | 130 | 6.91 秒 | 130 | 0 |
| `apisix-9c6c9c5c8-vckh6` | `llmpool02` | 130 | 6.85 秒 | 130 | 0 |

两组请求全部带有 `X-Request-ID`，均不再返回旧 `X-RateLimit-Limit` 响应头。
随后在两个 Pod 上分别 GET 首页、错误提示中的 JS 文件及此前被拒绝的
`/chunk/js/compilation-template-util-CoMLFtgn.js`，全部为 200，两个 JS 的 MIME
类型均为 `application/javascript`，两个 Pod 返回的文件哈希分别一致。

从本机绕过代理，经 `.70` 和 `.71` 的标准入口分别复测，使用 MARSLAB CA 校验证书：

| 检查 | `.70` | `.71` |
| --- | --- | --- |
| HTTPS 首页 | 200 | 200 |
| HTTPS 故障提示中的 JS 文件 | 200 | 200 |
| 未登录访问 `/api/v1/users/me` | 401 | 401 |
| `/api/v1/admin` | 403 | 403 |
| HTTP 首页跳转 | 308 | 308 |

上述 HTTPS 请求的 `ssl_verify_result` 均为 0。测试没有重新登录用户、修改业务数据、
提交解析/推理任务，也没有发送超大请求体；128 MiB 上限通过配置不变校验确认。
本次验证覆盖网关限流回归与认证入口保护，不是整个 RAGFlow 业务的负载或容量验收。

浏览器先前失败的动态导入可能仍保留在当前页面中；修复后应刷新页面重新加载模块。
代理直连规则是独立事项，本次未修改本机代理设置。

## 备份与后续

原始响应和变更后的配置保存在 `.70` 的 root 专用目录：

```text
/root/vela-backups/ragflow-remove-site-limit-20260916T075359Z/
  route-before.json
  route-after-put.json
  route-after.json
```

目录权限 0700、文件权限 0600。若确需回滚，平台管理员通过受控 Admin API 将
`route-before.json` 的 `value` 去掉 `id`、`create_time`、`update_time` 后，PUT 到
`/apisix/admin/routes/ragflow-lab`，并重新验证。回滚会恢复本次已确认误伤正常使用的旧限制。

后续分类保护仍待依据实际负载制定：登录尝试频率、解析任务并发/队列、模型服务并发
以及存储配额分别处理。当前没有新增按用户/租户配额，也没有改变可信代理设置。
恢复按终端来源计数前，需要明确 Nginx 信任边界及绕过 Nginx 的直连入口；不能直接
信任任意请求携带的 `X-Forwarded-For` 或用户 ID。
