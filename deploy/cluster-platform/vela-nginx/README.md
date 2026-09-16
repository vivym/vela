# Vela 标准 HTTPS 入口

`.70` 和 `.71` 的宿主机 Nginx 提供 `https://vela.marslab.ic/api`，经本机
`127.0.0.1:30443` 的 APISIX 转发到 Vela。签名产物下载使用同一域名的
`/vela-artifacts/artifacts/…`。公开入口不提供 Web 首页，访问 `/` 返回 404。

## DNS 与证书

由内网 DNS 管理员配置，建议初始 TTL 为 60 秒：

```dns
vela.marslab.ic. 60 IN A 10.1.201.70
vela.marslab.ic. 60 IN A 10.1.201.71
```

DNS 轮询没有健康检查，连接失败时的重试取决于客户端；既有连接也不会自动迁移。

客户端使用 [MARSLAB Root CA](../../../docs/evidence/marslab-root-ca.crt)。Nginx
复用 RAGFlow 的 `*.marslab.ic` 叶证书及站点私钥，路径为：

```text
/etc/nginx/ssl/ragflow.marslab.ic/wildcard.marslab.ic.crt
/etc/nginx/ssl/ragflow.marslab.ic/wildcard.marslab.ic.key
```

这是泛域名站点证书，不是 CA 私钥。站点私钥不复制到仓库或客户端。
当前叶证书有效期 2026-08-07 至 2044-10-21；macOS 原生验证已报告有效期过长。
显式 CA 的 OpenSSL/Python/curl 验证成功不代表所有浏览器可接受。需要签发方使用
同一 MARSLAB CA 重新签发符合客户端期限要求的叶证书；本次复用现有证书。
此外公开根证书缺少 `Key Usage`，本机 Python 3.14.7 默认严格验证报
`CA cert does not include key usage extension`。需由 CA 签发方修正根/链证书并
更新客户端信任，不能以关闭校验作为解决办法。

Nginx 到 APISIX 独立校验 Vela CA，使用证书已有的 SNI
`apisix-gateway.apisix.svc.cluster.local`。HTTP Host 保留公开域名，供路由和
S3 SigV4 校验；不能把它替换成上游 SNI。上游 CA 路径为：

```text
/etc/nginx/ssl/ragflow.marslab.ic/current/ca.crt
```

该路径继续由现有 `ragflow-nginx-cert-sync.timer` 维护。Vela 入口依赖此定时器和
共享证书目录，删除 RAGFlow 部署时不能同时删除这些被 Vela 使用的文件。

## 安装

复制本目录和公开 CA 到管理节点，在已有 RAGFlow Nginx 证书与同步服务的基础上运行：

```sh
sudo bash install.sh /path/to/marslab-root-ca.crt
```

脚本验证两个 TLS 连接所需证书、域名、有效期与站点私钥配对，备份现有站点配置，
通过 `nginx -t` 后平滑 reload。有限次重试等待旧 worker 退出；Vela 匿名鉴权与
RAGFlow 首页回归失败则恢复原站点。脚本不重启主机，也不修改其他站点。

Vela 仅开放 `/api/` 和产物 GET；其余路径返回 404，产物写请求返回 405。
请求体上限 1 MiB、读写超时 300 秒，禁用请求和响应缓冲以支持下载。HTTP 80
返回 308 到 HTTPS，保留原始路径和查询。下载支持 Range，转发时保留原始
Host、路径、查询参数。新站点 access log 不记录查询参数或 Authorization；
上游日志和异常日志仍应按敏感运维日志管理，不能对外公开。

APISIX 原有 API 限流仍为每来源地址、每实例 120 次/分钟，经过 Nginx 后来源
可能聚合为管理节点地址；这不是按 API Key 的业务配额。当前没有进行负载验收。

## 运行配置与验收

生成签名 URL 的 Control 配置必须同时设置：

```text
VELA_ARTIFACT_S3_DOWNLOAD_ENDPOINT=https://vela.marslab.ic
```

通过新不可变 ConfigMap 更新 `envFrom` 并滚动发布，保留镜像、seccomp 和其他
运行参数。修改下载 endpoint 后须验证签名 GET、版本、SHA256、Range 和匿名拒绝，
不能仅修改已返回 URL 的域名。旧签名 URL 在失效前继续使用其原始入口和对应 CA。

```sh
sudo nginx -t
sudo systemctl is-active nginx ragflow-nginx-cert-sync.timer
sudo systemctl is-enabled nginx ragflow-nginx-cert-sync.timer
curl --cacert marslab-root-ca.crt --resolve vela.marslab.ic:443:10.1.201.70 \
  -i https://vela.marslab.ic/api/v1/projects/62275ddc-ae83-4ca1-b80c-313161264836/jobs/00000000-0000-4000-8000-000000000001
```

最后一项预期 401，证明 TLS 和匿名访问拒绝有效。`.71` 用同样方法检查。
开机自启动依据现有 systemd enabled 状态；没有通过重启主机验证。

部署记录、证据和回滚方法见
[域名接入记录](../../../docs/vela-domain-https-2026-09-16.md)。
