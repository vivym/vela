# Vela 域名 HTTPS 接入记录（2026-09-16）

两台管理节点的宿主机 Nginx 已配置 `vela.marslab.ic`，API 根地址为
`https://vela.marslab.ic/api`，新签名下载也使用该域名。请求链路为
**客户端 → Nginx 443 → APISIX 30443 → Vela API / MinIO**。

DNS 由用户手动管理。本次从本机和 `.70` 显式指定 `.70/.71` 连接地址、保留真实
域名 SNI/Host 做验收；没有修改 DNS 或 hosts，不能把该结果表述为 DNS 已生效。
应添加两条 A 记录，TTL 建议 60 秒：

```dns
vela.marslab.ic. 60 IN A 10.1.201.70
vela.marslab.ic. 60 IN A 10.1.201.71
```

## 配置与证书

两节点新增 `/etc/nginx/conf.d/vela.marslab.ic.conf`，文件 SHA256：

```text
daa56d9b3027de96c434a1d6fc7a76ce123961b005c918ca74c6f3846ca33275
```

入口复用 RAGFlow 的 `*.marslab.ic` 叶证书及私钥。SAN 包含 `*.marslab.ic`，
签发者为 MARSLAB Root CA；证书链、域名、有效期与公钥配对检查均通过。站点私钥
未复制到本机或仓库。客户端应使用[公开 MARSLAB Root CA](evidence/marslab-root-ca.crt)，
SHA256 指纹：

```text
E1:FB:F1:F9:96:04:99:90:F3:CF:6D:67:95:4D:02:04:43:B6:B2:E1:FF:D2:90:96:30:3D:89:19:80:C9:E1:28
```

叶证书指纹为 `50:37:01:10:63:30:2A:62:6D:0B:7D:B5:BD:1B:6C:6F:64:8D:F8:1C:28:93:F8:2D:39:2E:F5:24:10:EC:D5:A6`。
有效期为 2026-08-07 至 2044-10-21。此前 [RAGFlow 证书检查](ragflow-nginx-certificate-alignment-2026-09-16.md)
已确认 macOS 原生验证因有效期过长拒绝这一证书。本次按要求复用，未重新签发；
需要签发方使用同一 CA 提供符合客户端期限要求的短期叶证书。显式 CA 的 curl/Python
验证成功不表示浏览器原生信任已解决。

本次本机 Python 3.14.7 的默认严格验证另报
`CA cert does not include key usage extension`。检查公开根证书确认其包含
`Basic Constraints: CA:TRUE`，但没有 `Key Usage` 扩展。需要 CA 签发方修正 CA
证书（包括合适的 `keyCertSign`、`cRLSign`）及重新签发兼容的短期叶证书，并按其
证书链更新客户端信任。没有关闭证书验证，也没有通过修改客户端默认严格校验
来掩盖问题；curl 和管理节点 Python 的通过结果只适用于已验证的客户端。
[本机接入文件复验](evidence/vela-domain-2026-09-16/client-configuration.json)记录了这一限制。

Nginx 上游 TLS 使用独立 Vela CA，`proxy_ssl_verify on`，SNI 为现有 APISIX
证书覆盖的 `apisix-gateway.apisix.svc.cluster.local`。HTTP Host 保持
`vela.marslab.ic`，签名 URL 的路径、查询和版本不改写。没有修改 APISIX 现有路由或证书。

HTTP 80 返回 308；443 只开放 `/api/` 与产物 GET，其他路径返回 404，产物写请求
返回 405。`/` 不是管理页面。请求体上限 1 MiB，超时 300 秒。新站点 access log
省略签名查询参数和 Authorization；异常日志及既有上游日志仍应作为敏感运维日志保管。

两台 Nginx 及 `ragflow-nginx-cert-sync.timer` 均为 active、enabled。Nginx 通过
`nginx -t` 后执行 reload，没有重启主机；其他站点配置 hash 未变。

## Control 下载域名切换

创建新的不可变 ConfigMap，仅修改下载 endpoint，并定向更新 Deployment 的
`envFrom` 引用。两个副本已滚动完成并 Ready：

| 项目 | 值 |
| --- | --- |
| 旧 runtime | `vela-control-runtime-api-95ce430963ad` |
| 新 runtime | `vela-control-runtime-api-0ec8cb63b1a5` |
| 内容 revision | `sha256:a048dfc7837a3141b7e4da2485980d777af290de1707bdfd3cbbeeea543697aa` |
| 下载 endpoint | `https://vela.marslab.ic` |
| 镜像 | `10.1.201.70:5005/vela-control@sha256:f12e249463984dd936364a0c06274e73ca76b02abded69f5933aa97cdad80b96` |

镜像、seccomp、ffprobe、凭据和其他运行参数未变。仓库 Marslab overlay 已同步；
`TestMarslabOverlayResolvesConfigurationAndPreservesValidationBoundary` 与
`TestMarslabFFprobeVersionMatchesBuiltRuntime` 通过，包含 ConfigMap 内容 revision 校验。

## 实际验收

[验收回执](evidence/vela-domain-2026-09-16/verification.json) 时间为
2026-09-16 09:58:51 UTC。两节点各通过 12 项入口/鉴权回归，以及视频和缩略图下载检查：

| 检查 | `.70` / `.71` |
| --- | --- |
| 本机 curl 显式 CA 与域名验证 | `ssl_verify_result=0`，匿名 API 401 |
| 永久中转 Key 查询不存在的 Job | 404，鉴权成功 |
| 匿名 / 篡改 Key | 401 |
| 跨项目 Job | 404 |
| 中转 Key 调用身份管理接口 | 403 |
| 查询既有成功 Job | 200、SUCCEEDED |
| HTTP 跳转 | 308，路径和查询完整保留 |
| RAGFlow 首页 / admin 阻断 | 200 / 403 |
| 视频与缩略图完整 GET | 200，长度和 SHA256 均匹配 |
| 两类产物 Range GET | 206，前 1024 字节和 Content-Range 正确 |
| 无签名产物读取 / 产物 PUT | 403 / 405 |
| 存储根路径 / Grafana 路径 | 404 / 404 |

本次复用 Job `d79a51f4-9c1c-4f23-90d0-8d95086515a7`：完整视频 855445 bytes，
SHA256 `5a1d3f3204976a9a20bad067e8fab9446d1ee4db892a6fd3fab185a878952306`；
缩略图 6944 bytes，SHA256 `3a995ac1955d64ffc5af38d6c90ebd470cb6d75ec9b04eafc70dcca4fde3983e`。
视频与已有完整音视频验收产物逐字节一致，124 帧及 AAC 音轨未裁剪。
没有提交新的 GPU 任务；中转项目仍为 0 Job、0 Charge，原验收 Job 仍为唯一一笔 Charge。

最后复查时本机直连管理节点出现 TCP 超时；随后于 2026-09-16 11:30:34 UTC
从 `marslab-server-170hx-66` 跳板机重新验证两台域名 HTTPS，严格域名/CA 校验
通过且匿名 API 均返回 401，见[跳板机最终检查](evidence/vela-domain-2026-09-16/jump-host-final-check.json)。
该检查时跳板机仍无法解析域名，DNS 尚待用户配置。不能将本机网络超时与服务器入口故障混同。

这是入口、鉴权和已有产物下载验收；不扩展为负载、节点故障切换或浏览器兼容性验收。
APISIX API 仍使用已有的每来源地址每实例 120 次/分钟限流，Nginx 后可能聚合为节点
地址；后续扩大接入并发时需单独改为经过认证的身份限流并做压测。

## 接入文件与备份

已更新本机及 `.70` 的中转私有 `connection.env`：`VELA_BASE_URL` 使用域名，
`CURL_CA_BUNDLE` 指向同目录已换为 MARSLAB CA 的 `gateway-ca.crt`。API Key 不变。
旧 IP 配置和 Vela CA 分别备份为 `connection-nodeport.env`、`nodeport-gateway-ca.crt`。
目录 0700，文件 0600，均在 Git 仓库之外。

远端部署材料、验证程序及无密钥回执在
`/opt/vela-cluster/vela-nginx-20260916/`。安装备份：

- `.70`：`/root/vela-backups/nginx-vela-20260916-175251/`。
- `.71`：`/root/vela-backups/nginx-vela-20260916-175432/`。

安装回执：[.70](evidence/vela-domain-2026-09-16/node70-installation.json)、
[.71](evidence/vela-domain-2026-09-16/node71-installation.json)。
仓库配置和安装脚本见 [vela-nginx](../deploy/cluster-platform/vela-nginx/README.md)。

## 回滚

先在 `.70` 用 RKE2 kubectl 把 Control 的 runtime 引用恢复为上述旧 ConfigMap；
确认 rollout 完成、新生成的下载 URL 恢复为 `https://10.1.201.70:30443` 后，再撤销
Nginx 域名入口。两个 runtime ConfigMap 和完整旧 Deployment 快照均已保留，后者位于：
`/opt/vela-cluster/vela-nginx-20260916/control-before-domain.json`。恢复时只改
runtime 引用，避免完整 apply 旧 Deployment 覆盖后续变更。

本次新增站点此前不存在，可在每台上将 `vela.marslab.ic.conf` 移出 `conf.d`，运行
`nginx -t` 并 reload；不修改 RAGFlow 配置和共享证书。已签发的域名下载 URL 最长
15 分钟过期，应等待有效链接用完再撤销入口。接入侧同时从上述私有备份恢复 IP
地址和 Vela CA，不能继续使用 MARSLAB CA 校验 NodePort 证书。
