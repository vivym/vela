# RAGFlow Nginx 入口证书对齐记录（2026-09-16）

按用户要求，将 `10.1.201.70` 的 `/etc/nginx/conf.d/ragflow.marslab.ic.conf`
入口证书配置对齐到 `10.1.201.71`。已在 `.70` 应用并平滑重载 Nginx，没有重启主机。

## 已应用的变更

两台入口现在均使用：

```nginx
ssl_certificate     /etc/nginx/ssl/ragflow.marslab.ic/wildcard.marslab.ic.crt;
ssl_certificate_key /etc/nginx/ssl/ragflow.marslab.ic/wildcard.marslab.ic.key;
```

站点证书和匹配的站点私钥从 `.71` 直接复制到 `.70`，传输时校验 SSH 主机密钥。
私钥未复制到本机或仓库；`.70` 文件归属 root，私钥权限为 `0600`。
原配置备份位于 `.70`：

```text
/root/vela-backups/nginx-ragflow-marslab-20260916-072423/ragflow.marslab.ic.conf
```

APISIX 上游仍使用独立的 Vela CA，保留以下配置：

```nginx
proxy_pass https://127.0.0.1:30443;
proxy_ssl_server_name on;
proxy_ssl_name ragflow.marslab.ic;
proxy_ssl_verify on;
proxy_ssl_trusted_certificate /etc/nginx/ssl/ragflow.marslab.ic/current/ca.crt;
```

客户端到 Nginx 的 MARSLAB TLS 与 Nginx 到 APISIX 的 Vela TLS 是两个连接。
入口证书更换不应改变上游信任材料。

## 验证结果

| 检查 | 结果 |
| --- | --- |
| `.70` `nginx -t` | 通过 |
| 本机指定各节点 IP、使用域名 SNI 和 MARSLAB CA 访问 HTTPS 首页 | `.70`、`.71` 均为 HTTP 200，`ssl_verify_result=0` |
| `.70` HTTP 首页 | HTTP 308，跳转到 HTTPS |
| `.70` `/api/v1/admin` | HTTP 403 |
| `.70` 原有 `admin.benpay.vip` 站点 | 变更前后均为 HTTP 307 |
| `.70` 其他七个 Nginx `.conf` 文件 | 内容未变 |
| `.70` Nginx 与证书同步 timer | active、enabled |
| 手动运行 `.70` `ragflow-nginx-cert-sync.service` | `ExecMainStatus=0`，入口配置未变，仍使用 wildcard 文件 |

两节点实际返回的站点证书 SHA-256 指纹相同：

```text
50:37:01:10:63:30:2A:62:6D:0B:7D:B5:BD:1B:6C:6F:64:8D:F8:1C:28:93:F8:2D:39:2E:F5:24:10:EC:D5:A6
```

用户提供的 [MARSLAB Root CA 公开副本](evidence/marslab-root-ca.crt) 的 SHA-256 指纹：

```text
E1:FB:F1:F9:96:04:99:90:F3:CF:6D:67:95:4D:02:04:43:B6:B2:E1:FF:D2:90:96:30:3D:89:19:80:C9:E1:28
```

仓库内 [站点配置](../deploy/cluster-platform/ragflow-nginx/ragflow.marslab.ic.conf)
与远端最终配置一致，文件 SHA-256 为：

```text
70622e3b688c9c9812ca652f6b5e69a843663af305ee8ab84af3d2b00f7483cc
```

首次尝试在 reload 后立即探测，仍读到了旧 worker 返回的旧证书，因此自动恢复配置。
最终应用使用有限次数的重试等待平滑重载完成，随后上述检查通过。

## 本地部署材料与限制

同步更新了仓库内站点配置、[安装说明](../deploy/cluster-platform/ragflow-nginx/README.md)
和 [安装脚本](../deploy/cluster-platform/ragflow-nginx/install.sh)。安装脚本新增了入口证书和
私钥文件检查、剩余有效期检查、域名检查与公钥配对检查，已通过 `bash -n`。
此次远端采用定向修改；更新后的安装脚本没有在远端重新执行。

定时同步继续维护 `current/*` 中的 Vela 上游信任材料，不覆盖 MARSLAB wildcard 文件。
MARSLAB 入口证书续期仍需签发方提供，并在两台入口更新。

当前 `*.marslab.ic` 证书有效期为 `2026-08-07 06:56:48 UTC` 至
`2044-10-21 06:56:48 UTC`。macOS 原生证书验证报告 `OtherTrustValidityPeriod`、
`Certificate exceeds maximum temporal validity period`。上述显式指定 CA 的 curl 成功
不能证明浏览器已接受该证书；仍需重新签发较短有效期的站点证书。本次任务只完成两节点对齐。

## 回滚

在 `.70` 上执行：

```sh
sudo cp -a /root/vela-backups/nginx-ragflow-marslab-20260916-072423/ragflow.marslab.ic.conf /etc/nginx/conf.d/ragflow.marslab.ic.conf
sudo nginx -t && sudo systemctl reload nginx
```

原 `current/tls.crt`、`current/tls.key` 和 `current/ca.crt` 均保留，恢复后入口将重新使用
Vela 证书。回滚后应等待旧 worker 退出，再验证实际返回的证书。
