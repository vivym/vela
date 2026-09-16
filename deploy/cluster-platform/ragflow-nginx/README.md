# RAGFlow 标准端口入口

`.70 / llmpool01` 和 `.71 / llmpool02` 运行宿主机 Nginx，入口为
`https://ragflow.marslab.ic/`。每台 Nginx 将请求转发到本机 NodePort
`https://127.0.0.1:30443`，由 APISIX 路由到 RAGFlow ClusterIP。
APISIX 的 Service 使用 `externalTrafficPolicy: Cluster`，后端 APISIX Pod
可能在另一台管理节点；不要求 Nginx 与最终 APISIX Pod 同节点。

企业 DNS 添加以下两条 A 记录，建议初始 TTL 为 60 秒：

```dns
ragflow.marslab.ic. 60 IN A 10.1.201.70
ragflow.marslab.ic. 60 IN A 10.1.201.71
```

客户端使用 **MARSLAB Root CA** 校验两台 Nginx 的 443 入口。公开 CA 副本为
[`marslab-root-ca.crt`](../../../docs/evidence/marslab-root-ca.crt)，SHA-256 指纹：
`E1:FB:F1:F9:96:04:99:90:F3:CF:6D:67:95:4D:02:04:43:B6:B2:E1:FF:D2:90:96:30:3D:89:19:80:C9:E1:28`。
当前 `*.marslab.ic` 叶证书有效期为 2026-08-07 至 2044-10-21，macOS 会因
有效期过长拒绝它；本次对齐证书不解决这个兼容性问题，仍需签发较短有效期的叶证书。
DNS 轮询没有健康检查；某台故障时是否及时尝试另一个 IP 取决于客户端。
TTL 到期本身不会从 DNS 中移除故障 IP，也不会迁移已有连接。

## 安装和证书轮换

前提：管理节点已安装 Nginx、RKE2 kubectl、Python 3、OpenSSL 和 flock，
现有 `/etc/rancher/rke2/rke2.yaml` 可读取 `apisix/vela-gateway-tls`。
先通过安全渠道将 MARSLAB 站点证书和匹配私钥放到两台节点的：

```
/etc/nginx/ssl/ragflow.marslab.ic/wildcard.marslab.ic.crt
/etc/nginx/ssl/ragflow.marslab.ic/wildcard.marslab.ic.key
```

私钥归属 root、权限 0600，不进入仓库。此处只有站点私钥，不需要复制 CA 私钥。
`install.sh` 先校验文件存在、有效期、域名和密钥配对；该证书的续期由签发方提供。
将本目录复制到管理节点，再运行 `sudo bash install.sh`。安装前备份写入
`/root/vela-backups/nginx-ragflow-sync-<时间>`；保留其他站点配置。

`ragflow-nginx-cert-sync.timer` 每五分钟运行一次，开机后自动启动。
该定时任务继续维护 **APISIX 上游的信任材料**，不覆盖上述 MARSLAB 入口文件。
同步程序读取同一个 Secret 快照中的 `tls.crt`、`tls.key`、`ca.crt`，验证
密钥配对、有效期、域名与 CA 链，随后原子切换 `current` 符号链接。
`nginx -t` 或 reload 失败时恢复旧链接；内容未变时不 reload。
旧版本保存在根用户可读的 `versions` 目录供回滚。私钥及目录均不进入仓库。

Nginx 的 `proxy_ssl_trusted_certificate` 保持指向 `current/ca.crt`，使用 Vela CA
校验 APISIX 上游证书及 `ragflow.marslab.ic` SNI。入口和上游属于不同 TLS 连接，
不能将这个上游 CA 路径改为 MARSLAB CA。客户端无需为标准 443 入口信任 Vela CA；
直接访问 APISIX 30443 则仍需它。CA 根轮换和 MARSLAB 入口证书续期需另行协调。

```sh
sudo systemctl status nginx ragflow-nginx-cert-sync.timer
sudo journalctl -u ragflow-nginx-cert-sync.service --since today
sudo nginx -t
```

`install.sh` 针对已经运行的 Nginx 执行平滑 reload；未用主机重启验证，
开机自启动依据 systemd enabled 状态与当前成功执行结果。

## 入口策略和边界

- 80 返回 308 到标准 HTTPS，保留路径及查询参数；443 上传上限 128 MiB，
  读写超时 3600 秒，关闭代理请求和响应缓冲。
- Nginx 覆盖 `X-Real-IP`、`X-Forwarded-For` 为实际连接来源，
  不追加客户端自报的 IP。未全局增加 APISIX 对转发头的信任。
- 2026-09-16 已移除 RAGFlow 全站 `120 requests/min` 的 `limit-count`，
  避免静态文件和业务 API 共用额度而阻断正常页面加载。此阶段不设替代的全站次数限制。
  APISIX 仍看到 Nginx 节点或 CNI 来源地址，不能据此按终端用户计数；
  后续分类限流需先确定可信来源或经过验证的用户身份，并依据真实负载设置阈值。
- APISIX 保留 `/api/v1/admin` 的 403 阻断；应用采用 RAGFlow 自带认证。
- 直接 NodePort `30080/30443` 仍保留。RAGFlow 模型端点、账号初始化、
  文档问答全流程及存储高可用不属于本次入口验证结果。

验证结果见 [部署记录](../../../docs/ragflow-k8s-deployment-evidence-2026-09-15.md)。

2026-09-16 两节点入口证书对齐记录见 [变更证据](../../../docs/ragflow-nginx-certificate-alignment-2026-09-16.md)。

全站限流移除及回归验证见 [限流修复记录](../../../docs/ragflow-rate-limit-fix-2026-09-16.md)。
