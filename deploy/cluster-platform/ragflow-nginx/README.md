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

客户端需信任现有 Vela private gateway CA。证书公开副本为
[`vela-gateway-ca.crt`](../../../docs/evidence/vela-gateway-ca.crt)，SHA-256 指纹：
`6D:08:42:6A:C9:F5:FF:1A:41:28:F9:A8:CE:86:3A:56:0B:19:54:C9:19:C3:BA:88:4B:D9:15:BA:89:23:AE:22`。
DNS 轮询没有健康检查；某台故障时是否及时尝试另一个 IP 取决于客户端。
TTL 到期本身不会从 DNS 中移除故障 IP，也不会迁移已有连接。

## 安装和证书轮换

前提：管理节点已安装 Nginx、RKE2 kubectl、Python 3、OpenSSL 和 flock，
现有 `/etc/rancher/rke2/rke2.yaml` 可读取 `apisix/vela-gateway-tls`。
将本目录复制到管理节点，再运行 `sudo bash install.sh`。安装前备份写入
`/root/vela-backups/nginx-ragflow-sync-<时间>`；保留其他站点配置。

`ragflow-nginx-cert-sync.timer` 每五分钟运行一次，开机后自动启动。
同步程序读取同一个 Secret 快照中的 `tls.crt`、`tls.key`、`ca.crt`，验证
密钥配对、有效期、域名与 CA 链，随后原子切换 `current` 符号链接。
`nginx -t` 或 reload 失败时恢复旧链接；内容未变时不 reload。
旧版本保存在根用户可读的 `versions` 目录供回滚。私钥及目录均不进入仓库。

Nginx 使用该 CA 校验 APISIX 上游证书及 `ragflow.marslab.ic` SNI。
现有 CA 根轮换需要另行协调客户端信任分发，定时任务不负责更新客户端。

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
- APISIX 原有 120 requests/min 限流仍按各实例所见的 `remote_addr` 共享计数。
  实测 NodePort 后看到 `.70` 或 `.71` 的 CNI 地址，不能宣称每个终端有独立额度。
  多用户高并发使用前需单独调整限流及可信代理方案；本次仅完成标准端口接入。
- APISIX 保留 `/api/v1/admin` 的 403 阻断；应用采用 RAGFlow 自带认证。
- 直接 NodePort `30080/30443` 仍保留。RAGFlow 模型端点、账号初始化、
  文档问答全流程及存储高可用不属于本次入口验证结果。

验证结果见 [部署记录](../../../docs/ragflow-k8s-deployment-evidence-2026-09-15.md)。
