# RAGFlow Kubernetes 验证部署记录

时间：2026-09-15（Asia/Shanghai）

## 已完成

- namespace：`ragflow-lab`
- Helm release：`ragflow-lab`，官方 chart `0.1.1`，RAGFlow `v0.27.2`
- 管理侧 Pod 固定在 `llmpool01`/`llmpool02`（`vela.ai/management=true`、`vela.ai/control-plane-tier=cpu`）
- task executor 独立为 `ragflow-lab-task-executor`，固定到 `vela.ai/node-role=gpu-worker`
- 文档引擎使用 Infinity；MySQL、Valkey、Silo/MinIO 均为 ClusterIP
- PVC 使用 `local-path`，容量：Infinity 20Gi、Silo 20Gi、MySQL 10Gi、Valkey 5Gi；均已 Bound
- RAGFlow API、Infinity、Silo、MySQL、Valkey、GPU task executor 均已 `1/1 Running`
- API Pod 内部 `/` 返回 HTTP 200；RAGFlow server 日志显示 database tables、Infinity 和 ingestion 初始化成功

## APISIX 入口

- Host：`ragflow.marslab.ic`
- 标准访问地址：`https://ragflow.marslab.ic/`，经下述 Nginx 入口进入 APISIX。
- 保留的直接 NodePort：`https://ragflow.marslab.ic:30443/`；`:30080` 自动返回 308 到 `:30443`。
- `.70`、`.71` HTTPS 根路径均返回 200；`/api/v1/admin` 均返回 403
- 使用现有 `vela-gateway-ca`；证书 SAN 已加入 `ragflow.marslab.ic`
- 路由配置见 [ragflow-route.json](../deploy/cluster-platform/ragflow-route.json)

## 标准端口入口

- `.70`、`.71` 均运行 Nginx systemd 服务并设置为开机自启。
- Nginx 监听 80/443，仅匹配 `ragflow.marslab.ic`，转发至本机 APISIX `30443`；`.70` 原有 `benpay.vip` 配置已保留。
- 两台主机均配置 `ragflow-nginx-cert-sync.timer`，每 5 分钟从 `apisix/vela-gateway-tls` 的单次 Secret 快照同步证书、私钥和 CA，验证匹配、有效期、域名及证书链后原子切换；Nginx 检查或 reload 失败则回滚，未变化时不 reload。证书目录为 0700、文件为 0600。
- Nginx 至 APISIX 的 HTTPS 上游也已启用现有 CA 和 SNI 校验，无须跳过证书验证。
- 验证：两 IP 的 443 根路径均 HTTP 200，80 均 308 到 HTTPS，管理路径均 403，私有 CA 校验结果为 0。
- DNS 需要将 `ragflow.marslab.ic` 配置为 A 记录 `10.1.201.70` 和 `10.1.201.71`，建议初始 TTL 60 秒。两节点当前解析器均未返回该域名记录，当前验证使用 curl 的 `--resolve`，不能视为企业 DNS 已完成。DNS 轮询没有健康检查，TTL 到期不会自动剔除故障 IP；客户端是否重试另一地址取决于具体实现。
- 已用隔离的临时目录和替代命令验证同步程序：正常更新、未变化不 reload、Nginx 检查失败、reload 失败回滚、成功切换及错误密钥拒绝。故障注入未操作运行中的 Nginx 或证书。
- `.70` 原有 `admin.benpay.vip` 复测保持 307。增量备份：`.70` 为 `/root/vela-backups/nginx-ragflow-sync-20260915-170853`；`.71` 为 `/root/vela-backups/nginx-ragflow-sync-20260915-171120`。
- 配置、同步程序、systemd 单元及安装回滚说明已保存到 [ragflow-nginx](../deploy/cluster-platform/ragflow-nginx/README.md)。未执行主机重启；自启依据是 enabled 状态、服务实际 active 和同步任务退出成功。
- `.71` 的 systemctl 仍曾输出 `Failed to allocate directory watch: Too many open files`；本次命令退出成功、服务及定时器状态正常，不代表该宿主机问题已修复。
- 限流边界：APISIX 原有 `limit-count` 仍是各实例按所见 `remote_addr` 的 120 requests/min 共享额度。探测日志中来源为 `10.1.201.70`、`10.42.1.0`，不是终端来源；多人并发前需另行调整可信代理与限流方案。Nginx 覆盖客户端自报的转发 IP 头，本次未扩大 APISIX 对请求头的信任。

## 私有镜像

镜像均已推送到 `10.1.201.70:5005/ragflow-lab`，工作负载使用 registry manifest digest：

| 镜像 | digest |
|---|---|
| `ragflow:v0.27.2` | `sha256:e6b3f1a185c0a70abb3a72e1092415df8c5c6a4fae0089545554f2936c4b27ab` |
| `infinity:v0.7.3-x64-v3` | `sha256:6640409359e6d515287d6255657ee78a86fdce996196fbb4d371a2e39243309f` |
| `silo:RELEASE.2026-08-06T00-00-00Z` | `sha256:da1284931914c3de5fc8e8ad0c43b88e4eb930d20064b36867daba4dfd546a00` |
| `mysql:8.0.40` | `sha256:ed04aca46b6fe0bdc192becc17d358b46eeb82762b81ee65b80feff3d0873e9d` |
| `valkey:8` | `sha256:f740fb12fb9dec890f4785ebd04ba184ab9bc556cc33063a01726a9180cc697c` |
| `busybox:1.37.0` | `sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0` |

## 适配与边界

- API/Web 与 task executor 已拆分；API 使用 `--disable-taskexecutor --disable-datasync`，GPU executor 使用 `--disable-webserver --workers=1`。
- 未启用官方 sandbox；避免 privileged 和 Docker socket。
- `.66` 未调度 RAGFlow 工作负载；没有重启主机，也没有初始化或清理 GPU 数据盘。
- `local-path` 是单节点本地持久化，不具备节点故障迁移能力；这次只用于验证。
- APISIX hostname/TLS 路由和两台主机的标准端口入口已配置；RAGFlow Service 保持 ClusterIP，由网关代理。企业 DNS 与客户端 CA 信任需在企业网络侧落实。
- Chat/Embedding 模型端点尚未配置；可登录和上传流程之后还需要绑定实际模型服务，才能验收完整问答链路。

## 复核命令

在 `.70` 上以 root kubeconfig 执行：

```sh
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
kubectl -n ragflow-lab get pods -o wide
kubectl -n ragflow-lab get pvc
helm -n ragflow-lab status ragflow-lab
```
