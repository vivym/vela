# Qwen3 公共 API 发布记录（2026-09-20）

## 发布内容

- Worker 镜像：`10.1.201.70:5005/llm-models/qwen3-model-worker:0.3.0`
- 镜像 digest：`sha256:e0b295f87a84f23cacebbd587fab5d4ef7b448c297e0aa9c5d974df120e73b05`
- Pod：`llm-models/qwen3-model-worker`，固定在 `server-53`（256 GiB GPU 节点），申请 2 张 GPU。
- `/v1/models` 由 Worker 代理统一返回：`qwen3-embedding-4b`、`qwen3-reranker-4b`。
- APISIX 路由：`qwen3-public`，公开前缀 `/qwen3/*`，转发到 Worker 的 `:8080`。
- 认证：独立 APISIX `key-auth` Consumer `vela-qwen3-relay`；支持标准
  `Authorization: Bearer <key>`，并继续兼容 `X-API-Key: <key>`。
- 限流：每个 APISIX 实例每个 Consumer 600 次/分钟，超限返回 `429`。
- Nginx：`.70`、`.71` 均已安装 `vela.marslab.ic` 虚拟主机并平滑 reload；未重启主机。

模型 API Key 没有过期时间，值只保存于 `.70`：

```text
/root/vela-secrets/qwen3-api-key  (root:root, mode 0600)
```

需要轮换时，更新同一 Consumer 的 `key-auth.key`，再同步替换该文件；不要把密钥写入 Git、镜像或日志。

## 访问方式

```text
Base URL: https://vela.marslab.ic/qwen3/v1
Header:   Authorization: Bearer <从 .70 受保护文件读取的密钥>
```

接口包括：

- `GET /qwen3/v1/models`
- `POST /qwen3/v1/embeddings`
- `POST /qwen3/v1/rerank`

`X-API-Key` 仍可用于旧客户端；OpenAI-compatible 客户端应使用
`Authorization: Bearer <key>`。

Worker Service 仍为 `ClusterIP`，没有新增 NodePort；外部流量始终经过
`Nginx TLS → APISIX → Worker`。

## 验收结果

使用 MARSLAB Root CA、分别将域名解析到两个入口节点进行测试：

| 检查 | `.70` | `.71` |
| --- | ---: | ---: |
| 无 API Key `/v1/models` | 401 | 401 |
| 错误 API Key `/v1/models` | 401 | 401 |
| 正确 API Key `/v1/models` | 200，返回两个模型 | 200，返回两个模型 |
| `/v1/embeddings` | 200 | 200 |
| `/v1/rerank` | 200 | 200 |

初始配置的 60 次/分钟限流在验证中过于严格，已调整为 600 次/分钟。由于
`limit-count` 使用 `policy=local` 且 APISIX 有两个副本，实际聚合阈值会随请求
在副本间分配；业务配额不能把这个实例级限流结果当作全局精确配额。
更新后在 `.70` 入口连续发送 650 个轻量请求，未触发 `429`。

Worker 更新前后均未修改模型本地缓存；启动日志确认两个 vLLM 子服务完成加载并且
`/readyz` 返回 200。Nginx 安装脚本在两台节点上完成 `nginx -t`，并验证现有
Vela 未授权请求仍返回 401、RAGFlow 站点返回 200。

## 回滚

1. 将 `deploy/model-serving/deployment.yaml` 的镜像恢复到上一版 digest，重新执行
   `kubectl apply -k deploy/model-serving`。
2. 通过 APISIX Admin API 删除或恢复 `qwen3-public` 路由和
   `vela-qwen3-relay` Consumer。
3. Nginx 安装脚本在每台节点生成的 `/root/vela-backups/nginx-vela-*` 中保留了
   发布前配置，可复制回站点文件后执行 `nginx -t && systemctl reload nginx`。
