# LLM API 发布验收清单

## 应用与模型

- 镜像使用 digest，chart 版本和 Git revision 可追溯。
- 明确 GPU 数量、显存、CPU/内存、临时盘、PVC 和模型缓存需求。
- 推理服务与聚合服务分别配置 readiness/startup probe、PDB、优雅终止和滚动策略。
- token 计量、租户配额、重试、超时和幂等行为有明确责任边界。

## API 与 APISIX

- 外部入口只有 APISIX；后端 Service 是 ClusterIP。
- OIDC/JWT/API key、租户识别、限流、并发上限和请求体限制已验证。
- SSE 禁用不适用的代理缓冲，读写超时覆盖最长生成时间，客户端取消能传递到后端。
- HTTPS 证书、SNI、trace/span header、CORS 和错误响应格式已验证。
- Admin、Metrics、调试和内部管理路径不可公开。

## 安全与数据

- provider key、TLS 私钥和数据库密码来自 Secret 管理系统。
- 日志不记录原始 prompt、完整 token、Authorization header 或 Secret。
- 审计记录包含调用者、tenant、route、状态码、延迟和 request id。
- 依赖镜像、Helm chart 和配置通过 CI 扫描并可回滚。

## 可观测性与回滚

- Grafana 有请求量、错误率、延迟、token、队列、SSE 中断、GPU 利用率、Pod 重启、PVC、Longhorn、MinIO、Loki 和 Tempo 面板。
- 已配置 PrometheusRule，并验证 firing、Grafana 可见、silence、unsilence 和自动恢复。
- 使用一次失败升级验证 `--atomic` 或 Argo 回滚；确认数据库迁移和 PVC 数据兼容。
- 记录上线 revision、路由 revision、镜像 digest、负责人和回滚命令。
