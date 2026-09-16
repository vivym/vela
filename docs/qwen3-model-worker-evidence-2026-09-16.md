# Qwen3 双 GPU vLLM 部署与验收记录

最终运行状态复核时间：2026-09-16 03:04 UTC（北京时间 11:04）。

## 部署实况

| 项目 | 验证值 |
| --- | --- |
| 节点 | `server-53` / `10.1.201.54`，内存档位 `256gb` |
| Namespace / Deployment | `llm-models` / `qwen3-model-worker` |
| Pod | `qwen3-model-worker-5f87dbb47f-4c8gn` |
| Pod 状态 | `1/1 Running`，`restartCount=0` |
| 私仓镜像 | `10.1.201.70:5005/llm-models/qwen3-model-worker:0.2.0` |
| 已运行镜像 digest | `sha256:b74ef4445b1d65e819d3be1aff55259d1131ba4375312f137c12237b3c322f4f` |
| 实测运行库 | vLLM `0.10.1.1` |
| GPU | 一个 Pod 申请 2 张，两个 vLLM 引擎各使用一张 |
| 显存 | GPU 0：24114 MiB / 65536 MiB；GPU 1：24242 MiB / 65536 MiB |
| 计算进程 | 两张 GPU 各有一个独立推理进程 |
| 上下文 / 显存预算 | 4096 tokens；每个引擎 `gpu_memory_utilization=0.36` |
| 本地缓存 | `/home/user/models/Qwen`，readonly local PVC `qwen3-models`，`Retain` |
| 健康检查 | `/readyz` 返回 `{"ready": true}` |
| Embedding metrics | 9090 `/metrics` 返回 HTTP 200 |

Embedding 通过 vLLM OpenAI pooling API 运行。Reranker 的 HTTP adapter 调用
`vllm.LLM.generate` 做一个 token 的 yes/no 评分，两个引擎均在 GPU 上运行。
Transformers 只用于 tokenizer，没有 CPU 模型推理。

本次 release 使用已经同步到私仓的 `0.1.4` 运行环境，基础 digest 为
`sha256:61d787ec85f10514187b5d5e0da7d84fda9065047f6d80d93b22125f2034497f`，
再覆盖两个适配器，构建路径记录在 `deploy/model-serving/Dockerfile.release`。
上游运行环境来自 `vllm/vllm-openai:v0.10.1.1`，固定 digest 为
`sha256:d731ee65c044ae0977421eed3d93f931d4b7d79614394184c939db35b8f28fc2`。

运行容器文件与仓库文件 SHA-256 一致：

- `entrypoint.py`：`22185eb8a49fc0f8807ed727dc98116761b0c1dd93350baf5e8b0bde2b848e92`
- `reranker_server.py`：`7b750e9fe1d5e9f7b3ce91f2b387b73bd42b328e9c3dba5741043e187c03393c`

本次未重启主机。离线环境变量和本地只读模型卷保证启动不下载权重；未进行
主机重启、节点故障或跨节点迁移演练。固定本地 PV 的单副本实例不具备跨节点 HA。

## 模型接口与并发验证

从 RAGFlow API Pod 经 ClusterIP 访问模型服务，4 个客户端线程提交 12 次请求：
6 次 Embedding（每次 2 条中文文本），6 次 Reranker（每次 2 个候选文档）。
全部成功；向量维度均为 2560 且数值有限；Reranker 全部返回顺序 `[1, 0]`，
相关 GPU 文档高于无关 CPU 文档。

该批次总耗时 0.168 秒，单请求 0.031–0.071 秒。这是短文本、重复输入、已预热
且启用 prefix cache 的功能冒烟结果，不能作为生产吞吐、长文本延迟或并发容量承诺。
中文相关候选分数约 0.999949；无关候选约 0.468791–0.5。
独立英文测试相关/无关分数分别为 0.999245 和 0.119203。

评分沿用模型卡的 top-20 logprobs 和缺失 yes/no token 的 `-10.0` 回退；
分数在缺失 token 时是近似值，不是校准概率。生产阈值需要业务语料评估。

最终复核最近 20 分钟的 worker 日志，确认 22 次 Embedding 和 13 次 Reranker
代理 HTTP 200，没有 `Traceback`、`OutOfMemoryError` 或 `CUDA out of memory`。
Pod 始终保持本版本启动后的零重启状态。

## RAGFlow 配置与知识库

- Provider：`VLLM`；instance：`qwen3-gpu`。
- Base URL：`http://qwen3-model-worker.llm-models.svc.cluster.local:8080/v1`。
- 默认 Embedding：`qwen3-embedding-4b@qwen3-gpu@VLLM`。
- 默认 Reranker：`qwen3-reranker-4b@qwen3-gpu@VLLM`。
- 测试知识库：`qwen3-gpu-smoke`，ID `732153aeb17911f1b7b605167d0e39b4`。
- `vela-platform.md`：ID `81030ed6b17911f1b7b605167d0e39b4`。
- `qwen3-model-worker.md`：ID `b1673416b17a11f1b7b605167d0e39b4`；已替换旧单卡描述。
- 两份文档均 `DONE` / progress 1.0，各 1 个 chunk。

通过 `POST /api/v1/datasets/search` 实际执行 Embedding 查询和 GPU Reranker。
测试配置为 similarity threshold 0、vector weight 0.7、`keyword=false`。

| 问题 | 排名与重排分数（四舍五入） | 结论 |
| --- | --- | --- |
| 管理面 API、网关和数据库运行在哪些节点？ | 平台文档 0.999739；模型文档 0.000153 | 正确 |
| Embedding 和 Reranker 一共使用几张 GPU，采用什么推理框架？ | 模型文档 0.999949；平台文档 0.007577 | 正确 |
| 模型权重缓存在哪里，Pod 重建时需要下载吗？ | 平台文档 0.999755；模型文档 0.999481 | 两份均包含正确答案 |

上述分数来自检索结果中承载 reranker 输出的 `vector_similarity` 字段；最终
`similarity` 还混合了词项权重。这两种值都不应在此路径下被称为纯向量相似度。
所有响应 `code=0`，两个候选按分数降序；已执行相应断言。

新增入站策略为 `llm-models/qwen3-from-ragflow`，仅选中 qwen3 worker，允许
`ragflow-lab` 访问 8080。曾临时增加的 namespace 级宽泛入站例外已移除；
收紧策略后再次通过上述检索测试。

## 验收边界

已完成：镜像入私仓、双 GPU vLLM 推理、本地缓存挂载、模型接口、RAGFlow 默认
模型配置、两文档解析索引、中文检索重排、短时混合并发与运行状态复核。
Python 语法、Kustomize 渲染和本次文件的 `git diff --check` 均通过。

尚未声称完成：持续容量压测、业务语料质量评估、主机重启/HA 演练、镜像漏洞扫描。
9090 当前只覆盖 Embedding 指标，Reranker 专用请求/队列指标尚未实现。
RAGFlow 的 Chat 模型仍为空，因此本次完成的是检索与重排，不包含生成式问答。
