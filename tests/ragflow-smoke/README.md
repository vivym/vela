# RAGFlow / Qwen3 smoke corpus

`corpus/` 是验证用的小型中文语料，不包含生产数据或凭据。2026-09-16 已上传至
`qwen3-gpu-smoke`（dataset ID `732153aeb17911f1b7b605167d0e39b4`），
两份文档均为 `DONE`、各 1 个 chunk。模型文档已更新为两张 GPU、两个 vLLM 引擎。

在 RAGFlow 的检索测试中选择 `qwen3-reranker-4b@qwen3-gpu@VLLM`，检查：

| 问题 | 预期首位文档 |
| --- | --- |
| 管理面 API、网关和数据库运行在哪些节点？ | `vela-platform.md` |
| Embedding 和 Reranker 一共使用几张 GPU，采用什么推理框架？ | `qwen3-model-worker.md` |
| 模型权重缓存在哪里，Pod 重建时需要下载吗？ | 两份均包含正确答案，任一排首位均可 |

对应的已验证 API 是 `POST /api/v1/datasets/search`，需要已登录的会话或授权 API key：

```json
{
  "dataset_ids": ["732153aeb17911f1b7b605167d0e39b4"],
  "question": "Embedding 和 Reranker 一共使用几张 GPU，采用什么推理框架？",
  "rerank_id": "qwen3-reranker-4b@qwen3-gpu@VLLM",
  "similarity_threshold": 0,
  "vector_similarity_weight": 0.7,
  "knn_top_k": 10,
  "page_size": 10,
  "keyword": false
}
```

断言响应 `code=0`，两个候选片段按分数降序排列，并且首位文档符合上表。
`keyword=false` 避免依赖尚未配置的 Chat 模型。阈值 0 用于观察完整测试候选，
不代表生产推荐值。`vector_similarity` 字段在开启 reranker 时承载重排分数，
`similarity` 是结合词项权重后的分数，不要将其误读为纯 embedding 余弦相似度。

直接模型接口验证可从有网络权限的 Pod 运行 `hack/qwen3-model-smoke.py`：
Embedding 必须返回 2560 维；Reranker 必须将相关的 GPU 文档排在无关文档之前。
本次还通过了 4 并发、12 次混合请求验证；详情见部署证据文档。
这组小语料验证接线和基本排序，不能代替生产语料质量评估。没有 Chat 模型时，
不执行生成式问答，也不宣称已验证回答引用。

PDF 解析验证已追加到同一知识库，使用独立的四页受控样本。
发现的表格漏数字与双栏顺序问题、OCR 结果及复现方法见
[pdf/README.md](pdf/README.md) 和 [PDF 验收报告](../../docs/ragflow-pdf-validation-2026-09-16.md)。
