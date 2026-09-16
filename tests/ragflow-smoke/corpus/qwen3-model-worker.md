# Qwen3 模型 worker 测试文档

Embedding 服务把文本转换为向量，Reranker 根据查询和候选文档的相关性输出分数。
Embedding 与 Reranker 部署在同一个 worker Pod，分别使用一张 64 GiB GPU，合计两张卡。
两个模型均使用 vLLM 进行 GPU 推理，每个引擎的初始显存预算为所在 GPU 的 36%。
模型权重缓存于 server-53 的本地磁盘，Pod 重建时不重新下载。
