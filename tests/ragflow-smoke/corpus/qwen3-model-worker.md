# Qwen3 模型 worker 测试文档

Embedding 服务把文本转换为向量，Reranker 根据查询和候选文档的相关性输出分数。
Embedding 与 Reranker 可以在同一张 64 GiB GPU 上运行，初始显存预算分别为 36%。
