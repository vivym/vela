# RAGFlow / Qwen3 smoke corpus

`corpus/` 是验证用的小型中文语料。上传到 RAGFlow 的知识库后，执行：

1. 文档解析完成；
2. 使用 `qwen3-embedding-4b` 建立向量索引；
3. 使用 `qwen3-reranker-4b` 对候选片段重排；
4. 检查回答引用了正确文档。

该目录不包含任何生产数据或凭据。
