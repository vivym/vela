# Qwen3 model worker deployment evidence

- Image: `10.1.201.70:5005/llm-models/qwen3-model-worker:0.1.4`
- Manifest digest: `sha256:61d787ec85f10514187b5d5e0da7d84fda9065047f6d80d93b22125f2034497f`
- Base image copied to the private registry from `vllm/vllm-openai:v0.10.1.1`,
  pinned to `sha256:d731ee65c044ae0977421eed3d93f931d4b7d79614394184c939db35b8f28fc2`.
- Placement: `server-53` (`10.1.201.54`), one `nvidia.com/gpu`, local model PVC
  `llm-models/qwen3-models`, `ReadOnlyMany`, `Retain`.
- Pod: `qwen3-model-worker-85c8d6b55c-lsqts`, `1/1 Running`, zero restarts at
  verification time.
- Embedding smoke: `POST /v1/embeddings` returned a 2560-dimensional vector.
- Reranker smoke: `POST /rerank` returned order `[1, 0]` for the relevant GPU
  worker document over an unrelated CPU document; scores were `0.999870` and
  `0.479553`.
- Runtime: Embedding uses GPU vLLM; Reranker uses CPU Transformers in the same
  Pod. This avoids two independent CUDA allocators exhausting one card's KV
  cache budget.
- Admission: `vela-application-pods` now allows only the fixed
  `llm-models/qwen3-models` PVC claim in `llm-models`; other persistent storage
  remains denied.

RAGFlow provider and knowledge-base UI configuration remain a separate step;
the worker endpoint is ready at
`http://qwen3-model-worker.llm-models.svc.cluster.local:8080`.
