# Qwen3 Embedding/Reranker worker 部署方案

2026-09-16 更新：**按用户授权改为两张 GPU，两个模型均通过 vLLM 部署；镜像入私仓、集群部署、RAGFlow 配置和测试知识库检索重排均已完成。**

## 最终资源配置

首个实例位于 `server-53`（`10.1.201.54`），该节点内存档位为 `256gb`。
一个 worker Pod 内运行两个独立 vLLM 引擎：Embedding 使用容器 GPU 0，
Reranker 使用容器 GPU 1。每个引擎使用 BF16、`gpu_memory_utilization=0.36`、
`max_model_len=4096`。Kubernetes 显式申请两张 `nvidia.com/gpu`，不依赖
GPU time-slicing，也没有 CPU Transformers 推理回退。

Pod 申请 8 CPU / 32 GiB，限制 16 CPU / 64 GiB；节点选择要求
`vela.ai/node-role=gpu-worker`、`vela.ai/memory-tier=256gb` 和固定主机名。
单副本、`Recreate` 更新策略；一个引擎退出会使整个 worker 重启。
Deployment 和探针负责恢复，主机启动后由现有 RKE2 服务恢复工作负载。
本次未重启主机，也未进行主机重启演练。

## 本地模型缓存和镜像

模型权重位于 `.54` 的：

```
/home/user/models/Qwen/Qwen3-Embedding-4B
/home/user/models/Qwen/Qwen3-Reranker-4B
```

`local-model-pv.yaml` 将 `/home/user/models/Qwen` 作为带节点亲和性的 `Retain`
PV，容器只读挂载 `/models`。离线环境变量禁止启动时下载权重。这里是现有本地
文件系统，未格式化数据盘。PVC 容量不是文件系统配额；仍需监控本地磁盘空间。

镜像为私仓 `10.1.201.70:5005/llm-models/qwen3-model-worker:0.2.0`，
实际 digest 已固定在 Deployment。`Dockerfile.release` 记录本次基于已缓存
vLLM 运行环境的增量构建，`Dockerfile` 提供从固定上游 digest 重建运行环境的路径。
镜像中只有运行环境和适配器，不包含权重。

## 接口和 RAGFlow

父进程管理两个 GPU 子进程，并提供 8080 协议代理：

- `/v1/embeddings` → Embedding vLLM OpenAI server（8000），2560 维。
- `/rerank`、`/v1/rerank`、`/v2/rerank` → Reranker adapter（8001），内部调用
  `vllm.LLM.generate` 获取一个 token 的 yes/no 分数；不是 CPU 推理。
- `/healthz`、`/readyz` → 两个子服务均响应后才 Ready。

Reranker 使用明确的 Qwen3 评分模板构造 query/document 输入，避免缓存 tokenizer
的特殊 chat-template 角色要求导致丢失查询和文档。请求之间串行调用同步 vLLM 引擎。
当前采用模型卡示例的 top-20 logprobs 和缺失 token 的 `-10.0` 回退，因此极端分数
是近似值，不应视为校准概率。输入正文会按 4096 上限减去模板和输出预算后截断。

RAGFlow 的 `VLLM / qwen3-gpu` 实例使用
`http://qwen3-model-worker.llm-models.svc.cluster.local:8080/v1`，默认模型为：

- Embedding：`qwen3-embedding-4b@qwen3-gpu@VLLM`
- Reranker：`qwen3-reranker-4b@qwen3-gpu@VLLM`

NetworkPolicy 将新增的 RAGFlow 入站权限限定到这个 worker 的 8080。
Service 为 ClusterIP；外部应用 API 仍走 APISIX。

## 验收与边界

测试知识库 `qwen3-gpu-smoke` 包含两份中文 Markdown，已完成解析、向量化、索引。
中文检索分别命中管理面文档和双 GPU 模型文档；worker 日志确认 RAGFlow 同时调用了
Embedding 和 Reranker 接口。4 并发、12 次混合请求通过，Pod 保持 Ready、零重启。
完整结果见 [部署证据](qwen3-model-worker-evidence-2026-09-16.md)。

当前闭环是文档索引、检索和重排。没有配置 Chat 模型，因此尚无生成式问答。
长期吞吐、长文档质量、故障切换与主机重启恢复不属于本次已验证结果。
9090 导出 Embedding 指标；Reranker 专用请求/队列指标尚未实现。
固定节点和本地缓存不支持节点故障时自动迁移；扩容需预先准备新的两张 GPU 和本地 PV。
