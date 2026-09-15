# Qwen3 Embedding/Reranker worker 部署方案

状态：**清单与镜像入口已完成，实际构建、推送和集群 rollout 待执行**。

## 资源决策

`Qwen3-Embedding-4B` 和 `Qwen3-Reranker-4B` 的 BF16 权重合计约 16.1 GB。
将两个独立 Pod 分别申请 GPU 会消耗两张 64 GiB 卡；当前没有 GPU
time-slicing 配置，因此采用一个 Pod、一个容器、一个 GPU，在容器内启动两个
vLLM 进程。初始每个进程 `gpu-memory-utilization=0.36`，总预算约 72%，保留
KV/cache、CUDA context 和碎片空间；`max_model_len=8192`，必须通过并发压测再调高。

Pod 申请 8 CPU / 32 GiB，限制 16 CPU / 64 GiB，并要求
`nvidia.com/gpu: 1`。调度选择 `vela.ai/node-role=gpu-worker`，首个已验证的
256 GiB 节点为 `server-53`，另加 `vela.ai/model-memory=256GiB` 标签作为容量闸门。

## 本地模型缓存

模型权重不进入镜像。`.54 / server-53` 已有：

```
/home/user/models/Qwen/Qwen3-Embedding-4B
/home/user/models/Qwen/Qwen3-Reranker-4B
```

`local-model-pv.yaml` 把 `/home/user/models/Qwen` 暴露为带节点亲和性的
`Retain` PV，Pod 以只读 PVC 挂载 `/models`，并设置
`HF_HUB_OFFLINE=1`、`TRANSFORMERS_OFFLINE=1`。Pod 重建只重新读取本地文件，
不会通过网络传输权重。PVC 容量是申请量，不是 ZFS/文件系统配额，仍需监控真实
占用和模型 revision 校验和。

## 接口与 RAGFlow 对接

`entrypoint.py` 由父进程管理 Embedding vLLM 和 Reranker adapter 两个子进程，并在 8080 提供稳定 ClusterIP
接口：

- `/v1/embeddings` → Embedding vLLM（8000）
- `/rerank`、`/v1/rerank`、`/v2/rerank`、`/score`、`/v1/score` → Reranker vLLM（8001）
- `/healthz`、`/readyz` → 两个子进程均健康后才 Ready

9090 仅导出 Embedding vLLM 的 `/metrics`，复用现有 `llm-models` PodMonitor。
Service 保持 ClusterIP；外部请求仍必须经过 APISIX。

RAGFlow 中 Embedding endpoint 使用
`http://qwen3-model-worker.llm-models.svc.cluster.local:8080/v1`，模型名
`qwen3-embedding-4b`。Reranker endpoint 使用同一主机的 `/rerank`，模型名
`qwen3-reranker-4b`；具体字段映射以 RAGFlow v0.27.2 的 UI/API smoke test
为准，不能只凭 Pod Running 宣称问答链路完成。

## rollout 闸门

1. 以 `deploy/model-serving/Dockerfile` 构建 amd64 镜像，推送至现有
   `10.1.201.70:5005`，扫描后把真实 digest 写入 `deployment.yaml`（当前保留
   `REPLACE_AFTER_BUILD`，因此不会误提交不可拉取镜像）。
2. 在 `.70` 以 root kubeconfig 标记 `server-53`，检查模型目录、文件权限、
   GPU 显存和节点 Ready 状态，然后 `kubectl apply -k deploy/model-serving`。
3. 先验证 `/healthz`、`/v1/embeddings`、`/rerank` 和 9090 metrics，再写入
   RAGFlow 配置；任何模型加载失败都保持 Pod Unready，不回退到外网下载。
4. 观察 GPU 显存、队列、p95 延迟和 RAGFlow 解析/检索结果；只有容量和质量
   达标后才增加副本或提升 token/context 上限。

本次本地验证未能通过跳板登录 `.70`：`user@10.1.201.70` 的密码认证返回
`Permission denied (publickey,password)`。因此镜像 digest 替换、节点标记和
`kubectl` rollout 仍是现场待执行项。
