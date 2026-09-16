# Qwen3 model worker

This bundle runs `Qwen/Qwen3-Embedding-4B` and `Qwen/Qwen3-Reranker-4B` in one
worker Pod, requesting **two `nvidia.com/gpu` resources**. Both models use
vLLM 0.10.1.1 in BF16. Embedding uses the pooling API; reranking uses a
`vllm.LLM` engine with the model card's one-token `yes`/`no` scoring method.
Each engine has a separate GPU (`CUDA_VISIBLE_DEVICES=0` and `1`). Transformers
is used only for tokenization; there is no CPU model inference fallback.

The Pod is pinned to `server-53` (`10.1.201.54`, memory tier `256gb`). Model
weights are read from a readonly local PV backed by:

```
/home/user/models/Qwen/Qwen3-Embedding-4B
/home/user/models/Qwen/Qwen3-Reranker-4B
```

`HF_HUB_OFFLINE=1` and `TRANSFORMERS_OFFLINE=1` prevent weight downloads at
startup. Kubernetes restarts the processes through the Deployment and health
probes; recovery depends on the same node and local disk being available.
This is a single replica deployment, not automatic failover across workers.
No host reboot was performed for this rollout.

## Build and apply

The deployed `0.2.0` release uses `Dockerfile.release`, which copies the two
current adapters over the already mirrored runtime image at a pinned digest.
`Dockerfile` is the alternative clean runtime build from pinned upstream
vLLM. A clean build can have a different digest and must be verified before
updating the workload. From a management node with registry access:

```sh
docker build -f deploy/model-serving/Dockerfile.release -t \
  10.1.201.70:5005/llm-models/qwen3-model-worker:0.2.0 deploy/model-serving
docker push 10.1.201.70:5005/llm-models/qwen3-model-worker:0.2.0
docker inspect --format '{{index .RepoDigests 0}}' \
  10.1.201.70:5005/llm-models/qwen3-model-worker:0.2.0
```

Use a new version tag for future changes. Record the pushed digest in
`deployment.yaml` before rollout; the manifest already pins the verified
`0.2.0` digest. This task does not establish vulnerability-scan clearance.

```sh
kubectl apply -k deploy/model-serving
kubectl -n llm-models rollout status deployment/qwen3-model-worker --timeout=30m
kubectl -n llm-models get pod,svc,pvc -l app.kubernetes.io/name=qwen3-model-worker -o wide
```

## Endpoints and RAGFlow

Stable endpoint: `http://qwen3-model-worker.llm-models.svc.cluster.local:8080`.

- `/v1/embeddings`: model `qwen3-embedding-4b`, 2560 dimensions.
- `/rerank`, `/v1/rerank`, `/v2/rerank`: model `qwen3-reranker-4b`, returning
  `results` sorted by `relevance_score` and preserving the original `index`.
- `/healthz`, `/readyz`: both child services must respond before the Pod is ready.

The adapter does not implement the `/score` request/response schema. Ports
8000 and 8001 are internal diagnosis ports, subject to NetworkPolicy.
`qwen3-from-ragflow` allows `ragflow-lab` to reach only this worker on 8080;
existing platform policies govern other callers. The Service is ClusterIP.
External application APIs continue to use APISIX.

RAGFlow provider `VLLM`, instance `qwen3-gpu`, uses base URL
`http://qwen3-model-worker.llm-models.svc.cluster.local:8080/v1`. Its tenant
defaults are `qwen3-embedding-4b@qwen3-gpu@VLLM` and
`qwen3-reranker-4b@qwen3-gpu@VLLM`. Test dataset `qwen3-gpu-smoke` is indexed
and verified; see `tests/ragflow-smoke/README.md` and the dated evidence.
No chat model has been configured, so this verifies retrieval and reranking,
not answer generation.

## Operating limits

Each vLLM engine uses a GPU memory budget of 0.36 and a maximum sequence length
of 4096 tokens. Reranker input reserves space for the scoring template and one
output token, truncating the remaining body to fit. Its synchronous engine
calls are serialized; this deployment is suitable for the current validation
phase and has not undergone sustained capacity testing.

Scoring follows the vLLM model-card example: request the top 20 logprobs and
use `-10.0` for a missing `yes` or `no` token. Scores are therefore approximate
when either token is absent and are not calibrated probabilities. Validate
ranking quality on the team's corpus before setting score cutoffs.

Port 9090 exports **embedding** vLLM metrics through the existing PodMonitor.
The reranker adapter does not yet export dedicated request/queue metrics;
Pod health, logs and node GPU metrics are the current diagnostics.

The PV is `Retain` and has no filesystem quota. Keep the two model directories
intact and monitor free space. Adding another Pod requires **two more GPUs**
and a separately prepared local cache/PV on its target worker; this pinned
single-PVC manifest is not a multi-node scale-out template.
