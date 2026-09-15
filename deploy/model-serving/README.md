# Qwen3 model worker

This bundle runs `Qwen/Qwen3-Embedding-4B` and `Qwen/Qwen3-Reranker-4B` in one
worker Pod. The Pod requests one `nvidia.com/gpu`; the image contains vLLM and
the lifecycle/protocol adapters, while the two model directories are read from
a local PV. Embedding uses vLLM's pooling API. Reranking uses a Transformers
CPU adapter around the model card's one-token `yes`/`no` logit method, because
two independent vLLM CUDA allocators cannot share this card's cache budget.
Restarting the Pod therefore reuses the node cache and does not download model
weights.

The first placement is deliberately pinned to `server-53`, the verified
256-GiB GPU node that currently contains:

```
/home/user/models/Qwen/Qwen3-Embedding-4B
/home/user/models/Qwen/Qwen3-Reranker-4B
```

Before applying, verify the node name and its existing memory label:

```sh
kubectl get node server-53 -o jsonpath='{.metadata.labels.vela\\.ai/memory-tier}{"\\n"}'
```

Build and promote the image through the existing release registry. The
admission policy requires the resulting digest in `deployment.yaml`; replace
`REPLACE_AFTER_BUILD` only after the image has been scanned and its digest is
recorded:

```sh
docker build -f deploy/model-serving/Dockerfile -t \
  10.1.201.70:5005/ragflow-lab/qwen3-model-worker:0.1.0 deploy/model-serving
docker push 10.1.201.70:5005/ragflow-lab/qwen3-model-worker:0.1.0
docker inspect --format '{{index .RepoDigests 0}}' \
  10.1.201.70:5005/ragflow-lab/qwen3-model-worker:0.1.0
```

Apply the PV and workload from a management node with the platform kubeconfig:

```sh
kubectl apply -k deploy/model-serving
kubectl -n llm-models rollout status deployment/qwen3-model-worker --timeout=30m
kubectl -n llm-models get pod,svc,pvc -l app.kubernetes.io/name=qwen3-model-worker -o wide
```

The stable in-cluster endpoint is
`http://qwen3-model-worker.llm-models.svc.cluster.local:8080`. It accepts
OpenAI embeddings at `/v1/embeddings`, reranker requests at `/rerank` (also
`/v1/score`), and exposes `/healthz` and `/readyz`. Direct engine ports 8000
and 8001 are kept ClusterIP-only for diagnosis. Port 9090 exports the
embedding engine metrics to the existing `llm-models` PodMonitor.

The validated settings use 20% GPU memory for the embedding engine and a 4,096
token limit; the CPU reranker has no GPU reservation. Increase the context only
after a concurrency test confirms that the embedding engine remains healthy. A second Pod would
consume a second physical GPU, so scale out by adding another labelled worker
and another local PV rather than splitting one GPU across Pods.

The local PV is `Retain` and has no filesystem quota. Keep model files under
the two named directories, monitor free space, and prefetch/verify revisions
before changing them. The checked revisions are recorded in the model
landscape research document.
