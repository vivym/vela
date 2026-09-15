#!/usr/bin/env python3
"""Smoke test for the in-cluster Qwen3 embedding/reranker endpoint."""
from __future__ import annotations

import argparse
import json
import urllib.request


def post(url: str, payload: dict) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://qwen3-model-worker.llm-models.svc.cluster.local:8080")
    args = parser.parse_args()
    embedding = post(args.base + "/v1/embeddings", {"model": "qwen3-embedding-4b", "input": ["Vela 使用 GPU worker 执行模型推理。"]})
    if not embedding.get("data"):
        raise SystemExit("embedding response has no data")
    rerank = post(args.base + "/rerank", {"model": "qwen3-reranker-4b", "query": "哪个节点执行模型推理？", "documents": ["CPU 节点运行数据库。", "GPU worker 执行模型推理。"]})
    results = rerank.get("results")
    if not results or results[0].get("relevance_score") is None:
        raise SystemExit("rerank response has no scored results")
    print(json.dumps({"embedding_dimensions": len(embedding["data"][0]["embedding"]), "rerank": results}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
