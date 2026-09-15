"""Qwen3-Reranker HTTP adapter running on CPU alongside the GPU embedder."""
from __future__ import annotations

import argparse
import math
from typing import Any

import torch
from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer
import uvicorn


class Request(BaseModel):
    query: str
    documents: list[str]
    instruction: str = "Given a web search query, retrieve relevant passages that answer the query"
    model: str | None = None


PREFIX = '<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be "yes" or "no".<|im_end|>\n<|im_start|>user\n'
SUFFIX = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"


def app_for(model_path: str, max_len: int) -> FastAPI:
    tokenizer = AutoTokenizer.from_pretrained(model_path, padding_side="left", local_files_only=True)
    tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        # CPU BF16 on this host collapses the final yes/no logits for short
        # prompts; float32 preserves the reranker margin while staying within
        # the worker's 64 GiB memory limit.
        model_path, torch_dtype=torch.float32, device_map="cpu", local_files_only=True
    ).eval()
    prefix_tokens = tokenizer.encode(PREFIX, add_special_tokens=False)
    suffix_tokens = tokenizer.encode(SUFFIX, add_special_tokens=False)
    true_id = tokenizer.convert_tokens_to_ids("yes")
    false_id = tokenizer.convert_tokens_to_ids("no")
    app = FastAPI()

    @app.get("/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/rerank")
    @app.post("/v1/rerank")
    @app.post("/v2/rerank")
    def rerank(req: Request) -> dict[str, Any]:
        results = []
        with torch.inference_mode():
            for index, doc in enumerate(req.documents):
                text = f"<Instruct>: {req.instruction}\n<Query>: {req.query}\n<Document>: {doc}"
                body = tokenizer(text, add_special_tokens=False, truncation=True,
                                  max_length=max_len - len(prefix_tokens) - len(suffix_tokens))["input_ids"]
                ids = prefix_tokens + body + suffix_tokens
                logits = model(input_ids=torch.tensor([ids], dtype=torch.long)).logits[0, -1]
                yes_logit = float(logits[true_id])
                no_logit = float(logits[false_id])
                score = 1.0 / (1.0 + math.exp(max(-80.0, min(80.0, no_logit - yes_logit))))
                results.append({"index": index, "relevance_score": score, "text": doc})
        results.sort(key=lambda item: item["relevance_score"], reverse=True)
        return {"results": results}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--max-model-len", type=int, default=4096)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.0)
    args = parser.parse_args()
    uvicorn.run(app_for(args.model, args.max_model_len), host="0.0.0.0", port=args.port)
