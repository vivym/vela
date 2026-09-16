#!/usr/bin/env python3
"""Small Qwen3-Reranker HTTP adapter using the model card's vLLM path."""
from __future__ import annotations

import argparse
import math
import threading
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoTokenizer
from vllm import LLM, SamplingParams
from vllm.inputs.data import TokensPrompt
import uvicorn


class Request(BaseModel):
    query: str
    documents: list[str]
    instruction: str = "Given a web search query, retrieve relevant passages that answer the query"
    model: str | None = None


PREFIX = '<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be "yes" or "no".<|im_end|>\n<|im_start|>user\n'
SUFFIX = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"


def app_for(model_path: str, max_len: int, gpu_util: float) -> FastAPI:
    tokenizer = AutoTokenizer.from_pretrained(model_path, padding_side="left", local_files_only=True)
    tokenizer.pad_token = tokenizer.eos_token
    llm = LLM(model=model_path, dtype="bfloat16", max_model_len=max_len,
              gpu_memory_utilization=gpu_util, enable_prefix_caching=True)
    prefix_tokens = tokenizer.encode(PREFIX, add_special_tokens=False)
    suffix_tokens = tokenizer.encode(SUFFIX, add_special_tokens=False)
    body_budget = max_len - len(prefix_tokens) - len(suffix_tokens) - 1
    if body_budget <= 0:
        raise ValueError("max_model_len cannot fit the reranker template")
    true_id = tokenizer.convert_tokens_to_ids("yes")
    false_id = tokenizer.convert_tokens_to_ids("no")
    sampling = SamplingParams(temperature=0, max_tokens=1, logprobs=20)
    generate_lock = threading.Lock()
    app = FastAPI()

    @app.get("/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/rerank")
    @app.post("/v1/rerank")
    @app.post("/v2/rerank")
    def rerank(req: Request) -> dict[str, Any]:
        if not req.documents:
            return {"results": []}
        prompts = []
        for doc in req.documents:
            # Use the exact scoring template: the cached tokenizer's chat
            # template expects query/document roles, not ordinary user chats.
            text = f"<Instruct>: {req.instruction}\n<Query>: {req.query}\n<Document>: {doc}"
            body = tokenizer(text, add_special_tokens=False, truncation=True,
                             max_length=body_budget)["input_ids"]
            prompts.append(TokensPrompt(prompt_token_ids=prefix_tokens + body + suffix_tokens))
        with generate_lock:
            outputs = llm.generate(prompts, sampling, use_tqdm=False)
        results = []
        for index, output in enumerate(outputs):
            logprobs = output.outputs[0].logprobs[0]
            yes = logprobs.get(true_id)
            no = logprobs.get(false_id)
            yes_logit = yes.logprob if yes is not None else -10.0
            no_logit = no.logprob if no is not None else -10.0
            delta = no_logit - yes_logit
            score = 1.0 / (1.0 + math.exp(max(-80.0, min(80.0, delta))))
            results.append({"index": index, "relevance_score": score,
                            "text": req.documents[index]})
        results.sort(key=lambda item: item["relevance_score"], reverse=True)
        return {"results": results}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--max-model-len", type=int, default=8192)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.36)
    args = parser.parse_args()
    uvicorn.run(app_for(args.model, args.max_model_len, args.gpu_memory_utilization), host="0.0.0.0", port=args.port)
