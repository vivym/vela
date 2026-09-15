#!/usr/bin/env python3
"""Small Qwen3-Reranker HTTP adapter using the model card's vLLM path."""
from __future__ import annotations

import argparse
import math
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


def format_instruction(instruction: str, query: str, doc: str) -> list[dict[str, str]]:
    return [{"role": "system", "content": "Judge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be \"yes\" or \"no\"."}, {"role": "user", "content": f"<Instruct>: {instruction}\n\n<Query>: {query}\n\n<Document>: {doc}"}]


def app_for(model_path: str, max_len: int, gpu_util: float) -> FastAPI:
    tokenizer = AutoTokenizer.from_pretrained(model_path, padding_side="left", local_files_only=True)
    tokenizer.pad_token = tokenizer.eos_token
    llm = LLM(model=model_path, max_model_len=max_len, gpu_memory_utilization=gpu_util, enable_prefix_caching=True)
    suffix = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
    suffix_tokens = tokenizer.encode(suffix, add_special_tokens=False)
    true_id = tokenizer("yes", add_special_tokens=False).input_ids[0]
    false_id = tokenizer("no", add_special_tokens=False).input_ids[0]
    sampling = SamplingParams(temperature=0, max_tokens=1, logprobs=20)
    app = FastAPI()

    @app.get("/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/rerank")
    @app.post("/v1/rerank")
    @app.post("/v2/rerank")
    def rerank(req: Request) -> dict[str, Any]:
        prompts = []
        for doc in req.documents:
            messages = format_instruction(req.instruction, req.query, doc)
            ids = tokenizer.apply_chat_template(messages, tokenize=True, add_generation_prompt=False, enable_thinking=False)
            prompts.append(TokensPrompt(prompt_token_ids=ids[:max_len] + suffix_tokens))
        outputs = llm.generate(prompts, sampling, use_tqdm=False)
        results = []
        for index, output in enumerate(outputs):
            logprobs = output.outputs[0].logprobs[-1]
            yes = logprobs.get(true_id)
            no = logprobs.get(false_id)
            yes_logit = yes.logprob if yes is not None else -10.0
            no_logit = no.logprob if no is not None else -10.0
            score = math.exp(yes_logit) / (math.exp(yes_logit) + math.exp(no_logit))
            results.append({"index": index, "relevance_score": score, "text": req.documents[index]})
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
