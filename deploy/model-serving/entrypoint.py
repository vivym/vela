#!/usr/bin/env python3
"""Run Qwen3 embedding and reranker vLLM servers on one allocated GPU.

The parent process owns the pod lifecycle and exposes a small compatibility
proxy on :envvar:`PROXY_PORT`. vLLM remains the implementation of both model
APIs; the proxy only gives RAGFlow one stable endpoint and health check.
"""
from __future__ import annotations

import http.client
import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


EMBED_PORT = int(os.getenv("EMBED_PORT", "8000"))
RERANK_PORT = int(os.getenv("RERANK_PORT", "8001"))
PROXY_PORT = int(os.getenv("PROXY_PORT", "8080"))
EMBED_MODEL = os.getenv("EMBED_MODEL", "/models/Qwen/Qwen3-Embedding-4B")
RERANK_MODEL = os.getenv("RERANK_MODEL", "/models/Qwen/Qwen3-Reranker-4B")
MAX_MODEL_LEN = os.getenv("MAX_MODEL_LEN", "8192")
EMBED_GPU_UTIL = os.getenv("EMBED_GPU_MEMORY_UTILIZATION", "0.36")
RERANK_GPU_UTIL = os.getenv("RERANK_GPU_MEMORY_UTILIZATION", "0.36")


def vllm_args(model: str, task: str, port: int, name: str, util: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--model",
        model,
        "--served-model-name",
        name,
        "--task",
        task,
        "--port",
        str(port),
        "--host",
        "0.0.0.0",
        "--dtype",
        "bfloat16",
        "--max-model-len",
        MAX_MODEL_LEN,
        "--gpu-memory-utilization",
        util,
        "--trust-remote-code",
        "--disable-log-requests",
    ]


def reranker_args() -> list[str]:
    return [
        sys.executable,
        "/opt/vela/reranker_server.py",
        "--model",
        RERANK_MODEL,
        "--port",
        str(RERANK_PORT),
        "--gpu-memory-utilization",
        RERANK_GPU_UTIL,
        "--max-model-len",
        MAX_MODEL_LEN,
    ]


def healthy(port: int) -> bool:
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
        conn.request("GET", "/health")
        return conn.getresponse().status < 500
    except OSError:
        return False


class Proxy(BaseHTTPRequestHandler):
    server_version = "vela-qwen3-proxy/0.1"

    def log_message(self, fmt: str, *args: object) -> None:
        print("proxy " + (fmt % args), flush=True)

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/healthz", "/readyz"):
            ok = healthy(EMBED_PORT) and healthy(RERANK_PORT)
            self.send_response(200 if ok else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ready": ok}).encode())
            return
        if self.path == "/metrics":
            self._forward(EMBED_PORT, "/metrics")
            return
        self._forward(EMBED_PORT, self.path)

    def do_POST(self) -> None:  # noqa: N802
        # RAGFlow integrations commonly use /rerank. Keep score spellings too.
        if self.path.startswith("/rerank") or self.path.startswith("/v1/rerank") or self.path.startswith("/v2/rerank") or self.path.startswith("/v1/score") or self.path.startswith("/score"):
            self._forward(RERANK_PORT, self.path)
            return
        self._forward(EMBED_PORT, self.path)

    def _forward(self, port: int, path: str) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else None
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in {"host", "content-length"}}
        try:
            conn.request(self.command, path, body=body, headers=headers)
            response = conn.getresponse()
            payload = response.read()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in {"transfer-encoding", "connection"}:
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(payload)
        except OSError as exc:
            self.send_error(503, str(exc))
        finally:
            conn.close()


class MetricsProxy(BaseHTTPRequestHandler):
    """Expose the embedding engine metrics on the scrape-only port."""

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/metrics":
            self.send_error(404)
            return
        conn = http.client.HTTPConnection("127.0.0.1", EMBED_PORT, timeout=5)
        try:
            conn.request("GET", "/metrics")
            response = conn.getresponse()
            payload = response.read()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in {"transfer-encoding", "connection"}:
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(payload)
        except OSError as exc:
            self.send_error(503, str(exc))
        finally:
            conn.close()

    def log_message(self, _fmt: str, *_args: object) -> None:
        return


def main() -> int:
    children = [
        subprocess.Popen(vllm_args(EMBED_MODEL, "embed", EMBED_PORT, "qwen3-embedding-4b", EMBED_GPU_UTIL)),
        subprocess.Popen(reranker_args()),
    ]
    stopping = threading.Event()

    def stop(_sig: int, _frame: object) -> None:
        stopping.set()
        for child in children:
            if child.poll() is None:
                child.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server = ThreadingHTTPServer(("0.0.0.0", PROXY_PORT), Proxy)
    metrics_server = ThreadingHTTPServer(("0.0.0.0", 9090), MetricsProxy)
    metrics_thread = threading.Thread(target=metrics_server.serve_forever, daemon=True)
    metrics_thread.start()
    server.timeout = 1
    print(f"qwen3 worker proxy listening on :{PROXY_PORT}", flush=True)
    try:
        while not stopping.is_set():
            server.handle_request()
            if any(child.poll() is not None for child in children):
                return 1
    finally:
        server.server_close()
        metrics_server.shutdown()
        metrics_server.server_close()
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
