# The image contains the serving runtime only. Model weights stay on the GPU
# worker's local PV and are never copied through the image or on each restart.
FROM --platform=linux/amd64 vllm/vllm-openai:v0.10.1.1

COPY entrypoint.py /opt/vela/entrypoint.py
COPY reranker_server.py /opt/vela/reranker_server.py
RUN chmod 0555 /opt/vela/entrypoint.py

ENV HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    VLLM_NO_USAGE_STATS=1 \
    VLLM_WORKER_MULTIPROC_METHOD=spawn

ENTRYPOINT ["python3", "/opt/vela/entrypoint.py"]
