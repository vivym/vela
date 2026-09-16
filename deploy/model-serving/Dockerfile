# The image contains the serving runtime only. Model weights stay on the GPU
# worker's local PV and are never copied through the image or on each restart.
FROM --platform=linux/amd64 vllm/vllm-openai:v0.10.1.1@sha256:d731ee65c044ae0977421eed3d93f931d4b7d79614394184c939db35b8f28fc2

COPY entrypoint.py /opt/vela/entrypoint.py
COPY reranker_server.py /opt/vela/reranker_server.py
RUN chmod 0555 /opt/vela/entrypoint.py
# The vLLM architecture probe calls getpass.getuser() while the Pod runs as
# UID 1000.  Keep the restricted non-root runtime and provide the corresponding
# passwd/group records in the otherwise minimal base image.
RUN grep -q '^vela:x:1000:' /etc/passwd || echo 'vela:x:1000:1000:Vela runtime:/tmp:/usr/sbin/nologin' >> /etc/passwd \
    && grep -q '^vela:x:1000:' /etc/group || echo 'vela:x:1000:' >> /etc/group

ENV HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    VLLM_NO_USAGE_STATS=1 \
    VLLM_WORKER_MULTIPROC_METHOD=spawn

ENTRYPOINT ["python3", "/opt/vela/entrypoint.py"]
