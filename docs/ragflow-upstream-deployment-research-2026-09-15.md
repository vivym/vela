# RAGFlow v0.27.2 upstream deployment research

Date: 2026-09-15

Scope: upstream facts for planning a disposable Kubernetes validation deployment; no cluster mutation or image pull was performed.

## Pin and release/chart mismatch

GitHub tag `v0.27.2` resolves through the official Git refs API to commit `a024bea0cd93f39e6652a42bf84dd20c55bc560b` ([API](https://api.github.com/repos/infiniflow/ragflow/git/ref/tags/v0.27.2)). At that commit the repository Helm chart is only chart version `0.1.1` and declares `appVersion: "dev"` ([helm/Chart.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/Chart.yaml)); pin the application image explicitly to `infiniflow/ragflow:v0.27.2`, rather than treating Helm appVersion as the release version.

The upstream chart documents Kubernetes >=1.24 and Helm >=3.10, and installs as a single release with optional dependencies ([helm/README.md](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/README.md)). The chart has no chart dependency lock; all dependency resources are templates in this directory.

## What the official chart actually deploys

Default `DOC_ENGINE` is Infinity. The default images and requested PVC sizes are:

- RAGFlow `infiniflow/ragflow:v0.27.2`; one Deployment replica, ClusterIP web service, optional API service (port 9380) and admin service (port 9381).
- Infinity `infiniflow/infinity:v0.7.3-x64-v3`, one StatefulSet replica, 5Gi RWO PVC.
- MySQL `mysql:8.0.40`, one StatefulSet replica, 5Gi RWO PVC.
- MinIO-compatible `pgsty/silo:RELEASE.2026-08-06T00-00-00Z`, one StatefulSet replica, 5Gi RWO PVC.
- Valkey `valkey/valkey:8`, one StatefulSet replica with persistence enabled and 5Gi PVC.
- Optional Elasticsearch `elasticsearch:8.11.3` or OpenSearch `opensearch:2.19.1`, each one replica and 20Gi PVC; only the selected document engine is rendered.

These defaults are visible in [helm/values.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/values.yaml) (RAGFlow lines 77-122; Infinity 124-137; Elasticsearch/OpenSearch 139-191; MinIO/MySQL/Redis 193-246). They are suitable for a smoke test, not durable production data: every stateful dependency is single replica and the PVCs are very small.

The chart supports an image mirror prefix (`global.repo`) and pull secrets applied to all Pods ([helm/README.md](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/README.md)). For this cluster, mirror every image into the existing internal registry and use immutable digests where possible; do not rely on public `latest` tags.

The chart can disable in-cluster MySQL, MinIO, and Redis and inject external host/port credentials. This is explicitly documented for MySQL, MinIO/S3-compatible storage, and Redis/Valkey ([helm/README.md](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/README.md)). It does not render PostgreSQL. The application Docker environment supports metadata `DB_TYPE=mysql|postgres|gaussdb|oceanbase`, but the Helm templates expose only MySQL connection variables; using PostgreSQL metadata would require an external-service adaptation outside the stock chart ([docker/README.md](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/README.md), metadata section; [helm/templates/env.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/env.yaml)).

S3-compatible object storage is an application service configuration option (access key, secret key, endpoint, bucket, region, addressing/signature options), but the stock Helm values do not provide a structured S3 block; use `ragflow.service_conf` or an external MinIO endpoint and verify generated config before rollout ([docker/README.md service configuration](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/README.md)).

## CPU/GPU and worker separation

The official Compose file has `ragflow-cpu` and `ragflow-gpu` profiles using the same `RAGFLOW_IMAGE`; the GPU profile only adds an NVIDIA device reservation ([docker/docker-compose.yml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/docker-compose.yml)). The container entrypoint starts nginx, the RAGFlow API server, task executors, data sync, and optionally the admin server in one container; flags include `--disable-taskexecutor`, `--disable-webserver`, and `--workers` ([docker/entrypoint.sh](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/entrypoint.sh)). The stock Helm chart creates only one RAGFlow Deployment and does not expose those separation flags or node affinity. Therefore it cannot, as-is, guarantee that API/control processes stay on CPU management nodes while OCR/task execution runs on GPU workers.

DeepDoc layout analysis, OCR, and table recognition are explicitly in-process inside the RAGFlow server using ONNX Runtime; there is no separate DeepDoc service ([docker/README.md](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/README.md)). This matters for the requested placement: scheduling the single chart Deployment on .70/.71 also places DeepDoc/task work there. To honor the CPU/GPU boundary, fork/overlay the chart into at least:

1. a CPU web/API Deployment with `--disable-taskexecutor` (and admin only where required), pinned to .70/.71;
2. a GPU worker Deployment using `--disable-webserver` and worker flags, pinned by the cluster's NVIDIA resource label/device plugin;
3. the same metadata/object/document services and Redis stream queue for Python task executors. Python task_executor imports REDIS_CONN and consumes queue messages (see [rag/svr/task_executor.py](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/rag/svr/task_executor.py), lines 88 and 251-273). NATS is for the optional Go ingestor path, not an extra requirement for the Python split. Set API_PROXY_SCHEME=python in both roles.

The split separates ordinary dataset ingestion, but it does not prove that every API flow avoids local parsing. For example, FileService.parse_docs/parse calls parser chunk methods synchronously and upload_document generates thumbnails ([api/db/services/file_service.py](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/api/db/services/file_service.py), lines 650 and 693-719). For the initial test, use dataset upload/queued parsing and external embedding/chat; defer direct chat-file/agent/browser parsing features until their placement is checked.

Do not assume that merely setting `DEVICE=gpu` in a Pod changes placement; Kubernetes needs an NVIDIA device plugin and a GPU resource request/limit. The official Compose GPU example is evidence of the needed device reservation, not a Kubernetes manifest.

## Embedding and model dependencies

Since v0.22.0 upstream only ships the slim edition without embedding models and no longer appends the -slim suffix. Do not invent a v0.27.2-slim tag ([root README](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/README.md), lines 213-220). The supplied environment documents optional Text Embeddings Inference services:

- CPU image `infiniflow/text-embeddings-inference:cpu-1.8`
- GPU image `infiniflow/text-embeddings-inference:1.8`
- `Qwen/Qwen3-Embedding-0.6B`: 25GB RAM/VRAM
- `BAAI/bge-m3`: 21GB RAM/VRAM
- `BAAI/bge-small-en-v1.5`: 1.2GB RAM/VRAM

([docker/.env](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/.env), lines 257-285). These memory figures are upstream .env comments; no measured sizing was performed here. Actual use depends on model/backend/batch settings. Place all embedding/chat inference on a GPU worker or use an existing endpoint, honoring the established management-node policy even for CPU inference.

The upstream local-model guide supports Ollama, Xinference, vLLM, SGLang, GPUStack, and similar external model servers; RAGFlow is configured with their reachable base URL ([deploy_local_llm.mdx](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docs/guides/models/deploy_local_llm.mdx)). This keeps model-serving workloads on GPU workers while RAGFlow control/API remains on CPU nodes.

## Ingress, APISIX and path assumptions

The chart Ingress example is host-based with path `/`; the template sends all paths to the RAGFlow web Service ([helm/README.md](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/README.md) and [helm/templates/ingress.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/ingress.yaml)). The embedded nginx config serves static files at root and proxies `/api/v1/admin` to 9381 and `/(v1|api)` to 9380 ([helm/templates/ragflow_config.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/ragflow_config.yaml)). Use a dedicated APISIX host such as `ragflow-lab.<domain>` routing prefix `/` to the web Service. Do not initially publish it under a URL subpath; root-relative static/API paths are not documented as subpath-safe. Keep the 9381 admin Service ClusterIP-only and block/restrict /api/v1/admin at the public gateway as well.

## Security and chart gaps to account for

The stock Helm chart puts all environment values, including database/object-store passwords, into one Secret consumed by the RAGFlow Pod and dependency Pods ([helm/templates/env.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/env.yaml)). For validation, create a dedicated namespace and random credentials; do not retain the example passwords in the repository or shell history.

If Elasticsearch or OpenSearch is selected, the chart runs a privileged root init container to set `vm.max_map_count=262144`; the engine container adds `IPC_LOCK` and runs as UID 1000 ([helm/templates/elasticsearch.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/elasticsearch.yaml), [opensearch.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/opensearch.yaml)). This may be disallowed by Pod Security Admission and should be avoided for the first test by using default Infinity, or explicitly approved and isolated.

The optional sandbox executor is high risk: upstream Compose marks it `privileged: true`, mounts `/var/run/docker.sock`, and warns that its /run endpoint executes arbitrary code; the API token is strongly recommended and runner network defaults to `none` ([docker-compose-base.yml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/docker-compose-base.yml)). Do not enable the sandbox for this validation deployment.

The root README declares generic self-hosting prerequisites of 4 CPU cores, 16GB RAM, 50GB disk and x86 images ([README](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/README.md), lines 152-157 and 192-220). These are not a per-component Kubernetes sizing guarantee.

The stock chart defines no CPU/memory resources for RAGFlow, Infinity, MySQL, MinIO, or Redis; only optional ES/OpenSearch default requests (4 CPU, 16Gi) are present ([helm/values.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/values.yaml)). Add explicit requests/limits, probes, and node affinity in a local overlay before applying.


Additional source-level mismatches require an overlay, not a blind helm install:

- **Explicit API mode is required.** Helm values do not set API_PROXY_SCHEME. The entrypoint chooses a Python nginx config when the variable is empty, but only starts API/admin/task processes inside comparisons to python/go/hybrid. The Dockerfile does not set it either. Set env.API_PROXY_SCHEME=python explicitly. This is a source-level startup gap; the actual image still needs runtime verification ([entrypoint.sh](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/entrypoint.sh), lines 199-220 and 299-373; [Dockerfile](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/Dockerfile)).
- **Custom MinIO usernames need MINIO_USER too.** Helm sets MINIO_ROOT_USER; the application template reads MINIO_USER, defaulting to rag_flow. A nondefault root username without MINIO_USER/local service config leaves application credentials mismatched ([env.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/env.yaml); [service_conf.yaml.template](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/service_conf.yaml.template), lines 18-25).
- **External Redis/MinIO ports need service configuration override.** Helm advertises REDIS_PORT/MINIO_PORT, but the application template concatenates fixed :6379 and :9000. Set explicit service_conf host:port when using other ports. PostgreSQL and S3 blocks are commented in the default application template and need to be populated deliberately ([service_conf.yaml.template](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/service_conf.yaml.template)).
- **Redis policy differs.** Helm launches maxmemory 128mb and allkeys-lru; Compose uses volatile-lru. The Python task executor uses Redis streams, so a small allkeys-lru queue can evict persistent queue keys. Set a reviewed persistence/memory/eviction policy for the lab (prefer noeviction with bounded memory and alerting); do not treat chart defaults as queue durability ([redis.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/redis.yaml), lines 62-63; [Compose](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/docker/docker-compose-base.yml), lines 269-285; [task_executor.py](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/rag/svr/task_executor.py), lines 251-273).
- **Admin HTTP paths exist through web nginx.** Keeping Service 9381 private alone is insufficient: stock nginx proxies /api/v1/admin to it on the public web listener. Disable admin or explicitly deny/restrict this path at APISIX ([ragflow_config.yaml](https://raw.githubusercontent.com/infiniflow/ragflow/a024bea0cd93f39e6652a42bf84dd20c55bc560b/helm/templates/ragflow_config.yaml)).
- **Chart policies/probes are incomplete.** No nodeSelector/affinity/tolerations are exposed. RAGFlow/MySQL/MinIO/Redis lack readiness/startup/liveness probes; Infinity has liveness only. No NetworkPolicy, ResourceQuota, or serviceAccount token minimization is present. Complete these in the namespace/local overlay; they are not supplied by the release.

## Practical recommendation for the requested lab

1. Treat this as a disposable `ragflow-lab` release, separate namespace, random Secret credentials, no sandbox and no user data.
2. Keep default Infinity to avoid the privileged ES/OpenSearch sysctl init.
3. Mirror and pre-pull pinned images through the internal registry; the official Docker docs list Huawei Cloud mirrors for the RAGFlow image, but those are fallback examples, not a substitute for an internal registry.
4. Render a split deployment from the outset: one web/API Pod on .70/.71 using --disable-taskexecutor --disable-datasync; one task-executor Pod on a selected healthy GPU worker using --disable-webserver --workers=1 (data sync may run there when enabled). Infinity, MySQL, Valkey and object storage belong on management nodes. Embedding/chat inference belongs on GPU workers or an already available model endpoint. Do not first run the unsplit app on management nodes, since that violates the accepted workload-placement rule.
5. For the user's intended architecture, do not call the stock single Deployment complete. Build a chart overlay that separates CPU API from GPU task executors and validates queue behavior, then route only the web host through APISIX.
6. Before any apply, verify available StorageClass/PVC capacity on .70/.71, GPU device-plugin resources and node labels, internal registry access, APISIX route ownership, and whether a safe RAGFlow split mode is supported by the selected image.

## Evidence boundary

This memo records source facts at commit `a024bea0cd93f39e6652a42bf84dd20c55bc560b`; it does not prove that the chart renders successfully against the current cluster, that images are present in the internal registry, or that RAGFlow's CPU/GPU split works in this environment. Those require a separate, controlled validation.
