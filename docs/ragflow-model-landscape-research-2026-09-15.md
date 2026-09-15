# RAGFlow Embedding 与 Rerank 模型调研（2026-09-15）

## 结论先行

对于当前企业内部、中文为主、需要在 GPU worker 上自托管的 RAGFlow，建议先做以下候选 A/B 测试：

1. **首选基线：`Qwen/Qwen3-Embedding-4B` + `Qwen/Qwen3-Reranker-4B`**。两者均为 Apache-2.0，支持 100+ 语言、32K 上下文；4B embedding 的向量维度最高 2560，reranker 与 embedding 配套，质量和显存开销较平衡。
2. **质量优先：`Qwen/Qwen3-Embedding-8B` + `Qwen/Qwen3-Reranker-8B`**。8B embedding 在 Qwen 模型卡所列的旧版 MTEB 多语评测为 70.58、C-MTEB 为 73.84；该表的对比数据抓取于 2025-05-24/模型卡发布于 2025-06，不能当作 2026 年全榜单的实时排名。8B 组合需要更多显存和吞吐评估。
3. **低资源候选：`Qwen/Qwen3-Embedding-0.6B` + `Qwen/Qwen3-Reranker-0.6B`**。适合先验证服务链路或显存紧张场景；质量应以企业语料测试决定。
4. **许可证允许商业自托管的轻量备选：`BAAI/bge-m3` + `BAAI/bge-reranker-v2-m3`**（MIT + Apache-2.0）。BGE-M3 支持 100+ 语言、8,192 token，并同时提供 dense、sparse、multi-vector 三种检索信号；适合 RAGFlow 的混合检索，但模型较旧，不能仅凭 2024 年结果宣称当前最佳。

Jina v5/v3.5 的质量很有吸引力，但目前模型卡标注 **CC BY-NC 4.0**，明确要求商业使用联系 Jina；企业生产自托管前必须完成法务确认。NVIDIA Nemotron 系列的模型质量和中文/跨语言覆盖值得测试，但许可证为 NVIDIA/OpenMDW 等定制条款，不能按 Apache/MIT 处理。

## 候选模型与可核实证据

| 用途 | 模型 | 规模/上下文/输出 | 语言与质量证据 | 许可证与部署判断 |
|---|---|---|---|---|
| Embedding | `Qwen/Qwen3-Embedding-8B` | 8B；32K；最高 4096 维，支持 MRL（32–4096） | 模型卡：MTEB multilingual 70.58，C-MTEB 73.84；评测表明确是旧表，比较分数抓取于 2025-05-24 | Apache-2.0；适合自托管，显存/吞吐需实测 |
| Embedding | `Qwen/Qwen3-Embedding-4B` | 4B；32K；最高 2560 维，MRL 32–2560 | 同一模型卡表：MTEB multilingual 69.45，C-MTEB 72.27 | Apache-2.0；推荐首轮生产候选 |
| Embedding | `Qwen/Qwen3-Embedding-0.6B` | 0.6B；32K；最高 1024 维，MRL 32–1024 | 同表：MTEB multilingual 64.33，C-MTEB 66.33 | Apache-2.0；适合低延迟/低显存验证 |
| Rerank | `Qwen/Qwen3-Reranker-8B` | 8B；32K；cross-encoder | Qwen 模型卡自测：MTEB-R 69.02、C-MTEB-R 77.45、MMTEB-R 72.94、MTEB-Code 70.19；候选由 Qwen3-Embedding-0.6B 初排 | Apache-2.0；质量优先候选 |
| Rerank | `Qwen/Qwen3-Reranker-4B` | 4B；32K | 同一自测：MTEB-R 69.76、C-MTEB-R 75.94、MMTEB-R 72.74、MTEB-Code 69.97 | Apache-2.0；首轮推荐；注意这些不是独立实时 leaderboard 结果 |
| Rerank | `Qwen/Qwen3-Reranker-0.6B` | 0.6B；32K | 同一自测：MTEB-R 65.80、C-MTEB-R 71.31、MMTEB-R 66.36、MTEB-Code 67.28 | Apache-2.0；低资源候选 |
| Embedding | `jinaai/jina-embeddings-v5-text-small` | 677M；32K；1024 维，MRL 32–1024；119+ 语言 | 模型卡称 MTEB English v2 71.7、MMTEB 67.7，发布 2026-02-18 | **CC BY-NC 4.0**；商业生产需 Jina 授权 |
| Embedding | `jinaai/jina-embeddings-v5-text-nano` | 239M；32K；768 维，MRL 32–768 | 模型卡称 MTEB English v2 71.0、MMTEB 65.5，发布 2026-02-18 | **CC BY-NC 4.0**；适合实验/非商业验证 |
| Rerank | `jinaai/jina-reranker-v3.5` | 0.6B；131K；listwise，一次可排多个候选 | 模型卡表：BEIR 63.20、MIRACL 74.11、RTEB 70.95、Struct-IR 48.3；比 v3 BEIR +1.10，宣称 1.22–1.56× 推理加速（A100/FlashAttention-2） | **CC BY-NC 4.0**；商业自托管需授权；接口与 v3 兼容 |
| Embedding/Hybrid | `BAAI/bge-m3` | 0.6B；8K；1024 维 | 支持 100+ 语言；dense+sparse+ColBERT 多向量；模型卡结果主要为 2024 年，需本地复测 | MIT；适合混合检索，但不是最新质量冠军 |
| Rerank | `BAAI/bge-reranker-v2-m3` | 0.6B；模型卡支持 multilingual | 轻量多语 cross-encoder；模型卡示例和旧评测可复现 | Apache-2.0；稳妥兼容备选 |
| Embedding | `perplexity-ai/pplx-embed-context-v1-4b` | 4B；32K；2560 维；MRL；INT8/BINARY 原生量化 | 模型卡定位 web-scale/contextual chunk retrieval；未发现可直接与 Qwen/C-MTEB 横比的中文实时分数 | MIT；可自托管，需验证 transformers/vLLM 与 RAGFlow 适配 |
| Embedding | `nvidia/llama-nemotron-embed-1b-v2` | 1B；8K；可配 384/512/768/1024/2048 维 | NVIDIA 模型卡覆盖 multilingual/cross-lingual，报告 MIRACL/MLQA/MLDR/BEIR/TechQA；部分评测是内部 hard-negative 子集 | NVIDIA Open Model + Llama 条款；法务审查后再生产 |
| Rerank | `nvidia/llama-nemotron-rerank-1b-v2` | 1B；8K；26 种语言（含中文）；cross-encoder | 模型卡报告多语 QA 检索能力；需要 vLLM >=0.14 和指定 chat template | OpenMDW-1.1 + Llama 条款；法务审查 |
| API（闭源） | Voyage `voyage-4-large` / Cohere `embed-v4.0` | API 服务；Voyage 4 系列 32K、256/512/1024/2048 维 | 官方文档标注 Voyage-4-large 为其 general-purpose/multilingual quality 首选；Cohere 当前文档列出 `embed-v4.0`、`rerank-v4.0-pro/fast` | 无本地权重；需外网、API key、数据出境评估；不适合当前内网零外联目标 |
| API（闭源） | Voyage `rerank-2.5` / Cohere `rerank-v4.0-pro` | API cross-encoder | Voyage 官方文档推荐 rerank-2.5/2.5-lite；单文档对可达 32K，总 token 600K | 闭源 API；用于质量对照或未来合规批准后接入 |

## 关于“当前最好”的判断边界

- MTEB、MMTEB、C-MTEB、BEIR、MIRACL 的任务集合、语言、版本、候选集和 pooling 不同，分数不可直接排序。模型卡自报结果与 MTEB 在线 leaderboard 也不是同一证据层级。
- Qwen3 模型卡中的“多语榜第一（70.58）”是 **2025-06 的声明**；Jina v5/v3.5、Qwen3-VL 等后续模型已经发布，因此只能作为历史基线。应固定一个日期和 leaderboard 数据导出后再比，或在企业语料上使用同一检索协议。
- Reranker 分数依赖第一阶段 embedding、top-k、chunk 长度和模板。Qwen 的表使用 Qwen3-Embedding-0.6B 初排；不能把该表直接解释为所有 embedding 组合下的排序质量。
- 真实 RAG 质量还受 chunking、OCR、BM25/hybrid、top-k、召回延迟和生成模型影响。上线前至少以企业中文问答集测 Recall@k、nDCG@10、MRR、端到端答案引用准确率及 p95 延迟。

## 给当前 RAGFlow 验证阶段的落地建议

1. 先在 GPU worker 上部署 Qwen3 4B 配对，固定 embedding 维度（建议 1024 或 1536，需与 Infinity/向量索引一致），固定 query instruction；不要中途更换维度而复用旧索引。
2. 用 0.6B 配对做低显存基线，用 8B 配对做质量上界候选；三组使用同一批文档、chunk、top-k 和问题集。
3. 如果企业文档含大量扫描表格/图片，再单独评估 **Qwen3-VL-Embedding-2B/8B + Qwen3-VL-Reranker-2B/8B**。其模型卡标称 30+ 语言、32K，可处理文本、图片、截图和视频；Qwen 自测中 VL-Embedding-8B 在 MMEB-V2 全部 78 个数据集为 77.9，VL-Reranker-8B 的 MMEB-V2 Retrieval Avg 为 79.2（均为模型卡自测，不能直接等同 MTEB 实时榜单）。这属于多模态路径，不能替代纯文本 RAG 的基准。
4. Jina v5/v3.5、NVIDIA、PPLX 作为第二阶段候选，先做许可证和 serving 兼容性核查；不要因为 benchmark 分数高就直接导入生产。
5. RAGFlow 的 embedding/rerank 接口需要确认模型服务的 OpenAI-compatible API、批量输入、最大 token、返回维度和 score 格式；必要时用独立 vLLM/TEI 服务，通过 RAGFlow 的自定义模型配置接入。

## 官方来源（访问日期 2026-09-15）

本次模型元数据查询固定的 Hugging Face revisions：Qwen3-Embedding-0.6B
`97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`、4B
`5cf2132abc99cad020ac570b19d031efec650f2b`、8B
`1d8ad4ca9b3dd8059ad90a75d4983776a23d44af`；Qwen3-Reranker-0.6B
`e61197ed45024b0ed8a2d74b80b4d909f1255473`、4B
`22e683669bc0f0bd69640a1354a6d0aebcfeede5`、8B
`77d193c791ed757ca307ee72715aa132723da912`。模型卡的 `main` 分支可能继续变化，
实际部署应使用 digest/revision 并记录 tokenizer、serving 镜像版本。

- Qwen3 Embedding/Reranker 模型卡（Apache-2.0、MTEB/C-MTEB 与配套 rerank 评测）：<https://huggingface.co/Qwen/Qwen3-Embedding-8B>、<https://huggingface.co/Qwen/Qwen3-Reranker-8B>
- Qwen3-VL Embedding/Reranker 模型卡：<https://huggingface.co/Qwen/Qwen3-VL-Embedding-8B>、<https://huggingface.co/Qwen/Qwen3-VL-Reranker-8B>
- Jina v5 text small/nano 模型卡及许可证：<https://huggingface.co/jinaai/jina-embeddings-v5-text-small>、<https://huggingface.co/jinaai/jina-embeddings-v5-text-nano>
- Jina reranker v3.5 模型卡、BEIR/MIRACL/RTEB/Struct-IR 表：<https://huggingface.co/jinaai/jina-reranker-v3.5>
- BGE-M3 与 bge-reranker-v2-m3 模型卡：<https://huggingface.co/BAAI/bge-m3>、<https://huggingface.co/BAAI/bge-reranker-v2-m3>
- Perplexity PPLX embedding 模型卡：<https://huggingface.co/perplexity-ai/pplx-embed-context-v1-4b>
- NVIDIA Nemotron embedding/rerank 模型卡：<https://huggingface.co/nvidia/llama-nemotron-embed-1b-v2>、<https://huggingface.co/nvidia/llama-nemotron-rerank-1b-v2>
- MTEB 在线 leaderboard（实时榜单，需记录导出时间和任务筛选）：<https://huggingface.co/spaces/mteb/leaderboard>
- Voyage 官方 embedding 文档（Voyage-4 系列）：<https://docs.voyageai.com/docs/embeddings>
- Voyage 官方 rerank 文档（rerank-2.5 等）：<https://docs.voyageai.com/docs/reranker>
- Cohere 官方模型总览（embed-v4/rerank-v4）：<https://docs.cohere.com/docs/models>
