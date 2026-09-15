# H3 镜像部署与节点选择

2026-09-15。目标是通过 Vela 执行真实 H3 Job，再经 APISIX 验收。
直接运行的 GPU canary 已生成视频；它没有经过正式 Fleet Worker 调度，
不能作为正式 Job 或 Production Launch Receipt。

## 镜像和运行边界

| 组件 | 镜像 | 运行位置与职责 |
| --- | --- | --- |
| Vela Control | `vela-control` | `.70/.71`，业务 API、调度与持久状态协调 |
| Fleet Controller | `vela-fleet-controller` | `.70/.71`，按批准的 ResidencyPlan 创建与保护 Worker |
| Worker Agent | `vela-stage-worker-agent` | GPU 节点，lease、运行时协议和产物传输 |
| H3 Encoder / DiT / VAE Runtime | 首版共用 `vela-h3-stage-runtime` | 按阶段启动独立进程，按 Worker 拓扑运行 |
| CPU 媒体编码 | 包含在 H3 Runtime 镜像中 | 与 VAE 同机，消费本地共享内存，生成 MP4 |

共用 Runtime 镜像不代表单一推理进程。三个阶段可以有各自的实例数和调度；
共享镜像可复用 CUDA、PyTorch、SGLang 和本项目代码层，减少重复构建与分发。
已支持的 AUX 拓扑允许 Encoder/VAE 在同一单槽 Worker 中串行共享 GPU；
是否采用它必须体现在批准的计划中，不能同时授权两个 Worker 使用同一 GPU。
后续若阶段依赖需要分离，可从同一固定基础镜像派生专用镜像。

SGLang 作为固定提交的依赖包含在 Runtime 内，复用它的 resident pipeline；
不额外启动 SGLang Head 或第二套调度器。Vela 负责排队、租约、重试、取消和
产物持久化。视频与音频编码保留在计算节点，避免未压缩帧跨网络传输。

模型权重不进入镜像，不使用网络卷作为运行时模型目录。镜像独立记录代码、
依赖和可执行文件身份，模型 manifest 独立记录文件大小和 SHA256。

## 256GB 优先规则

根据 kubelet 报告的 capacity，51 个当前可报告 GPU 容量的节点已标记为：

- `vela.ai/memory-tier=256gb`：43 台。
- `vela.ai/memory-tier=512gb`：8 台，即 `.66/.58/.59/.60/.62/.63/.64/.65`。
- `.19` 没有当前 GPU capacity，不纳入这次有效分组；这不是新的 DMI 硬件盘点。

首批选择 `.11/server-22` 与 `.12/server-23`，均为 256GB、8×64GiB GPU。
Fleet 按 `member.NodeIdentity` 精确选择 hostname，因此偏好必须落实到
ResidencyPlan 的设备与节点选择，不能只添加一个没有生效的 scheduler preference。
512GB 节点作为容量或兼容性不足时的备选；不因其内存更大而优先放置 H3。

256GB 是主机内存档位，不是 Pod 可用内存。实际部署还需给各阶段明确的
requests/limits，并核对并发加载峰值与合计预算；不能直接在每台机器同时
启动八个未测量内存峰值的 Runtime。

## 已完成的现场前置条件

- 两台节点均已发布本地只读缓存：
  `/var/lib/vela/models/dad0bd33673dee603d107fda712ee22e1e6dca2268748f2b3d723f93c74d5aa1`。
  每台 76 文件、59,510,503,579 字节，逐文件 SHA256 校验通过。
  禁用任何传输子进程后再次验证并复用成功。目录与文件分别为 0555/0444；
  启动时仍需验证，不以节点标签代替校验。
- 该缓存位于现有本地 NVMe/ext4 根卷，没有格式化额外数据盘。
  完成时每台仍有约 762GiB 非 root 可用空间；这里只描述当时快照。
- 临时 rsync 源服务已停止，18734 端口已关闭，源端凭据和本地临时 token 已删除。
- Fleet 两个 CPU 副本已启动。三台 API Server 逐个采用独立 mTLS 客户端配置，
  保留原 PodSecurity 配置；12 项真实 AdmissionReview allow/deny 检查通过。
  临时 allow 测试规则已删除，正式 Fleet 保护规则保留。
- NVIDIA DRA v0.5.0 已仅在 `.11/.12` 上启用，公布 16 个 GPU 设备。
  先停用这两台的 legacy 分配，再启用 DRA，防止双重分配。
  两节点各完成 UUID/PCI BDF 精确 claim、容器内 GPU 核对、第二个相同 GPU
  claim 无法分配的验证。测试 Pod/claim 已清理。
- 没有重启 `.44/.56/.57/.66` 主机或重载 GPU 驱动。`.66` 为采用准入配置
  更新了 RKE2 服务，其 boot ID 保持不变。

上游仍将 DRA GPU allocation 标记为未正式支持。上述结果是现场受控验证，
不是 NVIDIA 对该驱动、GPU 型号和应用组合的生产认证。

## 镜像构建与剩余验收

H3 镜像从隔离且干净的 fast-h3/SGLang/SageAttention 提交构建；不改远端原始
checkout。基础镜像从 `.66` 流式复制到 `.70`，构建在 `.70` 执行。
系统包、Python 和 Rust 引导使用中国镜像，并保留锁文件与下载校验。
符号链接的权限按 Linux 的 0777 语义规范化；文件模式、文件内容和链接目标
仍参与源码快照校验。

## 完整音视频输出

用户明确要求保留完整输出。未发布的时长裁剪改动已撤回，正在构建的
`0b23cb9` Runtime 也从未包含该改动。Encoder/DiT/VAE 生成的完整视频帧和音轨
交给媒体编码器，不按请求时长切片，也不以较短音轨或视频轨作为截断边界。

H3 原生帧数按 `17n+5` 向上对齐；例如 5 秒、24fps 请求生成 124 帧，
视频时长约 5.166667 秒。音频按 40Hz latent 边界生成，此例为 5.175 秒。
Vela 新增显式的 `h3-native-av-v1` OutputSpec 契约，精确检查对齐后的完整
帧数，并检查单一视频轨、单一 32kHz 双声道 AAC 音轨与容器时长。
音频只允许最多一个 AAC packet 的末尾编码填充，不允许短于模型应生成的音频。
其他额外轨道、缺音轨、裁短的视频或音频均拒绝发布。

`GET /v1/projects/{project_id}/jobs/{job_id}/artifacts` 的下载项新增 `media`，
分别返回请求时长、实际视频时长、容器时长、帧数、帧率和音轨属性。
下载 URL 仍指向经校验的完整对象版本。旧 OutputSpec 保留原契约；新契约必须
创建新的不可变 OutputSpec revision，并纳入正式 release，不修改旧认证定义。
数据库 migration 99 和新 Control 镜像尚需随正式发布采用。

本轮 Linux CPU 媒体测试验证了 124 帧全部保留、完整 5.2 秒双声道音频仍在，
并解码核对 5 秒以后的有声尾段。它验证的是编码和输出保留，不是新的 GPU 推理。

## 正式部署的剩余项

`.11/.12` 尚缺正式 host startup 组件：Node Agent、pidfd broker、runtime policy
issuer 与 runtime image maintenance。DRA 和模型缓存准备好，不等于 Worker 已可运行。
还需将 VAE 已编码产物、CPU_MEDIA/thumbnail 的实际图契约和最终产物发布接通。

完整业务部署还要通过以下四步，任一步失败都不能把 canary 当作完成：

1. 完成固定版本 Runtime 镜像构建、镜像内导入与模型缓存挂载检查。
2. 完成真实 release/PKI/配置引用与 ResidencyPlan，启动受 Vela 管理的 Runtime。
3. 提交真实视频 Job，核对三阶段、产物存储、最终 MP4 和 APISIX 下载。
4. 核对重建后的恢复、监控与告警，并记录验证覆盖范围；生产故障演练另行按
   已授权范围执行，不通过业务成功请求推断所有 Production Gates 已通过。

## 证据

- [GPU 内存分组](evidence/gpu-node-memory-tiers-2026-09-15.json)
- [两节点模型缓存与离线复用](evidence/h3-local-model-caches-2026-09-15.json)
- [临时下载服务清理](evidence/h3-model-prefetch-source-cleanup-2026-09-15.json)
- [API Server 配置采用](evidence/fleet-host-activation-2026-09-15.json)
- [真实准入验证](evidence/fleet-admission-live-2026-09-15.json)
- [DRA 镜像身份](evidence/h3-nvidia-dra-image-2026-09-15.json)
- [DRA 两节点迁移](evidence/h3-nvidia-dra-migration-2026-09-15.json)
- [DRA GPU 身份和独占验证](evidence/h3-nvidia-dra-smoke-2026-09-15.json)
- [此前原生 GPU canary](evidence/h3-native-gpu-canary-2026-09-15.json)
