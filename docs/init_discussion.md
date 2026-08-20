我要做个文生视频（目前主要是 minimax h3）的 api 服务，用很多个机器做服务，每个机器都是 8 卡的机器，请分析一下，整个集群如何管理比较好？这个卡可能也不太稳定，如果出问题，可能要重置驱动，或者做 FLR 的 pcie 重置。

我们的卡pcie 带宽极低，不是传统的 sglang 的部署方案，所以我们做了 fork。大概的方案是一张卡负责 encoder 和 vae decoder，其他 7 张卡负责 dit 部分，尽可能减少卡间互联。

请综合的分析一下，是不是用 k8s 比较合适？有没有更好的方案？推理引擎会采用 sglang的 fork 版本。

对，这样的话我会把之前的判断进一步修正：**K8s 仍然适合，但 K8s 管的是“机器/worker 生命周期”，不是 GPU 调度，更不是每个视频任务的调度。** 你们真正应该做的是一个 **appliance-style serving cluster**：每台 8 卡机器就是一个长期运行、预热好的 H3 Worker。

你的拓扑大致是：

```text
8-GPU H3 Worker

GPU-E:   Encoder + VAE Decoder
           │
           │ embedding / latent
           ▼
GPU-D0 ─┐
GPU-D1  │
GPU-D2  │
GPU-D3  ├── 7-GPU DiT
GPU-D4  │
GPU-D5  │
GPU-D6 ─┘
```

这个架构下，**GPU0~7 是一个不可拆分资源**。

## 1. 我会怎么做整个集群

整体建议是：

```text
                         ┌───────────────┐
Client ─── API Gateway ─▶│  Job Service  │
                         └───────┬───────┘
                                 │
                         persistent queue
                                 │
                         ┌───────▼───────┐
                         │ H3 Scheduler  │
                         └───────┬───────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
     H3 Worker 01           H3 Worker 02           H3 Worker N
     8 GPU appliance        8 GPU appliance        8 GPU appliance
     warm model             warm model             warm model
          │
       heartbeat
          │
    Worker Registry
          │
    health / load /
    model version /
    GPU status
```

**不要做：**

```text
一个请求
   ↓
创建 Kubernetes Job
   ↓
调度 8 GPU
   ↓
load model
   ↓
生成视频
```

这对你们明显是不合适的。

应该是：

```text
K8s:
    管长期运行的 H3 Worker

你们自己的 scheduler:
    管每一个 video generation request
```

Kubernetes 本身支持扩展资源、Device Plugin 和自定义 scheduler，但对你这个 workload，其实没必要让 kube-scheduler 理解 encoder GPU / DiT GPU 这种细节。([Kubernetes][1])

---

# 2. K8s 里面最好把“一台机器”抽象成一个资源

最简单的 MVP 可以直接：

```yaml
resources:
  limits:
    nvidia.com/gpu: 8
```

然后保证一个 H3 Worker Pod 独占整台 8 卡机器。

但长期我更喜欢做成：

```text
marslab.ai/h3-worker = 1
```

每台健康的 8 GPU 机器只提供：

```text
1 × h3-worker
```

而不是：

```text
8 × GPU
```

这样 Kubernetes 看到的是：

```text
Node 01:
    h3-worker = 1

Node 02:
    h3-worker = 1
```

而 H3 Pod 请求：

```text
h3-worker: 1
```

Kubernetes 的 Device Plugin / extended resource 机制本身允许设备厂商或你们自己的插件向 kubelet 暴露这种扩展资源。([Kubernetes][1])

这实际上非常符合你们的机器。

### GPU role 不应该按 CUDA index 固定

不要写：

```python
encoder_gpu = 0
dit_gpu = [1,2,3,4,5,6,7]
```

最好配置成 PCI BDF / GPU UUID：

```text
encoder_vae:
    0000:41:00.0

dit:
    0000:42:00.0
    0000:43:00.0
    ...
```

因为 reset、driver reload、enumeration 变化后，CUDA index 不一定是你真正想绑定的物理卡。NVML 本身就支持通过 PCI Bus ID / UUID 找设备。([NVIDIA Docs][2])

你甚至可以在 node provisioning 阶段生成：

```yaml
h3_topology.yaml

encoder_vae: GPU-xxxx
dit:
  - GPU-yyyy
  - GPU-zzzz
  ...
```

---

# 3. 一个 H3 Worker 最好就是一个 Pod

我不建议：

```text
encoder Pod
+
7-GPU DiT Pod
+
VAE Pod
```

虽然逻辑上漂亮，但没有太大收益。

你们更适合：

```text
H3 Worker Pod
│
├── coordinator
│
├── encoder process     → GPU-E
│
├── DiT process(es)     → GPU-D0~D6
│
└── VAE process         → GPU-E
```

也就是说 **SGLang fork 自己管理这 8 张卡内部的 pipeline**。

尤其你们的核心优化是：

> 为了避开超低 PCIe bandwidth，刻意减少跨 GPU 的 tensor movement。

这应该完全封装在 SGLang fork 内。

K8s 根本不需要知道这些。

---

# 4. 请求调度反而要自己写

这是我认为整个系统最值得自己做的一层。

不要：

```text
Nginx
    ↓
random / round-robin
    ↓
H3 worker
```

因为视频生成任务差异很大。

Scheduler 至少应该知道：

```text
worker health
当前 running jobs
queue depth
预计剩余时间
分辨率
帧数
denoise steps
model / LoRA
是否 warm
GPU memory
过去 1/5/15 min failure rate
```

然后计算一个类似：

```text
estimated_cost =
    frames
  × resolution_factor
  × denoise_steps
  × model_factor
```

最后做：

```text
least estimated work
```

而不是 least connections。

所以一条请求：

```text
POST /v1/videos
```

最好立刻返回：

```json
{
    "job_id": "xxx",
    "status": "queued"
}
```

然后：

```text
queued
  ↓
assigned
  ↓
running
  ↓
encoding / dit / vae
  ↓
uploading
  ↓
succeeded
```

这点对于**卡不稳定**尤其重要。

机器挂了之后：

```text
RUNNING
   ↓
WORKER_LOST
   ↓
RETRY_PENDING
   ↓
另一台 worker
```

而不是 HTTP connection 跟着机器一起死掉。

---

# 5. GPU 故障恢复：这一层不要完全交给 K8s

这里我会做一个非常明确的分层：

```text
K8s Control Plane
        │
        │ node status
        ▼
GPU Health Controller
        │
        ▼
host-level gpu-agent
        │
        ├── kill workload
        ├── gpu reset
        ├── PCIe FLR
        ├── driver reload
        ├── reboot
        └── power cycle
```

其中：

> **gpu-agent 我反而建议做成 host systemd service，而不是普通 Kubernetes Pod。**

原因很实际。

一旦出现：

```text
NVIDIA driver stuck
container runtime stuck
/dev/nvidia* abnormal
PCIe AER
GPU fallen off bus
```

你最不希望的事情就是：

> “负责修 GPU 的程序本身也依赖 GPU/K8s/container runtime 才能运行。”

所以：

```text
systemd
└── marslab-gpu-agent
```

最好永远活在 host OS。

Kubernetes 官方的 Node Problem Detector 支持自定义健康检查；NVIDIA 现在也已经有 NVSentinel 这种 GPU node fault detection / quarantine / remediation 设计，所以“检测 → 摘流量 → drain → remediation”的总体设计方向是很成熟的。([GitHub][3])

---

# 6. 我会定义一个明确的恢复阶梯

例如：

| Level | 动作                          | worker 状态   |
| ----- | --------------------------- | ----------- |
| L0    | restart H3 process          | unhealthy   |
| L1    | 清 CUDA process / context    | draining    |
| L2    | `nvidia-smi --gpu-reset`    | maintenance |
| L3    | PCIe FLR                    | maintenance |
| L4    | unload/reload NVIDIA driver | maintenance |
| L5    | reboot node                 | offline     |
| L6    | BMC power cycle             | offline     |
| L7    | quarantine                  | disabled    |

这里特别重要的是：

## 不要一发现 GPU fault 就立刻 reset。

先：

```text
GPU error
   ↓
worker = DRAINING
   ↓
scheduler 不再派新任务
   ↓
当前任务结束
或者超过 grace period
   ↓
kill worker
   ↓
reset
```

严重 fault 才立即 hard kill。

NVIDIA 官方的 reset 文档本身也要求 reset 前停止使用 GPU 的应用；不同 GPU/NVLink/NVSwitch 拓扑对单卡 reset 的限制也并不完全相同。([NVIDIA Docs][4])

Linux PCI 层确实提供 FLR，而且只有支持单 function reset 的设备才会暴露相应的 sysfs reset 能力。([Linux Kernel 文档][5])

所以我不会把：

```bash
echo 1 > /sys/bus/pci/devices/.../reset
```

写死成“任何错误都执行”。

应该让 recovery agent 根据 **error class** 选择恢复方法。

---

# 7. 一个非常关键的地方：你们应该“整机摘流量”，但不一定“整机 reset”

比如：

```text
GPU-D3 出问题
```

在你们这个架构里，7-GPU DiT 缺一张基本上整个 worker 就不能继续提供服务。

所以对 serving plane：

```text
1 GPU bad
    =
whole H3 worker unhealthy
```

但是 remediation plane 可以尝试：

```text
只 reset GPU-D3
```

成功后：

```text
health test
↓
重新启动 SGLang
↓
load/warm model
↓
readiness = true
↓
重新加入 scheduler
```

所以：

> **Fault domain 是 GPU，Serving domain 是整台 8-GPU machine。**

我觉得这个概念会让你们整个系统设计简单很多。

---

# 8. K8s 与 driver 的关系，我建议也做得保守一点

因为你已经明确说：

> 卡本身可能不稳定，而且可能需要特殊 reset / driver reload。

所以早期我甚至**不建议让 GPU Operator 掌控 driver 生命周期**。

我会：

```text
Host OS
    fixed kernel
    fixed NVIDIA driver
    fixed firmware/VBIOS
    fixed container toolkit
```

全部版本锁死。

通过：

```text
Ansible / PXE / image
```

管理。

然后 GPU Operator 可以只用其：

```text
device plugin
DCGM
DCGM exporter
metrics
```

之类的部分。

NVIDIA 官方 GPU Operator 本身支持这种部署：如果 host 已预装 driver，可以设置 `driver.enabled=false`，Container Toolkit 预装时同样可以关闭 Operator 对 toolkit 的管理。([NVIDIA Docs][6])

对你们这种“非标准 GPU 环境”，我尤其倾向这一方案。

---

# 9. K8s / Ray / Slurm 到底怎么选

我的判断现在会非常明确：

| 方案                            | 我对你这个场景的判断              |
| ----------------------------- | ----------------------- |
| **Kubernetes + 自研 scheduler** | **最推荐**                 |
| K8s + Ray Serve               | 不推荐作为第一版                |
| Slurm                         | 不适合作为在线 API serving 主框架 |
| 纯 systemd + 自研调度              | 小规模非常好，大规模运维成本开始上升      |
| Nomad                         | 可以，但没有足够理由为了它放弃 K8s 生态  |

Ray Serve 自己还有 replica / actor / resource scheduling 一层，而 Ray 官方生产方案本身也大量构建在 Kubernetes/KubeRay 之上。([Ray Docs][7])

对你们来说：

```text
K8s scheduler
+
Ray scheduler
+
SGLang scheduler
+
你们自己的 video scheduler
```

层次太多。

你其实只需要：

```text
K8s
    管 machine / process

H3 scheduler
    管 video request

SGLang fork
    管 8 GPU 内部执行
```

非常干净。

Slurm 确实有 node health check、drain 等成熟 HPC 节点管理能力。([Slurm 工作负载管理器][8])

但你们做的是：

```text
internet-facing
always-on
low-latency
API serving
rolling deployment
service discovery
health/readiness
```

从架构匹配度来说 K8s 明显更自然。

---

# 10. 最终我会做成四层

这是我目前最推荐你的完整方案：

```text
┌──────────────────────────────────────────┐
│            API / Control Plane           │
│                                          │
│ API Gateway                              │
│ Auth / Rate Limit                        │
│ Job DB                                   │
│ Job Queue                                │
│ H3 Scheduler                             │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│               Kubernetes                 │
│                                          │
│ node = one H3 appliance                  │
│                                          │
│ H3 Worker Pod                            │
│   └── SGLang fork                        │
│       ├── GPU-E encoder/VAE              │
│       └── GPU-D0~D6 DiT                  │
│                                          │
│ readiness / rollout / deployment         │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│           Node Management Plane          │
│                                          │
│ marslab-gpu-agent (systemd)              │
│ node-problem-detector                    │
│ DCGM / NVML / PCIe AER                   │
│                                          │
│ FLR / reset / driver reload / reboot     │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│                Hardware                  │
│                                          │
│ Encoder/VAE GPU + 7 × DiT GPU            │
└──────────────────────────────────────────┘
```

其中最核心的设计原则其实就三条：

**第一，8 张卡对外就是一个 resource。**

**第二，K8s 不调度 video request，只调度长期运行的 H3 worker。**

**第三，GPU remediation 独立于 serving Pod，最好落在 host-level agent。**

如果你们后面会做到 **50～500 台这种规模**，我甚至建议把这个 `marslab-gpu-agent + H3 Scheduler + K8s CRD` 做成你们自己的一个小型 **Video Serving Control Plane**。这部分长期看会比“选哪个 K8s GPU 插件”更有价值，因为它可以真正理解 **H3 的 stage、steps、frames、LoRA、worker warming、GPU failure 和任务重试**。

[1]: https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/?utm_source=chatgpt.com "Device Plugins | Kubernetes"
[2]: https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html?utm_source=chatgpt.com "nvmlDeviceGetClockInfo"
[3]: https://github.com/kubernetes/node-problem-detector?utm_source=chatgpt.com "GitHub - kubernetes/node-problem-detector: This is a place for various problem detectors running on the Kubernetes nodes. · GitHub"
[4]: https://docs.nvidia.com/deploy/nvidia-smi/index.html?utm_source=chatgpt.com "Nvidia-smi Manual"
[5]: https://docs.kernel.org/7.1/driver-api/pci/pci.html?utm_source=chatgpt.com "PCI Support Library"
[6]: https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/getting-started.html?utm_source=chatgpt.com "Installing the NVIDIA GPU Operator"
[7]: https://docs.ray.io/en/latest/serve/production-guide/kubernetes.html?utm_source=chatgpt.com "Deploy on Kubernetes - Ray Serve"
[8]: https://slurm.schedmd.com/slurm.conf.html?utm_source=chatgpt.com "Slurm Workload Manager - slurm.conf"


整个项目取个名字，你觉得叫什么比较合适？以后也会用于大模型推理调度，也是在相同的 gpu 集群上，vela 这个名字是否合适？

合适，而且**如果以后会同时做视频生成和 LLM 推理调度，Vela 反而比 Loom / CineGrid / FrameFleet 更合适**。

因为 Vela 本身没有“视频”语义，不会把项目锁死在某类 workload。它更像一个上层的 **AI inference fleet / serving control plane** 名字，可以自然覆盖：

```text
Vela
├── Gateway
├── Scheduler
├── Worker
├── Node Agent
├── Model Registry
├── Placement
└── Recovery
```

下面既可以跑：

```text
Video:
  MiniMax H3
  Wan
  Seedance
  ...

LLM:
  DeepSeek
  Qwen
  Kimi
  GLM
  ...

Future:
  Image generation
  Multimodal
  Embedding / reranker
```

所以从长期定位看，我会把 **Vela 定义成“GPU 推理集群控制面”**，而不是“视频推理框架”。

比如一句话可以是：

> **Vela — a resilient control plane for large-scale AI inference clusters.**

或者更偏 serving：

> **Vela — distributed scheduling and orchestration for heterogeneous AI inference workloads.**

你们现在这个 H3 方案只是 Vela 的第一个 backend：

```text
                    Vela
                      │
              Global Scheduler
                      │
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
 Video Pool        LLM Pool       Other Pool
       │              │
 H3 Worker        SGLang/vLLM
 8-GPU node       Worker
```

甚至以后同一个物理 GPU 集群里，可以由 Vela 根据机器拓扑和 workload 做 placement：

```text
Node A
8 GPUs
→ H3 worker
→ 1 Encoder/VAE + 7 DiT

Node B
8 GPUs
→ LLM worker
→ EP8

Node C
8 GPUs
→ LLM worker
→ TP4 × 2 replicas
```

这时候 **Vela 的核心抽象就不是 GPU，而是 Worker / Deployment Profile**。

例如：

```yaml
profile: minimax-h3
resource:
  nodes: 1
  gpus: 8
topology:
  encoder_vae: 1
  dit: 7
```

以及：

```yaml
profile: deepseek-v4
resource:
  nodes: 4
  gpus_per_node: 8
parallelism:
  ep: 32
```

Vela scheduler 只负责：

**“什么 workload 应该放在哪些机器，以什么 topology 启动。”**

这个定位其实很有发展空间。

---

不过有一个名字我觉得也值得和 **Vela** 对比一下：**Atlas**。

如果最终项目变成：

> 管整个公司的 GPU fleet，统一承载 Video / LLM / Multimodal inference

那么：

* **Vela**：更轻、更现代，像 serving/runtime 项目
* **Atlas**：更像整个 AI infrastructure control plane
* **Loom**：更像 workload scheduler / execution fabric
* **Forge**：更像 inference factory
* **Vela**：介于 scheduler、serving、cluster manager 之间，比较均衡

我现在会把排名改成：

1. **Vela** — 最推荐，边界最灵活
2. **Atlas** — 如果未来目标是整个 AI 计算集群平台
3. **Loom** — 如果核心卖点一直是复杂调度
4. **Forge** — 如果更强调 AI inference factory

尤其你们还涉及**异常 GPU 自动摘除、FLR、driver reset、重新入池**，Vela 完全可以形成一套很自然的术语：

```text
vela-scheduler
vela-controller
vela-worker
vela-node-agent
vela-health
vela-recovery
vela-cli
```

我会继续用 **Vela**。而且我会刻意避免叫 `vela-video-*`，从第一天就把它设计成 **通用 AI inference orchestration system**。
