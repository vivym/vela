# Vela 开发与运维交接：H3 三任务发布及 503 恢复

交接日期：2026-09-20（Asia/Shanghai）。面向接手开发、部署和运维的同事。
本文件的现场状态来自同日修复过程；本次整理和提交没有重新登录集群。
接手时必须刷新现场状态，尤其不能把维护期间的快照当作维护后的结果。

## 1. 接手结论与工作目标

当前首要任务是恢复正式模型 `minimax-h3` 的 503，并完成真实 API 闭环。
用户的完整目标是 T2VA、Ref2VA、FL2VA 三类任务全部正式发布，支持任务提交、
Jobs 列表/详情、完整音视频及缩略图交付、幂等、计费和中转站对账。
同时保留旧 `minimax-h3-live-validation` 的独立路由和已有调用。

**尚未完成上述目标。** 最近修复仅证明 `.25` 的 Ref2VA Encoder warmup
配置可被已部署镜像正确解析、输入摘要正确且图片可解码，未证明真实 GPU
warmup、Worker READY 或 API 成功。历史 T2VA 成功记录不能替代当前验收。

用户已明确：`.70/.71` 正在关机维护，暂时不用处理它们。三成员控制面仅剩
`.66`，etcd 无法取得多数票，Kubernetes API 尚不可用。应等待维护结束，
恢复原集群多数派后再推进服务恢复；不要为这次维护继续强制重建 etcd。

优先阅读[当日故障记录](minimax-h3-503-maintenance-followup-2026-09-20.md)。
其中包含完整 Worker/member 定位线索、镜像 digest、warmup 路径及校验结果、
RKE2 配置回退事实。该记录是本交接的证据依据，不是服务恢复声明。

## 2. 用户已经确定的约束

| 范围 | 必须遵守的要求 |
| --- | --- |
| 节点重启 | 本次修复不重启 GPU 节点；`.44/.56/.57/.66` 明确禁止重启。不能用重启来验收自启动 |
| 管理节点维护 | `.70/.71` 关机是用户确认的维护，当前不操作它们；等待维护结束后刷新状态 |
| 主机分工 | `.70/.71` 为纯 CPU 管理节点，`.66` 被明确接受为管理与 GPU 共用节点；GPU worker 承载模型及相关 CPU 媒体处理 |
| 内存档位 | 区分 256 GB 与 512 GB 节点，H3 优先使用 256 GB 档；不能用主机编号推断内存 |
| 实例布局 | 用户希望 Encoder/DiT/Decoder 独立部署，曾提出 1/8/2；这是部署意图，不是当前实例统计 |
| 权重 | 提前复制并验证本地权重；运行时不得隐式网络下载，不能覆盖正在使用的 revision |
| 输出 | 保留完整输出和音视频，禁止为通过验收裁帧、裁音频或只交付预览 |
| 网关 | 对外 API 统一经过 APISIX，当前域名 `vela.marslab.ic`；复用现有 CA，DNS 由用户手动管理 |
| 中转站 | 中转站不在本地；既有 Project/永久 Key 与较高额度需保留，双方计费需可对账；具体值从受保护材料取得 |
| 可观测性 | 需要节点、GPU、服务、存储、日志、追踪和告警；当前告警查看渠道为 Grafana，外部通知待配置 |
| 存储故障域 | 用户接受 `.70/.71/.66` 上 Longhorn/分布式 MinIO 的集群内复制，但没有独立故障域；不宣称机房级容灾 |
| 系统兼容 | 目标服务器不能升级系统；继续维持无 pidfs 的兼容启动协议及原始 pidfd 的安全交接 |

不要把其他任务的历史清理授权扩大为本次磁盘删除授权。保护客户数据、Worker
journal、startup ledger、证书、模型权重和其他项目文件。

## 3. 代码、文档与提交基线

本地仓库：`/Users/viv/projs/vela`，整理开始时分支为 `main`，HEAD 为
`1f57ad6`。本次分组提交：

| 提交 | 内容 |
| --- | --- |
| `6edb020` | warmup fixture 安装脚本及当日故障记录 |
| `3f5ee11` | 忽略根目录 `.artifacts/`，保留本地构建产物 |
| 本文所在提交 | 详细交接及旧交接文档的历史提示；可用下面命令定位 |

```bash
git status --short --branch
git log -5 --oneline
git log -1 --format='%h %s' -- docs/vela-handoff-2026-09-20.md
```

本次仅本地 commit，未 push。接收者若从远端 clone，需要持有人另行传递这些
提交或推送；不能假定远端已包含交接修复。

`.artifacts/r14-new-1789818854` 和 `.artifacts/r14-startup-1789818884`
约 100 MB，是 Linux ELF 二进制及配套构建清单，未提交、未删除。
它们没有在本轮被确认是当前正式 release；需要时从固定源码重新构建并验证。
被忽略不表示它们已归档到 Git，也不表示可以安全清理。

远端 fast-h3 工作目录为 `.66:/home/marslab/projs/fast-h3`。历史记录表明
它可能含未提交修改；源码 HEAD、dirty diff、SGLang revision、镜像 digest
必须分别核对。本轮该目录下预期的 `src/fast_h3/vela/h3_runtime.py` 不存在，
最终通过 `.25` 的已部署镜像检查了实际代码。不要拿同名本地 checkout 代替
运行镜像，也不要复制旧源码覆盖远端工作目录。

近期主线还包含 Qwen3 公共服务改动。详见
[Qwen3 发布记录](qwen3-public-api-deployment-2026-09-20.md)；恢复 H3 时不要
整体回滚 APISIX、Nginx 或主线而破坏 `/qwen3/*`、RAGFlow 等其他服务。

## 4. 现场入口与安全获取凭据

| 主机/资源 | 入口与用途 |
| --- | --- |
| `.66` | `ssh marslab@100.111.196.116`；实查与 `10.1.201.66` 是同一台主机，不是另一个独立故障域 |
| `.70/.71` | 从 `.66` 再 SSH 到 `user@10.1.201.70` / `.71`；`.70` 有 Ansible 和部署材料，维护期间不连接或启动 |
| `.25` | `user@10.1.201.25`，节点名 `server-43`，本轮定位到的 Encoder Worker 所在节点 |
| 公共入口 | `https://vela.marslab.ic/api`；需正确内网路由和既有私有 CA 信任 |
| RKE2 工具 | 节点上 `/var/lib/rancher/rke2/bin/{kubectl,crictl,ctr}` |
| CRI socket | `unix:///run/k3s/containerd/containerd.sock` |
| kubeconfig | 管理节点 `/etc/rancher/rke2/rke2.yaml`，仅授权运维读取，不能进 Git |

SSH/sudo 密码、API Key、PKI 私钥和数据库凭据不写入本文。用户已提供过登录
凭据，接手人通过安全渠道获取；各账号 sudo 方式可能不同，不能假定一个密码
适用于所有主机。不要用输出配置全文来诊断，因为 RKE2 配置含集群 token。

之前的坏 warmup 文件暴露了写入链路问题：不要让 sudo 读密码的标准输入与
准备写入的文件内容共用不受控的管道。使用受保护文件传输或独立数据通道，
原子替换后重新读取，并调用实际 runtime parser 验证。凭据临时助手已删除，
本文不依赖上一会话的 `/tmp` 登录脚本。

## 5. 哪些问题已解决，哪些仍未知

| 项目 | 已确认事实 | 仍需完成 |
| --- | --- | --- |
| pause 镜像 | `.66` 已缓存 `rancher/mirrored-pause:3.10.2`，etcd 容器已启动 | 不能继续把 pause 拉取失败当成当前根因 |
| etcd | 最近实查仍保留原 `.70/.71` peer，只有自身投票 | 维护结束后检查三成员身份、选主与健康，不只检查端口 |
| `.66` 配置 | 已逐字节还原维护前 RKE2 配置，恢复原 `server` 地址；最后这次回退未重启服务 | 确认维护恢复后生效配置和运行态一致 |
| `.25` warmup | 从非 JSON 修复为完整 Ref2VA spec、本地合成图；同一镜像解析与解码通过 | 真实 GPU warmup、Worker 注册/READY、正式调度 |
| T2VA | 仓库有 9 月 16–17 日完整调用和计费的历史证据 | 9 月 20 日当前服务仍需恢复与回归 |
| Ref2VA/FL2VA | 有权重缓存及后端支持证据，`.25` 运行日志确认 Ref2VA 分区加载 | 两类任务当前完整 API/产物/计费验收尚无本轮通过证据 |
| 实例/队列容量 | 早前有部署与扩容记录 | 当前各任务 E/D/V 数量、READY 数、运行中和排队数均需实时统计 |
| 权重批量复制 | 9 月 18 日记录覆盖 44 台目标，首台已校验 | 不能把旧分发进度当成全部完成；维护后读取逐节点 receipt |
| live 模型 | 本轮没有修改旧路由或 Worker | 不等于当前服务正常，维护结束需回归 |

早前 `.66` 的 `sdc/sdd` I/O 错误线索在上一轮摘要中被提及，对应 Longhorn
卷，本文整理时没有重新核验。不要据此断言物理磁盘损坏或执行 fsck/删除；
先在多数派恢复后核对卷、副本、attach 和底层设备。

## 6. 接手操作顺序及退出条件

### A. 维护期间：保存状态，不继续做灾难恢复

1. 阅读当日故障记录，保留既有备份及 force-test 数据。
2. 不重启节点、不清理 journal、不强行创建旁路推理 Worker，不手填 READY。
3. 可以准备请求、测试和 release 材料；明确这些准备不能替代现场验收。

**历史风险动作要知道：** 交接前更早的恢复尝试曾备份 etcd、在数据副本上
执行 force-new-cluster 测试、生成 `current-force.snap`，并停启过 `.66` 的
RKE2 服务。最近一次核验发现实际运行 etcd 仍在等待原两台 peer。
因此不能写成“从未做过恢复尝试”或“已经切成单节点集群”。
备份目录为 `.66:/var/backups/vela/`，包括
`etcd.pre-cluster-reset-20260920`、`etcd.force-test`、`current-force.snap`
及配置备份。不要自动恢复测试快照；相关配置回退细节见当日故障记录。

### B. 维护结束：先恢复基础服务

用户确认维护结束后，用受权运维身份检查原节点。以下为只读示例，不应在
维护期间反复探测两台关闭的主机：

```bash
# 在具有 kubeconfig 读取权限的管理节点 shell 中执行。
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
/var/lib/rancher/rke2/bin/kubectl --request-timeout=15s get --raw='/readyz?verbose'
/var/lib/rancher/rke2/bin/kubectl --request-timeout=15s get nodes -o wide
/var/lib/rancher/rke2/bin/kubectl --request-timeout=15s -n vela-system get pods -o wide
/var/lib/rancher/rke2/bin/kubectl --request-timeout=15s -n object-store get pods -o wide
/var/lib/rancher/rke2/bin/kubectl --request-timeout=15s -n apisix get pods -o wide
/var/lib/rancher/rke2/bin/kubectl --request-timeout=15s -n longhorn-system get pods -o wide
```

还要用 RKE2 自有 etcd TLS 身份检查 endpoint health/status 和 member list，
确认 cluster ID、成员和 leader；不要将 APISIX 的 etcd 当成 RKE2 etcd。
具体证书路径从现场部署读取，不在命令里猜测。

检查 CNPG primary/副本、NATS quorum/消费者、MinIO quorum/对象读写、
Longhorn 副本和卷、DNS、CA、Nginx upstream、APISIX 后端以及 Vela readiness。
现有手册入口：[控制面](runbooks/observability-control-plane.md)、
[数据库](runbooks/observability-database.md)、[消息](runbooks/observability-messaging.md)、
[对象存储](runbooks/observability-object-store.md)、[存储](runbooks/observability-storage.md)、
[网关](runbooks/observability-gateway.md)。

退出条件：原集群多数派及关键依赖可用；不能仅凭 Kubernetes `/readyz` 通过
就宣布业务恢复。若多数派恢复失败，先保存事件/日志并核对成员，勿直接 reset。

### C. 修复正式 Worker，逐任务恢复容量

先刷新 Fleet plan、WorkerBundle、Worker/member/epoch、ResourceClaim、
Pod UID、runtime 镜像和日志。重新确认 `.25` Worker 是否仍是活动计划目标；
若已被新 epoch 替换，处理新对象，不能直接重启旧 CRI 容器。

最近定位：Worker `c2d58a64-4596-5fbb-be7b-1580790861dd`，member
`8b47088c-8a26-5f7d-b490-3a7c3f2c0244`；这些 ID 仅用于检索，不能假定
维护后仍然对应活动实例。镜像和 warmup 摘要见当日故障记录。

只读日志示例：

```bash
# 在目标 GPU 节点，通过授权 sudo 使用 crictl。
sudo /var/lib/rancher/rke2/bin/crictl \
  --runtime-endpoint unix:///run/k3s/containerd/containerd.sock ps -a
# 从上一步选取当前目标容器 ID，再执行 logs --tail=100 <container-id>。
```

`.25` 当前配置已修好，**不要重新覆盖**。工具
[prepare-h3-encoder-warmup.py](../hack/prepare-h3-encoder-warmup.py) 支持
`--task t2va|ref2va`、绝对路径 `--directory`，仅在显式 `--replace-invalid`
时替换非 JSON 文件；它拒绝覆盖任何已有有效 JSON，不能修复内容合法但
语义错误的旧配置，也不生成 FL2VA/DiT/Decoder fixtures。

使用部署镜像的 `fast_h3.vela.h3_runtime._load_warmup_spec` 复核当前路径。
读取文件通过之后仍必须走正式 Kubernetes/DRA → Node 启动协议 → Runtime
warmup → Worker 注册 → 容量观测链。不能用 parser PASS 伪造 warmup receipt。

统计表应包含每类任务、partition、profile/release digest、节点/IP/内存、
本地 manifest、Encoder/DiT/Decoder 期望数与 READY 数、队列深度/限额及
worker failure reason。先建立最小真实可用链，再根据各阶段耗时扩容。

### D. 三任务与旧模型的真实 API 验收

契约与既有样例：[中转接入文档](api-integration.md)、
[中转全流程历史验收](relay-api-full-validation-2026-09-17.md)。
历史已验收 T2VA 组合为 `model=minimax-h3`、`generation_preset=fast`、
`service_class=standard`、`output_spec=h3-native-av-1344x768-5s-24fps`，
sampling 为 `num_inference_steps=20`、`quality=lossless`。
`standard` 不是 generation preset。维护恢复后还要核对当前 ACTIVE、已认证
Rate Card/profile 组合，不能反复猜测 SKU 或绕过 Admission 限制。

| 验收面 | 要收集的真实证据 |
| --- | --- |
| 公共访问 | 正确信任链、真实域名、认证成功；错误/缺失 Key 被拒绝；两个入口分别验证 |
| 提交与幂等 | `202`、Job ID、冻结报价；原请求重放同一 Job，变更请求且复用幂等键被拒绝 |
| Jobs | 列表、分页/过滤（按实际契约）、详情、执行阶段、终态以及项目隔离 |
| 三类输入 | T2VA 文本、Ref2VA 参考素材、FL2VA 首尾帧都经过正式输入/条件传递链 |
| 交付 | SUCCEEDED 后获取 ArtifactSet，下载完整视频、音频和缩略图，核对大小/digest/媒体流/时长 |
| 计费 | 唯一 Charge、唯一 Visible Completion、固定报价与预留额度一致；可供中转站对账 |
| 取消与失败 | 用专用验收 Job 测试取消及对应计费规则，不取消别人的运行任务；平台失败不产生错误收费 |
| 兼容回归 | `minimax-h3-live-validation`、既有 Job 查询及其他网关路由保持正常 |

现有工具可复用，先阅读实现及 `--help`，再注入私有 credential 文件：

```bash
python3 hack/verify-h3-api-flow.py --help
python3 hack/verify-h3-api-surface.py --help
```

`verify-h3-api-flow.py` 会提交真实任务、下载结果，并通过管理权限只读查询
PostgreSQL 计费事实；不是纯只读检查。`verify-h3-api-surface.py` 需要专用
验收 Project 和另一个 Project 的隔离凭据，还会创建/取消自己的测试 Job。
两者保存 run-directory/幂等状态；超时后续跑同一目录，不另起新键重复计费。
默认媒体检查绑定既有 124 帧、5175 ms 合同，不能不核对 release 就套用到
所有新规格。新增条件任务需要核对脚本覆盖并补充对应真实验收。

固定 IP 的网关检查不能代替中转站真实 DNS 和路由验证。中转站在外部，需
由接手人与其开发者协调受控验收；不要擅自给他人发送消息或暴露密钥。

## 7. 权重、发布与长期运维的待办

权重参考：[Ref2VA/FL2VA 缓存记录](h3-ref2va-fl2va-prefetch-2026-09-18.md)。
其中包含 manifest identity、Ref2VA 专用 transformer/adaLN 路径、已验证
共用文件、原子发布方式、44 台候选节点及 `.70` 上 systemd/receipt 目录。
首台是 `.25`；当前全量完成情况未知。维护后先读 receipt，再决定是否续跑，
不重建重复的大规模传输任务。

Ref2VA 必须使用 Ref2VA partition、专用 ConvRot transformer 和
`MiniMax-H3-adaln-table-ref2va-convrot/steps20.safetensors`；不能因路径相似
替换成 T2VA/FL2VA 的权重。缓存成功只证明文件可用，任务发布还需匹配 image、
StageProfile、接口、Rate Card、ResidencyPlan 及实际输出证据。

服务恢复后，补齐发布前 warmup/fixture/挂载/本地 manifest 预检，避免先加载
几十 GB 权重才发现缺文件。预检失败应阻止发布，不能静默使用另一任务配置。
记录所有组件实际 image digest、host package、配置 revision、Secret/PKI 引用
及 rollback 目标；不用目录名 r14/r17/r31 代替完整版本信息。

平台发布与权限隔离的设计已在 [platform-publishing](platform-publishing/README.md)
中，包括 Git/Helm/Argo CD、Rancher/RBAC、命名空间隔离和 APISIX 路由审批。
这是设计/操作文档索引，哪些已部署需现场核验。可观测性应覆盖维护后的
错误率、503、队列年龄、阶段耗时、GPU、磁盘、复制/仲裁、对象读写、日志和
trace；Grafana 为当前告警查看渠道，外部通知仍待配置。

## 8. 本轮本地验证与交付边界

- 在实际部署镜像中完成旧配置失败复现和新配置 parser/参考图解码通过，详见当日记录。
- 提交前本地验证了 T2VA/Ref2VA fixture 生成、输入长度/摘要、已有 JSON 保护、非 JSON 显式替换。
- 本次只涉及一个 Python 运维工具和文档/忽略规则；没有修改 Go/SQL/Proto，未运行全仓 Go 或集成测试。
- 本文的链接、脚本语法、暂存差异、敏感内容和提交范围在交付前检查。
- 没有新的远端发布、重启、数据库变更、API 成功任务或计费验收。

接手完成的标准：能解释当前维护阻塞；能找到实际运行版本、权重及安全材料；
能在不绕过 authority/epoch 的前提下恢复 Worker；并以三类真实 Job 和计费
记录证明最终结果。未闭合项必须明确列出，不用“所有问题已经解决”代替证据。

## 9. Suggested skills / 建议使用的技能

- `diagnosing-bugs`：先建立可以复现原始错误的反馈，再修复并回归；本轮同镜像 parser 是局部反馈，后续应增加真实 API 反馈。
- `handoff`：接手结束时更新状态、证据路径和剩余工作；保留敏感信息脱敏及历史/当前区分。
- `code-review`：改动启动协议、权限、journal 或 recovery 状态机时做针对性审查。

这些技能是可选辅助，不要求接手人安装特定工具；也不能覆盖用户已经明确的
节点维护、禁止重启、本地权重和完整交付要求。

## 10. 历史资料的使用方式

[旧交接文档](vela-handoff.md) 保留领域模型、代码导航和发布流程说明，但其
分支、schema、节点数、部署状态和优先级是 9 月 15 日历史快照，不再作为当前
事实。领域定义以 [CONTEXT.md](../CONTEXT.md) 为入口，整体设计参考
[architecture.md](architecture.md) 和
[H3 分阶段架构](h3-stage-execution-architecture.md)。

不要重复从最初部署开始排查，也不要照搬旧报告“全部已通过”的结论。每项
判断应对应当前版本和可复核结果；当前唯一确定的完成项与阻塞见第 1、5 节。
