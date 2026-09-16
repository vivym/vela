# H3 API 修复与真实验收（2026-09-16）

当前状态：真实 H3 API 闭环验收已通过（`passed=true`）。Job `d79a51f4-9c1c-4f23-90d0-8d95086515a7` 于 2026-09-16 09:12:15 UTC / 北京时间 17:12:15 达到 `SUCCEEDED`，完整音视频和缩略图可通过 APISIX 下载，唯一 Charge 与报价一致。数据库为 107，Control 两副本及三个 repair23 Worker 正常。当前部署仍为基础设施验证配置，单次业务成功不代表所有 Production Gates 通过。

本记录保留当时的镜像和 IP 入口快照。后续永久 Key 发布与域名接入后的配置，分别见
[中转项目交付](relay-station-onboarding-2026-09-16.md)和 [域名接入记录](vela-domain-https-2026-09-16.md)。

## 真实 API 验收结果

- API：`https://10.1.201.70:30443/api`；实际 Project：`94721677-3d9d-45ae-9f7d-b56035d6ca10`。
- 生成组合：`minimax-h3-live-validation` / `fast` / `standard` / `h3-native-av-1344x768-5s-24fps`，单条生成，20 步、`lossless`。
- 经 APISIX 提交、查询、获取产物和签名 URL 下载成功；匿名访问 401，同幂等键重复请求始终返回同一 Job，同键不同请求 409。
- 下载的视频为 H.264，1344×768、24 fps、**完整 124 帧**；视频 5167 ms，AAC 双声道 32 kHz 音轨 5175 ms，容器 5175 ms。没有裁剪音视频或输出。
- VIDEO 855,445 字节，THUMBNAIL 6,944 字节；下载绑定已提交对象版本，大小与 SHA256 均校验一致。
- 报价和唯一 `POSTED / VISIBLE_COMPLETION` Charge 均为 100 minor CNY（¥1.00）；仅一个 Visible Completion、ArtifactSet、幂等结果，预留为 CONSUMED。任务成功后继续重放提交，Charge ID 和金额保持不变。
- 四阶段完成时间（UTC）：Encoder 09:00:14、DiT 09:07:06、VAE 09:07:50、thumbnail 09:08:13。最终校验在修正版本配置和 Control 接续后于 09:12:15 完成；这次耗时包含修复期间的等待，不作为性能/SLO 基准。

证据：[API 与计费回执](evidence/h3-api-2026-09-16/api-receipt.json)、[客户端 ffprobe](evidence/h3-api-2026-09-16/ffprobe.json)、[部署与故障对照](evidence/h3-api-2026-09-16/deployment-snapshot.json)、[归档说明](evidence/h3-api-2026-09-16/README.md)。

## 本次故障与修复

Job `92f7a6d5-5b87-4b5c-b394-7028f2a2ce94` 经 APISIX 成功受理并进入 RUNNING，但 Encoder 后的权限续租失败。Worker 原日志将所有非接受响应统一描述为 `returned stale authority`，掩盖了模型后端的具体拒绝原因。

从原节点只读保存的 runtime journal 中确认：原权限与续租权限属于同一 execution，`ValidateRenewal` 成功，续租已持久化为 accepted，backend confirmed 仍指向原权限。真实 Python 驱动 `_require_active` 只允许完整 identity 相等，无法接收 Control 在 Start ACK 或 heartbeat 后签发的新 digest。

通过真实 stdio 驱动进程复现：Prepare → Start → renewed Status 返回 `stage identity does not match active execution`。修复位于相邻 `fast-h3-vela-serving` 仓库的 `src/fast_h3/vela/driver.py`：

- Status 接受经 Go ModelRuntime 验签并确认属于同一 allocation 的续租；逐字段保留所有稳定执行身份，版本不可倒退。
- 旧摘要记入当前执行的退休集合；同版本 heartbeat 也不能将身份退回旧摘要。
- 已封存、已停止、失败或已完成 drain 的执行不能借续租恢复运行。
- inspection 和 drain 使用最新的实际后端身份；不修改已封存 receipt。
- Vela Worker 错误包含 decision、身份匹配结果及有界 Detail，便于定位后续错误。

未修改签名校验、执行 journal 内容、READY 事实或计费记录。该失败 Job 的 Charge 数量已查询为 0。

## 验证

- 新增 Python 回归在修复前明确失败：合法续租被拒绝；修复后通过。
- Python 驱动协议与续租回归：本地 27 项通过。
- 实际 CPU 运行时镜像在隔离 Docker 容器内执行协议、续租、真实 ffmpeg 缩略图测试：31 项通过。测试容器禁网、根文件系统只读，测试写入独立 tmpfs。
- `go test ./internal/stageworkeragent ./internal/modelruntime` 通过。
- `TestProcessBackendFastH3AuthorityRenewal` 通过 Go ProcessBackend 驱动真实 Python 子进程，验证续租、旧摘要拒绝、封存、inspection、drain。
- `TestStatusPreservesRuntimeRejectionDetail` 通过。
- Linux Node 启动相关测试（`Test.*(RuntimeStartup|RuntimeContainerBootstrap|ListenRuntimeStartup)`）在管理节点以 root 身份通过。缺失 Fleet 公钥的测试补齐前置 policy stub 和明确的缺失公钥路径，确保实际验证目标拒绝原因。
- APISIX 实测：缺失 sampling、错误步数、错误 quality 均为 `400 invalid_request`，明确要求 20 步和 `lossless`。

本地 macOS ffmpeg 不含 `libwebp`，缩略图测试曾因该环境缺项失败；上述 Linux 镜像测试已覆盖实际编码器。未将本地环境失败当成服务端通过证据。

## 当前发布

GPU 运行时：

```
10.1.201.70:5005/vela-h3-stage-runtime@sha256:ed2002b1c9d94eea02680fcd362db21524ef41448c64d990442770816969c3a4
```

CPU 缩略图运行时：

```
10.1.201.70:5005/vela-h3-cpu-thumbnail@sha256:434c8e02ac79d62bd7401ceecb8f0e82c1f3f489b7c4b82908ce3c67e549f3fb
```

Worker Agent：

```
10.1.201.70:5005/vela-stage-worker-agent@sha256:d61bb31c185b84c95c72715494f3e359b77717ebf688b8be6a1c2e9a985024d0
```

GPU/CPU 镜像复用已有依赖层，只增加约 9 KB 的 Python 源码层；Worker 增量压缩层约 7.5 MiB。GPU 与 CPU 镜像的包路径不同，分别核对并发布。CPU 路径修正前的候选未启动，已撤回。

| 组件 | 节点 | WorkerInstance |
| --- | --- | --- |
| Encoder/VAE AUX | `.11` / `server-22` | `b66e21ad-5025-5069-a45d-2b50066d4be3` |
| DiT | `.12` / `server-23` | `5a3f760e-f782-58ed-af82-1407f06c1a3f` |
| 缩略图 | `.11` / `server-22` | `6bcaed29-d8c4-5f11-9e48-ab0c0d7a7670` |

使用 256 GB 节点与原本地权重缓存。未重启任何主机。

## 发布时的操作顺序

先将旧 bundle 从 Fleet 当前期望中 withdraw，并确认所有 Controller 副本已读入新配置，再停止、fence、退休旧 Pod 和关联 claim template。若先删旧对象、后替换 Controller 配置，旧 Controller 可能重新创建已退休实例的 gated Pod/模板，导致下一次独占 GPU 校验拒绝新实例。本次核验数据库 FENCED、Pod 未调度且无容器启动后，经 Registry 授权删除了这些重建对象。

新镜像需要先在每个目标节点的 containerd `k8s.io` namespace 中展开 `native` 快照；CRI 默认 snapshotter 的镜像缓存不等同于该观测视图。遗漏时 Observer 返回 `snapshot ... does not exist`，已消耗启动意图的实例须正式退休。本轮已完成 GPU/CPU 新镜像的 native 展开和真实 image preflight，并将镜像与独占目录检查前移到 `RecordBackendStartupIntent` 之前。

新节点安装不提前创建 launcher 独占的 `launch` 目录。启动预检查曾在 Pod 创建前失败，核验 ledger 仅含身份头、无启动记录后重试，未重写 ledger。

repair21 三个实例因缺失 native 快照而失败，已在切换 Fleet 期望配置后正式 FENCED 并退休。repair22 三个实例后续因下面的运行问题退役，已 FENCED；当前 Fleet 配置为 `vela-fleet-h3-rollouts-20260916-repair23`，上述 repair23 三个实例均已真实 READY/CONNECTED。模型就绪和业务验收分别核验，不能以 Kubernetes 容器 Running/Ready 替代 Worker Registry 事实。

启动期间 `.12` 的 GPU 1 在 DiT 预热时持续 100% 利用率，但温度 84°C，`nvidia-smi -q -d PERFORMANCE,TEMPERATURE` 明确报告 `SW Thermal Slowdown=Active`（2026-09-16 07:54 UTC）。这是实际散热降频证据，会影响耗时；未调整功耗、风扇或重启主机。节点当时可用内存约 242 GiB，磁盘可用约 607 GiB。

## repair22 真实任务暴露的后续问题

Job `5b28970d-1aa6-4cc4-b0fe-fce86e55930b` 的 Encoder 于 07:56:33 UTC 成功；DiT 于 07:56:34 开始，07:58:15 前的多次 Python 续租均成功，原驱动问题未复现。但 DiT 于 08:00:15 因 `WORKER_LOST` 终止，VAE/thumbnail 被取消，Charge 数量实查为 0。

1. 正式 Worker 没有接入已有 `NewJournalWorkerClient`。Encoder 收尾时 Worker journal 的 floor 已为 3，Runtime journal 仍为 0，Runtime 正确拒绝冒用 Worker 身份写入。现在正式启动在验证 Registry binding 后，使用绑定的 Runtime journal UUID/scope、同一 member 目录的 `runtime-journal.sock` 和 Worker PIDFD broker 包装 RPC client；本地调用和 member 转发都经过此包装，不改变 Node journal 所有权。
2. DiT 使用的容量观测 `4294967304` 于 07:58:29 过期，控制面随后拒绝续期。最新已签租约于 08:00:15 到期，任务被正式终止。容量观测是调度输入，不能限制已取得 allocation 的计算时长。迁移 106 在新 StageAllocation 中持久化已验证的观测序号并禁止修改；Start/Heartbeat/Reattach/运行权限快照使用该绑定，运行身份仍检查 Worker、成员、设备、驻留、session 和 lease。公共调度容量检查保持最新、未过期要求；Start/Complete/读取已分配任务改用独立执行身份校验。

迁移要求所有 allocation 已释放，不回填历史身份。旧观测过期、被替代和清理之后的 Start、Heartbeat、Reattach、Seal 已通过真实 PostgreSQL 集成测试；错序号、错 vector、错 allocation、错 Worker epoch、已释放后续期均被拒绝。空库迁移往返保留函数 OID、owner、ACL，已有绑定后拒绝降级。相关 Start/Heartbeat/Reattach、调度过期拒绝、terminal history 和存储边界共 10 项已有回归通过；3 项新增集成测试包含 3 种容量变化场景、迁移权限/回滚及活动任务迁移拒绝。

封存边界检查还发现 `vela_seal_stage_output` 和 Coordinator Complete 只读取最初的 `StageLease.expires_at`。新增回归用 2 秒初始租约、合法续期并等待超过原期限，准确复现封存拒绝；迁移 107 统一使用已持久化续期的有效截止时间，并增加数据库时钟校验，保留租约状态、fence、allocation、输出规格与存储边界检查。该迁移仅修改函数，先备份和事务回滚演练，再在当前任务运行期间应用，未重启模型。

真实任务 `ec15406b-3466-4abc-882c-28d82ca0f1de` 的 Encoder 成功后 Worker/Runtime journal floor 均为 5；DiT 两端随后均为 6。DiT 观测于 08:37:56 UTC 过期后，仍持续成功续期到 08:43:01，去噪于 08:42:30 完成（372.94 秒）。Encoder、DiT、VAE、thumbnail 分别于 08:36:16、08:43:03、08:43:48、08:44:12 成功，证明续期、journal 和封存修复进入了真实路径；该 Job 后续因沙箱问题未完成交付。

repair23 Worker 镜像：`10.1.201.70:5005/vela-stage-worker-agent@sha256:d61bb31c185b84c95c72715494f3e359b77717ebf688b8be6a1c2e9a985024d0`。GPU/CPU 运行时镜像继续使用上述已验证版本。数据库迁移先做全库备份和事务回滚演练，再提交；材料保存在 `.70` 的 `/opt/vela-cluster/h3-repair23-20260916/`。

## 最终校验沙箱修复

四阶段成功后，Control 最终校验器在启动 ffprobe helper 时被 `RuntimeDefault` seccomp 拒绝，错误为 `fork/exec .../artifact-validator-helper: operation not permitted`。内核支持所需 namespace；无需更改系统版本或 AppArmor/sysctl。

先在 `.70` 用当前正式 Control 镜像和同样的非 root、无 capability、只读根安全设置复现默认策略失败；然后从实际 containerd 默认策略生成专用 Localhost profile，仅增加一个 `clone` 规则：参数 0 按掩码 `0x7e020000` 匹配 `0x7c020000`，要求 NEWUSER/NEWNET/NEWNS/NEWPID/NEWIPC/NEWUTS 全套 namespace，禁止 NEWCGROUP。原普通 clone 规则、clone3 的 ENOSYS 和其他规则均保持一致。

策略文件 SHA256 为 `bd44ec5ac61a84a03c37cb405a62e80c16093ceba8ae7cbdcb66a0a59d546ca5`，已安装到 `.70/.71` 的 `/var/lib/kubelet/seccomp/vela/artifact-sandbox-bd44ec5ac61a.json`。两台无业务凭据的测试 Pod 均通过真实 pinned ffprobe 音视频、Landlock 文件隔离、video/thumbnail 生产沙箱测试。详细安装与升级前复核见 [seccomp 说明](../deploy/environments/marslab/vela-control/seccomp/README.md)。

Control 主容器使用该 profile，并要求节点 label `vela.ai/artifact-sandbox=bd44ec5ac61a`；init 容器仍继承 RuntimeDefault。Control 镜像保持 `10.1.201.70:5005/vela-control@sha256:b47d386c9ed9f4838cc68197136ad2eb1da3c9f377a9928d68c058d3e7326c18`，两副本滚动发布成功。未使用 Unconfined、privileged、CAP_SYS_ADMIN，也未跳过媒体校验；未重启主机或模型。

旧 Job 的 finalization deadline 是 08:54:12.586712 UTC，在修复发布前已到期，于 08:54:12.908581 正式 FAILED，Charge 为 0，预留已释放。保留该失败事实，没有重写终态或计费。新的验收任务通过正常 API 提交，并使用独立、持久化的幂等键。


沙箱放通后的真实调用还发现 `VELA_ARTIFACT_FFPROBE_VERSION` 被填成 `ffprobe-8.0.1`；校验器比较的是 JSON 的 `program_version.version`，应为 `8.0.1`。两副本的实际二进制版本均核实为 8.0.1，镜像未漂移。创建不可变配置 `vela-control-runtime-api-95ce430963ad`，仅修正此值并滚动更新，保留原版本严格相等检查。配置内容 revision 为 `sha256:b6d149529e1fba375fdcb8430e757b1b911f06675f64b267db2458bc8833ba3e`。新增 `TestMarslabFFprobeVersionMatchesBuiltRuntime` 对照 Dockerfile 的固定构建版本与 Kustomize 实际输出，修复前明确失败、修复后通过；原 Marslab 配置引用与内容 revision 回归也通过。

最终一次恢复沿用当前 Job，由旧 finalization claim 自然过期后接续，没有改写 lease、claim 或任务状态。最终媒体检查通过后由正式业务事务创建 Visible Completion 和 Charge。

客户端独立验收第一次因管理机未安装 ffprobe 而中断，此时服务端 Job 已成功。随后使用已存在、digest 固定的 CPU runtime 镜像内 ffprobe 6.1.1 校验下载文件（禁网、只读根、无 capabilities，仅挂该视频文件），通过验收脚本的 `--ffprobe` 指定受控 wrapper，并以原 run directory/幂等键恢复，未创建第二个任务。服务端的固定 ffprobe 8.0.1 与客户端独立解码校验分别保留证据。以后执行验收前先检查客户端 ffprobe 可用；正式脚本为 `hack/verify-h3-api-flow.py`。

## 证据位置与验收边界

`.70` 上保存镜像回执、失败快照、退役证据、Linux 测试日志和参数检查结果：`/opt/vela-cluster/h3-renewal-20260916/`。

当前发布材料：`/opt/vela-cluster/h3-live-20260916-repair23/`；运行时服务配置分别位于 `.11/.12` 的 `/etc/vela/h3-live-repair23-*`。最终校验后的验收目录为 `/opt/vela-cluster/h3-api-acceptance-20260916-repair23-sandbox/`。

上述完整业务验收要求均已满足。验收后三个 Worker 仍为 READY/CONNECTED、容量观测新鲜、active allocation 为 0；Control 两副本 Ready、restart count 为 0。`.11` AUX/thumbnail 与 `.12` DiT 的 systemd 服务均 enabled/active。诊断 Pod 的结果先归档，再按明确名称清理。

单次业务验收不证明主机重启后的自动恢复、故障切换、长期稳定性或所有 Production Gates。systemd enabled 也不替代重启恢复测试。
