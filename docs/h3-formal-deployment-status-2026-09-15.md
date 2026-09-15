# H3 正式部署状态与阻塞项 — 2026-09-15

**完整部署和业务验收尚未完成。** 本轮完成组件镜像、数据库 97–99 和 Control/Fleet
实际升级；正式 Worker、ModelResidency、ResidencyPlan、ProfileCertification 和 Job
仍均为 0。不能将下述构建、基础设施健康或先前的 GPU canary 记为完整 Vela Job 验收。

## 已落地的增量

- 从 Vela `6619846ed295ff8d0d8e880d10e9775445bee962` 构建 Linux/amd64
  Control、artifact validator、Fleet、Stage Worker 和 ModelRuntime；源码 archive 和
  八个二进制/命令文件均有 SHA256。fast-h3 入口来自独立 checkout，分别固定
  ENCODER、DIT、VAE_DECODER，不接受额外 argv。
- 在已验证的 slim H3 base 上加入 ModelRuntime 和三个真实 stage 入口。模型权重
  仍留在 `.11/.12` 本地只读缓存，没有加入镜像；本轮没有删除缓存或裁剪媒体。
- 四个组件镜像已在 `.70/.71/.66` 三处 registry 发布并逐项验证副本。
  镜像内八个文件的 SHA256、ELF 和 0555
  mode 校验通过；ModelRuntime CLI、三个 stage 入口拒绝额外 argv、真实 Python
  模块导入通过，共 13 项。此处没有生成新的 GPU Job。
- 数据库先执行三份原始 Up SQL 的事务回滚演练并备份，再用 Goose 升级 96 → 99。
  97/98 提供持久 trace 关联，99 提供 `h3-native-av-v1` OutputSpec 契约；没有写入
  伪造的 ProfileCertification、ResidencyPlan 或 Production Gate PASS。
- 21:03 CST 完成 Control 与 Fleet 顺序滚动升级，两者均 2/2 Ready，实际使用
  下表 digest。site overlay 同步这些镜像，保留原不可变配置和 Secret 引用。

| 组件 | 镜像 digest（仓库前缀均为 `10.1.201.70:5005/`） | 现场采用 |
| --- | --- | --- |
| `vela-control` | `sha256:e5132776a18db030cc3530cb6eaa0d6513eee88e349044741bb15c6666060dea` | 2/2 Ready |
| `vela-fleet-controller` | `sha256:e2bd94d501754098bd75c2c03138f944941824e5e14cb862e9433c887f41b86e` | 2/2 Ready |
| `vela-stage-worker-agent` | `sha256:b9ecf13abf48df1b0aed824bf88072744e2f50bea31b0af90f23d08b2635ec60` | 已发布；无正式 Worker |
| `vela-h3-stage-runtime` | `sha256:c6c5d23e695a5a76017c583facdbbf9036a871d7fc558b14b4f38f27612207c8` | 已发布；无正式 Worker |

H3 依赖基镜像保持 `sha256:503b28f654b549a95618112753bd80afb78fdd13f1d1be61c9c1888ec1e52c5b`。
这批镜像不是已通过完整 release bundle 校验的发布：Host package、bootstrap 和
实际 ResidencyPlan 仍未组成同一条可执行发布链。

## 现场复核

21:06 CST 的[集群快照](evidence/h3-formal-cluster-2026-09-15.json)显示：
54/54 节点 Ready、无压力条件；`.19/server-36` 仍 cordoned，未将一次 Ready
观测当成硬件恢复。PostgreSQL/NATS/APISIX etcd 均 3/3，APISIX 2/2，MinIO 6/6，
读写健康接口均 HTTP 200。两个管理节点的 HTTPS Grafana 登录页均 HTTP 200；
后续两个 `/api/v1/jobs` 未认证读取均 HTTP 401。

传统 `nvidia.com/gpu` allocatable 合计 392；`.11/.12` 的该标量为 0，但分别
通过 NVIDIA DRA ResourceSlice 发布 8 个设备。这是不同分配接口，不能把标量
下降直接称作物理 GPU 丢失，也不能仅相加便宣称全池可用于 H3。

`.11/server-22` 和 `.12/server-23` 实测均为 kernel `6.8.0-139-generic`：
`pidfs` 不存在，但 `pidfd_open` 和 `SO_PEERPIDFD` 可用，containerd 的 `native`
snapshotter 插件状态为 `ok`。没有升级系统的必要；这不能代替原始 pidfd broker
与正式启动链的实际验收。两节点 `/var/lib/vela` 在 ext4 根卷上，分别剩余约
742.51 GiB、761.80 GiB；GPU 各 8 张、每张 64 GiB，复核时占用均为 0。
Node Agent、broker、policy issuer 和 image maintenance 在这两台上均尚未安装/启用。

Grafana/Prometheus 仍如实报告缺少 Stage residency、API SLI/SLO、NATS stream/consumer
及 NATS 文件配额不足；快照还包含其他节点的 IPMI、文件系统和部分 Kubernetes
workload 告警。没有静默这些告警来取得“全绿”。

## 为什么正式启动仍失败

下表是既有 R3–R6 的具体阻塞，不新增另一套验收编号。

| 归属 | 当前事实 | 必须完成的改变 | 完成判据 |
| --- | --- | --- | --- |
| R4：启动归属与身份 | `RuntimeLaunchPlan.ExpectedPod()` 来自 Fleet 模板，没有 live UID；production launcher 首先要求该 UID。Fleet 同时生成 default scheduler、DRA Pod，而 launcher 自己创建 CRI sandbox 并拒绝 ResourceClaims | 统一为 Kubernetes/DRA 创建和管理 workload，Node 在其真实启动边界交接原始 pidfd、observer 与一次性授权；签名模板与 live UID 的核对必须进入同一链路 | 实际 materialized Pod 只启动一组容器；精确 GPU claim、Pod UID、CRI target 和 Permit 对应，退出/恢复可回放 |
| R4：入口与启动材料 | `checkRemoteCLIConfiguration` 要求镜像默认四参数远程 CLI、`/` 工作目录和严格环境。当前 H3 镜像默认 CMD 为空、工作目录为 `/opt/fast-h3`；direct CRI 又加 pidfd wrapper。Fleet 模板也没有完整 broker/bootstrap/journal 挂载 | 同时修正镜像入口、Pod 模板、原始进程交接和远程 CLI 身份验证，显式绑定每次启动材料；不能只删除一个拒绝条件 | 实际命令/环境/可执行文件校验通过；旧内核 broker、caller/observer 替换、重启和失败清理都经过同一真实链路 |
| R4：Host 与 scratch | `.11/.12` 没有启用正式 Host 服务，现有 ext4 不满足 XFS project quota 要求；现有 8 Worker 模板合计 memory requests 为 `8 × (32 + 1) = 264 GiB`，超过 256GB 档主机总内存 | 为首发实际 Worker 子集设置可容纳且测过加载峰值的预算，再提供 journal/binding/PKI 与受控 quota；评估新建专属文件上的 XFS 或经验证的 ext4 支持，不格式化已有数据盘 | 服务 enabled/active；独占设备和 quota 可核验；受控进程恢复不产生第二个 owner |
| R6：媒体图与发布材料 | 模板引用 `/opt/vela/bin/h3-cpu-media --role cpu-encode` 等；本次真实镜像只包含三个 GPU stage 命令。正式 catalog、profile、plan 仍为空 | 补齐实际 CPU 媒体处理组件及与 VAE 完整音视频的端口契约，生成真实 catalog/plan/bundle/身份材料 | Encoder → DiT → VAE → 所需媒体阶段均通过 Vela 执行，最终 MP4/音轨/缩略图符合冻结规格 |
| R3/R5：实际容量合同 | NATS 每副本 PVC 50 GiB，代码固定 stream 上限 64 GiB；当前缺少 stream/consumer。Control validation PVC 为 20 GiB，仍是基础设施验证档 | 按实际磁盘承诺统一应用容量合同、NATS/Control 配额与发布版本；不把运行副本数当成容量满足 | 真实 stream/consumer、Outbox 重放、scratch 配额/并发拒绝和对应告警全部验证 |
| R6：业务验收 | Job、WorkerInstance、ModelResidency、ResidencyPlan、ProfileCertification、Production Gate receipt 计数均为 0 | 上述阻塞解决后，执行认证 API、真实模型任务、对象版本/哈希、下载权限、trace/SLI、故障恢复及规定的长期验证 | 有可下载完整音视频的正式 Job 和独立证据；长期 soak 未完成时仍单列，不冒充九门全 PASS |

代码依据：

- [Fleet Pod materialization](../internal/fleetcontroller/worker_instance_actuator.go)
- [Registry-bound plan](../internal/nodeagent/runtime_launch_plan.go)
- [production launcher](../cmd/vela-runtime-launcher/main_linux.go)
- [remote CLI validation](../internal/nodeagent/runtime_remote_cli_linux.go)
- [OCI task/image matching](../internal/nodeagent/runtime_planned_image_linux.go)
- [CPU media template](../deploy/fleet-controller/residency-plan-rollouts.yaml)
- [NATS stream contract](../internal/eventstream/contract.go)

## 本轮修复与验证边界

新增回归直接调用 `BuildH3WorkerBundleActuation` 和
`MaterializeWorkerInstanceLaunchResources`，生成 8 个 Pod、8 个 claims，并调用实际
`launchProductionWorkload`。测试给模板绑定一个测试 UID，以穿过首个 UID 拒绝并暴露
下一处错误；清空 CRI socket 设置，要求 claims 在 CRI 操作之前被拒绝。

原代码在 Linux 上返回 `VELA_RUNTIME_LAUNCHER_CRI_SOCKET is required and canonical`，
测试失败；修复后在 CRI 连接、镜像下载、Secret materialization 和 init 执行之前拒绝
不支持的 Pod/main-container claims 及其他 main-container 字段/资源，回归通过。
该修复防止对已知不支持的 workload 执行部分启动，**没有实现 DRA 正式启动**。

验证包含 `make test`（Go 测试及 16 项 Python 测试）、`make lint`、Linux launcher vet，
以及实际 Linux 上的 launcher focused 回归。需要 root 的 pidfd 子进程测试在本次非 root
native 运行中明确 skip，未记为通过。GPU 完整输出仍引用先前
[slim runtime 实测](h3-full-output-validation-2026-09-15.md)，没有产生新的正式 Job receipt。

## 证据与回退

[部署证据](evidence/h3-formal-deployment-2026-09-15.json)保存构建输入、镜像内容检查、
数据库升级和 Control/Fleet rollout。远端完整材料位于 `.70`：

```text
/opt/vela-cluster/h3-formal-20260915/
  context/source.tar.gz
  context/build-inputs.json
  images.json
  image-validation.json
  schema99.json
  app-before-schema99.dump
  vela-control-before.json
  vela-fleet-controller-before.json
  control-rollout.json
  build-images.py
  rollout-control.py
  replicate-images.py
```

镜像复制使用逐 manifest/blob SHA256 校验。首次 H3 大 blob 在 registry 完成上传后的
响应阶段超过原 30 秒 socket deadline，保留了失败记录；复制工具增加可配置的
`--timeout`（默认仍为 30 秒，最大 900 秒），H3 重试使用 300 秒。重试按 digest
复用已经提交的内容，不覆盖标签或删除旧镜像。最终副本结果以证据中的对应 receipt
为准，未完成的尝试不记为通过。本次 H3 第二次复制已在 `.71/.66` 均完成：
每个目标 3 个 manifests、17 个 blobs、6,388,362,845 bytes 逐项验证通过；
Control、Fleet、Stage Worker 的两处副本也均通过。

数据库备份为私有 0600 文件，包含升级前真实 `app` 数据；只记录路径/哈希。
数据库迁移文件来自同一 source archive，采用 Goose ledger，不手工伪造迁移通过。
首次准备在 PostgreSQL 只读 `/tmp` 创建目录失败，未应用迁移；改用可写
`/controller/run/vela-h3-schema99` 后成功，临时工具和迁移目录已清理。

Control/Fleet 升级脚本在失败时恢复保存的 Pod template。本次未触发回退。
如需回退数据库，必须先停止写入并检查 97–99 的 Down 影响；本次只验证了 Up 的
事务回滚演练，没有做实际备份恢复演练，不把备份存在等同于恢复通过。

本轮没有重启任何主机、重载 GPU 驱动、格式化已有磁盘或清理其他人的数据。
