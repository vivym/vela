# H3 Kubernetes 启动链集成

目标是通过 APISIX 创建真实 H3 Job，取得完整音视频，并核对唯一 Charge。
2026-09-15 的业务验收失败，历史证据见 [API 验收快照](evidence/h3-api-flow-acceptance-2026-09-15.json)。
后续于 2026-09-16 完成真实 API、完整音视频与计费验收，见 [修复及验收记录](h3-api-repair-2026-09-16.md)。
本文件固定本次修复的执行边界，避免继续把基础设施测试当成业务完成。

## 启动归属

Kubernetes scheduler、DRA driver 和 kubelet 是 GPU Pod 的唯一创建与设备分配执行者。
Fleet 发布带 `vela.ai/runtime-startup` scheduling gate 的不可变 Worker Pod。Node
核对 Registry 签名模板与真实 Pod UID 后，在准备原始 pidfd offer socket 和 observer
socketpair 后移除这一个 gate。Node 不为该 Pod 另建 CRI sandbox/container。

镜像默认入口使用固定的 pidfd entrypoint：在 exec ModelRuntime 前打开自身 pidfd，
将句柄传给该 member 的 root-owned listener，并等待 observer 接管。之后 exec 镜像
明确声明的四参数 `vela-model-runtime serve-remote --bootstrap-file PATH`。Node
分别核对 OCI 初始 argv、wrapper 镜像内容、实际 Runtime 可执行文件和 exec 后 argv。
镜像必须使用受限 PATH/HOME、`/` 工作目录；后台 Python/CUDA 参数放入签名 Runtime
manifest，不通过 ambient Runtime 环境放宽限制。

Node 只接受本次 gate 释放产生的两个容器，绑定真实 Pod UID、CRI IDs、DRA claim、
原始 pidfd、observer custody、一次性 Fleet 授权和 journal。容器首次启动的
`restartCount=0` 是合法的 Kubernetes/CRI 值；不能与 Vela 的非零 incarnation epoch
混用。旧实例或自动重启进程不能继承旧 pidfd/grant。

失败后仅通过本次原始 pidfd 停止已交接进程；kubelet 管理容器回收，Fleet 仍经
Registry 授权执行 Pod 退休和保护 finalizer 删除。Node 只获得本节点的精确 gate
移除权限，不能创建、删除 Pod 或修改镜像。未交接的 wrapper 有受限超时自退，
在复核退出前不能声称清理成功。禁止接管已经运行但无法证明本次启动来源的实例。

## 依赖顺序及完成判据

1. **入口与 Kubernetes handoff**：实际 Pod 仅一组容器；合法首次启动可通过；
   Pod UID、镜像/argv、原始句柄不匹配以及重复启动必须拒绝。
2. **Node host 服务**：`.11/.12` 受控 scratch、journal、broker、policy issuer、Node
   enabled/active；不升级 OS，不格式化已有盘。首发只占经实际内存测量可承载的 GPU 子集。
3. **真实运行和 catalog**：Encoder、DiT、VAE 和媒体阶段接口实际连接；从实际测量
   生成 profile/plan/catalog，不写测试 fixture READY、假认证或 Production Gate PASS。
4. **API 和计费**：受控验收组织额度、真实 Job 202、SUCCEEDED、全帧全音轨、对象
   版本与 SHA256、唯一 Charge、原报价/额度对账、幂等无重复账、取消/失败规则。

以上依次验证。Kubernetes Ready、canary 视频、下载探针和单元/集成 fixture
均不得替代第 4 项。长期 soak 和生产九门另列实际状态。
