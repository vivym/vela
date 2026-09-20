# minimax-h3 503：维护期间的修复与验收边界

核查时间：2026-09-20，Asia/Shanghai。当前结论：修复了一项 Encoder
启动配置错误，**尚未完成 503 恢复及真实 API 验收**。

## 当前阻塞

用户已确认 `.70`、`.71` 正在关机维护，并要求暂不处理这两台节点。
从 `.66` 实测两台节点 ARP 为 `INCOMPLETE`，SSH 超时。
三成员 RKE2 etcd 只有 `.66` 在线；日志显示只收到自身一票，无法选主。
`.66` 上 `2379/2380/9345` 监听，`6443` 未监听，`rke2-server` 为
`activating`。这时不能通过集群控制面重建 Worker 或完成公开 API 验收。

此前交接所述 pause 镜像阻塞已不是当前故障：本次实查
`docker.io/rancher/mirrored-pause:3.10.2` 已缓存，etcd 容器运行中。
没有为临时维护继续执行 etcd reset、快照恢复或成员移除。

## 修复的 Worker 配置

- 节点：`10.1.201.25 / server-43`
- Worker：`c2d58a64-4596-5fbb-be7b-1580790861dd`
- Runtime identity：`minimax-h3-encoder-server-43-r31`
- 部署镜像：`10.1.201.70:5005/vela-h3-stage-runtime@sha256:27b6bb9b62dc1cd568eaa65b2072277e8907e9144ad8b44c5d5a075ad179a7e9`
- 配置：`/var/lib/vela/models/qualification/h3-kube-r3/ENCODER.json`

历史容器日志显示 Ref2VA 本地权重验证、组件加载成功，随后退出：

```text
initialize resident ModelRuntime driver: H3 warmup spec is unavailable
```

本次检查发现前一轮所谓“已补齐”的文件实际只有 10 字节，并非 JSON。
其旧 SHA256 为 `9addda9f632661512644ad937398be735bcf989262571b3c4d2830ad7715d579`。
在该 Worker 的同一镜像中调用实际 `_load_warmup_spec`，稳定复现：

```text
ValueError: H3 warmup spec is not valid UTF-8 JSON
```

这表明仅验证命令退出或文件存在不够；写入内容也必须用实际 runtime 验证。
原交接中的 T2VA 示例还缺少 `prompt`、`conditions` 和两个 sampling noise
字段，不能直接用于该 Ref2VA Worker。

使用 [prepare-h3-encoder-warmup.py](../hack/prepare-h3-encoder-warmup.py)
原子写入完整 Ref2VA warmup 配置和一张确定性的本地合成 RGB 参考图。
未使用客户内容，没有修改模型权重。修复后配置 SHA256：

```text
5a3c4f07bda0399816662c8f4c0947b953eb214429b09ceef2fec63a4adcca67
```

实际镜像验证结果：

```json
{
  "image_size": [1344, 768],
  "mode": "RGB",
  "task": "ref2va",
  "spec_sha256": "5a3c4f07bda0399816662c8f4c0947b953eb214429b09ceef2fec63a4adcca67",
  "validation": "runtime-parser-and-image-decode",
  "gpu_warmup_verified": false
}
```

验证覆盖 JSON/schema、输入数量、参考图文件大小及 SHA256、PNG 解码。
没有加载 GPU 模型、启动旁路 Worker、写入 READY 状态或生成计费记录。
该 Worker 的 Ref2VA 配置错误不能作为所有任务类型 503 的唯一根因。

## 恢复前一轮留下的临时配置

`.66` 当前 RKE2 配置与
`/var/backups/vela/config.yaml.pre-cluster-reset-20260920` 的差异仅为删除了
`server: https://10.1.201.70:9345`。确认没有其他配置差异后，已将配置
逐字节恢复到该备份版本。变更前配置保存在：

```text
/var/backups/vela/config.yaml.before-maintenance-revert-20260920
```

本轮没有重启 RKE2 服务或任何主机。现有 etcd 日志仍在与原 `.70/.71`
成员通信；未见 `reset-flag`。前一轮的原始备份及独立 force-test 快照
保持原样；**不要因维护尚未结束而自动恢复 force-test 快照**。

## 维护结束后的验收顺序

1. 确认 `.70/.71` 恢复网络，检查三成员 etcd 的 cluster/member identity、
   leader 和健康状态；先恢复原集群多数派。
2. 检查 Kubernetes `/readyz`、节点、数据库、NATS、对象存储、APISIX
   以及 Vela 控制服务。控制面在线不等于所有有状态服务均已恢复。
3. 查询当前 Fleet plan 和 Worker epoch，再由正式控制器重建该 Encoder。
   不直接重启旧 CRI 容器，不复用过期启动凭据或绕过 DRA/注册流程。
4. 检查 warmup 成功、实际 Worker READY、三类任务各自 Encoder/DiT/Decoder
   的可用容量。若配置路径或镜像已变，重新核验文件和解析器。
5. 对正式 `minimax-h3` 完成提交、Jobs 查询、终态、完整音视频下载和
   Charge 对账；另行回归 `minimax-h3-live-validation`。

只有最后两项得到实时证据后，才能宣布模型 503 已恢复。本轮没有重启
`.56/.66/.57/.44`，没有修改旧 live 模型的 Worker 或路由。
