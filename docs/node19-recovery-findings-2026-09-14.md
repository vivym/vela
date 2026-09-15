# .19 恢复检查 — 2026-09-14

`10.1.201.19 / server-36` 在本轮后半段可通过 SSH 访问，Kubernetes 曾显示
Ready，但 GPU 资源仍为 0。检查期间保持 cordon；本轮没有重启主机、修改驱动、
重启 agent、写入镜像缓存或启用新的主机监听端口。

## 已确认的事实

- SSH 盘点确认 8 个 NVIDIA PCI 设备，`nvidia-smi` 可读全部 8 张 GPU，
  驱动 580.159.03、Toolkit 1.19.1-1。rke2-agent 和 nvidia-smi-exporter
  都为 active/enabled。
- 设备插件 Pod 反复以 255 退出，日志为
  `exec /usr/bin/nvidia-device-plugin: exec format error`。
- 主机和 CRI 镜像配置均为 amd64，不能把错误直接归因于选错 ARM 镜像。
  当前 v0.17.4 对应多架构 index digest `3c54348f...`，其 amd64 child
  为 `ad155f1089b64673c75b2f39258f0791cbad6d3011419726ec605196981e1c32`。
- 本地解包目录中的实际可执行文件大小为 **0 bytes**：
  `/var/lib/rancher/rke2/agent/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/181/fs/usr/bin/nvidia-device-plugin`。
  这足以解释该二进制无法执行；尚未验证对应压缩 layer 的内容和校验和。
- 内核启动日志含 `mce: [Hardware Error]`，CPU 0 / Bank 27。该记录不足以
  单独确定故障器件，也未证明它导致了镜像文件损坏。
- 准备继续只读检查 layer 时，从 `.70` SSH 到 `.19` 返回
  `No route to host`。所以短暂 Ready 不构成稳定恢复证据。

现场已有 9091、9100 和 9835 监听；这些服务保持原状。新的 node-local-metrics
标签尚未启用，节点稳定后必须重新检查 19105–19108 端口，再接入该采集器。

## 后续处理顺序

1. 先确认供电、网络及 MCE/BMC 硬件日志，使主机持续可达。
2. 对照 registry 的 amd64 manifest 检查本地压缩层 SHA256、tar 中二进制
   大小/ELF 和解包 snapshot。只有确定所有权及引用后，才能通过 containerd
   API 清理这个损坏镜像的缓存并重新拉取；不能直接删除 snapshot 目录或
   全局清理其他工作负载的镜像。
3. 只替换该节点的失败设备插件，验证 8 GPU allocatable、镜像拉取、
   exporter/node-local-metrics 采集和稳定运行，最后再解除 cordon。

证据：

- [主机和历史 Control 日志](evidence/node19-recovery-preflight-2026-09-14.json)
- [空二进制及 MCE](evidence/node19-empty-plugin-binary-2026-09-14.json)
- [CRI 平台](evidence/node19-image-platform-2026-09-14.json)
- [registry manifest](evidence/nvidia-plugin-manifests-2026-09-14.json)
- [盘点时再次掉线](evidence/node19-disconnected-during-preflight-2026-09-14.json)

顺便核对的两个 Control Pod 分别有 1/2 次重启，最后一次失败均在 09:40Z，
原因是当时数据库端口尚未恢复。之后持续运行，不能描述为本次网关变更造成
的新重启，也不能继续沿用旧文档中“0 重启”的表述。
