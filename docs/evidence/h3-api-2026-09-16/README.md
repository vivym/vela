# H3 真实 API 验收证据（2026-09-16）

Job `d79a51f4-9c1c-4f23-90d0-8d95086515a7` 已 `SUCCEEDED`，完整验收
`passed=true`。该结果证明一次真实模型调用、完整媒体交付和固定价格计费闭环，
不代替故障切换、长期稳定性、重启恢复或全部 Production Gates。

| 文件 | 内容 |
| --- | --- |
| [api-receipt.json](api-receipt.json) | API 受理、完整产物元数据、对象版本/哈希、唯一 Charge、重放不重复计费 |
| [ffprobe.json](ffprobe.json) | 经 APISIX 下载后的独立解码检查，124 帧 H.264 和完整 AAC 音轨 |
| [job.json](job.json) | API 返回的最终 Job 与固定报价 |
| [deployment-snapshot.json](deployment-snapshot.json) | Control/Worker 健康、阶段续租、旧任务失败且未扣费、默认/专用 seccomp 对照测试 |
| [journal-floors.json](journal-floors.json) | 同节点 Worker/Runtime 执行 floor 一致；不含原始 journal 或签名权限 |
| [diagnostic-cleanup.json](diagnostic-cleanup.json) | 本轮三个测试 Pod 已清理，日志已保存 |

API 为 `https://10.1.201.70:30443/api`。原始受控验收目录位于 `.70`：
`/opt/vela-cluster/h3-api-acceptance-20260916-repair23-sandbox/`。
`run/complete-video.mp4` 与 `run/thumbnail.webp` 保存实际交付文件。

验收脚本为 `hack/verify-h3-api-flow.py`，本次执行的 SHA256：
`b7256e6a836f382c515ea3e12da133c6f7e0369132295082bde4ad216010ce2b`。
客户端通过该目录的 `ffprobe-container` wrapper 使用固定 CPU runtime 镜像内
ffprobe 6.1.1，服务端继续使用固定 ffprobe 8.0.1。两者都未重新编码、裁剪产物。

本目录不包含 Bearer Token、签名下载 URL、幂等键、PKI 私钥、数据库备份或原始
journal。完整故障原因、修复和验证边界见[修复报告](../../h3-api-repair-2026-09-16.md)。
