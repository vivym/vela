# Runtime observer 原始句柄交付与挂起保护

本轮把执行连续性实验推进到 Node custody 边界：Node 私有 `SOCK_SEQPACKET`
socketpair 接收 observer 提供的原始 target pidfd，确认前目标保持 ptrace stop；
Node 只能通过 `Start` 一次性释放目标。

`ReadProcessOffer` 只接受一个 `SCM_RIGHTS` descriptor，拒绝缺失、重复、普通
文件、截断 ancillary 和超长 frame，并关闭失败路径的全部 FD。旧 `ReadPacket`
明确拒绝带 descriptor 的消息。`ReceiveRuntimeObserverCustody` 复制 Node
事先保留的 observer pidfd，确认 observer 是 Node 子进程、目标是其 stopped 的
ptrace 子进程，并保存两个独立句柄。`Start` 只能成功一次；`MatchCaller` 用
原始 pidfd 比较，不能从 PID 或 observation 重建。

`Check` 使用 challenge/response；observer 被 SIGSTOP 时会超时，不把 live
pidfd 当成响应。排队检查可以被 context 取消，`Revoke` 不等待阻塞 I/O，并且
不会把终止请求伪装成退出证明。observer 关闭、通道 EOF、重复 Start、不同
caller、Node endpoint 关闭均有测试。

固定 Linux/arm64 builder 的原生 runner 通过：`TestRuntimeObserverCustody`
6 个场景、`TestRuntimeObserverProcessOfferDescriptors` 7 个场景，以及实际
CLI 的 6 个 observer 场景。原有 read-only、publication、actual CLI、writable
endpoint 测试无 skip、fail 或 race；`go test ./...`、Linux arm64 vet/lint
通过。

这些结果证明交付、持有、撤销协议及实际 CPU CLI fixture 的行为。生产
containerd 创建路径、镜像/namespace/argv 绑定、真实 Fleet/TLS/PostgreSQL、
一次性 grant、Worker materialization journal、掉电恢复和完整 descendant/device
containment 仍未完成。Production Gates 保持 **0/9**。
