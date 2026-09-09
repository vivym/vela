# Kubernetes RuntimeLaunchPodReader adapter

日期：2026-09-10。提交前的 CPU/mock 增量；Production Gates 仍为 **0/9**。

`internal/nodeagent/runtime_launch_pod_reader.go` 实现了
`RuntimeLaunchPodReader` 的真实 Kubernetes adapter。它只接受 Node assembly 已认证的
`corev1.CoreV1Interface`，按已验证 `RuntimeLaunchPlan` 派生的
`fleetcontroller.ResourceKey` 做 exact namespace/name 查询，并复制返回的 Pod。
它拒绝空或带路径分隔符的资源名、缺少 UID/ResourceVersion 的对象、nil client 和
已取消 context；不会接受 Runtime 提供的 Pod 内容，也不会把任意 namespace/name
转换成授权。

验证：

- `TestKubernetesRuntimeLaunchPodReaderReadsExactVerifiedKey` 使用 Kubernetes fake
  client 验证 exact lookup 和返回对象隔离。
- `TestKubernetesRuntimeLaunchPodReaderRejectsInvalidOrIncompleteIdentity` 覆盖 nil
  client、invalid key、NotFound 和不完整 Pod identity。
- `go test ./internal/nodeagent ./cmd/vela-node-agent -count=1`：通过。
- Linux/arm64 golangci-lint v2.13.1：`0 issues`。

这个 adapter 已具备接入真实 authority source 的接口，但 `cmd/vela-node-agent` 仍
没有加载 signed plan、创建 Kubernetes client、构造 `RuntimeStartupLedger` 或调用
`PrepareRemoteStartupOrchestration`。因此本提交没有提升 Production Gates，也没有
把单独的 Pod 查询误称为启动授权闭环。
