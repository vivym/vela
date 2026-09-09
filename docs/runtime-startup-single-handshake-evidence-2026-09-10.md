# 单次认证连接与原始 Runtime 绑定

日期：2026-09-10。基于 `b653ee8`，本轮把此前识别的 startup 二次握手缺口推进到
一个可验证的 mock/native seam；Production Gates 仍为 **0/9**。

`PrepareRemoteStartupOrchestration` 现在把已通过 `ReceiveRuntimeCaller` 的 caller
保留在 `RuntimeStartupOrchestration`。`ServeCaller` 直接把这个 caller 交给
`RuntimeStartupServer.HandleCaller`，由它解析已缓存 payload 并回复；该路径不再
读取第二个 challenge。`HandleBackendStartupWithCaller` 在激活前用 caller 的原始
pidfd 与 reservation 绑定的 Runtime owner pidfd 做 `SameLiveProcess` 检查。相同
UID/GID、相同 request bytes 或相同 digest 都不能代替这个关系。

原有 `Serve(listener)` 仍保留为受保护 listener 的 transport adapter，供已有 focused
测试使用；生产 Node 装配应选择 `Prepare...` + `ServeCaller`，不能把 listener adapter
误当成完整 authority path。

机器可读的[测试证据](runtime-startup-single-handshake-evidence-2026-09-10.json)
记录了源码 patch、镜像摘要、测试名称与原始日志。Linux/arm64 native race 套件共
`42 PASS`、`0 FAIL`、`0 SKIP`、`0 MISSING`，没有 `WARNING: DATA RACE`。新增验证：

- `TestRuntimeStartupCoordinatorRejectsSameUIDDifferentProcess`：另一个同 UID
  进程即使提交相同 startup request 也不能消费 grant 或得到 Permit。
- `TestRuntimeStartupOrchestrationUsesTheRetainedCallerOnce`：预留阶段保留的原始
  caller 完成启动和写入，未发生第二次 challenge 读取。

普通回归 `go test ./internal/nodeagent ./cmd/vela-node-agent ./hack -count=1`、
Linux/arm64 golangci-lint v2.13.1、shell syntax 与 diff 检查仍需在本提交后重跑；
本报告只记录原生套件结果，不把它们冒充全仓闭环。

仍未闭合的是 `cmd/vela-node-agent` 的真实 plan/Fleet/CRI/observer/授权来源，
remote-owner CPU Job 全链、Worker journal 权限隔离、真实进程替换与后代隔离、
长期资源上界、power-loss 恢复，以及完整 integration 分片的宿主压力问题。
