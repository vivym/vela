# Runtime startup authority composition seam

日期：2026-09-10。提交前的 CPU/native mock 增量；Production Gates 仍为 **0/9**。

新增 `internal/nodeagent/runtime_startup_authority_linux.go` 的
`RuntimeStartupAuthority`，把一次启动所需的 authority-bearing source 收敛为显式
对象：verified `RuntimeLaunchPlan`、`RuntimeStartupLedger`、authenticated
`RuntimeLaunchPodReader`、CRI `RuntimeContainerObserver`、held journal owner、独立
Worker journal owner、Fleet `RuntimeStartupRegistry`、Node-created
`RuntimeObserverCustody`、非零独立 authorization digest、caller credentials 和
bounded timeouts。

`Prepare` 只调用现有 `PrepareRemoteStartupOrchestration`，不创建默认 plan、读取历史
receipt 作为许可、生成 Permit 或把 digest 当成授权。缺任一 source、重复 credentials、
无效 timeout 或 nil caller 都 fail closed。返回的 orchestration 继续使用已认证 caller
和 `ServeCaller` 单次握手路径。

native runner 现验证 `TestRuntimeStartupAuthorityRejectsIncompleteSources`，并和原有
startup/observer/remote CLI 测试一起通过：全部 selected tests PASS、0 FAIL、0 SKIP，
无 race。`go test ./internal/nodeagent ./cmd/vela-node-agent ./hack -count=1`、Linux
arm64 golangci-lint v2.13.1、shell syntax 和 diff 检查通过。

这个 seam 还没有真实 authority implementation。`cmd/vela-node-agent` 仍未加载
signed plan、创建 ledger/Fleet/CRI/observer source 或调用它；因此本增量不提升
Production Gates。下一步是为这些字段实现真实配置和生命周期 owner，然后把同一个
对象接入 Node main；不允许通过填充 mock source 让生产命令看似启动成功。
