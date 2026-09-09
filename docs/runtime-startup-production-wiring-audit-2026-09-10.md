# `cmd/vela-node-agent` 启动装配审计

日期：2026-09-10。审计基于提交 `46eaf67` 的当前源码；这是装配事实记录，
不是生产就绪声明。Production Gates 仍为 **0/9**。

## 当前真实入口

`cmd/vela-node-agent/main.go:184-237` 构造 WorkerInstance 模板、epoch store、NVIDIA
inventory probe、Fleet TLS client 和 evidence reporter。`main.go:240-293` 构造 controller
identity、remediation executor、device policy、fence、postcheck、rate limiter、
Node Agent service 和普通 `FileLedger`。`main.go:302-344` 只发布一个 TCP gRPC
listener，并运行 WorkerInstance evidence reporting background process。

这条路径没有调用或构造：

- `OpenRuntimeStartupLedger` / `RuntimeStartupLedger`；
- `RuntimeLaunchPlan` 和已验证的启动 request；
- `RuntimeStartupReservationConfig`、真实 reservation registry 或 `ReserveRemote`；
- `RuntimeObserverCustody` 的 Node 创建与生命周期；
- 独立 grant issuer、backend startup policy 或 Permit 回包策略；
- 受保护 Unix startup listener、`RuntimeStartupOrchestration` 或 `ServeCaller`。

因此当前命令即使成功启动，也没有宣称或实际提供 Runtime startup authority。
WorkerInstance evidence reporter 的 Fleet TLS client 也不能自动成为 Runtime startup
reservation source；两者的请求、权限和失败语义不同。

## 已经具备的可组合 seam

`internal/nodeagent/runtime_startup_prepare_linux.go:49-94` 现在在一次 reservation
中保留已认证 `RuntimeCaller`，创建 operation-bound endpoint/grant，并将 caller
传入 `RuntimeStartupOrchestration`。`runtime_startup_orchestration_linux.go:55-63`
的 `ServeCaller` 直接消费该 caller；`runtime_startup_server_linux.go:71-97` 不会再次
读取 challenge。`runtime_startup_coordinator_linux.go` 在激活前比较 caller pidfd
和 reservation Runtime owner pidfd；同 UID/GID 或相同 request 不能绕过这一步。

这些 seam 仍只接受 trusted Node assembly 提供的 authority-bearing 对象。它们不会
从 JSON、历史 receipt、旧 reservation 或 WorkerInstance template 推导计划、授权或
observer。这个约束是刻意保留的，避免给生产命令添加伪 Permit 默认值。

## 进入生产装配前的必要输入

下一实现增量应先定义一个不可省略的 Node startup configuration/source：

1. Node 启动时验证并加载 signed launch plan、Node identity、runtime incarnation、
   journal pair 和完整 epoch vector；配置 digest 必须与 plan 绑定。
2. 通过独立 Fleet startup client 取得 Fresh reservation，并把 response 与本地
   ledger startup intent 逐字段比对；历史 lookup 只能诊断，不能激活。
3. 由 Node 自己创建受保护 Unix socket、原始 Runtime/Worker owner 和 observer
   custody。Incoming `RuntimeCaller` 必须在 reservation 前完成认证并作为同一个对象
   传递到 `Prepare...`/`ServeCaller`。
4. 独立策略 adapter 必须返回 operation-bound authorization evidence；摘要不能
   被当作授权本身。没有策略、observer、reservation、grant 或 caller 任一项时，
   composition root 必须拒绝启动。
5. shutdown ownership 必须加入同一个 root：先撤销路由和 pidfd custody，再停止
   socket，最后等待 ledger/endpoint 清理；丢回包、observer 失联、取消和不确定
   append 都必须保持 fail-closed。

实现这些 source 之前，不能把现有 `Serve(listener)` adapter 接到 `main.go`，也不能
用 WorkerInstance evidence 配置充当 startup plan。完成后才可以增加真实 Node/Runtime/
Worker/CRI/Fleet remote-owner CPU Job；当前 CPU mock 只证明上述模块 seam 和拒绝规则。

## 可审计的验收顺序

先做 source/config schema 与拒绝测试，再做 Node composition root 的 mock authority
adapters；然后跑单次 caller 的 native race、同 UID 冒充、observer 失联、回包丢失、
ledger 阻塞和重启不重建 owner。最后将同一 root 接入 `cmd/vela-node-agent`，执行
端到端 CPU mock Job。每一阶段都要单独记录 PASS、SKIP、environment failure 和
unverified，不得把 focused startup suite 提升为 Production Gate。
