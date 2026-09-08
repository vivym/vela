# Candidate journal 持久化期间的 Runtime 生命周期保护

基线：`9646404`。本次修复执行调用前后两处持有 `Service.mu` 的 candidate
持久化，使 watchdog 到期和 `Service.Shutdown` 能在这些 I/O 尚未返回时
处理当前执行。范围为 CPU/mock；完整 protected Job 装配仍未完成。

## 复现与原因

`executionCallContext` 原先持有 Service 锁等待 candidate 写入及读回。
watchdog 必须先取得同一把锁才能标记到期；`Shutdown` 也要取得它才能取消
正在执行的 call。持久化完成后，旧代码只再次检查签名的墙钟有效期，未重新
检查执行的 monotonic deadline、关闭状态或 generation。

`confirmBackendAuthority` 在 backend 返回后，同样持有 Service 锁等待确认
candidate 的持久化。第一次只修复调用前路径后，扩展测试仍在确认 fsync
边界失败；最终修复同时覆盖这两处。原始失败日志保留在 receipt 中。

测试保留最终测试代码，通过 Go overlay 将这两个产品源文件还原到基线，
四个用例均因生命周期信号被阻塞而失败。它们在最终代码上均通过：

| 注入边界 | 生命周期事件 | 最终行为 |
| --- | --- | --- |
| 调用前 candidate RPC 已提交、回执尚未返回 | watchdog 到期 | 到期标记可达；释放回执后拒绝 Prepare，backend Prepare 调用为零 |
| 同上 | Shutdown | Shutdown 可返回；释放回执后拒绝 Prepare，backend Prepare 调用为零 |
| backend 已返回、确认记录在 Rename 后尚未完成目录 fsync | watchdog 到期 | 到期取消当前 call；持久化返回后不报告 Prepare 成功 |
| 同上 | Shutdown | Shutdown 可返回并取消当前 call；持久化返回后不报告 Prepare 成功 |

调用前的候选内容可能已与 admission 中的内容相同，因此 RPC 可以是幂等
读回；它不必产生第二次 fsync。测试明确区分第二个 mutation RPC 与第二次
实际目录 fsync，后者发生在 backend 返回后的确认路径。

watchdog 测试直接交付 fixture 的 monotonic timer，墙钟仍在签名有效期内。
测试专用只读 accessor 只用于等待到期标记，不设置生产状态；backend 的
进入标记会记录连同已取消 context 的调用尝试。全部场景保留 `highest=10`
及一条 pending execution。关闭或到期不等于 drain、容量回收或重试许可。

## 实现不变量

`operationMu` 和 `admission.mu` 继续串行化实际命令、candidate 写入及共享
准入状态。只在短临界区内读取 active execution、generation、backend
已确认的 authority，再释放 `Service.mu` 等待存储。调用前对 authority
做独立副本，I/O 后重新锁定并核对原 execution/generation、到期、取消与
关闭状态，通过后才安装 backend call 的取消句柄。

确认路径保留实际 backend envelope 的持久化与记录语义；到期或关闭可以
取消已安装的 call。写入或读回的不确定性仍按既有规则保留恢复限制，不能
把已发生的 backend 操作改称未执行。

## 验证与范围

| 验证 | 最终结果 |
| --- | --- |
| 基线产品代码 + 最终四场景测试，race | 预期失败 4/4；最终产品代码通过 4/4 |
| 完整 ModelRuntime package，uncached race | PASS，104.236 s |
| Linux native race runner | 58 个 ModelRuntime + 25 个 Node 主测试通过，无 skip/race |
| ProductionAgent CPU Job loop | 6 Jobs / 24 Stages / 6 Charges，4 次已提交 materialization 回放；结束时 active allocation/lease、payload scratch、watchdog、各类 pin 为零 |
| direct 与 durable-stream exact-cache | 各完成 source 4 个物理 Stage、target 2 个物理 Stage，2 次 cache ADMIT / 2 次 HIT；durable-stream 额外回放 4 次提交 |
| 三个 CPU campaign，race | PASS，合计 45.441 s；同一源码摘要，计时不作性能对比 |
| 普通 repository tests / vet / lint | PASS，lint 0 issues |
| Linux ModelRuntime lint / amd64 compile-only | PASS；交叉编译不计作执行证据 |

原始终态、同源摘要、native image/binary 摘要和 CPU campaign receipts
见[验证记录](journal-dispatch-lifetime-evidence-2026-09-09.json)。Native runner
运行 Linux 非 root race binary；新用例的 owner/transport 在同一测试进程，
不是一次新的独立 Node/Runtime 进程故障实验。

CPU ProductionAgent Job 和 exact-cache campaign 用于检查此改动对原有
执行、回放、Charge 与清理路径的兼容性；其 Runtime journal 仍是本地
`runtime-admission` 装配，不能算 protected remote-owner Job 闭环。

这不是固定停止时限的证明：`operationMu`、共享 admission、其他 journal
操作、不可合作 backend 及其后代仍可能延迟真正的停止。未改变 fsync 的
阻塞语义，也未实现启动许可、Worker 输入/materialization 独立保管、历史
安全回收或持续负载。Production Gates 保持 **0/9**。
