# 同次启动请求中的镜像检查与单次预留

日期：2026-09-09。基线：`65183bd`。本地 Linux/arm64 CPU fixture，真实
containerd v2.3.1 / runc v1.4.2，无 GPU、部署或生产放行。

## 改动与不变量

[上一轮](node-planned-image-evidence-2026-09-09.md) 将批准镜像默认入口与原始
caller 关联，但接口只接受 manifest declaration，尚未接到真实启动 envelope
和 Node journal。现在增加以下装配：

- `ObserveStartupImageCaller` 只接受原始 caller 经认证通道发送的 canonical
  `BackendStartupRequest`。Node、Runtime journal、scope、Registry binding
  摘要和 launch 摘要全部匹配 verified plan；原有 ledger 使用同一个解析函数。
- `ReserveImageRemote` 在同一次调用内检查 Node 持有且 root 保管的 Runtime
  journal、首次 incarnation 和 epoch routes，再采样同 daemon 的镜像入口、
  task mechanism、OCI argv 与活 executable。接口不能接收先前保存的观察记录。
- 在持久记录 intent 前及 Fleet 调用两侧各进行镜像/task 观察。三次观察必须
  关联相同 Pod resource version、原始进程、executable、镜像与 task 文件
  identity/bytes。即使 argv 不变，其余 task bytes 的可见变化也会拒绝。
- 镜像测量可能阻塞，因此在后两次测量完成后再次检查持有的 journal 和原始
  owner，再调用 Fleet 或落盘回执。错误不能留下可返回的成功回执；intent
  一旦持久化，重试或重开 ledger 均不能再次调用 Fleet。

代码：[请求绑定](../internal/nodeagent/runtime_startup_plan_linux.go)、
[镜像预留入口](../internal/nodeagent/runtime_startup_image_linux.go)、
[持久预留顺序](../internal/nodeagent/runtime_startup_reservation_linux.go)。

## 验证

[真实 CRI 测试](../internal/nodeagent/runtime_startup_image_integration_linux_test.go)
在同一装配里使用原始非 root PID-1 caller、正确镜像默认入口、认证启动请求、
实际 root-held Runtime journal 和 Node ledger。Registry/Pod/Fleet 仍为 fixture。
17 个场景覆盖：

| 场景 | 最低断言 |
| --- | --- |
| 正常启动请求 | 从 held journal 推导 epoch=2，只调用 Fleet 一次，回执可读回 |
| 错误 Node/binding/journal/scope/launch | 不记录 intent，不调用 Fleet |
| 错误 incarnation、仅 manifest | 不记录 intent，不调用 Fleet |
| 缺 image observer、错误 runtime policy、入口被 bind mount 替换 | 不记录 intent，不调用 Fleet |
| intent fsync 后改变 task Env，保持 argv 和 executable 不变 | 保留 intent，Fleet 调用为 0，无回执 |
| Fleet 回包期间改变 task Env | Fleet 调用为 1，保留 unresolved intent，无回执 |
| Fleet 丢回包、回包期间关闭 image observer | 不返回成功回执，不重试预留 |
| 第二次镜像读取期间关闭 journal | 镜像读取后的 custody 检查拒绝，Fleet 调用为 0 |
| 第三次镜像读取期间关闭 journal | Fleet 调用为 1，落盘前 custody 检查拒绝，无回执 |

凡已记录 intent 的场景均实际重开 ledger，验证成功回执只作为历史保留、失败
不能恢复出回执、不能重建原始 pidfd 或再次预留。每个场景检查 `k8s.io`
namespace 没有遗留镜像观察 lease、view 或 activation。

为验证检查顺序，临时编译 overlay 将后两次 journal 检查移回镜像读取之前，
不改变工作区。上述两个 journal-close 测试分别复现“本应拒绝却调用 Fleet”
和“丢失 custody 后仍返回成功回执”。修正顺序的同源原生测试通过。
这是本次开发中检查顺序的反例，不是声称既有生产接口已经签发过错误许可。

最终 native runner 的 9 个主测试、以及独立的 11 个 startup ledger/reservation
主测试均通过，无 skip/race；后者包括实际 Node helper SIGKILL 恢复和实际
factory 之前的 ledger 屏障。普通 `go test ./...`、vet、lint 和 Linux integration
Node lint/vet 通过；amd64 为交叉编译证据。原始日志、overlay、测试命令、
binary/source 摘要见 [机器清单](node-startup-image-evidence-2026-09-09.json)。

## 尚未证明

返回值是 Fleet 预留的历史记录，没有 grant/Permit。原有 `ReserveRemote`
仍只提供其原先的观察与预留契约；两者都没有生产 CLI 调用方。此轮没有把
真实 PostgreSQL/TLS 和真实 CRI 镜像链放到同一测试中，也没有实际 Worker
业务 journal、完整 Runtime 服务入口或四 Stage Job。

task bytes 不变不代表其 env/config/mount 值已获批准，也不证明区间内从未
发生变化。当前策略来自测试装配，effective mount/writer、配置文件来源、
shim/runc executable 批准和执行连续性仍未完成。Root 管理员和内核仍受信任。
一次性 Node grant、受保护 CLI/Fleet 装配和真实后代/设备停止需要后续实现。
Production Gates 保持 **0/9**。
