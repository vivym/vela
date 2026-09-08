# Node 原始 Runtime 与首次启动预留的持久关联

更新：2026-09-09。实现基线：`a89e98f`；崩溃验证修复基线：`1fe814f`。
本增量为 CPU/mock 组件验证，
不发放 backend 启动许可，不构成完整 remote-owner Job 或 Production Gate。

## 问题与实现

Fleet 已能通过真实 mTLS 通道原子预留首次 Runtime epoch，但 Node 原来的
startup ledger 只保存进程关联，没有将本地持有的 journal 与这次远程操作
持久关联。回包丢失后，不能依据一个新调用或重启后的历史产生新权限。

`RuntimeStartupLedger.ReserveRemote` 现在执行以下顺序：

1. 匹配签名 launch plan 与认证 Registry client 的 Node/actor；检查 Node
   当前持有的 root journal、完整 manifest、首次启动 nonce 和实际 owner routes。
2. 认证原始 Runtime caller，保留其 pidfd，记录 executable 观察和 journal
   identity/digest、完整成员 epoch 向量；先 append/fsync 本地 startup intent。
3. 重新检查原始进程、Pod resourceVersion、executable 和 journal，再调用一次
   Fleet。请求 ID 使用本地 operation ID；owner 摘要覆盖完整持久 startup record。
4. 只接受本次创建且与完整请求相等的 Fresh 回包；再次检查同一进程与 journal，
   再 append/fsync reservation receipt。给 adapter 的可变字段均复制。

`ExecutionJournalOwner.InspectStartup` 在持有 owner 锁时比较 canonical manifest、
实际 routes 与独立 `floor + 1` 向量、当前文件状态及完整 journal 证明。
它不认证 UID 或进程；Node 另外检查 root custody。

新 ledger 使用 schema **2**，为 pending reservation receipt 和最终 owner exit
预留字节预算。schema **1** 仍支持原来的 local association 历史与写入，但拒绝
remote reservation，不能自动升级。receipt 无 `Fresh` 或 `Permit` 字段。

## 故障与恢复合同

| 故障位置 | 重开后的历史 | 可否再次远程预留 |
| --- | --- | --- |
| intent 写入前 | 尚无 intent，Fleet 未调用 | 允许安全的真正首次调用；当前不确定句柄已拒绝继续 |
| intent append 后、fsync 前后 | 完整记录需经过严格解析与重新 fsync；不完整记录拒绝打开 | 已存在 intent 时拒绝 |
| Fleet 请求/回包不确定、历史回包或字段不匹配 | intent 保留，receipt 缺失 | 拒绝 |
| 回包后原进程退出、Pod 或 journal 改变 | intent 保留，receipt 缺失 | 拒绝 |
| receipt append 后、fsync 前后 | 完整 receipt 仅供读取历史 | 拒绝 |

Node 重启不会从序列化 PID 重新打开或推断原 pidfd；即使测试已杀死后代，
`RecordExit` 也必须报告原 owner 证据丢失。失败返回空结果，不把已写但回包
不确定的内容当成新的启动许可。没有 release、retry 或 grant 恢复接口。

## 验证与证据

命令终态、主测试清单、source digest 和原始 artifact hashes 见
[机器可读 receipt](node-runtime-reservation-evidence-2026-09-09.json)，原始文件在
[evidence 目录](evidence/node-runtime-reservation-2026-09-09/)。

- native race：真实 root Node journal、独立非 root PID-1 Runtime、实际 caller
  IPC 与 pidfd；Fleet/Pod/CRI 为明确的 fixture。覆盖正常关联、别名隔离、重复与
  重启拒绝、错误 routes/principal、schema 1 兼容，以及 12 个错误返回边界。
- native SIGKILL：独立 Node helper 在 intent 三个写入边界、Fleet fixture
  回包前、receipt 三个写入边界被杀；父进程重新打开同一文件并检查历史与原 pidfd
  缺失。helper 使用自己的 PID/mount namespace 和对应 procfs；namespace 销毁
  仅用于测试后代清理，不是生产 containment 证据。
- `InspectStartup` 独立验证：合法首次状态、变更 manifest、journal/scope、
  incarnation、launch digest、实际 route epoch、文件变化、关闭与取消。
- 同源 native race 回归包含现有 journal owner、Node server/channel 和 startup
  ledger；普通 repository tests/vet/lint、Linux lint 与交叉编译另行记录。
  macOS 上 Linux 专属测试不会运行；`[no tests to run]` 不算行为验证。

初轮新 fixture 的目录权限及旧 route epoch 构造不满足已有前置条件，已修正；
SIGKILL fixture 初轮继承了外层 procfs，身份检查按预期拒绝，随后修正私有
namespace 内的 procfs 与过长的临时 socket 路径。PID namespace init 自发
SIGKILL 没有使进程在注入点退出，现改为 child 通过独立 pipe 报告精确边界，
外层 parent 杀死该 child 并核验 `WaitStatus.Signal()==SIGKILL`。

`1fe814f` 将测试名加 `_DISABLED` 的做法并未禁用 Go 测试，且使 helper 的
精确选择名失配；该提交不构成完整 native runner 已通过的证据。当前已恢复
精确测试名，并实际通过完整 runner。该提交曾引用但未创建的 JSON receipt
和 evidence 目录在本次补齐。未放宽运行时代码中的任何身份或目录检查。

## 尚未覆盖

这次 native 测试没有连接真实 PostgreSQL/mTLS；前两个 Fleet 增量的 DB/TLS
证据与本轮原始进程证据不能拼成一条已经通过的完整链路。进程 SIGKILL 不模拟
掉电、存储设备丢失或所有可能的 torn write。

executable hash 是时间点观察，不能证明有效 OCI 配置或不可中断的 executable
provenance。仍需有效 launch 批准、同一次调用的一次性 grant transaction、
Node 命令与受保护挂载、Worker input/transfer/materialization 独立保管，以及
同一装配的完整 CPU Job、恢复与长期运行验证。Production Gates 保持 **0/9**。
