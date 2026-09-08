# Runtime 启动预留的认证 Fleet 通道

日期：2026-09-09。基线：`5452a81`。范围：本地 CPU、真实 PostgreSQL 和
TLS 1.3，不包含 GPU 或部署。

## 已接通的路径

前一轮的持久预留只有 Go service 入口。本轮在 `FleetMaintenanceService`
加入 `ReserveRuntimeStartup` 和 `LookupRuntimeStartup`，为后续 Node grant
事务提供真实认证通道。`vela-control` 的现有 Fleet listener 显式装配该
service；未配置时返回 Unimplemented，不回退为测试授权器。

客户端复用 `BootstrapClient` 的已配置 Node identity。请求没有 Node/actor
字段，服务器必须从经过 TLS 验证且已注册的 Node Agent SPIFFE 身份推导二者。
服务器在调用消耗性操作前，先按该 principal 查询原始 bootstrap history 并
核对 journal pair；数据库事务继续独立核对完整 epoch、一次性身份和当前
Worker/bundle 状态。Node identity 相同但 Agent ID 不同也不继承原操作。

协议使用显式字段，不接受任意 JSON、路径或调用者指定的权限角色。所有 UUID
要求规范编码，incarnation 要求 UUIDv4，digest 必须为非零 32 字节，epoch
向量有 64 成员上限并拒绝重复 residency；嵌套消息和时间戳的未知字段也拒绝。
客户端单次 RPC 的消息上限为 64 KiB，服务器另做请求大小与结构检查。

创建预留的回包逐项绑定完整请求、认证 principal 和有效提交时间；client
只有完成检查才暴露 Fresh。错误、缺失或不匹配的回包均返回空结果。
history 的 protobuf response **没有 Fresh 字段**，不会从数据库历史恢复
启动权限。服务器拒绝底层 service 在 lookup 时错误返回 Fresh；客户端拒绝
通过未知字段夹带 Fresh 的 history。此封装没有应用层自动重试。

## 验证与证据边界

单元/race 测试验证未认证 caller、未配置 service、未知／畸形／重复向量、
错误 journal pair 在预留调用之前被拒绝；逐字段篡改创建回包不产生可用
reservation；lookup 不产生 Fresh。

真实 PostgreSQL 17 与 TLS 1.3 联合测试验证：

1. 其他 Node、同 Node 的另一 Agent、未注册证书不能预留或读取原始操作；
   请求携带未知扩展或不批准的 epoch 不新增任何 reservation。
2. 正常请求得到一个已提交的新预留。服务器 interceptor 在 handler 成功提交
   之后丢弃成功回包并返回 Unavailable 时，client 返回空结果；数据库仍只有
   一条记录，首次调用不会在客户端被自动重复。
3. 后续 history 和显式重试保留同一请求、全部绑定和首次时间，均为
   `Fresh=false`；更换 owner digest 不能复用旧 request。

这里的故障是在真实 RPC 服务器的回包边界注入，未声称进行物理网络丢包。
PostgreSQL receipt 和 owner digest 使用明确的 fixture 观察，未证明有效 OCI
配置、root 文件保管或原始 pidfd。该测试执行的是认证预留 RPC，不启动 backend
或执行完整 Job。

验证结果、工具命令、源码／协议 digest、生成一致性和原始日志见
[`runtime-startup-transport-evidence-2026-09-09.json`](runtime-startup-transport-evidence-2026-09-09.json)。
protobuf 变更是追加 RPC/message，经过 Buf lint 与相对基线的 breaking 检查。

最终源码下，12 个相关 PostgreSQL／TLS integration 主测试在 race 下通过，
用时 **63.939 秒**，无 skip、无 race。Fleet transport 与 Control 的完整
focused race 分别为 **3.220 秒**和 **2.084 秒**；仓库常规测试、vet、普通
lint、Linux/amd64 compile-only 检查通过。13 个 protobuf Go 生成文件与独立
输出目录中的重新生成结果逐字节一致。

integration-tag lint 仍报告 **82 项**（50 errcheck、4 staticcheck、28 unused），
其 23 个文件均与基线逐字节相同，本轮修改文件没有诊断；此项仍未通过。

## 仍须完成

这条通道消除了给 Fleet service 直接注入 Node/actor 的装配缺口，**不替代
Node 的持久 grant 事务**。下一段仍须从 Node 自己验证的 launch plan、root
journal 和原始 pidfd 构造 owner 摘要，持久关联 reservation 与启动调用，
并在进程／配置／journal 变化或任一步不确定时禁止发许可。不能把已认证 Node
提供的摘要当作其原像已被证明，也不能仅因预留成功就允许任意 backend factory。

Node 端生产命令、受保护挂载、有效 OCI 配置核对、Node 重启后的退出／对账、
Worker materialization journal 的独立保管和完整 remote-owner CPU Job
仍未完成。schema 保持 PostgreSQL **95**、Worker journal **5**、Runtime
journal **8**，Production Gates **0/9**。本轮不更新旧 native 启动或完整
CPU Job campaign 的证据归属。整体目标保持进行中。
