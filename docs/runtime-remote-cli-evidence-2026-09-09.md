# 实际 Runtime CLI 使用 Node journal 的启动装配

日期：2026-09-09。基线：`333d3e5`。范围：本地 CPU、Linux/arm64 原生进程与
真实 Unix RPC；未部署、未运行 GPU。

## 装配变化

此前 `StartRuntimeServer.RemoteStartup` 已支持 Node journal，但实际 CLI 仍
读取环境变量指定的多个文件并构造本地 epoch store。现在新增真实入口：

```sh
vela-model-runtime serve-remote --bootstrap-file /etc/vela/runtime/bootstrap.json
```

入口首先关闭 dumpability，并要求非 root UID/GID。它不调用旧环境配置加载器，
只读取一次由可信 Node 装配发布的 `RemoteRuntimeBootstrap`。配置包含：

- 完整 LaunchManifest、StageAuthority/Registry **公钥**与确定性 protobuf
  Registry binding；不接受私钥、backend factory 或任意 authorizer 回调。
- Node Runtime journal identity、storage identity、已持久化的 unresolved
  incarnation；这些是启动声明与历史，配置中没有 Permit。
- 三个互异的 journal/startup/Runtime socket，以及有界的操作/关闭时间。

读取器逐级检查 root-owned、不可由 group/other 写入且没有符号链接的目录；
最终文件必须是 root-owned、无任何写入或特殊权限位、单链接、有界的普通文件。
先用 O_PATH 检查类型，再通过自己的 procfs FD 重开被固定的 inode，避免重新
按可能变化的文件名打开设备/FIFO。读取前后核对文件身份、权限和变化时间。
只读指文件权限，不声称文件系统是只读挂载；root 管理员和内核仍受信任。

解析拒绝重复/未知字段、非规范编码和超限输入，并验证 manifest、scope、
签名 journal binding、public keyring、incarnation 及时间配置。服务装配使用
真实 `UnixRuntimeJournalTransport` 和 `NewNodeBackendStartupGate`，没有本地
epoch store 或 execution file owner；启动仍必须通过独立 Node 授权。

源代码：[配置契约](../internal/modelruntime/remote_bootstrap.go)、
[受保护读取与装配](../internal/modelruntime/remote_bootstrap_linux.go)、
[CLI](../cmd/vela-model-runtime/remote_linux.go)。

## 实际验证

[原生 runner](../hack/run-remote-runtime-cli-native.sh) 构建并运行实际
`vela-model-runtime`、独立非 root Worker 和真正的 process backend；backend
是实现 initialize/shutdown 协议的 CPU test helper，没有注入 BackendFactory。
root Node 使用实际 JournalServer、原始 pidfd 角色和持锁 journal。测试只在
继承同一个已登记 PID 的 helper 上 exec CLI；这用于装配验证，不是生产登记协议。

18 个实际 CLI 场景全部通过，无 skip/race：

| 场景 | 结果 |
| --- | --- |
| permit fixture | 授权前无 backend 事件；实际 initialize 一次，Worker 发现 epoch=2，SIGTERM 后实际 shutdown 一次且 CLI exit=0 |
| deny fixture | CLI exit=1，无 backend 初始化、无 Runtime socket |
| 非规范配置、重复/未知字段、socket 重合、错误公钥、scope | 在使用配置或读 journal 时拒绝，无 backend 初始化 |
| 旧 incarnation | 实际 Node journal 读取后拒绝，无启动授权请求 |
| workload-owned 文件/父目录、可写文件、symlink、hardlink、FIFO | 拒绝，不打开非普通文件内容 |
| 缺失、空或超过 4 MiB 的文件 | 拒绝，无 backend 初始化 |

正例带有故意错误的旧 `VELA_MODEL_RUNTIME_*` 文件、socket、journal 与 epoch
环境变量，服务仍使用 bootstrap 内的配置。所有场景检查 Node journal 的原始
incarnation 与 Highest=0 不变、工作目录没有额外 journal/epoch 文件；成功
关闭后 socket 删除，Node server joined 且 InFlight=0。

同一 runner 保留实际 Worker 执行、seal/drain 的既有远程服务测试；其 backend
仍为独立的 fake fixture，不能与本次 CLI 初始化/关闭证据合写成完整 CLI Job。
另有 bootstrap 正例、返回数据独立性与 15 个绑定/编码/参数反例的普通测试。
全库 test/vet/lint、Linux 三个相关包的 lint/vet 通过；amd64 仅交叉编译。
固定源代码、binary、镜像、命令和原始日志见
[机器清单](runtime-remote-cli-evidence-2026-09-09.json)。

联调失败均保留：首次由 root 测试端连接非 root 私有 socket，触发正确的目录
信任拒绝，已改由实际 Worker 发起发现；第二次测试 helper 把带 Manifest 的
发现请求误路由到 Supervisor 分支，已修正测试路由；第三次沿用 fake fixture
的 1 秒退出预算，与 race binary 默认 1 秒退出延迟冲突，已改用现有 CLI
fixture 的 5 秒 driver 预算。未降低产品权限检查或放宽产品时间边界。

## 剩余边界

配置发布、有效 mount/writer、获准镜像与这个原始 CLI 的关联仍需可信 Node/Fleet
装配；root-owned 文件本身不能证明具体发布者或当前 mount 来源。当前 Node
启动回复为显式 fixture Permit，尚未把 `ReserveImageRemote`、一次性 grant、
真实 Fleet/TLS/PostgreSQL 和本入口放到同一调用。生产 Node daemon、Fleet
挂载/配置发布尚未接通，默认 CLI/Fleet 路径仍使用此前的本地模式。

没有运行四 Stage CLI Job、Worker 独立业务 journal 或真实后台执行/后代隔离
故障 campaign；本轮只证明实际 CLI 的启动/发现/关闭及相关拒绝行为。
Production Gates 保持 **0/9**。
