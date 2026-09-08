# Node → Fleet → PostgreSQL 首次 Runtime 预留验证

基线：`0a4cb28`。范围：CPU 组件装配，不使用 GPU、不部署、不发放 backend
启动许可。本次把此前分开的原始进程证据与真实 PostgreSQL/mTLS 接在同一次
预留操作中。结果与原始文件见[receipt](node-fleet-reservation-evidence-2026-09-09.json)。

## 实际运行路径

host integration test 创建独立 PostgreSQL，迁移到 schema 95，装入批准的
单成员 bundle，并启动真实 Fleet service/transport 与临时 Registry signer。
临时 listener 绑定随机 TCP 端口，要求已注册的 Node Agent mTLS 身份。
测试结束后关闭 listener、删除 PostgreSQL 容器。

Linux scratch 容器运行 source-matched 静态 race 测试二进制。容器不挂载 host
目录、Docker socket 或 GPU；仅赋予 PID namespace 与原始进程检查所需的
`SYS_ADMIN`/`SYS_PTRACE`，并关闭测试容器的 seccomp 默认过滤。临时证书经
stdin 进入 Node helper，再写入容器内 0600 文件；不出现在 argv/env 或证据日志。

root Node 通过真实 Fleet TLS 客户端取得首次 bootstrap claim，创建实际
root 私有 Runtime journal 并保留文件锁、记录 startup intent，将该 journal
ID/scope 写入 Registry receipt，然后通过 TLS 取得签名 binding。Node 验证
bundle 与签名后，认证独立非 root PID-1 Runtime 的 challenge-bound 请求，
保留原始 pidfd，并调用 `RuntimeStartupLedger.ReserveRemote`。

完整 Node record 的 SHA256 成为 Fleet `OwnerObservationDigest`。服务端从
已注册 TLS principal 确定 Node/actor，数据库独立核对批准向量与 journal。
测试两端分别读取同一条数据库历史，逐项匹配原请求和完整 Node record 摘要。

## 结果

| 情景 | Fleet 预留 RPC 次数 | 数据库预留条数 | Node 本地成功回执 | 重开后行为 |
| --- | --- | --- | --- | --- |
| 正常首次调用 | 1 | 1 | 1 | 读取精确历史，拒绝再次预留，不重建 pidfd |
| 数据库提交后服务端丢失回包 | 1 | 1 | 0 | 保留 intent，拒绝再次预留，不重建 pidfd |

两种情景均使用 TLS 1.3。丢包在真实 handler 成功提交后由 interceptor 注入；
Node 收到 `Unavailable` 和空结果。计数覆盖所有预留 RPC，包含 live 重试和
ledger 关闭重开后的尝试；只读 history RPC 不计为预留调用。

native runner 同时通过 57 个 modelruntime 和 25 个 Node 主测试，包含此前
7 个实际 SIGKILL 边界与 12 个错误返回边界。当前两种 PostgreSQL/TLS 情景
本身没有杀死 Node；不能把独立 SIGKILL 测试当成已覆盖本次跨组件崩溃。
普通 tests/vet/lint、Linux lint 与 compile-only 通过。integration-tag lint
保留 82 项旧诊断，本次修改文件无新诊断，详见 receipt 中的 baseline 对照。

复现顺序：使用 `hack/run-journal-owner-native.sh` 构建并验证 native image，
再将其 `image.txt` 的精确 ID 传入 `VELA_RUNTIME_STARTUP_NODE_IMAGE`，执行：

```sh
go test -race -tags=integration ./internal/integration \
  -run '^TestRuntimeStartupNodeProcessPostgresTLS$' -count=1 -v -timeout=3m
```

该 integration test 是显式 opt-in；未设置 image 时的 skip 不构成成功证据。
当前 receipt 固定 source manifest、image ID、二进制 hash、命令终态与原始
Node report。本次使用 Docker Desktop 的 `host.docker.internal` 访问临时
Fleet listener；其他环境需具备等价 host gateway 路由。

## 证据边界与下一步

Pod/CRI responses 仍为 fixture，只是它们关联的 Runtime 进程与 pidfd 为真实
Linux 对象；未运行 Kubernetes/containerd workload。Worker journal ID/scope
也为显式 fixture，Runtime journal 则是实际 Node-held root storage。模型
bundle 的设备描述仅用于批准数据校验，没有执行 GPU 分配或 backend factory。

因此，本次闭合的是**原始 Runtime → Node 持久 intent → 认证 Fleet → 数据库
预留 → Node history**。有效 OCI/executable/config 批准、一次性 Node grant、
Node 命令与受保护挂载、Worker input/transfer/materialization 独立保管、
完整 remote-owner Job 和替换/持续运行仍需完成。Production Gates 保持 **0/9**。
