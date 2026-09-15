# marslab Linux native validation

日期：2026-09-11。测试主机：`marslab@100.111.196.116`（Ubuntu 24.04，amd64，kernel `6.8.0-137-generic`）。本记录只描述隔离 CPU native 测试，不构成 Production Gate 或部署批准。

## 内核前置条件

主机实际创建的 pidfd 为：

```text
anon_inode:[pidfd]
pidfd filesystem type 0x9041934; requires pidfs (0x50494446)
```

该主机没有 `pidfs`；`f_type=0x09041934`，而 `PID_FS_MAGIC=0x50494446`。`internal/runtimechannel/wire_linux.go` 因而走 anonymous-inode pidfd 分支：句柄本身可做 live-poll、signal 和 `FD_CLOEXEC` 检查，只有当 procfs 能提供非零 `fdinfo` `Pid/NSpid` 时才可做同进程身份比较。进入独立 PID namespace 后，隐藏进程的 `Pid/NSpid` 都可能为 `0`；这种句柄必须交给宿主 namespace 的受信任身份 broker，当前 `SameLiveProcess` 会 fail-closed。Docker 与 scratch image 共享宿主 kernel；更换 Go builder、镜像或 compiler 不能改变 pidfd 语义。

## 官方 runner 结果

证据目录：`/home/marslab/vela-evidence/remote-cli-official-20260911-v5`。

```text
PASS 9
FAIL 35
SKIP 0
WARNING: DATA RACE 0
terminal FAIL
```

构建器已经固定为 `golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1`，scratch image 为 `sha256:3975381f5dcea5096a709e13bbe70d71faceca6dc05f5e0f08c801885682864c`。该 v5 运行使用旧版强制 `pidfs` 的源码，失败集中在 pidfd identity/custody 前置条件，不能表述为完整 native suite 通过。

## runner 修复

`hack/run-remote-runtime-cli-native.sh` 和 `hack/run-journal-owner-native.sh` 现在显式设置 `umask 022`，并在创建后将 rootfs 与 `/run` 设为 `0755`；`/tmp` 继续为 `01777`。在远端用 `umask 002` 复测，目录权限为 `755/755/1777`，消除了此前的 `secure executable ancestor "run" is not trusted` 环境噪声。

`SameLiveProcess` 对旧 kernel 采用受限 `fdinfo` 兼容路径；descriptor 不是真正 pidfd、fdinfo 缺失/异常、身份不可见或身份不一致时仍返回 `ErrIdentity`（身份不可见同时包含 `ErrPIDFDIdentityUnavailable`），不会通过 numeric PID 重新打开进程。

## 当前边界

Production Gates 仍为 `0/9`，没有 Launch Receipt。本次兼容路径允许 6.8 kernel 在身份可见的宿主 namespace 中运行 pidfd custody；私有 PID namespace 的跨 namespace 比较仍需 Node-side identity broker。完整 native suite、安全审计和 `cmd/vela-node-agent` 的真实 startup authority composition 仍未完成。

兼容源码重新构建后的 v2 runner 统计为 `42 PASS / 2 FAIL / 0 SKIP`；两个失败用例随后在同一镜像中单独重跑均通过：`TestJournalServerExecObservedRemoteCLI`（6 个场景）和 `TestRuntimeJournalObservationConcurrentClose`。这表明剩余问题是时序/资源波动，不能把 v2 首次整套结果直接当作零失败闭环；应继续做稳定性复跑并保留原始日志。

修复退出态 `Pid: -1` 后，v5 镜像的 native 选择集实际为 `44 PASS / 0 FAIL / 0 SKIP`。runner 末尾的 `rg` 汇总命令在该主机未安装 `rg`，因此脚本返回 `127`；原始 `native.log` 本身已完整结束为 `PASS`，不能把脚本返回码误记为测试失败。

随后将 runner 结果检查改为 `grep`，v6 在同一目标主机重新构建并执行完整选择集，脚本输出 `Actual remote Runtime CLI checks passed`，证据目录为 `/home/marslab/vela-evidence/remote-cli-compat-20260911-v6`。该结果覆盖 44 个顶层 PASS，0 FAIL，0 SKIP，并通过脚本内的场景清单检查。

## 2026-09-12 Worker journal 分片 focused native evidence

为验证 Node-owned Worker journal 的大记录传输边界，将当前工作树复制到目标机
`/tmp/vela-validation-src`，在 Ubuntu 24.04 / kernel `6.8.0-137-generic` native
执行：

```text
go test ./internal/workerjournalwire -count=1                         PASS
go test -c -o /var/tmp/nodeagent.test ./internal/nodeagent              PASS
/var/tmp/nodeagent.test -test.run WorkerJournalMaterializationChunk -test.v PASS
  TestWorkerJournalMaterializationChunkAssembly                       PASS
  TestWorkerJournalMaterializationChunkRejectsDigestAndSequence       PASS
```

该 focused 证据覆盖 12 KiB 分片的严格 offset、总长、SHA-256、request ID 生命周期、
顺序错误和错误 digest 拒绝；它没有覆盖真实 signed plan、CRI socket mount、Node restart
或九类 Production Gate receipt。`Production Gates` 仍为 `0/9`。

同一目标机以 root native 执行 `go test ./internal/runtimechannel -run
TestPIDFDBrokerComparesRetainedHandles -count=1` 通过（`same`、`different`、
`nested-same`、`wrong-gid`）。测试将临时 helper 放在可执行的 `/var/tmp`，而 broker
socket 仍在受保护的 `/run`；这是因为目标机 `/run` 明确挂载为 `noexec`，不能把该环境
限制误判为 pidfd broker 协议失败。

随后以 root native 执行 Node journal transport 集成测试：

```text
/var/tmp/nodeagent.test -test.run TestWorkerJournalRPC -test.v PASS
  TestWorkerJournalRPCPreActivationAndMutation PASS
  TestWorkerJournalRPCClientHelper               PASS
```

该测试真实启动 root-owned `JournalServer`、非 root Worker 子进程和 retained original
pidfd，验证 pre-activation mutation 被拒绝、同一 pidfd 通过 `SO_PEERPIDFD` 认证后经
`Activate` 成功写入并读取 Node-owned input journal。它仍不证明 signed plan/Fleet/CRI/
ModelRuntime 同一 composition，也不生成 Production Gate receipt。
