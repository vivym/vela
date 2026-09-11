# marslab Linux native validation

日期：2026-09-11。测试主机：`marslab@100.111.196.116`（Ubuntu 24.04，amd64，kernel `6.8.0-137-generic`）。本记录只描述隔离 CPU native 测试，不构成 Production Gate 或部署批准。

## 内核前置条件

主机实际创建的 pidfd 为：

```text
anon_inode:[pidfd]
pidfd filesystem type 0x9041934; requires pidfs (0x50494446)
```

`internal/runtimechannel/wire_linux.go` 在 `pidfs` 上使用 device/inode；旧 kernel 的 anonymous-inode pidfd 则使用内核提供的 `fdinfo` `Pid/NSpid` 身份、live-poll 和 `FD_CLOEXEC` 检查。Docker 与 scratch image 共享宿主 kernel；更换 Go builder、镜像或 compiler 不能改变 pidfd 语义。

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

`SameLiveProcess` 对旧 kernel 采用受限 `fdinfo` 兼容路径；descriptor 不是真正 pidfd、fdinfo 缺失/异常或身份不一致时仍返回 `ErrIdentity`，不会通过 numeric PID 重新打开进程。

## 当前边界

Production Gates 仍为 `0/9`，没有 Launch Receipt。本次兼容路径允许 6.8 kernel 运行 pidfd custody，但仍需重跑完整 native suite 和安全审计；本次改动尚未完成 `cmd/vela-node-agent` 的真实 startup authority composition root 接线。

兼容源码重新构建后的 v2 runner 统计为 `42 PASS / 2 FAIL / 0 SKIP`；两个失败用例随后在同一镜像中单独重跑均通过：`TestJournalServerExecObservedRemoteCLI`（6 个场景）和 `TestRuntimeJournalObservationConcurrentClose`。这表明剩余问题是时序/资源波动，不能把 v2 首次整套结果直接当作零失败闭环；应继续做稳定性复跑并保留原始日志。

修复退出态 `Pid: -1` 后，v5 镜像的 native 选择集实际为 `44 PASS / 0 FAIL / 0 SKIP`。runner 末尾的 `rg` 汇总命令在该主机未安装 `rg`，因此脚本返回 `127`；原始 `native.log` 本身已完整结束为 `PASS`，不能把脚本返回码误记为测试失败。
