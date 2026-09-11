# marslab Linux native validation

日期：2026-09-11。测试主机：`marslab@100.111.196.116`（Ubuntu 24.04，amd64，kernel `6.8.0-137-generic`）。本记录只描述隔离 CPU native 测试，不构成 Production Gate 或部署批准。

## 内核前置条件

主机实际创建的 pidfd 为：

```text
anon_inode:[pidfd]
pidfd filesystem type 0x9041934; requires pidfs (0x50494446)
```

`internal/runtimechannel/wire_linux.go` 的 `SameLiveProcess` 要求 `unix.PID_FS_MAGIC`，并拒绝 anonymous-inode pidfd。`ReceiveRuntimeObserverCustody` 在握手前调用该检查，因此 observer custody、startup orchestration 和依赖它们的 native 用例会 fail closed。Docker 与 scratch image 共享宿主 kernel；更换 Go builder、镜像或 compiler 不能满足该条件。

## 官方 runner 结果

证据目录：`/home/marslab/vela-evidence/remote-cli-official-20260911-v5`。

```text
PASS 9
FAIL 35
SKIP 0
WARNING: DATA RACE 0
terminal FAIL
```

构建器已经固定为 `golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1`，scratch image 为 `sha256:3975381f5dcea5096a709e13bbe70d71faceca6dc05f5e0f08c801885682864c`。失败集中在 pidfd identity/custody 前置条件，不能表述为完整 native suite 通过。

## runner 修复

`hack/run-remote-runtime-cli-native.sh` 和 `hack/run-journal-owner-native.sh` 现在显式设置 `umask 022`，并在创建后将 rootfs 与 `/run` 设为 `0755`；`/tmp` 继续为 `01777`。在远端用 `umask 002` 复测，目录权限为 `755/755/1777`，消除了此前的 `secure executable ancestor "run" is not trusted` 环境噪声。

`SameLiveProcess` 对错误 filesystem type 现在返回包含实际 magic 值的 `ErrIdentity`，便于区分内核不兼容与其他身份错误，同时保持 fail closed。

## 当前边界

Production Gates 仍为 `0/9`，没有 Launch Receipt。要取得完整 custody 证据，需要支持 `pidfs` 的 Linux kernel 或另一台满足该前置条件的隔离主机；共享 GPU 主机的 kernel upgrade/reboot 需要单独协调。本次改动尚未完成 `cmd/vela-node-agent` 的真实 startup authority composition root 接线。
