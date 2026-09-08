# Node task runtime options、共享 shim 与 hook 内容检查

日期：2026-09-09。基线：`39b80db`。范围：本地 Linux/arm64、真实
containerd v2.3.1 / runc v1.4.2、CPU helper；无 GPU、部署或生产启动放行。

## 改动

OCI `config.json` 不能单独描述启动机制。containerd 的 runc shim 还使用 task
bundle 中的 `options.json` 与 `runtime`，例如显式 binary、runc state root、
NoPivotRoot、keyring、cgroup 和 checkpoint 选项。因此，在前一轮已验证的
daemon 文件系统视图中，`ObserveTaskLaunch` 现在还读取与复查这些文件。
`runtime` 可以为空以如实观察默认配置，但必须与 options 的 BinaryName 相同。

真实 CRI v3 的工作负载 task 复用 sandbox shim：`shim_manager.go` 在 task bundle
记录 `sandbox` 与 `bootstrap.json`，并从 sandbox 复用连接参数；只有启动 shim
的 sandbox bundle 包含 `shim-binary-path`。本次只支持这种实际验证过的布局：

1. task 的 `sandbox` 必须等于已由 CRI 关联的 SandboxID。
2. task/sandbox 两份 `bootstrap.json` 必须逐字节相同，且是受支持的 canonical
   version 3 / ttrpc 记录；不接受额外 capabilities/metadata。
3. sandbox bundle 的目录与文件保持 root 私有约束；读取结束重开目录与复查
   所有文件的 identity/content，不能静默换用另一个 sandbox。
4. 从该 sandbox 读取 shim 路径；不回退到可变 container metadata。

`RuntimeOptions()` 返回独立副本；`CheckRuntimeMechanism(policy)` 使用对象内部
保留的原始字节，调用方修改公开 observation 字段不能改变检查结果。这个内容
策略要求显式、规范的 shim/runc 路径和 runc state root；cgroup 模式必须匹配。
其他 options 必须为默认值，所有 OCI Hooks 结构均拒绝，包括空结构。
`options.json` 按 upstream 实际 `encoding/json` 输出做 canonical 校验，拒绝
未知字段、重复或大小写变形字段、null 与多余 JSON；不使用 protojson 字段命名。

代码：[配置来源](../internal/nodeagent/runtime_task_launch_linux.go)、
[内容策略](../internal/nodeagent/runtime_task_mechanism_linux.go)、
[原生反例](../internal/nodeagent/runtime_task_mechanism_integration_linux_test.go)。

## 验证

最终 [runner](../hack/run-task-launch-native.sh) 的七个主测试全部通过，无 skip/race：
实际 CRI 主测试 16.78s，native containerd 主测试 6.40s。编译和运行隔离、固定
依赖与无宿主挂载/无网络/私有 PID/cgroup 范围沿用
[daemon/state 验证](node-task-state-evidence-2026-09-09.md)。

| 场景 | 结果 |
| --- | --- |
| 默认实际 CRI handler，options 为 `{}`、runtime 为空 | 来源可观察，显式内容策略拒绝 |
| 实际 CRI handler 明确配置 `/usr/local/bin/runc` 与 `/run/vela-approved-runc` | 来源可观察，内容策略匹配 |
| 两个 caller × 六个机制文件 × 七种错误：missing/writable/wrong-owner/symlink/hardlink/FIFO/oversize | 84 个反例全部拒绝 |
| 两个 caller 的 sandbox marker、bootstrap、runtime 三种矛盾 | 六个反例全部拒绝 |
| 显式 handler 的 bundle 出现 createRuntime hook 声明 | 来源仍可读取，内容策略拒绝；测试未执行 hook |
| 12 种附加 options、deprecated task API 覆盖、错误策略路径、伪造公开字段 | 策略拒绝；返回副本不能改变内部字节 |
| canonical options、bootstrap 解析；匹配的 cgroup 配置 | 按对应正负用例通过 |
| 既有复制 state、Node shadow mount、caller/daemon lifetime、20 个 config 文件反例 | 继续通过 |

`go test ./...`、`go vet ./...`、golangci-lint v2.13.1，以及 Linux integration Node
lint/vet 均通过。Linux/amd64 是交叉编译证据，没有声称实际 amd64 执行。
原始日志、测试使用的 source patch、逐文件摘要、官方源码路径/摘要、镜像与
二进制摘要见 [机器可读清单](node-task-mechanism-evidence-2026-09-09.json)。

首次实现错误地从 task 自身寻找 `shim-binary-path`，两个真实 caller 都以
`no such file or directory` 拒绝；该失败日志与初始 patch 已保留。随后根据
固定 upstream `shim_manager.go`/`binary.go` 修正为上述 sandbox 关联，未把
“找不到文件”当成默认配置放行，也未从 metadata 猜测 shim。

## 边界与下一步

这里通过的是 **内容策略检查**。Node policy 与显式 handler 由测试配置提供，
没有声称它们已接到生产的批准链；planned-owner 的 Registry/Pod 仍是 fixture。
读取的路径没有证明 daemon/shim/runc 的 executable 字节或它们曾经执行的历史。
默认 ancillary options 和 hook 拒绝也不等于工作负载有效 mounts、argv/env、
config preimage、image/executable、后代与设备隔离均已批准。

下一步仍需把这些内容与固定发布/manifest、实际 protected mount/file 和原始
owner 关联，再接入一次性 grant、Node/Runtime CLI/Fleet、Worker 业务 journal，
完成同一装配下完整四 Stage CPU Job 及其故障恢复。此报告未运行 PostgreSQL
或完整 Job campaign，不能替代那些证据。Production Gates 保持 **0/9**。
