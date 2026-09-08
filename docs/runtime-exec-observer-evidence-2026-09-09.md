# Runtime 执行连续性：创建时 syscall observer 与实际 CLI 实验

基线：`8ae8673`，2026-09-09。CPU/Linux arm64，无 GPU。
本轮验证了一个从目标创建时开始跟踪的候选机制，并让**实际 Runtime CLI、
Node 只读 journal 和真实 process backend**在其下完成初始化、发现和关闭。
这是实验装配，不是生产 Node grant；生产启动行为没有修改，Production Gates
仍为 **0/9**。

## 从前一轮否证推进了什么

[旧 mem 句柄实验](runtime-execution-continuity-evidence-2026-09-09.md) 已证明
可读旧 mm 与原进程未 exec 不是同一回事。继续检查 Linux 后发现，单纯开启
`PTRACE_O_TRACEEXEC/TRACECLONE` 也不充分：`CLONE_UNTRACED` 可让新线程
不继承跟踪，再由该线程 exec 替换原进程。

本轮的关闭 clone 限制对照实际完成了这种替换：observer 没有记录 root exec
拒绝，同一原进程 PID 再次输出了 executable 入口的 READY。测试随后通过
保留的 pidfd 主动杀死逃逸目标；不能把这个清理算成 observer 的保护。

## 候选机制

实验 supervisor 是独立的 root C 进程，只监督自己创建的目标。目标在最初的
fork 后 `PTRACE_TRACEME` 并停止；parent 在第一次 executable 运行之前设置
选项，再开始 syscall 跟踪。目标随后清空附加组、降为计划的非 root UID/GID，
设置 `no_new_privs` 并执行入口。实际 CLI 场景中 supervisor 留在外层 PID
namespace，通过 `unshare(CLONE_NEWPID)` 后创建子进程，使 CLI 仍为其私有
namespace 的 PID 1；没有把 supervisor 冒充 Runtime namespace owner。

被验证的组合包括：

1. 在每个 syscall 入口/出口停下目标及继承跟踪的后代，读取内核 syscall
   信息并拒绝未支持的 ABI。原始线程组只允许最初一次入口 exec；后续
   `execve/execveat` 在内核切换映像之前触发拒绝和终止，覆盖非 leader 线程。
2. 在传统 `clone` 的入口检查寄存器中的标量 flags，拒绝 `CLONE_UNTRACED`。
   `clone3` 的参数是指针，其他线程可能在恢复后、内核复制前修改它，因此本
   原型不把采样参数当作约束，而是跳过该 syscall，返回 `ENOSYS` 触发 fallback。
   正常线程、fork/vfork 与独立 backend 进程的 exec 仍可运行。
3. 通过 `PTRACE_O_EXITKILL` 使 observer 退出时由内核终止仍受跟踪的目标。
   独立无 PID namespace 的对照覆盖目标与已 exec 子进程；关闭 EXITKILL 后，
   两者都能在 observer 被 SIGKILL 后继续存活，最后才由测试清理。

原型固定最多保留 128 个 task 记录，超过上限失败；这不是生产容量设计。
未验证的体系结构不允许降级为 PID 或 executable 周期采样。源码包含 amd64
分支，但本轮只执行了 arm64，不能声称 amd64 已验证。

## 原生实验结果

内核 `6.10.14-linuxkit`、`aarch64`；固定 builder，私有进程环境，无宿主
路径挂载、网络或 GPU。独立原型限制 2 CPU、512 MiB、256 PID，observer
拥有 SYS_PTRACE，目标使用 UID/GID 65534。C probe 关闭 dumpability；Go
compatibility probe 检查非 root 身份，用于线程/子进程兼容性，不作为内存
保护测试。实际 CLI 自身执行原有的进程保护入口。

| 独立场景 | 结果 |
| --- | --- |
| 正常 root 退出且有存活子进程 | root 正常退出，受跟踪子进程随 observer 退出终止 |
| 原始线程组 execve | 切换前拒绝；没有第二次 READY |
| 原始线程组 execveat | 切换前拒绝；没有第二次 READY |
| 新建 pthread 从非 leader exec | 创建事件覆盖新线程，在其 syscall 入口拒绝 |
| CLONE_UNTRACED 后 exec | clone 返回 EPERM，原目标继续正常运行 |
| clone3 | 返回 ENOSYS，原目标继续正常运行 |
| observer SIGKILL | 原目标和真实已 exec 子进程的保留 pidfd 都报告退出 |
| 关闭 EXITKILL 后 SIGKILL observer | 原目标与子进程仍活，反例成立 |
| 关闭 clone 限制 | 未跟踪线程成功重执行原进程，反例成立 |
| 实际 Go 线程和子进程 | 24 个同时保留 OS thread 的工作 goroutine 与 10 次 process 执行完成 |

共 **10 个场景**符合预期；包含两个故意移除约束的反例，不能把“10 PASS”
解释为十个生产安全属性全部闭环。观察者测试 FD 增量为零。Go 场景保存了单次
elapsed time，只是兼容性执行时间，不是吞吐、停止上界或长期 overhead 证明。

## 实际 Runtime CLI 装配

新增 `TestJournalServerExecObservedRemoteCLI`，使用同源编译的 `/exec-observer`
直接创建实际 `serve-remote --bootstrap-file`。配置来自 Node publication，
Runtime 首次 journal RPC 的原始 socket pidfd 被保留，确认 UID/GID 10001、
私有 namespace PID 1，再装配只读 journal endpoint；没有先执行另一个
Runtime helper 后再把 exec 例外当成保护。

| 场景 | 通过标准与实际结果 |
| --- | --- |
| permit | schema 2 的消费摘要/路径匹配 publication；明确 fixture Permit 前无 backend；之后真实 backend initialize，实际 Worker 发现 epoch 2，SIGTERM 后 shutdown；原 CLI 与已保留 backend pidfd 均退出 |
| deny | 明确 fixture 拒绝后退出，backend 事件文件从未创建 |
| observer-lost-before-permit | observer SIGKILL 后原 namespace owner 的保留 pidfd 报告退出；原 caller 不能再 Reply，backend 未启动 |
| observer-lost-after-permit | backend initialize 后 observer SIGKILL；原 Runtime 与 backend 的保留 pidfd 均报告退出；仅有 initialize 事件，不伪造 graceful shutdown |

Backend pidfd 从这个隔离测试环境中的实际 `/runtime-command.test` 子进程取得，
随后直接查询保留句柄的退出事件。这个测试 helper 的 `/proc` 枚举不是产品
级后代 inventory/custody API，也没有将 PID 枚举结果用于生产授权。

Node journal 的最高执行序号和原 incarnation 保持不变，server 全部 join。
CLI runner 共 **12 个主测试**通过，新增的四个场景均执行，无 skip 或 Go race。
C observer 不受 Go race detector 覆盖，使用编译器 `-Wall -Wextra -Werror`。

独立回归镜像仅移除 EXITKILL。生成回归镜像之前，重新编译启用保护的 observer，
与正常 CLI 镜像里的 observer 二进制逐字节比较相同，再覆盖为关闭保护的版本。
结果：正常 permit 仍通过，两个 observer-lost 场景均因原 namespace owner
在观察预算内仍活而失败，测试整体退出码 1。由此排除了 fixture cleanup 或
另一条停止路径让故障场景假通过。

## 源码依据与证据边界

Upstream stable `v6.10.14`，commit
`47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4`：

- [kernel/fork.c](https://github.com/gregkh/linux/blob/47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4/kernel/fork.c#L2777)
  根据 `CLONE_UNTRACED` 和退出信号选择是否报告 ptrace fork/clone/vfork 事件。
- [kernel/ptrace.c](https://github.com/gregkh/linux/blob/47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4/kernel/ptrace.c#L594)
  在 `exit_ptrace` 中对带 PT_EXITKILL 的 tracee 发送 SIGKILL。

这些源码解释与原生结果一致；未验证 LinuxKit 完整构建源码等同于 upstream。
同样，syscall observer 的 exec 转换约束不等于加载内存/动态库完整性、配置
不可变或此前已取得的内存句柄全部失效。代码信任、UID/user namespace/mount
隔离和首次保护生效前的访问窗口仍必须放在生产创建链中验证。

**不能直接把这个实验程序作为生产 Node 入口或 grant issuer：**

- 它的命令行来自 root 测试代码，尚未校验 signed plan、镜像、实际 CRI/task
  与 supervisor 的对应关系。生产 containerd 的 task PID 关系也未改造；
  不能用这里的 CRI fixture 覆盖现有独立 task/executable 检查。
- Node 尚未通过可信 IPC 取得 observer 创建关系、原始 pidfd、策略和存活状态。
  不能用 observer 输出的 PID/字符串或晚附加 ptrace 重建这条关系。
- SIGKILL/退出已经覆盖，observer 挂起、被 SIGSTOP、长期阻塞和外部监督
  deadline 尚未覆盖；EXITKILL 本身不处理“仍活但不响应”。
- 同次真实 Fleet/TLS/PostgreSQL、一次性 grant、grant 后角色写路由仍未接上。
  此处 Permit 明确为 fixture，journal 在整个实验中保持只读。
- 已证明的是这个 CPU backend 进程的退出，不是任意后代 inventory、设备
  quiescence、多成员停止上界、完整四 Stage Job 或持续资源运行。

后续生产接入应先确定可信创建/observer 交付关系，并验证挂起与丢失的停止策略，
再把执行约束与一次性 grant、写路由绑定到同一原始进程。仍不能宣布第 1 项
生产启动装配或整个 Vela 正确性验收完成。

## 复现

```bash
bash hack/run-runtime-exec-observer-experiment.sh

VELA_REMOTE_CLI_EVIDENCE=/tmp/vela-observed-cli \
  bash hack/run-remote-runtime-cli-native.sh

bash hack/experiments/runtime-exec-observer/check-cli-exitkill.sh \
  /tmp/vela-observed-cli /tmp/vela-observed-cli-control
```

第三条 runner 应成功，但其保存的 Go 测试退出码必须为 1，且两个故障场景
失败、正常 permit 通过。原生执行需要 Linux Docker 及所列 sandbox 权限。

额外检查：`go test ./...`、Linux arm64 的 nodeagent vet/lint 通过；shell
和 Python 语法、证据摘要与链接另行检查。全库本机 Go 测试不代替原生 Linux
场景；本轮也没有解释历史 `11ce026` STALE 失败。

- [机器可读索引](runtime-exec-observer-evidence-2026-09-09.json)
- [原型原始结果](evidence/runtime-exec-observer-2026-09-09/prototype/results.json)
- [实际 CLI 原始日志](evidence/runtime-exec-observer-2026-09-09/cli/native.log)
- [关闭 EXITKILL 的实际 CLI 回归](evidence/runtime-exec-observer-2026-09-09/regression/native.log)
- [实验 observer 源码](../hack/experiments/runtime-exec-observer/observer.c)
- [实际 CLI 测试](../internal/nodeagent/journal_exec_observed_cli_linux_test.go)
