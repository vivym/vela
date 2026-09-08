# Runtime 执行连续性：旧内存句柄方案的否证

基线：`31a8cdb`，2026-09-09。范围为隔离 Linux/arm64 CPU 实验，未修改
Vela 生产启动或 journal 授权行为。**保留 `/proc/PID/mem` 并检查其可读性，
不能证明原进程未发生 `exec`，因此不将这个候选机制接入 grant。**

## 为什么检查这个候选方案

现有 `RuntimeCaller` 保留原始 pidfd，`InspectExecutable` 检查 executable
文件，实际 CLI 预留还采样 argv/env。这些检查都没有承诺执行连续性。
pidfd 可以跨 `exec` 存活；同一进程 A→B→A 后，采样值可能恢复。
候选方案是在 Node 发送 challenge **之前**保留进程内存 FD，并在接收请求、
观察和回包时检查旧地址空间是否仍存活。只在收到请求后取得句柄还会额外留下
请求发送与句柄取得之间的空隙。

本实验先检查该候选的必要条件，没有装配 Unix challenge、CRI、Fleet、
Vela `RuntimeCaller` 或实际 CLI。因此它否定的是内核机制足以充当 authority
这一假设，不是已完成的 Vela 授权漏洞利用，也不是完整启动链验证。

## 原生结果

执行环境为 Docker Desktop 私有 PID namespace，内核 `6.10.14-linuxkit`、
`aarch64`。测试无宿主路径挂载、网络或 GPU，限制 1 CPU、256 MiB、32 PID。
观察者是带 `CAP_SYS_PTRACE` 的 root；目标 UID/GID 均为 `65534`，有效
capabilities 为零，设置 `PR_SET_DUMPABLE=0` 和 `PR_SET_NO_NEW_PRIVS=1`。
这里未启用 seccomp syscall 过滤，结果不代表任意生产 seccomp profile 都
允许 `CLONE_VM`。

两个独立 executable A/B 由固定 builder 编译。每次 exec 的 argv 和 environ
逐字节保持相同；观察者独立从 procfs 检查其摘要、启动 ticks、UID/GID、线程
数和 executable inode。读取地址来自保留 proc 目录的 `maps`，不是目标声明。
旧 FD 在任何 exec 命令之前打开，并读取已知 ELF 头 `7f454c46` 作正对照。

| 场景 | 旧 mem FD 的实际行为 | 含义 |
| --- | --- | --- |
| 不 exec | 原目标活着时可读，退出后 EOF | 可读性正对照 |
| 同 executable 重新 exec | 原 pidfd 仍活，旧 FD 返回 EOF | 普通 exec 可被发现 |
| A→B→A | 原 pidfd 仍活，旧 FD 返回 EOF | 无旧 mm 使用者时 ABA 可被发现 |
| 普通 fork 后 exec | 子进程仍活，但旧 FD 返回 EOF | fork 复制 mm，不保留父进程原 mm |
| `CLONE_VM` 后同 executable exec | 原 pidfd 仍活，旧 FD 仍读到原 ELF 头 | 产生假阳性 |
| `CLONE_VM` 后 A→B→A | 两次 exec 后都可读，最终采样身份完全恢复 | 原 pidfd、相同最终身份与可读旧 mm 联合仍不足 |
| `CLONE_VM` 后原进程退出 | 原 pidfd 已报告退出，旧 FD 仍可读 | 旧 FD 的存活对象不是原进程 |

共享 mm holder 使用 `CLONE_VM | SIGCHLD`，没有 `CLONE_THREAD`，因此原目标
在所有观察点的 `Threads` 都是 **1**。检查单线程不能排除这个反例。
holder 不读取 Vela 配置、不取得额外权限，也不需要写原目标代码；只阻塞并
保留 mm。杀死 holder 并等待退出后，已经 exec/退出的原目标的旧 FD 才返回 EOF。
7 个场景全部满足上述预期，观察者 FD 增量为零。

移除观察者 `SYS_PTRACE` 的独立对照以 `PermissionError`、退出码 1 拒绝
读取非 dumpable 目标的 procfs executable。这说明该候选还会引入额外检查权限；
不能不加区分地使所有通用 `RuntimeCaller` 接收路径依赖 ptrace 权限。

## Linux 源码交叉核对

对应 upstream stable `v6.10.14` 的 commit：
`47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4`。

- [fs/proc/base.c](https://github.com/gregkh/linux/blob/47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4/fs/proc/base.c#L832)：
  `proc_mem_open` 对 mm 使用 `mmgrab` 后 `mmput`，保留结构而不固定用户内存。
  `__mem_open` 将这个 mm 存入文件的 `private_data`；`mem_rw` 使用
  `mmget_not_zero(mm)` 决定是否仍可访问旧 mm，并不要求原 task 当前 mm 与其相同。
- [kernel/fork.c](https://github.com/gregkh/linux/blob/47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4/kernel/fork.c#L1710)：
  `copy_mm` 在 `CLONE_VM` 分支对旧 mm 使用 `mmget`；普通 fork 走 `dup_mm`。

以上是同版本 upstream 源码解释，未证明 Docker LinuxKit 的完整构建源码与
upstream 字节相同。运行行为以保存的原生结果为准，源码来源与摘要另行保存。

## 对后续实现的约束

当前决策是放弃“旧 mem 可读 ⇒ 未 exec”的授权推导，也不增加无效的通用
`RuntimeCaller` mem FD 和 ptrace 权限要求。原有的只读启动 journal 继续有效，
预留回执不升级为初始化许可。

下一增量应选择能对**原进程所有线程的 exec 转换**建立内核约束的机制，或补齐
独立可信的启动来源与受信 Runtime 代码不变量。两条路线都尚未验收，不能仅凭
下面列出的机制名称赋予启动权限：

| 候选 | 必须解决的问题 |
| --- | --- |
| 从可信创建边界持续跟踪 exec 事件 | 原始 pidfd 与 tracer 绑定；challenge 前开始覆盖；既有和新增线程、非 leader exec、fork/clone/vfork；事件未处理时阻止新映像使用旧许可；Node/tracer 退出与重启不能恢复旧写权限 |
| 内核强制限制后续 exec | 覆盖 execve/execveat 与实际 syscall ABI；全部线程及继承关系；Node 独立验证真实生效的规则，不能接受 caller 声明或仅看 Seccomp 状态；明确对 process backend 启动的影响 |
| 可信 launcher 与受信 Runtime 的完整来源链 | 从实际创建到请求消费期间，task 配置、入口、加载内容与受保护 Runtime 的对应关系成立；不能把当前 executable 文件测量提升为历史执行证明；明确哪些代码和内核组件受信 |

最小新增反例清单：challenge 前后切换、请求发送后切换、Fleet 持久化前后切换、
回包前后切换、同文件重执行、A→B→A、非 leader exec、新线程竞态、共享 mm
的独立进程、原进程退出与替换、观察权限缺失、观察者退出/句柄丢失。
需证明失败后没有可用写通道和重复 grant，而不仅是最终检测到异常。

执行连续性只覆盖 exec 转换；即使实现也不自动证明加载内存完整性、动态加载、
进程内配置不可变、全部 backend 后代停止或完整 Job 成功。Worker 独立业务
journal、同装配四 Stage Job 和长期资源验证仍按剩余清单推进。
Production Gates 保持 **0/9**。

## 复现与证据

```bash
VELA_MM_WITNESS_EVIDENCE=/tmp/vela-mm-witness-new \
  bash hack/run-runtime-mm-witness-experiment.sh
```

正常 runner 必须成功，含义是七个内核预期（包括反例）成立；不是执行连续性
授权通过。需要 Linux Docker、可用的 pinned builder 及 ptrace capability。
结果不得用来声称原生 amd64 已验证，本次仅执行 arm64。

- [实验代码](../hack/experiments/runtime-mm-witness/check.py)
- [实际 probe](../hack/experiments/runtime-mm-witness/probe.c)
- [原始结果](evidence/runtime-execution-continuity-2026-09-09/results.json)
- [缺少 ptrace 权限对照](evidence/runtime-execution-continuity-2026-09-09/no-ptrace.log)
- [机器可读索引](runtime-execution-continuity-evidence-2026-09-09.json)

仅新增实验和文档。C 编译启用 `-Wall -Wextra -Werror`；runner 语法和 Python
语法检查通过。没有重跑不受这次修改影响的 Vela Go/CRI 历史 campaign，上一轮
`31a8cdb` 的组件回归证据仍保留原来的范围。
