# 原始 Runtime 调用者视图与 Node 发布配置关联

日期：2026-09-09。基线：`1e985f9`。范围：本地 Linux/arm64 CPU/mock、真实
containerd 2.3.1 / runc 1.4.2 / CRI；没有部署、GPU 或 Production Gate 放行。

## 补齐的关联

此前 Node 可以发布可信 bootstrap，也能检查原始 caller 的镜像入口并预留
Fleet epoch，但这两条路径没有证明 caller 文件系统中看到的配置属于那次发布。

[ReservePublishedImageRemote](../internal/nodeagent/runtime_startup_publication_linux.go)
将发布目录和预期容器内路径作为可信 Node 部署输入：

1. 重开 Node 私有 publication history，使用实际 verified plan、held journal、
   incarnation 和 owner 的实际 StageAuthority 公钥重新生成配置并逐字节匹配。
2. 从原始 `RuntimeCaller` 的固定 procfs 目录进入它的 `root` 视图。不会重新按
   调用者声明的 PID 或 Node 本地同名路径查文件；逐级拒绝符号链接及非 root/
   group-other 可写目录，先检查最终普通文件 inode，再固定该 inode 读取内容。
3. 检查 root:planned-GID `0440`、单 hardlink、device/inode、内容摘要和大小
   与 publication 记录完全一致；通过 `fstatfs` 验证**实际挂载只读**，通过
   `statx(STATX_MNT_ID)` 获取 mount ID。再次从原始 root 解析路径，核对文件、
   mount ID、mount namespace 和进程身份。
4. 文件系统读取后重查 Node publication 和 journal。原有同次镜像检查及
   单次 Fleet 调用前后都经过这条关联；失败保留已消耗 intent，不重试预留。
5. 将 publication 记录、容器路径、mount namespace 与 mount ID 写入 Node
   startup intent，纳入 Fleet `OwnerObservationDigest`。历史接口深拷贝该
   嵌套结构，调用方修改返回值不能改变 Node 内存关联或后续恢复结果。

这是对**采样时调用者视图中文件**的验证，不能证明进程之前读取了什么。
接口没有 Permit/Fresh 输出，也没有历史转 grant 路径。root 管理员和内核仍
受信任；实际 Node 是私有记录的可信写入者，记录不是抵御恶意 root 的签名证明。

## 实际验证

[runner](../hack/run-task-launch-native.sh) 新增 `VELA_TASK_LAUNCH_SCOPE=startup-publication`
以选择这一组实际 CRI 测试，并明确要求所有 12 个场景存在且通过；原有 full
模式保留。测试在无网络、无 host mount、无 GPU 的一次性特权沙箱中运行。

| 场景 | Fleet 次数 / intent 数 | 结果 |
| --- | --- | --- |
| 原始文件的实际只读挂载 | 1 / 1 | 预留成功，历史保留 publication/mount 关联 |
| 缺失、同内容复制文件、可写挂载、错误 plan、hardlink、错误 incarnation | 0 / 0 | 预留前拒绝 |
| intent 持久化后、Fleet 前替换发布文件 | 0 / 1 | 已消耗 intent，拒绝调用 Fleet |
| Fleet 返回后替换发布文件 | 1 / 1 | 不返回预留回执 |
| Fleet 已提交但回包丢失 fixture | 1 / 1 | 不返回回执，重开后不重试 |
| Fleet 前/后对同一原文件实际重新挂载 | 0 / 1、1 / 1 | 即使内容和 inode 相同，也因 mount 身份变化拒绝 |

重新挂载由独立 root helper 完成：从固定 FD 克隆并设为只读 mount，在一次性
OS 线程解除 `CLONE_FS` 共享后进入原始 caller mount namespace，再以固定
目标 FD 接入。它是故障注入，不是允许 Runtime 自己持有 mount 权限。测试
caller 仍使用无 capabilities、NoNewPrivs、独立 PID namespace 的实际 CRI 容器。

所有持久 intent 在重试或重开后均不会增加 Fleet 调用，也不会重建原始 pidfd。
正例还修改 `Inspect` 返回的 bootstrap 字段，确认内部关联和重开历史不变。
另保留 11 个原生 startup/ledger 回归，包含已有进程 SIGKILL 恢复、容量保留、
不确定 append 和回包丢失语义；这些不等于新增 mount 路径已做掉电测试。

对照实验通过 Go overlay 仅关闭 mount ID 的有效比较及嵌套记录深拷贝，重跑
正例与两个实际重新挂载反例。这三处应分别暴露返回值篡改和错误接受 mount
替换，而正式源码的相同场景通过。overlay 不修改工作区产品源码。

实测 overlay 三个场景均按预期失败：返回历史可以篡改内部关联；Fleet 前
重新挂载仍调用了 Fleet；Fleet 后重新挂载仍返回预留回执。它证明这两项
检查对这些具体反例有效，不扩大为完整启动许可的证明。

固定源代码、binary/镜像摘要、命令和完整原始日志见
[机器清单](node-startup-publication-evidence-2026-09-09.json)。首轮完整 native
campaign 和最终 focused campaign 分别保留，不把前一版本的 full 结果写成
最终版本所有历史 campaign 的重跑。全库 test/vet/lint、Linux integration-tag
lint/vet、amd64 交叉编译的范围也在机器清单中单列。

## 测试装配中修正的问题

初次编译误将 `os.Root` 当作有 `Fd()` 的文件，已改为从固定 root 打开 procfs
目录，与已有 executable observer 的处理一致。实际 mount helper 首次切换
namespace 因 Go 多线程共享 `CLONE_FS` 被内核拒绝；解除共享后，跨 namespace
路径又无法解析，因此最终改为固定源/目标 FD 和 OpenTree/MoveMount。两次
实际 helper 失败均保留日志，没有通过弱化产品 mount 校验使测试变绿。
overlay 构建曾把本地 image ID 直接用于 Dockerfile `FROM`，被当作远程镜像名
解析而失败；改为给该已有本地镜像加唯一标签后完成构建和上述实验。

## 尚未完成

Registry/Pod/Fleet 仍为 fixture，实际 CRI caller 是镜像中的测试 probe。
本轮不能与上一轮真实 CLI 初始化/关闭测试合写成同一装配下的完整启动。

仍须把实际 CLI 消费的 bootstrap、有效 argv/env、可执行代码连续性、一次性
grant、真实 Fleet/TLS/PostgreSQL 与生产 Node 入口接到同次启动。之后还需
Worker input/transfer/materialization 独立 journal、完整四 Stage CPU Job、
真实后代停止、多成员失败、历史安全回收和持续资源趋势验证。
Production Gates 保持 **0/9**；完整目标继续按
[剩余验证顺序](remaining-validation-2026-09-09.md) 推进。
