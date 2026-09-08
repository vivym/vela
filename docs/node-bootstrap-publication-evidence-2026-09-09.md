# Node 可信 Runtime 启动配置发布

日期：2026-09-09。基线：`d9687d5`。范围：本地 CPU/mock、Linux/arm64
原生进程与真实 Unix RPC；没有部署、GPU 或 Production Gate 放行。

## 实现与信任边界

[PublishRuntimeBootstrap](../internal/nodeagent/runtime_bootstrap_publication_linux.go)
从 verified `RuntimeLaunchPlan` 推导 manifest、Registry binding 和 Runtime GID，
从当前持锁 `ExecutionJournalOwner` 推导 journal/storage identity、unresolved
incarnation 及实际 StageAuthority 公钥。调用者只提供可信部署中的路径、时限
和 Registry 公钥，不能另外指定 manifest、incarnation 或 StageAuthority keyring。
返回数据是独立副本，不改变原始 plan、journal 或 verifier。

发布需要预先存在的 root:root `0700` 空目录，所有父目录必须 root-owned、
不可由 group/other 写入且不能包含符号链接；与真实 CLI 一致，拒绝 `/tmp`
这类 sticky 可写祖先。操作顺序为：

1. `O_EXCL` 创建并锁住 root:root `0600` 的 `publication.json`。
2. 写入 `bootstrap.pending`，设置 root:planned-GID `0440` 并 fsync。
3. 私有记录写入目录、记录文件及 bootstrap 的 device/inode、大小、内容摘要、
   binding 摘要和 node identity；fsync 记录及目录。
4. 核对实际 held journal 后以 `RENAME_NOREPLACE` 改名为 `bootstrap.json`；
   仍在 `0700` 状态检查完整内容、文件身份、路径关联，并再次核对 journal。
5. 将目录改为 root:planned-GID `0750` 并 fsync，使 Runtime 可以读取；
   重新检查文件和 journal 后返回历史观察。

失败保留现场，不提供覆盖、补写或清理后重试路径。`InspectRuntimeBootstrapPublication`
只接受未被活跃发布者锁住的完整记录，拒绝部分文件、内容/身份/权限变化。
完整记录在 Node 退出或回包丢失后仍能检查，但不恢复原始 pidfd，不创建 journal，
也不提供 Permit。创建一次的约束是**每个目录**，并不禁止同一 incarnation
在另一个新目录发布，因此不能替代后续一次性启动授权。

root 管理员、Node 部署输入和内核仍受信任。记录是 root 私有的本地历史，
不是抵御恶意 root 重写全部状态的签名证明。`0440` 是文件权限，不是只读挂载。
发布前后检查只能证明检查时刻，实际启动仍必须独立核对有效 mount/writer、
executable、原始 caller、journal 与 Fleet 状态。

## 验证

[原生 runner](../hack/run-remote-runtime-cli-native.sh) 在无网络、无 GPU、无 host
mount 的 scratch 容器中执行 race binaries。root Node 保有 journal 和原始
进程句柄；Runtime/Worker 使用 verified plan 的实际 UID/GID **10001**。

- 发布正例：从完整规范 bundle 重新生成并验证 plan；不可修改私有 plan 字段
  伪造匹配。实际非 root 子进程能用 CLI 的读取器解析配置，不能读私有记录、
  写入任何发布文件、创建文件或删除 bootstrap。
- 11 个 preflight 反例：nil/closed/wrong owner-plan、错误 Registry key、socket
  重合、不安全目录、额外文件、sticky 祖先、取消；不会创建发布 intent。
- 8 个并发发布者恰好 1 个成功；活动锁阻止历史检查，后续不能覆盖重试。
- 6 个错误返回边界及 6 个由外层 parent 实际 SIGKILL 的持久化/暴露边界：
  暴露前只留下不完整现场，暴露后可以检查完整历史；所有已有现场拒绝再次发布。
  SIGKILL 证明进程退出恢复，不证明整机掉电、磁盘丢写或生产子进程 containment。
- 14 个已发布产物变化反例：内容、同内容新 inode、hardlink、symlink、FIFO、
  owner/mode、私有记录缺失/替换、目录替换、额外文件均拒绝。
- journal custody 在 intent-sync、rename、exposure 后丢失均不返回成功；
  pending 内容或目录/私有记录在发布期间替换，不会把无效配置开放给 Runtime。
- 真实发布产物 → `serve-remote --bootstrap-file` → 实际 Node journal RPC →
  显式 **fixture Permit** → process backend initialize 一次 → 实际 Worker 发现
  epoch=2 → SIGTERM → shutdown 一次、exit=0。没有本地 epoch/journal 文件，
  Node journal 原始 incarnation 与 Highest=0 不变，server joined、InFlight=0。

保留此前 18 个实际 remote CLI 场景和已有 Worker execution/barrier 回归。
普通测试还验证从 held journal 返回的 public keyring 副本不能改变其 verifier，
cancel/closed owner 不能返回发布公钥。全库 test/vet/lint、Linux 相关包
lint/vet 与 amd64 交叉编译的命令、原始日志、固定源代码/镜像/binary 摘要见
[机器证据清单](node-bootstrap-publication-evidence-2026-09-09.json)。

## 发现并修正的问题

首轮原始日志保留两项失败：

- 发布接口允许另传合法但不同的 StageAuthority 公钥。空 journal 的 scope
  验证不能证明 key bytes 与 owner 一致；现改为从 owner 实际 verifier 提取
  公钥，删除重复配置入口，没有放宽反例预期。
- 新 plan 使用 UID 10001，旧 Worker helper 固定期望 65532，真实 socket 校验
  正确拒绝。改为与实际测试进程身份一致，没有放宽产品 socket 权限检查。

进一步加入 rename 后丢失 custody 的反例，复现了文件系统检查完成前失去
journal 仍可能暴露配置的窗口；补上最终暴露前的 held journal 核对。原始
失败与修复后结果一并保留，不能将后者替代前者。

## 尚未完成

这个 CLI 调用的 Registry/CRI、Node Permit 仍为 fixtures；生产 Node daemon、
Fleet mounts/config 接线、镜像与有效 env/mount/writer 检查、单次 reservation
到一次性 grant 的关联还没有合并到同一次操作。默认 Fleet 启动路径仍未切换。

没有同一装配下的四 Stage Job、Worker input/transfer/materialization 独立
journal、真实后台执行/后代隔离和多成员故障 campaign，也没有历史安全回收与
持续资源趋势结论。下一主线仍按
[剩余验证顺序](remaining-validation-2026-09-09.md) 完成真实启动授权及完整 Job。
Production Gates 保持 **0/9**。
