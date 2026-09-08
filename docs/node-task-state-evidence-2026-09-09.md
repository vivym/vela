# Node task state 与原始 containerd daemon 关联

日期：2026-09-09。基线：`344e0f2`。范围：本地 Linux/arm64 CPU 原生隔离验证，
无 GPU、部署或 Production Gate 放行。

## 问题与改动

上一轮的 `ObserveTaskLaunch` 从 Node 配置的 root 私有目录读取 task bundle，
但未证明这个目录属于当前连接的 daemon。复制一份完整的 `config.json` 和
`init.pid`，保留 root ownership 与可信权限，仍能与原始活进程匹配。
这个目录映射是当时明确保留的部署前提，尚未由代码核验。

现在 `DialRuntimeContainerObserver` 在原有 socket inode/UID 检查之外，通过
Linux `SO_PEERPIDFD` 保留原始 root:root socket peer 与 procfs 目录。
重连必须匹配同一个活进程；关闭或原 daemon 退出不能重新绑定另一个进程。
读取启动配置时：

1. 从同一条认证连接的 CRI `Status(Verbose=true)` 取得确切的 `stateDir`。
   containerd v2.3.1 的 `plugins/cri/runtime/plugin.go` 将它设置为 daemon state
   下的 `io.containerd.grpc.v1.cri`；仅支持该已验证布局。
2. 从原始 peer 的 procfs `root` 进入 daemon 文件系统视图，不重找 numeric PID。
   逐级检查 root:root、无符号链接、无 group/other 写权限。
3. Node 配置路径必须指向 daemon state 的同一 device/inode。合法 bind-mount
   别名可用；内容相同但 inode 不同的目录不能替代它。
4. 在 daemon 视图中打开实际 task bundle，并在读取结束后重查配置路径、目录
   inode、原始进程、CRI/task 与文件内容。Node 别名下面的挂载不能重定向读取。

相关代码：
[daemon peer](../internal/nodeagent/runtime_container_daemon_linux.go)、
[state 关联](../internal/nodeagent/runtime_task_state_linux.go)、
[bundle 读取](../internal/nodeagent/runtime_task_launch_linux.go)。

## 验证与反例

- **基线反例**：Go overlay 恢复 `344e0f2` 的完整 `runtime_task_launch_linux.go`；
  测试仅移除旧结构不存在的 `DaemonStatePath` 展示字段检查。两种实际 CRI caller
  均在 `copied-state-root` 失败：root 私有副本被旧实现接受。测试进程 exit 1。
- **最终原生运行**：containerd v2.3.1 / runc v1.4.2；四个主测试通过，无 skip/race。
  实际 CRI 主测试 15.98s，native containerd 主测试 6.43s。
  两个 caller 各验证复制目录拒绝、独立 Node mount namespace 内的合法别名与
  shadow mount、以及此前十种错误 bundle 文件，原有 wrapper/shared-PID 与
  caller 退出反例继续通过。
- **原始 daemon 生命周期**：同 daemon 的新连接可认证；另一台实际 root daemon
  被拒绝且不损坏原观察器。明确 SIGKILL 原 daemon 后，检查、目录读取与替换认证
  均失败；关闭后不能重新取得句柄。
- **资源与解析**：并发 authenticate/check/open/close 八轮后 FD 数量与开始一致，
  race 开启。17 种不完整、重复、非规范或超大 Status 配置被拒绝。
- **公开 observer 回归**：四个 socket/peer 主测试通过，无 skip/race。
- `go test ./...`、`go vet ./...`、golangci-lint v2.13.1、Linux integration Node
  lint/vet 通过；Linux/amd64 仅交叉编译通过，不是 amd64 运行证据。

最终原生测试通过 [runner](../hack/run-task-launch-native.sh) 重现。编译容器以只读
挂载访问源码与模块缓存；执行测试的容器无宿主挂载/socket、无网络、无 host PID
namespace，仅在其私有 cgroup namespace 内运行嵌套 containerd/runc。
版本、镜像、二进制、源码 patch、overlay、检查日志及逐文件摘要见
[机器可读清单](node-task-state-evidence-2026-09-09.json)。

首次生命周期测试留下裸 Unix peer 连接，尝试 SIGTERM 等待超过 fixture 的 5s
关闭窗口，随后 fixture 强制结束进程，测试失败；日志已保留。最终关闭裸连接，
以明确 SIGKILL 验证崩溃后的 identity loss。这里不声称 daemon 优雅关闭时限已验证。
最初 PATH 的 golangci-lint 用 Go 1.25 构建，无法分析项目 Go 1.26.7；最终使用
同版本 Go 构建的 v2.13.1，通过检查。首次 Linux lint 的错误文本大小写诊断已修正。

## 仍未证明的边界

root 管理员、内核和 daemon 仍在信任边界内；socket activation、代理与把监听
socket 委托给其他进程的部署不在支持范围。SO_PEERPIDFD 是必要条件，无 PID
数字回退。本次不批准 daemon/shim/runc 的 executable，也不对 root 的临时改写、
exec ABA、挂载变化历史或已加载内存作持续证明。

`RuntimeTaskLaunch` 仍只是配置来源。runtime options/hooks、工作负载有效挂载与
writer 隔离、批准的 image/executable/config、一次性 grant、真实 CLI/Fleet 装配、
Worker 业务 journal 与完整四 Stage protected CPU Job 都还需继续完成。
Production Gates 保持 **0/9**。
