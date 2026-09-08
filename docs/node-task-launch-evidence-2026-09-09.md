# Node 的 containerd task bundle 配置来源

基线：`b553ddb`。本轮为启动许可补充可读取的实际 task 配置来源，并恢复
可复现的 containerd CPU 实验环境。配置是否获准、实际 executable/config
文件及挂载是否符合批准内容、一次性 grant 仍是后续步骤。

## 来源与信任条件

固定 containerd `v2.3.1 / 64b425cf570b3b8dd1d4cc46da7c1fce65c6651a` 的
[NewBundle](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/core/runtime/v2/bundle.go)
在 task 创建时写入 `config.json`，rootful bundle 目录为 `0700`。
[Linux bundle 权限逻辑](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/core/runtime/v2/bundle_linux.go)
对 remapped root GID 使用不同权限；本轮明确不支持该分支。
[runc-v2 创建路径](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/cmd/containerd-shim-runc-v2/runc/container.go)
把该 bundle 交给 runc，同时还另行保存 runtime options。这些 options、
hooks 和运行后的挂载状态不能仅由本轮 `config.json` 摘要替代。

`ObserveTaskLaunch` 的 `stateDirectory` 必须由可信 Node 配置给出，指向
**同一个 daemon 在 Node 挂载视图中的 state root**。它不是 workload 请求
字段。本轮不自动推断 daemon 配置、不穿越另一 daemon 的 mount namespace，
也不以目录名称相同证明这项部署配置正确。管理员、daemon 与 host kernel
仍属于信任边界；对 root 管理员改写历史没有连续性或防回滚保证。

读取路径固定为：

```text
<stateDirectory>/io.containerd.runtime.v2.task/k8s.io/<CRI container ID>/config.json
```

方法先通过既有认证连接匹配 CRI、native task、原始 caller pidfd 与嵌套
PID namespace 的 PID 1，再读取 bundle 及 `init.pid`，核对原始 host PID。
完整路径的目录祖先不得由非 root 写入；bundle 必须 root:root `0700`。
文件必须是 root:root、单链接、无 group/other 写权限的有界普通文件。
先用 `O_PATH` 检查类型，再通过固定 procfs FD 读取，避免验证时打开设备或
等待 FIFO。拒绝符号链接、缺失、超限、重复/未知 JSON 字段和不完整配置。

两次 CRI/task/process 关联及两次文件检查保留一致的配置内容、PID、inode、
权限及修改时间；返回前再次检查原始 caller。其作用是拒绝可见变化；配置
来源的可信性依赖上述独立 root 写入边界，不来自“重复读到相同内容”。
`Configuration()` 每次返回独立副本，保留字节不会被调用者修改。

## 真实运行结果

`TestRuntimeCallerContainerCRI` 通过私有 containerd daemon、native
snapshotter 和真实 CRI 创建两种有效 caller：UID/GID 65532 的 direct init，
以及使用 Registry/Pod fixture 计划的 UID/GID 10001 init。CRI、runc task、
PID namespace、bundle 文件与内核权限均为实际对象；进程仍为测试 helper。

两个有效 caller 分别验证：

- 取得的 OCI argv、UID、字节数与 SHA256 匹配实际 bundle 文件。
- 修改 `Containers.Get.spec` 后，metadata 明确返回新值，bundle 配置仍为
  原始 `/probe`；副本修改不能改变读取器保留的数据。
- 知道 bundle 完整路径、并处于 Node 侧挂载视图的独立非 root 进程，仍无法
  以写方式打开配置或 rename 替换配置。
- 错误 state root，以及 writable、wrong-owner、missing、oversize、symlink、
  hardlink、FIFO、duplicate-key、unknown-key、wrong-init-pid 共十种文件反例
  均无有效返回。
- 原始 caller 退出后不再取得 live launch observation。

原有 wrapper/shared-PID 拒绝、精确 pidfd 退出、CRI 禁止同 ID 重新启动及
native API 同 container ID task 替换反例继续通过。

| 检查 | 结果 |
| --- | --- |
| 真实 CRI 扩展实验，static race binary | PASS，13.24 s；两组各十个文件反例及生命周期检查 |
| 真实 containerd 原生进程反例，static race binary | PASS，6.44 s |
| 以上 native 运行 | 无 skip/race，进程退出 0 |
| 普通 repository tests / vet / lint | PASS；lint 0 issues |
| Linux Node integration-tag lint / vet | PASS；lint 0 issues |
| Linux amd64 integration-tag compile-only | PASS，不计作执行证据 |

## 重建与证据

[脚本](../hack/run-task-launch-native.sh) 将以下固定内容构建为新的本地镜像：

- containerd 2.3.1 arm64 官方 archive，SHA256
  `46a83603a850f3916ca7c942310daaf82ed17773a85b1d431e92d4a541e46d0d`。
- runc 1.4.2 arm64 官方 release asset 387432606，SHA256
  `ea54032310588e115633aa2f4bba8bf9500257f657e1deca88df5778775138db`。
- 固定 Go 1.26.7 Linux 镜像和本仓库的 static race test binary。

runc 的普通发布下载 URL 在本轮发生 HTTP2 framing error/连接超时；官方
GitHub API 入口下载成功，字节摘要与官方 release metadata 一致。没有更换
版本或跳过校验。旧记录中的本地镜像 ID 已不存在，现可通过以下命令重建：

```sh
bash hack/run-task-launch-native.sh
```

脚本目前限定 Linux arm64 Docker host。compiler 容器只读挂载源码/modules；
实际测试容器无 host bind/socket、无网络、无 host PID namespace。外层
privileged 仅用于嵌套 runc namespace/cgroup，未访问模型权重或运行 GPU。
实际镜像、binary/source 摘要与原始日志见[receipt](node-task-launch-evidence-2026-09-09.json)。

## 尚未证明

此读取器不批准 spec、不证明所有有效挂载及 runtime options、不认证已加载
内存，不发放 startup grant，也没有接到生产 Node/Runtime CLI。可信 Node
配置中的 daemon/state root 对应关系仍需部署装配验证；其他 runtime/userns
组合未支持。后续必须把原始 caller、该来源、批准的 image/executable 与
配置、实际隔离及 Fleet reservation 接到同一授权事务。

本轮未运行 PostgreSQL 或完整 Job。Worker 输入/materialization 独立保管、
完整 remote-owner CPU Job、替换、停止时限及历史回收仍未闭环；数据库与
journal schema 未修改，Production Gates 保持 **0/9**。
