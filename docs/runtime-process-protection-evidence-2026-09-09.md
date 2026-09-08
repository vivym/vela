# Runtime 同 UID 进程内存保护

基线：`0f02f2e`。范围：Linux Runtime 服务入口的进程内存边界，CPU-only。

## 问题与修复

`ProcessBackend` 通过 `exec.Cmd` 启动 backend，没有切换到独立 UID。Node
私有 journal 虽然限制了文件写入，但不会自动限制相同 UID 的 backend 访问
Runtime 进程内存。是否能访问还取决于 dumpability、Yama/LSM、capability 等
内核策略，不能将某个环境中默认拒绝 ptrace 当成 Runtime 自身的保证。

实际 `vela-model-runtime` 服务入口 `run` 现在先检查 context，然后执行
`PR_SET_DUMPABLE=0` 并通过 `PR_GET_DUMPABLE` 确认，才进入配置读取、epoch
装配及 backend factory 路径。设置或确认失败均直接返回错误。该属性作用于
共享 mm 的整个进程，覆盖 Go 线程；重复安装保持相同结果。

此修改也会关闭 Linux Runtime core dumps，非特权 debugger 无法再 attach。
Node/host 的特权进程观察需要在目标 user namespace 中具备 `CAP_SYS_PTRACE`。
本轮不新增 Node 权限或改变部署清单。非 Linux 开发入口保持既有行为，不声称
具备这条 Linux 保护。offline journal 子命令没有变成服务或启动授权入口。

## 实测

使用 pinned Go builder 生成静态 race 二进制，在 scratch 容器中以
UID/GID 10001、所有 capabilities 删除、无网络/host mounts 运行。为确保
baseline 的 ptrace 测试能到达内核，测试容器使用 `seccomp=unconfined`。
Yama 可用时仅在 victim fixture 内设置 `PR_SET_PTRACER_ANY`；内核没有该
选项时接受 `EINVAL`，但仍严格要求下面的 baseline 实际访问成功。

| 阶段 | 相同 UID exec 子进程 O_RDWR 打开父 `/proc/pid/mem` | ptrace attach | 父 dumpable |
| --- | --- | --- | --- |
| 明确设置 baseline | 成功 | 成功并 detach | 1 |
| 经过实际 `run` 入口 | EACCES/EPERM | EPERM | 0 |
| 重复调用并再次 exec 子进程 | EACCES/EPERM | EPERM | 0 |

子进程按正常 exec 运行，证明其 exec 不会重置父 Runtime 的保护；没有向父
内存写入任意数据，也没有将模拟读取成功升级为实际内存破坏证据。ptrace
attach/wait/detach 固定在同一个 OS thread 上。

`deny-set`、`deny-get` 两个独立 helper 在固定 thread 安装测试专用 seccomp
filter，分别让设置或确认返回 `EPERM`；实际 `run` 必须传播该内核错误，不能
继续到“缺少配置”等后续错误。这个 filter 不进入产品代码。

native race 的三个主测试均通过，包括现有 Runtime 服务启动/关闭回归。
服务回归仍使用已有测试配置/authorizer，不能作为 Node production grant
已装配的证据。repository tests、vet、普通/Linux lint、相关 host race 与
Linux compile-only 同步留证；host race 不覆盖 Linux 专属系统调用。

运行脚本：`bash hack/run-runtime-process-protection-native.sh`。脚本要求每个
选中主测试确实 PASS，拒绝 skip/race，并保存源码补丁、image ID、binary hash
和原始日志。精确结果见[receipt](runtime-process-protection-evidence-2026-09-09.json)。

## 不能据此放行的事项

- 这是可信 Runtime 入口主动施加的保护；恶意 Runtime 自己可以重新设置
  dumpability。它不认证 Runtime executable、OCI effective config 或已加载内存。
- Runtime 自己 exec 或改变某些凭据可重置该属性；当前常驻服务入口没有以
  这种方式替换自身，未来 launcher/恢复协议必须明确处理这种转换。
- 不阻止同 UID 信号、共享可写文件修改、继承 descriptor、后代逃逸或设备写入。
  Worker 状态保管与 backend/resolver 的独立 UID/mount/cgroup 边界仍需完成。
- `CAP_SYS_PTRACE`、host root/kernel compromise 不在本条非特权保护保证内。

下一步仍是建立有效 launch provenance 与独立批准，在同一次原始进程调用中
完成一次性 Node grant，再接入完整 remote-owner CPU Job。Production Gates
保持 **0/9**。
