# 实际远程 CLI、CRI 与 Node 预留的同次启动验证

日期：2026-09-09。基线：`8b4b182`。本地 CPU/mock，Linux/arm64，真实
containerd 2.3.1 / runc 1.4.2。无部署、外部网络、GPU 或 Production Gate
放行，Production Gates 保持 **0/9**。

## 本轮闭合的路径

此前实际 CLI 配置消费与真实 CRI/image/mount 预留来自两种测试装配。
新增 `RuntimeStartupLedger.ReservePublishedRemoteCLI`，在同次调用中要求：

1. 原始 caller、schema 2 消费声明、verified plan、held journal 和发布文件
   通过已有核对，调用者视图中的发布文件处于实际只读 mount。
2. 从签名 fixture 绑定的单平台 image manifest 推导默认入口并测量其内容。
   image 默认 argv 必须为 `[absolute-entrypoint, serve-remote,
   --bootstrap-file, Node-expected-path]`，实际 task argv 与之逐项相同。
3. task 必须非 TTY、cwd 为 `/`，image cwd 为空或 `/`。环境严格要求
   `PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`、
   `HOME=/`、`HOSTNAME=<planned-Pod-name>`，每个恰好一次；image 环境只能
   声明相同的 PATH/HOME。未知变量、重复值、额外参数及相同含义的其他参数
   写法均不在这个入口的批准范围内。
4. 从原始 pidfd 对应的固定 procfs 目录读取 `cmdline` 与 `environ`，每份
   限制 64 KiB，连续两次与 task 中 NUL 分隔的精确字节比较，前后核对进程。
5. 将 argv/env 摘要写入私有 startup intent，纳入 Fleet owner observation
   摘要。Fleet 单次调用前后重新观察 image/task/procfs/journal/publication。
   history API 深拷贝新字段，恢复保持 schema 与摘要有效性且不会重建原 pidfd。

环境明文不会写入生产 ledger 或产品错误信息。测试失败时可以记录本测试自行
生成的 argv/env；这些日志不来源于用户或宿主进程环境。

`ReserveImageRemote` 和 `ReservePublishedImageRemote` 仍为此前的观察型
接口。它们不能提供上述严格 CLI 结论，也不能签发 grant。新接口返回值仍是
reservation receipt，没有 Permit/Fresh 授权输出，不能由历史恢复启动许可。

## 同次测试装配

runner 将实际 `vela-model-runtime` 和实际 CPU process backend 测试程序
构建为静态、带 race instrumentation 的 binary，打包成单平台 OCI image，
本地导入同一个真实 containerd native snapshotter。

Node 从实际 plan/journal 发布配置。真实 CRI 容器以 UID/GID 10001、无
capabilities、NoNewPrivs、独立 PID namespace 和只读 rootfs 启动 CLI。
挂载包含 Node 控制通道目录、原始发布目录与可写 scratch。首次真实 journal
RPC 在原始 caller 认证及 namespace owner 留存后，通过实际
`JournalEndpoint` / `JournalServer` 提供服务；随后的真实 startup 请求
进入上述严格预留入口。

正常场景保留一个 intent、调用一次 Fleet fixture、持久记录回执。在尚未
回复显式测试决策时，backend 事件文件必须不存在。随后**显式测试 Permit**
放行 CPU backend，实际观察 initialize；CRI StopContainer 后观察 shutdown。
这项 Permit 不是 reservation API 发放的生产授权。

| 场景 | Fleet / intent | 实测 |
| --- | --- | --- |
| 合法实际 CLI | 1 / 1 | 预留成功；返回 history 不能篡改内部 CLI 摘要；显式测试 Permit 后实际初始化/关闭 |
| 额外环境变量、错误 PATH、错误 HOSTNAME、重复 bootstrap 参数 | 0 / 0 | 预留前拒绝 |
| task 删除实际存在的额外环境变量 | 0 / 0 | task 看似合法，原始进程 `environ` 不匹配而拒绝 |
| task 隐藏实际存在的重复参数 | 0 / 0 | task 看似合法，原始进程 `cmdline` 不匹配而拒绝 |
| intent 后、Fleet 前修改 task 环境 | 0 / 1 | 不调用 Fleet，保留已消耗 intent |
| Fleet 返回后修改 task 环境 | 1 / 1 | 不返回 reservation receipt，不允许 fixture backend 初始化 |
| Fleet 提交后丢回包 fixture | 1 / 1 | 不返回回执；活进程重试及恢复均不再次调用 Fleet |

全部 **10 个场景通过**，无 skip/race。兼容验证在同一最终 binary/image 上
通过既有 17 个 publication CRI 场景、11 个 ledger/startup 主测试，以及
新增 20 个严格配置规则子场景。全库 test/vet/lint、Linux integration-tag
nodeagent lint/vet 与 amd64 交叉编译通过；没有声明 amd64 原生运行。

## 反例的独立有效性

Go overlay 仅关闭 `cmdline/environ` 与 task 字节比较，保留环境批准、image
测量、实际 journal、发布挂载和 Fleet 逻辑。正常场景仍通过；隐藏环境与
隐藏参数两个场景都错误地产生 `1 intent / 1 Fleet`，因而按预期失败。
正式源码中同样反例为 `0 / 0`。overlay 不修改工作区源码。

这证明 task bundle 的可信文件来源不能代替活进程输入比较；它不证明 procfs
视图等于完整的历史配置消费或不可篡改的执行状态。

## 发现并处理的实际装配问题

- 首次构造 OCI config 覆盖了已有 RootFS/diff IDs，containerd 拒绝导入：
  `mismatched image rootfs and manifest layers`。改为保留 base ConfigFile
  的完整 layer 元数据后设置入口与环境。
- 无网络测试 sandbox 使用其私有外层环境的 host-network。runc 拒绝设置
  私有 UTS hostname：`unable to set hostname without a private UTS namespace`。
  测试显式注入计划派生的 HOSTNAME 环境值，不更改 UTS，不放宽环境检查。
- task.Env 原本仅有 PATH/HOSTNAME，但实际 `/proc/.../environ` 多出
  `HOME=/`。真实检查因此拒绝正常场景。最终 image/task 显式配置 HOME，
  避免 runc 在 task config 之外追加默认值；保留逐字节检查。

各次失败的日志、source.patch 和 binary/image 摘要均保留。最终证据见
[机器清单](node-remote-cli-reservation-evidence-2026-09-09.json)，其中 native
为最终源码，early-* 为前述不同源码的失败记录。

## 边界与下一步

本轮实际 CLI、CRI、image/task、只读 publication、held journal、Fleet
fixture 预留已在同次调用中成立。Registry、Pod API、Fleet 以及最后 Permit
仍为 fixture；没有把前轮真实 PostgreSQL/TLS 的单独证据合并成此次结果。

当前 Fleet production renderer 仍注入本地 journal 模式环境，本测试直接
构造了严格远程 CLI 的 CRI 参数。Pod API fixture 仍使用 verified plan 的
既有渲染结果，不能因此声称生产 Pod→CRI 配置转换已经闭合；生产渲染和
Node daemon/Fleet 挂载、通道、配置入口必须一起更新。

procfs 读取证明的是所采样的 argv/exec 环境视图，不能证明环境从未被改写，
也不覆盖进程内部后续 `os.Setenv`；task cwd 校验不是 live cwd 连续性证明。
原 pidfd 可以跨 exec，重复 executable/参数采样仍不能排除未观察到的 ABA
切换、动态加载或代码内存改写。一次性 grant 前必须补执行连续性与代码信任
约束，不能凭此次采样直接发放生产许可。

首次 journal 请求的 owner enrollment 目前也是测试装配。生产入口需明确
**授权前只读、grant 后才允许相应写入**的状态边界，不能在仅凭初次 pidfd/CRI
身份注册后就开放全部 Runtime journal 写入角色。本轮未新增此生产注册流程。

接下来将上述启动授权、真实 Fleet 与生产接线合并，再完成 Worker 业务
journal 独立保管、同一路径四 Stage CPU Job、exact-cache/transfer/一次
Charge/cleanup、真实后代停止、多成员失败、历史安全回收与持续到达验证。
完整顺序见[剩余验证清单](remaining-validation-2026-09-09.md)。
