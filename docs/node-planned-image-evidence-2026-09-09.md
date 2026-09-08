# Registry 绑定镜像默认入口与原始 caller 的关联

日期：2026-09-09。基线：`d8da8c9`。范围：本地 Linux/arm64、真实
containerd v2.3.1 / runc v1.4.2 与 CPU helper；无 GPU 或部署。

## 问题与改动

此前 `ObservePlannedCaller` 能验证 Registry/Pod/CRI/caller 的关联，并记录活
进程 executable 摘要，但尚未把该摘要与批准镜像中的文件进行比较。仅有相同
CRI image ID 与签名 launch declaration，不能排除入口被 bind mount 替换。
这是未来启动批准的缺失前提，不代表原有观察接口已经签发过生产许可。

新增 `RuntimeImageObserver.InspectLaunch`，从可信输入的单平台 manifest 摘要
推导 config 摘要、默认 argv 和 executable 路径，复用原有已解包 native snapshot
测量与资源保管/清理逻辑。调用方不能另选同一镜像中的其他可执行文件。
原来的 `InspectExecutable` 保留显式目标检查语义。新对象内部保存配置字节，
返回的 configuration 是独立副本，任何失败（包括末尾清理失败）都不返回对象。

新增 `ObservePlannedImageCaller` 将这些已有职责接到同一次观察：

1. 从经验证的 `RuntimeLaunchPlan` 取 model-runtime 的 immutable image digest，
   不采用请求指定的 image/path；当前只支持 Fleet 使用镜像默认 command/args。
2. 用两侧保留的 kernel pidfd 确认 image/container observer 属于同一个 daemon。
3. 重新读取计划 Pod、原始 caller、实际 task bundle，并检查 runtime options/hook
   内容策略。独立测量上述批准镜像的默认入口。
4. 比较实际 OCI argv、CRI config image ID、活 executable 的 digest/size；要求
   当前 executable 是 root:root、单链接、无危险写入/特殊权限的普通文件。
5. 末尾复查同一 caller、Pod resource version、executable、boot 与 daemon，
   拒绝可见变化。输出仍是观察记录，不是可以兑换启动许可的 token。

代码：[image launch](../internal/nodeagent/runtime_image_launch_linux.go)、
[镜像测量复用](../internal/nodeagent/runtime_image_linux.go)、
[caller 关联](../internal/nodeagent/runtime_planned_image_linux.go)。

## 当前证据

最终 [原生 runner](../hack/run-task-launch-native.sh) 的 **9 个主测试全部通过**，
无 skip/race。实际 CRI 主测试 20.01s，镜像层主测试 80.83s，native containerd
主测试 6.41s；这是固定 CPU fixture 的执行时间，不是生产时限或性能保证。

- **实际正例**：签名 Registry fixture 绑定导入的真实镜像 manifest；从该镜像
  推导 `/probe` 与默认 argv，并与 UID/GID 10001 的原始 caller 完整比较。
  image/container 观察器都独立连接真实 daemon。
- **入口替换反例**：另一实际 CRI container 保持同一 image ID 和同一签名
  manifest，在 `/probe` bind mount 一份可执行但追加了不同字节的 ELF。
  原有 `ObservePlannedCaller` 可以观察该进程；新增关联接口拒绝它。
- **argv/daemon 反例**：actual task bundle 声明额外 argv 时拒绝；另一个相同
  NodeIdentity 的真实 root containerd 也不能供应 image 观察。
- **镜像语义**：regular overlay、whiteout/recreate、opaque directory、hardlink
  copy-up、relative symlink 五种场景中，新的默认入口路径与原有显式测量都符合
  独立预期。没有使用简单 tar flatten 结果替代 native snapshot 语义。
- **新接口失败语义**：corrupt content、丢失 activation 回包、cleanup 期间取消、
  cleanup 失败均返回 nil launch。cleanup 失败保留 lease，显式清理后再核对；
  每轮在正确的 containerd namespace 检查无遗留观察 lease/view/activation。
  原有十二种 image observer 故障和并发观察继续通过。
- **输入与隔离**：默认 argv 可从 Entrypoint+Cmd 或仅 Cmd 推导；空、相对、
  非规范、带 NUL、过大输入拒绝；返回 argv/configuration 不能改写内部数据。
- 既有 task options、sandbox/bootstrap、state copy/shadow mount 与 caller/daemon
  生命周期反例继续通过。

`go test ./...`、`go vet ./...`、golangci-lint v2.13.1、Linux integration Node
lint/vet 均通过；Linux/amd64 只有交叉编译证据。版本、镜像、binary、source patch、
逐文件摘要和日志见 [机器可读清单](node-planned-image-evidence-2026-09-09.json)。

首次联调使用此前的 Docker archive fixture。containerd 导入时重建 manifest，
导致导入前摘要对应的 content 不存在，观察拒绝。现改用 OCI layout 保留确切
manifest，并在导入后逐镜像核对摘要；失败日志和初始 patch 已保留。没有以导入
后的任意 metadata 值重新定义预期摘要来让检查通过。

## 仍未闭环

Registry/Pod 与 runtime policy 仍由测试装配。当前新接口验证的是 canonical
manifest declaration；生产 BackendStartupRequest envelope、持有 journal、epoch
预留与一次性 grant 尚需在同一次启动调用中组装，不能从返回记录恢复许可。

本次没有批准有效 env、运行时读取的 config 文件、工作负载 mounts/writers，
也没有证明 executable 在发送请求时及整个区间内从未 exec/改写、加载内存与
文件一致、shim/runc executable 已被批准或后代/设备已停止。Root 管理员和内核
仍被信任。多架构 index、相对/脚本入口与 Pod command/args override 不属于
当前已经验证的默认入口关联路径。

接下来应把批准的 image/config 与实际 protected 文件和有效挂载关联，接入
Node/Runtime CLI、一次性 grant、Worker 独立业务 journal，再完成同一装配下的
四 Stage CPU Job 与故障恢复。此轮未运行 PostgreSQL 或完整 Job campaign。
Production Gates 保持 **0/9**。
