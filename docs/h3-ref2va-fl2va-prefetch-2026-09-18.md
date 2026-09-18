# Ref2VA / FL2VA 支持核查与权重预缓存（2026-09-18）

本轮准备条件生成任务的本地权重，不代表 Vela 已发布 Ref2VA/FL2VA 或完成
真实 API、完整音视频、任务查询、计费与质量验收。

## 已核查的支持范围

检查位置是 `marslab@100.111.196.116:/home/marslab/projs/fast-h3`。主仓库
HEAD 为 `0de6ff6619bb5562fa3a5eeba7bceb9213e48b8a`，SGLang HEAD 为
`6dbcc18254cd288e7edb7e016efc641e089e7abc`。工作目录含未提交修改，包括
Ref2VA overlay、partition manifest 生成器和条件 tensor 跨阶段传输实现。
因此上述 HEAD 本身不能重现全部当前能力，构建新镜像前需固定实际源码快照。

- T2VA 与 FL2VA 共用 FL2VA partition、hardened v5 ConvRot INT8 DiT 与
  `MiniMax-H3-adaln-table-hardened/steps20.safetensors`，现有缓存可以复用。
- Ref2VA 使用独立的 `MiniMax-H3-int8-ref2va-convrot/transformer`、
  `MiniMax-H3-adaln-table-ref2va-convrot/steps20.safetensors` 及 Ref2VA
  index/processor/tokenizer/VAE 路径。
- 已读取 `/tmp/h3-task-benchmark/{fl2va,ref2va}-5s-dense/` 的运行记录，并对
  实际 MP4 执行 ffprobe：两者均含 1344×768、24fps H.264 视频及双声道 AAC
  音频。视频时长 5.166667s，容器时长 5.175s。这是既有单体生成结果的媒体
  核验，不是本次执行了新的分段推理或 Vela API 验收。
- 远端文档明确将三任务单卡 disaggregated E2E 和 joint AV 质量验收列为
  未闭合事项；不从任务枚举、配置文件或缓存成功推断正式服务可用。

## 固定权重清单

[Ref2VA manifest](evidence/h3-ref2va-prefetch-20260918/model-manifest-ref2va.json)：

- 文件数：76；总字节数：59,510,503,638（55.4235 GiB）。
- Manifest 文件 SHA256：`9cfdd65b1768f678860c21de08f86b3f4e09c427903930aa02796fdcc88f5d04`。
- 内容 identity：`sha256:df458abdc238abf3e891e123be1e93b05104614d4768b7472f5d85275653fa4c`。
- 源路径 `.66:/srv/models` 的完整校验通过，见[源校验](evidence/h3-ref2va-prefetch-20260918/source-verification.json)。
- 按 SHA256 与尺寸比较，60 个文件、38,185,908,455 字节与旧 FL2VA 缓存
  相同；16 个文件、21,324,595,183 字节（19.8601 GiB）需要新增。

缓存发布目录固定为：

```text
/var/lib/vela/models/df458abdc238abf3e891e123be1e93b05104614d4768b7472f5d85275653fa4c
```

每台先在 `.partial-<identity>` 私有目录准备。共同文件必须是 canonical
本地路径、0444 只读文件，重新校验 SHA256 后才硬链接；rsync 文件列表排除
这些已校验文件，不更改原权重内容或时间戳。差异文件下载后，再对完整清单
校验；文件 0444、目录 0555、必需空目录齐全后，原子 rename 发布。
最终 receipt 之前的 `.partial-*` 目录不能作为推理缓存使用。

## 节点与空间

[逐节点预检](evidence/h3-ref2va-prefetch-20260918/node-preflight.json)覆盖 44 台
256GB 档 GPU 节点。检查时可用磁盘为 545.47～762.20 GiB；44 台均有原
FL2VA 缓存目录。分发执行时再次检查节点名、物理内存档位、本地磁盘、缓存
路径和容量，并保留至少总容量 20% 或 10 GiB（二者取大）的空闲空间。

目标不包含 `.44/.56/.57/.66`。`.66` 仅作为首台复制的只读源；未重启任何
主机，也没有改动已有 H3 服务。缓存文件会在目标节点落盘，推理不依赖远端
权重路径。44 台理论新增内容约 0.8534 TiB，不含文件系统元数据及少量临时
重试开销。

## 后台任务与恢复

管理节点 `.70` 的配置、工具和进度目录：

```text
/opt/vela-cluster/h3-ref2va-weight-distribution-20260918
```

两阶段 systemd 任务：

| 阶段 | 管理节点 unit | 目标与数据源 |
| --- | --- | --- |
| 首台 | `vela-ref2va-weight-pilot-20260918.service` | `.66` → `.25/server-43` |
| 批量 | `vela-ref2va-weight-batch-20260918.service` | 已校验的 `.25` → 其余 43 台 |

批量任务必须观察到首台 unit 正常结束且 `receipts-pilot/progress.json` 的
`passed=true` 才启动。优先复制尚未承载当前 H3 实例的节点，最后复制当前
服务节点。批量并发 4，单节点 rsync 限速 65,536 KiB/s（64 MiB/s）。

任务运行期间配置开机启动，失败重试有界，已经 VERIFIED 的节点可跳过；
阶段正常完成后自动 disable 本阶段 unit，保留进度与逐节点 receipt。
执行时记录 boot ID 并核对复制前后相同，不通过重启验证启动行为。

每个阶段使用专用 `vela-ref2va-weight-source-20260918.service`。源配置位于
各源节点 `/opt/vela-ref2va-weight-source-20260918`，服务以普通用户运行，
只读、绑定内网地址 `:18736`、密码认证、限定客户端 IP，文件白名单来自
固定 manifest。已验证清单内文件可读、清单外 FL2VA 文件不可读。首台完成
后关闭 `.66` 的专用源，批量完成后关闭 `.25` 的专用源。

`.19` 的 sudo 需要密码：只对该节点使用既有授权凭据，经 SSH 标准输入
传递；凭据文件留在 `.70` 的 root-only 目录，未修改 sudoers，也不写入 Git。

查看进度（在 `.70`）：

```bash
sudo systemctl status vela-ref2va-weight-pilot-20260918.service
sudo systemctl status vela-ref2va-weight-batch-20260918.service
sudo cat /opt/vela-cluster/h3-ref2va-weight-distribution-20260918/receipts-pilot/progress.json
sudo cat /opt/vela-cluster/h3-ref2va-weight-distribution-20260918/receipts-batch/progress.json
```

`COPYING_OR_VERIFYING` 表示仍在复制或校验；`VERIFIED` 才表示该节点完整
缓存可用。出现 FAILED 应读取该节点错误，修复后重新 start 对应 unit，不能
手动将状态改成 VERIFIED。暂停复制只停止本轮两个管理 unit 和专用源，不删除
旧缓存或本轮部分文件。

## 工具验证与后续发布

### 当前分发进度

截至北京时间 **2026-09-18 01:54:45**，44 台目标中 **1 台 VERIFIED、
4 台正在复制或校验、39 台等待**，此时没有失败节点。

- `.25/server-43` 在 01:51:07 完成完整 59,510,503,638 字节校验并发布，
  缓存根目录权限为 0555；见[首台 receipt](evidence/h3-ref2va-prefetch-20260918/pilot-receipt.json)。
- 首台管理任务正常结束并自动禁用；`.66` 本轮专用源已停止并禁用。
- 批量管理任务已启动且配置开机启动，`.25` 的只读源处于 running。
  首批 `.19/.26/.27/.28` 正在复制或校验，其余节点自动按计划继续。
- 这是启动后的进度快照，不是 44 台全部完成的验收；见
  [服务与逐节点状态](evidence/h3-ref2va-prefetch-20260918/distribution-progress.json)。
  最新结果以 `.70` 上的 `receipts-batch/progress.json` 为准。

### 验证与下一阶段

本仓库保存[分发协调器](../hack/distribute-h3-model-cache.py)、
[增量预缓存工具](../hack/prefetch-h3-model-cache.py)与回归测试。
本地测试覆盖限定节点、工具摘要、原子文件、子进程超时终止、前置任务失败、
密码脱敏、共同文件复用与原时间戳保持，以及损坏/软链接/可写源拒绝。
实际节点的完整 SHA256 校验和 receipt 才是分发成功证据。

缓存完成后仍需：固定 fast-h3/SGLang 实际源码，构建包含最新条件边界与
partition 校验的镜像，生成 Ref2VA 独立 release/profile 并预热；分别测试
FL2VA 首尾帧和 Ref2VA 参考素材经过上传、执行图、完整音视频、Jobs 查询、
幂等及唯一计费的真实 API 闭环，再开放对应路由和 Rate Card。
