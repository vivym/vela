# GPU worker 数据盘启用计划 — 2026-09-14

**最新决定：用户要求先不处理硬盘，继续其他集群工作。本计划暂缓，没有获得清空/格式化授权；14 块盘均未格式化或挂载。**

本计划只使用本机磁盘承载模型、缓存和临时数据，不加入 Longhorn/MinIO。
已只读盘点 53 台在线机器；`.19` 无法连接，`.44/.56/.57` 没有操作。
`.70/.71` 只检测到系统盘；`.66` 的 ZFS 数据盘已有模型/容器数据，保留现有布局。

## 待确认的初始化范围

以下每台两块 Intel SSDPE2KX040T8 NVMe，每块 4,000,787,030,016 字节。
合计 14 块约 56.01 TB（50.94 TiB）。全部未挂载、没有已识别分区或文件系统签名、
没有块设备 holders 或打开该块设备的进程；但每块的稀疏读取样本都包含非零内容。
**这些证据不能证明盘内没有旧数据。初始化会覆盖可能遗留的内容，需要明确同意清空这些指定磁盘。**

| 节点 | 模型盘：当前 nvme1n1 / 序列号 | 缓存盘：当前 nvme2n1 / 序列号 |
| --- | --- | --- |
| 10.1.201.58 / server-120 | PHLJ9245024P4P0DGN | PHLJ8294038U4P0DGN |
| 10.1.201.59 / server-124 | PHLJ9266016P4P0DGN | PHLJ921302VW4P0DGN |
| 10.1.201.60 / server-121 | PHLJ920200F34P0DGN | PHLJ9202037S4P0DGN |
| 10.1.201.62 / server-128 | PHLJ921302HR4P0DGN | PHLJ920200P24P0DGN |
| 10.1.201.63 / server-126 | PHLJ926500LR4P0DGN | PHLJ9211013M4P0DGN |
| 10.1.201.64 / server-127 | PHLJ1104043A4P0DGN | PHLJ110404364P0DGN |
| 10.1.201.65 / server-125 | PHLJ921201ZB4P0DGN | PHLJ920201S94P0DGN |

执行按 `by-id` 路径、序列号、容量和审批清单 SHA256 锁定磁盘，不依赖 nvme 编号在重启后保持不变。

## 初始化后的用途与启动行为

- 模型盘采用 XFS，挂载到 `/srv/vela/models`，模型权重目录为 `weights/`。
- 缓存盘采用 XFS，挂载到 `/srv/vela/cache`，分别建立 `artifacts/`、`hf/`、`tmp/`。
- 使用 UUID 写入 fstab 并保留原 fstab 备份，启用 noatime/prjquota。
- 新增启动检查服务，验证两个挂载点的真实 UUID 和文件系统；RKE2 agent 在下次开机时依赖检查成功，避免数据盘缺失后把模型写进系统盘目录。
- 本次只启用检查服务，不重启 RKE2、不重启主机、不操作 GPU 驱动。
- 后续 Kubernetes 本地卷按节点绑定，配套挂载缺失、容量、inode 和读写错误监控。数据盘没有复制副本，模型应从原始制品重新拉取，缓存/临时内容可重建。

## 已准备的执行物

- [精确清单](../deploy/environments/marslab/worker-model-storage/initialization-plan.json)
- [初始化脚本](../hack/initialize-worker-model-storage.py)：默认只读 prepare；apply 必须传入明确批准的清单 SHA256。
- [签名和样本证据](evidence/worker-data-disk-signatures-2026-09-14.json)
- [全节点磁盘盘点](evidence/node-disk-inventory-2026-09-14.json)

清单 SHA256：`867dc1e524464f0fee0c51e6d714189110c6e81a5f8babc6db806a0d15c1ba49`。

四项本地安全测试通过：受保护/管理主机拒绝、重复设备/根挂载拒绝、第二块盘预检失败时不格式化第一块、部分初始化状态拒绝再次格式化。七台现场 prepare 全部通过，xfsprogs 已安装；此结果只表示预检通过，不授权清空旧数据。当前尚未执行任何格式化。
