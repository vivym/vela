# .66 临时产物归档回收清单 — 2026-09-15

状态：用户明确批准后，70/70 个目录已于 2026-09-15 16:02 CST 前回收。
执行前重新核验两份归档 SHA256、逐字节内容、完整元数据、进程引用、挂载和硬链接；
实测释放约 94.00 GiB。两份完整归档仍保留在 `.70`，没有扩大源码/结果目录的删除范围。
见 [执行 receipt](evidence/node66-approved-reclamation-2026-09-15.json)。

16:10 CST 完成后续容量恢复：清理 5 个无运行引用、可重新生成的 marslab 工具缓存，
实际再释放约 19.79 GiB；根卷 ext4 保留块从 9,623,059 调整为 2,338,969（1%，约 8.94 GiB），
新增可用约 27.79 GiB，回退命令保存在 [容量 receipt](evidence/node66-capacity-recovery-2026-09-15.json)。
根盘可用达到约 225.11 GiB，kubelet 驱逐阈值没有修改。主机 boot ID 保持不变。
这仍只是有限余量，其他宿主作业继续写入会消耗空间；集群恢复以实时检查为准。

以下保留授权前的历史盘点及完整路径清单。

07:00 CST 更新：根卷可用约 122.10GiB，按原清单回收 94.00GiB 后仍比恢复线
少约 3.32GiB。因此原清单单独已不足以解除压力。待授权范围仍是下列 70 个
目录，没有添加其他路径；不能因目标缺口扩大而自行扩大删除范围。
见 [当前文件系统与服务状态](evidence/async-tracing-production-preflight-2026-09-15.json)。

06:39 CST 更新：根卷可用已降至 125.45GiB。按清单核定的 94.00GiB 计算，
回收后仅比 219.42GiB 恢复线高约 0.03GiB，持续写入可能随时消耗该余量。
因此以下 06:07 的恢复比例估算已过时，不能保证本清单单独足以解除压力。
没有扩展清单或删除任何原目录；执行前须复测空间和逐项预检。
见 [现场快照](evidence/cluster-readiness-after-fleet-preparation-2026-09-15.json) 与
[主机文件系统读数](evidence/fleet-host-idempotence-2026-09-15.json)。

## 原因与动作

`.66` 根文件系统约 877.67 GiB，kubelet 的 `imagefs.available=15%`、
`evictionMinimumReclaim.imagefs.available=10%` 使本次 DiskPressure
恢复目标为约 219.42 GiB 可用（25%）。回收本任务三份独立 Go 构建缓存后，
05:45 CST 实测约 137.23 GiB 可用，距恢复目标仍差约 82.19 GiB。
没有修改驱逐保护、污点或主机服务。

本清单共 70 个目录、94.00 GiB，以 `du` 实际分配块计。首批 67 个为
84.04 GiB；因为其他作业继续写入，06:03 CST 根卷可用又降至约 133.28 GiB，
仅回收首批已不足以达到 25% 门槛。因此补充 3 个超过 18 小时未修改、无运行
引用的旧编译目录，共 9.96 GiB。
绝大多数是其他项目的 Verilator 编译、仿真产物；少量包含源码和结果文件，
因此不能仅以“临时目录”视为可直接删除的数据。

计划将下列完整目录归档至
`.70:/opt/vela-cluster/disk-pressure-20260915/archive/node66-inactive-tmp.tar.gz`，
补充归档位于同级 `archive-extra/node66-inactive-tmp.tar.gz`（相对事件根目录）。
保留权限/属主/ACL/xattr，逐字节比较归档与原文件，核对复制前后完整元数据，
生成归档 SHA256。确认后才回收列出的原路径；回收前再次检查全部路径的
进程引用、容器挂载和文件元数据，发生变化的目录停止处理。可从完整归档
恢复原路径。归档可能含其他项目输入，只存于管理节点 root 私有目录。

`.70` 归档前实测可用 696.44 GiB；`.66` 所在 VG 没有空闲 extent。
此动作不使用暂缓初始化的 worker NVMe，不迁移/删除模型，不停止运行作业，
不重启 `.44/.56/.57/.66`。其他最近修改目录均排除。

## 明确路径

大小基于 05:47 CST 只读盘点。最后修改指目录中最新文件时间，时区 CST。
后续复核仍是删除前的必要条件；无进程引用不等于获准丢弃结果。

| 完整路径 | GiB | 最新文件修改 |
| --- | ---: | --- |
| `/tmp/fchip-block-major-logical3` | 5.005 | 09-13 18:00 |
| `/tmp/fchip-block-major-logical2` | 5.003 | 09-13 17:23 |
| `/tmp/fchip-p1-z-p2-numeric` | 3.961 | 09-13 15:26 |
| `/tmp/fchip-block-major-p1-sfu-20260912` | 3.109 | 09-12 23:45 |
| `/tmp/fchip-cluster-p1-sfu-20260912-o1b` | 2.747 | 09-12 22:44 |
| `/tmp/real-phase1-z-w2-20260913b` | 2.64 | 09-13 18:39 |
| `/tmp/real-phase1-z-w2-20260913` | 2.64 | 09-13 18:26 |
| `/tmp/fchip-real-p1-verilator` | 2.615 | 09-13 13:02 |
| `/tmp/fchip-real-p1-20260913-pass` | 2.611 | 09-13 13:52 |
| `/tmp/fchip-real-p1-20260913` | 2.598 | 09-13 13:32 |
| `/tmp/fchip-block-major-p1-sfu-numeric-retry` | 2.19 | 09-13 02:22 |
| `/tmp/fchip-source-cluster-elastic-20260912g` | 1.97 | 09-12 17:01 |
| `/tmp/fchip-source-cluster-single-20260912i` | 1.968 | 09-12 17:46 |
| `/tmp/fchip-source-cluster-single-20260912h` | 1.968 | 09-12 17:20 |
| `/tmp/p1-sfu-z-exact` | 1.791 | 09-13 07:09 |
| `/tmp/fchip-phase1-to-phase2-bounded` | 1.769 | 09-13 03:28 |
| `/tmp/fchip-phase1-source-20260912-rowdone` | 1.744 | 09-12 20:46 |
| `/tmp/fchip-phase1-source-20260912-rowdone3` | 1.744 | 09-12 21:05 |
| `/tmp/fchip-phase1-source-20260912-final` | 1.744 | 09-12 22:58 |
| `/tmp/fchip-phase1-source-20260912-rowdone2` | 1.744 | 09-12 20:56 |
| `/tmp/fchip-phase1-source-20260912e` | 1.743 | 09-12 20:29 |
| `/tmp/fchip-phase1-source-verilator-exact2` | 1.738 | 09-13 03:06 |
| `/tmp/fchip-phase1-source-verilator` | 1.738 | 09-13 02:33 |
| `/tmp/fchip-phase1-census-20260912` | 1.724 | 09-12 18:20 |
| `/tmp/fchip-phase1-census-20260912b` | 1.724 | 09-12 18:51 |
| `/tmp/fchip-phase1-census-20260912c` | 1.724 | 09-12 19:08 |
| `/tmp/fchip-real-phase1-package-docker` | 1.649 | 09-13 10:39 |
| `/tmp/fchip-real-phase1-package-docker2` | 1.649 | 09-13 11:07 |
| `/tmp/fchip-real-phase1-package2` | 1.649 | 09-13 10:27 |
| `/tmp/fchip-source-cluster-elastic-20260912c` | 1.614 | 09-12 15:56 |
| `/tmp/fchip-source-cluster-elastic-20260912f` | 1.494 | 09-12 16:52 |
| `/tmp/fixture-build` | 1.242 | 09-13 09:49 |
| `/tmp/fchip-source-cluster-elastic-20260912e` | 1.236 | 09-12 16:23 |
| `/tmp/fchip-phase1-source-icarus` | 0.991 | 09-12 17:35 |
| `/tmp/fchip-source-cluster-elastic-20260912` | 0.961 | 09-12 15:39 |
| `/tmp/fchip-real-phase1-package4` | 0.876 | 09-13 10:26 |
| `/tmp/fchip-p1-sfu-14wave-20260912` | 0.721 | 09-12 14:24 |
| `/tmp/block-major-p1-z-p2-logical` | 0.486 | 09-13 19:40 |
| `/tmp/fchip-run-src` | 0.477 | 09-13 04:09 |
| `/tmp/fchip-src` | 0.419 | 09-13 05:26 |
| `/tmp/fchip-phase1-z-w2-transaction` | 0.294 | 09-13 09:42 |
| `/tmp/fchip-z-w2-run2` | 0.293 | 09-13 05:11 |
| `/tmp/fchip-phase1-packet-replay-phase2` | 0.29 | 09-13 03:38 |
| `/tmp/fchip-phase1-packet-replay-phase2-rust` | 0.29 | 09-13 03:45 |
| `/tmp/fchip-phase1-packet-replay-phase2-negative` | 0.29 | 09-13 03:41 |
| `/tmp/fchip-phase1-packet-replay-phase2-rust-final` | 0.29 | 09-13 03:49 |
| `/tmp/fchip-z-w2-run` | 0.286 | 09-13 05:07 |
| `/tmp/fchip-phase1-packet-replay-phase2-new` | 0.286 | 09-13 05:00 |
| `/tmp/phase1-z-w2-final` | 0.285 | 09-13 05:15 |
| `/tmp/phase1-z-w2-transaction` | 0.285 | 09-13 05:06 |
| `/tmp/z-w2-obj` | 0.285 | 09-13 05:00 |
| `/tmp/phase1-p2-epoch` | 0.285 | 09-13 04:58 |
| `/tmp/phase1-replay-phase2-20260913` | 0.285 | 09-13 04:18 |
| `/tmp/phase1-replay-phase2-20260913b` | 0.285 | 09-13 04:27 |
| `/tmp/fchip-rust-die-golden` | 0.282 | 09-12 17:59 |
| `/tmp/phase1-z-w2-run` | 0.247 | 09-13 09:42 |
| `/tmp/fchip-run-phase1-z-w2` | 0.247 | 09-13 09:38 |
| `/tmp/ppmany-20260913` | 0.245 | 09-13 08:31 |
| `/tmp/fchip-p1-sfu-w2` | 0.24 | 09-12 23:28 |
| `/tmp/fchip-combine-src` | 0.24 | 09-13 04:17 |
| `/tmp/fchip-p1-sfu-w2-current` | 0.237 | 09-12 17:53 |
| `/tmp/vela-target-run` | 0.22 | 09-13 00:01 |
| `/tmp/fchip-scale-diff` | 0.15 | 09-13 16:56 |
| `/tmp/phase1-dual` | 0.124 | 09-13 08:27 |
| `/tmp/phase1-z-w2-iverilog` | 0.122 | 09-13 05:18 |
| `/tmp/fchip-z-w2-i` | 0.121 | 09-13 05:14 |
| `/tmp/fchip-z-w2-build` | 0.121 | 09-13 05:02 |


## 补充的三个明确路径

以下目录原先因不足 24 小时而排除。本次单独复核 18 小时内无修改、无运行引用，
使用独立归档与 receipt；不扩展到其他近期目录。

| 完整路径 | GiB | 最新文件修改 CST |
| --- | ---: | --- |
| `/tmp/real-q21-scaled` | 3.709 | 09-14 09:38 |
| `/tmp/real-q21-direct` | 3.703 | 09-14 08:07 |
| `/tmp/q21-node-combine-20260914-j1` | 2.547 | 09-14 10:06 |

## 验证边界

首批回收后曾预计仅略高于 25% 门槛，持续写入已消耗了这一余量。
补充三个目录后按 06:07 的可用量预计约 25.8%；仍以执行后的实测为准。
这解决本次驱逐恢复，不证明长期存储容量已经充足。必须等待 kubelet 自然解除
压力，再验证 Longhorn、PostgreSQL、NATS、APISIX etcd 和 MinIO。MinIO 的
被驱逐成员曾重新调度到 CPU 节点，恢复后还需核对三台主机各两成员的分布。

## 已完成的归档与预检

06:07 CST，两批的 `preflight-passed-no-removal` 均通过；检查了全部条目的
元数据、进程引用、挂载与硬链接，没有外部硬链接导致回收量虚高的情况。
两批共 167,846 个文件/目录/链接条目，预计释放 `100930142208 B`
（94.00 GiB）。归档创建与 GNU tar 内容比较都成功，复制前后完整元数据一致。

| 批次 | 压缩字节数 | SHA256 |
| --- | ---: | --- |
| 首批 67 目录 | 7,192,627,970 | `1f9325a269c63ecd7aa542e3477b9be4cfe7f138b53d6475b052f2ed1f6a0e37` |
| 补充 3 目录 | 662,968,681 | `63e36c12093cfb2766d6e9269cf0558cfe009ced2852021fd2eabb11876dcfaa` |

首批第一次流式比较因 sudo 输入与归档数据未分帧而失败；增加显式流标记后
独立重试通过，原始失败 receipt 仍保留。失败没有触发删除。两批回收工具已在
`.66:/opt/vela/verified-archive-reclamation-20260915/{archive,archive-extra}/`
完成无删除预检，默认只预检，只有显式 `--apply` 才会回收。该授权随后已取得，新的独立执行目录完成了重新复核与回收；原始预检记录保留。
