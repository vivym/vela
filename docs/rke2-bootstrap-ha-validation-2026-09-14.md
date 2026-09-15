# RKE2 首次入群与管理 API 切换

这项验证覆盖 R2：新 worker 在主入口不可达时首次加入，以及管理命令在执行前
选择通过证书与认证校验的健康 API。物理主机和 GPU 驱动均未重启，GPU 数据盘
未初始化。已加入 worker 的缓存切换演练不再重复。

## 实现

`deploy/cluster-platform/rke2-bootstrap-ha.py` 按 `.70/.71/.66:9345` 顺序检查
`/ping` 和 `/cacerts`，使用分发的集群 CA 验证证书及主机名。选择器不读取 join
token；认证由 RKE2 原生 bootstrap 处理。它原子写入 0600 的
`/etc/rancher/rke2/config.yaml.d/99-vela-bootstrap-ha.yaml`。全部失败时保留旧文件、
返回失败，由 systemd 重试。原生 ExecStartPre 保留，新增启动钩子只在 agent 启动时执行。

`configure-rke2-bootstrap-ha.yml` 检查命令行、三个原生 EnvironmentFile 与后续
YAML 片段不会覆盖 server 配置。安装后仅 daemon-reload、保留 enabled；通过
boot ID、MainPID、InvocationID 和启动时间的前后比较确认未重启主机或 agent。
集群 CA 轮换时须先重新分发该 CA；新的 worker 入群流程也须安装此启动钩子。

`vela-kubectl.py` 使用当前 KUBECONFIG，按 `.70/.71/.66:6443` 验证 `/readyz`，
然后只执行一次用户命令。连接及身份覆盖参数被拒绝，TLS 验证强制开启；实际
写命令出错时不切换端点重放，以免重复创建资源。管理节点使用：

```sh
sudo vela-kubectl get nodes
sudo vela-kubectl get pods -A
```

远程客户端可安装同一脚本与 endpoint 文件，并通过 `KUBECONFIG` 指定自己的
凭据；这不是 VIP，也不修改原生 kubectl。选择完成后若 API 随即故障，当前命令
仍会报错；操作者应先确认写入结果再决定重试。

## 真实首次入群

在 `.70` 启动一台 KVM 临时机，2 vCPU、4GiB RAM、12GiB 稀疏根盘；VM unit
限制 6GiB、200% CPU 和 30 分钟存活。Ubuntu 镜像来自南大中国镜像，经过官方
签名清单和 SHA256 双重校验。SSH 仅绑定宿主 loopback。

临时节点 `vela-bootstrap-drill-c8b6bf7bc6` 的 guest 内屏蔽 `.70:6443/9345`；
宿主防火墙未修改。初始 agent 目录与负载均衡缓存不存在。选择器验证 `.70`
失败、`.71:9345` 成功，使用实际 RKE2 `v1.35.7+rke2r1` 和原生 unit 加入集群，
于 14:22:38Z 注册，随后确认 Ready。使用该 guest 自己的 kubeproxy 凭据，
管理选择器也在相同故障下通过 `.71:6443` 读取真实 API 版本。

临时机有专用 NoSchedule taint，不接收业务。启动前将 Longhorn
`create-default-disk-labeled-nodes` 设为 true，仅三台已批准存储节点具有 opt-in
标签；现场 54 个既有 Longhorn Node 的 spec 完全不变。v1.12.1 源码确认：
`DataStore.CreateDefaultNode` 在此配置下不创建默认磁盘，
`KubernetesNodeController.syncDefaultDisks` 只处理显式标签且无既有磁盘的节点。
没有采用会被控制器提前删除的“预建 Longhorn Node”作为隔离保证。

验证后停止该 VM，删除测试 Node，检查 Pod、Longhorn Node/replica 和
node-password Secret 均不存在，并删除含 token 的介质、guest 根盘和 SSH
私钥。NAT guest 的 Ready 只作为首次 bootstrap 证据，不作为生产跨节点 CNI 验收。

## 下发与证据

50 个在线普通 worker 完成无重启下发；`.19` 不可达，保留为待恢复后的补装项。
`.44/.56/.57/.66` 未进入此 worker rollout。管理命令安装到两个 CPU 管理节点。
完整结果及清理检查见
[现场证据](evidence/rke2-bootstrap-ha-2026-09-14.json)。

本地验证：7 个真实 HTTPS selector 测试通过；5 个管理命令进程测试通过，覆盖
备用选择、写失败不重放、全部不可达时不执行、连接/身份覆盖拒绝和 exec 参数保留。
部署清单和演练分别由两个 Ansible playbook、
`hack/verify-rke2-fresh-bootstrap.py` 与 `hack/rke2-bootstrap-vm-guest.sh` 重放。

证据中 `vm.vm_pid`/`vm.vm_state` 是停止前的最后观测；最终清理状态由独立的
`cleanup_postcheck` 给出，确认 MainPID=0、unit 已回收和所有临时资源不存在。
