# Worker 到 Control 网络入口验收 — 2026-09-15

07:55–07:58 CST，完成 51 个在线 GPU 节点到两个 CPU 管理节点的来源测量，
并将 `vela-system/vela-control-allow-node-agent` 从仅 `.66` 扩展为这 51 个
实测来源。只允许 TCP 8444，每个来源为独立 `/32`；未开放整个 PodCIDR。
本次归入固定清单 R4，不代表应用 Node Agent 已部署或完成注册。

## 现场范围与结果

| 检查 | 结果 |
| --- | --- |
| 注册清单 | 52 个 GPU 节点，51 个 Ready；离线 `.19` 不探测、不加入规则 |
| 来源测量 | 51 × 2 = 102 次；每台主机到 `.70/.71` 的临时 ClusterIP 目标观察到相同来源，均为该节点自己的 PodCIDR 网络地址 |
| Control 可达性 | 每台主机到两 Pod IP 和 `vela-control` Service 的 8444，153 条路径通过 |
| 其他监听端口 | 每台主机到两 Pod 的 8081/8445/8446/8447，408 条路径超时拒绝；不把 connection refused 当成策略拒绝证据 |
| 未认证连接 | 先从原已允许的 `.66` 做 2 次基线，再从全部在线 worker 做 102 次；精确匹配服务端叶证书 SHA256 后，均收到 `TLSV13_ALERT_CERTIFICATE_REQUIRED` |
| CPU 跨主机来源 | `.70` 到 `.71` Control Pod、`.71` 到 `.70` Control Pod 的 8444 均超时拒绝 |
| 稳定性 | 来源测量前后及规则切换后，51 个 worker 的 boot ID、受检服务状态/InvocationID 不变；两个 Control Pod UID/IP、Service UID/IP、服务端证书不变 |
| 清理 | 两个探针 Pod、两个 Service、ConfigMap 和临时 NetworkPolicy 全部移除；后续 API 查询无本次资源残留 |

规则切换共 **667 项**检查通过，来源测量的 102 项另计。没有重启主机或
systemd 服务，没有安装 worker 软件、操作 GPU 驱动、初始化数据盘或删除既有数据。
`.44/.56/.57` 不在部署/SSH 探测范围；`.66` 仅执行只读状态查询和网络探测。

探针是固定 digest 的 BusyBox，运行在两个 CPU 管理节点，非 root、根文件系统
只读、无 ServiceAccount token、无额外 Linux capabilities。先在本地实际验证
CGI 返回来源地址，显式绑定 IPv4 后才执行集群探测。

## 证据与执行工具

- [来源与候选规则](evidence/worker-control-network-discovery-2026-09-15.json)
- [单策略切换、667 项检查和前后状态](evidence/worker-control-network-adoption-2026-09-15.json)
- [清理与执行脚本 SHA256](evidence/worker-control-network-postcheck-2026-09-15.json)
- [探针镜像本地实测](evidence/worker-control-network-cgi-2026-09-15.json)
- [15 项本地故障/边界测试](evidence/worker-control-network-tests-2026-09-15.log)
- [完整 deployment-contract 测试](evidence/worker-control-network-contracts-2026-09-15.log)
- [切换后集群快照](evidence/cluster-readiness-after-worker-network-2026-09-15.json)

执行工具为 [hack/worker-control-network.py](../hack/worker-control-network.py)，
`.70` 保留 `/opt/vela-cluster/worker-control-network-20260915/`，含执行脚本、
`discovery/receipt.json`、`adoption/receipt.json`、`readiness.json` 和 `postcheck.json`。
目录 0700，记录 0600；公开证据不包含 SSH 密码、客户端私钥或 Secret 内容。
同目录 `review.tar.gz` 保存本次脚本、测试、站点规则与公开证据，归档身份见
[归档验证](evidence/worker-control-network-archive-2026-09-15.json)。这是本次
网络变更的复核材料，不是全部 Vela 组件的 canonical release。

`discover` 只使用明确授权的节点集合；`apply` 要求来源记录完成清理、少于一小时，
且当前 Node UID/IP/PodCIDR/Ready 与测量一致。只有服务端固定证书的匿名 TLS
拒绝已经通过，才使用 resourceVersion/UID 前置条件修改单个 NetworkPolicy。
检查失败会恢复旧规则；即使 API 已成功但响应丢失，也会查询实际状态并回滚。
遇到其他操作者的新规则则拒绝覆盖。上述失败分支由本地测试验证，本次现场
成功切换未触发回滚。

站点 [node-agent-access.patch.json](../deploy/environments/marslab/vela-control/node-agent-access.patch.json)
已同步实测规则。**本次未整体应用 Control overlay**：其中 Fleet trust 仍有
未采用候选，必须按 Fleet 发布流程处理，不能借网络变更顺带覆盖。

## 仍未完成的条件

网络来源绑定到主机的 CNI 路径，不绑定某一个进程。匿名 mTLS 拒绝证明没有
客户端证书的连接不能进入 RPC；不证明持有证书的 Node Agent 已被正确注册、
授权、续期或运行模型。正式启动这些服务前仍需完成应用身份和 release 采用。
节点替换、PodCIDR 或 Service masquerading 改变时必须重新测量；`.19` 恢复后
单独补测，不从其历史 PodCIDR 推断当前可用性。

07:58 CST 快照仍为 54 注册、53 Ready、407 GPU；`.66` DiskPressure，
PostgreSQL/NATS/APISIX etcd 各 2/3，MinIO 5/6。Control 2/2、Argo 六个 Pod
Ready、两管理节点 HTTPS 返回 200。原 R1/R3/R5 的硬件与容量阻塞没有消失，
canonical release、实际身份注册及模型/SLO/Launch Receipt 仍按 R4/R6 推进。
