# Fleet Controller 部署准备 — 2026-09-15

当前状态：Fleet 和 Control 均为 2/2 Ready，运行在 `.70/.71`。Control 已采用
追加的 Fleet client CA 信任；三台 API Server 已逐台采用 webhook mTLS 配置，
正式 `vela-fleet-protection` 已注册。三个 API Server 共 12 项真实 allow/deny
AdmissionReview 验证通过，临时测试规则已清理。ResidencyPlan 仍为空，尚无正式 H3 Worker。

恢复后的首次 apply 曾因计划 ConfigMap 的 0440 权限导致 UID 10001 读取失败；
仅将非秘密计划文件改成 0444 后成功。TLS materializer 的私有文件权限保留。
主机配置采用过程中仅更新 RKE2 服务，没有 reboot `.66`，其 boot ID 不变。
现场证据：[主机采用](evidence/fleet-host-activation-2026-09-15.json)、
[真实准入](evidence/fleet-admission-live-2026-09-15.json)。

以下准备、失败和预检记录保留原发生时的状态；其中“未安装”“未采用”描述的是
恢复前快照，不代表上面的当前状态。05:30 CST 安装器因 NATS 2/3 和 `.66`
DiskPressure 在变更前停止，详见[磁盘事件](node66-disk-pressure-2026-09-15.md)。

## 已完成的准备

- 修复 Fleet 的 Secret 文件入口：`securefile.Read` 不接受投影 Secret 符号链接；
  init container 将六个指定文件复制到 8Mi 内存卷，目录 0700、文件 0400、
  UID/GID 10001。主容器仅能读取复制后的常规文件。init 使用只读根文件系统，
  drop ALL 后仅增加 CHOWN；Pod 不使用 fsGroup。
- `deploy/environments/marslab/fleet-controller/` 固定镜像 digest、两个 CPU 管理
  节点反亲和、PDB、只读镜像凭据、网络策略和 OTLP。计划输入明确是
  `rollouts: []`，不是可运行的模型发布。Stage Worker 仍需要真实模型与授权输入。
- cert-manager 六个 Certificate、三个 Issuer 在准备时均 Ready；三个独立 CA
  分隔 Fleet client、webhook server 和 webhook client。根证书十年，叶证书
  90 天、提前 30 天续期；当前叶证书到期时间为 `2026-12-13T21:21:42Z`。
- 六份不可变材料已生成并应用，只有材料 Secret/ConfigMap 被创建。
  [材料证据](evidence/fleet-material-snapshots-2026-09-15.json) 不包含秘密值。
- Control 当前 Fleet serving 证书沿用旧 finance CA；候选信任仅追加新 Fleet
  client CA，保留旧 CA 与已有 Node Agent 身份，未擅自更换服务端根信任。

镜像：

```text
10.1.201.70:5005/vela-fleet-controller@sha256:cee95fbf27503c81d420c7dff8cd46e5c754dea69627f90aa53a43d435fe96ca
source_sha256: b51c1a871cb6ed0165122d82e827235f8e0a357115b51361214daefe7bbb276b
binary_sha256: 92f3e1f4c66b85f2b233389e632a5d38b20517b57801140c2f8947a64aa849ad
source_archive_sha256: a8273a5da3e8078517ea46809e7baf4a3fb775f34cbf0cffdbc53dabfb407efd
```

这是工作树快照构建，不是 canonical release 所要求的 clean HEAD。Go 1.26.7
交叉编译 Linux amd64、CGO disabled；Scratch 镜像包含 Fleet 二进制与现有
Control 的 CA bundle。镜像复制过程的最终结果字段使用 `result`，不能把
不存在的 `passed` 字段视为复制失败或验收成功。已核对原始
[复制 receipt](evidence/fleet-image-replication-2026-09-15.json)，`.71/.66` 均为
`EXACT_IMAGE_REPLICA_PASS`，各 manifest 与 blob 已逐一验证。

## 候选和实际状态的差异

本地 `vela-control/deployment.patch.json`、`secret-contract.json` 已指向候选
`vela-control-transport-tls-fleet-v1-r-1c7bbdf59dad`；现场仍使用旧的
`vela-control-transport-tls-v-ebd9cc4d0cb4-r-e065c3d7fabb`。
因此恢复前不可直接重放整个现场 overlay。原失败记录见
[foundation interruption](evidence/fleet-foundation-interrupted-2026-09-15.json)。

安装候选有 12 个资源。恢复后应使用新尝试目录和 receipt，首先验证
Control 2/NATS 3/PostgreSQL 3、管理节点压力、MinIO 与 Longhorn，再仅切换
Control 的一处 Secret 引用。先安装 Fleet 运行组件并验证两个 CPU 副本与
mTLS，最后才注册 webhook；不能覆盖已有失败记录或略过预检。

07:00 只读核验确认 CNPG 的数据库名为 `app`。旧临时 foundation 安装器在尚未
执行到的 Jobs 检查中误写了 `-d vela`；新的
[`hack/deploy-fleet-foundation.py`](../hack/deploy-fleet-foundation.py) 从实时 CNPG
配置读取数据库名，并补充三控制面无压力、APISIX etcd、MinIO 三主机各两个
健康成员和活动 Longhorn 卷健康检查。默认只预检，实际变更需要显式 `--apply`；
每次运行必须使用新的 `foundation-*` 子目录。旧失败记录保留。
07:03 的[默认预检实测](evidence/fleet-foundation-preflight-v2-2026-09-15.json)
在节点压力检查处按预期拒绝，未进入任何生产变更步骤；尚未验证恢复后的 apply
路径。受控版本位于 `.70:/opt/vela-cluster/fleet-deployment-20260915/deploy-fleet-foundation-v2.py`。

## 三台主机的候选配置已验证

三台 API Server 当前使用 `/etc/rancher/rke2/rke2-pss.yaml`，其中的 PodSecurity
配置必须保留。候选是将 ValidatingAdmissionWebhook 的 kubeconfig 引用加入
AdmissionConfiguration，通过 RKE2 支持的 `pod-security-admission-config-file`
持久化；文件放入已挂载的 `/var/lib/rancher/rke2/server/` 根目录内。
三台主机已写入独立候选目录，但尚未安装到生效的 RKE2 配置目录，也未重启
RKE2/API Server。三台候选路径相同：

```text
/var/lib/rancher/rke2/server/vela-fleet-admission/candidates/218cab1ddacab6acecca1266/
```

`hack/prepare-fleet-webhook-host.py` 从实际 kube-apiserver static Pod 读取当前
admission 文件，核对不可变 Secret 名称/UID/revision、独立固定 CA、私钥配对、
SPIFFE 身份、仅 clientAuth 的用途以及至少 14 天剩余有效期。候选目录为
root:0700，五个文件均为 0400；证书和私钥合并在 `client.pem`，没有写入公开证据。
有冲突的 webhook 配置、额外身份、符号链接或已变化的候选文件均会被拒绝。

| 验证 | 结果 | 证据 |
| --- | --- | --- |
| .70/.71/.66 主机准备 | 3/3 成功，`activated:false` | [主机准备](evidence/fleet-host-preparation-2026-09-15.json) |
| 原生 Kubernetes 解析和身份范围 | 每台 14 项，共 42 项通过 | [原生校验](evidence/fleet-host-native-validation-2026-09-15.json) |
| 重复准备 | 3/3 `created:false`，所有文件 hash 不变 | [重复执行](evidence/fleet-host-idempotence-2026-09-15.json) |
| 运行状态保持 | 三台 boot ID、RKE2 InvocationID、static Pod manifest hash 均不变 | 同上 |

独立 Go 模块 `hack/fleet-webhook-native-check` 固定上游 Kubernetes v0.35.7，
匹配现场 `v1.35.7+rke2`；没有升级根项目的 v0.34.1 依赖。它使用上游
AdmissionConfiguration/WebhookAdmissionConfiguration 解析器、webhook 身份
解析器和 client-go TLS 文件加载器，验证 PodSecurity/原 mutating 配置保留，
以及十种服务名、namespace、端口和 URL 情形的证书作用域。

首次检查在 .70 因上游 decoder 将空 map 初始化为非 nil 而被本地严格比较拒绝，
未继续其他主机。修复只规范化空集合，仍拒绝额外认证字段；真实 decoder
回归和 12 个负例通过，六个 Python 测试也通过。保留
[原失败记录](evidence/fleet-host-native-validation-initial-2026-09-15.json)，成功
版本使用 `native-check-v2` 与独立 receipt，没有覆盖失败证据。

运行方法及采用顺序见
[HOST-ADMISSION](../deploy/environments/marslab/fleet-controller/HOST-ADMISSION.md)。
这 42 项检查证明配置能解析、身份范围与材料加载正确，没有产生真实 AdmissionReview。

## Webhook 准备阶段的待执行项与后续结果

Kubernetes apiserver v0.35.7 的
[authentication.go](https://github.com/kubernetes/apiserver/blob/v0.35.7/pkg/util/webhook/authentication.go)
已核实：Service webhook 的 AuthInfos 首先精确匹配
`vela-fleet-admission.vela-system.svc:443`，443 还允许不带端口的精确回退。
配置应只给该精确服务名 client cert/key，不配置 `*` 或通用 current-context，
避免把 Fleet client 身份附加到其他 webhook。服务端证书信任来自 webhook
自身的 caBundle。

必须完成三控制面逐个采用配置、真实 Kubernetes AdmissionReview mTLS、
受保护资源拒绝与普通资源不受影响的实测。直接 curl webhook 不能替代
API Server 发起的准入证明。还需要把 cert-manager 续期后的证书推广成新的
不可变版本并滚动消费者；当前只准备了签发与快照，不宣称自动采用已完成。

06:39 CST [现场复核](evidence/cluster-readiness-after-fleet-preparation-2026-09-15.json)
仍有 `.66` DiskPressure，PostgreSQL/NATS/APISIX etcd 各 2/3、MinIO 5/6。
不能在该状态继续采用配置或注册 fail-closed webhook。必须先恢复有状态服务，
并确认 MinIO 三台主机各有两个健康成员。

后续已完成三控制面采用和 12 项真实 AdmissionReview，见本文开头证据。
证书续期后的不可变版本推广仍需单独验收。

已通过 Fleet 相关 Go 定向测试，以及 `go test ./internal/deploymentcontract
-count=1`。空计划、PKI Ready 和构建成功均不等于业务发布或 R4 收口。
