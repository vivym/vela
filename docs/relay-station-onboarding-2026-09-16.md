# 中转站项目接入与对账（2026-09-16）

中转站使用独立的 Organization 和 Project。初始可用合同额度为 **¥1,000,000.00**，
API Key 不自动过期，可由管理员撤销。创建及鉴权检查未产生新 Job 或 Charge。

## 项目配置

| 配置 | 值 |
| --- | --- |
| 项目名称 | `relay-station` |
| API 地址 | `https://vela.marslab.ic/api` |
| 两台入口 | `10.1.201.70:443`、`10.1.201.71:443`，以域名 SNI/Host 访问 |
| Organization ID | `40f37967-d2e0-4028-a6ab-1c75eb243289` |
| Project ID | `62275ddc-ae83-4ca1-b80c-313161264836` |
| Credential ID（不是密钥） | `3cf54270-5349-462b-b206-d57bcdb67ee2` |
| 初始合同额度 | `100000000` minor CNY，即 ¥1,000,000.00 |
| 创建后已占用 / 已消费未结算 | `0 / 0` |
| 自动过期 | 无；数据库为 `infinity`，管理 API 返回 `expires_at: null` |
| 权限 | `jobs:submit`、`jobs:read`、`jobs:cancel`、`artifacts:read` |
| 排队 / 运行上限 | `10 / 8`（2026-09-17 提升）；实际执行仍受可用模型容量限制 |

金额是后付费合同额度，不是收到现金的记录。可用额度 = 合同额度 − 已预留额度 −
已产生但未结算的 Charge。正常失败任务释放预留；收费规则以 Job 固定报价及最终
Charge 为准。结算通过财务核销流程回写，不删除历史 Charge 或直接清零账本。

实际 Key 不进入文档或 Git。私有交付文件：

- 当前工作机：`/Users/viv/.codex/private/vela/relay-station-20260916/connection.env`。
- 配套 CA：同目录的 `gateway-ca.crt`，已换为 MARSLAB Root CA。
- `.70` 管理节点：`/opt/vela-cluster/relay-station-20260916/connection.env`、`api-key`、`gateway-ca.crt`。

旧 NodePort 入口的 Vela CA 另存为 `nodeport-gateway-ca.crt`，不要用于域名入口。
私有目录权限 0700，Key 文件权限 0600。把配置及 CA 放到中转后端自己的 Secret 配置中。
此 Key 只访问自己的项目，不能管理其他项目、发放 Key 或修改财务额度。

## 当前可调用的 H3 组合

2026-09-17 已为本项目开通正式名称 `minimax-h3`，配置为独立的
1 Encoder、8 DiT、2 Decoder，旧名称 `minimax-h3-live-validation` 的路由保留。
新模型已通过真实 API、完整音视频/缩略图下载、唯一计费、幂等重放及执行后容量恢复
验收，见[独立部署与验收](minimax-h3-independent-deployment-2026-09-17.md)。
现有永久 Key 无须更换；建议新调用使用下方正式名称。
`balanced`、`quality`、10 秒与纯视频规格仍未在本次发布开通。
历史 SKU 问题见 [SKU 匹配诊断](h3-sku-diagnosis-2026-09-16.md)。

```json
{
  "model": "minimax-h3",
  "generation_preset": "fast",
  "service_class": "standard",
  "output_spec": "h3-native-av-1344x768-5s-24fps",
  "generation_count": 1,
  "prompt": "A cinematic view of a mountain lake at sunrise, with gentle water sounds.",
  "h3": {"sampling": {"num_inference_steps": 20, "quality": "lossless"}},
  "client_metadata": {"relay_order_id": "your-durable-order-id"}
}
```

请求地址为 `POST /v1/projects/62275ddc-ae83-4ca1-b80c-313161264836/jobs`，
加上 API 根地址；请求头需要 `Authorization: Bearer …` 和持久化的 `Idempotency-Key`。
收到 `202` 后保存 `job_id` 和完整报价，并按 [API 接入指南](api-integration.md) 查询和下载。
当前示例组合报价为 ¥1.00/条，实际以每次受理返回的 PricingSnapshot 为准。

API 与新签名下载地址均使用 `https://vela.marslab.ic`；DNS 由用户配置到 `.70/.71`。
域名入口已通过指定 IP 的双节点验收。客户端需信任 MARSLAB Root CA；现有泛域名
叶证书有效期过长，macOS 原生验证仍有兼容限制；根证书缺少 Key Usage，
本机 Python 3.14 默认严格验证也会拒绝，见[域名接入记录](vela-domain-https-2026-09-16.md)。
下载请求不携带 API Key。完整输出为 1344×768、124 帧视频与 AAC 音轨及缩略图。

## 中转站如何对账

中转站保存自己的 `relay_order_id → project_id / job_id` 映射、幂等键、Vela 报价、
终态、对用户的售价和退款记录。中转售价由中转站决定，允许与 Vela 成本不同。
对账以 `job_id` 连接订单，以 `charge_id` 去重；不要因为请求重试次数多就多算费用。
`client_metadata` 是辅助信息，不承担长期财务主键责任；中转站需自行持久保存映射。

管理员在 `.70` 导出指定时间段的 Vela 成本账单：

```sh
sudo python3 /opt/vela-cluster/relay-station-20260916/export-reconciliation.py \
  --project-id 62275ddc-ae83-4ca1-b80c-313161264836 \
  --from 2026-09-16T00:00:00+08:00 \
  --to 2026-09-17T00:00:00+08:00 \
  --output-directory /opt/vela-cluster/relay-station-20260916/reconciliation-20260916
```

开始时间包含、结束时间不包含，必须指定时区。工具在只读、一致性数据库快照中导出：

- `charges.csv`：Charge ID、Job ID、ArtifactSet ID、原因、状态、币种、金额（分）、入账时间。
- `summary.json`：期间笔数/金额、导出时可用额度、组织财务核销记录，以及 CSV 的 SHA256。

文件不含 prompt、API Key 或签名下载 URL。财务核销记录属于组织；本组织只给该中转站使用。
这不是开票系统：外部 Invoice 自动导出和结算回写尚未接入，当前可用 CSV 与中转账单核对。

仓库脚本：[export-project-reconciliation.py](../hack/export-project-reconciliation.py)。
已验证新项目导出 0 笔，以及既有验收项目导出唯一一笔 100 minor CNY；后者不计入中转项目。

## 永久 Key 的实现与验证

普通 ProjectAdmin 发 Key 接口仍要求未来 366 天内的到期时间；此次由平台按用户指令
配置永久 Key。数据库只保存 HMAC-SHA256 摘要，认证仍验证密钥、Project 归属、撤销和
Service Principal 禁用状态。

修复了管理接口直接把 PostgreSQL `infinity` 解码到 `time.Time` 的错误：永久到期
现在通过可空字段表示。真实 PostgreSQL 集成测试覆盖永久 Key 认证、查询和撤销；
有限期 Key、轮换/禁用回归及 HTTP JSON 投影测试通过。修复前永久 Key 查询明确失败，
修复后通过。没有放宽中转 Key 的管理权限。

Control 已滚动发布两个副本，镜像：
`10.1.201.70:5005/vela-control@sha256:f12e249463984dd936364a0c06274e73ca76b02abded69f5933aa97cdad80b96`。
保留 H3 已验证的 seccomp、ffprobe 8.0.1 配置和数据库 schema 107；无需迁移数据库或重启主机。

现场 `.70/.71` 两个网关共 12 项检查通过：永久 Key 鉴权成功，匿名/篡改 Key 被拒绝，
其他项目及其 Job 不可见，身份管理接口拒绝中转 Key。没有为检查创建新付费任务。
上述 Control 镜像和 schema 为 2026-09-16 的历史部署记录，不是当前全量清单。
2026-09-17 的新模型发布、容量及验收边界见
[独立部署与验收](minimax-h3-independent-deployment-2026-09-17.md)；8 个运行额度不等于
已完成 8 并发吞吐压测。旧模型历史证据见 [H3 验收报告](h3-api-repair-2026-09-16.md)。

证据：[创建回执](evidence/relay-station-2026-09-16/provisioning.json)、
[现场检查](evidence/relay-station-2026-09-16/verification.json)、
[初始对账导出](evidence/relay-station-2026-09-16/initial-reconciliation.json)。
