# 中转站 API 全流程验证（2026-09-17）

平台侧完整业务流程通过；当前正式模型为 `minimax-h3`，1 Encoder、8 DiT、2 Decoder。
使用中转站现有永久 Key 完成了一笔真实生成、完整音视频交付及唯一成本对账。
后续边界检查发现原生输出规格校验遗漏，修复与复验记录见下文；已有固定规格正常流程通过不代表所有请求参数都已验证。
中转站位于外部，本次没有登录或修改外部中转系统。

## 验证范围与结果

| 场景 | 结果与证据 |
| --- | --- |
| `.70/.71` HTTPS 网关、域名 SNI、CA 和身份认证 | 管理侧 Python 3.12 验证通过；现代严格 CA 兼容性见下文 |
| 正式模型精确 SKU、中文 prompt、自定义订单元数据 | 使用真实中转 Key 受理 202，最终 SUCCEEDED |
| 模拟首次受理响应丢失，再到另一入口重试 | 同一幂等键与原请求恢复同一 Job，没有新增费用；这里模拟丢弃响应，没有注入网络故障 |
| 4 路同幂等键并发重试、JSON 字段重排 | 都返回原 Job；报价不变 |
| 修改 prompt / 订单号重试 | 409；整数 `9007199254740993` 与 `9007199254740992` 没有混淆 |
| 查看 `jobs`、`active=true`、各状态筛选、limit、游标 | 两入口通过；分页不重复、顺序稳定、末页正确、详情与列表一致 |
| 生成期间轮询与列表发现任务 | 两入口通过；完成后从 active 列表消失，成功列表可查 |
| 未登录、无效 Key、跨项目访问 | 401 / 403 / 404 按接口边界拒绝，不暴露其他项目内容 |
| 非法 SKU / 模型 / 采样、空 prompt、非法数量、幂等键及 JSON | 400，拒绝后 Job / 幂等记录 / 额度预留数量没有增长 |
| 运行中取消、重复取消、对成功任务取消 | 专用验收项目通过；原 DiT 恢复新鲜正容量；成功任务保持成功 |
| 完整视频、音轨、缩略图 | 两入口完整下载，字节数 / SHA256 / 对象版本匹配，ffprobe 与 ffmpeg 全流解码通过 |
| 播放器 Range 读取与无签名下载 | 两类文件前 1024 字节返回正确 206；无签名访问被 403 拒绝 |
| 唯一收费、订单到任务到费用映射 | 真实中转任务一笔 POSTED / VISIBLE_COMPLETION，100 minor CNY，CSV 对账一致 |
| 永久 Key 与最小权限 | `expires_at=infinity`，四个业务 scope，不能管理身份、Webhook 或组织财务 |
| 服务持续运行 | 11 个 Worker READY/CONNECTED，服务 enabled/active、0 重启，最终无非终态 Job 和未释放 allocation |

专用验收项目第一轮包含 **173 次 HTTP 检查**；复用同一批已结束 Job 再运行包含
**127 次 HTTP 检查**，没有新建任务或重复收费。这些计数包含轮询，不是 300 个独立
故障类型。对应[首轮回执](evidence/relay-api-full-20260917/surface-first.json)、
[终态重跑回执](evidence/relay-api-full-20260917/surface-resumed.json)。

真实中转测试的[补充回执](evidence/relay-api-full-20260917/relay-extra.json)含 98 个检查记录，
同样包含轮询。中转永久 Key 没有更换，64 排队 / 8 运行及阶段池 128 的配置保持不变。
外部中转的每个用户仍由中转系统自身鉴权：Project Key 不是终端用户隔离机制。

## 真实中转订单与账本

| 字段 | 实测值 |
| --- | --- |
| relay_order_id | `relay-integration-20260917-full` |
| Job | `be2f655f-0027-4171-95cd-2124bf59655f` |
| Charge | `677b6c00-679e-47e3-bb7a-3daf48031523` |
| ArtifactSet | `c66d912f-cbca-429f-82a0-9948f56b921c` |
| 金额 | 100 minor CNY，即 ¥1.00；测试产生真实成本记录，没有清零或删除 |
| 视频 | 2,183,018 字节，1344×768、124 帧、24 fps，完整 AAC 双声道 32 kHz |
| 缩略图 | 7,258 字节，WebP |

视频 SHA256：`0a485655cbe5f8cffeb3192eff099e061e6ba707969ffd80d50268023cf7ca3f`。
[API 与固定报价](evidence/relay-api-full-20260917/relay-api.json)、
[账单导出验证](evidence/relay-api-full-20260917/relay-reconciliation.json)、
[.70 媒体探测](evidence/relay-api-full-20260917/relay-node70-ffprobe.json)、
[.71 媒体探测](evidence/relay-api-full-20260917/relay-node71-ffprobe.json)。

`.70:/opt/vela-cluster/relay-api-full-20260917/` 保存完整视频、缩略图及
`reconciliation/charges.csv`、`reconciliation/summary.json`。本次时间窗账单只有上述
一笔 ¥1 费用。导出时可用合同额度为 99,999,300 minor CNY（¥999,993），包含此前的
账本消费，不能将其余额变化全部算成本次测试。中转对终端用户的定价与退款是它自己的账本，
本次没有访问或验证外部中转的零售账本。

专用验收项目的成功 Job 为 `b8b243c4-6d18-450d-8cbc-77ef09c699b8`，
取消 Job 为 `73bb2ca8-ab3a-4749-9c7f-3d9f755a57b6`。两者各一笔 ¥1 费用，取消收费
符合已开始执行后的既有规则。首轮脚本中断产生的两条内部测试任务也已通过 API 取消，
两条各产生一笔 ¥1 的既有取消费用，未涉及其他用户任务；
[清理与费用回执](evidence/relay-api-full-20260917/initial-campaign-cleanup.json)保留，原始失败记录位于 `.70:/opt/vela-cluster/api-surface-20260917/`。

## 已修正的问题

1. 接入指南前半部仍声明 `minimax-h3` 不存在，后半部却推荐它；现已统一正式名称、
   JSON 示例和旧名兼容说明，并补充 Job 响应的 model 字段及跨项目 404/403 边界。
2. 初版验收脚本缺少 KUBECONFIG，并把所有跨项目访问都断言为 403。修正后真实服务
   拒绝行为通过，未为适应测试修改服务授权边界。
3. 新增可重复执行的[业务面验收脚本](../hack/verify-h3-api-surface.py)，先保存幂等键，
   原 Job 可恢复重试；保存每个错误回执，先前回执留档。时间排序按 RFC3339 实际时刻比较。
4. 全仓静态检查发现测试清理返回值未检查、Linux 专用辅助函数缺少构建约束、冗余选择器
   及错误文本规范问题，已修正。没有用这些格式修复重建或扰动正在通过验收的 GPU 服务。

## 原生输出合同补充检查（2026-09-18）

最小回归测试发现，已有校验只限制采样参数，会放过 `generation_count=2`、
`duration_seconds=4/10` 和 `aspect_ratio=9:16`。当前发布图只有一个视频和一个缩略图
输出，结果核验要求交付完整数量并符合固定规格，因此这些请求可能在受理后无法交付。

修复在原生 SKU 的 Admission 校验处检查单结果、768 短边、16:9、5 秒；
不匹配返回 `400 invalid_request`，不会改写用户参数或裁剪输出。省略 target 使用既有
默认值，其他 OutputSpec 的计数行为不受此校验影响。既有幂等记录的查找仍先于此校验。
真实数据库测试使用可受理的原生 SKU，验证拒绝时 Job、额度、重试状态、幂等和 Outbox
均无副作用，同时验证默认参数成功受理和重放。

Control 已滚动更新至 `sha256:1d37a552afd346ae4db373ef392edb195ce4c558e2f20a50e532e4abcfe13664`，
两个副本均就绪、0 次重启；无数据库迁移，模型 Runtime/Worker 镜像不变。
[发布回执](evidence/relay-api-native-contract-20260918/deployment-receipt.json)、
[精确构建来源](evidence/relay-api-native-contract-20260918/source.json)与
[相对基础提交的补丁](evidence/relay-api-native-contract-20260918/source.patch)已保存。

线上 [26 项检查](evidence/relay-api-native-contract-20260918/negative-api.json)通过：
两个网关、新旧模型名分别拒绝 2/16 结果、4/10/5.000001 秒及竖屏请求；
专用验收项目的 Job、幂等、额度预留和 Charge 计数不变。发布前真实中转订单在两端
均重放到原 Job。

发布后[完整接口回归](evidence/relay-api-native-contract-20260918/surface-post-contract.json)
**135 条 HTTP 检查记录全部通过**，使用既有验收任务重放，未新增其费用。
新真实中转任务 `3278edc6-0b5a-49e4-aa26-38d86097786d` 完成从受理、列表/详情轮询、
完整音视频/缩略图下载、SHA256/全流解码到收费与账单导出；同一幂等键的并发重试仍只生成
一个 Job。DiT 约 386 秒、Encoder 约 2 秒、Decoder 约 42 秒，四个阶段均首次执行成功。
[真实 API 回执](evidence/relay-api-native-contract-20260918/relay-api.json)、
[双网关完整下载与 Range 验证](evidence/relay-api-native-contract-20260918/relay-extra.json)、
[对账回执](evidence/relay-api-native-contract-20260918/relay-reconciliation.json)已保存。
唯一 Charge 为 `25fc787e-8b45-4896-bfc1-3b2b9a01e5d8`，¥1.00，
ArtifactSet 为 `e1e47176-e2b8-4ac6-9552-6bd88ab78195`。本轮时间窗导出一条收费，
合计 ¥1.00；新增验收费用保留在真实账本。

最终 [11 个 Worker 检查](evidence/relay-api-native-contract-20260918/final-runtime.json)通过，
无未终态 Job、无残留活动分配。远程完整媒体和对账文件保存在
`.70:/opt/vela-cluster/relay-native-contract-20260918/`。
本轮再次运行 `go test ./...`、`make lint` 并针对 Admission 执行 4 项数据库集成测试，
全部通过；[测试](evidence/relay-api-native-contract-20260918/go-test.log)、
[集成测试](evidence/relay-api-native-contract-20260918/integration.log)、
[静态检查](evidence/relay-api-native-contract-20260918/lint.log)可复核。
本次不扩展条件生成、多结果或其他规格的认证范围。

## 本地与真实数据库回归

- `go test ./...`、`go build ./...` 通过。
- 两组真实 PostgreSQL 集成测试共 **37 项**通过，无跳过：项目分页/隔离、幂等、余额与
  队列上限、并发受理、权限撤销、取消、账单和 Webhook 管理等。
- `make lint`：`go vet` 通过，golangci-lint `0 issues`；Linux/amd64 Worker 工具构建通过。
- API 媒体校验器 8 项 Python 测试通过；实际安装镜像的驱动取消契约 3 项通过。
- `go run ./hack/export-integration-openapi -check` 通过，消费侧契约与源契约一致。

证据：[Go 测试](evidence/relay-api-full-20260917/go-test.log)、
[集成测试](evidence/relay-api-full-20260917/integration-summary.log)、
[静态检查](evidence/relay-api-full-20260917/lint.log)、
[实际驱动契约](evidence/relay-api-full-20260917/installed-driver-contract.log)、
[最终运行态](evidence/relay-api-full-20260917/final-runtime.json)。
这些数据库测试使用独立临时环境，没有把生产队列故意填满或耗尽中转额度。
Webhook 的外网投递没有实际接收端，本次只验证管理/权限实现，当前中转 Key 也没有其权限。

## 外部中转环境仍需确认的条件

中转不在管理节点上；以下是已证实的环境边界，不能据管理节点结果推断外部已经连通：

- 本机与 `.70/.71` 默认解析不到 `vela.marslab.ic`；`.70` DNS 为公共
  `223.5.5.5`、`114.114.114.114`。平台验收固定连接 `.70/.71`，同时保留真实域名
  SNI/Host 和 CA 验证。实际外部中转需有相应内网连接及域名解析；本次未修改它的 DNS。
- [Go 1.26 默认 TLS 验证通过](evidence/relay-api-full-20260917/tls-go.json)，
  [Python 3.12 默认 TLS 验证通过](evidence/relay-api-full-20260917/tls-python312.json)，
  [Python 3.14 默认严格验证失败](evidence/relay-api-full-20260917/tls-python314.json)：
  MARSLAB Root CA 缺少 Key Usage 扩展。此前 macOS 原生验证还曾拒绝有效期至 2044 年
  的叶证书，见[域名证书记录](vela-domain-https-2026-09-16.md)。这是真实客户端兼容问题，
  没有通过 `verify=False` 或取消主机名校验绕过。已检查的 Nginx 证书目录没有 CA 签发
  私钥，需要原 CA 签发方提供含 `keyCertSign/cRLSign` 的合规 CA 证书和短期叶证书，
  再部署及更新客户端信任。

外部中转主机可以运行只读、零新增费用的[接入预检](../hack/verify-relay-external-access.py)：

```bash
# 在真实中转环境注入 VELA_BASE_URL、VELA_PROJECT_ID、VELA_API_KEY。
# 私有 CA 通过 VELA_CA_FILE 指向其可信 CA 文件；不关闭 TLS 验证。
python3 verify-relay-external-access.py \
  --job-id be2f655f-0027-4171-95cd-2124bf59655f \
  --download-directory ./vela-media-check \
  --output ./vela-external-preflight.json
```

它验证默认 DNS、默认严格 TLS 策略、Key、jobs/详情以及已完成任务的完整媒体哈希，
不会提交或取消任务。不得把签名下载 URL、API Key 或私钥提交 Git。
本次仍未覆盖 8-DiT 饱和负载、节点断电、外部中转进程崩溃及其零售退款逻辑。
