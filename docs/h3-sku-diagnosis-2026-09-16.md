# H3 invalid_sku 排查记录（2026-09-16）

根因是客户端请求与已启用价格表的精确标识不一致。通用接入文档的最小示例误写
`model=minimax-h3`，而线上只有 `minimax-h3-live-validation`。用户按该示例编写的
探测脚本对全部候选都固定发送前者，所以换 preset、时长、音视频规格均无法命中。

`vela_t2v.py` 还有第二个不匹配：默认 `generation_preset=balanced`，线上当前仅
开通 `fast`。只改模型名仍会失败，必须同时选择正确 preset。

## 线上有效组合

本次在 `.70` 的 PostgreSQL primary 查询现状，并使用中转项目的实际 Credential
建立 `jobs:submit` 请求上下文，以 `vela_request` 角色调用线上
`vela_resolve_active_sku`；整个诊断事务最后 ROLLBACK，不创建 Job 或 Charge。

| 字段 | 当前值 |
| --- | --- |
| model | `minimax-h3-live-validation` |
| generation_preset | `fast` |
| service_class | `standard` |
| output_spec | `h3-native-av-1344x768-5s-24fps` |
| generation_count | `1`（此处单条示例） |
| h3.task | `t2va` |
| h3.target | `short_edge=768`、`aspect_ratio=16:9`、`duration_seconds=5` |
| h3.sampling | `num_inference_steps=20`、`quality=lossless` |
| 单价 | `100 minor CNY`，即 ¥1.00 |
| Rate line | `166a7e2c-f880-5046-be06-c11e06a233f7` |

该 Rate Card 为 ACTIVE，生效时间已到，`expires_at` 为空；不是价格表过期。
另外存在 `h3-internal-validation` 内部服务类的测试价格行，中转接入使用上表的 `standard`。
不把不同服务类的行解释为更多公开套餐。

## 复现和单变量核对

原始探测脚本第 2 项请求（`minimax-h3 + fast`，采样参数正确）在 `.70`、`.71`
域名入口均复现 `HTTP 400 / invalid_sku`，消息与用户一致：

```text
no ACTIVE certified Rate Card line matches the request
```

复现前后中转项目均为 0 Job、0 Charge。模型不存在的只读预检通过后才发送这两次
已知无效请求，没有批量试投有效 SKU 并取消，因此没有新增 GPU 任务或费用。

线上实际匹配函数的结果：

| 请求差异 | 匹配行数 |
| --- | --- |
| `minimax-h3 + fast` | 0 |
| CLI 原默认：`minimax-h3 + balanced` | 0 |
| 只修正 model：`minimax-h3-live-validation + balanced` | 0 |
| 正确 model + `fast` + 5 秒音视频 | 1 |
| 正确 model + `quality` | 0 |
| 正确 model + `fast` + 10 秒 | 0 |
| 正确 model + `fast` + 5 秒纯视频 | 0 |

因此当前 `quality`、`balanced`、10 秒以及纯视频规格没有可购买的价格行。
OpenAPI Schema 允许枚举，不代表部署环境已经认证并开通每一种组合。

不传 sampling 的第 4–6 项在 SKU 匹配之前被原生 H3 参数检查拒绝，返回
`invalid_request`，这是另一条明确的校验规则，不能通过省略采样参数绕过。

提供的请求 ID `c1d7dc08-8c20-4fea-9b03-1de4f31c56ec` 在当前 Control Pod 日志中找到；
现场 [日志摘要](evidence/h3-sku-2026-09-16/request-log.json)仅保留请求关联信息及可提取的状态字段。

## 使用现有脚本调用

无需先修改用户脚本，显式覆盖两个默认值即可构造正确 SKU：

```sh
python3 ~/Downloads/vela_t2v.py run "清晨的海边，伴随自然海浪声" \
  --model minimax-h3-live-validation \
  --preset fast \
  --output-spec h3-native-av-1344x768-5s-24fps \
  --duration 5 \
  --thumbnails
```

`test_h3_sku.py` 第 75 行的固定 model 同样需要改成 `minimax-h3-live-validation`；
此后候选中只有 `fast + 5 秒音视频 + 20 步/lossless` 符合当前公开价格行。
该脚本通过真实提交再取消来探测，可能进入计费执行阶段，不能把成功后立即取消
视为无费用 SKU 查询。目前没有公开 SKU 目录/试算接口，本次使用数据库回滚诊断验证。

用户附件没有被修改。已修正 [通用接入文档](api-integration.md) 的模型名和当前
开通范围，并在 [中转项目接入文档](relay-station-onboarding-2026-09-16.md)明确标识。
没有修改线上 Rate Card、模型状态、认证数据或服务配置。

## 验证边界和预防

已用用户脚本自身的 `build_parser` / `build_request` 生成默认与覆盖后的请求，
确认覆盖命令以及修正文档中的 JSON 都与线上唯一 `standard` 匹配组合一致。
本次验证了 SKU 解析成功，没有再次提交有效付费生成任务，也不把价格表匹配等同于
本轮完整视频生成验收。既有完整生成验收见 [H3 API 修复记录](h3-api-repair-2026-09-16.md)。

交付示例应该直接来自已发布 SKU 清单，并在发布时用当前请求身份检查匹配结果；
通用能力说明需要与当前环境实际套餐分开。后续可提供只读 SKU 目录/报价接口，
让接入方不用猜组合或提交真实任务来发现规格。本次未扩大为新 API 开发。

证据：

- [线上目录](evidence/h3-sku-2026-09-16/catalog.json)。
- [两入口复现](evidence/h3-sku-2026-09-16/reproduction.json)。
- [实际匹配函数](evidence/h3-sku-2026-09-16/resolver.json)。
- [脚本与修正文档请求检查](evidence/h3-sku-2026-09-16/client-request-check.json)。
