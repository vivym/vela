# 两个 H3 模型的修复同步核查

2026-09-17 19:15（Asia/Shanghai）现场核查结果：**尚未全部同步**。
`minimax-h3-live-validation` 的完整取消恢复验收不能替代 `minimax-h3` 的对应验收。

| 项目 | `minimax-h3` 当前状态 |
| --- | --- |
| DiT 忙时容量观测过期、无法继续排队 | 已共享数据库修复；现场 Admission 函数调用有效分配辅助函数 |
| Admission 数据库死锁/序列化失败的有界重试 | 已共享 Control 修复；两个副本均为 `d3be381e…` |
| 早期终止证明时钟容差与 execution floor 重试 | 正式模型发布记录已有，当前 GPU 镜像仍与该发布一致 |
| 后续 Worker journal 拒绝后的 drain 恢复、Control 重连及授权失效恢复 | 11 个正式 GPU 实例尚未升级到最终修复 Worker 镜像 |
| Python 驱动取消后主动收尾及 shutdown 锁顺序 | 未同步；现场正式 DiT 实际导入的文件仍为补丁前摘要，取消收尾方法缺失 |
| 共用缩略图 Worker | 两模型路由到同一新 Worker，已使用最终 Worker Agent 镜像 |

## 当前实际镜像

| 组件 | 正式模型的 11 个 GPU 实例 | live-validation GPU 实例 |
| --- | --- | --- |
| Worker Agent | `sha256:2422e76ac483182a12a2a6161fb15157e1e1f564b5eb8d08e818e742156a3ac8` | `sha256:a4896ac14b7280bd2bb203e915e66c9708f10eb6e8ac261ea3aed22dc56834f2` |
| H3 Runtime | `sha256:5bbb3a6306849da82f649703b241b757207b393dc558e09ef035ac0f8ad4e6ff` | `sha256:71bd111548526028064af17ed575b04dfb4a8b733a0306ec433208e52f6b3f95` |

11 个正式 GPU 实例均为 READY/CONNECTED，Pod 为 Running；这是当前可用状态，
不是取消/重连恢复缺陷已修复的证明。核查时没有非终态 Job。

除了检查全部正式实例的容器 imageID，还分别在两个模型的一个 DiT 容器中使用
实际解释器导入 `fast_h3.vela.driver`。两者均导入
`/opt/venv/lib/python3.12/site-packages/fast_h3/vela/driver.py`：

- 正式模型文件 SHA256：`664ab4c73ddd6dd3863c8449d6dff929ed196e98fbc0ed56bec4559d36cd17f6`，缺少 `_finish_cancelled_execution` 及其调度。
- live-validation 文件 SHA256：`4c3df85e0e2bf14556d635e1a3bc5539e0a8be8731fe1f2f0f2e40a6dbaa859e`，包含上述修复。

这与 [驱动补丁记录](../deploy/h3-runtime-patches/cancel-finalization.json) 的前后摘要一致。
现场证据见 [live-audit.json](evidence/h3-model-fix-parity-20260917/live-audit.json)。

## 后续发布要求

将已验证的 Worker/Runtime 修复发布到正式模型的 1 Encoder、8 DiT、2 Decoder，
生成与新 Runtime digest 匹配的 StageProfile、认证及版本化路由；不能原地改写不可变
旧 Profile 或复用旧 Worker journal 身份。发布时保留现有 Job、计费与两模型路由，
并继承中转排队 64、关联阶段池 128 的运行期设置，新建池同样需显式设置 128。

随后对正式模型重新验证忙时受理、运行中取消、无需人工重启的容量恢复、后续完整
音视频交付、幂等性和唯一计费。本次只核查版本与实际代码，没有发布升级或执行新的
取消故障验收。历史普通生成成功仍有效，但不能用于声称最新异常恢复场景也已通过。
