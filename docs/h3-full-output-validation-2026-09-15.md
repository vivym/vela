# H3 完整音视频输出

用户要求保留模型生成的完整视频和音频。此前尚未提交、尚未发布的裁剪逻辑
已经撤回；fast-h3 的 `local_media.py` 与 Runtime 构建来源 `0b23cb978d51`
保持一致。新增的测试提交为 `cfa3d597f46639deb1d2571274ec8389c494ee60`。

## 生成与校验

固定 SGLang `868143cc66` 的 `minimax_h3/time_request.py` 按 `17n+5`
向上对齐帧数，音频在 40Hz latent 边界取整。完整输出可能比请求时长略长：

| 请求时长 | 完整帧数（24fps） | 视频时长（毫秒，四舍五入） | 音频时长（毫秒） |
| --- | --- | --- | --- |
| 4 秒 | 107 | 4458 | 4450 |
| 5 秒 | 124 | 5167 | 5175 |
| 10 秒 | 243 | 10125 | 10125 |
| 15 秒 | 362 | 15083 | 15075 |

编码器传入完整帧序列和完整波形，不按请求时长切片，不用 `-shortest` 截断。
产物仍为包含视频和音轨的 MP4，不要求用户分别下载和拼装。

Vela migration 99 为 OutputSpec 增加显式 `media_contract`：

- `exact-video-v1` 是已有 OutputSpec 的默认值，保持原校验规则。
- `h3-native-av-v1` 用于新的 H3 原生音视频 OutputSpec revision。
  目前限定 4–15 秒、24fps、H.264/MP4，以及整数请求帧数。

新契约精确检查对齐后的帧数与视频时长，同时要求一条 32kHz 双声道 AAC
音轨。音轨时长不能低于模型原生预期，只允许最多 32 毫秒 AAC 末尾编码填充。
视频和音频必须从零时刻开始，容器时长应覆盖两条轨道；多视频轨、多音轨、
缺失音轨、字幕和数据流均拒绝发布。对象版本、哈希、尺寸和编码检查继续执行。

音轨时长以毫秒核验；这不是逐采样点无损证明，AAC 本身是有损编码。
实际输出保留由编码器完整输入路径、帧数检查和音频尾段解码回归共同验证。

## 对用户的返回值

`GET /v1/projects/{project_id}/jobs/{job_id}/artifacts` 的每个视频下载项增加
`media`。数据来自已提交产物的验证 receipt，并且只映射公开媒体字段：

```json
{
  "requested_duration_milliseconds": 5000,
  "duration_milliseconds": 5167,
  "container_duration_milliseconds": 5175,
  "frame_count": 124,
  "frame_rate_milli": 24000,
  "audio": {
    "codec": "aac",
    "sample_rate": 32000,
    "channels": 2,
    "duration_milliseconds": 5175
  }
}
```

`duration_milliseconds` 是实际视频轨时长，请求时长单独保留。下载 URL 仍指向
原来的完整、已验证对象版本；API 不额外转码或截断产物。

## 验证与部署状态

- Linux 的 8 项 native integration 测试通过。真实 CPU 编码输入包含 124 帧
  和 5.2 秒音频，验证 124 帧全部保留，并解码检查 5 秒之后的有声尾段。
- 固定 ffprobe 8.0.1 解析真实 H.264/AAC MP4 成功；额外轨道、缺音轨、
  错误编码、裁短视频、缩短音频等负例均通过。
- 数据库 migration 99 的升降级、旧契约默认值、新契约约束与不可变性通过。
  首次测试因测试夹具误用了不存在的 `DRAFT` 状态失败，改用实际 `REGISTERED`
  枚举后通过；未在生产数据库试错。
- `go test ./...` 通过，94 个有测试的 Go 包成功；定向测试强制要求固定 ffprobe。
- 另将仅本次暂存改动导出为隔离源码，定向 Go 与数据库迁移测试再次通过，
  确认无需带入原工作树中其他未提交修改。
- Spec 和 Standards 两轮只读复核无阻塞发现。

证据与日志见 [完整输出验证](evidence/h3-full-output-2026-09-15.json)。

这些结果验证了输出保留和媒体契约，不是新一次 GPU 推理或正式 Vela Job。
现场 Runtime 镜像仍在 `.70` 构建，采用不含裁剪的 `0b23cb978d51` 源码。
新 Control 镜像、migration 99、新的不可变 OutputSpec revision，以及绑定该
OutputSpec 的 release 仍需一并采用。正式 Worker、CPU_MEDIA/thumbnail 图契约、
真实视频 Job 与 APISIX 下载验收尚未完成。
