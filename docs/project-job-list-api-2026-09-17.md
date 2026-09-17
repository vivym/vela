# 项目 Jobs 列表接口（2026-09-17）

新增 `GET /v1/projects/{project_id}/jobs`，支持中转站查看项目内排队、生成和其他
未结束任务。入口前缀仍为 `https://vela.marslab.ic/api`，现有 `jobs:read` Key 可用。
调用参数和分页示例见 [API 接入指南](api-integration.md#41-列出排队中生成中及其他未结束任务)。

## 行为

- `active=true` 返回 QUEUED、ASSIGNED、RUNNING、FINALIZING、RETRY_WAIT、CANCELING。
- `active=false` 或省略时列出所有状态；`state` 可指定一个精确状态。
- `active=true` 与终态 state 组合返回 400。
- `limit` 默认 50，范围 1–100；下一页使用 `next_cursor`，末页不返回该字段。
- 结果按 `(created_at DESC, job_id DESC)` 排序，空结果为 `{"jobs":[]}`。
- 列表沿用单任务 Job 投影，并补充稳定模型名称 `model`；不读取或返回 prompt、
  client_metadata、凭据或签名下载链接。

游标绑定组织、项目与筛选条件，是翻页位置而非授权凭据。每一页都重新鉴权，
通过真实数据库请求上下文及 RLS 隔离项目。状态是实时读取，不保证跨页冻结快照；
新增任务需要刷新第一页，状态变化可能使某任务进入或离开筛选结果。

## 实现与权限

Admission 使用一次数据库查询读取 `limit + 1` 条，以确定是否还有下一页。
分页依据不可变创建时间及 UUID，不使用 OFFSET。迁移 110 增加全量与非终态的
项目排序索引；迁移 111 将模型名称加入已有的 `vela_request_job_runtime`
受限视图，不向请求角色开放模型目录表。

首次发布在启动验证时被最小权限检查拒绝：迁移 110 的直接模型目录列授权超出
`vela_request` 精确权限边界。旧副本仍可服务；随即撤销该授权、回退镜像并恢复
两个旧副本。迁移 111 保留项目隔离视图的现有授权边界，修复后再发布。
保留已经应用的 110 和追加的 111 历史，不改写迁移记录。

升级须应用到 111；不能停在首次候选 110。若需回退应用，可回到此前 Control 镜像
并保留 111 的兼容视图和索引。111 的 Down 不重新授予已撤销的目录访问权限。

## 验证

- `go test ./...` 通过；最终 Admission/HTTP 定向测试和 `go vet` 通过。
- 真实 PostgreSQL 测试覆盖全部 9 个状态、空列表、分页、相同时间戳、翻页期间
  插入、项目和组织隔离、参数错误、不同筛选条件/项目的游标、匿名、缺失 scope、
  已撤销凭据。
- 新增启动权限回归先复现拒绝，再验证受限视图修复通过。
- 最终隔离构建源运行 `TestProjectJobList.*`、原单任务提交/读取，以及
  `TestDatabasePoolsFailClosedOnRoleConfusion`，全部通过。
- OpenAPI 导出一致性和文档链接检查通过。

发布只更新 Control 服务，没有为验证启动 GPU 任务。线上验收使用既有内部验收
项目的真实历史任务、中转 Key 和两个 HTTPS 入口，具体结果见随附部署回执。

## 线上结果

2026-09-17 07:05 UTC 完成发布，Control 两个副本全部 Available，数据库版本 111。
07:06 UTC 分别经 `.70/.71`、保留域名 TLS 校验完成真实只读验收：

- 两个项目的 active、QUEUED、RUNNING 和全量列表均返回 200。
- 内部验收项目的两条历史任务可分两页读取，无重复，单任务查询与列表投影一致。
- 匿名返回 401，跨项目返回 403，非法参数及跨筛选游标返回 400。
- 中转项目当时有 3 条历史记录、0 条非终态记录；验收没有新增 Job 或费用。
- 请求角色仍没有模型目录的直接列读取权限。

Control 镜像：
`10.1.201.70:5005/vela-control@sha256:517aed8b54ea1f2226693240cfa2d01e4ace1353e4d60b959d2df20aaf95ba49`。

证据：[部署与双入口验收](evidence/project-job-list-20260917/deployment.json)。
这是列表 API 与访问隔离的验证，不是新的模型生成、吞吐或 Production Gate 验收。
