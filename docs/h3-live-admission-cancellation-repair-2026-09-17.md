# H3 live-validation 并发接单与取消恢复修复

本次范围是 `minimax-h3-live-validation` 的两个线上问题：运行中提交第二个
Job 返回 `503 capacity_unavailable`；取消正在运行的 Job 后不能继续接单。
独立的 `minimax-h3` 扩容发布不在本报告的验收范围内。

## 当前发布与真实验收

2026-09-17 10:33 UTC，修正版 V14/AUX V8 已完成原生预热、真实导入检查及
目录发布。新 ExecutionProfile 为 `0e6715a4-7ed5-57db-b609-cbc37b4dd86f`，
cutover 为 `9eee563b-c970-5ef7-a594-3745967f5a12`。模型名、原 Rate Card、
全部原授权项目和独立 `minimax-h3` 路由均通过发布后核验。

本轮真实 API campaign 位于 `.70` 的
`/opt/vela-cluster/h3-busy-cancel-installed-finalizer-20260917`。

- 第一条 `e815ea93-c004-4685-bce3-f1273e2279fa` 在 DiT 连续运行超过 150 秒后取消。
- 第二条 `5624781f-5439-47b5-8b02-0644fe75edb4` 在 DiT 忙时成功返回 HTTP 202。
- 取消接口于 10:36:27 UTC 返回 HTTP 200，第一条随后达到 CANCELED。
- 第三条 `b60fad60-fd7e-4746-abbb-cb7d10bfcd27` 于 10:38:17 UTC 返回 HTTP 202，
  与取消请求相隔约 110 秒；第二条已自动进入 DiT，期间未人工重启服务。

本轮最终验收通过。第二、第三条均已 SUCCEEDED，完整下载并校验了视频、
音轨和缩略图；每条只有一笔 CNY 100 minor-unit（¥1）Charge。取消的第一条
依照既有 CUSTOMER_CANCELLATION 策略收取一次 ¥1，没有可见完成结果或
ArtifactSet，预留金额正确消费。没有修改取消计费策略。

两条完整视频均为 1344×768、24 fps、124 帧，视频约 5.167 秒；双声道 AAC
音轨与容器为 5.175 秒。完整保留原生输出，没有裁成请求名义上的 5 秒。
HTTP 下载后验证对象摘要，另用 ffprobe 核验媒体。本地查看样本来自第二条
任务，下载到本机后的文件摘要与 API/远端验收记录一致。

匿名访问返回 401；两条成功任务分别重复提交同一幂等键两次，仍返回原 Job，
账本没有新增 Charge；修改请求内容但复用同一幂等键均返回 409。整个取消与
后续执行过程中，三个 Pod UID、六个容器 ID 和 restartCount 全部不变。
最终三个 Worker 均为 READY/CONNECTED，活动 allocation 为 0；后续任务的
实际 allocation 也已逐一绑定到本次发布的三个 Worker。

约 109.556 秒是这一轮从取消请求到新请求受理的观测值，包含停止、写入排空、
退休与容量恢复，不能解释为立即恢复或延迟 SLO。本轮验证两个报告的问题及
完整 API/媒体/内部计费闭环，不代替大规模并发、故障域冗余或全部 Production Gates。

最终证据：

- [整轮验收、媒体摘要和 Worker 分配](evidence/h3-live-admission-cancel-20260917/installed-final-evidence.json)
- [鉴权、幂等与计费复核](evidence/h3-live-admission-cancel-20260917/installed-supplemental-verification.json)
- [第二条媒体 ffprobe](evidence/h3-live-admission-cancel-20260917/installed-second-ffprobe.json)
  与 [第三条媒体 ffprobe](evidence/h3-live-admission-cancel-20260917/installed-third-ffprobe.json)
- [发布输入与 Secret 引用](evidence/h3-live-admission-cancel-20260917/installed-release-inputs.json)
- [实际 Python 导入与服务状态](evidence/h3-live-admission-cancel-20260917/installed-publication-verification.json)
- [目录、价格、授权与其他模型路由核验](evidence/h3-live-admission-cancel-20260917/installed-catalog_verification.json)

## 原因与修复

### 运行中无法排队

DiT 的执行租约持续续期，但用于接单判断的空闲容量观测在两分钟后过期。
Admission 因此拒绝第二个 Job，虽然当前 Worker 正常执行且允许排队。

数据库迁移 `00108_stage_busy_queue_admission.sql` 允许使用当前有效分配
绑定的同一条正容量观测进行接单。它检查有效租约及续期会话、Worker
连接和 fencing、非终止 Job/Attempt/Stage 等约束。较新的显式容量撤回
仍生效；调度器不允许把占用的执行槽位再分配一次。

迁移通过事务与 goose advisory lock 发布，schema 从 107 到 108。
迁移文件 SHA-256：
`28e2ed1c2c4b60c5b868d492d2417b13b1b5750f5ed8bc4e4649c4c5fa65cf64`。

### Stop 后 Worker 退出

Stop 关闭持久化 admission gate 后，监控路径曾把 `ErrAdmissionClosed`
作为致命异常返回。修复使 Worker 保留未退休的执行记录，进入终止恢复，
在取得写入停止与临时目录退休证明前不宣告槽位可重用。

### 终止恢复重试拒绝同一截止线

真实取消测试进一步发现 Node journal owner 对 execution floor 的
幂等性只认可完全相同的 protobuf 字节。Control 在每次终止历史查询时
重新签名，签名和有效期变化，但执行截止线仍为 32。journal owner
因此返回 `ModelRuntime execution floor cannot regress`，阻断后续
Runtime drain 和 Worker retirement。

修复在验证签名、有效期、Worker/member/device scope 后，将已被当前
持久截止线覆盖的请求视为无修改的成功。旧截止线与原始证明保持不变；
该结果只证明截止线已覆盖请求，不代表执行已停止、写入已排空或目录
可以删除。Runtime 的 drain 与 Worker 的完整退休证明仍独立验证。

### 终止历史跨节点时差

替换后的完整任务虽然成功，但后续恢复暴露了独立问题：数据库节点的
时钟比 Worker/Control 快几十毫秒，而终止证明的未来时间校验没有任何
容差。每次重新查询都拿到略晚于本机时间的观测，导致持续 `StageAuthority
is stale`，无法恢复下一次接单。

终止证明现在允许最多 1 秒的未来观测偏差，仍严格拒绝过期证明和超过
此边界的时间差。该容差仅针对已签名的终止限制事实，不延长执行租约，
不替代写入停止证明。never-admitted checkpoint 的本地时间保留真实值，
跨节点对比使用相同边界，包含落盘恢复与 member 转发两端。

### 已执行任务的 non-admission 拒绝阻断 drain

V8 的真实测试再次确认忙时提交返回 202，运行中的取消也到达 CANCELED。
但 Worker journal 包装层先尝试记录 non-admission；Node 正确拒绝为已经
admitted 的执行签发“从未执行”证明。包装层把明确拒绝直接返回为 RPC
错误，导致终止协调器未进入已有的真实 drain 恢复分支。

修复只对 Node 明确返回的 `ErrJournalRejected` 进行只读 Runtime 查询。
查询重新验证 scope/身份/签名，只返回已持久化的证明；没有证明时返回
unproven 语义，让协调器继续检查原始 backend 的真实身份、停止与 drain。
不把拒绝本身当作任何证明，也不再次尝试写 journal。超时、丢失写入回复、
混合不确定错误及非法请求继续失败关闭。terminal non-admission 同样处理。

回归测试先得到 `execution journal owner rejected the mutation`，修复后
通过真实 owner → Worker wrapper → Runtime RPC 链；另用生产 Worker
恢复循环确认只有完整 drain/退休后才恢复容量且下一执行真正可 Prepare。
覆盖签名/身份/未知字段、写入结果不确定及从未执行的正常证明路径。
新一轮 `go test ./...` 和三个相关包的完整 race 测试均通过。

原失败回执保留于 `.70`：
`/opt/vela-cluster/h3-busy-cancel-exact-profile-20260917/receipt.json`。
取消 Job `c563dd29-8cb5-4961-893f-1c8aa014a2ca` 已产生一次取消 Charge；
已接收的第二个 Job `97a9784a-746b-44f8-920d-c707471fa1de` 后来在 V9
执行期间失败，原因见下一节。本修复仅改变 Worker 镜像，Runtime/StageProfile/定价不变。

### Control 重连失败与执行授权失效

V9 发布后，原排队任务确实开始 DiT 执行，但在 Control 的另一轮滚动发布
期间断开，最终成为 `WORKER_LOST`。线上前五条心跳间隔约 20 秒，最后
一条为 07:04:11 UTC；新 Control Pod 分别于 07:03:29、07:03:56 创建。
Worker 在 07:05:41 的授权单调有效期截止时返回
`observe ModelRuntime before reattach: StageAuthority is stale` 并退出。
这轮任务 `97a9784a-746b-44f8-920d-c707471fa1de` 是 FAILED，不能作为
完整交付通过的证据，其原始状态和失败记录保留。
独立核账确认该失败 Job 的 Charge 数量为 0，reservation 为 RELEASED；
项目保留金额与仍有效的 reservation 总和一致，证据为
`evidence/h3-live-admission-cancel-20260917/failed-original-billing.json`。

两个独立回归分别复现了失效授权下的 Worker 退出，以及重连临时失败一次
即退出。修复保持重连重试标识；已受理的执行不重复 Prepare/Start。
错误恢复时检查最新持久授权（不是可能落后的内存副本），只有其真实过期
才关闭本地 admission，随后等待 Control 的完整终止证明、真实 Runtime
停止/drain 和退休。RETAIN 不等于释放，不伪造终止，也不删除旧日志。
未来时间、非法签名与 scope 仍不能进入“过期恢复”分支。

V9 campaign 已停止其验收 runner 并保留失败回执。已受理的新测试 Job
`dfbb5eaa-63bd-4ac1-8835-c74de4c95b59` 当时保留，随后已由兼容修复实例成功执行；
未通过修改数据库复活失败 Job，也未静默替换其计费结果。

### 原生 Python 驱动取消后没有主动发布终态

V10 真实验收中，DiT 已执行超过 150 秒，第二个提交返回 202，首个任务
最终 CANCELED；但随后恢复日志持续为 `live backend writer drain is unproven`。
GPU 已停止计算、原生取消异常已返回，不能因此推断写入证明成立。

核对部署镜像中的实际驱动源码后发现：`_DriverSession._refresh()` 只在
普通 Status 路径推进取消终态。执行授权已经关闭后，恢复协调器按设计
使用只读 inspection 和独立 drain；两者都不会执行 `_refresh()`，所以
已完成的 Future 仍保留 CANCELING，无法取得真实 drain。

新增 `hack/h3_cancel_drain_contract_test.py` 直接运行部署版本的 Python
驱动，在 0.003 秒内稳定复现。修复在 Cancel 时向现有单线程执行器追加
取消收尾工作；只有计算 Future 实际结束、清理完成后，才在命令锁内发布
STOPPED。inspection/drain 继续保持有界且不执行 CUDA join 或文件清理。
shutdown 的等待移到命令锁外，避免等待同一个锁造成死锁。取消后写入
收尾失败仍不能 drain。覆盖取消前 Future 已完成、取消后才完成、未知
执行身份和序号、重复 drain、写入失败，以及 shutdown 收尾。

可复现补丁与前后 SHA-256 位于 `deploy/h3-runtime-patches/`。该变更生成
新 Runtime 镜像与 CANARY StageProfile，实际原生 warmup 后才允许认证。
V10 失败回执保留为 `expired-reconnect-failed-receipt.json`，不标记通过。

### 取消与 Admission 并发导致数据库死锁暴露为 HTTP 500

V10 的取消后提交在 07:51:46 UTC 遇到 PostgreSQL `40P01`：
`lock Stage graph READY capacity path: ERROR: deadlock detected`。
此前 Admission 未重试已被数据库确认回滚的事务，直接返回 HTTP 500。

修复以原幂等键重试整笔 Admission 事务，最多 3 次，仅接受 PostgreSQL
明确的 `40P01` / `40001`；不重试网络错误或结果不确定的 Commit 回复。
持续冲突返回可重试的 503。真实 PostgreSQL/API 回归先复现 500，再确认
连续两次事务中止后仅生成一份 Job、图、Attempt、幂等记录和额度预留；
重放无新增副作用。另验证重试上限和其他数据库错误不重试。

V10 期间恢复的原任务 `dfbb5eaa-63bd-4ac1-8835-c74de4c95b59` 已真实
SUCCEEDED，完整视频、音轨、缩略图及一次 CNY 100 minor-unit Charge
通过验收，证据为 `recovered-original-job-receipt.json`。V10 新接收的
`2a45c102-85c7-44c1-a682-dcc2fb7f2c3d` 保留原不可变配置，由兼容 V11
实例继续执行；不能把它静默改成新 StageProfile。

## 验证与证据边界

- 迁移的六个真实数据库/API 测试在当前完整迁移集上再次通过，覆盖运行中排队、租约过期/撤销、显式
  撤回、断连和队列上限；拒绝请求不留下 Job 或额度副作用。
- floor 回归测试先复现失败，再验证 Node owner 在实时和重开日志两种
  情况下接受重新签名的同一截止线；错误签名、scope 和过期证明仍拒绝。
- Worker → Node journal → Runtime RPC 测试覆盖重签重试和较低截止线
  请求，确认不降低当前截止线、不进入 backend。
- 本地相关包测试、race 检查、`go vet` 与 `go test ./...` 通过。
- `.70` 上 Linux root 环境执行 modelruntime 的 Journal 测试通过。

已经完成的真实 API Job `4d1bc058-c0ee-4b75-b060-75fbc805ee46`：
完整视频与音频可下载；视频 124 帧、24 fps、约 5.167 秒，音频和容器
5.175 秒；产生一次 CNY 100 minor-unit Charge，幂等重放不重复计费。
回执位于 `.70` 的
`/opt/vela-cluster/h3-api-acceptance-20260917-stop-recovery/run/receipt.json`。

首次取消验收中，第二次提交已返回 202；取消 Job
`63c1d429-c7fa-4a8f-af2b-8ff6d4dfe7c5` 到达 CANCELED 并产生一条
CUSTOMER_CANCELLATION Charge，但该轮验收随后暴露 floor 重试问题。
该失败回执保留，不能作为取消后可接单的成功证据。

截至 2026-09-17 08:37 UTC，Control 的 Admission 事务重试修复已发布到两个
健康副本，回执为 `control-publication-verification.json`。V10 取消 campaign
失败回执仍保留。兼容 V11/AUX V6 已把原排队任务
`2a45c102-85c7-44c1-a682-dcc2fb7f2c3d` 执行到 SUCCEEDED；通过 API 下载完整
视频/音轨与缩略图，并验证一次 CNY 100 minor-unit Charge，回执为
`recovered-queued-job-receipt.json`。

在既有用户任务全部自然结束后，V11/AUX V6 已撤回并按实际容器退出证明
退休。V12/AUX V7 完成原生 warmup 后发布，但真实取消验收再次失败。
当时核验了 Worker 二进制和源码目录中的 Python 文件，没有验证 Python
实际导入的模块，因此那份 publication verification 不能证明驱动修复已生效。

AUX V7 的初次 host unit 错误引用 V6 的 EnvironmentFile，启动校验因旧 Pod
不存在而拒绝。已保留错误 unit 和 journal，仅修正新 unit 的路径，并解除
systemd 启动频率限制；未清空或重建 Worker/Runtime journal/startup identity。
此前新 Pod 尚未启动，修正后所有容器首次运行，restartCount 均为 0。
该 campaign 位于 `/opt/vela-cluster/h3-busy-cancel-finalizer-20260917`，
回执为失败。所有旧失败 campaign 与原始日志保留，不覆盖失败记录。

### V12 打包路径错误与实际导入校验

运行时实际导入 `/opt/venv/lib/python3.12/site-packages/fast_h3/vela/driver.py`，
V12 只修改了 `/opt/fast-h3/src/fast_h3/vela/driver.py`。两份文件本来就不同：
安装版还包含此前的执行授权续期处理，不能用旧源码整份覆盖安装版。

修正以实际安装版为基线，仅增加取消收尾与 shutdown 锁顺序调整，再同步
两份文件。针对安装版先复现回归失败，再验证三个测试通过。构建新镜像时，
使用镜像内 `/opt/venv/bin/python` 验证 `fast_h3.vela.driver.__file__`、文件
摘要，并在镜像内执行同一组回归测试。部署后还必须再次检查实际导入结果。

修正后 Runtime 为
`sha256:71bd111548526028064af17ed575b04dfb4a8b733a0306ec433208e52f6b3f95`，
实际驱动 SHA-256 为
`4c3df85e0e2bf14556d635e1a3bc5539e0a8be8731fe1f2f0f2e40a6dbaa859e`。
V12 原补丁和元数据保留为 `v12-source-only-cancel-finalization.*`。

V12 已接收的 `b6db79fd-e5a6-49e3-9893-a287b0627be9` 保留其原始执行配置。
恢复实例 V13 曾因 Fleet 发布失败后过早调用 bootstrap 留下未完成 intent；
没有启动容器。其日志保留，bundle 已撤回、Worker 已 fenced，startup-gated
Pod 因 Registry 无完整 bootstrap receipt 拒绝删除而保留隔离，不能伪造 receipt
或清空目录。确认 Pod 从未调度、ResourceClaim 没有设备分配后，仅释放其
未使用的 ResourceClaimTemplate；原 Pod、未分配的 claim 与 intent 保留。

V15 已把该排队 Job 执行到 SUCCEEDED，通过完整音视频、缩略图下载及一次
CNY 100 minor-unit Charge 校验，证据为 `v15-recovered-queued-job-receipt.json`。
当时 V14/AUX V8 留作修正镜像的最终发布，尚未通过完整取消自动恢复验收；
随后最终结果见本文开头。

## 发布材料与回退边界

本次已自行生成替换实例的 release bundle、manifest、ConfigMap 与使用
现有 workload CA 签发的 member TLS Secret，无需用户补充私钥或发布材料。
私钥只保存在管理节点的受限目录和 Kubernetes Secret，不进入 Git。

- DiT V14：`.12`，Worker `f8ff3c79-f094-5cf8-9ea3-b37f6cf55b83`。
- Encoder/VAE V8：`.11`，Worker `734acfb8-90b2-5122-9b2b-47a94a97361f`。
- Thumbnail V8：`.11`，Worker `979d6984-9d1f-5e5f-be19-732d18824d42`。
- Node Agent：
  `064d64b9d74d6b1299d02418c632bb200a5e345a0ea09bf005871dfb331f911c`。
- Worker Agent 镜像：
  `10.1.201.70:5005/vela-stage-worker-agent@sha256:a4896ac14b7280bd2bb203e915e66c9708f10eb6e8ac261ea3aed22dc56834f2`。
- Worker Agent 二进制：
  `177d5598a863beefc514105b978a13634e6f35a78114658c8259ac8c76c75833`。

Fleet 更新每次读取当前 ConfigMap，保留其他发布中的 rollout，仅撤回
被替换 bundle、追加新 bundle，并对 Deployment resourceVersion 作 CAS。
旧实例先停止新调度，检查零活动分配，再 fencing、停止容器；核对 CRI
与 containerd 中旧进程均不再运行后才移除 Pod finalizer/旧 GPU claim。
原始执行日志、bootstrap 历史、旧二进制和发布前 Fleet 快照全部保留。
没有重启主机。

V14/AUX V8 于 10:22 UTC 首次启动，三个实例均保持容器 restartCount 0。
`installed-publication-verification.json` 记录 Worker 二进制摘要，以及两个
GPU Runtime 内真实 Python import 的 `__file__`、SHA-256 和 systemd 状态。
发布前先按 Admission 的相同顺序锁住该 ExecutionProfile 的容量池，
在同一事务中检查零非终止 Job、零活动 allocation，再 fence 三个旧 Worker，
避免“检查后又接入新任务”的切换窗口。之后才停止容器并核验真实退出。

回退不能重新启用已经 fenced 的 Worker identity 或删除日志重新初始化。
若修复版本故障，应保留现有证据，按相同发布流程创建新的受批准身份。
迁移回退到 107 会恢复忙时排队限制；若已发布更高版本数据库迁移，须先
核验依赖，不能直接执行旧 migration 的 Down。

本地测试日志与非敏感发布输入位于
[evidence/h3-live-admission-cancel-20260917](evidence/h3-live-admission-cancel-20260917)。

### Runtime 镜像引用修正

V5/AUX V2 的 Runtime 引用了包含 provenance 的 OCI index，Node 在释放
startup gate 前拒绝，错误为 `image manifest is not a bounded exact runtime image`。
这三个 Pod 没有被调度，也没有启动过容器。新发布 V6/AUX V3 使用同一构建的
Linux/amd64 子 manifest；evidence-template 与实际 Runtime 引用一同更新，
不降低启动校验要求。索引摘要保留在 deployment inputs 的 `index_image`。

GPU Runtime：
`sha256:5bbb3a6306849da82f649703b241b757207b393dc558e09ef035ac0f8ad4e6ff`。
CPU Thumbnail Runtime：
`sha256:455e06129ee7665e18019e873b36d30efd622b61d9bf7865ad6d46ecfa047280`。

### StageProfile 与实际 Runtime 一致性

V6/AUX V3 完成四个原生组件的 warmup 后，Worker 注册仍被拒绝：
原 StageProfile 固定旧 Runtime 镜像，新 residency 的真实镜像不同。
本次没有修改不可变的旧 StageProfile，也没有伪报旧镜像。以实际组件
warmup 回执为依据创建四个新 StageProfile，用 V7/AUX V4 的完整新
plan/bundle/worker/launch identity 引用，再生成新 ExecutionGraph、
ExecutionProfile、RateCard 和同名模型 cutover。原路由的项目授权与
价格保留，独立 minimax-h3 路由通过当前状态 CAS 保留。

组件回执 SHA-256：
`bf20095e137dc52627b053aab4cf6633e19b41fa0adf86fe495ecbcc1ef16c39`。
范围仅为真实组件初始化与原生 warmup，不代表生产 gates 或组合 API 验收。

旧测试 Job `0def2bc9-8538-47ba-94b9-389e9f272d76` 固定旧目录，尚未执行。
已通过 API 取消这一条本次创建的验收任务，原幂等键、state.json 和失败回执
保留在旧 campaign 目录。这不能作为运行中取消恢复通过的证据。其他模型
的运行 Job 未取消。新 campaign 使用独立目录与幂等键：
`/opt/vela-cluster/h3-busy-cancel-exact-profile-20260917`。

### 系统自动更新触发的意外服务重启

2026-09-17 06:11:38 UTC，`.12` 的 unattended-upgrades/needrestart
在升级系统库后重启 Vela policy issuer 与 DiT 服务。V7 DiT 在启动数秒后
被 SIGTERM，Node 停止持有的 Runtime（容器退出 137）；再次启动因已有
startup publication 目录被安全拒绝。没有主机重启，也没有删除日志或
复用旧身份绕过检查。只有该 DiT 以 V8 新身份替换，AUX V4 保持运行。

在 `.11/.12` 配置 `/etc/needrestart/conf.d/99-vela-controlled-restart.conf`：
`$nrconf{override_rc}->{qr(^vela-.*\.service$)} = 0;`。真实加载 needrestart
配置并验证规则匹配，保留安全更新，只让 Vela 服务通过受控 rollout 重启。
规则与主机日志证据位于 `.70` 的 `h3-terminal-skew-20260917` 目录。

## 后续发布的必检项

1. Runtime 固定具体平台的 manifest digest。若构建带 provenance，保留父
   OCI index 作为构建记录，但不把父索引用作 Node 启动的 exact Runtime。
2. 同一组件的 StageProfile、WorkerBundle、LaunchManifest、Node evidence
   template 必须绑定同一 Runtime digest。镜像变化时生成新不可变目录版本，
   不只替换 Deployment 镜像，也不伪装成旧镜像。
3. 先使用真实原生 warmup 取得组件证据，再创建对应目录版本；组合 API、
   音视频、取消和计费使用单独验收回执，不能由组件 warmup 代替。
4. Fleet 合并当前所有 rollout 并以 resourceVersion CAS 更新，保留其他
   模型路由和正在执行的工作。先撤回旧 bundle，再基于活动分配、Registry
   fencing 和真实容器退出证据退役旧 Pod，最后清理无引用的 GPU claim。
5. 检查 needrestart 排除规则和 Vela systemd 自启动。不能对带持久启动
   身份的服务执行无条件 restart，也不能删除 startup/journal 目录来重用身份。
6. 真实验收同时记录 API 状态、请求 ID、Job、完整媒体与 ffprobe 结果、
   Charge/余额保留一致性，以及取消前后的容器 ID/重启次数。
7. Python 镜像修复必须核验实际解释器的模块 `__file__` 与摘要，并在镜像内
   运行回归测试；源码目录存在同名文件不代表运行时加载它。
8. Fleet 发布命令成功且目标已注册后才执行 bootstrap。发布归档用 Python
   tarfile，排除 macOS AppleDouble 文件，避免 `._configmap.json` 触发错误。
