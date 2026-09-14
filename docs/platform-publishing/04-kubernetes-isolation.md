# Kubernetes 隔离与准入策略

## 隔离边界

Namespace 是资源命名边界，不是内核级安全边界。应用必须使用独立 namespace、ResourceQuota、LimitRange、Pod Security Admission、NetworkPolicy 和审计策略。恶意租户若拥有节点 root、cluster-admin 或可写准入策略，仍能破坏集群。

## 工作负载安全

准入策略需要覆盖 Pod 的 initContainers、ephemeralContainers 和 containers，并拒绝 hostNetwork、hostPID、hostIPC、hostPath、hostPort、privileged、危险 capability、任意 ServiceAccount token、任意 nodeName、NodePort/LoadBalancer 和控制存储节点选择。表达式必须对字段缺失安全处理，并用实际拒绝用例测试。

## 调度规则

CPU 管理服务选择 `vela.ai/control-plane-tier=cpu`，模型推理和模型相关 CPU 服务选择 `vela.ai/node-role=gpu-worker`。控制面和存储节点使用 taint/toleration 与 required node affinity；应用团队不能写 nodeName 或修改节点标签。

## 网络策略

默认拒绝 ingress；只允许 APISIX 到应用端口、同一应用的明确依赖、Prometheus/OTel 抓取和必要的 DNS/外部 provider egress。策略发布前要验证服务发现、数据库、NATS、MinIO、日志和指标路径，不能用一条“只允许网关”的策略误伤运行时。

## 存储和 Secret

应用只能申请批准的 StorageClass 和容量；Longhorn/MinIO 的复制数、容量和节点选择由平台根据当前余量批准。Secret 使用外部 Secret/PKI 流程或平台 materializer，发布者只引用 Secret 名称，不能读取其他团队的 Secret。

## 验收命令

对每个角色执行 `kubectl auth can-i --list --namespace <ns>`，并测试以下操作均被拒绝：读取 Secret、创建 RoleBinding、创建 NodePort、访问其他 namespace、`pods/exec`、设置 hostPath/privileged、修改 Quota/NetworkPolicy。成功用例必须覆盖正常 Helm chart、滚动更新、探针、监控抓取和依赖访问。
