# Git、Helm 与 Argo CD 发布流程

## 组件职责

Git 托管服务（GitLab、GitHub、Gitea 或 Forgejo）提供仓库、Pull Request/Merge Request、保护分支、审批和审计。Helm 只负责将应用模板渲染成 Kubernetes 资源。Argo CD 读取被批准的 Git revision，并把声明同步到指定 namespace。

Argo CD 接管一个应用后，不要再用人工 `helm upgrade` 写同一组资源。回滚使用 Git revert 或 Argo CD revision sync，避免两个控制器互相覆盖。

## 建议仓库布局

```text
llm-api-chart/
  Chart.yaml
  templates/
  values.yaml
platform-environments/
  apps/llm-team-a/validation/llm-api.yaml
  apps/llm-team-a/production/llm-api.yaml
  projects/llm-team-a.yaml
```

应用 chart 放模板；环境仓库只放经过评审的 chart 版本、镜像 digest、资源和非敏感配置。密码、provider key、TLS 私钥只放 Secret 管理系统，Git 中保存引用名和校验信息。

## CI 门禁

CI 至少执行：

```sh
helm lint charts/llm-api
helm template llm-api charts/llm-api -n llm-team-a-validation -f values.yaml > rendered.yaml
kubectl apply --dry-run=server -f rendered.yaml
```

随后执行镜像漏洞扫描、digest 检查、资源配额检查、Pod 安全检查、Service 类型检查和 Helm diff。生产分支必须启用强制评审、状态检查、禁止直接 push、签名提交或等效的供应链控制。

## Argo CD 边界

每个团队使用独立 Argo Project：只允许访问该团队的 Git 路径和 namespace，只允许白名单资源类型；生产 Application 由平台创建，团队不能修改 destination、sync policy 或 project。Argo ServiceAccount 不应拥有 cluster-admin。

## 临时验证发布

在 Argo CD 安装前，平台管理员可以执行：

```sh
helm upgrade --install llm-api ./charts/llm-api \
  --namespace llm-team-a-validation --create-namespace \
  --values env/validation.yaml --atomic --timeout 15m --history-max 10
```

该命令不会创建 APISIX 外部路由。验证完成后，应删除临时 release，并改用 Argo 管理生产资源。

## 失败和回滚

`--atomic` 在升级失败时回滚 Helm release；Argo 管理的应用应通过 Git revert 回滚。回滚前确认数据库 schema、PVC 数据格式和模型缓存是否兼容，不能只看 Deployment 是否 Ready。
