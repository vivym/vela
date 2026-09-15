# Git、Helm 与 Argo CD

GitLab 已存在，部署及对接按用户要求暂缓。Argo CD v3.5.3 已安装；两个
Project 的 `sourceRepos` 默认空，repo-server 没有外部 Git 的放行规则或凭据。
没有接入真实仓库前，不创建虚假的生产 Application。

## 发布过程

应用发布者在既有 GitLab 提交代码/chart/配置变更。GitLab CI 负责检查与构建，
以受限仓库身份推送 digest 镜像；该身份不拥有 Kubernetes 写权限。
审批人审核后在 Argo 同步或回滚本项目。Argo 使用自身 ServiceAccount 写入目标
namespace；人和旧 CI 不再通过共享 kubeconfig 直接 `helm upgrade`。

Argo 渲染 Helm chart，不依赖 Helm release Secret。密码、provider key、TLS 私钥
由平台管理，应用仓库只保存批准的引用；镜像拉取 Secret 不能被应用挂载读取。

## 接入真实仓库时的平台操作

1. 核对既有 GitLab 地址、仓库只读凭据、服务端 CA、protected branch 和 MR 审批设置。
2. 只放行 repo-server 到指定 GitLab 地址/端口；Secret 由平台注入，不放进 Git 或日志。
3. 把准确 repoURL 加入对应 Project，创建固定 `project/repoURL/path/destination` 的 Application。
4. 指定 digest 镜像和 revision，验收首次同步、业务健康和回滚。

AppProject 限制仓库、namespace 和资源类型，**不能限制 Git path**。
路径固定依赖平台创建且团队无权编辑的 Application。
审批人可以选择仓库中的 revision；Argo 的 sync 权限不等于强制验证该 commit 已经
通过 GitLab MR 审批。该门禁待既有 GitLab 对接时验证。

## CI 检查

CI 应执行 Helm lint/template、schema/digest/资源预算/Pod 安全检查和变更比较。
服务端 dry-run 由平台受控验证入口执行，不应为 CI 恢复生产 namespace 写权限。
检查 Deployment 时还要检查其 Pod 模板：不安全的模板可能在 ReplicaSet 创建 Pod 时
才被准入拒绝。

## 失败和回滚

本次使用临时只读 Git fixture，已经实际验证两个 namespace 同步、故意失败的
Deployment 升级和审批人回滚。临时仓库服务、Application、工作负载和身份在结束
时清理。它不承担生产 Git 服务，也不证明真实业务的数据库迁移/PVC 格式兼容。
