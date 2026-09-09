# Database role boundary evidence

日期：2026-09-09  
范围：PostgreSQL migration/schema 95 与所有声明的 database role boundary。

## 执行命令

```bash
go test -tags=integration ./internal/integration \
  -run '^TestDatabasePoolsFailClosedOnRoleConfusion$' -count=1 -v
```

## 结果

通过，退出码为 `0`，耗时约 `5.8s`。测试在全量 migration `00001` 至 `00095`
完成后，逐一验证声明的 service role privilege boundary，并覆盖 Fleet、
StageWorkerControl、StageScheduler、StageArtifact、AttemptCoordinator、Usage/Cost
Ledger、H3 campaign evidence 以及跨 role confusion 组合。Fleet 当前精确边界已与
schema 95 的最终授权一致。

这次重跑确认此前 `vela_fleet` privilege drift 修复仍然有效；它只证明当前 fixture
数据库上的角色边界检查，不证明真实部署的 secret、网络、TLS 或生产连接池装配。
Production Gates 仍为 `0/9`。
