# pidfd broker deployment evidence

日期：2026-09-12。目标机：`marslab@100.111.196.116`，Ubuntu 24.04，Linux
`6.8.0-137-generic`，amd64。

本 receipt 只证明 legacy anonymous-inode pidfd 的 host broker 已完成一次真实部署和
跨 namespace transport 验证。它不证明 Runtime startup composition 或 Production Gates。

部署内容：

- `/usr/local/bin/vela-pidfd-broker`
- `/etc/systemd/system/vela-pidfd-broker.service`
- `/etc/vela/pidfd-broker.env`
- `VELA_PIDFD_BROKER_SOCKET=/run/vela/pidfd-broker.sock`
- `VELA_PIDFD_BROKER_RUNTIME_GID=65532`

部署后状态：

```text
systemctl is-active:  active
systemctl is-enabled: enabled
socket owner:        uid=0 gid=65532
socket mode:         0660
socket type:         Unix socket
binary sha256:       d868ed2e60d22e05ecfd4f74bb8401e065dea8400589e9fd8b0a45f03c7d1345
```

Linux amd64 native external-socket test：

```text
TestPIDFDBrokerExternalSocket/same          PASS
TestPIDFDBrokerExternalSocket/different     PASS
TestPIDFDBrokerExternalSocket/nested-same   PASS
TestPIDFDBrokerExternalSocket/wrong-gid     PASS
```

该测试通过真实 `/run/vela/pidfd-broker.sock`，以 `SCM_RIGHTS` 传递已保留 pidfd，未
序列化或重新打开 numeric PID。`nested-same` 使用独立 PID namespace；`wrong-gid`
确认非授权 GID 被拒绝。

另外验证了 systemd restart 后 socket 重新发布，replacement pathname 不会被旧实例
删除。后者修复了 `net.UnixListener` 默认 unlink-on-close 与 inode identity cleanup
冲突的问题；对应测试为
`cmd/vela-pidfd-broker/TestBrokerDoesNotUnlinkPathnameReplacement`。

broker 在 Node startup composition 中仍需由 Node 配置显式引用；broker 本身不能替代
生产 helper、signed plan、Fleet reservation、Node journal 或 ModelRuntime Permit。
