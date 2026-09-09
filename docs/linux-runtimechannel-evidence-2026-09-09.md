# Linux runtimechannel evidence

日期：2026-09-09。使用特权 `linux/arm64` 容器编译并直接运行 test binary：

```bash
GOTMPDIR=/go/tmp go test -race -c \
  -o /src/runtimechannel-linux.test ./internal/runtimechannel
/src/runtimechannel-linux.test -test.count=1 -test.timeout=5m -test.v
```

结果：`PASS`。

覆盖：

- `pollLivePIDFD` 在 signal interruption 后重试，不隐藏目标退出；
- 实际进程退出被 pidfd 正确观察；
- descriptor error 与 live/exit 状态区分保持 fail closed；
- process helper 本身在 Linux 容器中成功运行。

这是底层 pidfd 轮询包的运行证据；它不替代 Node/containerd 生产装配、真实
observer 创建关系或 Production Gates。
