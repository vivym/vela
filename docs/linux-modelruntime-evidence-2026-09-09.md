# Linux ModelRuntime test binary evidence

日期：2026-09-09。使用特权 `linux/arm64` Go 容器将
`internal/modelruntime` 的 `-race` test binary 输出到 workspace 后直接运行：

```bash
GOTMPDIR=/go/tmp go test -race -c \
  -o /src/modelruntime-linux.test ./internal/modelruntime
/src/modelruntime-linux.test -test.count=1 -test.timeout=10m -test.v
```

结果：完整 test binary `PASS`。

覆盖的实际 Linux 行为包括：

- process backend inherited pipe、child writer、blocked request/stderr、escaped
  child、unexpected exit、timeout 和 initialization cleanup；
- backend inspection/drain failure 仍保留 resident command 和 cancellation；
- durable journal response binding、uncertain/rejected/ambiguous/trailing wire；
- allocation authority、execution discovery、exact historical scope、lost RPC
  response replay 和 replacement/runtime epoch fencing；
- startup bootstrap consumption、journal owner API、read-only/terminal routes、
  watchdog/cancel interruption；
- durable execution state 的 ownership lock、replacement detection、uncertain
  rename、SIGKILL recovery、history bound 和 topology binding；
- terminal drain、non-admission、sealed receipt replay、corruption rejection、
  retired allocation re-entry rejection；
- watchdog/cancel 对 blocked process 与 child writer 的实际停止。

显式跳过的项目仅包括外部 Fast H3 Python driver conformance，以及需要 non-root
filesystem permission 的单项测试；它们没有被计入 PASS，也不影响本报告列出的
Linux runtime 结果。

这份证据与 [Linux Node/Runtime evidence](linux-nodeagent-runtime-evidence-2026-09-09.md)
共同覆盖了大量真实 Linux 进程和持久状态语义；生产 Kubernetes/containerd
装配、actual CLI observer、host Fleet orchestrator、GPU 和 Production Gates 仍未闭合。
