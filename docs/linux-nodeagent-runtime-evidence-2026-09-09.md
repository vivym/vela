# Linux Node/Runtime test binary evidence

日期：2026-09-09。宿主为 Darwin，但使用 `golang:1.26-bookworm` 的
`linux/arm64`、`--privileged` 容器，将 test binary 输出到可执行 workspace 路径
后直接运行。直接运行是必要的：当前 Docker 沙箱不能 exec Go 在临时目录生成的
helper binary。

执行流程：

```bash
GOTMPDIR=/go/tmp go test -race -c \
  -o /src/nodeagent-linux.test ./internal/nodeagent
/src/nodeagent-linux.test -test.count=1 -test.timeout=10m -test.v
```

结果：`PASS`。完整 `internal/nodeagent` Linux test binary 运行成功，覆盖的实际
Linux 语义包括：

- journal server cancellation、overload、timeout、listener failure 和 Worker
  barrier recovery；
- root-owned Node journal、non-root PID-1 Runtime、read-only endpoint、single-use
  grant transition、真实 RPC prepare/start/seal/drain；
- Unix packet channel round-trip、private PID namespace identity、frame/request
  bounds、peer UID/GID、socket inode/mode/ancestor 反例和 reply lifetime；
- CRI/container caller correlation、observer identity、boot ID、socket replacement、
  task/pod/namespace mismatch；
- bootstrap publication 的非 root read boundary、并发、内容/权限/链接替换、
  SIGKILL recovery 和 custody loss；
- startup ledger 的 owner exit、容量上限、uncertain append、wrong route、legacy
  writer、Fleet loss 和 reservation crash recovery；
- runtime task mechanism/options policy、launch plan canonical binding、Node TLS
  identity 和 server replay/conflict handling。

输出中仍有少量显式 `SKIP`，它们要求另一个外部装配：native exec observer/actual
CLI sandbox、host PostgreSQL/TLS orchestrator、CRI Node Agent binary 或专用 observer
probe。它们不能被当前 fixture 测试替代，也没有被计入 PASS。

这份证据将 Linux runtime 行为从“仅可编译”提升为“在特权 Linux 容器中实际运行并
通过”；生产 Kubernetes/containerd 装配、真实 GPU 和 Production Gates 仍未闭合。
