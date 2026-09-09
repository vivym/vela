# Linux validation boundary on the current host

当前开发宿主是 `darwin/arm64`。因此本地默认 `go test ./...` 不会编译
`*_linux.go` / `*_linux_test.go`，包括 pidfd、SOCK_SEQPACKET、ptrace、namespace、
Node/Runtime startup 和 observer custody 路径。

本轮使用 `golang:1.26-bookworm` (`linux/arm64`) Docker 容器执行了两种检查：

```bash
docker run --rm -v /Users/viv/projs/vela:/src -w /src \
  golang:1.26-bookworm bash -c 'GOTMPDIR=/go/tmp go test -race ./internal/nodeagent'

docker run --rm -v /Users/viv/projs/vela:/src -w /src \
  golang:1.26-bookworm bash -c 'GOTMPDIR=/go/tmp go test -c -o /src/.linux-test-artifacts/nodeagent-linux.test ./internal/nodeagent'
```

Linux 编译检查成功，生成的 test binary SHA-256 为
`a119876d3f4f12523d1bd66524eaa5ffd68bb0b598fa2ce03f48769408cb7a1e`。

Linux 运行检查没有形成有效测试证据：测试需要 fork/exec 自身 helper binary，
Docker 环境对这些 exec 返回 `operation not permitted` 或 `permission denied`；
加入 `--security-opt seccomp=unconfined --cap-add=SYS_PTRACE` 后仍然失败。这是
当前执行环境限制，不是代码通过，也不能归类为代码失败。

因此当前证据边界是：Linux 源码可交叉编译，Linux 进程/权限/namespace 语义仍需
真实 Linux host 或允许 exec/ptrace 的 CI runner 执行。Production Gates 继续保持
**0/9**。
