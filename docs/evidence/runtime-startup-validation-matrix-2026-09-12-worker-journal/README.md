# Worker journal socket mount validation receipt

目标机：`marslab@100.111.196.116`，Ubuntu 24.04，kernel `6.8.0-137-generic`。

本目录的 `matrix.json` 和六个场景 receipt 来自当前 launcher 和
`hack/run-runtime-startup-validation-matrix.py`。本次矩阵的 signed Pod 同时包含
`model-runtime` 和 `stage-worker-agent`；后者声明 canonical
`VELA_WORKER_JOURNAL_SOCKET`，launcher 将其 parent directory 以 read-only host mount
映射到 Worker container，并注入同一 socket path。

结果：

| 场景 | outcome | cleanup |
| --- | --- | --- |
| `normal` | `completed` | `true` |
| `caller-replacement` | `failed` | `true` |
| `observer-channel-loss` | `failed` | `true` |
| `policy-response-loss` | `failed` | `true` |
| `helper-timeout` | `failed` | `true` |
| `helper-crash` | `failed` | `true` |

这证明 validation-only launcher 的 Worker socket mount contract 与 CRI workload cleanup
一致；它没有启动真实 Node composition、Fleet reservation、Node-owned journal server
或 ModelRuntime Permit，因此 `Production Gates` 仍为 `0/9`。
