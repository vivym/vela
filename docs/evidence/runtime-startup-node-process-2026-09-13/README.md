# Node process integration validation (2026-09-13)

The source-matched `internal/nodeagent` test image was run through
`TestRuntimeStartupNodeProcessPostgresTLS` with Docker Desktop and PostgreSQL
17. All three scenarios passed: normal reservation/activation, committed
response loss, and missing `SYS_PTRACE` rejection. The test image was removed
afterward.

This remains validation evidence. It exercises the Node/Fleet/TLS/PostgreSQL
process helper and durable journal/grant behavior, but does not start the
`cmd/vela-node-agent` daemon composition root, issue a production ModelRuntime
Permit, or produce a production Launch Receipt.
