# Runtime channel liveness checks interrupted by signals

The Node-private journal CPU prototype exposed a production channel availability
bug on the `68491ab` baseline. With four actual UID/GID 10001 processes issuing
authenticated floor updates to a root broker, valid callers were occasionally
rejected. A diagnostic-only build captured the actual Node rejection twice:

```text
runtime caller kernel identity is untrusted or no longer live:
prototype diagnostic pidfd poll count=-1 error=interrupted system call
```

The signal source was not traced. The observation proves `EINTR`, not which
signal caused it. No unsafe permission or identity substitution was observed.

## Repair

`runtimechannel.PollLivePIDFD` retries only `EINTR` on the same nonblocking pidfd
check. Both Node caller validation and mutual channel process comparison use it.
Exit/error readiness and every other syscall error still reject. The retry does
not synthesize liveness from an interrupted observation, reopen a PID, skip
pidfs identity comparison or relax credential/descriptor checks.

The helper establishes only absence of pidfd exit/error events; independently
authenticated descriptor/process identity and domain authorization are still
required. Journal custody/startup permission is not implemented by this repair.

## Validation

- Full `go test ./...` and `make test-cross` pass.
- Linux `go vet` and golangci-lint 2.13.1 for `runtimechannel` and `nodeagent` pass.
- Native Linux static race binaries were built with Go 1.26.7. The channel
  tests exercise repeated EINTR followed by live, exited or EBADF outcomes,
  and an actual child process's live-to-exited pidfd transition. Both behavioral
  main tests pass; the third selected entry is the subprocess helper.
- The Node `^TestRuntime(Caller|Channel)` selection passes all ten behavioral
  main tests without race reports. It includes actual mutual exchange,
  inherited-descriptor/credential rejection, cancellation/deadline behavior,
  process exit, nested PID namespace identity, descriptor exhaustion and lock
  inspection. Three standalone subprocess helper entries skip without their
  helper environment; their actual child invocations run inside parent tests.
- The experiment used disposable CPU containers without workload/host mounts or
  network. Node namespace regression tests used the existing privileged sandbox
  pattern; the narrower custody prototype requires only added SYS_PTRACE.

The failing prototype timing runs are not performance receipts. The corrected
prototype must rerun before any direct-versus-IPC performance conclusion.

## Local evidence

Logs are preserved under `/tmp/vela-startup-validation.syiSMk/`:

| File | SHA-256 |
| --- | --- |
| `custody-prototype-poll-diagnostic.log` | `336295a8d68983cc9cf65863c6eec894773d78b502e8598e6c6ba331f89807b8` |
| `custody-pidfd-node-race.log` | `b7ebec028e97eae85b108e95f21faf525bd77aed77e478fd8cb8d4f75cc6e8b3` |
| `custody-pidfd-channel-race.log` | `764de4d1b25ac952cf1cf624ccb2403116d774828801ca6fd4c8ef6cdb8655d8` |
| `custody-pidfd-unit.log` | `c8c6f30ace3740f347f19cf54c07d16f5ad4dccf3c6434a15ead08d79b90f4f8` |
| `custody-pidfd-cross.log` | `56633f98337c8581efb1acb73254f5b192f6c8afaab36a556ccb0149b3aa82ba` |

The diagnostic image is
`sha256:a4f26da8fe3abb59d8c04a94c3714066e02f3ddcb98b6b4799c16e20f36acdef`.
The fixed Node/channel race image is
`sha256:391e96da23b61e3ffe3799aa1f42f8c48dd86160f91b2219fad591388c8a16e9`;
its ModelRuntime prototype binary is the older diagnostic build and was not used
as evidence for the repair. Entrypoints `/nodeagent.test` and
`/runtimechannel.test` contain the fixed source.

PostgreSQL 94, Worker journal 5, Runtime journal 6 and Production Gates 0/9 are
unchanged. No remote CI, push, deployment or GPU validation was performed.
