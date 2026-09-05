# Resident ProcessBackend teardown evidence

Date: 2026-09-05
Base checkpoint: `a9a1f7abb5613eac981625e1948ef162a647cd9b`
Scope: local CPU driver teardown; schema remains 88.

## Reproduced defects and repair

Public `NewProcessBackend`, `Probe`, `Prepare`, `Close`, `Done`, and `Err`
regressions start a real helper driver and child writer. Before repair,
shutdown, timeout, unexpected exit, and a full response queue leave `Done`
open and the child writing. The full-queue case also survives test cleanup
because response delivery and process waiting depend on each other. A separate
large Prepare blocks on stdin after its context is canceled, while a queued
Probe also ignores its deadline. Finally, initialization failure hides a
simultaneous stderr drain failure.

The driver now starts in a separate process group. Linux `waitid(WNOWAIT)` and
Darwin kqueue observe its exit without reaping it; a single group signal occurs
before `command.Wait`, while the unreaped PID prevents group identity reuse.
Darwin's zombie-only group EPERM is accepted only after a kernel process
snapshot establishes that no live group member remains.

Owned input/output pipes decouple direct-process supervision from inherited
descriptors and response delivery. Draining has a deadline and preserves the
last shutdown ACK. The context-aware call gate permits a queued caller to leave
without canceling the active operation. Active cancellation closes stdin and
signals teardown; its callback completes before the gate is released. The
constructor preserves both initialization and cleanup errors.

Stage Cancel/Status semantics and model residency are unchanged. Process-group
teardown is a whole-driver operation, not a new scratch retirement authority.

## Final verification

All final checks below use the source hashes recorded at the end of this file:

- `go test ./...`: PASS; `internal/modelruntime` 5.912 s.
- `make lint test-cross`: PASS, 0 lint issues; Linux amd64 cross-compilation.
- `go test -race ./internal/modelruntime -run '^TestProcessBackend' -count=1
  -timeout=120s`: PASS, 8.463 s on macOS arm64.
- Linux arm64 compiled test binary with `-test.run '^TestProcessBackend'
  -test.v -test.timeout 3m`: PASS, including native H3 command build/execution,
  three resident components, child cleanup, blocked writes, and initialization
  error propagation. External Fast-H3 Python conformance remains opt-in and
  was skipped; this is not GPU or external backend certification.

The Linux run used the locally present `golang:1.26.7-bookworm` image ID
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`,
UID/GID 10001, no network, read-only source/module mounts, and a writable
executable tmpfs for test outputs. The disposable container was removed after
exit. The first Linux run's native-command test failed because the temporary
mount lacked execute permission; that environment diagnostic is retained at
`/tmp/vela-process-backend-linux-20260905.log` and is not counted as a pass.

Before the final constructor error-propagation change, 50 repeated acknowledged
shutdowns also passed in 9.439 s. The final race run covers the constructor
change. Windows's new unsupported-platform helper compiles and rejects launch;
the existing package still fails Windows compilation in `internal/securefile`
because of Unix-only APIs. No Windows runtime support is claimed.

## Limits and next work

A real child that calls `setsid` can outlive group teardown. Its inherited
stdout causes a reported drain timeout, and the regression observes it still
writing until its explicit test stop gate. An arbitrary blocked `Stderr.Write`
also cannot be forcibly canceled: the test requires the drain error and checks
that the writer remains blocked until released. These are explicit incomplete
cleanup outcomes, not successful physical quiescence. Successful group signal
delivery alone proves neither all-descendant exit nor execution-specific drain.

Worker input admission, the complete terminal allocation cutoff at both Worker
and Runtime, and persistent retirement intents remain open in the
[terminal scratch design](terminal-scratch-retirement-design-2026-09-05.md).
This repair does not inherit the earlier 512-Job measurements; those still
belong to checkpoint `a9a1f7a`. No integration/CNPG/load campaign or Production
Gate was advanced by these narrower teardown checks.

## Retained logs

Paths are under `/tmp/`; SHA-256 binds the captured bytes.

| File | SHA-256 |
| --- | --- |
| `vela-process-backend-cleanup-red.log` | `9e7f0fb9154e211b43e263d5e8d32e8377c4c591d2f6d06e277388ec2c86b78a` |
| `vela-process-backend-write-red.log` | `93f5ce5cc6af2b10f9fb7b4a3633d2164a40672afd047afbeb848318aae0d1ef` |
| `vela-process-backend-initialize-red.log` | `03226d22cce74f63944cd55438654a144655a79b8327a730eb84e794753330ab` |
| `vela-process-backend-cleanup-race.log` | `607d2e3f1f6a2567ec20986f18b1d4b8e63417113e8671bf434ac9032446a87d` |
| `vela-process-backend-final-unit-20260905.log` | `e4332a6c254006f3481a5c3e379473df74598e1d10ce5cf3d8e2c70f4ee65188` |
| `vela-process-backend-final-quality-20260905.log` | `9b2c5ad079298479e541b857a90f8130d9ee0ed7facf6407ee571e65e08400ae` |
| `vela-process-backend-final-linux-20260905.log` | `8de03aeafdbf5f658d46724443acd7823aa7cb387a3ef5b7dd148b667a2fed15` |

## Changed source hashes

All other executable source remains at the base checkpoint. These six files
identify the source delta used for the final checks.

| Path under `internal/modelruntime/` | SHA-256 |
| --- | --- |
| `process_backend.go` | `f065411fc2411f9944df5306a583cab34be0fce283bbc43910b265643f940e1f` |
| `process_backend_cleanup_test.go` | `a70a2af1efca0bd417df77f5f1f7f44ec6873fce55cc2177c952088fbf3c8070` |
| `process_backend_process_darwin.go` | `f4d95dc22875ae74fb3c6fb989f40dd39bdea59f9d8c1d16469bb9d8f7be589a` |
| `process_backend_process_linux.go` | `526beb750578bdb898595dbeeeb80bf107dba18c12a538d8a8eb661adfdfb7c1` |
| `process_backend_process_unix.go` | `be5a6f9e24b221749f67dc07f6b51803662d61ccdf323276f742d54be0574d29` |
| `process_backend_process_unsupported.go` | `3a3465b5f6c6cba7bc38fdb580f4d00ddda9d25c7def4c521203848a1f5b0e34` |
