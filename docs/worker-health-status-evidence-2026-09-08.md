# Preserve explicit Worker health across stopped execution status

Against `aa8a70b`, the public gRPC path reproduced readiness becoming true after
an explicit `WorkerReusable=false`, successful exact execution drain, and a
later `STOPPED` status without health evidence. Removing STOPPED's health reset
also exposed an independent readiness gap: the shared admission check considered
terminal floors but did not check explicit health denials above the floor.

The fix preserves the denial until validated failure evidence explicitly changes
it. Readiness checks every resident profile's health denial. The public gRPC
regression verifies both profiles remain unready, fresh AUX work is rejected,
and an explicit validated `WorkerReusable=true` permits readiness and Prepare.
Existing ordinary STOPPED, cancellation, exact-drain and renewal reuse tests pass.

Validation: full `go test ./...`, scoped `go vet` and golangci-lint 2.13.1,
host race and native Linux/arm64 race regressions pass. Linux scratch tests run
as UID/GID 10001 without capabilities or network. The initial Linux selection
failed one compiled-driver test because scratch has no Go executable; that test
passed separately in the pinned Go builder. The other tests passed in a fresh
scratch invocation. The failed invocation is not counted as a passing run.

Logs remain in `/tmp/vela-startup-validation.syiSMk/`:

| Log | SHA-256 |
| --- | --- |
| `worker-health-stopped-red.log` | `66d7e71bc7abd90ddf57776cb165778f34637b75eae885a76d41c7218dc02d8b` |
| `worker-health-unit.log` | `6ec2e36aca18e717514f56748c6a9f4dcb6ba04aa1d608d6c56c36f4cb7f6a0d` |
| `worker-health-race.log` | `4726c60442c4c36f555ef12cd77afb1420fdc7441aa7e657c79d1d31ba3d84f0` |
| `worker-health-lint.log` | `e92606b0bf483111dff0a120c315ea165821348f31365020e2468a0059095c47` |
| `worker-health-linux-focused.log` | `23bdec715bf51367d4213b6b5fc9aea6e6035aec880ce28796afbb1c18625532` |
| `worker-health-linux-compiled-process.log` | `27776266ff34650cb957f47927afeab37f60afc74db1c004e4a6cdf1279e5937` |

Linux image: `sha256:f7a9a3410dfa45fdeea60107990fe8865c2c9c2f4a34967b62031f22d02a8603`.
Only its `/modelruntime.test` was rebuilt for this increment.

This fixes live-owner state transitions. Health denial is still volatile and
this increment does not define durable clearance or prove device health. The
production server's unresolved-incarnation restart barrier remains restrictive;
it is not a substitute for explicit persistent health evidence and authorized
replacement. Runtime journal remains schema 7; Production Gates remain 0/9.
