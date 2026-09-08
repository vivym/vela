# Complete Runtime journal verification without file recovery

This increment follows `bd85752` on `feature/vela-mock-hardening`. It separates
complete journal semantics from filesystem ownership and recovery so a Node
observer can verify supplied journal bytes without taking the Runtime's lock,
syncing its files, cleaning temporary state or allocating epochs. PostgreSQL 94,
Worker journal 5, Runtime journal 6, Registry binding 1, release bundle 3 and
Production Gates `0/9` are unchanged.

## Shared semantic boundary

Previously, the complete decoder and evidence validators were methods of the
concrete file owner. Calling `PrepareExecutionJournal` for Node observation
would contend with the actual startup lock and perform recovery writes. A
second, simplified JSON reader would risk accepting history that recovery
rejects.

`executionJournal` now contains only state, scope and semantic validators. The
existing `executionStateFile` embeds it and retains all file, lock, persistence,
upgrade and recovery operations. The file recovery path and
`VerifyExecutionJournalSnapshot` share canonical decoding and the entire proof
validation graph: lifecycle, highest/floor witnesses, signed retained executions,
renewal candidates, drain checkpoints and both non-admission formats. Status
projection and manifest-derived scope construction are also shared.

The public snapshot verifier requires an independent expected journal UUID,
member scope and original root/lock identity, exact lock-file UUID bytes, trusted
launch manifest and signature verifier. It accepts only current schema 6, checks
the 12 MiB document bound and rejects duplicate/unknown JSON fields, trailing or
noncanonical data, mismatched ownership and invalid nested evidence. No upgrade
or file operation exists on the pure journal type.

The returned snapshot owns its verified scalar results and digests; callers
cannot construct a valid snapshot by filling exported fields or mutate it
through returned status/input aliases. `MatchStartup` requires the exact
persisted unresolved incarnation and launch, and an unused execution history.
A semantically valid running/recovery journal does not qualify as first-start
evidence. Historical signatures are verified as durable restrictions without
requiring their old execution TTLs to remain live.

These checks prove semantics of supplied bytes, not provenance or freshness.
They do not authenticate a caller-supplied copy, prove continuous locking,
loaded executable/configuration, current Fleet activation or backend retirement.
`MatchStartup` does not independently bind the request's Node/Registry identity;
that remains the surrounding trusted Node transaction's responsibility. No
production permission issuer is introduced here.

## Verification

Three new test groups exercise:

- Actual `StartRuntimeServer` before its first factory, using the original
  journal identity from preparation. Snapshot verification succeeds while
  offline preparation is denied by the held lock. Wrong nonce, scope, UUID or
  launch rejects; even a different valid launch with the same member scope
  cannot match. Returned status and input buffers cannot change the result.
- Nineteen malformed/unbound cases, including legacy schema, invalid lifecycle,
  missing history, oversized data, bad lock identity and missing verifier.
- Five histories generated through real Supervisor operations: pending,
  drained, renewed, execution-envelope non-admission and terminal-allocation
  non-admission. Results agree with offline recovery, including pending/drained
  distinctions and both checkpoint counts. Signature, nested renewal, retained
  history and checkpoint-contract corruption rejects. Valid running histories
  still reject a structurally matching first-start request.

The Linux Node/Runtime startup exchange fixture now uses this same complete
verifier before any explicit mock decision. Its original paths/identities and
Registry are fixture-owned expectations, not a new production provenance
implementation. Actual non-root PID-1 Runtime, authenticated Node channel,
passive kernel flock observation and durable Node association remain exercised.
Permit starts factories; denial, lost response and uncertain Node append do
not. Every mode reopens recovery-only afterward.

Passed checks:

- Full `go test ./...`, `go vet ./...`, ordinary golangci-lint v2.13.1 and
  `make test-cross`.
- Linux integration-tag lint for ModelRuntime, Node Agent and Worker bootstrap.
- Host race tests for all new snapshot groups.
- Static Linux race: 51 behavioral main tests plus three subprocess helper
  entrypoints across journal recovery/upgrades, admission, renewal, terminal
  proofs and the real Node startup exchange; no skips or race reports.
- Four Node Linux race main tests: actual startup association exchange and three
  passive lock observer regressions, no skips or race reports.
- PostgreSQL host-race integration: actual local bootstrap journals, committed
  Registry binding with Runtime startup, and the eight protected provisioning
  command scenarios; 53.526 package seconds.

Linux/arm64 tests use Docker `28.3.2` and native Go `1.26.7`, builder digest
`sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1`.
Static race binaries use `-ldflags '-linkmode external -extldflags=-static'`;
glibc NSS linker warnings are fixture limitations, not production-link evidence.
Containers are CPU-only, network-isolated, bounded, and mount no host workload
directories. The image is `FROM scratch`, with only test binaries and empty
`/run` and `/tmp` directories. No containerd base image is needed for these
selected tests; CRI/Kubernetes observations in the startup fixture remain mocks.

The broad Runtime/Node race image was
`sha256:50ce042dd4d1f663abf983ca3c58588ab2c9f0b4c92273869b8310019e6ee8b0`.
The final snapshot tests add explicit drained-history and checkpoint-contract
cases in image
`sha256:6c9cb7eb0d187174c18eb6416ae8f3dfd8a52554f8e7e1cfa1c926f56075ec68`.
The production implementation is identical between these two images.

Logs remain under `/tmp/vela-startup-validation.syiSMk`:

| File | SHA-256 |
| --- | --- |
| `journal-snapshot-linux-race.log` | `5ef25d1a5b6dbcfb4b09865cc2b3a530bc47fe5f1b7e4168d4fbe8218c474537` |
| `journal-snapshot-node-race.log` | `c36845da7a3ce4b8521ab59bf14b304f82b5550788bf66b1dcc7c0a17b3ded2d` |
| `journal-snapshot-postgres.log` | `79bfdf3752561ca3c8375e69aef2d8ff52e9811f74a691e7841d72328b4bf3d9` |
| `journal-snapshot-unit.log` | `2422c122780a248582df09bdd82878f920cca202257aeaff671c10b681cd742a` |
| `journal-snapshot-linux-final.log` | `e0f2973909cf89a09f5d424a0ad761ec11db9fd367c27b088afb2b0f229baec3` |

The final focused Linux and host logs are `journal-snapshot-linux-final.log`
and `journal-snapshot-new-race-final.log`. These receipts establish a shared
complete semantic verifier; independent live storage custody and the full
durable startup/retirement transaction remain open. Earlier exact-cache/load
receipts are not promoted to latest-source system closure.
