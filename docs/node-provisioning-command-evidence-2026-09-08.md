# Protected provisioning: actual command and Linux race evidence

This increment follows `69e46ca` on `feature/vela-mock-hardening`. It verifies
the existing protected provisioning protocol through the actual Linux Node
command and PostgreSQL/mTLS, and makes the isolated Linux race campaign a CI
requirement. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding
1, release bundle 3 and Production Gates `0/9` remain unchanged.

## Actual command campaign

`TestProtectedProvisioningCommandPostgres` passes all eight scenarios with a
new PostgreSQL fixture migrated to schema 94 for each scenario:

| Scenario | Observed result |
| --- | --- |
| Normal | One Claim and recorded pair; completed root evidence; exact journal UUIDs, file inventory, modes and UID/GID 10001 handover. |
| Lost Claim response | Claim remains committed; no receipt or completed handover; restart rejects retained attempt. |
| Lost receipt response | Both Claim and receipt remain committed; no completed handover; restart preserves the ambiguous local state. |
| KILL after Claim | Actual process exits 137; committed Claim survives; restart cannot initialize again. |
| KILL after receipt | Actual process exits 137; committed pair survives; restart does not replay receipt or change evidence. |
| TERM after receipt | Actual signal cancels the command, exit 1; committed pair survives; restart rejects. |
| Wrong Node | Local scope mismatch rejects before Claim, leaving the private directory unchanged. |
| Unregistered principal | TLS 1.3 transport reaches Registry rejection; local intent is retained, Registry first use is unconsumed, restart cannot retry Claim. |

Success is verified from actual stdout, the full tar inventory of the stopped
container, and separately queried PostgreSQL history. Every scenario restarts
the same container. Failed commands publish no successful stdout. The test
interceptor drops responses only after the actual service transaction commits;
signal cases wait for that boundary before interrupting the process.

The static Linux command image is built from this checkout using `FROM scratch`.
Ephemeral test credentials and manifests are copied to the stopped container as
an explicit root-owned 0600 tar archive, never embedded in the image. Direct
host-file copy was found to retain macOS UID 501 and was correctly rejected by
the production secure-file reader. The fixture was corrected, without relaxing
that reader. Test certificates authenticate an ephemeral TCP listener; no live
Fleet endpoint, backend, containerd or GPU is involved.

Observed Docker version: `28.3.2`, platform: `linux/arm64`.

- Node command image: `sha256:5a858f9ac01a6894e9bd073612e0c6cccc036895646a44b52d334cc650118f73`.
- PostgreSQL image: `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73` (`postgres:17-alpine`).
- Complete campaign: 39.65 seconds, eight scenarios pass, no skips.

## Linux race and CI

The provisioning sandbox now accepts `VELA_TEST_PROVISION_RACE=1`, requiring
a matching native Linux toolchain and building the actual test executable with
`-race -ldflags '-linkmode external -extldflags=-static'`. Unsupported toolchains
fail instead of skipping. Docker diagnostics are separated from stdout so a
legacy-builder warning cannot be mistaken for the immutable image identifier.

All six mandatory sandbox tests pass under Linux race, with no skips: real
UID isolation and bind mount, repeated-use rejection, concurrent first use,
process-exit boundaries, unsafe roots and changed transfer storage. The fixture
remains network-isolated with no host workload mounts.

- Builder: `golang:1.26.7`, digest `sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1`.
- Race fixture: `sha256:98d291be85b775b3badc01f3cfed2b989a4dc369186a4c7bec5c1d6c47efef4c`.
- Sandbox wrapper: 8.33 seconds; all mandatory test names checked explicitly.

The new `protected_provisioning` CI job runs both campaigns and is included in
the aggregate `verify` dependency and success checks. The workflow has been
checked locally with actionlint; no remote CI execution is claimed.

Reproduction on native Linux with Docker:

```sh
VELA_TEST_PROVISION_SANDBOX=1 VELA_TEST_PROVISION_RACE=1 \
  go test -race -tags=integration ./internal/workerbootstrap \
  -run '^TestProtectedProvisioningSandbox$' -count=1 -timeout=6m -v
VELA_TEST_PROVISION_SANDBOX=1 \
  go test -race -tags=integration ./internal/integration \
  -run '^TestProtectedProvisioningCommandPostgres$' -count=1 -timeout=8m -v
```

The command campaign's Go test/Registry host uses race instrumentation; the
separate command executable is static and not race-instrumented. The first
campaign supplies the Linux race evidence for the provisioning implementation.
Neither campaign simulates physical power failure or authorizes production
startup. Protected record reading/recovery, current Fleet activation binding,
mount/initializer integration and safe Runtime replacement remain open.

## Validation receipts

Full `go test ./...`, `go vet ./...`, ordinary golangci-lint v2.13.1 and
actionlint pass. Integration-tag diff-aware lint reports zero new issues.
Unfiltered integration-tag lint still reports 82 existing findings (50 errcheck,
4 staticcheck, 28 unused); this increment does not claim that baseline is clean.
Four existing PostgreSQL/mTLS regression groups also pass with host race:
cross-process bootstrap, committed Registry binding, principal-scoped read-only
history, and response-loss transport behavior (`provision-transport-regressions.log`,
57.393 seconds including the race runtime).

Logs under `/tmp/vela-startup-validation.syiSMk`:

| File | SHA-256 |
| --- | --- |
| `provision-native-race-final.log` | `0d37923053f6dde93715960bbaf8abc061a557535b3dbb84ec332d0dab180440` |
| `provision-command-postgres-fixed.log` | `a1e7a9d19d40202dce6ed4666320ad67bfcd7949e893a73e5f9ecb93b1e2219e` |
| `provision-campaign-unit.log` | `8000192a5b9d6b86305a44439b8961c9c46969f8c3efe3376608a6028e0f147e` |
| `provision-command-diff-lint-final.log` | `e92606b0bf483111dff0a120c315ea165821348f31365020e2468a0059095c47` |
