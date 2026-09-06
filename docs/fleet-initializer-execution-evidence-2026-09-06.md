# Fleet initializer execution repair

CPU-only review after `1508906` found two faults in the actual Worker Pod init
scripts. No journal, database, launch or Fleet schema changes are involved.
Production Gates remain `0/9 PASS`.

## Findings and repair

Both root materializers dropped all capabilities and added only `CHOWN`.
The Worker materializer makes the socket and scratch directories private, then
changes their owner to UID 10001. Changing a 0700 parent first prevents the same
root process from reaching its children without a DAC bypass. Execution of the
rendered command with that exact capability set failed on fresh volumes:

```text
chown: /run/vela-model-runtime/private: Permission denied
```

The same restriction also prevents the later Runtime materializer from entering
the now-private scratch root. On repeated initialization, chmod of UID 10001
directories additionally requires ownership override.

The two init containers now retain `CHOWN`, `DAC_OVERRIDE` and `FOWNER`, with all
other capabilities dropped. This permits the intended ownership and mode changes
within their mounted filesystems. They still use UID/GID 0 only during init,
read-only container roots, read-only projected credentials, runtime-default
seccomp and disabled privilege escalation. The serving containers retain their
existing non-root security contexts. This is an explicit increase in init-only
filesystem privileges; it does not add a privileged container or device access.

The Runtime script also chmodded and recursively chowned the obsolete
`scratch/model-runtime/*` tree. Its directory list now contains only epochs,
inputs and outputs, so a fresh filesystem has no matching path and `sh -e`
would stop. Those stale operations were removed; only the current epoch tree
and private launch/verifier material are changed by that script.

## Execution evidence

`TestWorkerInstanceInitializersExecuteOnFreshAndRetainedVolumes` materializes a
real Worker Pod from a validated H3 bundle and executes both emitted init commands
in order. Docker maps each declared mount and its read-only setting, each literal
environment variable, user/group and capability set. It uses the deployment's
pinned BusyBox linux/amd64 image:

```text
docker.io/library/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0
```

Synthetic projected files start root-owned with mode 0400. A unique Docker
volume with subpaths preserves Linux ownership across the separate containers;
no host scratch path is mounted. Runs have no network, read-only root filesystems
and `no-new-privileges`. The test requires Docker CLI support for volume subpaths
and a runnable linux/amd64 image, including emulation on this arm64 host.

After the first pass, a separate UID/GID 10001 container with no capabilities
checks private directory/file ownership and modes, then writes retained input
and epoch fixtures. Both init commands run again on the same volume. The final
non-root check verifies those bytes and 0600 file modes survived, the launch and
verifier files remain 0400, private roots are 0700, and no obsolete Runtime tree
or Runtime copy of the Worker's signing key was created. Test volumes are removed
on success or test failure; the post-run inventory was empty.

Passed:

- The new integration test, first failing on the original capability set and
  passing both fresh and repeated execution after the repair.
- `go test ./...`.
- Focused Fleet, launch evidence and deployment contract tests.
- Ordinary `make lint`, including `go vet ./...`.
- `golangci-lint run --build-tags integration ./internal/fleetcontroller/...`
  with `0 issues`.
- `git diff --check`.

The integration shard script discovers packages by their build-selected test
files, so the new tagged Fleet test enters the existing integration test matrix.
No CI run or remote deployment was performed in this increment.

## Remaining boundary

This exercises initialization scripts, not Kubernetes admission, scheduling,
hostPath provisioning, PKI validation, Runtime/model startup or journal first-use
authorization. Existing Pod specifications differ from the new desired render;
they need a controlled rollout and fresh launch evidence, not an assertion that
already-running Pods were repaired.

Fleet still lacks independently authorized, one-time journal provisioning and
durable serving activation. An absent hostPath, empty directory or init-container
retry must not grant journal initialization. Worker/Runtime recovery state must
remain separate from replaceable private credential volumes. These concerns and
unknown historical writers, receipt recovery and bounded reclamation remain open.
