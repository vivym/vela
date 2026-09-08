# Protected Node journal provisioning

This increment follows `5ceea4c` on `feature/vela-mock-hardening`. It creates
initialization evidence outside the future journal owner's write authority.
It does not issue Runtime startup permission or activate durable Fleet Pods.
PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1, release
bundle 3 and Production Gates `0/9` remain unchanged.

## Initialization and ownership boundary

The earlier `bootstrap/journal-origin.json` belongs to the same UID as its
journals. Coordinated rewriting of that local origin and journal metadata is
therefore outside its protection. Copying an owner-supplied status document into
a root directory would not establish how the journals were initialized.

`workerbootstrap.Provision` instead requires Linux root and an empty,
pre-existing root-owned `0700` Node directory under trusted ancestry. It creates
the scratch child itself and performs existing paired initialization as root,
using the normal fresh Registry claim and committed receipt. No workload
process, backend, runtime epoch allocation or mount is started by this API.

The command exposes this operation explicitly:

```sh
vela-node-agent bootstrap --action provision \
  --node-state-directory /var/lib/vela/node-provisions/<operation> \
  --bundle-manifest-file <approved-bundle> \
  --launch-manifest-file <approved-member-launch> \
  --verifier-keyring-file <stage-authority-public-verifiers> \
  --max-records <1-to-64> \
  --fleet-address <host:port> --fleet-server-name <tls-name> \
  --fleet-ca-file <ca> --client-cert-file <registered-node-cert> \
  --client-key-file <private-node-key>
```

The parent must already exist and remain outside workload mounts. This action
derives `<operation>/scratch`; it rejects `--scratch-directory` and caller-supplied
request IDs. Node and actor identity come from the existing registered Node
mTLS client. The daemon and recurring Pod initializers do not invoke it.

Three schema-1 Node records remain owned by root:

| Record | Meaning |
| --- | --- |
| `provision-intent.json` | Exclusive first attempt, UUID, root/file identities, Node/actor, approved bundle/launch digests, target UID/GID and history limit. |
| `provision-origin.json` | The actual private preparation result and complete initial storage inventory: six directories and nine files, including both admission journals, locks, bootstrap records and input/output ownership markers. Each file has inode/device, mode, byte count and SHA-256. |
| `provision-handover.json` | Completed ownership transfer, provision/request UUIDs and the origin document digest. |

Intent is exclusively created and lifetime-locked, then file and directory
synced before obtaining first use. The exact known tree is opened without
following final symlinks; unexpected files reject. Its files/directories are
synced and the independent origin is durably published before any ownership
transfer. Held descriptors transfer only this inventory to UID/GID `10001`,
matching the current Fleet contract. Files precede their containing directories;
scratch is transferred last. Final checks retain exact inode, mode, ownership,
file bytes and single-link regular-file identity before completion publication.
The Node parent remains root-only throughout, excluding the future workload UID
even after scratch ownership changes.

The implementation makes no recursive `chown` or `chmod` pass over an existing
workload tree. It does not import an owner's origin/status JSON as independent
initialization evidence. The local bootstrap origin remains within scratch for
its original purpose; the separate Node origin retains its initial digest.

## Failure and recovery semantics

Any existing parent content rejects before a new claim. A partial intent,
lost Claim/receipt response, partial journal initialization, interrupted ownership
transfer and even a previously completed provisioning operation cannot be
adopted or reinitialized by repeating `provision`. No automatic rollback,
ownership restoration, receipt replay after handover or completion reader is
implemented. Preserve the original Node directory and its Registry history for
future explicit reconciliation. A lost success response remains ambiguous to
the caller; the operation does not manufacture a new grant.

The `prepare`/`reconcile-pair` commands keep their existing owner/path binding
contracts. They are not a recovery mechanism for these handed-over Node roots.
Direct journal recovery through a different bind-mount path is supported and
tested without altering journal UUIDs, scopes, storage identity or lifecycle.

## CPU validation

Reproduce the new isolated experiment with:

```sh
VELA_TEST_PROVISION_SANDBOX=1 go test -tags=integration \
  ./internal/workerbootstrap -run '^TestProtectedProvisioningSandbox$' -count=1 -v
```

The wrapper compiles this checkout's static Linux test binary and builds a
`FROM scratch` image without a base image download. The container has no network,
no host directory mounts and bounded CPU/memory/process resources. Privileged
mode permits the test's local bind mount and UID transition. It uses no GPU or
containerd observer. The prior sandbox image was absent from the current Docker
inventory; this run uses Docker `28.3.2`, Linux/arm64, image
`sha256:76508b40a1f55648cf76476129b9e2d982a3ff6d3d2720b87f3754c207594b56`.

All six mandatory main tests pass without skips:

- Actual UID 10001 processes cannot open Node-private state before, during or
  after transfer. After a scratch-only bind mount, both real journal preparation
  APIs recover identical state. The workload can change its local origin, but
  cannot read, write or remove the independent Node origin.
- Seven injected interruption boundaries preserve state and reject a second
  claim. Five actual subprocess-exit boundaries prove the same after process
  restart, including partial ownership transfer and durable completion.
- Eight concurrent provisioning calls produce one successful operation and one
  Claim/receipt pair.
- Unsafe mode/owner, symlink, pre-existing scratch, wrong/relative paths and
  cancellation reject before Claim. A non-root process cannot provision.
- Extra files, symlinks, lock replacement and changed bytes reject completion;
  tests assert each injected fault was actually reached.

Full `go test ./...`, `go vet ./...`, host race tests for bootstrap and the Node
command, full golangci-lint `v2.13.1`, and affected Linux integration-tag lint
pass. The non-Linux command explicitly has only a rejection path; final focused
command tests cover the platform dispatch refactor. The six Linux tests use an
in-process mock Registry; they do not constitute a new PostgreSQL/mTLS command
receipt, Linux race run, physical power-loss experiment or live Fleet rollout.

Logs remain under `/tmp/vela-startup-validation.syiSMk`:

| File | SHA-256 |
| --- | --- |
| `provision-sandbox.log` | `8da8816d909cfeaadb5ce0ab07cef9c3e289225c8cb621d5a2249817b0627fb0` |
| `provision-unit.log` | `7f08142af970837b23e34110eca5ec54a3589db87015dee381c48334a39a94be` |
| `provision-host-race.log` | `84bc0a202b31bcbefff255926c2427123758e30785205478271d32b3dd42b1f7` |

## Remaining architecture work

This record is protected local evidence under trusted Node root administration,
not a signature, remote attestation or startup token. It does not protect against
root rewriting/rollback, inode reuse, remount changes or later owner corruption
of mutable scratch. Trusted provisioning must keep the new scratch path out of
workload mounts until initialization completes. The CPU bind mount demonstrates
the intended ownership boundary; current Fleet mounting and recurring root
initializers have not been changed to enforce this protocol.

Next work must authenticate retained Node records and complete current journal
state, preserve current Fleet activation, verify effective launch, and bind the
held journal/startup intent to the independently retained containment owner in
one durable permission transaction. Owner-handle loss recovery, exact-owner
retirement, execution/device quiescence and bounded retention remain separate.
Normal durable restart availability and production authorization remain open.
