# Read-only inspection of original protected handover

This increment follows `aebf674` on `feature/vela-mock-hardening`. A completed
provisioning operation can now be inspected by a new Node process without
repeating first use. This closes the lost-success-result inspection gap for
complete, still-pristine storage. Interrupted ownership transfer and already-used
journals still require a separate recovery protocol. PostgreSQL 94, Worker
journal 5, Runtime journal 6, Registry binding 1, release bundle 3 and Production
Gates `0/9` remain unchanged.

## Contract and command

`workerbootstrap.InspectProvisionedJournals` accepts only a `HistoryReader`, which
has no Claim, receipt-write or activation capability. The Linux root-only
implementation:

1. Opens the trusted original Node directory and strictly decodes all three
   private records. Canonical JSON, bounded size, no duplicate/unknown fields,
   single-link files, root ownership and private modes are required.
2. Verifies the intent's self/root identity, Node and actor, approved bundle and
   launch digests, target UID/GID and history bound; the independent origin and
   completed handover must agree, including the origin digest and request UUID.
3. Holds the intent, bootstrap operation and both journal exclusive locks.
   Live owners cause immediate rejection. It opens the exact known inventory,
   rejects duplicate/path-substituted entries, and verifies all six directories
   and nine files against original identities, modes, UID/GID 10001 and bytes.
4. Reads Registry history through the existing authenticated client. Fresh,
   missing, abandoned or differently scoped claims/receipts reject. Actual
   original Worker/Runtime storage and journal UUIDs/scopes must agree with the
   recorded pair and committed receipt timestamp.
5. Rechecks the held files, path bindings, private records, complete inventory
   and cancellation after lookup. Any failure returns a zero result. Descriptors
   close on every path; failed inspections do not leak lifetime locks.

The command uses the same approved manifests, verifier and registered Node mTLS
credentials as provisioning:

```sh
vela-node-agent bootstrap --action inspect-provision \
  --node-state-directory /var/lib/vela/node-provisions/<original-operation> \
  --bundle-manifest-file <approved-bundle> \
  --launch-manifest-file <approved-member-launch> \
  --verifier-keyring-file <stage-authority-public-verifiers> \
  --max-records <original-bound> \
  --fleet-address <host:port> --fleet-server-name <tls-name> \
  --fleet-ca-file <ca> --client-cert-file <registered-node-cert> \
  --client-key-file <private-node-key>
```

Successful stdout contains the original `ProvisionedJournals` schema-1 result.
The command derives scratch from the original Node path; it rejects an explicit
scratch path or caller-selected request UUID. Non-Linux command assembly returns
an explicit unsupported error. It neither writes nor fsyncs recovery state.

This result is a point-in-time inspection of initial handover. It releases the
locks before returning and cannot be used as a startup grant, current Fleet
activation, mount permission, continuous ownership proof, or writer-quiescence
receipt. Exact byte comparison intentionally rejects valid later journal
evolution as well as corruption. Root administration, rollback and inode reuse
remain outside the local provenance guarantee. Copying the tree to another
directory cannot establish the original storage identities.

## CPU verification

The required provisioning sandbox now checks eleven main tests, including the
previous six provisioning regressions. All pass under static Linux `-race`,
without skips, on Docker `28.3.2`, Linux/arm64. New coverage includes:

- Repeated read-only inspection with all four locks independently probed during
  Registry lookup, preserving the original files and first-use counts.
- Seven interrupted boundaries: only the already-durable complete handover can
  be inspected after the caller loses its result; earlier boundaries remain
  rejected and byte-for-byte untouched.
- Twenty-two changed-state/history cases: malformed records, wrong ownership,
  hard links, replaced inodes, changed journals, symlinks, extra files, missing
  or abandoned receipts, mismatched pair/time/identity/bounds and cancellation.
- Five mutations/cancellation injected during Registry lookup, proving the
  final validation runs and never reports stale success.
- Each of four live locks separately blocks inspection before Registry lookup;
  inspection succeeds after the owner releases it.

The native race fixture is
`sha256:20df7d5057ed45c352298d73b6b2daf0a79443e3614ea3ec90d72b56f2104d13`.
The eleven-test wrapper finishes in 11.42 seconds. Reproduction remains the
`TestProtectedProvisioningSandbox` command in the
[command/race evidence](node-provisioning-command-evidence-2026-09-08.md).

`TestProtectedProvisioningCommandPostgres` now uses a fresh named Docker volume
for each scenario. After the original process exits and its repeated provisioning
attempt rejects, a new container runs the actual `inspect-provision` command
against that same volume. The normal scenario returns exactly the original
result; the five committed failure scenarios reject inspection with no success
stdout. Both containers are checked against the same file inventory, and an
independent Registry lookup verifies that history is unchanged. Wrong Node and
unregistered-principal provisioning checks remain in the eight-scenario suite.
No copied journal snapshot is substituted for the original volume.

All eight real PostgreSQL/mTLS scenarios pass. The existing seven-scenario
cross-process bootstrap command regression also passes after adding the new
action. Full unit tests, vet, ordinary lint, Linux affected-module integration-tag
lint, integration diff-aware lint and cross-platform compilation pass. The
existing unfiltered integration lint baseline remains open as recorded in the
preceding evidence. Remote CI and production deployment have not been run.

The final actual-command image is
`sha256:299d97716e0a126d6ccd1f5698c94773e60d9936529bee93a81f7d3c54243d83`;
its eight-scenario suite completes in 37.82 seconds. Logs are retained under
`/tmp/vela-startup-validation.syiSMk`:

| File | SHA-256 |
| --- | --- |
| `provision-inspect-race-final.log` | `04a76015f91ba41dab31e270b973b6ce90e740a1af2f077024338bb8a0bb6bc7` |
| `provision-inspect-command-receipt.log` | `b36c103f35ce334cc192727caedc8d4761bcf7284918a356b2ecc36f3eaf6d5e` |
| `provision-inspect-unit-final.log` | `edb811629172172567795a6f9dbdba618d73018cdb59ca6cb80f2e7f0cfbadb6` |

`provision-inspect-cross-final.log`, `provision-inspect-host-race-final.log`,
`provision-inspect-vet-final.log`, `provision-inspect-lint-final.log`,
`provision-inspect-linux-lint-final.log` and
`provision-inspect-integration-lint-final.log` record the remaining checks.

## Remaining work

The initial protected records are now readable, but no API repairs interrupted
ownership transfer or validates evolved current journals for startup. Next,
startup authorization must bind protected origin, complete current journal and
its startup nonce, effective executable/configuration, exact containment owner
and current Fleet activation in one durable transaction. Fleet durable mounts
and initializer ownership must enforce the same protocol. Safe owner retirement,
lost-pidfd recovery, backend/input quiescence, sealed outputs, cleanup and bounded
history remain required before a latest-source sustained-arrival CPU campaign
can establish system-level availability. Existing exact-cache and 512-Job
receipts remain valid for their pinned earlier sources; the latter drains each
wave and is not an open-loop soak.
