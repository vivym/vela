# Worker Local Recovery State

## Goal

Implement the Worker-local portion of ADR 0028 without creating a cross-Worker
Durable Checkpoint. Encoder, DiT, VAE, and upload state is kept under the
current Worker's NVMe root and may be reopened only by the same Worker identity,
epoch, Attempt, and execution Lease fence.

The control plane remains authoritative for Attempt state and Lease fencing. A
local file is an optimization for same-node process restart, never evidence
that a LOST Attempt can be resumed by another Worker.

## Contract

`internal/workerrecovery` exposes a `Manager` and per-Attempt `Handle`:

- the root is an absolute, non-root directory with mode `0700` and a durable
  `.worker.json` binding to one Worker ID and epoch;
- each Attempt has a UUID directory and an identity record containing Worker ID,
  Worker epoch, Attempt ID, and Lease fence;
- `Open` rejects a different Worker, epoch, or fence, and a terminal tombstone
  prevents an old Attempt ID from being reopened after cleanup;
- state names are bounded ASCII leaf names and stage names are a fixed allowlist
  (`encoder`, `dit`, `vae`, `upload`); no caller can introduce a path component;
- writes use a bounded temporary file, `fsync`, atomic rename, regular-file and
  symlink checks, per-Attempt byte quota, and maximum entry count;
- `MarkTerminal` records a no-content tombstone before immediately removing
  local state; `Reconcile` only removes terminal-marked leftovers and defers
  unknown or malformed directories;
- watermarks expose `NORMAL`, `PRESSURED`, and `CRITICAL` states. Assignment
  admission is false at the high used-space watermark or critical free-space
  watermark, and resume eligibility is reported only below the low watermark.

## Filesystem layout

```text
<root>/.worker.json
<root>/attempts/<attempt-id>/.identity.json
<root>/attempts/<attempt-id>/<stage>--<bounded-name>
<root>/terminal/<attempt-id>.json
```

No prompt, user name, original filename, or arbitrary object-store key is used
as a local path component. The local files are customer content and therefore
remain private to the Worker process account.

## Quota and deployment boundary

The manager enforces a hard logical per-Attempt byte quota and refuses writes
at the critical watermark. `Config.SpaceProbe` must report the effective quota
for the mounted scratch project. The default `statfs` probe is suitable only
for development and test; production deployment must use an XFS project quota
or an equivalent hard quota and provide its capacity receipt.

## Failure and recovery semantics

- Same Worker process restart with the same epoch and active fence may reopen
  the local state and resume the local stage or multipart upload.
- Lease fencing, epoch change, Worker loss, NVMe loss, or an Attempt terminal
  transition rejects local state reuse. The replacement Worker recomputes from
  the beginning within the existing Retry Budget.
- An incomplete cleanup is recoverable only through the terminal marker and
  bounded reconciler. Unknown state is left in place and raises an operational
  signal rather than being recursively deleted.

## Evidence

The unit and race tests in `internal/workerrecovery/local_test.go` cover same-
epoch process restart, Worker/epoch/fence binding, path traversal, symlink
replacement, quotas, watermarks, immediate terminal cleanup, and stale terminal
reconciliation. This is repository evidence for the local component; an actual
NVMe/XFS deployment, Worker Agent integration, and production launch receipt
remain separate ADR 0028 and Production Gate evidence.
