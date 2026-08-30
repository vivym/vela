# Persistent H3 mock Runner lab deployment

This directory deploys the non-canonical H3 mock Runner on one idle eight-GPU
lab Worker. It does not install the Worker Agent, RKE2, control plane, Artifact
Store, or a saleable Catalog profile. It cannot produce a Launch Receipt or
advance a Production Gate.

The installer requires the digest-pinned image to be present in the local
Docker content store. It does not log in to the private registry or persist a
registry credential. It derives the one Encoder/VAE plus seven DiT role map
from the host's eight physical GPU UUIDs, rejects active GPU compute processes,
requires the exact Worker LAN identities `10.1.200.19` or `10.1.200.16`,
explicitly sets runtime UID/GID `10001:10001`, and creates only:

```text
/var/lib/vela-lab/mock-runner
vela-h3-mock-runner
```

Run on an approved idle Worker from a staged copy of this directory:

```text
sudo ./install-mock-runner.sh \
  10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
sudo /var/lib/vela-lab/mock-runner/admin/smoke-mock-runner.sh \
  10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
```

The service has no network namespace, runs as UID/GID `10001:10001`, drops all
capabilities, uses a read-only root filesystem, and is limited to four CPUs,
4 GiB memory, and 256 PIDs. All eight GPUs are visible because DEVICE readiness
must verify the physical role map, but an idle Runner performs no GPU work.

The smoke client verifies all four readiness modes, one successful execution,
the exact two-output inventory, byte counts, and output SHA-256 digests through
the persistent Unix-socket gRPC interface. Its JSON result is non-production
experiment evidence only.

The abrupt-restart rehearsal starts an Attempt, opens a Linux pidfd for the
observed host PID, then re-verifies the full container ID, PID, start timestamp,
running state, and cgroup identity before sending `SIGKILL` through that pidfd.
This prevents PID reuse from redirecting the signal to an unrelated host
process. It waits for `unless-stopped` to produce a new PID and process start
identity, then resumes the exact Attempt authority from local state. It
deliberately does not use `docker kill`: Docker treats that CLI action as an
operator stop and suppresses restart-policy recovery. A signal sent from inside
the PID namespace also cannot be used because Linux protects container PID 1.

```text
sudo /var/lib/vela-lab/mock-runner/admin/recover-mock-runner.sh \
  10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
```

This is a non-production Runner-process exercise. It does not exercise Worker
Agent fencing, Node loss, a stale Lease, PostgreSQL/NATS authority, or any fixed
Production Gate fault scenario.

## Replacement-budget lab profile

The Worker-control-network-partition rehearsal uses a second immutable mock
GenerationPresetRevision with ID
`84000000-0000-0000-0000-000000000201`. Before the control-host harness can
activate that lab Catalog revision, both persistent Runners must retain the
revision 1 allowlist entry and add revision 2. The Runner reads this file only
at process start, so the updater performs a guarded restart of the same
digest-pinned image.

First use a guarded control-database update to drain both exact Worker IDs and
verify zero active Leases and zero active Jobs. Then run on each approved Worker:

```text
sudo /var/lib/vela-lab/mock-runner/admin/upgrade-mock-runner-catalog-profile.sh \
  10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3 \
  --apply
```

The updater accepts only the two fixed Worker LAN identities, requires
`success` mode, one idle Runner process, zero GPU compute processes, and valid
restart state. It validates the exact old or already-upgraded allowlist shape,
atomically installs the two-entry file, and verifies the new container health
and configuration identity. If restart fails, it restores the old allowlist and
starts the previous configuration. It does not pull or transfer an image.

After both hosts pass, restore only the two exact drained Worker records and run
`deploy/lab/control-plane/worker-control-network-partition.sh` from
`marslab-server`. That harness independently reads and validates both host
allowlists before changing the mock Catalog or injecting the partition. The
result remains `NON_PRODUCTION_MOCK_REHEARSAL`, fixed scenarios `1/10`, and
Production Gates `0/9`.

## Failure-mode and retry-budget rehearsal

The bounded retry-budget rehearsal requires both Workers to support a guarded
runtime mode switch among `success`, `hang`, and `failure`. Upgrade an idle
Runner only after the control database proves zero active Jobs and Leases. A
Worker upgrade must retain the same digest-pinned image, exact allowlist, and
`success` mode after restart:

From an approved staged copy of this directory on each Worker, run:

```text
sudo ./upgrade-mock-runner-failure-mode.sh --apply
```

The initial lab image predates the backend fix that serializes an absent
implicated-device list as `gpu_uuids: []`. Its strict Runner therefore requires
the wrapper-only compatibility update below, which adds one deterministic mock
GPU only in `failure` mode. This does not pull, rebuild, or transfer an image:

```text
sudo ./upgrade-mock-runner-failure-receipt.sh --apply
```

Run `deploy/lab/control-plane/retry-budget-exhaustion.sh` from
`marslab-server` only after both Workers pass its structural preflight. The
harness requires exactly two `READY/HEALTHY` Workers, zero active Jobs and
Leases, zero Production Gate receipts, the fixed two-Attempt ServiceClass, and
both Runners in `success` mode. It switches both Runners to deterministic
`TRANSIENT_BACKEND` failure, verifies `RETRY_WAIT` after Attempt 1 and terminal
`FAILED` after Attempt 2, then restores `success` mode. An independent watchdog
also restores both modes if the harness is interrupted.

The passing lab result is fixed scenarios `2/10`, Production Gates `0/9`, with
zero Visible Completions, Charges, and Artifact rows, including zero committed
Artifacts. It is synthetic failure-path evidence only, not real-backend
reliability or a Launch Receipt.

The retained live receipt was produced by harness SHA-256
`852a7ff868bb2cb88808bd746c74e42ed0186865f4ca19d0b7848954f2a13222`.
After review, the repository harness was hardened to SHA-256
`b39652e15234f37cf9096f3a7268cfd1b2d830594b4ea4863d9eb9aefbdb132b`.
The hardened revision has not been rerun and is not the provenance of that
receipt.

Rollback removes only the managed container and preserves Runner state:

```text
sudo /var/lib/vela-lab/mock-runner/admin/remove-mock-runner.sh
```

Data removal is deliberately separate and destructive. After reviewing the
preserved path, use the installed `remove-mock-runner.sh --purge` only with
exact approval.
