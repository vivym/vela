# Passive Runtime file-lock observation

This increment follows `dfa5471` on `feature/vela-mock-hardening`. It adds
`RuntimeCaller.InspectFileLock` and validates it against the actual Runtime
startup path. It does not assemble a Node authorization issuer or authenticate
original journal provenance. PostgreSQL 94, Worker journal 5, Runtime journal 6,
Registry binding 1, release bundle 3 and Production Gates `0/9` are unchanged.

## Kernel semantics

The local Linux CPU experiment distinguishes a file's inode from its open file
description, the kernel object that owns a `flock`:

- An independent open of the same inode has no source `flock` in its `fdinfo`.
- A duplicated descriptor shares the original open file description and lock.
  Closing the original descriptor leaves that lock held by the duplicate.
- Unlocking through a duplicate removes the lock observed through the original.
- An `O_PATH` open through the original procfs descriptor link pins only the
  inode. After every actual lock-owning descriptor closes, a competing open
  acquires the lock while the passive `O_PATH` handle is still open.
- On the tested local filesystem, Node's POSIX `F_GETLK` reports no conflicting
  POSIX lock while a different Runtime process holds `flock` and Node's competing
  `flock` fails. It cannot substitute for the descriptor's `FLOCK` evidence.
  The cross-process check matters because `F_GETLK` ignores one's own locks.

Consequently, the observer uses `O_PATH` and never duplicates the source open
file description, calls `flock`, or reads through a shared file offset. It also
does not invoke an untrusted device or FIFO's open operation while selecting a
descriptor. The earlier kernel test explicitly unlocked before probing release;
the final test closes the last owning descriptor, which directly checks that
the passive observation cannot extend lock lifetime.

## Observation contract

The method accepts a bounded descriptor number solely as a selector. It uses
the authenticated caller's retained original procfs root and rechecks its live
pidfd/process identity before and after observation. No caller-supplied path is
used. It requires a regular, single-link, `0600` file owned by that non-root
caller's UID/GID and a read/write, close-on-exec source descriptor.

Bounded kernel `fdinfo` must contain exactly one whole-file advisory exclusive
`FLOCK` entry, matching the observed device/inode and Node-visible caller PID.
Missing, shared, POSIX, OFD, pending, partial-range and multiple locks reject, as
do malformed/duplicate/unknown fields and inconsistent inode/mount/flag data.
Repeated descriptor, metadata, fdinfo and process reads reject visible changes
within the bounded observation interval. The result records the process,
descriptor, device/inode, size, mount ID, numeric lock PID and observation times.

This is a local kernel observation, not an uninterrupted ownership lease. The
same caller can unlock and relock between reads, reuse a descriptor for another
open of the same inode, or share an open file description with another process.
Numeric lock PID and inode values are not durable incarnation identities.
The method neither proves the lock acquirer's original process incarnation nor
excludes an unobserved ABA transition. It does not read journal contents or
compare them with Registry history.

## Validation

The authenticated caller test uses an independent non-root namespace PID 1.
It accepts the actual locked descriptor and its duplicate, rejects an
independent open of the same inode and non-regular/invalid descriptors, and
covers unlock through a duplicate, shared-lock conversion, permissions,
hardlinks, cancellation, descriptor closure and original-process exit. Node
descriptor counts remain unchanged after observations.

The existing actual `StartRuntimeServer` startup exchange now independently
matches the observer's result to the original fixture lock's device/inode/size
before any mock decision. All four cases, mock permit, denial, lost response
and uncertain Node append, retain their factory and recovery restrictions.
The fixture knows the original lock path and enumerates bounded descriptors;
production does not yet have that trusted birth-to-serving association.

Validation uses the isolated Linux/arm64 CPU image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`,
Docker `28.3.2`, containerd `v2.3.1` and runc `1.4.2`:

- Complete Node CPU wrapper: 57 mandatory main tests and both volatile-state-reset
  scenarios pass without skips in 165.13 seconds.
- Full `go test ./...` and `go vet ./...` pass.
- Linux integration-tag Node/command vet and golangci-lint `v2.13.1` pass with
  zero lint issues.
- The final static Linux race run selects the three new main tests, actual
  startup exchange, authenticated caller/message rejection, channel round trip,
  planned caller and actual CRI caller explicitly. All nine pass without skips
  or race reports. Its log is retained below.

The full wrapper ran before the last-descriptor-close and cross-process-query
test strengthenings; the final race binary includes both assertions. The race
build uses Go
`1.26.7` image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`
with `-race -ldflags '-linkmode external -extldflags=-static' -p=2`.
Static glibc NSS linker warnings concern the test binary, not production linking.

Reproduce the complete CPU wrapper with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Logs are retained under `/tmp/vela-startup-validation.syiSMk`:

| File | SHA-256 |
| --- | --- |
| `file-lock-full-cpu.log` | `04a5daee8372ccff8c7f49f3f1bc9e3e4f17bfe6939374650ebfc2dd9c52865c` |
| `file-lock-unit.log` | `c8c6f30ace3740f347f19cf54c07d16f5ad4dccf3c6434a15ead08d79b90f4f8` |
| `file-lock-race-final.log` | `e9ec7808e1c616faccc102d93549f003f1b91c2b595a70078f840bd0b95604fa` |

## Remaining authorization work

The Node must independently retain the original journal's storage identity at
authorized initialization and bind the observed file, complete journal state,
Registry pair, startup intent, effective launch and current Fleet activation.
The existing Registry pair signs journal UUID/scope, not this observer's file
identity. A caller-selected descriptor and matching journal bytes cannot supply
the missing provenance. Passive observation alone also cannot supply continuous
exclusivity across the authorization decision.

The production Node endpoint, durable permission transaction, recovery after
owner-handle loss and independent retirement remain incomplete. These CPU
observations grant no startup, replacement, execution drain or DeviceSet release
and close none of the nine Production Gates.
