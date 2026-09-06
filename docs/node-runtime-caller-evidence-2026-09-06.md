# Authenticated local Runtime caller

This increment follows `5adea05`. Node now has a Linux library boundary for
receiving one local Runtime request from a pinned live process. The real
containerd CPU experiment uses this implementation. The request is not yet
connected to a Node serving command, Registry startup binding, or Runtime
factory authorization. No durable journal or database format changes.

## Implemented authentication

`internal/nodeagent/runtime_caller_linux.go` exposes `ReceiveRuntimeCaller`,
`RuntimeCaller.Inspect`, `Payload` and `Close`. The receiver accepts an already
accepted Unix `SOCK_SEQPACKET` connection and an expected non-root UID/GID from
trusted Node configuration. UID/GID values from the payload cannot configure
this expectation. The API takes exclusive use of the connection during its
exchange, bounds its deadline to five seconds or the earlier context deadline,
and clears the connection deadlines before returning. The calling endpoint
retains responsibility for closing that connection.

The receiver checks `SO_PEERCRED` and obtains `SO_PEERPIDFD` for the connection
opener. It requires the kernel-returned descriptor to be close-on-exec. It then
enables `SO_PASSCRED` and `SO_PASSPIDFD`, sends a fresh challenge, and receives one
message with `MSG_CMSG_CLOEXEC`. The message must carry exactly one
`SCM_CREDENTIALS` and one `SCM_PIDFD`, both identifying the same live process as
the connection opener. A child sending over an inherited connection fails even
if it has the expected UID/GID. There is no fallback to opening a PID number
supplied by a request or to trusting connection credentials alone.

The protocol has two packets:

1. Node sends the ASCII bytes `vela-runtime-caller-v1`, one NUL byte, and 32
   cryptographically random challenge bytes.
2. The caller echoes that exact packet prefix and appends one nonempty payload
   of at most 32 KiB. Kernel credentials/pidfd accompany this message; user
   supplied descriptor rights are forbidden.

Challenge mismatch, unsupported socket/kernel operations, missing or extra
credentials, message/control truncation, descriptor passing, wrong UID/GID,
dead original process, cancellation and I/O errors return no caller object.
Received descriptors are closed on rejection, including rights recovered from
a truncated ancillary buffer. The temporary message pidfd is closed after
validation. Cancellation after receive also goes through descriptor cleanup.

The returned object retains the connection opener's original pidfd and an
opened `/proc/<pid>` directory. Its fields are private; a JSON observation or
caller-supplied PID cannot reconstruct a live object. `Inspect` checks the
original process handle before and after bounded process reads, compares the
kernel boot ID, verifies the host-view PID and real/effective/saved/filesystem
UID/GID, and reports process start ticks and PID namespace diagnostics. It
rejects exited or closed handles. `Close`, `Inspect` and copying `Payload` are
serialized; callers never receive the retained payload slice itself.

The observation deliberately permits a process that is not namespace PID 1.
It reports `NamespacePID` and depth so the container-binding layer can reject
unsupported ownership. Process authentication is useful evidence, not a
container membership, trusted executable, launch configuration, device,
Registry or retirement authorization. Inspecting a live process is not a lock
that prevents it from exiting after the observation.

## Kernel and runtime evidence

The experiment runs on Linux `6.10.14-linuxkit`, Linux arm64, Docker `28.3.2`,
the same exact CPU image and pinned containerd/runc as the
[containerd report](node-containerd-process-evidence-2026-09-06.md).
Kernel support for the named pidfd socket options is mandatory. No production
host kernel inventory or compatibility claim is inferred from this local run.

The upstream Linux `v6.10`
[socket implementation](https://github.com/torvalds/linux/blob/v6.10/net/core/sock.c)
resolves `SO_PEERPIDFD` from the socket's retained peer PID object, and its
[SCM receive implementation](https://github.com/torvalds/linux/blob/v6.10/include/net/scm.h)
constructs `SCM_PIDFD` from the message's retained PID object. These are distinct
sources; the inherited-connection test exercises why both are checked. The
host PID shown in `/proc/self/fdinfo` and `poll` of the retained pidfd validate
the original live process rather than independently reopening its PID number.

The actual tests launch non-root subprocesses across a Unix socket and cover:

- accepted request, exact process identity, copied payload, close-on-exec FD,
  process exit, and concurrent/idempotent handle closure;
- inherited connection, wrong UID/GID, stream socket, wrong challenge, empty
  payload, oversized packet, one extra FD and 250 extra FDs causing control
  truncation, with no receiver descriptor growth in each rejection case;
- cancellation before receipt and during a pending receive, with bounded exit
  and no descriptor growth;
- process field validation, changed credentials/PID, missing/duplicate namespace
  fields, invalid start time and exited process status;
- real containerd direct-init, wrapper, shared PID namespace, mutable OCI
  metadata and same-container task replacement using the retained caller handle.

This is stronger than the earlier fixture's `SO_PEERCRED` plus numeric
`pidfd_open` observation. The synthetic payload is explicitly not a Registry
claim or schema-6 startup nonce. No Vela backend factory is dispatched by it.

## Validation

The existing reproducible containerd command now includes the authenticated
caller tests and fails if any selected test skips:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

The normal CPU run passes, including all rejection and real containerd cases.
Full repository unit tests and vet pass. Standard full lint and relevant
integration-tag lint/vet with `GOOS=linux` pass using the repository Go toolchain
and `golangci-lint v2.13.1`. This does not claim a full-repository integration-tag
lint sweep.

Linux race validation also passes without skips or race reports, including
concurrent handle operations and the actual containerd cases (`6.49s` for the
containerd portion). It uses Go `1.26.7` from the existing Linux CPU image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`.
The first, dynamically linked race binary passed the caller-only tests but
could not connect from the nested empty rootfs: its required
`/lib/ld-linux-aarch64.so.1` was absent. A statically linked race rebuild reran
and passed the same complete selection. No rootfs/library mounts were added
to make the nested Runtime fixture more permissive.

The race reproduction is:

```sh
CALLER_TEST_DIR=$(mktemp -d /tmp/vela-runtime-caller-race.XXXXXX)
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true --pids-limit 256 --memory 2g --cpus 2 \
  --tmpfs /tmp:rw,exec,nosuid,nodev \
  --mount "type=bind,src=$PWD,dst=/src,readonly" \
  --mount "type=bind,src=$(go env GOMODCACHE),dst=/go/pkg/mod,readonly" \
  --mount "type=bind,src=$CALLER_TEST_DIR,dst=/out" --workdir /src \
  --env GOCACHE=/tmp/go-cache --env GOTOOLCHAIN=local \
  --entrypoint /usr/local/go/bin/go \
  sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2 \
  test -race -ldflags '-linkmode external -extldflags=-static' \
  -tags=integration -p=2 -c -o /out/nodeagent.test ./internal/nodeagent
docker run --rm --network none --privileged --cgroupns private \
  --pids-limit 256 --memory 1g --cpus 2 --env VELA_TEST_CONTAINERD_SANDBOX=1 \
  --mount "type=bind,src=$CALLER_TEST_DIR/nodeagent.test,dst=/nodeagent.test,readonly" \
  --entrypoint /nodeagent.test \
  sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  '-test.run=^Test(RuntimeContainerdProcessEvidence|RuntimeCallerAuthenticatedMessage|RuntimeCallerRejectsInvalidMessages|RuntimeCallerDeadline|RuntimeCallerProcessParser)$' \
  -test.v -test.timeout=120s
rm "$CALLER_TEST_DIR/nodeagent.test"
rmdir "$CALLER_TEST_DIR"
```

This build requires the dependencies to be present in the mounted module cache.
The static linker warns about glibc NSS lookup functions; the tested Unix-socket
paths do not validate DNS/user-database lookup behavior. This is a CPU test
binary, not a production static-linking or release-image claim.

## Remaining binding

The next layer must correlate the authenticated live process with the trusted
exact container's actual task and namespace-init ownership, authenticate its
effective launch configuration, and durably bind the Node-issued non-reusable
incarnation to the Registry journal pair and startup nonce before factory
dispatch. No endpoint may treat `RuntimeCallerObservation` alone as that grant.
The Runtime client also needs trusted Node socket/server authentication; this
receiver does not implement the opposite direction of authentication.

Independent exact-owner retirement, crash/restart and lost-response recovery,
metadata GC, durable unhealthy/receipt recovery, history reclamation, renewal
write cost and Fleet durable activation remain open. Runtime `UNRESOLVED` and
`LEGACY_UNKNOWN` still permit only recovery endpoints on subsequent starts.
PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1 and
Production Gates `0/9` remain unchanged.
