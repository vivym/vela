# Node / Runtime authenticated request and response

This increment follows `7264c9b` on `feature/vela-mock-hardening`. It adds local
message transport, not backend startup permission, retirement or a Launch
Receipt. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1,
release bundle 3 and Production Gates `0/9` remain unchanged.

## Implemented contract

`internal/runtimechannel.Exchange` is the Linux Runtime client. The transport
package has no dependency on Node Agent or ModelRuntime. Node Agent retains its
opaque process observer and shares the framing constants and descriptor parser
with the client; ModelRuntime can use the client without an import cycle.

The client runs with non-root effective UID/GID. Every path component is opened
relative to retained directory descriptors with `O_NOFOLLOW`. Ancestors must be
root-owned and not writable by group/others; even a sticky world-writable
directory is excluded. The socket must be root-owned, mode `0660`, with the
Runtime's effective GID. `O_PATH` retains the socket inode, including across
unlink, and connection uses the retained parent directory. Checks before
request transmission and after response receipt reject visible replacement of
the socket or its directory. Filesystem ownership alone is insufficient: the
kernel connection peer must have UID/GID `0/0`.

The client enables `SO_PASSCRED` and `SO_PASSPIDFD` before connect, then retains
`SO_PEERPIDFD`. Every challenge and response must carry matching kernel
credentials and `SCM_PIDFD`. Both pidfds must have close-on-exec set and remain
live. On pidfs kernels they must name the same pidfs device/inode. Older kernels
use the kernel-maintained anonymous pidfd `fdinfo` (`Pid` and complete `NSpid`
chain) for the equality check; no process is reacquired from a numeric PID and a
missing handle cannot be reconstructed. This client does not read the host
process through container procfs.

The namespace experiment explains why numeric PID comparison is insufficient:
from Runtime PID 1, both its original root Node and a different live root
delegate report `peer_pid=0`, `message_pid=0`, UID 0 and GID 0. The original
sender's retained pidfs identity matches; the delegate's does not. The test
keeps both original and delegated processes alive through comparison.

Request framing remains `vela-runtime-caller-v1\0`, 32 random challenge bytes,
then a nonempty payload of at most 32 KiB. The response begins with the distinct
`vela-runtime-response-v1\0` prefix, the entire original challenge, and another
nonempty payload of at most 32 KiB. Distinct direction framing rejects reflected
requests; challenge binding rejects a response from another exchange. Truncated
frames, extra descriptor rights and malformed framing reject while releasing
all received descriptors.

`RuntimeCaller.Reply` attempts one response on the original accepted connection.
Concurrent attempts are serialized and only one can submit it. Closed, expired
or exited caller ownership rejects. The caller of `ReceiveRuntimeCaller` still
owns and closes that connection; it must not perform concurrent socket I/O.
`RuntimeCaller.Close` releases proof handles without closing that socket.

Dial and initial framing have five-second bounds. The client has a 45-second
total exchange bound; the server reply expires 45 seconds after authenticated
request receipt. Earlier caller deadlines take precedence. Cancellation wakes
blocked I/O. A race run exposed that the network timer can fire before the
context timer at the same deadline, losing `context.DeadlineExceeded`; the final
client and reply code also check the absolute context deadline when preserving
the error cause. Successful transmission is not acknowledgment of receipt.

## CPU validation

The mandatory containerd wrapper includes four new main tests:

- `TestRuntimeChannelRoundTrip`: normal and independent-PID-namespace clients,
  authenticated request/response content, eight simultaneous reply attempts
  with exactly one success, and rejection after caller closure.
- `TestRuntimeChannelRejectsUntrustedExchange`: 24 challenge, response, path,
  peer-credential and cancellation cases. Delegation and all framing cases run
  with Runtime as namespace PID 1. Identity rejection cannot be replaced by a
  timeout. Root-owned socket files backed by a non-root UID or non-root GID
  listener also reject, independently of path validation.
- `TestRuntimeChannelReplyRejectsLostLifetime`: expired response lifetime,
  actual caller exit and canceled response context.
- `TestRuntimeChannelPIDNamespaceIdentity`: directly reads equal PID-zero root
  credentials for distinct live processes and proves different pidfs identity.

The client helpers check descriptor counts after success and every rejection.
The full wrapper also retains all previous caller descriptor-exhaustion,
containerd, launch observation, image recovery and volatile-state-reset checks.

Final validation passed on Docker `28.3.2`, Linux/arm64, with containerd `v2.3.1`
revision `64b425cf570b3b8dd1d4cc46da7c1fce65c6651a` and runc `1.4.2`:

- Complete CPU wrapper: **157.199 seconds**, all **44 mandatory main tests**
  plus both volatile-state-reset scenarios passed, with no selected skips.
- Static Linux race binary: all **9 selected main tests** passed with no skips
  or race reports. These are the four new channel tests plus caller message,
  invalid-message, deadline, process-parser and actual descriptor-exhaustion
  tests. All 24 untrusted-exchange cases passed, including the previously
  failing context-deadline case.
- Linux/arm64 integration-tag vet and pinned golangci-lint `v2.13.1` passed for
  `internal/runtimechannel`, `internal/nodeagent` and `cmd/vela-node-agent`, with
  zero lint issues. The working diff passed whitespace checks.

The complete wrapper is reproducible with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Race compilation used Go `1.26.7` from builder image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`,
the same static link flags and isolated sandbox command documented in the
[caller evidence](node-runtime-caller-evidence-2026-09-06.md#validation),
with this test selection:

```text
^Test(RuntimeChannelRoundTrip|RuntimeChannelRejectsUntrustedExchange|RuntimeChannelReplyRejectsLostLifetime|RuntimeChannelPIDNamespaceIdentity|RuntimeCallerAuthenticatedMessage|RuntimeCallerRejectsInvalidMessages|RuntimeCallerDeadline|RuntimeCallerProcessParser|RuntimeCallerPIDFDExhaustion)$
```

The builder mounted the repository and existing Go module cache read-only,
disabled networking, and wrote only to a task-owned output/cache directory.
The static linker emitted the previously documented glibc NSS lookup warnings;
this is CPU test-binary validation, not production static-link qualification.

Retained final output SHA-256 values:

```text
full-cpu-final.log   1d2324e577bdaf6e47f3a48b77a9c9c2cf89135fa05bbc863913f6441f2c6004
race-tests-final.log ff21537f1924250268a4cfd6f2e1ab33d2c981e42d97eae898f6719f447cc737
```

## Remaining lifecycle work

The client is a library; Node command serving and Runtime command startup
assembly do not call this protocol yet. The trusted root Node and its socket
directory must come from the supported deployment. Message authentication does
not prove that an earlier sender has not executed another executable, measure
loaded configuration, or authorize the response's domain content. Root Node
compromise and release-image conformance are outside these CPU fixtures.

Before factory dispatch, startup permission still needs the actual held journal,
Registry pair, member/device ownership, startup nonce and independently retained
containment incarnation. Retiring an old incarnation needs positive evidence
from an independent owner; missing metadata, process disappearance, a fresh
namespace and normal `Close()` remain insufficient. Lost replies cannot permit
another factory dispatch. This increment does not change recovery-only behavior
for `UNRESOLVED` or `LEGACY_UNKNOWN` Runtime journals.
