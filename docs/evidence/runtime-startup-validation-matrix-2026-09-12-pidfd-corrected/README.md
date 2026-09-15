# Corrected validation launcher receipt

This directory contains the full six-scenario rerun after correcting the
validation launcher pidfd path. The target was `marslab@100.111.196.116` with
Ubuntu 24.04, kernel `6.8.0-137-generic`, and k3s CRI
`/run/k3s/containerd/containerd.sock`.

The Runtime and Worker containers run a wrapper from the launcher binary. The
wrapper obtains `pidfd_open(getpid())` before `exec`, then sends that descriptor
over the mounted `SOCK_SEQPACKET` offer socket. The host launcher therefore does
not recreate a pidfd from a CRI numeric PID.

Observed result:

- `normal`: `completed`, `launcher_exit=0`
- five controlled failure scenarios: `failed`, with explicit error receipts
- every scenario: historical `fd_count=3` after handoff and `cleanup_verified=true`
- every scenario: exact Runtime/Worker containers and sandbox removed from CRI

The launcher offer receiver additionally binds each offered descriptor to the
authenticated Unix peer pidfd and to the current invocation's CRI task PID while
the kernel handle is live. Wrong task, wrong UID, regular-file, and different
process descriptors are rejected by native tests on the target host.

This is validation-only evidence. It does not constitute a production launch
receipt, Fleet permission, journal custody, Node restart proof, or a Production
Gate pass; the overall status remains `0/9`.

This directory predates launcher protocol v2. Use
`../runtime-startup-validation-matrix-2026-09-13-v2/` for current four-descriptor
helper evidence.
