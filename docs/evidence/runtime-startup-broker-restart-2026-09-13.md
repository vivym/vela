# pidfd broker restart validation (2026-09-13)

This is validation evidence only. It does not change `Production Gates=0/9`.

The Linux amd64 `runtimechannel.test` binary was built from the current
worktree, copied to `marslab`, and run as root with
`-test.run=^TestPIDFDBrokerComparesRetainedHandles$`. The test now exercises a
broker restart after the first listener is canceled: the old socket is removed,
a downtime request is rejected with `ErrPIDFDBrokerUnavailable`, then a new
root-owned `unixpacket` listener is started and a fresh SCM_RIGHTS request
succeeds. The existing matrix also covers same-process, different
process, nested PID namespace, and wrong-GID requests.

Observed result:

```text
=== RUN   TestPIDFDBrokerComparesRetainedHandles
--- PASS: TestPIDFDBrokerComparesRetainedHandles (0.07s)
    --- PASS: .../same (0.01s)
    --- PASS: .../different (0.01s)
    --- PASS: .../nested-same (0.01s)
    --- PASS: .../wrong-gid (0.01s)
PASS
```

The test proves broker protocol restart and descriptor ownership for the local
fixture. It does not prove the complete Node composition, Fleet reservation,
ModelRuntime Permit, or production bundle installation.
