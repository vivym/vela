# Stage Worker control stream generation evidence

Date: 2026-09-05. Baseline: `1a99484fbadffcb0ac6d6edd3bf2a5ea471ccda2`.
Evidence class: local CPU regression. Production Gate: false.

## Findings and resulting behavior

The Connect receiver previously outlived cancellation when its unsolicited
command queue was full. Checking the generation before a blocking channel send
was insufficient: replacement could occur between that check and send, and
commands already queued before replacement remained visible to the next session.
Late readiness responses could also affect the current epoch observer.

The client now admits bounded queue entries under the same mutex as stream
replacement. Entries carry their originating generation. `NextCommand(ctx)`
discards entries whose generation is no longer active at consumer admission;
it replaces the internal Go `Commands()` channel API. A full queue fails the
stream and wakes its pending exchanges instead of blocking the receiver while
holding a mutex. Normal control reconnect and exact command replay remain in
force. No protobuf or database schema change is involved.

A command admitted before a subsequent renewal or assignment can still be
superseded before execution. Unsolicited Stops for a superseded authority are
nonfatal; synchronous `HandleStop` retains its strict mismatch error. Local
Runtime authority installation and cancellation are serialized by `runtimeMu`,
but waiting for a control ACK does not hold that mutex. Authority already
accepted by Runtime is retained as `startingAuthority` through pending START or
Reattach confirmation. Both Stop paths use that same authority. A late heartbeat
or reattach response cannot reinstall authority after the expected local active
state has changed.

A separate exchange cleanup race allowed an old blocked Send to return after
reconnection and delete the new exchange's waiter with the same durable command
ID. Cleanup now requires the original waiter identity, preserving the replacement
exchange even when request IDs are equal.

## Reproduction and verification

| Case | Before repair | Verification |
| --- | --- | --- |
| Canceled receiver with full queue; late readiness/result/Stop | Deterministic cancellation/generation regression failed, 0.648 s | Internal synctest cases pass in the final race suite |
| Already queued Stop from replaced connection | Both same-attempt and different-attempt real gRPC cases consumed the old Stop, 0.637 s | Public `NextCommand` returns only the current generation's Stop |
| Superseded Stop after renewal | Consumer exited on authority mismatch, 0.643 s | Consumer continues and handles the subsequent current Stop; direct HandleStop still rejects mismatch |
| Stop while control START/heartbeat ACK is blocked | Both cases waited for the blocked ACK, 2.697 s | START, heartbeat and Reattach gates remain blocked while all real test Runtime members reach CANCELING |
| Reattach with renewed Runtime authority and previous active authority | Both members remained RUNNING despite consuming the Stop, 0.517 s | Renewed Stop reaches the same current authority in both cancellation checks |
| Delayed old Send cleanup after same-ID reconnect | Identified by concrete code interleaving; no separate pre-fix run claimed | Deterministic delayed-Send regression preserves the new waiter |

Final command:

```sh
go test -race ./internal/stageworkertransport ./internal/stageworkeragent ./cmd/vela-stage-worker-agent -count=1
```

All three packages passed in `1.687`, `2.835`, and `3.468` seconds respectively.
Full golangci-lint v2.13.1 reported `0 issues`.

Logs:

- `/tmp/vela-client-generation-red.log`
- `/tmp/vela-command-generation-red.log`
- `/tmp/vela-stop-consumer-red.log`
- `/tmp/vela-stop-liveness-red.log`
- `/tmp/vela-stop-renewed-reattach-red.log`
- `/tmp/vela-control-command-generation-final-race.log`
- `/tmp/vela-schema86-final-lint.log`

Final source SHA-256:

| File | SHA-256 |
| --- | --- |
| `internal/stageworkertransport/client.go` | `eef99013ed87e59ee0c129c42ec95db88c00583c0bc95cb8643dc79166b068de` |
| `internal/stageworkeragent/stream.go` | `5cc793b3056ce70728c1c7e60357e72e52aed89a8952521335e9b06b95fc615a` |
| `internal/stageworkeragent/materialization.go` | `6450467f8e10db209c684f77206f7d7fc8f304bb80edadb674e47bb9e17a1d5b` |
| `internal/stageworkertransport/client_generation_test.go` | `c146a7efa6dd67cb887633eb58c27237ba52ae824f60ab206d05513117191301` |
| `internal/stageworkertransport/command_generation_test.go` | `badfb93c34e2027d97d8d47bc903175b341a919bfd31a0b6d75133ee416a6c95` |
| `internal/stageworkertransport/client_pending_generation_test.go` | `2b748e3d24b860a26e96b5034b1df3304009b718453236e93b947c82425d54da` |
| `internal/stageworkeragent/stop_liveness_test.go` | `201c21152f01fa46d3f0111603453331fb50bff474cd926ed51f6562fe71c1a7` |
| `internal/stageworkeragent/stream_test.go` | `62812e4b282a449f2f0fb0408fe8b66268354c6cb33cb67b9f6e16e8355d4737` |

These tests establish cancellation signaling and command isolation at the named
boundaries. CANCELING is not STOPPED, and none of these results authorize scratch
deletion or prove physical GPU process termination. Runtime-unavailable and
historical terminal scratch recovery remain separate lifecycle work.
