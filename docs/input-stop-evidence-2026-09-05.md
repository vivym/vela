# Stage Worker input cancellation evidence

Date: 2026-09-05
Base checkpoint: `e75ecca`
Scope: local CPU input cancellation; schema remains 88.

## Reproduced defects and repair

A public StreamAgent test stalls a real HTTPS response after its first byte,
then delivers an unsolicited matching StopStage. Before repair the Stop finds
neither an active nor a starting Runtime authority and is ignored. Releasing
the response completes the download, starts the Runtime and obtains a control
START acceptance. The original failing and passing observations are retained at
`/tmp/vela-input-stop-red-20260905.log` and
`/tmp/vela-input-stop-green-20260905.log`.

StreamAgent now registers a digest-bound cancellable input phase before local
journal capacity checks or Resolve. Both direct and unsolicited Stop can cancel
that phase without waiting for Runtime admission. Under the input-state lock,
ExecuteAssignment checks cancellation before removing the phase and entering
the serialized Runtime path. A resolver returning nil after cancellation cannot
continue to Prepare/Start. Invalid assignment shape or member sets are rejected
before input resolution. This is structural validation, not signature validation.

A separate public HTTPSRootInputResolver regression supplies a response body
that cancels its context while still yielding the exact complete content. Before
repair Resolve returns nil and publishes the final file. It now checks context
before creating download state and before final publication, removes the partial
file on this failure, and returns context cancellation. Existing exact-content
revalidation and digest-drift rejection continue to pass.

## Verification

Checks use the executable source introduced with this report:

- Focused input/HTTPS tests: PASS, 0.726 s. Coverage includes direct and
  unsolicited Stop, mismatched authority, unspecified Stop reason, late resolver
  success, and a complete body delivered after cancellation.
- `go test ./...`: PASS; Stage Worker Agent 1.653 s.
- `go test -race ./internal/stageworkeragent ./internal/stageworkertransport
  ./internal/stageworkermembertransport ./internal/modelruntime`: PASS;
  package times 3.961 / 1.633 / 2.714 / 12.414 s respectively.
- `make lint`: PASS, 0 issues. `git diff --check`: PASS.

The body-after-cancellation regression initially failed in 1.124 package seconds
with both a nil Resolve error and a final input file. The repair was applied only
after reproducing that failure.

## Limits and next work

Stop signals input cancellation; it does not report AllStopped or any Runtime
cancellation acknowledgment before Runtime admission. Resolver return and handle
closure remain necessary before local input drain can be established. Context
checks do not atomically serialize publication with cancellation or deletion.
An uncooperative resolver can remain blocked, and an already published valid file
is not deleted by this repair.

This ephemeral input slot is not a persistent replay watermark or a namespace
retirement gate. Replaying the same assignment can still reenter Resolve. The
terminal allocation cutoff, complete Runtime scope, signed disposition and
durable retirement recovery remain in the
[terminal scratch design](terminal-scratch-retirement-design-2026-09-05.md).
Late control acceptance after a Runtime Stop was a separate finding at this
checkpoint, subsequently repaired in the
[response ordering evidence](late-control-stop-evidence-2026-09-05.md).

No GPU, remote lab, database integration, CNPG or load campaign was run for this
change. The previous 512-Job measurements remain bound to `a9a1f7a`.
Production Gates remain 0/9.
