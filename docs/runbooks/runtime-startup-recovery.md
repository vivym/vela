# Runtime startup recovery runbook

This runbook prevents a failed canary from being retried against consumed
WorkerInstance authority. Runtime startup is deliberately fail-closed: a
restart never infers that a previous model process exited.

## Required preflight

Run the read-only composition preflight on every target node with the exact
release environment:

```bash
python3 hack/runtime_startup_composition_preflight.py --json
```

The preflight must report:

- a pinned runtime image and all required files/sockets with trusted parents;
- `runtime_journal_backend_lifecycle` equal to `UNSTARTED` or `RETIRED`;
- `runtime_startup_ledger` with no active startup lacking an exit record;
- absent `launch`, `worker-bootstrap` and `runtime-bootstrap` directories.

`UNRESOLVED`, an active startup ledger entry, or a leftover publication
directory is `requires-reprovision`. Do not restart the service repeatedly and
do not remove the journal files by hand.

## Failure handling

If the service fails before a bootstrap receipt is recorded, use the existing
authenticated `bootstrap --action abandon` operation. It fences only the
unobserved first-use Worker authority and never grants a replacement.

If a Runtime backend startup intent was recorded, the same Node process can
retire it only through the Node startup ledger. The ledger must observe the
original namespace owner through its retained pidfd, persist the exit event, and
then call `RetireBackendIncarnation`. This produces a durable `RETIRED` journal
state. The next startup creates a new incarnation while preserving the old
evidence.

If Node restarted or the original pidfd was lost before the exit event was
persisted, retirement is intentionally unavailable. Provision a new
WorkerInstance, member, bootstrap claim, journal pair and Pod. The old claim
must be abandoned only when its database preconditions permit abandonment.

## Release automation rules

The rollout driver must execute every target's preflight before mutating any
target. It must stage all files and native image snapshots first, then start
agents only for targets that passed. A failure on one node must not cause the
driver to retry a different phase against the same request ID. Every request
ID, WorkerInstance ID, journal ID, image digest and preflight report belongs in
the rollout evidence directory.

The following are not recovery actions:

- deleting `execution-admission.json` or `runtime-startups.jsonl`;
- removing Kubernetes scheduling gates by hand;
- changing the request ID while reusing the same WorkerInstance/member;
- treating a missing PID, an empty process list, `Close()` success, or a
  containerd garbage-collection result as retirement evidence.

## Acceptance gates

Before declaring a canary ready, verify in order:

1. every target has a passing preflight report;
2. every Node Agent is active without restart loops;
3. every Pod has its gate released by the authenticated Node Agent;
4. Runtime and Worker journal receipts match the Registry binding;
5. the model process reaches readiness and returns a real artifact;
6. a controlled Node Agent restart leaves the service either running under the
   same retained owner or explicitly recovery-only, never silently replaced;
7. a second rollout uses new authority and succeeds after the first rollout is
   retired.

These checks validate deployment recovery. They do not replace GPU quality,
latency, billing, or output-equivalence acceptance tests.
