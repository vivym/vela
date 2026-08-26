# Do not interrupt accepted Jobs for routine releases

Routine releases must not interrupt Accepted Jobs. Database changes follow expand, backfill, switch, and later contract; REST v1, Protobuf, and JetStream events remain compatible across adjacent releases and retained Outbox backlog; Scheduler admits only compatible Worker Agent, runner, ModelRevision, and ExecutionProfileRevision combinations; and Worker rollout canaries, drains, and waits for active Jobs before replacement.

## Consequences

Destructive schema contraction cannot ship with the expansion that replaces it, and normal rollout duration may exceed the longest Job runtime. Quality, failure-rate, or latency regression stops rollout and restores the prior desired version; only an emergency security action may fence active work, and affected Jobs create no Charge.

An adjacent version change that cannot preserve both old and new execution authority is not eligible for ordinary mixed-version rollout. It requires an explicit, fail-closed migration operation: expand in a disabled compatibility state, drain active authority, prove N-1 control and Worker references are zero, atomically switch with an auditable receipt, reject legacy writers after switch, and drain again before rollback. A schema backfill or a new binary's compatibility shim is not by itself evidence that the actual N-1 binary can safely coexist.

## Implementation Status

Partial. Twenty-seven additive migrations, exact N/N-1 database/control
compatibility at fixed migration points, an operator-receipted protocol
transition, migration round trips, and Protobuf/OpenAPI breaking checks are
repository-proven. Migration 00027 adds dedicated debug-dump roles without
widening the N-1 retention or audit role allowlists (`6603c36`). A deployed
mixed control/Worker/event rollout, long-Job drain, rollback, and
retained-backlog receipt remain unimplemented.
