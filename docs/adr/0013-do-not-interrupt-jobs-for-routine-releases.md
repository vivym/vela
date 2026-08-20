# Do not interrupt accepted Jobs for routine releases

Routine releases must not interrupt Accepted Jobs. Database changes follow expand, backfill, switch, and later contract; REST v1, Protobuf, and JetStream events remain compatible across adjacent releases and retained Outbox backlog; Scheduler admits only compatible Worker Agent, runner, ModelRevision, and ExecutionProfileRevision combinations; and Worker rollout canaries, drains, and waits for active Jobs before replacement.

## Consequences

Destructive schema contraction cannot ship with the expansion that replaces it, and normal rollout duration may exceed the longest Job runtime. Quality, failure-rate, or latency regression stops rollout and restores the prior desired version; only an emergency security action may fence active work, and affected Jobs create no Charge.
