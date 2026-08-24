# Enforce organization isolation on shared infrastructure

Vela uses shared regional PostgreSQL and object storage by default while enforcing Organization Isolation with explicit `organization_id` and `project_id`, composite foreign keys, restricted request database roles, transaction-local identity context, forced row-level security, exact object-key and version authorization, and cross-organization negative tests. Scheduler and Reconciler use a separate internal database role and connection pool rather than inheriting request identity or connections.

## Consequences

The default service does not provision a database or bucket per Customer Organization. Dedicated Deployment is a separately contracted delivery profile, and no customer-facing request path may use the privileged internal database role or list a shared object bucket.

## Implementation Status

Partial. Forced RLS, composite target keys, transaction-revalidated Customer and
Platform Operator sessions, dedicated runtime and NOLOGIN owner roles, exact
object versions, and cross-Organization/Project/role negative tests cover the
implemented APIs. Break-glass grants bind one exact Organization/Project/Job
tuple and cannot be reached through Customer roles or credentials. NATS workload
identity and deployment isolation evidence remain unimplemented.
