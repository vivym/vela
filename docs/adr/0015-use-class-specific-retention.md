# Use class-specific retention

Launch retains successful ArtifactSets and request content for 30 days, with contracted Artifact options of 7, 30, or 90 days; incomplete objects and uploads for at most 24 hours; terminal Worker scratch for at most 24 hours; opt-in debug dumps for at most 72 hours; non-content Job and Attempt metadata for one year; and Charge, Credit Reservation, and settlement audit records for seven years or a longer statutory period. Signed download URLs last 15 minutes and do not alter retention.

## Consequences

Customers may request early Content Deletion, which completes asynchronously within 24 hours without reversing Charges or required non-content audit history. Retention is enforced and evidenced across PostgreSQL, object storage, multipart uploads, Worker scratch, backups, and debug paths rather than delegated only to an object-store lifecycle rule.

## Implementation Status

Partial. Request content, successful and incomplete Artifacts, multipart uploads,
Worker scratch, Local Recovery State, and opt-in debug dumps have repository
expiry or deletion paths. Debug dumps expire at the immutable 72-hour
authorization ceiling, remain separate from customer Artifacts, and use
exact-version or multipart-prefix cleanup with immutable receipts under either
retention or Customer Content Deletion (`6603c36`). Migration 00028 adds
committed-only OFF_CLUSTER_BACKUP targets, an independent least-privilege
Reconciler, all-version/delete-marker purge, two-tier immutable receipts, and
PostgreSQL restore replay after deletion authority is durable. Artifact backup
replication now copies each committed PRIMARY exact version into the versioned
backup with immutable evidence, independent runtime and storage authorities,
response-loss recovery, and copy/delete serialization (`c08ba84`). Migration
00030 adds immutable non-content Legal Hold placement/release events for exact
Organization, Project, or Job targets and only `METADATA`/`FINANCIAL` classes.
An independent Compliance Principal, PostgreSQL role, and TLS 1.3 mutual-auth
listener own that authority; no hold can preserve Customer Content or delay its
24-hour deletion contract. Migration 00031 physically expires terminal Job and
Attempt metadata after 365 days and Job/Organization financial sources after
2557 days, while minimal roots preserve independent evidence and exact active
Legal Holds serialize with deletion (`7c12884`). Live provider and network fault
receipts, restore points before deletion authority, production expiry
configuration/credentials/failover/observability evidence, live production
scratch lifecycle evidence, and Launch Receipts remain unimplemented.
