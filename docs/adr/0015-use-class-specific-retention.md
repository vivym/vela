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
retention or Customer Content Deletion (`6603c36`). Off-cluster backup
expiry/replay, metadata and financial lifecycle enforcement, legal holds, live
production scratch lifecycle evidence, and Launch Receipts remain unimplemented.
