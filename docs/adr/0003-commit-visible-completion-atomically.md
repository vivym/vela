# Commit visible completion atomically

After every required Artifact has been uploaded, validated, and bound to immutable object versions, Vela will commit the winning ArtifactSet, Job SUCCEEDED state, one posted Charge, and Artifact access eligibility in one PostgreSQL transaction. If any part cannot commit, the Job remains in FINALIZING for reconciliation; invoice export and downstream notifications occur asynchronously through the Outbox.

## Consequences

Vela has no normal state in which a successful Job lacks its Charge or complete downloadable ArtifactSet, and external payment settlement cannot delay Artifact access. A Customer Cancellation after Billable Start may still create a Charge without Visible Completion under ADR 0002.
