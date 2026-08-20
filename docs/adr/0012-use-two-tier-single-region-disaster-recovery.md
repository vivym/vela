# Use two-tier single-region disaster recovery

Vela targets zero data loss and control-plane recovery within five minutes for a single node, disk, or control-plane instance failure. For operator error or loss of the primary site, PostgreSQL WAL is archived to an independent fault domain with metadata RPO at most fifteen minutes and RTO at most four hours; PostgreSQL restore, JetStream rebuild, and Outbox replay are exercised quarterly rather than inferred from successful backup jobs.

## Consequences

Launch does not build cross-region active-active control or Vela-managed cross-region Artifact replication. The selected regional S3-compatible store, whether self-hosted on Control/Storage Nodes or supplied by an existing external service, must provide versioning, verified durability, and off-cluster backup. Full regional Artifact loss and GPU serving-capacity restoration remain outside the single-region 99.9% API SLO and must be disclosed in the customer contract.
