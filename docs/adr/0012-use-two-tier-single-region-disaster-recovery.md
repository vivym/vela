# Use two-tier single-region disaster recovery

Vela targets zero data loss and control-plane recovery within five minutes for a single node, disk, or control-plane instance failure. For operator error or loss of the primary site, PostgreSQL WAL is archived to an independent fault domain with metadata RPO at most fifteen minutes and RTO at most four hours; PostgreSQL restore, JetStream rebuild, and Outbox replay are exercised quarterly rather than inferred from successful backup jobs.

## Consequences

Launch does not build cross-region active-active control or Vela-managed cross-region Artifact replication. The selected regional S3-compatible store, whether self-hosted on Control/Storage Nodes or supplied by an existing external service, must provide versioning, verified durability, and off-cluster backup. Full regional Artifact loss and GPU serving-capacity restoration remain outside the single-region 99.9% API SLO and must be disclosed in the customer contract.

## Implementation Status

Partial. Slice 38 replaces CloudNativePG's deprecated native object-store
backup surface with a digest-pinned Barman Cloud Plugin `ObjectStore`, WAL
archiver, and immediate plus daily base-backup schedule. A fresh four-node
kind/MinIO drill completed a plugin base backup, archived the target WAL, and
restored a second cluster to a timestamp between two durable markers
(`4f4bc2d`, credential-isolation review closure `e8a4149`). The release-owned
install render restricts both Barman principals to the exact backup Secret and
denies Artifact credential reads. This is repository and local recovery-path
evidence, not proof of production RKE2, an independent S3 fault domain,
provider/network failure,
JetStream rebuild, Outbox replay, credential rotation, the four-hour site RTO,
or a quarterly `data-disaster-recovery` Launch Receipt.
