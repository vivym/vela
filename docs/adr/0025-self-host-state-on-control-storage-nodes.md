# Self-host state on three control and storage nodes

Launch uses at least three Control/Storage Nodes separate from GPU Workers instead of managed PostgreSQL and object storage. RKE2 control plane and etcd, a replicated CloudNativePG cluster, three-replica NATS JetStream, and a distributed S3-compatible store run across these nodes with anti-affinity, independent disk paths and I/O limits; an existing external S3 store remains the fallback when the available disk topology cannot provide safe erasure coding.

## Consequences

The design retains basic single-node tolerance and off-cluster PostgreSQL WAL and Artifact backups, but accepts manual restoration after loss of the whole control cluster or site and does not build cross-region failover. Sharing nodes reduces infrastructure cost while making resource isolation, disk health, restore drills, and aggregate failure testing production gates.

## Implementation Status

Partial. `deploy/control-storage` renders three-instance CloudNativePG and
three-replica JetStream contracts with required anti-affinity, disruption
budgets, independent WAL storage, and off-cluster backup selectors. Slice 38
adds the Barman Cloud Plugin `ObjectStore`, plugin WAL archiver, immediate and
daily base backups, and exact third-party manifest/image identities; its fresh
four-node kind/MinIO drill completes a real timestamp restore (`4f4bc2d`,
credential-isolation review closure `e8a4149`). The install render gives the
Barman principals only exact `vela-backup-s3` access and denies Artifact
credential reads. Slice 46 pins the JetStream StatefulSet to the exact NATS
`2.10.22` `linux/amd64` OCI manifest and verifies that identity through the
final Control/Storage Kustomize render (`760cd7a`, review closure `431bf3f`).
The repository does not prove three physical RKE2 Control/Storage Nodes, independent
disks, colocated I/O isolation, production operator installation, secret
rotation, durable external S3, or a real-environment failover/restore Launch
Receipt.
