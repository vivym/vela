# Self-host state on three control and storage nodes

Launch uses at least three Control/Storage Nodes separate from GPU Workers instead of managed PostgreSQL and object storage. RKE2 control plane and etcd, a replicated CloudNativePG cluster, three-replica NATS JetStream, and a distributed S3-compatible store run across these nodes with anti-affinity, independent disk paths and I/O limits; an existing external S3 store remains the fallback when the available disk topology cannot provide safe erasure coding.

## Consequences

The design retains basic single-node tolerance and off-cluster PostgreSQL WAL and Artifact backups, but accepts manual restoration after loss of the whole control cluster or site and does not build cross-region failover. Sharing nodes reduces infrastructure cost while making resource isolation, disk health, restore drills, and aggregate failure testing production gates.
