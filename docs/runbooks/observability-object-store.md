# MinIO health, quorum and recovery

Check the cluster metrics endpoint and `/minio/health/cluster` (write), plus
`/minio/health/cluster/read`. On 2026-09-14 the live server reports write quorum 3,
read quorum 2, one four-drive erasure set, and four online drives. Do not sum
cluster metrics across scrape replicas: each endpoint reports the same erasure
set. Quorum rules use the dedicated `/minio/metrics/v3/cluster/erasure-set`
endpoint (`job=minio-quorum`) and aggregate conservatively by
`namespace,service,pool_id,set_id`, keeping separate clusters distinct.

The pinned 2025-04-22 server's `mc admin info` can misreport healthy remote
drives as offline during a two-peer network blackhole. Its local server inventory
serially probes each peer with a 5-second deadline, while the enclosing remote
ServerInfo call allows only 10 seconds. The full v2 metrics endpoint can also
exceed its scrape deadline. Check the narrow v3 erasure-set collector and actual
S3 operations before interpreting this inventory result as disk loss. Keep
inventory scrape failures visible through `VelaMinIOMetricsMissing`; a failed
narrow collector additionally fires `VelaMinIOQuorumEndpointUnavailable`.

The current placement is one member on `.70`, one on `.71`, and two on `.66`.
Loss of `.66` therefore leaves two members, below write quorum, until a member
and its Longhorn volume recover. This is a derived availability limitation from
live topology and server-reported quorum, not a destructive failure experiment.
A successful single-member maintenance check does not test losing both members
on a host. Do not reboot or otherwise disrupt protected `.66` to test this.

`VelaMinIOWriteQuorumLost` and `VelaMinIOReadQuorumLost` require recovering the
unavailable members. `VelaMinIOWriteQuorumAtLimit` prohibits further maintenance
until margin returns. A green HTTP scrape with missing quorum metrics also alerts.
Check Longhorn replica health, attachment and member placement before repair.

Continuous write availability after losing any one host needs a different
validated topology. A separate six-member pool/cluster with two members per host
and a tested parity/quorum policy is a candidate, subject to temporary migration
capacity, object/version and policy preservation, and checksum/read-write tests.
Never edit an existing four-member erasure set into six members in place.

The user accepts intra-cluster replication without an independent physical
failure domain. This does not by itself resolve the four-member write-quorum
limitation or establish a measured recovery time.
