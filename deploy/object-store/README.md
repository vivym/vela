# Cluster-internal MinIO

This directory preserves the original four-member baseline. The current
MarsLab site uses `deploy/environments/marslab/object-store`: six active
members and the original StatefulSet stopped with its PVCs retained. Apply the
site overlay for that cluster; applying this baseline alone would undo routing
and restart the frozen source.

This bundle describes the accepted three-node object store used by the
management cluster. `minio-root` is an externally provisioned immutable Secret;
its values are intentionally absent from the repository.

MinIO runs four erasure-set members with 20Gi Longhorn PVCs. The
`vela.ai/node-role=control-storage` selector keeps future GPU-only workers out
of the object store. A hard topology spread constraint keeps the four members
distributed across all three eligible hosts; the current placement is one
member per CPU node and two on the shared GPU/control-plane node. Longhorn's
default two-way replica setting provides an additional copy for each PVC.

This arrangement supplies replication within one physical failure domain. It
does not provide site, rack, power, or network-domain independence. Keep
versioning enabled. The user accepts this shared physical failure domain;
independent-site disaster recovery is unavailable in the current scope.

Live health headers report read quorum 2 and write quorum 3. Because two members
share `.66`, that host failing leaves only two members and interrupts writes
until recovery. This topology does not provide continuous writes under every
single-host failure. See `docs/runbooks/observability-object-store.md` and the
finite acceptance list before maintenance or topology migration.
