# MarsLab active object store

This is the post-migration site configuration. The stable `minio` Service now
selects the six `minio-ha` members (two per accepted host), EC:3, six 8Gi claims
with two Longhorn copies each. Approximate S3 data capacity is 24Gi before
filesystem, erasure metadata and operational headroom. Expansion to six 16Gi
claims is deferred under the user's latest instruction to work on other items.

The original four-member StatefulSet stays at zero replicas. Its four 20Gi,
two-copy PVCs remain as the frozen migration source. Do not apply the original
four-member base or the historical 8Gi staging overlay independently to this
site. Do not resume the source or reverse the Service without accounting for
post-cutover destination writes and rechecking storage commitments.

The native bucket/IAM archives, object hash manifest and operational checkpoints
stay in root-only `/opt/vela-cluster/minio-migration/20260914T091919Z` on `.70`.
Credentials remain in `minio-root` and the existing consumer Secrets. See
`docs/minio-ha-validation-2026-09-14.md` for measured migration evidence and the
remaining retention/restore boundaries.

Render with `kubectl kustomize deploy/environments/marslab/object-store`.
Before later changing this site's 8Gi patch, first expand its PVCs through CSI and reconcile
the immutable claim template while retaining Pods/PVCs; applying this overlay
alone cannot perform that migration.
