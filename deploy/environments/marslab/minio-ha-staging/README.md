# MarsLab isolated MinIO migration staging

Historical pre-cutover configuration. The client Service was switched to the
six-member cluster on 2026-09-14; use `../object-store` for the active site.
The following describes the isolated validation phase, not current routing.

Render with `kubectl kustomize deploy/environments/marslab/minio-ha-staging`.
This overlay reproduces the six live `object-store/minio-ha` members with
8Gi claims, two Longhorn copies per claim, and explicit EC:3 for both storage
classes. The final topology in `deploy/object-store/ha-six` requests 16Gi per
member. Both use the same pinned image and colon-prefixed listener addresses.

The existing `object-store/minio` Service still selects `app=minio`; it is not
changed by this overlay. Applications continue to use the original four-member
cluster. The staging services select only `app=minio-ha`, and are deliberately
excluded from the existing MinIO ServiceMonitor until its cluster labels and
quorum aggregations are reconciled. Credentials are referenced from the existing
`minio-root` Secret and are never embedded here.

The 8Gi staging size accommodates concurrent old/new cluster commitments; it is
not the final storage capacity promise. Six claims require 96Gi of Longhorn
commitments at two replicas. Before expanding, recheck disk commitments,
replica placement and physical free space. Kubernetes cannot update a
StatefulSet's volumeClaimTemplates in place. Any final transition must preserve
these PVCs, expand them through the supported CSI path, and deliberately
reconcile the retained StatefulSet template. Do not apply the 16Gi base over
this StatefulSet and assume its claims expanded.

`hack/verify-minio-ha-quorum.py` is limited to synthetic data on this isolated
cluster. A member outage exercise does not prove whole-host or Longhorn failure
recovery. Migration and service cutover additionally require inventory and
verification of all versions, delete markers, bucket configuration, IAM and
application credentials. Original data must remain until that verification
passes.
