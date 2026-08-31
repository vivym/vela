# Three-host lab observability stack

This directory deploys a deliberately small, non-production Prometheus,
Alertmanager, and Grafana stack to `vela-lab-control-1`. It scrapes the private
Vela management endpoint, loads the checked-in SLO rules and dashboard, and
routes a lab-only heartbeat alert to a null receiver. It does not configure an
external paging integration and cannot create a Production Gate receipt.

## Fixed boundary

- The three `linux/amd64` image manifests in `images.env` total 624,172,137
  compressed bytes (595.26 MiB). The Prometheus Docker Hub and Quay releases
  resolve to the same content digest; publication uses Docker Hub so the
  control host's existing mirror can carry the bytes without SSH transfer.
- Runtime image references use only `10.1.200.17:5443/observability/*` plus an
  immutable digest.
- All three Pods select `vela.ai/node-role=control-storage`, request no GPU,
  use bounded `emptyDir` storage, and expose only ClusterIP Services.
- The `vela-observability` namespace and Prometheus Pod must jointly present
  `vela.ai/network-role=observability` and
  `vela.ai/client-role=otel-collector`. The control policy admits port 8081
  only from that combined identity.
- The lab heartbeat proves Prometheus-to-Alertmanager delivery only. Its
  receiver is `lab-null`; it does not prove notification delivery,
  acknowledgement, 24x7 staffing, an incident exercise, or any of the nine
  Production Gates.

No global Docker prune, image prune, Docker restart, Registry restart, Worker
filesystem change, or shared-container change is part of this procedure.

## Local validation and render

```text
make test-lab-scripts

rendered=$(mktemp -d)/rendered
deploy/lab/observability/render-manifests.sh \
  deploy/lab/observability/images.env "$rendered"
```

Copy the small checked-in directory and rendered manifests to a new root-only
directory on `marslab-server`. Image layers are never copied over SSH.

## Private Registry publication

The publication state directory must be new, root-owned, and mode `0700`.
Publication verifies the shared container and Registry identities before
pulling through the existing Docker Hub mirror and pushing over the LAN-local
Registry endpoint.

```text
sudo deploy/lab/observability/publish-images.sh \
  /srv/vela-rke2-airgap/observability-v3.14.0-v0.34.0-13.2.0 \
  /etc/vela-registry/secrets/vela-rke2-publisher.username \
  /etc/vela-registry/secrets/vela-rke2-publisher.password \
  --apply
```

If and only if the retained state says `status=in_progress`, inspect which
private tags exist and repeat with `--apply --resume`. A complete state is not
resumable and must be verified rather than overwritten.

If a Docker mirror supplies an unpackable image but omits one compressed
content blob needed for digest-preserving push, do not transfer the whole image
archive. Download only the missing blob through an approved proxy, verify its
published size and SHA-256, install it as a root-owned `0600` file, and use the
guarded Registry V2 helper before resuming:

```text
sudo deploy/lab/observability/upload-verified-blob.sh \
  observability/grafana /root/vela-observability-ops/blob \
  <exact-bytes> sha256:<exact-digest> \
  /etc/vela-registry/secrets/vela-rke2-publisher.username \
  /etc/vela-registry/secrets/vela-rke2-publisher.password \
  --apply
```

The helper accepts only the three fixed observability repositories, verifies
host/container identities, file ownership, byte count, and digest, rejects an
unexpected upload URL, then confirms the private blob with an authenticated
HEAD request. Retain this action as diagnostic publication evidence; it is not
a Production Gate receipt.

## Deploy and verify

`deploy.sh` requires an idle Vela authority boundary of `0|0|0|2`: zero active
Attempt Leases, zero Production Gate receipts, zero active Jobs, and two
READY/HEALTHY Workers. It server-dry-runs the rendered objects, records the
existing control NetworkPolicy, applies the namespace and stack, then invokes
`verify.sh`. Any failure deletes only `vela-observability` and restores the
captured control policy.

```text
sudo deploy/lab/observability/deploy.sh \
  /root/vela-observability-ops/rendered \
  /root/vela-rke2-receipts/YYYYMMDDTHHMMSSZ-observability \
  --apply
```

The verifier requires all three Pods on the control node, exact private image
digests, zero GPU requests, the SLO exporter success gauge at one, no forbidden
identity labels, all canonical rule groups, the provisioned dashboard, a
healthy Grafana datasource, and the lab heartbeat in Alertmanager. It records
the current `vela_gateway_sli_requests_total` series count without converting
missing external-gateway telemetry into a pass.

For temporary local UI access, start a root-only port-forward on the control
host and carry it through SSH; do not add a NodePort or public Ingress:

```text
sudo env KUBECONFIG=/etc/rancher/rke2/rke2.yaml \
  /var/lib/rancher/rke2/bin/kubectl --namespace vela-observability \
  port-forward --address=127.0.0.1 service/vela-lab-grafana 13000:3000

ssh -L 3000:127.0.0.1:13000 marslab@100.111.196.116
```

## Explicit rollback

Rollback requires the successful deployment receipt so the exact pre-deploy
control policy can be restored. It deletes only the labeled lab namespace;
Registry and Docker images are retained, and no prune is run.

```text
sudo deploy/lab/observability/rollback.sh \
  /root/vela-rke2-receipts/YYYYMMDDTHHMMSSZ-observability \
  /root/vela-rke2-receipts/YYYYMMDDTHHMMSSZ-observability-rollback \
  --apply
```
