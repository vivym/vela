# Internal release registry access

The three TLS release endpoints are `10.1.201.70:5005`, `.71:5005`, and
`.66:5005`. They now require authentication. Docker Hub and upstream caches on
ports 5000–5004 keep their existing configuration. Host/Docker/RKE2 restarts are
not required by this rollout. Live evidence is in
[`docs/registry-access-validation-2026-09-15.md`](../../docs/registry-access-validation-2026-09-15.md).

| Identity | Read | Publish | Catalog/delete |
| --- | --- | --- | --- |
| `platform-publisher` | All repositories | All repositories | Yes |
| `node-pull` | All repositories | No | No |
| `llm-api-publisher` / `llm-api-pull` | `llm-api/` | Publisher only, same prefix | No |
| `llm-models-publisher` / `llm-models-pull` | `llm-models/` | Publisher only, same prefix | No |

The Nginx gateway authenticates each request and verifies TLS to Distribution
bound only to `127.0.0.1:5007`. Passwords are 32 random bytes encoded as URL-safe
text. The gateway stores SHA-512 password hashes. Plain credentials and client
configurations stay in root-private files. Tenant credentials cannot mount blobs
from other repository prefixes. Ambiguous encoded/traversal paths are rejected.

## Existing deployment

On `.70`, `/opt/vela-cluster/registry-access-20260914/live/` contains the protected
production credentials, per-role `docker-clients/<identity>/config.json`, and
safe receipts. Do not print or commit credential files, Docker auth values,
Ansible inventories or host `input.json` files.

Kubernetes uses immutable `vela-release-pull-v1` Secrets in `vela-system`,
`llm-api`, and `llm-models`. Control uses `node-pull`; the two application runtime
ServiceAccounts use their own read-only identity. All three endpoint names are
included in each Docker config so containerd mirror fallback can authenticate.
No global node credential was installed. Existing ServiceAccount pull references
are preserved. Control's Pod template includes the reference explicitly.

Application images use the tenant prefix and an immutable digest, for example
`10.1.201.70:5005/llm-api/service@sha256:<digest>`. Runtime credentials do not grant
publication permission. An operator can run Docker with a scoped existing config:

```sh
sudo docker --config /opt/vela-cluster/registry-access-20260914/live/docker-clients/llm-api-publisher \
  push 10.1.201.70:5005/llm-api/service:RELEASE
```

This is an example for a deliberately built release image, not an instruction to
publish the current dirty checkout. Shared role passwords are automation identities;
they do not provide individual SSO attribution. Rotate with a new Secret revision,
update consumers, verify pulls, then remove the old credential from all gateways.

## Image availability and promotion

The three Distribution stores are independent. A configured mirror list does
not replicate content. The current Control OCI index, child manifests and blobs
have been copied and SHA-256 checked at both peers. Future releases must be
promoted before changing workload digests:

```sh
sudo python3 hack/replicate-registry-image.py \
  --repository llm-api/service --digest sha256:EXACT_DIGEST \
  --target 10.1.201.71 --target 10.1.201.66 \
  --credentials /opt/vela-cluster/registry-access-20260914/live/credentials.json \
  --user llm-api-publisher --receipt /root/registry-promotion.json
```

The tool preserves multi-platform indexes/attestations, appends by digest, verifies
destination bytes and does not move existing tags. Background replication and
garbage collection have not been configured. Test manifests were deleted;
small unreferenced test blobs can remain until a separately planned GC.

## Reproduction and rollback

For a new installation, run `hack/prepare-registry-access.py` on `.70`, passing
the host installer and this Nginx template. It verifies the image archive and
prepares each host without changing live ports. Then run
`hack/configure-registry-pull-secrets.py`, wait for both Control replicas to be
Ready, and run `hack/cutover-registry-access.py`. Existing preparation state is
not overwritten; inspect receipts to resume after a failure.
Run Kubernetes steps with `KUBECONFIG=/etc/rancher/rke2/rke2.yaml` and
`/var/lib/rancher/rke2/bin` in PATH. Preserve those explicit values through sudo.

The monitoring bundle's `registry-probes.yaml` and `Vela · Internal Registry`
dashboard watch all three endpoints. `monitoring/vela-registry-prober` holds only
the read-only password and CA. It is independent of the immutable application
pull Secrets. Keep probe credentials synchronized during a deliberate rotation.

Each host keeps root-private `/opt/vela-registry-access/receipt.json`, the original
Docker configuration and the stopped original container. New backend/gateway
containers have `restart=always`; the retained old container has restart disabled.
Automatic rollback is local to a failed cutover. Explicit rollback on that host:

```sh
sudo python3 /opt/vela-registry-access/install.py --phase rollback
```

Rollback removes only recorded replacement container IDs, restores the original
name/restart policy and checks the old endpoint. It restores anonymous access and
therefore is a recovery operation, not the normal steady state. Storage contents
and registry certificates are preserved. Keep the other authenticated endpoints
serving while diagnosing a failed host.
