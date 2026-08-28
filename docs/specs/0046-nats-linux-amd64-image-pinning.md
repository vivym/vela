# NATS Linux/amd64 image pinning

Date: 2026-08-29

Status: Repository conformance implemented by Slice 46.

Implementation: `760cd7a`; review closure: `431bf3f`.

This slice removes the mutable NATS tag from the Control/Storage deployment
contract. It pins the exact `linux/amd64` manifest used by the three-replica
JetStream StatefulSet and verifies the identity through the final Kustomize
render. It does not pin release-specific Vela or materializer images, publish
or sign an image, deploy RKE2, or change the current `0/9 PASS` Production Gate
result.

## Image identity

The existing runtime version remains NATS `2.10.22`. The configured Docker
mirror was queried with:

```sh
docker buildx imagetools inspect docker.1ms.run/library/nats:2.10.22 \
  --format '{{json .Manifest}}'
```

The tag resolved to multi-platform OCI index
`sha256:d15749dc10d8e67b55f496551ca3794b2f131556342b313eb3e6115ec75f6fb3`.
The deployment pins the contained `linux/amd64` OCI manifest instead:

```text
docker.io/library/nats@sha256:26b0ee1a95285aedae137aefb953701d9da1dfffcf7818eb3aeb536c4373892f
```

A second lookup by that digest returned media type
`application/vnd.oci.image.manifest.v1+json`, the same digest, and size 1220
bytes. The repository records the immutable platform manifest rather than the
mutable tag or multi-platform index so the release bundle can bind the exact
runtime executed by the production `linux/amd64` nodes.

## Public seam

The public seam is the rendered resource graph from:

```sh
kubectl kustomize deploy/control-storage
```

`internal/deploymentcontract.TestRenderedJetStreamUsesPinnedNATSImage` locates
the exact `StatefulSet/vela-system/nats` in that output and requires its sole
ordinary container to use the expected digest reference. It also rejects any
init or ephemeral container. A source YAML change, Kustomize overlay drift, tag
regression, repository change, platform digest change, or additional container
therefore fails repository validation.

## Release boundary

The canonical Slice 40 release bundle still requires the final rendered
Control/Storage YAML plus the matching OCI manifest and config bytes. Slice 45
still requires registry publication, signature, SPDX SBOM, vulnerability scan,
and independent approval evidence for every image in a real release. This
repository pin supplies none of those external artifacts and is not a registry
receipt or Launch Receipt.

The zero-digest Vela, BusyBox materializer, Worker Agent, and H3 Runner values
remain deliberate release-overlay inputs. Their exact identities depend on the
approved release artifacts and must not be inferred from local smoke images.
The example S3 endpoint, real Secrets, storage topology, and RKE2 target also
remain external deployment inputs.

## Verification evidence

- the test was observed failing against `nats:2.10.22` before the deployment
  change and passing after the digest pin;
- the complete `internal/deploymentcontract` package passes; and
- `make validate-deployment` renders all five deployment bases and passes their
  focused contract tests; and
- post-review `make verify`, the focused race test, and `go mod verify` pass.
