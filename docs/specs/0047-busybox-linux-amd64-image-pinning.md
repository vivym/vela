# BusyBox Linux/amd64 materializer image pinning

Date: 2026-08-29

Status: Repository conformance implemented by Slice 47.

Implementation: `6d916bb`; review closure: `9f4063e`.

This slice removes the zero BusyBox digest from the three repository-owned root
materializer inputs. It pins one exact `linux/amd64` manifest for the
`vela-control` Secret materializer, the static H3 Worker initializer, and the
Fleet desired revision that produces the same Worker Pod template. It does not
replace release-specific Vela image placeholders, publish or sign an image,
deploy RKE2/H3, or change the current `0/9 PASS` Production Gate result.

## Image identity

The selected runtime is BusyBox `1.37.0`. The configured Docker mirror was
queried with:

```sh
docker buildx imagetools inspect docker.1ms.run/library/busybox:1.37.0 \
  --format '{{json .Manifest}}'
```

The tag resolved to multi-platform OCI index
`sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0`.
Its `linux/amd64` entry is an OCI manifest with size 610 bytes, version
annotation `1.37.0-glibc`, and digest:

```text
sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0
```

A lookup and pull by that digest through the mirror returned the same digest.
Local inspection reported `linux/amd64`, and executing the pulled image printed
`BusyBox v1.37.0`. The deployment contract keeps the registry-canonical
reference rather than the environment-specific mirror hostname:

```text
docker.io/library/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0
```

The platform manifest is pinned instead of the mutable tag or multi-platform
index so production `linux/amd64` nodes and the canonical release bundle bind
the exact executable image identity.

## Public seam

The public seams are the final rendered resource graphs from:

```sh
kubectl kustomize deploy/vela-control
kubectl kustomize deploy/worker-agent
kubectl kustomize deploy/fleet-controller
```

`internal/deploymentcontract.TestRenderedRootMaterializersUsePinnedBusyBoxImage`
requires the exact digest at all three consumers:

- `Deployment/vela-system/vela-control` init container
  `secret-materializer`;
- `DaemonSet/vela-system/vela-h3-worker` init container
  `runner-socket-permissions`; and
- `ConfigMap/vela-system/vela-fleet-desired-placeholder` revision
  `initImage`.

The static Worker/Fleet equality test separately requires that Fleet
materialization preserves the same image. A source YAML change, Kustomize drift,
tag regression, zero digest, repository change, platform digest change, or
divergence between the static and dynamically materialized Worker template now
fails repository validation.

## Release boundary

The canonical Slice 40 release bundle still requires all three exact final
renders, each Worker materialization, and the matching OCI manifest/config
bytes. Slice 45 still requires registry publication, DSSE signature, SPDX SBOM,
trusted vulnerability scan, and independent approval evidence under the
external trust policy. The repository pin and mirror pull supply none of those
external artifacts and are not a publication receipt, deployment receipt, or
Launch Receipt.

The `vela-control`, Fleet Controller, Worker Agent, and H3 Runner zero digests
remain deliberate release-specific inputs. They must come from the approved
Slice 43/44 Vela release artifacts and must not be replaced with local smoke
digests. Real PKI/Secrets, storage, NetworkPolicy overlays, node materialization,
RKE2, and H3 certification also remain external deployment inputs.

## Verification evidence

- the render test was observed failing at all three consumers before the YAML
  changes and passing after the digest pin;
- the complete `internal/deploymentcontract` and `internal/fleetcontroller`
  packages pass;
- the mirror pull and local execution confirmed the selected manifest reports
  `linux/amd64` and runs BusyBox `1.37.0`;
- `make validate-deployment` renders all five deployment bases and explicitly
  runs the BusyBox contract test; and
- post-review `make verify`, the focused deployment-contract race test, and
  `go mod verify` pass.
