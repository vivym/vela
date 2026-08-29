# Production release bundle and configuration revision closure

Date: 2026-08-28

Status: Repository conformance implemented by Slice 40.

This slice makes one canonical release bundle the repository boundary between
release assembly, deployment validation, Launch Receipt verification, and
Catalog promotion. It closes the prior gap in which a release digest and a
configuration revision could be supplied as independent strings without one
verified artifact graph proving how they were derived. It does not publish or
sign an OCI artifact, deploy a production environment, create a Launch Receipt,
or change the current `0/9 PASS` result.

## Canonical identity

1. A strict `schema_version: 1` build plan names every input beneath one rooted
   directory. References must be canonical local paths to regular files and may
   not escape through an absolute path, `..`, backslash, or symbolic link.
2. The configuration manifest contains the exact five final Kubernetes renders
   (`control-storage`, `fleet-controller`, `observability`, `vela-control`, and
   `worker-agent`), the exact `h3-runner` and `node-agent` packages, the
   Node Agent systemd unit, every Worker materialization, and the declared
   external Secret/ConfigMap revisions.
3. Every OCI image is canonical, lowercase, tag-free, and pinned by a non-zero
   `sha256` digest. Its manifest and referenced OCI config blob are read from
   rooted artifacts; digest and size are recomputed, and the config must derive
   the production platform `linux/amd64`.
4. Canonical JSON of the configuration manifest derives the configuration
   revision. A Vela release descriptor with media type
   `application/vnd.vela.release.descriptor.v1+json` binds that configuration
   descriptor to the ordered OCI manifest descriptors and derives the release
   digest. The Vela descriptor is an internal identity contract, not a claim of
   OCI Index interoperability.
5. Re-loading a bundle rebuilds the complete graph from its rooted references
   and requires the reconstructed bundle, configuration revision, release
   descriptor, and release digest to match exactly.

## Exact deployment graph

Each final render must match its versioned exact inventory by `apiVersion`,
kind, namespace, and name. Extra, missing, duplicate, wrong-group, template, or
embedded Secret objects fail closed. Workload image references, external
resource references, revision annotations, Secret keys, and consumer identities
must be complete exact sets. A whole-Secret selector is not accepted where the
release contract requires named keys.

A Fleet desired revision represents one logical `WorkerPool` and contains one
or more node placements. Each placement is hostname-pinned and owns one
`OnDelete` DaemonSet plus versioned runtime/profiles/GPU-role ConfigMaps and a
Worker TLS Secret. The logical pool continues to own the shared
ExecutionProfileRevision, backend revision, images, capacity policy, and
scheduler/circuit authority; it is not split into per-node pools.

Each Worker materialization binds exactly one placement to a registered Node
identity, Worker UUID and epoch, WorkerPool, Fleet revision, canonical Node
Agent SPIFFE identity, the placement materials, TLS Secret revision,
ExecutionProfileRevision, InferenceBackendRevision, and ModelRevision. Every
desired placement must have exactly one materialization and every
materialization must resolve to one desired placement. Node, Worker, placement,
TLS, ConfigMap, material digest, and GPU UUID identities may not alias across
placements in the same or different pools.

The two host packages use strict contracts that bind `linux/amd64`, revision,
absolute entrypoint, artifact digest, and artifact size. The privileged Node
Agent systemd unit is parsed as an exact allowlist: one expected `ExecStart`, no
additional start hooks, and no unknown or conflicting service directives.

## Resource and write safety

The plan, bundle, metadata, package, graph entry, Worker count, YAML document,
YAML node/depth, and aggregate artifact bytes are bounded. Artifact references
are inventoried and stat-bounded before content reads. Package artifacts are
hashed as streams rather than retained as 256 MiB byte slices, and all reads
consume one shared graph byte budget. Duplicate references are rejected even
when their bytes or digests happen to match.

`vela-release-bundle build` writes a mode-`0600` temporary candidate in the
destination directory, syncs it, verifies it before replacement, atomically
renames it, and syncs the directory. It refuses to overwrite or alias the build
plan or any referenced artifact. `vela-release-bundle verify` is read-only and
prints the derived release and configuration identities only after full graph
verification.

## Launch and Catalog binding

`vela-verify-launch` now requires both the canonical release bundle and the
Launch Receipt manifest. A PASS result requires every receipt to bind the
bundle-derived release digest and configuration revision.

Catalog promotion plans require a rooted `release_bundle_ref` in addition to
the receipt manifest. `catalogpromotion.Service.Apply` verifies the complete
bundle, the complete typed receipt graph, exact release/configuration equality,
and exact promotion claims before `BeginTx`. A missing, malformed, escaped,
tampered, or mismatched bundle therefore leaves all database state unchanged.

## Verification evidence

- focused unit and race tests cover canonical rebuilds, OCI config binding,
  platform mismatch, exact render and Secret inventories, systemd parsing,
  aliasing, rooted paths, graph bounds, byte budgets, and atomic replacement;
- Catalog unit/integration tests cover a real verified bundle, exact
  release/configuration matching, and mismatch rejection before transaction;
- full Go and Python tests, vet/lint, generated-output checks, Linux/amd64
  cross-build, integration tests, and deployment rendering remain required for
  delivery validation.

## Evidence boundary

The repository still does not publish the descriptor or images to a registry,
produce a signature or SBOM, approve vulnerability results, provision real PKI
or Secret values, install an RKE2 production cluster, materialize production
Worker nodes, or execute the nine production exercises. Those operations must
produce externally retained evidence and versioned Launch Receipts bound to the
derived identities. Repository fixtures are conformance inputs only;
Production Gates remain `0/9 PASS`.
