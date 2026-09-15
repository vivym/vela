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

1. A strict `schema_version: 3` build plan names every input beneath one rooted
   directory. References must be canonical local paths to regular files and may
   not escape through an absolute path, `..`, backslash, or symbolic link.
2. The configuration manifest contains the exact five final Kubernetes renders
   (`control-storage`, `fleet-controller`, `observability`, `stage-worker`,
   and `vela-control`), the exact `node-agent` package, the remediation Node
   Agent and runtime image maintenance systemd units, the selected Fleet authority input,
   and the declared external Secret/ConfigMap canonical content digests.
3. Every OCI image is canonical, lowercase, tag-free, and pinned by a non-zero
   `sha256` digest. Its manifest and referenced OCI config blob are read from
   rooted artifacts; digest and size are recomputed, and the config must derive
   the production platform `linux/amd64`.
4. Canonical JSON of the configuration manifest derives the configuration
   revision. A Vela release descriptor with media type
   `application/vnd.vela.release.descriptor.v3+json` binds that configuration
   descriptor to the ordered OCI manifest descriptors and derives the release
   digest. The Vela descriptor is an internal identity contract, not a claim of
   OCI Index interoperability.
5. Re-loading a bundle rebuilds the complete graph from its rooted references
   and requires the reconstructed bundle, configuration revision, release
   descriptor, and release digest to match exactly.

Schema 3 adds the required `runtime_image_maintenance_unit` input and named
artifact, using `runtime-image-maintenance-systemd-unit`. Configuration and
bundle media types also use `v3+json`. A production runtime plan may additionally
carry the explicit `runtime_startup` graph, which binds the launcher, pidfd
broker, policy issuer, and their two systemd units. Schema 2 inputs are rejected
and must be rebuilt from the complete graph. This is not a change to OCI image
manifests or the release descriptor's OCI-style `schemaVersion: 2` field.

## Exact deployment graph

Each final render must match its versioned exact inventory by `apiVersion`,
kind, namespace, and name. Extra, missing, duplicate, wrong-group, template, or
embedded Secret objects fail closed. Workload image references, external
resource references, revision annotations, Secret keys, and consumer identities
must be complete exact sets. A whole-Secret selector is not accepted where the
release contract requires named keys.

### Kubernetes render contract v2 — 2026-09-15

Schema 3 now accepts the explicit `render_contract: "kubernetes-v2"` selector.
The selector is part of the configuration manifest and its derived digest;
loading reconstructs and validates the selected inventory. Omitting it retains
the legacy schema-3 inventory and encoding. Unknown selectors and attempts to
relabel a legacy inventory as v2 fail closed; existing bundles are not silently
reinterpreted.

V2 changes three exact resource inventories:

- `control-storage`: 11 resources. NATS consumes a required external Secret's
  single `nats.conf` key through its read-only config directory, replacing the
  previous `nats-config` ConfigMap. Dependency images in the Barman contract,
  including PostgreSQL and the sidecar, join the exact OCI descriptor inventory.
- `observability`: five application resources in `monitoring`: three hashed
  SLO ConfigMaps, the Control PodMonitor, and the application PrometheusRule.
  `hack/render-release-observability.py` reuses the platform source files.
- `vela-control`: 20 resources, including `vela-control-allow-node-agent`.
  That policy must select only Control, allow only TCP 8444, and enumerate
  exact host source `/32` or `/128` CIDRs. Wildcards, duplicate, loopback,
  link-local, documentation and IPv4-mapped sources fail closed. Site sources
  must be measured at the destination after CNI/Service translation.

The Fleet and Stage Worker inventories remain unchanged. Control manifests now
name each consumed Secret environment key and file item, allowing the assembler
to derive an exact key contract. The MarsLab Control/NATS consumers now reference
16 immutable snapshots with canonical revision annotations; original material
is retained. The Control image also contains the schema-96 Fleet privilege
allowlist correction, validated against a fresh native PostgreSQL schema before
rollout. See [material adoption](../release-material-adoption-2026-09-15.md) and
[release input evidence](../release-input-validation-2026-09-15.md). These cover
current Control/NATS inputs, not the complete all-component release gate.

The H3 preflight and launch-evidence boundary resolves those external
declarations against Kubernetes. It requires `immutable=true`, a live UID and
resource version, the exact revision annotation and Secret key set, and a
recomputed `ExternalResourceContentV1` digest equal to the declared revision.
It emits no Secret or ConfigMap payload and double-reads launch objects to reject
same-name recreation or content drift during capture.

The production target mode embeds one immutable approved `ResidencyPlan`
rollout in the Fleet render. It binds every per-member Stage Worker Pod
actuation, Stage Worker Agent and external ModelRuntime image, exact device and
member epochs, Stage Worker ConfigMap, and four Secret contracts. The external
ModelRuntime image digest transitively binds an OCI config with an absolute
`ENTRYPOINT`; Fleet does not override that entrypoint. Target mode rejects
legacy desired revisions and Worker materializations.

The legacy desired-revision plus Worker-materialization graph was removed by
S49.12 contraction. Legacy materialization fields are rejected by the strict
build plan and configuration manifest parsers.

The host package uses a strict contract that binds `linux/amd64`, revision,
absolute entrypoint, artifact digest, and artifact size. Both Node Agent systemd
units are parsed as separate exact allowlists: one package-bound `ExecStart`, no
additional start hooks, and no unknown or conflicting service directives. The
remediation unit requires its deployed `StateDirectoryMode=0750`. The independent
maintenance unit invokes `runtime-image-maintenance` with a fixed private config
path, retries failures without start rate limiting, and requires an empty
capability bounding set. Dependencies on remediation or a containerd unit are
not accepted. Host configuration values, effective drop-ins and enablement
still require external deployment evidence.

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
