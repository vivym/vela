# Materialize And Pin StageArtifacts

Date: 2026-08-29

Status: Accepted and implemented in the repository; production acceptance remains `0/9 PASS`.

## Context

Disaggregated H3 stages need a recoverable handoff. Encoder output and final DiT
latent are expensive to recompute but are small enough, relative to stage
service time, that durable transfer is acceptable for the target design.
Relying only on direct process-to-process transfer would couple liveness of two
Workers and lose reuse after downstream failure. Treating every intermediate as
a customer Artifact would expose internal data and retention semantics.

## Decision

Vela uses three storage tiers:

- L1: evictable Worker-local NVMe;
- L2: durable, internal, short-retention StageArtifact storage;
- L3: customer-visible ArtifactSet storage with formal retention and access.

Encoder output and final DiT latent must become immutable L2 StageArtifacts
before a downstream StageRun becomes READY. A StageArtifact binds exact object
version, digest, size, StageInterfaceRevision, lineage, scope, state, and expiry.
Object-store versioning, conditional publication, and revocation of write
authority enforce immutability in addition to PostgreSQL CAS.

Compute may release its GPU after sealing a local materialization receipt. A
separate StageMaterializationLease allows the Worker Agent or node-local Data
Mover to upload, verify, and commit L2 without holding compute capacity. If the
node is lost before commit, the StageRun retries.

## Transfer

The first Connector Adapter is exact-version object-store pull. A
`TransferTicket` is short-lived and destination-bound; it includes Artifact,
version, digest, size, destination Worker/model epochs, connector revision, and
expiry. The destination verifies integrity before starting ModelRuntime.

Same-node NVMe, RDMA, NIXL, P2P, or rack-cache Adapters may be added after
certification. They remain optimizations and must fall back to L2; they cannot
be the only durable copy.

## Exact cache

- Cross-Job cache is exact and Project-scoped by default.
- Organization-scope reuse requires explicit Organization authorization.
- Customer Content is never reused across Organizations.
- The scoped HMAC key binds stage kind, result-equivalence and input
  canonicalization revisions, root and upstream input digests, normalized
  parameters, seed/RNG revision, output shape, and adapter/LoRA digests.
- A `StageResultEquivalenceRevision` explicitly certifies canonical-byte or
  bitwise equivalence when different StageProfiles may share results.
  Quality-only or tolerance-based similarity is insufficient. Compatibility is
  never inferred from matching shapes or model names; the first release
  otherwise limits equivalence to one exact StageProfile.
- PostgreSQL metadata is authoritative. Redis or memory may suggest candidates
  but cannot authorize a hit.
- A hit is valid only after one transaction rechecks scope, policy, TTL,
  equivalence, exact object version, deletion state, and creates an
  `ExecutionPin`.
- `ExecutionPin` is strong and blocks ordinary eviction. `CacheReference` is
  weak and may be reclaimed after execution releases its pin.

`DurableCheckpoint` remains a same-Job resume contract and is not an ordinary
cache entry. Approximate embedding or latent reuse is not enabled.

## Capacity and deletion

Admission reserves risk-adjusted L2 bytes for every cache miss path. Each graph
edge also has bounded count and byte buffer credits so upstream work cannot
create unbounded WIP.

Content Deletion immediately blocks new hits and pins, then covers exact L1,
L2, cache, checkpoint, and L3 versions within the deletion SLA. Cancellation is
not deletion; already committed StageArtifacts may remain under Project policy.

## Consequences

- Downstream retries can reuse expensive upstream work without requiring the
  original Worker.
- L2 availability and capacity become Admission dependencies.
- Cache savings are measurable without changing customer price.
- Internal data receives distinct credentials, namespace, lifecycle, and
  access controls from customer Artifacts.

## Rejected alternatives

- Direct transfer only: rejected because it is not durable recovery authority.
- Store payloads in PostgreSQL or JetStream: rejected due to size, I/O, and
  authority/RPO pollution.
- Cross-Organization content-addressed cache: rejected by Organization
  Isolation and Customer Content policy.
- Approximate cache as the first release: rejected because output equivalence
  and product quality would be ambiguous.

## Evidence boundary

This ADR does not establish tensor sizes, storage throughput, cache hit rate,
reuse distance, or economic value. The trace simulator and target workload
receipts must calibrate them.
