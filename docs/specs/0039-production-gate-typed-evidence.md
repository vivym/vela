# Production Gate typed evidence

Date: 2026-08-28

Status: Repository conformance implemented at `7829104`.

This slice hardens the eight Production Gates that previously accepted an
opaque evidence file after only SHA-256 verification. It advances ADR 0029 and
strengthens the Catalog promotion boundary introduced by Slice 35. It does not
run a production exercise, create a Launch Receipt, or change the current
`0/9 PASS` result.

## Governing boundary

1. `observability-on-call` retains the established `internal/sloevidence`
   schema. The other eight gates use the typed evidence schema implemented by
   `internal/productiongates`.
2. Every typed envelope binds the gate, canonical criteria revision, release
   digest, configuration revision, validation environment, owner, and exact
   receipt time window.
3. The receipt `acceptance_threshold` is the canonical criteria revision. The
   `observed_result` is the verifier-produced summary, not operator prose.
4. Each gate has an exact check set, numeric measurement contract, and required
   artifact-kind inventory. Unknown, missing, duplicate, failed, or
   threshold-violating observations fail closed.
5. Referenced artifacts are strict typed JSON. Each repeats the parent binding,
   and the union of artifact checks and measurements must exactly reproduce the
   envelope. Artifact bytes are rooted beneath the manifest directory,
   regular-file-only, digest verified, limited to 16 MiB each, and limited to
   128 MiB across the complete manifest.
6. Typed envelopes are limited to 4 MiB and reject duplicate keys, unknown
   fields, trailing JSON, path escape, symlink escape, and invalid digests.
7. The real-H3 soak measurement must equal the receipt window and be at least
   72 hours. DR thresholds retain the architecture limits: single-node metadata
   RPO 0 and RTO at most 5 minutes; site metadata RPO at most 15 minutes and RTO
   at most 4 hours.
8. Preset evidence explicitly lists the authoritative saleable-group snapshot.
   Every listed group has exactly independent `quality`, `balanced`, and `fast`
   claims with versioned backend, driver baseline, corpus, sample count,
   quality, success, p50/p95, cost, currency, and confidence fields.
9. Catalog promotion must exactly match the verified preset claims, including
   every certification value and each `(binding_id, rate_card_revision_id)`
   pair, before `Service.Apply` opens a PostgreSQL transaction.

## Gate contracts

| Gate | Required semantic inventory | Fixed verifier outcomes |
| --- | --- | --- |
| `preset-certification` | saleable Catalog snapshot, benchmark observations, certification results | exact saleable-group coverage, exactly three stable Presets, no missing combination or failed threshold |
| `real-h3-soak` | hardware inventory, saleable SKU snapshot, soak observations, Job reconciliation, mixed-version inventory | at least 72 hours and one Accepted Job; zero lost Job, duplicate completion, duplicate Charge, or unreconciled Job |
| `state-event-fault-injection` | scenario matrix, authority before/after, raw event payloads | all ten fixed crash/fence scenarios; zero lost or duplicate authority and zero stale-authority acceptance |
| `gpu-remediation` | hardware capability matrix, remediation operations, negative approval exercises | L0-L5, identity, rate, post-check, canary, quarantine, L6 approval, and L7 fail-closed coverage |
| `organization-isolation-content-safety` | surface/role snapshot, negative probes, credential and signed-URL results, break-glass audit, reuse audit | zero unexpected allow, revocation bypass, content reuse, or unaudited break-glass access |
| `data-disaster-recovery` | backup inventory, failover, PITR, Artifact restore, rebuild/replay/rotation | architecture RPO/RTO limits, non-empty Artifact sample, and zero duplicate authority |
| `release-rollback` | release inventory, compatibility checks, rollout timeline, long-Job ledger, retained backlog, rollback | N/N-1 coexistence and backlog coverage; zero Job loss, duplicate completion/Charge, or unconsumed backlog |
| `commercial-data-lifecycle` | Admission/credit, completion/cancel/failure, Invoice, Webhook, retention/deletion scenarios | zero credit, Charge, Invoice, Webhook-window, retention, or resurrection violation |

The check IDs and artifact kinds are versioned under
`vela.production-gates/<gate>/v1`. A changed criterion requires a new criteria
revision rather than a waiver or an in-place semantic reinterpretation.

## Catalog promotion

`productiongates.LoadManifestWithin` exposes verified typed evidence only after
the complete manifest and artifact graph passes. `catalogpromotion.Service`
compares the promotion plan with the verified `preset-certification` claims
before decoding database inputs or opening a transaction. A mismatch therefore
leaves production-gate manifests, receipts, certification evidence, RateCard
bindings, and protocol transitions untouched.

This protects the normal CLI and service path. A caller that already holds the
dedicated Catalog promotion DSN can still invoke the five SQL mutation seams
without presenting evidence bytes to PostgreSQL. Moving typed evidence
verification into database authority would require a new migration and a
separate N/N-1 compatibility slice; this change does not claim that boundary.

## Verification evidence

- every non-observability gate has one static typed contract;
- opaque evidence, malformed JSON, duplicate/unknown/trailing fields, failed or
  incomplete checks, threshold drift, contradictory measurements, missing
  saleable groups, invalid RateCard bindings, and free-form receipt summaries
  are rejected;
- receipt owner, environment, and time mismatch are rejected;
- artifact tamper, path escape, symlink escape, oversize, binding mismatch, and
  incomplete aggregate observations are rejected;
- preset plan metric and RateCard binding mismatch are rejected before any
  database mutation;
- focused race tests, full integration tests, generated-output verification,
  lint, unit and Python tests, Linux/amd64 cross-build, and deployment-contract
  validation pass.

## Evidence boundary

The repository can verify schema, exact-set coverage, numeric thresholds,
receipt bindings, promotion equality, and artifact digests. It cannot prove
that a machine was physically H3, that an S3 target has an independent failure
domain, that a named exercise occurred in production, or that externally
retained evidence is truthful. Those facts require real validation owners and
versioned production artifacts. Production remains closed at `0/9 PASS`.
