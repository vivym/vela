# Slice 37: Statistical SLO Measurement And Observability Conformance

## Status

Accepted for implementation on 2026-08-28. This slice closes the repository
contract for ADR 0009. It does not assert a measured production SLO, deploy an
observability stack, complete a P1 exercise, create a Launch Receipt, or advance
the current `0/9 PASS` Production Gate result.

## Authority And Product Boundary

The statistical Job SLI starts at immutable `jobs.created_at`, the transaction
that durably admits the Job as QUEUED. Successful latency ends only at
`visible_completions.completed_at`. Queueing, retries, Worker loss, recovery and
Artifact finalization remain inside the measurement. Attempt runtime, progress,
Dynamic ETA and benchmark compute p95 are not substitutes for this SLI.

Every contract binds one exact `ModelRevision + GenerationPresetRevision +
ServiceClassRevision + OutputSpec + generation_count`. `quality`, `balanced`
and `fast` are measured independently. Customer cancellation is excluded from
the success-rate denominator only under the fixed
`exclude-customer-cancellation-v1` policy and remains a visible count. FAILED
and expired Jobs are failures. A monthly UTC half-open cohort with any open Job,
missing contract, insufficient sample, mixed revision or malformed observation
cannot PASS.

Public API availability is a separate monthly 99.9% SLO. Its authoritative
production input is the external gateway classification, including TLS,
transport, timeout and 5xx failures. Explicit customer-caused 4xx requests are
excluded. Pod-private `/healthz` and `/readyz` probes never enter the public API
SLI.

## Statistical Contract

`internal/slo.Evaluate` is the single in-repository algorithm authority:

- one UTC calendar month `[start,end)` selected by Accepted/QUEUED time;
- one observation per Job, with duplicate IDs rejected;
- successful latency is `VisibleCompletion.completed_at - jobs.created_at`;
- p95 uses integer milliseconds and nearest-rank `ceil(0.95 * N)`;
- observed success PPM uses integer floor division;
- confidence uses fixed one-sided 95% Wilson lower bound revision
  `wilson-one-sided-95-v1`;
- PASS requires a closed window, no open observations, minimum eligible sample,
  at least one success, p95 at or below target, and Wilson lower bound at or
  above the success target;
- OPEN Jobs expired by evaluation, and success or cancellation recorded after
  Job Expiry, are failures; and
- canonical Job-ID-sorted, length-prefixed UTF-8 observation fields produce a
  SHA-256 source-set digest that PostgreSQL and `internal/slo` reproduce exactly.

`internal/sloevidence` strictly decodes the existing `observability-on-call`
gate evidence. It rejects duplicate or unknown JSON keys, oversized documents,
opaque PASS strings, recomputation mismatch, missing saleable-contract
coverage, absent independent Presets, missing API 99.9% evidence, malformed P1
timelines and missing/digest-mismatched dashboard, rules, rule-test, runbook,
gateway, SKU snapshot or page-event artifacts. The general Launch Receipt
loader still verifies the outer evidence bytes and now performs this semantic
verification for the observability gate. The nested gateway artifact is itself
strict JSON with exact external-gateway and synthetic-probe streams, contiguous
at-most-daily buckets over the full measurement window and bounded counts; only
the external-gateway eligible/good totals reproduce the authoritative API
report, while synthetic probes remain independently visible. The nested saleable-SKU
snapshot is also strict JSON and its complete immutable contract values must
exactly reproduce the evidence cohorts.

## Database Contract

Migration `00033_statistical_slo_measurement.sql` adds:

- immutable `statistical_slo_contract_revisions` bound to a sealed
  `preset-certification` receipt and exact saleable dimensions;
- a one-way `LEGACY -> ENFORCED` protocol switch;
- permanent, non-content `job_slo_admissions` snapshots captured at Admission;
- permanent terminal `job_slo_outcomes` derived from canonical terminal time
  and Visible Completion;
- immutable monthly `slo_measurement_reports`; and
- narrow `vela_slo_reporting` functions owned by a separate NOLOGIN/BYPASSRLS
  role, with no direct runtime fact-table access.

The additive LEGACY phase preserves the exact adjacent N-1 writer. Enabling the
protocol first proves every ACTIVE RateCard line has same-release/config exact
target coverage for every API-accepted `generation_count` from 1 through 16,
then backfills every retained Job and terminal outcome. Any
unclassifiable retained row aborts the transition. ENFORCED Admission without
an exact generation-count contract returns named SQLSTATE `55000`. Empty
Down/Up is supported; any durable contract, observation, report or protocol
transition makes Down fail closed as `statistical_slo_rollback_is_unsafe`.

Reports are computed by a SECURITY DEFINER function from immutable snapshots;
sealing is rejected until the protocol is ENFORCED.
They preserve observation, success, failure, customer-cancellation and open
counts; deterministic p95, observed PPM, Wilson lower bound, result and an
ordered source digest. They remain after the 365-day operational Job row expiry
and contain no Customer Content.

## Runtime And Deployment Contract

The public chi router records `vela_api_requests_total` and duration using only
method, OpenAPI route pattern and status. Raw URL, Organization, Project,
Principal, Job, Attempt and Worker identifiers are forbidden labels. The
PostgreSQL-authoritative collector exports only latest sealed report values,
immutable targets, eligible/success/failure/customer-cancellation/open counts,
exact-contract report coverage and freshness by controlled catalog dimensions.

`/metrics` is registered only on the existing Pod-private management listener
at port 8081. No Service exposes it. NetworkPolicy admits only namespaces with
`vela.ai/network-role=observability` and Collector Pods with
`vela.ai/client-role=otel-collector`. The SLO collector is not part of
`/readyz`; exporter failure reports a zero health gauge instead of making the
public API unavailable.

`deploy/observability` supplies a PodMonitor, API 30-day availability/error
budget rules, fast and slow multi-window burn alerts, exact Preset p95/success
alerts, missing-coverage/exporter/freshness alerts, rule tests and a Grafana
dashboard. A separate fail-closed API alert pages when either external gateway
eligible or good series is absent. `docs/runbooks/statistical-slo-breach.md`
fixes triage, mitigation,
safe label use and receipt closure. These are deployable contracts, not evidence
that Prometheus, Grafana, paging delivery or 24x7 ownership exists in a real
environment.

## Acceptance Evidence

1. Pure evaluator tests cover mixed success/failure/cancel/open cohorts,
   nearest-rank p95, Wilson confidence, retry-inclusive clocks, duplicate Job
   rejection, mixed revisions and the fixed API 99.9% target.
2. Strict evidence tests reject opaque, duplicate-key, incomplete, tampered and
   recomputation-mismatched evidence and verify every nested artifact digest.
3. PostgreSQL integration tests prove exact three-Preset coverage, one-way
   activation only after all generation counts 1 through 16 are covered,
   Admission snapshot, pre-enforcement seal refusal, monthly report arithmetic,
   internal collector readability, privilege negatives, immutable replay, empty
   Up/Down/Up and durable-evidence rollback refusal.
4. Runtime tests prove route-pattern labels, forbidden-label absence,
   management-only `/metrics`, SLO database failure visibility and no readiness
   coupling.
5. Deployment tests and kustomize rendering prove the exact scraper identity,
   required rule/dashboard/runbook surface and forbidden metric-label contract.
6. Full integration, `make verify`, generated drift, race-relevant focused tests
   and fixed-point Standards/Spec reviews must pass before local delivery.

## Explicit Non-Claims

This slice does not prove actual monthly API availability, actual Job p95 or
success rate, H3 workload volume, fault-delay distribution, production
fairness, live Prometheus/Grafana/Alertmanager installation, alert delivery or
acknowledgement, 24x7 staffing, a completed P1 exercise, live N/N-1 coexistence,
or any Production Gate receipt. Hard Deadline and CapacityReservation remain
out of scope.
