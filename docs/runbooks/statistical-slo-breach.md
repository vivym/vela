# Statistical SLO breach

Owner: Platform On-call (24x7)

This runbook covers public API availability and exact ModelRevision,
GenerationPresetRevision, ServiceClassRevision, OutputSpec and generation-count
Job SLO cohorts. `/healthz` and `/readyz` are deployment signals, not public API
availability observations. Management probes are excluded.

## Triage

1. Acknowledge the page and record the alert fired, delivered and acknowledged
   timestamps in the incident record.
2. Confirm `vela_slo_report_exporter_last_scrape_success` and report freshness.
   Missing, open, low-sample or stale cohorts are insufficient data, never PASS.
3. For API alerts, inspect the gateway-classified eligible/good request series.
   Include TLS, transport, timeout and 5xx failures; exclude only explicitly
   classified customer-caused 4xx requests. Management probes are excluded.
4. For Job alerts, select the exact immutable cohort. Do not merge Presets,
   OutputSpecs or ServiceClassRevisions, and do not remove fault, retry, queue or
   finalization delay.
5. Correlate through traces or audited queries. Never add Organization, Project,
   Principal, Job, Attempt or Worker identifiers as metric labels.

## Mitigation

- Stop Catalog promotion when target coverage or evidence freshness is missing.
- Apply the relevant database, queue, Worker, storage or gateway runbook. Do not
  promise a per-Job deadline or hide failures by reclassifying cancellations.
- Preserve the source observation set and its digest before changing runtime
  configuration.

## Closure

Resolve only after the paging condition clears, authoritative observations are
sealed, and the incident timeline records fired, delivered, acknowledged and
resolved times. A Production Gate receipt additionally requires the release,
configuration, environment, owner, dashboard, rules, rule-test, runbook and P1
exercise artifacts with verified SHA-256 digests.
