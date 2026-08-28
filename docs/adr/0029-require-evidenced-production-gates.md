# Require evidenced production gates

Vela opens formal production traffic only after versioned Launch Receipts prove all three generation presets, a 72-hour real-H3 mixed-load soak, component and network fault injection, no lost Accepted Job or duplicate Visible Completion or Charge, cross-organization negative isolation, PostgreSQL restore, JetStream rebuild, Outbox replay, Artifact recovery, release rollback, Worker drain rollout, old-event backlog consumption, and exercised dashboards, alerts, runbooks, and P1 response ownership.

## Consequences

Every Production Gate binds release digest, configuration revision, validation environment, result, owner, and acceptance threshold. A failed or missing receipt cannot become PASS through verbal waiver, known-customer status, or a plan to repair the capability after launch.

Slice 39 (`7829104`) adds versioned typed semantic contracts for the eight
non-observability gates. Each contract fixes its check IDs, numeric thresholds,
artifact inventory, receipt bindings, and canonical summaries. Referenced
artifacts are themselves strict, digest-bound payloads whose aggregate must
reproduce the envelope. The existing `observability-on-call` evidence schema
remains separately versioned in `internal/sloevidence`.

Preset evidence additionally binds the authoritative saleable-group snapshot,
all three independent certification claims, and complete RateCard binding
pairs. Catalog promotion rejects any plan/evidence mismatch before opening a
database transaction.

Repository fixtures prove the validator, not the external facts asserted by a
production owner. Real H3 identity, fault domains, exercises, and retained raw
evidence remain external requirements. No receipt is created by this decision;
the current result remains `0/9 PASS`.
