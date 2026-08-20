# Require evidenced production gates

Vela opens formal production traffic only after versioned Launch Receipts prove all three generation presets, a 72-hour real-H3 mixed-load soak, component and network fault injection, no lost Accepted Job or duplicate Visible Completion or Charge, cross-organization negative isolation, PostgreSQL restore, JetStream rebuild, Outbox replay, Artifact recovery, release rollback, Worker drain rollout, old-event backlog consumption, and exercised dashboards, alerts, runbooks, and P1 response ownership.

## Consequences

Every Production Gate binds release digest, configuration revision, validation environment, result, owner, and acceptance threshold. A failed or missing receipt cannot become PASS through verbal waiver, known-customer status, or a plan to repair the capability after launch.
