# Sell statistical SLOs, not per-Job deadlines

Launch contracts use a 99.9% monthly control-plane API availability target plus GenerationPresetRevision-specific end-to-end p95 completion and success-rate SLOs measured from QUEUED to Visible Completion. Dynamic ETA is explicitly non-binding, and Vela does not sell a Hard Deadline until persistent CapacityReservation and dedicated capacity can prove it.

## Consequences

Returning `202 Accepted` means the Job is durably admitted, not that a particular completion timestamp is guaranteed. Each active preset requires measured SLO evidence and a declared eligibility envelope, while exact SLO values remain a production-certification gate rather than an architecture constant.
