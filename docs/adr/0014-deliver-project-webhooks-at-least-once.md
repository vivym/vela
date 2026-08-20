# Deliver Project webhooks at least once

Vela provides Project-scoped Webhook Subscriptions for Job terminal events while retaining `GET /v1/projects/{project_id}/jobs/{job_id}` as the authoritative state query. Outbox-backed Webhook Deliveries are at-least-once, identify the event and Job version, use timestamped HMAC signatures with overlapping secret rotation, retry non-2xx responses for up to 72 hours, and then remain visible and manually replayable.

## Consequences

Customers must deduplicate by `event_id` and fetch current state through the API. Webhook payloads contain no prompt, Artifact content, or signed URL, and delivery failure or replay cannot change Job, Charge, Artifact access, or Preset SLO results.
