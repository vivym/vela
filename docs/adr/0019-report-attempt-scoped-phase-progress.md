# Report attempt-scoped phase progress

The customer Job view reports state, a backend-neutral Execution Phase, optional Phase Progress, attempts started, next retry time, Dynamic ETA, and progress update time. Phase Progress describes only the current Attempt and phase, may reset after Retry, becomes unknown when updates are stale, and reaches customer-visible completion only through Visible Completion.

## Consequences

Vela does not manufacture a monotonic global percentage or expose Encoder, DiT, VAE, GPU role, or other backend-specific stages. Clients use `attempts_started` and phase changes to explain retries and must not treat Phase Progress or Dynamic ETA as an SLO or completion guarantee.
