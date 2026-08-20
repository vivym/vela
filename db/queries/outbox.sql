-- name: ClaimOutboxEvents :many
WITH candidates AS (
    SELECT event_id
    FROM outbox_events
    WHERE published_at IS NULL
      AND available_at <= clock_timestamp()
      AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())
    ORDER BY available_at, occurred_at
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_size)
)
UPDATE outbox_events AS event
SET claimed_by = sqlc.arg(claimed_by),
    claim_token = sqlc.arg(claim_token),
    claim_expires_at = clock_timestamp() + make_interval(secs => sqlc.arg(claim_seconds)::integer),
    publish_attempts = publish_attempts + 1,
    last_error = NULL
FROM candidates
WHERE event.event_id = candidates.event_id
RETURNING event.event_id, event.aggregate_type, event.aggregate_id,
    event.aggregate_version, event.event_type, event.schema_version, event.payload,
    event.occurred_at, event.claim_token;

-- name: MarkOutboxPublished :execrows
UPDATE outbox_events
SET published_at = clock_timestamp(),
    broker_stream = sqlc.arg(broker_stream),
    broker_sequence = sqlc.arg(broker_sequence),
    claimed_by = NULL,
    claim_token = NULL,
    claim_expires_at = NULL,
    last_error = NULL
WHERE event_id = sqlc.arg(event_id)
  AND claim_token = sqlc.arg(claim_token)
  AND published_at IS NULL;

-- name: MarkOutboxFailed :execrows
UPDATE outbox_events
SET available_at = clock_timestamp() + make_interval(secs => sqlc.arg(retry_after_seconds)::integer),
    claimed_by = NULL,
    claim_token = NULL,
    claim_expires_at = NULL,
    last_error = left(sqlc.arg(last_error), 2000)
WHERE event_id = sqlc.arg(event_id)
  AND claim_token = sqlc.arg(claim_token)
  AND published_at IS NULL;
