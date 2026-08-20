-- name: RecordInboxReceipt :one
INSERT INTO inbox_receipts (
    consumer_name,
    event_id,
    organization_id,
    project_id,
    aggregate_type,
    aggregate_id,
    aggregate_version,
    event_type
) VALUES (
    sqlc.arg(consumer_name),
    sqlc.arg(event_id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(aggregate_type),
    sqlc.arg(aggregate_id),
    sqlc.arg(aggregate_version),
    sqlc.arg(event_type)
)
ON CONFLICT DO NOTHING
RETURNING event_id;
