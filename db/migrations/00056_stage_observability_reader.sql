-- +goose Up
-- +goose StatementBegin
GRANT SELECT ON transfer_tickets, stage_cache_entries TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE SELECT ON transfer_tickets, stage_cache_entries FROM vela_internal;
-- +goose StatementEnd
