-- +goose Up
-- An immutable receipt must remain acceptable to the Registry binding verifier.
-- Validate existing rows; never rewrite historical identity during migration.
ALTER TABLE worker_bootstrap_receipts
    ADD CONSTRAINT worker_bootstrap_receipts_nonzero_worker_scope
        CHECK (worker_scope <> decode(repeat('00', 32), 'hex')),
    ADD CONSTRAINT worker_bootstrap_receipts_nonzero_runtime_scope
        CHECK (runtime_scope <> decode(repeat('00', 32), 'hex'));

-- +goose Down
ALTER TABLE worker_bootstrap_receipts
    DROP CONSTRAINT worker_bootstrap_receipts_nonzero_worker_scope,
    DROP CONSTRAINT worker_bootstrap_receipts_nonzero_runtime_scope;
