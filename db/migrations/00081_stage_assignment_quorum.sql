-- +goose Up
-- +goose StatementBegin
-- Fence the current Stage acquisition/renewal transactions at commit, using
-- the same opt-in quorum contract as Admission. Read-only idle hints, durable
-- replay and terminal/expiry maintenance do not insert these authority rows.
CREATE CONSTRAINT TRIGGER stage_acquire_intents_require_synchronous_quorum
AFTER INSERT ON stage_worker_acquire_intents
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

CREATE CONSTRAINT TRIGGER stage_scheduler_snapshots_require_synchronous_quorum
AFTER INSERT ON stage_scheduler_snapshot_traces
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

CREATE CONSTRAINT TRIGGER stage_scheduler_claims_require_synchronous_quorum
AFTER INSERT ON stage_scheduler_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

CREATE CONSTRAINT TRIGGER stage_attempts_require_synchronous_quorum
AFTER INSERT ON stage_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

CREATE CONSTRAINT TRIGGER stage_renewals_require_synchronous_quorum
AFTER INSERT ON stage_authority_renewals
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER stage_renewals_require_synchronous_quorum ON stage_authority_renewals;
DROP TRIGGER stage_attempts_require_synchronous_quorum ON stage_attempts;
DROP TRIGGER stage_scheduler_claims_require_synchronous_quorum ON stage_scheduler_claims;
DROP TRIGGER stage_scheduler_snapshots_require_synchronous_quorum ON stage_scheduler_snapshot_traces;
DROP TRIGGER stage_acquire_intents_require_synchronous_quorum ON stage_worker_acquire_intents;
-- +goose StatementEnd
