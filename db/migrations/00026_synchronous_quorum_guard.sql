-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_enforce_synchronous_quorum() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF current_setting('vela.require_synchronous_quorum', true) IS DISTINCT FROM 'on' THEN
        RETURN NEW;
    END IF;

    IF current_setting('synchronous_standby_names') = '' OR NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_stat_replication AS replication
        WHERE replication.state = 'streaming'
          AND replication.sync_state IN ('sync', 'quorum')
    ) THEN
        RAISE EXCEPTION 'synchronous replication quorum is unavailable'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_synchronous_quorum() FROM PUBLIC;
ALTER FUNCTION vela_enforce_synchronous_quorum() OWNER TO vela_quorum_guard_owner;

CREATE CONSTRAINT TRIGGER jobs_require_synchronous_quorum
AFTER INSERT ON jobs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

CREATE CONSTRAINT TRIGGER scheduler_dispatch_intents_require_synchronous_quorum
AFTER INSERT ON scheduler_dispatch_intents
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

CREATE CONSTRAINT TRIGGER attempts_require_synchronous_quorum
AFTER INSERT ON attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER attempts_require_synchronous_quorum ON attempts;
DROP TRIGGER scheduler_dispatch_intents_require_synchronous_quorum ON scheduler_dispatch_intents;
DROP TRIGGER jobs_require_synchronous_quorum ON jobs;
DROP FUNCTION vela_enforce_synchronous_quorum();
-- +goose StatementEnd
