-- +goose Up
-- +goose StatementBegin
ALTER TABLE jobs
    ADD COLUMN billable_started_at timestamptz,
    ADD CONSTRAINT jobs_billable_started_after_creation
        CHECK (billable_started_at IS NULL OR billable_started_at >= created_at),
    ADD CONSTRAINT jobs_running_requires_billable_start
        CHECK (state <> 'RUNNING' OR billable_started_at IS NOT NULL);

ALTER TABLE attempts
    ADD CONSTRAINT attempts_running_requires_started_at
        CHECK (
            (state <> 'ASSIGNED' OR started_at IS NULL)
            AND (state NOT IN ('RUNNING', 'FINALIZING', 'SUCCEEDED') OR started_at IS NOT NULL)
        );

CREATE FUNCTION vela_reject_billable_start_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.billable_started_at IS NOT NULL THEN
            RAISE EXCEPTION 'Billable Start must be unset when a Job is created';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.billable_started_at IS NOT NULL
        AND NEW.billable_started_at IS DISTINCT FROM OLD.billable_started_at
    THEN
        RAISE EXCEPTION 'Billable Start cannot be changed or cleared';
    END IF;
    IF OLD.billable_started_at IS NULL
        AND NEW.billable_started_at IS NOT NULL
        AND (OLD.state <> 'ASSIGNED' OR NEW.state <> 'RUNNING')
    THEN
        RAISE EXCEPTION 'Billable Start requires ASSIGNED to RUNNING transition';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_billable_start_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_billable_start_mutation() OWNER TO vela_internal;

CREATE TRIGGER jobs_billable_start_immutable
BEFORE INSERT OR UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_reject_billable_start_mutation();

CREATE FUNCTION vela_reject_attempt_start_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'ASSIGNED' OR NEW.started_at IS NOT NULL THEN
            RAISE EXCEPTION 'Attempt must be created ASSIGNED without a start time';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at THEN
        RAISE EXCEPTION 'Attempt start cannot be changed or cleared';
    END IF;
    IF OLD.started_at IS NULL
        AND NEW.started_at IS NOT NULL
        AND (OLD.state <> 'ASSIGNED' OR NEW.state <> 'RUNNING')
    THEN
        RAISE EXCEPTION 'Attempt start requires ASSIGNED to RUNNING transition';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_attempt_start_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_attempt_start_mutation() OWNER TO vela_internal;

CREATE TRIGGER attempts_start_immutable
BEFORE INSERT OR UPDATE ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_start_mutation();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS attempts_start_immutable ON attempts;
DROP TRIGGER IF EXISTS jobs_billable_start_immutable ON jobs;
DROP FUNCTION IF EXISTS vela_reject_attempt_start_mutation();
DROP FUNCTION IF EXISTS vela_reject_billable_start_mutation();
ALTER TABLE attempts DROP CONSTRAINT IF EXISTS attempts_running_requires_started_at;
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_running_requires_billable_start;
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_billable_started_after_creation;
ALTER TABLE jobs DROP COLUMN IF EXISTS billable_started_at;
-- +goose StatementEnd
