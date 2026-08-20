-- +goose Up
-- +goose StatementBegin
CREATE TYPE worker_lifecycle_state AS ENUM (
    'REGISTERING', 'WARMING', 'READY', 'BUSY', 'DRAINING', 'RECOVERING', 'QUARANTINED'
);
CREATE TYPE worker_reachability_condition AS ENUM ('HEALTHY', 'SUSPECT', 'OFFLINE');
CREATE TYPE attempt_state AS ENUM ('ASSIGNED', 'RUNNING', 'FINALIZING', 'SUCCEEDED', 'FAILED', 'LOST', 'CANCELED');
CREATE TYPE lease_phase AS ENUM ('EXECUTION', 'FINALIZATION');
CREATE TYPE lease_owner_kind AS ENUM ('WORKER', 'RECONCILER');

ALTER TABLE jobs
    ADD COLUMN current_fence bigint NOT NULL DEFAULT 0 CHECK (current_fence >= 0);
ALTER TABLE jobs
    ADD CONSTRAINT jobs_assignment_pool_identity
    UNIQUE (organization_id, project_id, id, worker_pool_id);
ALTER TABLE execution_profile_revisions
    ADD CONSTRAINT execution_profile_revisions_assignment_pool_identity
    UNIQUE (id, worker_pool_id);

CREATE TABLE workers (
    id uuid PRIMARY KEY,
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    spiffe_id text NOT NULL UNIQUE CHECK (length(spiffe_id) BETWEEN 1 AND 500),
    epoch bigint NOT NULL CHECK (epoch > 0),
    lifecycle_state worker_lifecycle_state NOT NULL DEFAULT 'REGISTERING',
    reachability_condition worker_reachability_condition NOT NULL DEFAULT 'SUSPECT',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (worker_pool_id, id)
);

CREATE TABLE worker_epochs (
    worker_id uuid NOT NULL REFERENCES workers(id),
    epoch bigint NOT NULL CHECK (epoch > 0),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (worker_id, epoch)
);

CREATE FUNCTION vela_record_worker_epoch() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.epoch <= OLD.epoch THEN
        RAISE EXCEPTION 'Worker epoch must increase from %, got %', OLD.epoch, NEW.epoch
            USING ERRCODE = '23514';
    END IF;
    INSERT INTO worker_epochs (worker_id, epoch)
    VALUES (NEW.id, NEW.epoch);
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_record_worker_epoch() FROM PUBLIC;
ALTER FUNCTION vela_record_worker_epoch() OWNER TO vela_internal;

CREATE TRIGGER workers_record_epoch
AFTER INSERT OR UPDATE OF epoch ON workers
FOR EACH ROW EXECUTE FUNCTION vela_record_worker_epoch();

CREATE TABLE attempts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    execution_profile_revision_id uuid NOT NULL REFERENCES execution_profile_revisions(id),
    worker_pool_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL,
    state attempt_state NOT NULL DEFAULT 'ASSIGNED',
    fence bigint NOT NULL CHECK (fence > 0),
    assigned_at timestamptz NOT NULL,
    started_at timestamptz,
    finalization_started_at timestamptz,
    finalization_deadline_at timestamptz,
    ended_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, id, worker_id, worker_epoch, fence),
    UNIQUE (job_id, attempt_number),
    UNIQUE (job_id, fence),
    FOREIGN KEY (organization_id, project_id, job_id, worker_pool_id)
        REFERENCES jobs(organization_id, project_id, id, worker_pool_id),
    FOREIGN KEY (execution_profile_revision_id, worker_pool_id)
        REFERENCES execution_profile_revisions(id, worker_pool_id),
    FOREIGN KEY (worker_pool_id, worker_id) REFERENCES workers(worker_pool_id, id),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch),
    CHECK (started_at IS NULL OR started_at >= assigned_at),
    CHECK (finalization_started_at IS NULL OR finalization_started_at >= assigned_at),
    CHECK (
        finalization_deadline_at IS NULL
        OR (
            finalization_started_at IS NOT NULL
            AND finalization_deadline_at > finalization_started_at
        )
    ),
    CHECK (ended_at IS NULL OR ended_at >= assigned_at)
);

CREATE UNIQUE INDEX attempts_one_active_per_job
    ON attempts (job_id)
    WHERE state IN ('ASSIGNED', 'RUNNING', 'FINALIZING');
CREATE UNIQUE INDEX attempts_one_active_per_worker
    ON attempts (worker_id)
    WHERE state IN ('ASSIGNED', 'RUNNING', 'FINALIZING');

CREATE TABLE attempt_leases (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL,
    phase lease_phase NOT NULL,
    owner_kind lease_owner_kind NOT NULL,
    owner_id text NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 500),
    fence bigint NOT NULL CHECK (fence > 0),
    token_digest bytea NOT NULL CHECK (octet_length(token_digest) = 32),
    signing_key_id text NOT NULL CHECK (length(signing_key_id) BETWEEN 1 AND 100),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (
        organization_id, project_id, attempt_id, worker_id, worker_epoch, fence
    ) REFERENCES attempts (
        organization_id, project_id, id, worker_id, worker_epoch, fence
    ),
    CHECK (expires_at > issued_at),
    CHECK (revoked_at IS NULL OR revoked_at >= issued_at),
    CHECK (owner_kind <> 'WORKER' OR owner_id <> '')
);

CREATE UNIQUE INDEX attempt_leases_one_active_per_attempt
    ON attempt_leases (attempt_id)
    WHERE revoked_at IS NULL;
CREATE UNIQUE INDEX attempt_leases_one_active_per_worker
    ON attempt_leases (worker_id)
    WHERE revoked_at IS NULL AND owner_kind = 'WORKER';

ALTER TABLE attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE attempts FORCE ROW LEVEL SECURITY;
ALTER TABLE attempt_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE attempt_leases FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_validate_job_state_transition() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'QUEUED' AND NEW.state IN ('ASSIGNED', 'CANCELED', 'FAILED'))
        OR (OLD.state = 'ASSIGNED' AND NEW.state IN ('RUNNING', 'RETRY_WAIT', 'CANCELED', 'FAILED'))
        OR (OLD.state = 'RUNNING' AND NEW.state IN ('FINALIZING', 'RETRY_WAIT', 'CANCELING', 'FAILED'))
        OR (OLD.state = 'FINALIZING' AND NEW.state IN ('SUCCEEDED', 'RETRY_WAIT', 'CANCELING', 'FAILED'))
        OR (OLD.state = 'RETRY_WAIT' AND NEW.state IN ('ASSIGNED', 'CANCELED', 'FAILED'))
        OR (OLD.state = 'CANCELING' AND NEW.state = 'CANCELED')
    ) THEN
        RAISE EXCEPTION 'invalid Job state transition from % to %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_job_state_transition() FROM PUBLIC;
ALTER FUNCTION vela_validate_job_state_transition() OWNER TO vela_internal;

CREATE TRIGGER jobs_state_transition_valid
BEFORE UPDATE OF state ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_validate_job_state_transition();

CREATE FUNCTION vela_validate_attempt_state_transition() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'ASSIGNED' AND NEW.state IN ('RUNNING', 'FAILED', 'LOST', 'CANCELED'))
        OR (OLD.state = 'RUNNING' AND NEW.state IN ('FINALIZING', 'FAILED', 'LOST', 'CANCELED'))
        OR (OLD.state = 'FINALIZING' AND NEW.state IN ('SUCCEEDED', 'FAILED', 'LOST', 'CANCELED'))
    ) THEN
        RAISE EXCEPTION 'invalid Attempt state transition from % to %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_attempt_state_transition() FROM PUBLIC;
ALTER FUNCTION vela_validate_attempt_state_transition() OWNER TO vela_internal;

CREATE TRIGGER attempts_state_transition_valid
BEFORE UPDATE OF state ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_validate_attempt_state_transition();

CREATE FUNCTION vela_reject_attempt_identity_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.job_id IS DISTINCT FROM OLD.job_id
        OR NEW.attempt_number IS DISTINCT FROM OLD.attempt_number
        OR NEW.execution_profile_revision_id IS DISTINCT FROM OLD.execution_profile_revision_id
        OR NEW.worker_pool_id IS DISTINCT FROM OLD.worker_pool_id
        OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
        OR NEW.worker_epoch IS DISTINCT FROM OLD.worker_epoch
        OR NEW.fence IS DISTINCT FROM OLD.fence
        OR NEW.assigned_at IS DISTINCT FROM OLD.assigned_at
    THEN
        RAISE EXCEPTION 'immutable Attempt identity fields cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_attempt_identity_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_attempt_identity_mutation() OWNER TO vela_internal;

CREATE TRIGGER attempts_identity_immutable
BEFORE UPDATE ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_identity_mutation();

CREATE FUNCTION vela_reject_lease_identity_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
        OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
        OR NEW.worker_epoch IS DISTINCT FROM OLD.worker_epoch
        OR NEW.owner_kind IS DISTINCT FROM OLD.owner_kind
        OR NEW.owner_id IS DISTINCT FROM OLD.owner_id
        OR NEW.fence IS DISTINCT FROM OLD.fence
        OR NEW.token_digest IS DISTINCT FROM OLD.token_digest
        OR NEW.signing_key_id IS DISTINCT FROM OLD.signing_key_id
        OR NEW.issued_at IS DISTINCT FROM OLD.issued_at
    THEN
        RAISE EXCEPTION 'immutable Lease identity fields cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_lease_identity_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_lease_identity_mutation() OWNER TO vela_internal;

CREATE TRIGGER attempt_leases_identity_immutable
BEFORE UPDATE ON attempt_leases
FOR EACH ROW EXECUTE FUNCTION vela_reject_lease_identity_mutation();

GRANT SELECT, INSERT, UPDATE, DELETE ON workers, worker_epochs, attempts, attempt_leases TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS attempt_leases_identity_immutable ON attempt_leases;
DROP TRIGGER IF EXISTS attempts_identity_immutable ON attempts;
DROP TRIGGER IF EXISTS attempts_state_transition_valid ON attempts;
DROP TRIGGER IF EXISTS jobs_state_transition_valid ON jobs;
DROP TRIGGER IF EXISTS workers_record_epoch ON workers;
DROP FUNCTION IF EXISTS vela_reject_lease_identity_mutation();
DROP FUNCTION IF EXISTS vela_reject_attempt_identity_mutation();
DROP FUNCTION IF EXISTS vela_validate_attempt_state_transition();
DROP FUNCTION IF EXISTS vela_validate_job_state_transition();
DROP FUNCTION IF EXISTS vela_record_worker_epoch();
DROP TABLE IF EXISTS attempt_leases;
DROP TABLE IF EXISTS attempts;
DROP TABLE IF EXISTS worker_epochs;
DROP TABLE IF EXISTS workers;
ALTER TABLE execution_profile_revisions
    DROP CONSTRAINT IF EXISTS execution_profile_revisions_assignment_pool_identity;
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_assignment_pool_identity;
ALTER TABLE jobs DROP COLUMN IF EXISTS current_fence;
DROP TYPE IF EXISTS lease_owner_kind;
DROP TYPE IF EXISTS lease_phase;
DROP TYPE IF EXISTS attempt_state;
DROP TYPE IF EXISTS worker_reachability_condition;
DROP TYPE IF EXISTS worker_lifecycle_state;
-- +goose StatementEnd
