-- +goose Up
-- +goose StatementBegin
ALTER TABLE workers
    ADD COLUMN last_heartbeat_at timestamptz;

CREATE TABLE execution_lease_renewal_protocol (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    enabled boolean NOT NULL DEFAULT false,
    transition_receipt text NOT NULL CHECK (length(transition_receipt) BETWEEN 1 AND 1000),
    transitioned_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO execution_lease_renewal_protocol (
    singleton, enabled, transition_receipt
) VALUES (
    true, false, 'expanded by migration 00004; switch not yet authorized'
);

REVOKE ALL ON execution_lease_renewal_protocol FROM PUBLIC;
GRANT SELECT ON execution_lease_renewal_protocol TO vela_internal;

CREATE VIEW vela_request_execution_lease_renewal_protocol
WITH (security_barrier = true) AS
SELECT enabled
FROM execution_lease_renewal_protocol
WHERE singleton;
REVOKE ALL ON vela_request_execution_lease_renewal_protocol FROM PUBLIC;
GRANT SELECT ON vela_request_execution_lease_renewal_protocol TO vela_request;

CREATE FUNCTION vela_lock_execution_lease_renewal_protocol() RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT enabled
    FROM public.execution_lease_renewal_protocol
    WHERE singleton
    FOR SHARE
$$;
REVOKE ALL ON FUNCTION vela_lock_execution_lease_renewal_protocol() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_lock_execution_lease_renewal_protocol() TO vela_internal;

ALTER TABLE attempt_leases
    ADD COLUMN token_claim_expires_at timestamptz,
    ADD COLUMN renewal_protocol_version smallint NOT NULL DEFAULT 1
        CHECK (renewal_protocol_version IN (1, 2));

UPDATE attempt_leases
SET token_claim_expires_at = expires_at;

CREATE FUNCTION vela_set_lease_token_claim_expiry() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.token_claim_expires_at IS NULL THEN
            NEW.token_claim_expires_at := NEW.expires_at;
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.token_claim_expires_at IS DISTINCT FROM OLD.token_claim_expires_at THEN
        RAISE EXCEPTION 'Lease token claim expiry cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_set_lease_token_claim_expiry() FROM PUBLIC;
ALTER FUNCTION vela_set_lease_token_claim_expiry() OWNER TO vela_internal;

CREATE TRIGGER attempt_leases_token_claim_expiry
BEFORE INSERT OR UPDATE ON attempt_leases
FOR EACH ROW EXECUTE FUNCTION vela_set_lease_token_claim_expiry();

CREATE FUNCTION vela_guard_execution_lease_renewal_protocol() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    renewal_enabled boolean;
BEGIN
    SELECT enabled
    INTO STRICT renewal_enabled
    FROM public.execution_lease_renewal_protocol
    WHERE singleton
    FOR SHARE;

    IF TG_OP = 'INSERT' THEN
        IF renewal_enabled
            AND NEW.phase = 'EXECUTION'
            AND NEW.renewal_protocol_version < 2
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'execution_lease_renewal_protocol_legacy_writer',
                MESSAGE = 'legacy Lease writer is disabled after renewal protocol switch';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.renewal_protocol_version IS DISTINCT FROM OLD.renewal_protocol_version THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_lease_renewal_protocol_version_immutable',
            MESSAGE = 'Lease renewal protocol version cannot be changed';
    END IF;
    IF NEW.expires_at > OLD.expires_at
        AND (OLD.phase = 'EXECUTION' OR NEW.phase = 'EXECUTION')
    THEN
        IF OLD.renewal_protocol_version < 2 THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'execution_lease_renewal_protocol_legacy_lease',
                MESSAGE = 'legacy Lease expiry cannot be renewed';
        END IF;
        IF NOT renewal_enabled THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'execution_lease_renewal_protocol_disabled',
                MESSAGE = 'execution Lease renewal protocol has not been switched on';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_execution_lease_renewal_protocol() FROM PUBLIC;

CREATE TRIGGER attempt_leases_renewal_protocol_guard
BEFORE INSERT OR UPDATE ON attempt_leases
FOR EACH ROW EXECUTE FUNCTION vela_guard_execution_lease_renewal_protocol();

CREATE FUNCTION vela_transition_execution_lease_renewal_protocol(
    requested_enabled boolean,
    receipt text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    current_enabled boolean;
BEGIN
    IF receipt IS NULL OR length(receipt) NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'a non-empty execution Lease renewal transition receipt is required';
    END IF;

    LOCK TABLE public.attempt_leases IN SHARE ROW EXCLUSIVE MODE;
    SELECT enabled
    INTO STRICT current_enabled
    FROM public.execution_lease_renewal_protocol
    WHERE singleton
    FOR UPDATE;

    IF current_enabled = requested_enabled THEN
        IF requested_enabled THEN
            EXECUTE 'REVOKE SELECT ON public.retry_runtime_states FROM vela_request';
        ELSE
            EXECUTE 'GRANT SELECT ON public.retry_runtime_states TO vela_request';
        END IF;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.attempt_leases
        WHERE phase = 'EXECUTION'
          AND revoked_at IS NULL
          AND expires_at > clock_timestamp()
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_lease_renewal_protocol_active_leases',
            MESSAGE = 'active EXECUTION Leases must drain before renewal protocol transition';
    END IF;

    IF requested_enabled THEN
        EXECUTE 'REVOKE SELECT ON public.retry_runtime_states FROM vela_request';
    ELSE
        EXECUTE 'GRANT SELECT ON public.retry_runtime_states TO vela_request';
    END IF;

    UPDATE public.execution_lease_renewal_protocol
    SET enabled = requested_enabled,
        transition_receipt = receipt,
        transitioned_at = clock_timestamp()
    WHERE singleton;
END
$$;
REVOKE ALL ON FUNCTION vela_transition_execution_lease_renewal_protocol(boolean, text) FROM PUBLIC;

ALTER TABLE attempt_leases
    ALTER COLUMN token_claim_expires_at SET NOT NULL,
    ADD CONSTRAINT attempt_leases_token_claim_expiry_after_issue
        CHECK (token_claim_expires_at > issued_at);

ALTER TABLE attempts
    ADD CONSTRAINT attempts_progress_identity
    UNIQUE (
        organization_id, project_id, id, job_id, worker_id, worker_epoch, fence
    );

CREATE TYPE execution_phase AS ENUM (
    'QUEUED', 'PREPARING', 'GENERATING', 'FINALIZING', 'RETRY_WAIT'
);

ALTER TABLE jobs
    ADD COLUMN execution_phase execution_phase
    GENERATED ALWAYS AS (
        CASE state
        WHEN 'QUEUED' THEN 'QUEUED'::public.execution_phase
        WHEN 'ASSIGNED' THEN 'PREPARING'::public.execution_phase
        WHEN 'RUNNING' THEN 'GENERATING'::public.execution_phase
        WHEN 'FINALIZING' THEN 'FINALIZING'::public.execution_phase
        WHEN 'RETRY_WAIT' THEN 'RETRY_WAIT'::public.execution_phase
        ELSE NULL
        END
    ) STORED;

CREATE TABLE attempt_progress (
    attempt_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL,
    fence bigint NOT NULL CHECK (fence > 0),
    heartbeat_sequence bigint NOT NULL CHECK (heartbeat_sequence > 0),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    backend_stage text NOT NULL CHECK (length(backend_stage) BETWEEN 1 AND 100),
    execution_phase execution_phase NOT NULL
        CHECK (execution_phase IN ('PREPARING', 'GENERATING')),
    phase_progress double precision CHECK (phase_progress >= 0 AND phase_progress < 1),
    estimated_remaining_seconds bigint CHECK (
        estimated_remaining_seconds BETWEEN 0 AND 9223372036
    ),
    estimated_finish_at timestamptz,
    gpu_health_summary jsonb NOT NULL CHECK (
        jsonb_typeof(gpu_health_summary) = 'object'
        AND octet_length(gpu_health_summary::text) <= 16384
    ),
    local_artifact_state jsonb NOT NULL CHECK (
        jsonb_typeof(local_artifact_state) = 'object'
        AND octet_length(local_artifact_state::text) <= 16384
    ),
    scratch_free_bytes bigint NOT NULL CHECK (scratch_free_bytes >= 0),
    artifact_store_reachable boolean NOT NULL,
    progress_updated_at timestamptz NOT NULL,
    progress_valid_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL,
    UNIQUE (organization_id, project_id, job_id, fence),
    CONSTRAINT attempt_progress_attempt_identity_fkey FOREIGN KEY (
        organization_id, project_id, attempt_id, job_id, worker_id, worker_epoch, fence
    ) REFERENCES attempts (
        organization_id, project_id, id, job_id, worker_id, worker_epoch, fence
    ),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    CHECK (
        (estimated_remaining_seconds IS NULL) = (estimated_finish_at IS NULL)
    ),
    CHECK (progress_valid_until > progress_updated_at)
);

CREATE FUNCTION vela_reject_attempt_progress_regression() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.job_id IS DISTINCT FROM OLD.job_id
        OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
        OR NEW.worker_epoch IS DISTINCT FROM OLD.worker_epoch
        OR NEW.fence IS DISTINCT FROM OLD.fence
    THEN
        RAISE EXCEPTION 'immutable Attempt progress identity fields cannot be changed';
    END IF;
    IF NEW.heartbeat_sequence <= OLD.heartbeat_sequence
        OR NEW.progress_updated_at <= OLD.progress_updated_at
        OR NEW.updated_at <= OLD.updated_at
    THEN
        RAISE EXCEPTION 'Attempt progress sequence and update time must increase';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_attempt_progress_regression() FROM PUBLIC;
ALTER FUNCTION vela_reject_attempt_progress_regression() OWNER TO vela_internal;

CREATE TRIGGER attempt_progress_identity_and_sequence
BEFORE UPDATE ON attempt_progress
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_progress_regression();

ALTER TABLE attempt_progress ENABLE ROW LEVEL SECURITY;
ALTER TABLE attempt_progress FORCE ROW LEVEL SECURITY;

DROP POLICY retry_runtime_states_request_policy ON retry_runtime_states;
CREATE POLICY retry_runtime_states_request_select_policy ON retry_runtime_states
    FOR SELECT TO vela_request
    USING (
        vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );
CREATE POLICY retry_runtime_states_request_insert_policy ON retry_runtime_states
    FOR INSERT TO vela_request
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

CREATE POLICY attempt_progress_request_select_policy ON attempt_progress
    FOR SELECT TO vela_request
    USING (
        vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

CREATE VIEW vela_request_job_runtime
WITH (security_barrier = true) AS
SELECT
    job_id,
    attempts_started,
    next_retry_at
FROM retry_runtime_states
WHERE vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
  AND organization_id = vela_current_organization_id()
  AND project_id = vela_current_project_id();
REVOKE ALL ON vela_request_job_runtime FROM PUBLIC;
GRANT SELECT ON vela_request_job_runtime TO vela_request;

CREATE VIEW vela_request_job_progress
WITH (security_barrier = true) AS
WITH postgres_time AS MATERIALIZED (
    SELECT clock_timestamp() AS observed_at
)
SELECT
    ap.job_id,
    CASE
        WHEN ap.progress_valid_until > postgres_time.observed_at THEN ap.phase_progress
        ELSE NULL
    END::double precision AS phase_progress,
    CASE
        WHEN ap.progress_valid_until > postgres_time.observed_at THEN ap.estimated_finish_at
        ELSE NULL
    END::timestamptz AS estimated_finish_at,
    ap.progress_updated_at
FROM jobs AS j
JOIN attempt_progress AS ap
 ON ap.job_id = j.id
 AND ap.fence = j.current_fence
 AND ap.execution_phase = j.execution_phase
CROSS JOIN postgres_time
WHERE vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
  AND j.organization_id = vela_current_organization_id()
  AND j.project_id = vela_current_project_id();
REVOKE ALL ON vela_request_job_progress FROM PUBLIC;
GRANT SELECT ON vela_request_job_progress TO vela_request;

-- The disabled expand phase must retain the exact N-1 request-role privilege set.
GRANT SELECT ON retry_runtime_states TO vela_request;
REVOKE SELECT ON attempt_progress FROM vela_request;
GRANT SELECT, INSERT, UPDATE, DELETE ON attempt_progress TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS vela_request_job_progress;
DROP VIEW IF EXISTS vela_request_job_runtime;
REVOKE SELECT ON attempt_progress FROM vela_request;
DROP TABLE IF EXISTS attempt_progress;
DROP FUNCTION IF EXISTS vela_reject_attempt_progress_regression();
ALTER TABLE jobs DROP COLUMN IF EXISTS execution_phase;
DROP TYPE IF EXISTS execution_phase;
ALTER TABLE attempts DROP CONSTRAINT IF EXISTS attempts_progress_identity;

DROP POLICY IF EXISTS retry_runtime_states_request_insert_policy ON retry_runtime_states;
DROP POLICY IF EXISTS retry_runtime_states_request_select_policy ON retry_runtime_states;
CREATE POLICY retry_runtime_states_request_policy ON retry_runtime_states
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );
REVOKE SELECT ON retry_runtime_states FROM vela_request;
GRANT SELECT ON retry_runtime_states TO vela_request;

ALTER TABLE attempt_leases
    DROP CONSTRAINT IF EXISTS attempt_leases_token_claim_expiry_after_issue;
DROP TRIGGER IF EXISTS attempt_leases_renewal_protocol_guard ON attempt_leases;
DROP TRIGGER IF EXISTS attempt_leases_token_claim_expiry ON attempt_leases;
DROP FUNCTION IF EXISTS vela_transition_execution_lease_renewal_protocol(boolean, text);
DROP FUNCTION IF EXISTS vela_guard_execution_lease_renewal_protocol();
DROP FUNCTION IF EXISTS vela_set_lease_token_claim_expiry();
DROP FUNCTION IF EXISTS vela_lock_execution_lease_renewal_protocol();
ALTER TABLE attempt_leases
    DROP COLUMN IF EXISTS renewal_protocol_version,
    DROP COLUMN IF EXISTS token_claim_expires_at;
DROP VIEW IF EXISTS vela_request_execution_lease_renewal_protocol;
DROP TABLE IF EXISTS execution_lease_renewal_protocol;
ALTER TABLE workers DROP COLUMN IF EXISTS last_heartbeat_at;
-- +goose StatementEnd
