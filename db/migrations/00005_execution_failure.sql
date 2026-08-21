-- +goose Up
-- +goose StatementBegin
CREATE TYPE execution_failure_source AS ENUM (
    'WORKER_REPORTED', 'EXECUTION_LEASE_EXPIRED', 'JOB_EXPIRED'
);
CREATE TYPE retry_disposition AS ENUM ('RETRY_WAIT', 'FAILED');

ALTER TABLE projects
    ADD COLUMN retry_wait_count integer NOT NULL DEFAULT 0
        CHECK (retry_wait_count >= 0);
ALTER TABLE worker_pools
    ADD COLUMN retry_wait_count integer NOT NULL DEFAULT 0
        CHECK (retry_wait_count >= 0);

ALTER TABLE projects
    DROP CONSTRAINT projects_check,
    ADD CONSTRAINT projects_waiting_count_consistent CHECK (
        queued_count >= retry_wait_count
    );
ALTER TABLE worker_pools
    DROP CONSTRAINT worker_pools_check,
    ADD CONSTRAINT worker_pools_waiting_count_consistent CHECK (
        queued_count >= retry_wait_count
    );

CREATE FUNCTION vela_check_project_normal_queue_bound() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.projects
        WHERE id = NEW.id
          AND queued_count - retry_wait_count > queued_limit
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'projects_normal_queue_bounded',
            MESSAGE = 'Project normal Admission queue exceeds its committed bound';
    END IF;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_check_project_normal_queue_bound() FROM PUBLIC;
ALTER FUNCTION vela_check_project_normal_queue_bound() OWNER TO vela_internal;

CREATE CONSTRAINT TRIGGER projects_normal_queue_bounded
AFTER INSERT OR UPDATE OF queued_count, queued_limit, retry_wait_count ON projects
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_check_project_normal_queue_bound();

CREATE FUNCTION vela_check_worker_pool_normal_queue_bound() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.worker_pools
        WHERE id = NEW.id
          AND queued_count - retry_wait_count > queued_limit
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'worker_pools_normal_queue_bounded',
            MESSAGE = 'Worker pool normal Admission queue exceeds its committed bound';
    END IF;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_check_worker_pool_normal_queue_bound() FROM PUBLIC;
ALTER FUNCTION vela_check_worker_pool_normal_queue_bound() OWNER TO vela_internal;

CREATE CONSTRAINT TRIGGER worker_pools_normal_queue_bounded
AFTER INSERT OR UPDATE OF queued_count, queued_limit, retry_wait_count ON worker_pools
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_check_worker_pool_normal_queue_bound();

CREATE OR REPLACE FUNCTION vela_lock_compatible_pool(
    p_model_revision_id uuid,
    p_generation_preset_revision_id uuid,
    p_output_spec_id uuid
) RETURNS TABLE (
    id uuid,
    admission_open boolean,
    queued_count integer,
    queued_limit integer,
    retry_after_seconds integer
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        wp.id,
        wp.admission_open,
        wp.queued_count - wp.retry_wait_count,
        wp.queued_limit,
        wp.retry_after_seconds
    FROM public.profile_certifications AS pc
    JOIN public.execution_profile_revisions AS epr
      ON epr.id = pc.execution_profile_revision_id
    JOIN public.worker_pools AS wp ON wp.id = epr.worker_pool_id
    WHERE pc.model_revision_id = p_model_revision_id
      AND public.vela_current_request_scope() = 'jobs:submit'
      AND pc.generation_preset_revision_id = p_generation_preset_revision_id
      AND pc.output_spec_id = p_output_spec_id
      AND pc.state = 'ACTIVE'
      AND pc.invalidated_at IS NULL
      AND epr.state = 'ACTIVE'
      AND wp.admission_open
      AND wp.queued_count - wp.retry_wait_count < wp.queued_limit
    ORDER BY
        CASE
            WHEN wp.queued_limit = 0 THEN 1::numeric
            ELSE (wp.queued_count - wp.retry_wait_count)::numeric / wp.queued_limit::numeric
        END,
        wp.stable_id
    LIMIT 1
    FOR SHARE OF pc, epr
    FOR UPDATE OF wp
$$;

ALTER TABLE attempts
    ADD CONSTRAINT attempts_terminal_state_requires_end_time CHECK (
        (state IN ('ASSIGNED', 'RUNNING', 'FINALIZING') AND ended_at IS NULL)
        OR
        (state IN ('SUCCEEDED', 'FAILED', 'LOST', 'CANCELED') AND ended_at IS NOT NULL)
    );

CREATE TABLE execution_failure_decisions (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid,
    worker_id uuid,
    worker_epoch bigint,
    attempt_fence bigint,
    source execution_failure_source NOT NULL,
    disposition retry_disposition NOT NULL,
    attempt_state attempt_state,
    failure_class text NOT NULL CHECK (
        failure_class ~ '^[A-Z][A-Z0-9_]{0,99}$'
    ),
    failure_fingerprint text NOT NULL CHECK (
        failure_fingerprint ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$'
    ),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    error_summary text NOT NULL CHECK (length(error_summary) BETWEEN 1 AND 2000),
    backend_stage text NOT NULL CHECK (length(backend_stage) BETWEEN 1 AND 100),
    gpu_uuids jsonb NOT NULL CHECK (
        jsonb_typeof(gpu_uuids) = 'array'
        AND jsonb_array_length(gpu_uuids) <= 8
    ),
    inference_backend_revision text NOT NULL
        CHECK (length(inference_backend_revision) BETWEEN 1 AND 200),
    retry_recommended boolean NOT NULL,
    worker_reusable boolean NOT NULL,
    attempt_compute_seconds bigint NOT NULL CHECK (attempt_compute_seconds >= 0),
    total_compute_seconds bigint NOT NULL CHECK (total_compute_seconds >= 0),
    next_retry_at timestamptz,
    job_fence bigint NOT NULL CHECK (job_fence > 0),
    job_version bigint NOT NULL CHECK (job_version > 0),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (
        organization_id, project_id, attempt_id, job_id,
        worker_id, worker_epoch, attempt_fence
    ) REFERENCES attempts (
        organization_id, project_id, id, job_id,
        worker_id, worker_epoch, fence
    ),
    CHECK (
        (
            attempt_id IS NULL
            AND worker_id IS NULL
            AND worker_epoch IS NULL
            AND attempt_fence IS NULL
            AND attempt_state IS NULL
            AND source = 'JOB_EXPIRED'
        )
        OR
        (
            attempt_id IS NOT NULL
            AND worker_id IS NOT NULL
            AND worker_epoch IS NOT NULL
            AND worker_epoch > 0
            AND attempt_fence IS NOT NULL
            AND attempt_fence > 0
            AND attempt_state IN ('FAILED', 'LOST')
        )
    ),
    CHECK (
        (disposition = 'RETRY_WAIT' AND next_retry_at > decided_at)
        OR
        (disposition = 'FAILED' AND next_retry_at IS NULL)
    )
);

CREATE UNIQUE INDEX execution_failure_decisions_attempt_once
    ON execution_failure_decisions (attempt_id)
    WHERE attempt_id IS NOT NULL;
CREATE UNIQUE INDEX execution_failure_decisions_job_expiry_without_attempt_once
    ON execution_failure_decisions (job_id)
    WHERE attempt_id IS NULL;

DO $$
BEGIN
    IF to_regclass('vela_private.execution_failure_decisions_rollback') IS NOT NULL THEN
        INSERT INTO public.execution_failure_decisions
        SELECT (
            jsonb_populate_record(
                NULL::public.execution_failure_decisions,
                snapshot.decision
            )
        ).*
        FROM vela_private.execution_failure_decisions_rollback AS snapshot;

        DROP TABLE vela_private.execution_failure_decisions_rollback;
    END IF;
END
$$;

CREATE TABLE execution_retry_evidence (
    job_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    excluded_workers jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(excluded_workers) = 'array'
    ),
    failure_fingerprints jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(failure_fingerprints) = 'array'
    ),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id)
);

INSERT INTO execution_retry_evidence (
    job_id,
    organization_id,
    project_id,
    excluded_workers,
    failure_fingerprints,
    updated_at
)
SELECT
    job_id,
    organization_id,
    project_id,
    '[]'::jsonb,
    '[]'::jsonb,
    updated_at
FROM retry_runtime_states;

DO $$
BEGIN
    IF to_regclass('vela_private.execution_retry_evidence_rollback') IS NOT NULL THEN
        INSERT INTO public.execution_retry_evidence (
            job_id,
            organization_id,
            project_id,
            excluded_workers,
            failure_fingerprints,
            updated_at
        )
        SELECT
            job_id,
            organization_id,
            project_id,
            excluded_workers,
            failure_fingerprints,
            updated_at
        FROM vela_private.execution_retry_evidence_rollback
        ON CONFLICT (job_id) DO UPDATE
        SET excluded_workers = EXCLUDED.excluded_workers,
            failure_fingerprints = EXCLUDED.failure_fingerprints,
            updated_at = EXCLUDED.updated_at;

        DROP TABLE vela_private.execution_retry_evidence_rollback;
    END IF;
END
$$;

UPDATE retry_runtime_states
SET excluded_workers = '[]'::jsonb,
    failure_fingerprints = '[]'::jsonb;

CREATE FUNCTION vela_seed_execution_retry_evidence() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.attempts_started IS DISTINCT FROM 0
        OR NEW.compute_seconds_consumed IS DISTINCT FROM 0
        OR NEW.finalization_seconds_consumed IS DISTINCT FROM 0
        OR NEW.finalization_retry_count IS DISTINCT FROM 0
        OR NEW.next_retry_at IS NOT NULL
        OR NEW.excluded_workers IS DISTINCT FROM '[]'::jsonb
        OR NEW.failure_fingerprints IS DISTINCT FROM '[]'::jsonb
        OR NEW.circuit_breaker_state IS DISTINCT FROM '{}'::jsonb
        OR NEW.last_failure_class IS NOT NULL
        OR NEW.version IS DISTINCT FROM 1
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'retry_runtime_states_canonical_initial_state',
            MESSAGE = 'RetryRuntimeState must be inserted in its canonical initial state';
    END IF;

    INSERT INTO public.execution_retry_evidence (
        job_id,
        organization_id,
        project_id,
        excluded_workers,
        failure_fingerprints,
        updated_at
    ) VALUES (
        NEW.job_id,
        NEW.organization_id,
        NEW.project_id,
        '[]'::jsonb,
        '[]'::jsonb,
        NEW.updated_at
    );
    NEW.excluded_workers := '[]'::jsonb;
    NEW.failure_fingerprints := '[]'::jsonb;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_seed_execution_retry_evidence() FROM PUBLIC;
ALTER FUNCTION vela_seed_execution_retry_evidence() OWNER TO vela_internal;

CREATE TRIGGER retry_runtime_states_seed_execution_retry_evidence
BEFORE INSERT ON retry_runtime_states
FOR EACH ROW EXECUTE FUNCTION vela_seed_execution_retry_evidence();

CREATE FUNCTION vela_reject_excluded_worker_attempt() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.execution_retry_evidence AS evidence
        CROSS JOIN LATERAL jsonb_array_elements(evidence.excluded_workers) AS excluded(worker)
        WHERE evidence.job_id = NEW.job_id
          AND excluded.worker ->> 'worker_id' = NEW.worker_id::text
          AND (excluded.worker ->> 'expires_at')::timestamptz > clock_timestamp()
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_retry_evidence_worker_excluded',
            MESSAGE = 'Worker is excluded by protected Job retry evidence';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_excluded_worker_attempt() FROM PUBLIC;
ALTER FUNCTION vela_reject_excluded_worker_attempt() OWNER TO vela_internal;

CREATE TRIGGER attempts_reject_excluded_worker
BEFORE INSERT ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_reject_excluded_worker_attempt();

CREATE FUNCTION vela_decrement_retry_subset_on_assignment() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF OLD.state = 'RETRY_WAIT' AND NEW.state = 'ASSIGNED' THEN
        UPDATE public.projects
        SET retry_wait_count = retry_wait_count - 1
        WHERE organization_id = NEW.organization_id
          AND id = NEW.project_id
          AND retry_wait_count > 0;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'projects_retry_wait_assignment_counter',
                MESSAGE = 'Project retry-wait counter is inconsistent with Assignment';
        END IF;

        UPDATE public.worker_pools
        SET retry_wait_count = retry_wait_count - 1
        WHERE id = NEW.worker_pool_id
          AND retry_wait_count > 0;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_pools_retry_wait_assignment_counter',
                MESSAGE = 'Worker pool retry-wait counter is inconsistent with Assignment';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_decrement_retry_subset_on_assignment() FROM PUBLIC;
ALTER FUNCTION vela_decrement_retry_subset_on_assignment() OWNER TO vela_internal;

CREATE TRIGGER jobs_decrement_retry_subset_on_assignment
AFTER UPDATE OF state ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_decrement_retry_subset_on_assignment();

ALTER TABLE execution_failure_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_failure_decisions FORCE ROW LEVEL SECURITY;
ALTER TABLE execution_retry_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_retry_evidence FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_execution_failure_decision_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Execution failure decisions are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_execution_failure_decision_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_execution_failure_decision_mutation() OWNER TO vela_internal;

CREATE TRIGGER execution_failure_decisions_immutable
BEFORE UPDATE OR DELETE ON execution_failure_decisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_execution_failure_decision_mutation();

GRANT SELECT, INSERT ON execution_failure_decisions TO vela_internal;
GRANT SELECT, INSERT, UPDATE, DELETE ON execution_retry_evidence TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    workers,
    attempt_leases,
    attempts,
    jobs,
    retry_runtime_states,
    credit_reservations,
    projects,
    worker_pools,
    organization_credit_accounts,
    execution_failure_decisions,
    execution_retry_evidence,
    outbox_events
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM projects WHERE retry_wait_count <> 0
    ) OR EXISTS (
        SELECT 1 FROM worker_pools WHERE retry_wait_count <> 0
    ) OR EXISTS (
        SELECT 1 FROM jobs WHERE state = 'RETRY_WAIT'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_failure_contract_requires_drained_retry_wait',
            MESSAGE = 'Migration 00005 cannot contract while RETRY_WAIT Jobs remain; roll back binaries against the expanded schema or drain first';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM execution_failure_decisions) THEN
        CREATE TABLE vela_private.execution_failure_decisions_rollback AS
        SELECT to_jsonb(decision) AS decision
        FROM execution_failure_decisions AS decision;
        ALTER TABLE vela_private.execution_failure_decisions_rollback OWNER TO vela_internal;
        REVOKE ALL ON vela_private.execution_failure_decisions_rollback
            FROM PUBLIC, vela_request, vela_auth;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM execution_retry_evidence
        WHERE excluded_workers <> '[]'::jsonb
           OR failure_fingerprints <> '[]'::jsonb
    ) THEN
        CREATE TABLE vela_private.execution_retry_evidence_rollback AS
        SELECT
            job_id,
            organization_id,
            project_id,
            excluded_workers,
            failure_fingerprints,
            updated_at
        FROM execution_retry_evidence
        WHERE excluded_workers <> '[]'::jsonb
           OR failure_fingerprints <> '[]'::jsonb;
        ALTER TABLE vela_private.execution_retry_evidence_rollback OWNER TO vela_internal;
        REVOKE ALL ON vela_private.execution_retry_evidence_rollback
            FROM PUBLIC, vela_request, vela_auth;
    END IF;
END
$$;

DROP TRIGGER IF EXISTS jobs_decrement_retry_subset_on_assignment ON jobs;
DROP FUNCTION IF EXISTS vela_decrement_retry_subset_on_assignment();
DROP TRIGGER IF EXISTS attempts_reject_excluded_worker ON attempts;
DROP FUNCTION IF EXISTS vela_reject_excluded_worker_attempt();
DROP TRIGGER IF EXISTS retry_runtime_states_seed_execution_retry_evidence ON retry_runtime_states;
DROP FUNCTION IF EXISTS vela_seed_execution_retry_evidence();
DROP TRIGGER IF EXISTS execution_failure_decisions_immutable ON execution_failure_decisions;
DROP FUNCTION IF EXISTS vela_reject_execution_failure_decision_mutation();
DROP TABLE IF EXISTS execution_retry_evidence;
DROP TABLE IF EXISTS execution_failure_decisions;
ALTER TABLE attempts
    DROP CONSTRAINT IF EXISTS attempts_terminal_state_requires_end_time;
DROP TRIGGER IF EXISTS worker_pools_normal_queue_bounded ON worker_pools;
DROP FUNCTION IF EXISTS vela_check_worker_pool_normal_queue_bound();
DROP TRIGGER IF EXISTS projects_normal_queue_bounded ON projects;
DROP FUNCTION IF EXISTS vela_check_project_normal_queue_bound();
CREATE OR REPLACE FUNCTION vela_lock_compatible_pool(
    p_model_revision_id uuid,
    p_generation_preset_revision_id uuid,
    p_output_spec_id uuid
) RETURNS TABLE (
    id uuid,
    admission_open boolean,
    queued_count integer,
    queued_limit integer,
    retry_after_seconds integer
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        wp.id,
        wp.admission_open,
        wp.queued_count,
        wp.queued_limit,
        wp.retry_after_seconds
    FROM public.profile_certifications AS pc
    JOIN public.execution_profile_revisions AS epr
      ON epr.id = pc.execution_profile_revision_id
    JOIN public.worker_pools AS wp ON wp.id = epr.worker_pool_id
    WHERE pc.model_revision_id = p_model_revision_id
      AND public.vela_current_request_scope() = 'jobs:submit'
      AND pc.generation_preset_revision_id = p_generation_preset_revision_id
      AND pc.output_spec_id = p_output_spec_id
      AND pc.state = 'ACTIVE'
      AND pc.invalidated_at IS NULL
      AND epr.state = 'ACTIVE'
      AND wp.admission_open
      AND wp.queued_count < wp.queued_limit
    ORDER BY
        CASE
            WHEN wp.queued_limit = 0 THEN 1::numeric
            ELSE wp.queued_count::numeric / wp.queued_limit::numeric
        END,
        wp.stable_id
    LIMIT 1
    FOR SHARE OF pc, epr
    FOR UPDATE OF wp
$$;
ALTER TABLE worker_pools
    DROP CONSTRAINT IF EXISTS worker_pools_waiting_count_consistent,
    ADD CONSTRAINT worker_pools_check CHECK (
        queued_count >= 0 AND queued_count <= queued_limit
    ),
    DROP COLUMN IF EXISTS retry_wait_count;
ALTER TABLE projects
    DROP CONSTRAINT IF EXISTS projects_waiting_count_consistent,
    ADD CONSTRAINT projects_check CHECK (
        queued_count >= 0 AND queued_count <= queued_limit
    ),
    DROP COLUMN IF EXISTS retry_wait_count;
DROP TYPE IF EXISTS retry_disposition;
DROP TYPE IF EXISTS execution_failure_source;
-- +goose StatementEnd
