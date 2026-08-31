-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_graph_instantiation_state AS ENUM (
    'PENDING', 'CLAIMED', 'COMPLETED', 'DISCARDED'
);

CREATE TABLE stage_graph_instantiation_work (
    job_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    stage_cutover_revision_id uuid NOT NULL,
    command_id uuid NOT NULL UNIQUE,
    expected_job_version bigint NOT NULL CHECK (expected_job_version > 0),
    expected_job_fence bigint NOT NULL CHECK (expected_job_fence >= 0),
    execution_graph_snapshot_id uuid NOT NULL UNIQUE,
    execution_graph_revision_id uuid NOT NULL,
    execution_profile_revision_id uuid NOT NULL,
    attempt_id uuid NOT NULL UNIQUE,
    storage_reservation_id uuid NOT NULL UNIQUE,
    reserved_storage_bytes bigint NOT NULL CHECK (reserved_storage_bytes > 0),
    source text NOT NULL CHECK (source IN ('MIGRATION_BACKFILL', 'ADMISSION_TRIGGER')),
    state stage_graph_instantiation_state NOT NULL DEFAULT 'PENDING',
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_owner text,
    claim_token uuid,
    claimed_at timestamptz,
    claim_expires_at timestamptz,
    claim_count integer NOT NULL DEFAULT 0 CHECK (claim_count >= 0),
    last_error text CHECK (last_error IS NULL OR length(last_error) BETWEEN 1 AND 2000),
    completed_at timestamptz,
    completion_reason text CHECK (
        completion_reason IS NULL OR length(completion_reason) BETWEEN 1 AND 500
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (
        stage_cutover_revision_id,
        execution_graph_revision_id,
        execution_profile_revision_id
    ) REFERENCES stage_cutover_revisions(
        id,
        execution_graph_revision_id,
        execution_profile_revision_id
    ),
    CHECK (claim_expires_at IS NULL OR claim_expires_at > claimed_at),
    CHECK (
        (
            state = 'PENDING'
            AND claim_owner IS NULL
            AND claim_token IS NULL
            AND claimed_at IS NULL
            AND claim_expires_at IS NULL
            AND completed_at IS NULL
            AND completion_reason IS NULL
        ) OR (
            state = 'CLAIMED'
            AND claim_owner IS NOT NULL
            AND claim_token IS NOT NULL
            AND claimed_at IS NOT NULL
            AND claim_expires_at IS NOT NULL
            AND completed_at IS NULL
            AND completion_reason IS NULL
        ) OR (
            state IN ('COMPLETED', 'DISCARDED')
            AND claim_owner IS NULL
            AND claim_token IS NULL
            AND claimed_at IS NULL
            AND claim_expires_at IS NULL
            AND completed_at IS NOT NULL
            AND completion_reason IS NOT NULL
        )
    )
);

CREATE INDEX stage_graph_instantiation_work_claim_idx
    ON stage_graph_instantiation_work (available_at, created_at, job_id)
    WHERE state IN ('PENDING', 'CLAIMED');

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM jobs AS job
        JOIN execution_graph_snapshots AS snapshot ON snapshot.job_id = job.id
        LEFT JOIN attempts AS attempt
          ON attempt.execution_graph_snapshot_id = snapshot.id
         AND attempt.execution_authority_kind = 'STAGE_GRAPH'
        LEFT JOIN attempt_coordinator_commands AS command
          ON command.job_id = job.id
         AND command.attempt_id = attempt.id
         AND command.command_kind = 'INSTANTIATE'
        LEFT JOIN stage_storage_reservations AS reservation
          ON reservation.attempt_id = attempt.id
        WHERE job.execution_authority_kind = 'STAGE_GRAPH'
          AND (
              attempt.id IS NULL
              OR command.command_id IS NULL
              OR reservation.id IS NULL
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_instantiation_backfill_incomplete',
            MESSAGE = 'Existing Stage graph authority cannot be backfilled exactly';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jobs AS job
        JOIN execution_graph_snapshots AS snapshot ON snapshot.job_id = job.id
        JOIN attempts AS attempt
          ON attempt.execution_graph_snapshot_id = snapshot.id
         AND attempt.execution_authority_kind = 'STAGE_GRAPH'
        JOIN attempt_coordinator_commands AS command
          ON command.job_id = job.id
         AND command.attempt_id = attempt.id
         AND command.command_kind = 'INSTANTIATE'
        GROUP BY job.id
        HAVING count(*) <> 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_instantiation_backfill_ambiguous',
            MESSAGE = 'Existing Stage graph authority has ambiguous instantiate history';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jobs AS job
        JOIN stage_cutover_revisions AS revision
          ON revision.id = job.stage_cutover_revision_id
         AND revision.execution_graph_revision_id = job.execution_graph_revision_id
         AND revision.execution_profile_revision_id = job.stage_execution_profile_revision_id
        JOIN execution_graph_snapshots AS snapshot ON snapshot.job_id = job.id
        JOIN attempts AS attempt
          ON attempt.execution_graph_snapshot_id = snapshot.id
         AND attempt.execution_authority_kind = 'STAGE_GRAPH'
        JOIN stage_storage_reservations AS reservation
          ON reservation.attempt_id = attempt.id
        WHERE job.execution_authority_kind = 'STAGE_GRAPH'
          AND reservation.reserved_bytes <> revision.reserved_storage_bytes
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_instantiation_backfill_storage_mismatch',
            MESSAGE = 'Existing Stage graph storage authority does not match its cutover revision';
    END IF;
END
$$;

INSERT INTO stage_graph_instantiation_work (
    job_id, organization_id, project_id, stage_cutover_revision_id,
    command_id, expected_job_version, expected_job_fence,
    execution_graph_snapshot_id, execution_graph_revision_id,
    execution_profile_revision_id, attempt_id, storage_reservation_id,
    reserved_storage_bytes, source, state, completed_at, completion_reason
)
SELECT
    job.id,
    job.organization_id,
    job.project_id,
    job.stage_cutover_revision_id,
    COALESCE(command.command_id, gen_random_uuid()),
    CASE WHEN attempt.id IS NULL THEN job.version
         ELSE GREATEST(1, job.version - 1) END,
    CASE WHEN attempt.id IS NULL THEN job.current_fence
         ELSE GREATEST(0, attempt.fence - 1) END,
    COALESCE(snapshot.id, gen_random_uuid()),
    job.execution_graph_revision_id,
    job.stage_execution_profile_revision_id,
    COALESCE(attempt.id, gen_random_uuid()),
    COALESCE(reservation.id, gen_random_uuid()),
    revision.reserved_storage_bytes,
    'MIGRATION_BACKFILL',
    CASE
        WHEN snapshot.id IS NOT NULL THEN 'COMPLETED'::stage_graph_instantiation_state
        WHEN job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
            THEN 'DISCARDED'::stage_graph_instantiation_state
        ELSE 'PENDING'::stage_graph_instantiation_state
    END,
    CASE
        WHEN snapshot.id IS NOT NULL
          OR job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
        THEN clock_timestamp()
        ELSE NULL
    END,
    CASE
        WHEN snapshot.id IS NOT NULL THEN 'MIGRATION_BACKFILL_CONFIRMED'
        WHEN job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
            THEN 'JOB_TERMINAL_BEFORE_INSTANTIATION'
        ELSE NULL
    END
FROM jobs AS job
JOIN stage_cutover_revisions AS revision
  ON revision.id = job.stage_cutover_revision_id
 AND revision.execution_graph_revision_id = job.execution_graph_revision_id
 AND revision.execution_profile_revision_id = job.stage_execution_profile_revision_id
LEFT JOIN execution_graph_snapshots AS snapshot ON snapshot.job_id = job.id
LEFT JOIN attempts AS attempt
  ON attempt.execution_graph_snapshot_id = snapshot.id
 AND attempt.execution_authority_kind = 'STAGE_GRAPH'
LEFT JOIN attempt_coordinator_commands AS command
  ON command.job_id = job.id
 AND command.attempt_id = attempt.id
 AND command.command_kind = 'INSTANTIATE'
LEFT JOIN stage_storage_reservations AS reservation ON reservation.attempt_id = attempt.id
WHERE job.execution_authority_kind = 'STAGE_GRAPH';

CREATE FUNCTION vela_guard_stage_graph_instantiation_work() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_instantiation_work_is_durable',
            MESSAGE = 'Stage graph instantiation work cannot be deleted';
    END IF;
    IF ROW(
        NEW.job_id,
        NEW.organization_id,
        NEW.project_id,
        NEW.stage_cutover_revision_id,
        NEW.command_id,
        NEW.expected_job_version,
        NEW.expected_job_fence,
        NEW.execution_graph_snapshot_id,
        NEW.execution_graph_revision_id,
        NEW.execution_profile_revision_id,
        NEW.attempt_id,
        NEW.storage_reservation_id,
        NEW.reserved_storage_bytes,
        NEW.source,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.job_id,
        OLD.organization_id,
        OLD.project_id,
        OLD.stage_cutover_revision_id,
        OLD.command_id,
        OLD.expected_job_version,
        OLD.expected_job_fence,
        OLD.execution_graph_snapshot_id,
        OLD.execution_graph_revision_id,
        OLD.execution_profile_revision_id,
        OLD.attempt_id,
        OLD.storage_reservation_id,
        OLD.reserved_storage_bytes,
        OLD.source,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_instantiation_authority_immutable',
            MESSAGE = 'Stage graph instantiation authority cannot change';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER stage_graph_instantiation_work_guard
BEFORE UPDATE OR DELETE ON stage_graph_instantiation_work
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_graph_instantiation_work();
CREATE TRIGGER stage_graph_instantiation_work_truncate_guard
BEFORE TRUNCATE ON stage_graph_instantiation_work
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_stage_graph_instantiation_work();

CREATE FUNCTION vela_enqueue_stage_graph_instantiation() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_reserved_storage_bytes bigint;
BEGIN
    IF NEW.execution_authority_kind <> 'STAGE_GRAPH' THEN
        RETURN NEW;
    END IF;
    SELECT revision.reserved_storage_bytes
    INTO STRICT v_reserved_storage_bytes
    FROM public.stage_cutover_revisions AS revision
    WHERE revision.id = NEW.stage_cutover_revision_id
      AND revision.execution_graph_revision_id = NEW.execution_graph_revision_id
      AND revision.execution_profile_revision_id = NEW.stage_execution_profile_revision_id;

    INSERT INTO public.stage_graph_instantiation_work (
        job_id, organization_id, project_id, stage_cutover_revision_id,
        command_id, expected_job_version, expected_job_fence,
        execution_graph_snapshot_id, execution_graph_revision_id,
        execution_profile_revision_id, attempt_id, storage_reservation_id,
        reserved_storage_bytes, source
    ) VALUES (
        NEW.id, NEW.organization_id, NEW.project_id, NEW.stage_cutover_revision_id,
        gen_random_uuid(), NEW.version, NEW.current_fence,
        gen_random_uuid(), NEW.execution_graph_revision_id,
        NEW.stage_execution_profile_revision_id, gen_random_uuid(), gen_random_uuid(),
        v_reserved_storage_bytes, 'ADMISSION_TRIGGER'
    );
    RETURN NEW;
END
$$;

CREATE TRIGGER jobs_enqueue_stage_graph_instantiation
AFTER INSERT ON jobs
FOR EACH ROW
WHEN (NEW.execution_authority_kind = 'STAGE_GRAPH')
EXECUTE FUNCTION vela_enqueue_stage_graph_instantiation();

CREATE FUNCTION vela_guard_stage_graph_instantiate_command() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.command_kind <> 'INSTANTIATE' THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM public.stage_graph_instantiation_work AS work
        JOIN public.jobs AS job
          ON job.id = work.job_id
         AND job.execution_authority_kind = 'STAGE_GRAPH'
         AND job.version - 1 = work.expected_job_version
         AND job.current_fence - 1 = work.expected_job_fence
        JOIN public.execution_graph_snapshots AS snapshot
          ON snapshot.id = work.execution_graph_snapshot_id
         AND snapshot.job_id = work.job_id
         AND snapshot.execution_graph_revision_id = work.execution_graph_revision_id
         AND snapshot.execution_profile_revision_id = work.execution_profile_revision_id
        JOIN public.attempts AS attempt
          ON attempt.id = work.attempt_id
         AND attempt.job_id = work.job_id
         AND attempt.execution_graph_snapshot_id = work.execution_graph_snapshot_id
         AND attempt.execution_authority_kind = 'STAGE_GRAPH'
         AND attempt.fence - 1 = work.expected_job_fence
        JOIN public.stage_storage_reservations AS reservation
          ON reservation.id = work.storage_reservation_id
         AND reservation.job_id = work.job_id
         AND reservation.attempt_id = work.attempt_id
         AND reservation.reserved_bytes = work.reserved_storage_bytes
        WHERE work.job_id = NEW.job_id
          AND work.command_id = NEW.command_id
          AND work.attempt_id = NEW.attempt_id
          AND work.state = 'CLAIMED'
          AND work.claim_expires_at > clock_timestamp()
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_instantiate_requires_exact_claim',
            MESSAGE = 'Stage graph instantiation does not match a live durable claim';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER attempt_coordinator_commands_stage_graph_instantiate_claim
BEFORE INSERT ON attempt_coordinator_commands
FOR EACH ROW
WHEN (NEW.command_kind = 'INSTANTIATE')
EXECUTE FUNCTION vela_guard_stage_graph_instantiate_command();

CREATE FUNCTION vela_claim_stage_graph_instantiations(
    p_instance_id text,
    p_claim_token uuid,
    p_claim_ttl_seconds integer,
    p_batch_size integer
) RETURNS TABLE (
    job_id uuid,
    command_id uuid,
    expected_job_version bigint,
    expected_job_fence bigint,
    execution_graph_snapshot_id uuid,
    execution_graph_revision_id uuid,
    execution_profile_revision_id uuid,
    attempt_id uuid,
    storage_reservation_id uuid,
    reserved_storage_bytes bigint,
    reclaimed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_instance_id IS NULL OR length(p_instance_id) NOT BETWEEN 1 AND 200
       OR btrim(p_instance_id) <> p_instance_id
       OR p_instance_id ~ '[[:cntrl:]]'
       OR p_claim_token IS NULL
       OR p_claim_ttl_seconds NOT BETWEEN 1 AND 300
       OR p_batch_size NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_instantiation_claim_invalid',
            MESSAGE = 'Stage graph instantiation claim is invalid';
    END IF;

    RETURN QUERY
    WITH candidates AS MATERIALIZED (
        SELECT
            work.job_id,
            work.state = 'CLAIMED' AS reclaimed
        FROM public.stage_graph_instantiation_work AS work
        JOIN public.jobs AS job ON job.id = work.job_id
        WHERE work.available_at <= clock_timestamp()
          AND (
              work.state = 'PENDING'
              OR (
                  work.state = 'CLAIMED'
                  AND work.claim_expires_at <= clock_timestamp()
              )
          )
          AND (
              job.state IN ('QUEUED', 'RETRY_WAIT')
              OR EXISTS (
                  SELECT 1
                  FROM public.attempt_coordinator_commands AS command
                  WHERE command.command_id = work.command_id
                    AND command.command_kind = 'INSTANTIATE'
              )
          )
        ORDER BY work.available_at, work.created_at, work.job_id
        LIMIT p_batch_size
        FOR UPDATE OF work SKIP LOCKED
    ), claimed AS (
        UPDATE public.stage_graph_instantiation_work AS work
        SET state = 'CLAIMED',
            claim_owner = p_instance_id,
            claim_token = p_claim_token,
            claimed_at = clock_timestamp(),
            claim_expires_at = clock_timestamp()
                + make_interval(secs => p_claim_ttl_seconds),
            claim_count = work.claim_count + 1,
            last_error = NULL,
            updated_at = clock_timestamp()
        FROM candidates AS candidate
        WHERE work.job_id = candidate.job_id
        RETURNING work.*
    )
    SELECT
        claimed.job_id,
        claimed.command_id,
        claimed.expected_job_version,
        claimed.expected_job_fence,
        claimed.execution_graph_snapshot_id,
        claimed.execution_graph_revision_id,
        claimed.execution_profile_revision_id,
        claimed.attempt_id,
        claimed.storage_reservation_id,
        claimed.reserved_storage_bytes,
        candidate.reclaimed
    FROM claimed
    JOIN candidates AS candidate ON candidate.job_id = claimed.job_id
    ORDER BY claimed.created_at, claimed.job_id;
END
$$;

CREATE FUNCTION vela_complete_stage_graph_instantiation(
    p_job_id uuid,
    p_claim_token uuid,
    p_command_id uuid,
    p_execution_graph_snapshot_id uuid,
    p_attempt_id uuid
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_rows bigint;
BEGIN
    IF p_job_id IS NULL OR p_claim_token IS NULL OR p_command_id IS NULL
       OR p_execution_graph_snapshot_id IS NULL OR p_attempt_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_instantiation_completion_invalid',
            MESSAGE = 'Stage graph instantiation completion is invalid';
    END IF;
    UPDATE public.stage_graph_instantiation_work AS work
    SET state = 'COMPLETED',
        claim_owner = NULL,
        claim_token = NULL,
        claimed_at = NULL,
        claim_expires_at = NULL,
        completed_at = clock_timestamp(),
        completion_reason = 'INSTANTIATION_CONFIRMED',
        last_error = NULL,
        updated_at = clock_timestamp()
    WHERE work.job_id = p_job_id
      AND work.state = 'CLAIMED'
      AND work.claim_token = p_claim_token
      AND work.claim_expires_at > clock_timestamp()
      AND work.command_id = p_command_id
      AND work.execution_graph_snapshot_id = p_execution_graph_snapshot_id
      AND work.attempt_id = p_attempt_id
      AND EXISTS (
          SELECT 1
          FROM public.attempt_coordinator_commands AS command
          JOIN public.execution_graph_snapshots AS snapshot
            ON snapshot.id = work.execution_graph_snapshot_id
           AND snapshot.job_id = work.job_id
           AND snapshot.execution_graph_revision_id = work.execution_graph_revision_id
           AND snapshot.execution_profile_revision_id = work.execution_profile_revision_id
          JOIN public.attempts AS attempt
            ON attempt.id = work.attempt_id
           AND attempt.job_id = work.job_id
           AND attempt.execution_graph_snapshot_id = snapshot.id
           AND attempt.execution_authority_kind = 'STAGE_GRAPH'
          JOIN public.stage_storage_reservations AS reservation
            ON reservation.id = work.storage_reservation_id
           AND reservation.job_id = work.job_id
           AND reservation.attempt_id = attempt.id
           AND reservation.reserved_bytes = work.reserved_storage_bytes
          WHERE command.command_id = work.command_id
            AND command.command_kind = 'INSTANTIATE'
            AND command.job_id = work.job_id
            AND command.attempt_id = work.attempt_id
      );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END
$$;

CREATE FUNCTION vela_release_stage_graph_instantiation(
    p_job_id uuid,
    p_claim_token uuid,
    p_retry_after_seconds integer,
    p_last_error text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_rows bigint;
BEGIN
    IF p_job_id IS NULL OR p_claim_token IS NULL
       OR p_retry_after_seconds NOT BETWEEN 0 AND 86400
       OR p_last_error IS NULL OR length(p_last_error) NOT BETWEEN 1 AND 2000
       OR p_last_error ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_instantiation_release_invalid',
            MESSAGE = 'Stage graph instantiation release is invalid';
    END IF;
    UPDATE public.stage_graph_instantiation_work AS work
    SET state = 'PENDING',
        available_at = clock_timestamp()
            + make_interval(secs => p_retry_after_seconds),
        claim_owner = NULL,
        claim_token = NULL,
        claimed_at = NULL,
        claim_expires_at = NULL,
        last_error = p_last_error,
        updated_at = clock_timestamp()
    WHERE work.job_id = p_job_id
      AND work.state = 'CLAIMED'
      AND work.claim_token = p_claim_token;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END
$$;

CREATE FUNCTION vela_reconcile_stage_graph_instantiations(p_limit integer)
RETURNS TABLE (
    job_id uuid,
    state stage_graph_instantiation_state,
    reason text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_instantiation_reconcile_limit_invalid',
            MESSAGE = 'Stage graph instantiation reconcile limit is invalid';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.stage_graph_instantiation_work AS work
        JOIN public.attempt_coordinator_commands AS command
          ON command.command_id = work.command_id
         AND command.command_kind = 'INSTANTIATE'
        WHERE work.state IN ('PENDING', 'CLAIMED')
          AND NOT EXISTS (
              SELECT 1
              FROM public.execution_graph_snapshots AS snapshot
              JOIN public.attempts AS attempt
                ON attempt.id = work.attempt_id
               AND attempt.job_id = work.job_id
               AND attempt.execution_graph_snapshot_id = snapshot.id
               AND attempt.execution_authority_kind = 'STAGE_GRAPH'
              JOIN public.stage_storage_reservations AS reservation
                ON reservation.id = work.storage_reservation_id
               AND reservation.job_id = work.job_id
               AND reservation.attempt_id = attempt.id
               AND reservation.reserved_bytes = work.reserved_storage_bytes
              WHERE snapshot.id = work.execution_graph_snapshot_id
                AND snapshot.job_id = work.job_id
                AND snapshot.execution_graph_revision_id = work.execution_graph_revision_id
                AND snapshot.execution_profile_revision_id = work.execution_profile_revision_id
                AND command.job_id = work.job_id
                AND command.attempt_id = work.attempt_id
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'stage_graph_instantiation_committed_authority_incomplete',
            MESSAGE = 'Committed Stage graph instantiation authority is incomplete';
    END IF;

    RETURN QUERY
    WITH candidates AS MATERIALIZED (
        SELECT
            work.job_id,
            CASE
                WHEN command.command_id IS NOT NULL
                    THEN 'COMPLETED'::public.stage_graph_instantiation_state
                ELSE 'DISCARDED'::public.stage_graph_instantiation_state
            END AS next_state,
            CASE
                WHEN command.command_id IS NOT NULL
                    THEN 'COMMITTED_INSTANTIATION_RECONCILED'::text
                ELSE 'JOB_TERMINAL_BEFORE_INSTANTIATION'::text
            END AS next_reason
        FROM public.stage_graph_instantiation_work AS work
        JOIN public.jobs AS job ON job.id = work.job_id
        LEFT JOIN public.attempt_coordinator_commands AS command
          ON command.command_id = work.command_id
         AND command.command_kind = 'INSTANTIATE'
         AND command.job_id = work.job_id
         AND command.attempt_id = work.attempt_id
        WHERE work.state IN ('PENDING', 'CLAIMED')
          AND (
              command.command_id IS NOT NULL
              OR job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
          )
        ORDER BY work.updated_at, work.job_id
        LIMIT p_limit
        FOR UPDATE OF work SKIP LOCKED
    ), reconciled AS (
        UPDATE public.stage_graph_instantiation_work AS work
        SET state = candidate.next_state,
            claim_owner = NULL,
            claim_token = NULL,
            claimed_at = NULL,
            claim_expires_at = NULL,
            completed_at = clock_timestamp(),
            completion_reason = candidate.next_reason,
            last_error = NULL,
            updated_at = clock_timestamp()
        FROM candidates AS candidate
        WHERE work.job_id = candidate.job_id
        RETURNING work.job_id, work.state, work.completion_reason
    )
    SELECT reconciled.job_id, reconciled.state, reconciled.completion_reason
    FROM reconciled
    ORDER BY reconciled.job_id;
END
$$;

ALTER TABLE stage_graph_instantiation_work OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_guard_stage_graph_instantiation_work()
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_enqueue_stage_graph_instantiation()
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_guard_stage_graph_instantiate_command()
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_claim_stage_graph_instantiations(text, uuid, integer, integer)
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_complete_stage_graph_instantiation(uuid, uuid, uuid, uuid, uuid)
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_release_stage_graph_instantiation(uuid, uuid, integer, text)
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_reconcile_stage_graph_instantiations(integer)
    OWNER TO vela_attempt_coordinator_owner;

ALTER TABLE stage_graph_instantiation_work ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_graph_instantiation_work FORCE ROW LEVEL SECURITY;

REVOKE ALL ON stage_graph_instantiation_work FROM PUBLIC;
REVOKE ALL ON FUNCTION
    vela_claim_stage_graph_instantiations(text, uuid, integer, integer),
    vela_complete_stage_graph_instantiation(uuid, uuid, uuid, uuid, uuid),
    vela_release_stage_graph_instantiation(uuid, uuid, integer, text),
    vela_reconcile_stage_graph_instantiations(integer)
FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON stage_graph_instantiation_work
TO vela_attempt_coordinator_owner;
GRANT SELECT ON stage_cutover_revisions TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION
    vela_claim_stage_graph_instantiations(text, uuid, integer, integer),
    vela_complete_stage_graph_instantiation(uuid, uuid, uuid, uuid, uuid),
    vela_release_stage_graph_instantiation(uuid, uuid, integer, text),
    vela_reconcile_stage_graph_instantiations(integer)
TO vela_attempt_coordinator;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_graph_instantiation_work) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_instantiation_dispatch_rollback_is_unsafe',
            MESSAGE = 'Stage graph instantiation work must be empty before rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION
    vela_claim_stage_graph_instantiations(text, uuid, integer, integer),
    vela_complete_stage_graph_instantiation(uuid, uuid, uuid, uuid, uuid),
    vela_release_stage_graph_instantiation(uuid, uuid, integer, text),
    vela_reconcile_stage_graph_instantiations(integer)
FROM vela_attempt_coordinator;
REVOKE SELECT ON stage_cutover_revisions FROM vela_attempt_coordinator_owner;
REVOKE SELECT, INSERT, UPDATE, DELETE ON stage_graph_instantiation_work
FROM vela_attempt_coordinator_owner;
DROP TRIGGER attempt_coordinator_commands_stage_graph_instantiate_claim
    ON attempt_coordinator_commands;
DROP FUNCTION vela_guard_stage_graph_instantiate_command();
DROP TRIGGER jobs_enqueue_stage_graph_instantiation ON jobs;
DROP FUNCTION vela_reconcile_stage_graph_instantiations(integer);
DROP FUNCTION vela_release_stage_graph_instantiation(uuid, uuid, integer, text);
DROP FUNCTION vela_complete_stage_graph_instantiation(uuid, uuid, uuid, uuid, uuid);
DROP FUNCTION vela_claim_stage_graph_instantiations(text, uuid, integer, integer);
DROP FUNCTION vela_enqueue_stage_graph_instantiation();
DROP TRIGGER stage_graph_instantiation_work_truncate_guard
    ON stage_graph_instantiation_work;
DROP TRIGGER stage_graph_instantiation_work_guard ON stage_graph_instantiation_work;
DROP FUNCTION vela_guard_stage_graph_instantiation_work();
DROP TABLE stage_graph_instantiation_work;
DROP TYPE stage_graph_instantiation_state;
-- +goose StatementEnd
