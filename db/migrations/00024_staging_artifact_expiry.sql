-- +goose Up
-- +goose StatementBegin
ALTER TYPE content_deletion_source
    ADD VALUE 'RETENTION_INCOMPLETE_ARTIFACT';

GRANT SELECT (version, retention_incomplete_content_hours)
    ON jobs TO vela_retention_owner;

CREATE FUNCTION vela_enqueue_incomplete_artifact_deletions(
    p_limit integer
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job record;
    v_request_id uuid;
    v_requests_created integer := 0;
BEGIN
    IF p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'invalid incomplete Artifact enqueue limit'
            USING ERRCODE = '22023';
    END IF;

    FOR v_job IN
        SELECT
            job.id,
            job.organization_id,
            job.project_id,
            terminal_event.occurred_at AS terminal_at,
            job.retention_incomplete_content_hours
        FROM public.jobs AS job
        JOIN public.outbox_events AS terminal_event
          ON terminal_event.organization_id = job.organization_id
         AND terminal_event.project_id = job.project_id
         AND terminal_event.aggregate_type = 'Job'
         AND terminal_event.aggregate_id = job.id
         AND terminal_event.aggregate_version = job.version
         AND terminal_event.event_type = CASE job.state
                WHEN 'SUCCEEDED' THEN 'job.succeeded'
                WHEN 'FAILED' THEN 'job.failed'
                WHEN 'CANCELED' THEN 'job.canceled'
             END
        WHERE job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
          AND job.retention_incomplete_content_hours = 24
          AND EXISTS (
              SELECT 1
              FROM public.artifacts AS artifact
              WHERE artifact.organization_id = job.organization_id
                AND artifact.project_id = job.project_id
                AND artifact.job_id = job.id
                AND artifact.state IN ('STAGING', 'UPLOADED', 'VERIFIED')
          )
          AND NOT EXISTS (
              SELECT 1
              FROM public.content_deletion_requests AS existing
              WHERE existing.organization_id = job.organization_id
                AND existing.project_id = job.project_id
                AND existing.job_id = job.id
                AND existing.source = 'RETENTION_INCOMPLETE_ARTIFACT'
          )
        ORDER BY terminal_event.occurred_at, job.id
        FOR UPDATE OF job SKIP LOCKED
        LIMIT p_limit
    LOOP
        v_request_id := gen_random_uuid();
        INSERT INTO public.content_deletion_requests (
            id,
            organization_id,
            project_id,
            job_id,
            source,
            requested_at,
            deadline_at
        ) VALUES (
            v_request_id,
            v_job.organization_id,
            v_job.project_id,
            v_job.id,
            'RETENTION_INCOMPLETE_ARTIFACT',
            v_job.terminal_at,
            v_job.terminal_at + make_interval(
                hours => v_job.retention_incomplete_content_hours
            )
        );

        INSERT INTO public.content_deletion_targets (
            id,
            organization_id,
            project_id,
            job_id,
            request_id,
            action,
            artifact_id,
            object_key,
            object_version_id
        )
        SELECT
            gen_random_uuid(),
            artifact.organization_id,
            artifact.project_id,
            artifact.job_id,
            v_request_id,
            CASE
                WHEN artifact.object_version_id IS NULL
                THEN 'OBJECT_DISCOVERY'::public.content_deletion_target_action
                ELSE 'OBJECT_VERSION'::public.content_deletion_target_action
            END,
            artifact.id,
            artifact.object_key,
            artifact.object_version_id
        FROM public.artifacts AS artifact
        WHERE artifact.organization_id = v_job.organization_id
          AND artifact.project_id = v_job.project_id
          AND artifact.job_id = v_job.id
          AND artifact.state IN ('STAGING', 'UPLOADED', 'VERIFIED');

        INSERT INTO public.content_deletion_targets (
            id,
            organization_id,
            project_id,
            job_id,
            request_id,
            action,
            object_key
        ) VALUES (
            gen_random_uuid(),
            v_job.organization_id,
            v_job.project_id,
            v_job.id,
            v_request_id,
            'MULTIPART_PREFIX',
            format(
                'artifacts/%s/%s/%s/',
                v_job.organization_id,
                v_job.project_id,
                v_job.id
            )
        );
        v_requests_created := v_requests_created + 1;
    END LOOP;

    RETURN v_requests_created;
END
$$;
REVOKE ALL ON FUNCTION vela_enqueue_incomplete_artifact_deletions(integer) FROM PUBLIC;
ALTER FUNCTION vela_enqueue_incomplete_artifact_deletions(integer)
    OWNER TO vela_retention_owner;

ALTER FUNCTION vela_enqueue_expired_content_deletions(integer)
    RENAME TO vela_enqueue_expired_content_deletions_v23;
REVOKE EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions_v23(integer)
    FROM vela_retention;

CREATE FUNCTION vela_enqueue_expired_content_deletions(
    p_limit integer
) RETURNS TABLE (
    request_content_completed integer,
    artifact_requests_created integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_request_content_completed integer;
    v_artifact_requests_created integer;
    v_incomplete_artifact_requests integer := 0;
    v_protocol_version text;
BEGIN
    SELECT
        expired.request_content_completed,
        expired.artifact_requests_created
    INTO STRICT
        v_request_content_completed,
        v_artifact_requests_created
    FROM public.vela_enqueue_expired_content_deletions_v23(p_limit) AS expired;

    v_protocol_version := current_setting(
        'vela.retention_protocol_version',
        true
    );
    IF v_protocol_version = '24' THEN
        SELECT public.vela_enqueue_incomplete_artifact_deletions(p_limit)
        INTO STRICT v_incomplete_artifact_requests;
    ELSIF v_protocol_version IS NOT NULL THEN
        RAISE EXCEPTION 'unsupported retention protocol version'
            USING ERRCODE = '22023';
    END IF;

    RETURN QUERY SELECT
        v_request_content_completed,
        v_artifact_requests_created + v_incomplete_artifact_requests;
END
$$;
REVOKE ALL ON FUNCTION vela_enqueue_expired_content_deletions(integer) FROM PUBLIC;
ALTER FUNCTION vela_enqueue_expired_content_deletions(integer)
    OWNER TO vela_retention_owner;
GRANT EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions(integer)
    TO vela_retention;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE content_deletion_requests IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM content_deletion_requests
        WHERE source = 'RETENTION_INCOMPLETE_ARTIFACT'
    ) THEN
        RAISE EXCEPTION 'cannot remove incomplete Artifact cleanup with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'incomplete_artifact_cleanup_requires_empty_evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions(integer)
    FROM vela_retention;
DROP FUNCTION vela_enqueue_expired_content_deletions(integer);
ALTER FUNCTION vela_enqueue_expired_content_deletions_v23(integer)
    RENAME TO vela_enqueue_expired_content_deletions;
GRANT EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions(integer)
    TO vela_retention;
DROP FUNCTION vela_enqueue_incomplete_artifact_deletions(integer);
REVOKE SELECT (version, retention_incomplete_content_hours)
    ON jobs FROM vela_retention_owner;

DROP INDEX content_deletion_requests_customer_idempotency_idx;
DO $$
DECLARE
    v_source_constraint text;
BEGIN
    SELECT constraint_entry.conname INTO STRICT v_source_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.content_deletion_requests'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%source%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%CUSTOMER%';
    EXECUTE format(
        'ALTER TABLE public.content_deletion_requests DROP CONSTRAINT %I',
        v_source_constraint
    );
END
$$;
ALTER TABLE content_deletion_requests
    ALTER COLUMN source TYPE text USING source::text;
DROP TYPE content_deletion_source;
CREATE TYPE content_deletion_source AS ENUM (
    'CUSTOMER', 'RETENTION_REQUEST_CONTENT', 'RETENTION_ARTIFACT'
);
ALTER TABLE content_deletion_requests
    ALTER COLUMN source TYPE content_deletion_source
    USING source::content_deletion_source;
ALTER TABLE content_deletion_requests
    ADD CONSTRAINT content_deletion_requests_source_actor_check CHECK (
        (
            source = 'CUSTOMER'
            AND length(idempotency_key) BETWEEN 1 AND 200
            AND octet_length(request_hash) = 32
            AND actor_kind IS NOT NULL
            AND actor_principal_id IS NOT NULL
            AND actor_credential_id IS NOT NULL
        )
        OR (
            source <> 'CUSTOMER'
            AND idempotency_key IS NULL
            AND request_hash IS NULL
            AND actor_kind IS NULL
            AND actor_principal_id IS NULL
            AND actor_credential_id IS NULL
        )
    );
CREATE UNIQUE INDEX content_deletion_requests_customer_idempotency_idx
    ON content_deletion_requests (organization_id, project_id, idempotency_key)
    WHERE source = 'CUSTOMER';
-- +goose StatementEnd
