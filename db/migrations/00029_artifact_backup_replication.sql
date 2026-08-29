-- +goose Up
-- +goose StatementBegin
CREATE TYPE artifact_backup_replication_state AS ENUM (
    'PENDING', 'COMPLETED', 'CANCELED'
);

CREATE TABLE artifact_backup_replications (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    artifact_id uuid NOT NULL UNIQUE,
    source_object_key text NOT NULL CHECK (
        length(source_object_key) BETWEEN 1 AND 1000
        AND source_object_key !~ '(^|/)\.\.(/|$)'
    ),
    source_object_version_id text NOT NULL CHECK (
        length(source_object_version_id) BETWEEN 1 AND 1000
    ),
    source_size_bytes bigint NOT NULL CHECK (source_size_bytes > 0),
    source_sha256 bytea NOT NULL CHECK (octet_length(source_sha256) = 32),
    source_content_type text NOT NULL CHECK (
        length(source_content_type) BETWEEN 1 AND 200
    ),
    state artifact_backup_replication_state NOT NULL DEFAULT 'PENDING',
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_retry_at timestamptz,
    last_error_code text CHECK (
        last_error_code IS NULL OR last_error_code IN (
            'SOURCE_MISSING', 'SOURCE_IDENTITY_MISMATCH',
            'BACKUP_CONFLICT', 'STORAGE_OPERATION_FAILED'
        )
    ),
    claim_id uuid,
    claim_owner_id text CHECK (
        claim_owner_id IS NULL OR length(claim_owner_id) BETWEEN 1 AND 500
    ),
    claim_expires_at timestamptz,
    last_attempt_owner_id text CHECK (
        last_attempt_owner_id IS NULL
        OR length(last_attempt_owner_id) BETWEEN 1 AND 500
    ),
    backup_object_version_id text CHECK (
        backup_object_version_id IS NULL
        OR length(backup_object_version_id) BETWEEN 1 AND 1000
    ),
    backup_size_bytes bigint CHECK (
        backup_size_bytes IS NULL OR backup_size_bytes > 0
    ),
    backup_sha256 bytea CHECK (
        backup_sha256 IS NULL OR octet_length(backup_sha256) = 32
    ),
    backup_content_type text CHECK (
        backup_content_type IS NULL
        OR length(backup_content_type) BETWEEN 1 AND 200
    ),
    completed_at timestamptz,
    canceled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, project_id, artifact_id)
        REFERENCES artifacts (organization_id, project_id, id),
    CHECK (
        (claim_id IS NULL AND claim_owner_id IS NULL AND claim_expires_at IS NULL)
        OR (claim_id IS NOT NULL AND claim_owner_id IS NOT NULL AND claim_expires_at IS NOT NULL)
    ),
    CHECK (
        (
            state = 'PENDING'
            AND backup_object_version_id IS NULL
            AND backup_size_bytes IS NULL
            AND backup_sha256 IS NULL
            AND backup_content_type IS NULL
            AND completed_at IS NULL
            AND canceled_at IS NULL
        ) OR (
            state = 'COMPLETED'
            AND backup_object_version_id IS NOT NULL
            AND backup_size_bytes = source_size_bytes
            AND backup_sha256 = source_sha256
            AND backup_content_type = source_content_type
            AND completed_at IS NOT NULL
            AND canceled_at IS NULL
            AND next_retry_at IS NULL
            AND last_error_code IS NULL
            AND claim_id IS NULL
            AND claim_owner_id IS NULL
        ) OR (
            state = 'CANCELED'
            AND backup_object_version_id IS NULL
            AND backup_size_bytes IS NULL
            AND backup_sha256 IS NULL
            AND backup_content_type IS NULL
            AND completed_at IS NULL
            AND canceled_at IS NOT NULL
            AND next_retry_at IS NULL
            AND last_error_code IS NULL
            AND claim_id IS NULL
            AND claim_owner_id IS NULL
        )
    )
);
CREATE INDEX artifact_backup_replications_pending_idx
    ON artifact_backup_replications (coalesce(next_retry_at, created_at), created_at, id)
    WHERE state = 'PENDING';
ALTER TABLE artifact_backup_replications ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_backup_replications FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_backup_replications OWNER TO vela_artifact_replication_owner;
GRANT SELECT ON artifacts, content_deletion_targets
    TO vela_artifact_replication_owner;

CREATE FUNCTION vela_guard_artifact_backup_replication() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'DELETE' OR OLD.state IN ('COMPLETED', 'CANCELED') THEN
        RAISE EXCEPTION 'Artifact backup replication evidence is immutable';
    END IF;
    IF NEW.id <> OLD.id
        OR NEW.organization_id <> OLD.organization_id
        OR NEW.project_id <> OLD.project_id
        OR NEW.job_id <> OLD.job_id
        OR NEW.artifact_id <> OLD.artifact_id
        OR NEW.source_object_key <> OLD.source_object_key
        OR NEW.source_object_version_id <> OLD.source_object_version_id
        OR NEW.source_size_bytes <> OLD.source_size_bytes
        OR NEW.source_sha256 <> OLD.source_sha256
        OR NEW.source_content_type <> OLD.source_content_type
        OR NEW.created_at <> OLD.created_at
    THEN
        RAISE EXCEPTION 'Artifact backup replication source identity is immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_artifact_backup_replication() FROM PUBLIC;
ALTER FUNCTION vela_guard_artifact_backup_replication()
    OWNER TO vela_artifact_replication_owner;
CREATE TRIGGER artifact_backup_replications_guard
BEFORE UPDATE OR DELETE ON artifact_backup_replications
FOR EACH ROW EXECUTE FUNCTION vela_guard_artifact_backup_replication();

CREATE FUNCTION vela_enqueue_artifact_backup_replication() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_canceled boolean;
BEGIN
    IF NEW.state <> 'COMMITTED' OR OLD.state = 'COMMITTED' THEN
        RETURN NEW;
    END IF;
    SELECT EXISTS (
        SELECT 1
        FROM public.content_deletion_targets AS target
        WHERE target.artifact_id = NEW.id
    ) INTO v_canceled;
    INSERT INTO public.artifact_backup_replications (
        id, organization_id, project_id, job_id, artifact_id,
        source_object_key, source_object_version_id, source_size_bytes,
        source_sha256, source_content_type, state, canceled_at
    ) VALUES (
        gen_random_uuid(), NEW.organization_id, NEW.project_id, NEW.job_id, NEW.id,
        NEW.object_key, NEW.object_version_id, NEW.size_bytes,
        NEW.sha256, NEW.content_type,
        CASE WHEN v_canceled THEN 'CANCELED'::artifact_backup_replication_state
             ELSE 'PENDING'::artifact_backup_replication_state END,
        CASE WHEN v_canceled THEN clock_timestamp() ELSE NULL END
    ) ON CONFLICT (artifact_id) DO NOTHING;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enqueue_artifact_backup_replication() FROM PUBLIC;
ALTER FUNCTION vela_enqueue_artifact_backup_replication()
    OWNER TO vela_artifact_replication_owner;
CREATE TRIGGER artifacts_enqueue_backup_replication
AFTER UPDATE OF state ON artifacts
FOR EACH ROW EXECUTE FUNCTION vela_enqueue_artifact_backup_replication();

INSERT INTO artifact_backup_replications (
    id, organization_id, project_id, job_id, artifact_id,
    source_object_key, source_object_version_id, source_size_bytes,
    source_sha256, source_content_type, state, canceled_at
)
SELECT
    gen_random_uuid(), artifact.organization_id, artifact.project_id,
    artifact.job_id, artifact.id, artifact.object_key,
    artifact.object_version_id, artifact.size_bytes, artifact.sha256,
    artifact.content_type,
    CASE WHEN EXISTS (
        SELECT 1
        FROM content_deletion_targets AS target
        WHERE target.artifact_id = artifact.id
    ) THEN 'CANCELED'::artifact_backup_replication_state
    ELSE 'PENDING'::artifact_backup_replication_state END,
    CASE WHEN EXISTS (
        SELECT 1
        FROM content_deletion_targets AS target
        WHERE target.artifact_id = artifact.id
    ) THEN clock_timestamp() ELSE NULL END
FROM artifacts AS artifact
WHERE artifact.state = 'COMMITTED'
ON CONFLICT (artifact_id) DO NOTHING;

CREATE FUNCTION vela_cancel_artifact_backup_replication() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.artifact_id IS NULL THEN
        RETURN NEW;
    END IF;
    UPDATE public.artifact_backup_replications AS replication
    SET state = 'CANCELED', next_retry_at = NULL, last_error_code = NULL,
        claim_id = NULL, claim_owner_id = NULL,
        claim_expires_at = NULL,
        canceled_at = clock_timestamp(), updated_at = clock_timestamp()
    WHERE replication.artifact_id = NEW.artifact_id
      AND replication.state = 'PENDING';
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_cancel_artifact_backup_replication() FROM PUBLIC;
ALTER FUNCTION vela_cancel_artifact_backup_replication()
    OWNER TO vela_artifact_replication_owner;
CREATE TRIGGER content_deletion_targets_cancel_backup_replication
AFTER INSERT ON content_deletion_targets
FOR EACH ROW EXECUTE FUNCTION vela_cancel_artifact_backup_replication();

CREATE FUNCTION vela_claim_artifact_backup_replication(
    p_claim_owner_id text,
    p_claim_id uuid,
    p_claim_seconds integer
) RETURNS TABLE (
    replication_id uuid,
    source_object_key text,
    source_object_version_id text,
    source_size_bytes bigint,
    source_sha256 bytea,
    source_content_type text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_replication_id uuid;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_claim_id IS NULL OR p_claim_owner_id IS NULL
        OR length(p_claim_owner_id) NOT BETWEEN 1 AND 500
        OR btrim(p_claim_owner_id) <> p_claim_owner_id
        OR p_claim_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid Artifact backup replication claim'
            USING ERRCODE = '22023';
    END IF;
    SELECT replication.id INTO v_replication_id
    FROM public.artifact_backup_replications AS replication
    JOIN public.artifacts AS artifact
      ON artifact.organization_id = replication.organization_id
     AND artifact.project_id = replication.project_id
     AND artifact.id = replication.artifact_id
    WHERE replication.state = 'PENDING'
      AND (replication.next_retry_at IS NULL OR replication.next_retry_at <= v_now)
      AND (replication.claim_id IS NULL OR replication.claim_expires_at <= v_now)
      AND artifact.state = 'COMMITTED'
      AND NOT EXISTS (
          SELECT 1
          FROM public.content_deletion_targets AS target
          WHERE target.artifact_id = replication.artifact_id
      )
    ORDER BY coalesce(replication.next_retry_at, replication.created_at),
        replication.created_at, replication.id
    FOR UPDATE OF replication SKIP LOCKED
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    UPDATE public.artifact_backup_replications AS replication
    SET attempt_count = replication.attempt_count + 1,
        claim_id = p_claim_id, claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_now + make_interval(secs => p_claim_seconds),
        last_attempt_owner_id = p_claim_owner_id,
        next_retry_at = NULL, last_error_code = NULL, updated_at = v_now
    WHERE replication.id = v_replication_id;
    RETURN QUERY
    SELECT replication.id, replication.source_object_key,
        replication.source_object_version_id, replication.source_size_bytes,
        replication.source_sha256, replication.source_content_type
    FROM public.artifact_backup_replications AS replication
    WHERE replication.id = v_replication_id;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_artifact_backup_replication(text, uuid, integer)
    FROM PUBLIC;
ALTER FUNCTION vela_claim_artifact_backup_replication(text, uuid, integer)
    OWNER TO vela_artifact_replication_owner;

CREATE FUNCTION vela_complete_artifact_backup_replication(
    p_replication_id uuid,
    p_claim_id uuid,
    p_backup_object_version_id text,
    p_backup_size_bytes bigint,
    p_backup_sha256 bytea,
    p_backup_content_type text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_row_count integer;
BEGIN
    IF p_replication_id IS NULL OR p_claim_id IS NULL
        OR length(p_backup_object_version_id) NOT BETWEEN 1 AND 1000
        OR p_backup_size_bytes <= 0
        OR octet_length(p_backup_sha256) IS DISTINCT FROM 32
        OR length(p_backup_content_type) NOT BETWEEN 1 AND 200
    THEN
        RAISE EXCEPTION 'invalid Artifact backup replication result'
            USING ERRCODE = '22023';
    END IF;
    UPDATE public.artifact_backup_replications AS replication
    SET state = 'COMPLETED',
        backup_object_version_id = p_backup_object_version_id,
        backup_size_bytes = p_backup_size_bytes,
        backup_sha256 = p_backup_sha256,
        backup_content_type = p_backup_content_type,
        completed_at = clock_timestamp(), claim_id = NULL, claim_owner_id = NULL,
        claim_expires_at = NULL,
        next_retry_at = NULL, last_error_code = NULL, updated_at = clock_timestamp()
    FROM public.artifacts AS artifact
    WHERE replication.id = p_replication_id
      AND replication.claim_id = p_claim_id
      AND replication.claim_expires_at > clock_timestamp()
      AND replication.state = 'PENDING'
      AND artifact.organization_id = replication.organization_id
      AND artifact.project_id = replication.project_id
      AND artifact.id = replication.artifact_id
      AND artifact.state = 'COMMITTED'
      AND p_backup_size_bytes = replication.source_size_bytes
      AND p_backup_sha256 = replication.source_sha256
      AND p_backup_content_type = replication.source_content_type
      AND NOT EXISTS (
          SELECT 1
          FROM public.content_deletion_targets AS target
          WHERE target.artifact_id = replication.artifact_id
      );
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RETURN v_row_count = 1;
END
$$;
REVOKE ALL ON FUNCTION vela_complete_artifact_backup_replication(
    uuid, uuid, text, bigint, bytea, text
) FROM PUBLIC;
ALTER FUNCTION vela_complete_artifact_backup_replication(
    uuid, uuid, text, bigint, bytea, text
) OWNER TO vela_artifact_replication_owner;

CREATE FUNCTION vela_retry_artifact_backup_replication(
    p_replication_id uuid,
    p_claim_id uuid,
    p_retry_seconds integer,
    p_error_code text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_row_count integer;
BEGIN
    IF p_replication_id IS NULL OR p_claim_id IS NULL
        OR p_retry_seconds NOT BETWEEN 1 AND 86400
        OR p_error_code NOT IN (
            'SOURCE_MISSING', 'SOURCE_IDENTITY_MISMATCH',
            'BACKUP_CONFLICT', 'STORAGE_OPERATION_FAILED'
        )
    THEN
        RAISE EXCEPTION 'invalid Artifact backup replication retry'
            USING ERRCODE = '22023';
    END IF;
    UPDATE public.artifact_backup_replications AS replication
    SET next_retry_at = clock_timestamp() + make_interval(secs => p_retry_seconds),
        last_error_code = p_error_code,
        claim_id = NULL, claim_owner_id = NULL,
        claim_expires_at = NULL,
        updated_at = clock_timestamp()
    WHERE replication.id = p_replication_id
      AND replication.claim_id = p_claim_id
      AND replication.claim_expires_at > clock_timestamp()
      AND replication.state = 'PENDING';
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RETURN v_row_count = 1;
END
$$;
REVOKE ALL ON FUNCTION vela_retry_artifact_backup_replication(
    uuid, uuid, integer, text
) FROM PUBLIC;
ALTER FUNCTION vela_retry_artifact_backup_replication(
    uuid, uuid, integer, text
) OWNER TO vela_artifact_replication_owner;

GRANT USAGE ON SCHEMA public TO vela_artifact_replication;
GRANT EXECUTE ON FUNCTION vela_claim_artifact_backup_replication(text, uuid, integer)
    TO vela_artifact_replication;
GRANT EXECUTE ON FUNCTION vela_complete_artifact_backup_replication(
    uuid, uuid, text, bigint, bytea, text
) TO vela_artifact_replication;
GRANT EXECUTE ON FUNCTION vela_retry_artifact_backup_replication(
    uuid, uuid, integer, text
) TO vela_artifact_replication;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE artifact_backup_replications IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM artifact_backup_replications) THEN
        RAISE EXCEPTION 'cannot remove Artifact backup replication with durable intent or evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'artifact_backup_replication_requires_empty_evidence';
    END IF;
END
$$;
REVOKE EXECUTE ON FUNCTION vela_retry_artifact_backup_replication(
    uuid, uuid, integer, text
) FROM vela_artifact_replication;
REVOKE EXECUTE ON FUNCTION vela_complete_artifact_backup_replication(
    uuid, uuid, text, bigint, bytea, text
) FROM vela_artifact_replication;
REVOKE EXECUTE ON FUNCTION vela_claim_artifact_backup_replication(text, uuid, integer)
    FROM vela_artifact_replication;
REVOKE USAGE ON SCHEMA public FROM vela_artifact_replication;
DROP FUNCTION vela_retry_artifact_backup_replication(uuid, uuid, integer, text);
DROP FUNCTION vela_complete_artifact_backup_replication(
    uuid, uuid, text, bigint, bytea, text
);
DROP FUNCTION vela_claim_artifact_backup_replication(text, uuid, integer);
REVOKE SELECT ON artifacts, content_deletion_targets
    FROM vela_artifact_replication_owner;
DROP TRIGGER content_deletion_targets_cancel_backup_replication
    ON content_deletion_targets;
DROP FUNCTION vela_cancel_artifact_backup_replication();
DROP TRIGGER artifacts_enqueue_backup_replication ON artifacts;
DROP FUNCTION vela_enqueue_artifact_backup_replication();
DROP TRIGGER artifact_backup_replications_guard ON artifact_backup_replications;
DROP FUNCTION vela_guard_artifact_backup_replication();
DROP TABLE artifact_backup_replications;
DROP TYPE artifact_backup_replication_state;
-- +goose StatementEnd
