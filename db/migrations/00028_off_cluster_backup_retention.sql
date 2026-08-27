-- +goose Up
-- +goose StatementBegin
CREATE TYPE content_storage_tier AS ENUM ('PRIMARY', 'OFF_CLUSTER_BACKUP');

ALTER TABLE content_deletion_targets
    ADD COLUMN storage_tier content_storage_tier NOT NULL DEFAULT 'PRIMARY',
    ADD COLUMN purged_version_count integer CHECK (
        purged_version_count IS NULL OR purged_version_count >= 0
    );
ALTER TABLE content_deletion_receipt_targets
    ADD COLUMN storage_tier content_storage_tier NOT NULL DEFAULT 'PRIMARY',
    ADD COLUMN purged_version_count integer CHECK (
        purged_version_count IS NULL OR purged_version_count >= 0
    );

DROP INDEX content_deletion_targets_artifact_idx;
CREATE UNIQUE INDEX content_deletion_targets_artifact_tier_idx
    ON content_deletion_targets (request_id, artifact_id, storage_tier)
    WHERE artifact_id IS NOT NULL;

DO $$
DECLARE
    v_target_outcome_constraint text;
    v_receipt_outcome_constraint text;
BEGIN
    SELECT constraint_entry.conname INTO STRICT v_target_outcome_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.content_deletion_targets'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%storage_outcome%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%MULTIPART_ABORTED%';
    EXECUTE format(
        'ALTER TABLE public.content_deletion_targets DROP CONSTRAINT %I',
        v_target_outcome_constraint
    );

    SELECT constraint_entry.conname INTO STRICT v_receipt_outcome_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.content_deletion_receipt_targets'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%storage_outcome%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%MULTIPART_ABORTED%';
    EXECUTE format(
        'ALTER TABLE public.content_deletion_receipt_targets DROP CONSTRAINT %I',
        v_receipt_outcome_constraint
    );
END
$$;
ALTER TABLE content_deletion_targets
    ADD CONSTRAINT content_deletion_targets_storage_outcome_check CHECK (
        storage_outcome IS NULL
        OR storage_outcome IN (
            'DELETED', 'ALREADY_ABSENT', 'MULTIPART_ABORTED',
            'NO_INCOMPLETE_UPLOADS', 'PURGED'
        )
    ),
    ADD CONSTRAINT content_deletion_targets_backup_result_check CHECK (
        (
            storage_tier = 'PRIMARY'
            AND purged_version_count IS NULL
            AND storage_outcome IS DISTINCT FROM 'PURGED'
        )
        OR (
            storage_tier = 'OFF_CLUSTER_BACKUP'
            AND action = 'OBJECT_DISCOVERY'
            AND artifact_id IS NOT NULL
            AND debug_dump_authorization_id IS NULL
            AND debug_dump_id IS NULL
            AND object_version_id IS NULL
            AND discovered_object_version_id IS NULL
            AND (
                state <> 'COMPLETED'
                OR (
                    state = 'COMPLETED'
                    AND purged_version_count IS NOT NULL
                    AND (
                        (purged_version_count = 0 AND storage_outcome = 'ALREADY_ABSENT')
                        OR (purged_version_count > 0 AND storage_outcome = 'PURGED')
                    )
                )
            )
        )
    );
ALTER TABLE content_deletion_receipt_targets
    ADD CONSTRAINT content_deletion_receipt_targets_storage_outcome_check CHECK (
        storage_outcome IN (
            'DELETED', 'ALREADY_ABSENT', 'MULTIPART_ABORTED',
            'NO_INCOMPLETE_UPLOADS', 'PURGED'
        )
    ),
    ADD CONSTRAINT content_deletion_receipt_targets_backup_result_check CHECK (
        (
            storage_tier = 'PRIMARY'
            AND purged_version_count IS NULL
            AND storage_outcome <> 'PURGED'
        )
        OR (
            storage_tier = 'OFF_CLUSTER_BACKUP'
            AND purged_version_count IS NOT NULL
            AND (
                (purged_version_count = 0 AND storage_outcome = 'ALREADY_ABSENT')
                OR (purged_version_count > 0 AND storage_outcome = 'PURGED')
            )
        )
    );

CREATE FUNCTION vela_attach_off_cluster_backup_target() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_source public.content_deletion_source;
    v_artifact_state public.artifact_state;
BEGIN
    IF NEW.storage_tier <> 'PRIMARY' OR NEW.artifact_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT deletion.source INTO STRICT v_source
    FROM public.content_deletion_requests AS deletion
    WHERE deletion.id = NEW.request_id;
    IF v_source NOT IN ('CUSTOMER', 'RETENTION_ARTIFACT') THEN
        RETURN NEW;
    END IF;
    SELECT artifact.state INTO STRICT v_artifact_state
    FROM public.artifacts AS artifact
    WHERE artifact.id = NEW.artifact_id
      AND artifact.organization_id = NEW.organization_id
      AND artifact.project_id = NEW.project_id;
    IF v_artifact_state <> 'COMMITTED' THEN
        RETURN NEW;
    END IF;

    INSERT INTO public.content_deletion_targets (
        id, organization_id, project_id, job_id, request_id, action,
        artifact_id, object_key, storage_tier
    ) VALUES (
        gen_random_uuid(), NEW.organization_id, NEW.project_id, NEW.job_id,
        NEW.request_id, 'OBJECT_DISCOVERY', NEW.artifact_id, NEW.object_key,
        'OFF_CLUSTER_BACKUP'
    ) ON CONFLICT (request_id, artifact_id, storage_tier)
        WHERE artifact_id IS NOT NULL DO NOTHING;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_attach_off_cluster_backup_target() FROM PUBLIC;
ALTER FUNCTION vela_attach_off_cluster_backup_target() OWNER TO vela_retention_owner;
CREATE TRIGGER content_deletion_targets_attach_off_cluster_backup
AFTER INSERT ON content_deletion_targets
FOR EACH ROW EXECUTE FUNCTION vela_attach_off_cluster_backup_target();

-- Existing schema-27 targets do not pass through the new INSERT trigger. Use
-- immutable ArtifactSet membership because a completed primary purge may have
-- already advanced the current Artifact state to DELETED.
INSERT INTO public.content_deletion_targets (
    id, organization_id, project_id, job_id, request_id, action,
    artifact_id, object_key, storage_tier
)
SELECT
    gen_random_uuid(), target.organization_id, target.project_id, target.job_id,
    target.request_id, 'OBJECT_DISCOVERY', target.artifact_id, target.object_key,
    'OFF_CLUSTER_BACKUP'
FROM public.content_deletion_targets AS target
JOIN public.content_deletion_requests AS deletion
  ON deletion.id = target.request_id
WHERE target.storage_tier = 'PRIMARY'
  AND target.artifact_id IS NOT NULL
  AND deletion.source IN ('CUSTOMER', 'RETENTION_ARTIFACT')
  AND deletion.state <> 'COMPLETED'
  AND EXISTS (
      SELECT 1
      FROM public.artifact_set_items AS item
      WHERE item.organization_id = target.organization_id
        AND item.project_id = target.project_id
        AND item.job_id = target.job_id
        AND item.artifact_id = target.artifact_id
  )
ON CONFLICT (request_id, artifact_id, storage_tier)
    WHERE artifact_id IS NOT NULL DO NOTHING;

CREATE FUNCTION vela_apply_off_cluster_backup_outcome() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    v_count_text text;
    v_count integer;
BEGIN
    IF OLD.storage_tier <> 'OFF_CLUSTER_BACKUP'
        OR NEW.state <> 'COMPLETED'
        OR OLD.state = 'COMPLETED'
    THEN
        RETURN NEW;
    END IF;
    v_count_text := current_setting('vela.off_cluster_purged_version_count', true);
    IF v_count_text IS NULL OR v_count_text !~ '^[0-9]+$' THEN
        RAISE EXCEPTION 'off-cluster backup purge outcome is required'
            USING ERRCODE = '22023';
    END IF;
    v_count := v_count_text::integer;
    NEW.purged_version_count := coalesce(OLD.purged_version_count, 0) + v_count;
    NEW.storage_outcome := CASE
        WHEN NEW.purged_version_count = 0 THEN 'ALREADY_ABSENT'
        ELSE 'PURGED'
    END;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_apply_off_cluster_backup_outcome() FROM PUBLIC;
ALTER FUNCTION vela_apply_off_cluster_backup_outcome() OWNER TO vela_retention_owner;
CREATE TRIGGER content_deletion_targets_apply_off_cluster_backup_outcome
BEFORE UPDATE OF state ON content_deletion_targets
FOR EACH ROW EXECUTE FUNCTION vela_apply_off_cluster_backup_outcome();

CREATE FUNCTION vela_snapshot_content_deletion_storage_tier() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    SELECT target.storage_tier, target.purged_version_count
    INTO STRICT NEW.storage_tier, NEW.purged_version_count
    FROM public.content_deletion_targets AS target
    WHERE target.id = NEW.target_id
      AND target.request_id = NEW.request_id;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_snapshot_content_deletion_storage_tier() FROM PUBLIC;
ALTER FUNCTION vela_snapshot_content_deletion_storage_tier() OWNER TO vela_retention_owner;
CREATE TRIGGER content_deletion_receipt_targets_snapshot_storage_tier
BEFORE INSERT ON content_deletion_receipt_targets
FOR EACH ROW EXECUTE FUNCTION vela_snapshot_content_deletion_storage_tier();

CREATE OR REPLACE FUNCTION vela_claim_content_deletion_target(
    p_claim_owner_id text,
    p_claim_id uuid,
    p_claim_seconds integer
) RETURNS TABLE (
    target_id uuid,
    target_action content_deletion_target_action,
    object_key text,
    object_version_id text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_target_id uuid;
    v_claimed_at timestamptz := clock_timestamp();
BEGIN
    IF p_claim_id IS NULL OR p_claim_owner_id IS NULL
        OR length(p_claim_owner_id) NOT BETWEEN 1 AND 500
        OR p_claim_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid Content Deletion claim' USING ERRCODE = '22023';
    END IF;

    SELECT target.id INTO v_target_id
    FROM public.content_deletion_targets AS target
    JOIN public.content_deletion_requests AS deletion ON deletion.id = target.request_id
    WHERE target.storage_tier = 'PRIMARY'
      AND deletion.state <> 'COMPLETED'
      AND (deletion.next_retry_at IS NULL OR deletion.next_retry_at <= v_claimed_at)
      AND (deletion.claim_id IS NULL OR deletion.claim_expires_at IS NULL
           OR deletion.claim_expires_at <= v_claimed_at)
      AND target.state <> 'COMPLETED'
      AND (target.next_retry_at IS NULL OR target.next_retry_at <= v_claimed_at)
      AND (target.claim_id IS NULL OR target.claim_expires_at IS NULL
           OR target.claim_expires_at <= v_claimed_at)
    ORDER BY deletion.deadline_at, deletion.requested_at, target.created_at, target.id
    FOR UPDATE OF deletion, target SKIP LOCKED
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE public.content_deletion_requests AS deletion
    SET state = 'IN_PROGRESS', next_retry_at = NULL, claim_id = p_claim_id,
        claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = deletion.version + 1, updated_at = v_claimed_at
    FROM public.content_deletion_targets AS target
    WHERE target.id = v_target_id AND deletion.id = target.request_id;
    UPDATE public.content_deletion_targets AS target
    SET state = 'IN_PROGRESS', attempt_count = target.attempt_count + 1,
        next_retry_at = NULL, claim_id = p_claim_id, claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = target.version + 1, updated_at = v_claimed_at
    WHERE target.id = v_target_id;

    RETURN QUERY SELECT target.id, target.action, target.object_key, target.object_version_id
    FROM public.content_deletion_targets AS target WHERE target.id = v_target_id;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_content_deletion_target(text, uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_claim_content_deletion_target(text, uuid, integer)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_claim_off_cluster_content_deletion_target(
    p_claim_owner_id text,
    p_claim_id uuid,
    p_claim_seconds integer
) RETURNS TABLE (target_id uuid, object_key text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_target_id uuid;
    v_claimed_at timestamptz := clock_timestamp();
BEGIN
    IF p_claim_id IS NULL OR p_claim_owner_id IS NULL
        OR length(p_claim_owner_id) NOT BETWEEN 1 AND 500
        OR p_claim_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid off-cluster Content Deletion claim'
            USING ERRCODE = '22023';
    END IF;

    SELECT target.id INTO v_target_id
    FROM public.content_deletion_targets AS target
    JOIN public.content_deletion_requests AS deletion ON deletion.id = target.request_id
    WHERE target.storage_tier = 'OFF_CLUSTER_BACKUP'
      AND deletion.state <> 'COMPLETED'
      AND (deletion.next_retry_at IS NULL OR deletion.next_retry_at <= v_claimed_at)
      AND (deletion.claim_id IS NULL OR deletion.claim_expires_at IS NULL
           OR deletion.claim_expires_at <= v_claimed_at)
      AND target.state <> 'COMPLETED'
      AND (target.next_retry_at IS NULL OR target.next_retry_at <= v_claimed_at)
      AND (target.claim_id IS NULL OR target.claim_expires_at IS NULL
           OR target.claim_expires_at <= v_claimed_at)
    ORDER BY deletion.deadline_at, deletion.requested_at, target.created_at, target.id
    FOR UPDATE OF deletion, target SKIP LOCKED
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE public.content_deletion_requests AS deletion
    SET state = 'IN_PROGRESS', next_retry_at = NULL, claim_id = p_claim_id,
        claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = deletion.version + 1, updated_at = v_claimed_at
    FROM public.content_deletion_targets AS target
    WHERE target.id = v_target_id AND deletion.id = target.request_id;
    UPDATE public.content_deletion_targets AS target
    SET state = 'IN_PROGRESS', attempt_count = target.attempt_count + 1,
        next_retry_at = NULL, claim_id = p_claim_id, claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = target.version + 1, updated_at = v_claimed_at
    WHERE target.id = v_target_id;

    RETURN QUERY SELECT target.id, target.object_key
    FROM public.content_deletion_targets AS target WHERE target.id = v_target_id;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_off_cluster_content_deletion_target(text, uuid, integer)
    FROM PUBLIC;
ALTER FUNCTION vela_claim_off_cluster_content_deletion_target(text, uuid, integer)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_complete_off_cluster_content_deletion_target(
    p_target_id uuid,
    p_claim_id uuid,
    p_receipt_id uuid,
    p_purged_version_count integer
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_storage_tier public.content_storage_tier;
    v_marked boolean;
BEGIN
    IF p_purged_version_count IS NULL OR p_purged_version_count < 0 THEN
        RAISE EXCEPTION 'invalid off-cluster purge count' USING ERRCODE = '22023';
    END IF;
    SELECT target.storage_tier INTO v_storage_tier
    FROM public.content_deletion_targets AS target
    WHERE target.id = p_target_id
    FOR UPDATE;
    IF NOT FOUND OR v_storage_tier <> 'OFF_CLUSTER_BACKUP' THEN
        RETURN false;
    END IF;
    PERFORM set_config(
        'vela.off_cluster_purged_version_count',
        p_purged_version_count::text,
        true
    );
    SELECT completed.marked INTO STRICT v_marked
    FROM public.vela_complete_content_deletion_target(
        p_target_id, p_claim_id, p_receipt_id, NULL, 'ALREADY_ABSENT'
    ) AS completed;
    RETURN v_marked;
END
$$;
REVOKE ALL ON FUNCTION vela_complete_off_cluster_content_deletion_target(
    uuid, uuid, uuid, integer
) FROM PUBLIC;
ALTER FUNCTION vela_complete_off_cluster_content_deletion_target(
    uuid, uuid, uuid, integer
) OWNER TO vela_retention_owner;

CREATE FUNCTION vela_retry_off_cluster_content_deletion_target(
    p_target_id uuid,
    p_claim_id uuid,
    p_retry_seconds integer,
    p_purged_version_count integer
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_storage_tier public.content_storage_tier;
    v_marked boolean;
BEGIN
    IF p_purged_version_count IS NULL OR p_purged_version_count < 0 THEN
        RAISE EXCEPTION 'invalid partial off-cluster purge count' USING ERRCODE = '22023';
    END IF;
    SELECT target.storage_tier INTO v_storage_tier
    FROM public.content_deletion_targets AS target
    WHERE target.id = p_target_id
    FOR UPDATE;
    IF NOT FOUND OR v_storage_tier <> 'OFF_CLUSTER_BACKUP' THEN
        RETURN false;
    END IF;
    SELECT public.vela_retry_content_deletion_target(
        p_target_id, p_claim_id, p_retry_seconds, 'STORAGE_OPERATION_FAILED'
    ) INTO STRICT v_marked;
    IF v_marked THEN
        UPDATE public.content_deletion_targets AS target
        SET purged_version_count = coalesce(target.purged_version_count, 0)
                + p_purged_version_count
        WHERE target.id = p_target_id;
    END IF;
    RETURN v_marked;
END
$$;
REVOKE ALL ON FUNCTION vela_retry_off_cluster_content_deletion_target(
    uuid, uuid, integer, integer
) FROM PUBLIC;
ALTER FUNCTION vela_retry_off_cluster_content_deletion_target(
    uuid, uuid, integer, integer
) OWNER TO vela_retention_owner;

GRANT USAGE ON SCHEMA public TO vela_backup_retention;
GRANT EXECUTE ON FUNCTION vela_claim_off_cluster_content_deletion_target(
    text, uuid, integer
) TO vela_backup_retention;
GRANT EXECUTE ON FUNCTION vela_complete_off_cluster_content_deletion_target(
    uuid, uuid, uuid, integer
) TO vela_backup_retention;
GRANT EXECUTE ON FUNCTION vela_retry_off_cluster_content_deletion_target(
    uuid, uuid, integer, integer
) TO vela_backup_retention;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE content_deletion_targets, content_deletion_receipt_targets
    IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM content_deletion_targets
        WHERE storage_tier = 'OFF_CLUSTER_BACKUP'
    ) THEN
        RAISE EXCEPTION 'cannot remove off-cluster retention with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'off_cluster_retention_requires_empty_evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_retry_off_cluster_content_deletion_target(
    uuid, uuid, integer, integer
) FROM vela_backup_retention;
REVOKE EXECUTE ON FUNCTION vela_complete_off_cluster_content_deletion_target(
    uuid, uuid, uuid, integer
) FROM vela_backup_retention;
REVOKE EXECUTE ON FUNCTION vela_claim_off_cluster_content_deletion_target(
    text, uuid, integer
) FROM vela_backup_retention;
REVOKE USAGE ON SCHEMA public FROM vela_backup_retention;
DROP FUNCTION vela_retry_off_cluster_content_deletion_target(uuid, uuid, integer, integer);
DROP FUNCTION vela_complete_off_cluster_content_deletion_target(uuid, uuid, uuid, integer);
DROP FUNCTION vela_claim_off_cluster_content_deletion_target(text, uuid, integer);

CREATE OR REPLACE FUNCTION vela_claim_content_deletion_target(
    p_claim_owner_id text,
    p_claim_id uuid,
    p_claim_seconds integer
) RETURNS TABLE (
    target_id uuid,
    target_action content_deletion_target_action,
    object_key text,
    object_version_id text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_target_id uuid;
    v_claimed_at timestamptz := clock_timestamp();
BEGIN
    IF p_claim_id IS NULL OR p_claim_owner_id IS NULL
        OR length(p_claim_owner_id) NOT BETWEEN 1 AND 500
        OR p_claim_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid Content Deletion claim' USING ERRCODE = '22023';
    END IF;
    SELECT target.id INTO v_target_id
    FROM public.content_deletion_targets AS target
    JOIN public.content_deletion_requests AS deletion ON deletion.id = target.request_id
    WHERE deletion.state <> 'COMPLETED'
      AND (deletion.next_retry_at IS NULL OR deletion.next_retry_at <= v_claimed_at)
      AND (deletion.claim_id IS NULL OR deletion.claim_expires_at IS NULL
           OR deletion.claim_expires_at <= v_claimed_at)
      AND target.state <> 'COMPLETED'
      AND (target.next_retry_at IS NULL OR target.next_retry_at <= v_claimed_at)
      AND (target.claim_id IS NULL OR target.claim_expires_at IS NULL
           OR target.claim_expires_at <= v_claimed_at)
    ORDER BY deletion.deadline_at, deletion.requested_at, target.created_at, target.id
    FOR UPDATE OF deletion, target SKIP LOCKED
    LIMIT 1;
    IF NOT FOUND THEN RETURN; END IF;
    UPDATE public.content_deletion_requests AS deletion
    SET state = 'IN_PROGRESS', next_retry_at = NULL, claim_id = p_claim_id,
        claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = deletion.version + 1, updated_at = v_claimed_at
    FROM public.content_deletion_targets AS target
    WHERE target.id = v_target_id AND deletion.id = target.request_id;
    UPDATE public.content_deletion_targets AS target
    SET state = 'IN_PROGRESS', attempt_count = target.attempt_count + 1,
        next_retry_at = NULL, claim_id = p_claim_id, claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = target.version + 1, updated_at = v_claimed_at
    WHERE target.id = v_target_id;
    RETURN QUERY SELECT target.id, target.action, target.object_key, target.object_version_id
    FROM public.content_deletion_targets AS target WHERE target.id = v_target_id;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_content_deletion_target(text, uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_claim_content_deletion_target(text, uuid, integer)
    OWNER TO vela_retention_owner;

DROP TRIGGER content_deletion_receipt_targets_snapshot_storage_tier
    ON content_deletion_receipt_targets;
DROP FUNCTION vela_snapshot_content_deletion_storage_tier();
DROP TRIGGER content_deletion_targets_apply_off_cluster_backup_outcome
    ON content_deletion_targets;
DROP FUNCTION vela_apply_off_cluster_backup_outcome();
DROP TRIGGER content_deletion_targets_attach_off_cluster_backup ON content_deletion_targets;
DROP FUNCTION vela_attach_off_cluster_backup_target();

ALTER TABLE content_deletion_targets
    DROP CONSTRAINT content_deletion_targets_backup_result_check,
    DROP CONSTRAINT content_deletion_targets_storage_outcome_check;
ALTER TABLE content_deletion_receipt_targets
    DROP CONSTRAINT content_deletion_receipt_targets_backup_result_check,
    DROP CONSTRAINT content_deletion_receipt_targets_storage_outcome_check;
ALTER TABLE content_deletion_targets
    ADD CONSTRAINT content_deletion_targets_storage_outcome_check CHECK (
        storage_outcome IS NULL
        OR storage_outcome IN (
            'DELETED', 'ALREADY_ABSENT', 'MULTIPART_ABORTED', 'NO_INCOMPLETE_UPLOADS'
        )
    );
ALTER TABLE content_deletion_receipt_targets
    ADD CONSTRAINT content_deletion_receipt_targets_storage_outcome_check CHECK (
        storage_outcome IN (
            'DELETED', 'ALREADY_ABSENT', 'MULTIPART_ABORTED', 'NO_INCOMPLETE_UPLOADS'
        )
    );

DROP INDEX content_deletion_targets_artifact_tier_idx;
CREATE UNIQUE INDEX content_deletion_targets_artifact_idx
    ON content_deletion_targets (request_id, artifact_id)
    WHERE artifact_id IS NOT NULL;
ALTER TABLE content_deletion_receipt_targets
    DROP COLUMN purged_version_count,
    DROP COLUMN storage_tier;
ALTER TABLE content_deletion_targets
    DROP COLUMN purged_version_count,
    DROP COLUMN storage_tier;
DROP TYPE content_storage_tier;
-- +goose StatementEnd
