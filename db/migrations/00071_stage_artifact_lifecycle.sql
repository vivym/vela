-- +goose Up
-- +goose StatementBegin
CREATE TABLE stage_artifact_deletions (
    stage_artifact_id uuid PRIMARY KEY REFERENCES stage_artifacts(id),
    reason text NOT NULL CHECK (reason IN ('CONTENT_DELETION', 'RETENTION')),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_id uuid,
    claim_expires_at timestamptz,
    retry_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    completed_at timestamptz,
    CHECK ((claim_id IS NULL) = (claim_expires_at IS NULL))
);
ALTER TABLE stage_artifact_deletions ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_artifact_deletions FORCE ROW LEVEL SECURITY;
REVOKE ALL ON stage_artifact_deletions FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON stage_artifact_deletions TO vela_retention_owner;
GRANT SELECT ON stage_artifact_deletions TO vela_attempt_coordinator_owner;
GRANT SELECT ON stage_artifacts, stage_artifact_pins, stage_cache_entries,
    stage_cache_references, edge_buffer_credits TO vela_retention_owner;
GRANT UPDATE (state, deletion_fence, deleted_at) ON stage_artifacts TO vela_retention_owner;
GRANT UPDATE (state, released_at, release_reason) ON stage_artifact_pins,
    stage_cache_references TO vela_retention_owner;
GRANT UPDATE (state, released_at) ON edge_buffer_credits TO vela_retention_owner;
GRANT UPDATE (state, deletion_requested_at, terminal_at) ON stage_cache_entries
    TO vela_retention_owner;
GRANT SELECT (source_stage_artifact_id) ON artifacts TO vela_retention_owner;
CREATE INDEX stage_artifact_deletions_pending_idx
    ON stage_artifact_deletions(requested_at, stage_artifact_id) WHERE completed_at IS NULL;
CREATE INDEX stage_artifact_pins_active_owner_idx
    ON stage_artifact_pins(owner_job_id, id) WHERE state = 'ACTIVE';
CREATE INDEX stage_artifacts_expiry_idx
    ON stage_artifacts(expires_at, id) WHERE state = 'COMMITTED';

CREATE TABLE stage_materialization_deletions (
    materialization_lease_id uuid PRIMARY KEY REFERENCES stage_materialization_leases(id),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    object_version text CHECK (object_version IS NULL OR length(object_version) BETWEEN 1 AND 1000),
    resolved boolean NOT NULL DEFAULT false,
    claim_id uuid,
    claim_expires_at timestamptz,
    retry_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    completed_at timestamptz,
    CHECK ((claim_id IS NULL) = (claim_expires_at IS NULL)),
    CHECK (resolved OR object_version IS NULL),
    CHECK (completed_at IS NULL OR (resolved AND claim_id IS NULL))
);
ALTER TABLE stage_materialization_deletions ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_materialization_deletions FORCE ROW LEVEL SECURITY;
REVOKE ALL ON stage_materialization_deletions FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON stage_materialization_deletions TO vela_retention_owner;
GRANT SELECT ON stage_materialization_leases TO vela_retention_owner;
GRANT UPDATE (state, revoked_at, revoke_reason) ON stage_materialization_leases TO vela_retention_owner;
CREATE INDEX stage_materialization_deletions_pending_idx
    ON stage_materialization_deletions(requested_at, materialization_lease_id)
    WHERE completed_at IS NULL;

CREATE FUNCTION vela_validate_stage_artifact_lifecycle() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    IF TG_OP = 'DELETE'
       OR (to_jsonb(NEW) - ARRAY['state', 'deletion_fence', 'deleted_at'])
          IS DISTINCT FROM
          (to_jsonb(OLD) - ARRAY['state', 'deletion_fence', 'deleted_at'])
       OR NOT (
           (OLD.state = 'COMMITTED' AND NEW.state = 'DELETION_BLOCKED'
            AND NEW.deletion_fence = OLD.deletion_fence + 1
            AND NEW.deleted_at IS NULL)
           OR (OLD.state = 'DELETION_BLOCKED' AND NEW.state = 'DELETED'
               AND NEW.deletion_fence = OLD.deletion_fence
               AND NEW.deleted_at IS NOT NULL)
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'stage_artifact_evidence_immutable',
            MESSAGE = 'StageArtifact identity is immutable and deletion must move forward';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM stage_artifact_deletions
                   WHERE stage_artifact_id = OLD.id)
       OR EXISTS (SELECT 1 FROM stage_artifact_pins
                  WHERE stage_artifact_id = OLD.id AND state = 'ACTIVE') THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'stage_artifact_deletion_pinned',
            MESSAGE = 'StageArtifact deletion requires durable authority and no execution pins';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_validate_stage_artifact_lifecycle() OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_validate_stage_artifact_lifecycle() FROM PUBLIC;
DROP TRIGGER stage_artifacts_identity_immutable ON stage_artifacts;
CREATE TRIGGER stage_artifacts_identity_immutable
BEFORE UPDATE OR DELETE ON stage_artifacts
FOR EACH ROW EXECUTE FUNCTION vela_validate_stage_artifact_lifecycle();

CREATE FUNCTION vela_enqueue_job_stage_artifact_deletion(p_job_id uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public AS $$
BEGIN
    -- This lock serializes deletion authority against cache admission and hits.
    PERFORM 1 FROM stage_artifacts WHERE job_id = p_job_id ORDER BY id FOR UPDATE;
    INSERT INTO stage_artifact_deletions (stage_artifact_id, reason)
    SELECT id, 'CONTENT_DELETION' FROM stage_artifacts
    WHERE job_id = p_job_id AND state <> 'DELETED'
    ON CONFLICT (stage_artifact_id) DO NOTHING;
    UPDATE stage_cache_entries AS entry
    SET state = 'EVICTING', deletion_requested_at = clock_timestamp()
    WHERE entry.source_job_id = p_job_id AND entry.state = 'LIVE';
    UPDATE stage_materialization_leases
    SET state = 'REVOKED', revoked_at = clock_timestamp(), revoke_reason = 'CONTENT_DELETION'
    WHERE job_id = p_job_id AND state = 'ACTIVE';
    INSERT INTO stage_materialization_deletions (materialization_lease_id)
    SELECT id FROM stage_materialization_leases
    WHERE job_id = p_job_id AND state <> 'COMMITTED'
    ON CONFLICT (materialization_lease_id) DO NOTHING;
END
$$;
ALTER FUNCTION vela_enqueue_job_stage_artifact_deletion(uuid) OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_enqueue_job_stage_artifact_deletion(uuid) FROM PUBLIC;

CREATE FUNCTION vela_attach_content_deletion_stage_artifacts() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NEW.source = 'CUSTOMER' THEN
        PERFORM vela_enqueue_job_stage_artifact_deletion(NEW.job_id);
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_attach_content_deletion_stage_artifacts() OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_attach_content_deletion_stage_artifacts() FROM PUBLIC;
CREATE TRIGGER content_deletion_requests_attach_stage_artifacts
AFTER INSERT ON content_deletion_requests
FOR EACH ROW EXECUTE FUNCTION vela_attach_content_deletion_stage_artifacts();

CREATE FUNCTION vela_prepare_stage_artifact_lifecycle(p_limit integer)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_job_id uuid;
BEGIN
    IF p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'invalid StageArtifact lifecycle limit' USING ERRCODE = '22023';
    END IF;
    WITH terminal AS (
        SELECT pin.id FROM stage_artifact_pins AS pin
        JOIN jobs AS job ON job.id = pin.owner_job_id
        WHERE pin.state = 'ACTIVE' AND job.state IN ('SUCCEEDED', 'CANCELED', 'FAILED')
        ORDER BY pin.id LIMIT p_limit FOR UPDATE OF pin SKIP LOCKED
    )
    UPDATE stage_artifact_pins AS pin
    SET state = 'RELEASED', released_at = greatest(v_now, pin.acquired_at),
        release_reason = 'OWNER_JOB_TERMINAL'
    FROM terminal WHERE pin.id = terminal.id;
    UPDATE stage_cache_references AS reference
    SET state = 'RELEASED', released_at = greatest(v_now, reference.acquired_at),
        release_reason = 'OWNER_JOB_TERMINAL'
    FROM stage_artifact_pins AS pin
    WHERE reference.execution_pin_id = pin.id
      AND reference.state = 'ACTIVE' AND pin.state = 'RELEASED';
    UPDATE edge_buffer_credits AS credit
    SET state = 'RELEASED', released_at = greatest(v_now, credit.acquired_at)
    FROM attempts AS attempt JOIN jobs AS job ON job.id = attempt.job_id
    WHERE credit.attempt_id = attempt.id AND credit.state = 'HELD'
      AND job.state IN ('SUCCEEDED', 'CANCELED', 'FAILED');
    WITH expired AS (
        SELECT entry.id FROM stage_cache_entries AS entry
        WHERE entry.state = 'LIVE' AND entry.expires_at <= v_now
        ORDER BY entry.expires_at, entry.id LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE stage_cache_entries AS entry
    SET state = 'EXPIRED', terminal_at = v_now
    FROM expired WHERE entry.id = expired.id;
    UPDATE stage_cache_references AS reference
    SET state = 'RELEASED', released_at = greatest(v_now, reference.acquired_at),
        release_reason = 'CACHE_RETIRED'
    FROM stage_cache_entries AS entry
    WHERE reference.stage_cache_entry_id = entry.id
      AND reference.execution_pin_id IS NULL AND reference.state = 'ACTIVE'
      AND entry.state <> 'LIVE';
    UPDATE stage_cache_entries AS entry
    SET state = 'DELETED', terminal_at = v_now
    WHERE entry.state = 'EVICTING' AND NOT EXISTS (
        SELECT 1 FROM stage_artifact_pins AS pin
        WHERE pin.stage_artifact_id = entry.stage_artifact_id AND pin.state = 'ACTIVE'
    );
    UPDATE stage_materialization_leases
    SET state = 'EXPIRED', revoked_at = v_now, revoke_reason = 'RETENTION_EXPIRED'
    WHERE state = 'ACTIVE' AND expires_at <= v_now;
    INSERT INTO stage_materialization_deletions (materialization_lease_id)
    SELECT lease.id FROM stage_materialization_leases AS lease
    WHERE lease.state IN ('REVOKED', 'EXPIRED') AND NOT EXISTS (
        SELECT 1 FROM stage_materialization_deletions AS deletion
        WHERE deletion.materialization_lease_id = lease.id
    ) ORDER BY lease.expires_at, lease.id LIMIT p_limit
    ON CONFLICT (materialization_lease_id) DO NOTHING;
    -- Repair pre-migration deletion omissions without rewriting historical receipts.
    FOR v_job_id IN
        SELECT DISTINCT artifact.job_id FROM stage_artifacts AS artifact
        JOIN content_deletion_requests AS request ON request.job_id = artifact.job_id
          AND request.source = 'CUSTOMER'
        WHERE artifact.state <> 'DELETED' AND NOT EXISTS (
            SELECT 1 FROM stage_artifact_deletions AS deletion
            WHERE deletion.stage_artifact_id = artifact.id
        ) LIMIT p_limit
    LOOP
        PERFORM vela_enqueue_job_stage_artifact_deletion(v_job_id);
    END LOOP;
    INSERT INTO stage_artifact_deletions (stage_artifact_id, reason)
    SELECT artifact.id, 'RETENTION' FROM stage_artifacts AS artifact
    WHERE artifact.expires_at <= v_now AND artifact.state = 'COMMITTED'
      AND NOT EXISTS (SELECT 1 FROM stage_artifact_deletions AS deletion
                      WHERE deletion.stage_artifact_id = artifact.id)
      AND NOT EXISTS (SELECT 1 FROM artifacts AS visible
                      WHERE visible.source_stage_artifact_id = artifact.id
                        AND visible.state <> 'DELETED')
    ORDER BY artifact.expires_at, artifact.id LIMIT p_limit
    ON CONFLICT (stage_artifact_id) DO NOTHING;
END
$$;
ALTER FUNCTION vela_prepare_stage_artifact_lifecycle(integer) OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_prepare_stage_artifact_lifecycle(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_prepare_stage_artifact_lifecycle(integer) TO vela_retention;

CREATE FUNCTION vela_claim_stage_artifact_deletion(p_claim_id uuid, p_claim_seconds integer)
RETURNS TABLE (artifact_id uuid, object_key text, object_version text, sha256 bytea, size_bytes bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_id uuid;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_claim_id IS NULL OR p_claim_seconds NOT BETWEEN 1 AND 3600 THEN
        RAISE EXCEPTION 'invalid StageArtifact deletion claim' USING ERRCODE = '22023';
    END IF;
    SELECT artifact.id INTO v_id
    FROM stage_artifact_deletions AS deletion
    JOIN stage_artifacts AS artifact ON artifact.id = deletion.stage_artifact_id
    WHERE deletion.completed_at IS NULL
      AND (deletion.claim_expires_at IS NULL OR deletion.claim_expires_at <= v_now)
      AND (deletion.retry_at IS NULL OR deletion.retry_at <= v_now)
      AND NOT EXISTS (SELECT 1 FROM stage_artifact_pins AS pin
                      WHERE pin.stage_artifact_id = artifact.id AND pin.state = 'ACTIVE')
    ORDER BY deletion.requested_at, artifact.id LIMIT 1
    FOR UPDATE OF deletion, artifact SKIP LOCKED;
    IF NOT FOUND THEN RETURN; END IF;
    UPDATE stage_artifacts SET state = 'DELETION_BLOCKED', deletion_fence = deletion_fence + 1
    WHERE id = v_id AND state = 'COMMITTED';
    UPDATE stage_artifact_deletions
    SET claim_id = p_claim_id, claim_expires_at = v_now + make_interval(secs => p_claim_seconds),
        attempt_count = attempt_count + 1, retry_at = NULL
    WHERE stage_artifact_id = v_id;
    RETURN QUERY SELECT artifact.id, artifact.object_key, artifact.object_version, artifact.sha256, artifact.size_bytes
    FROM stage_artifacts AS artifact WHERE artifact.id = v_id;
END
$$;
ALTER FUNCTION vela_claim_stage_artifact_deletion(uuid, integer) OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_claim_stage_artifact_deletion(uuid, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_claim_stage_artifact_deletion(uuid, integer) TO vela_retention;

CREATE FUNCTION vela_finish_stage_artifact_deletion(
    p_artifact_id uuid, p_claim_id uuid, p_succeeded boolean, p_retry_seconds integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public AS $$
DECLARE v_now timestamptz := clock_timestamp();
BEGIN
    IF p_artifact_id IS NULL OR p_claim_id IS NULL OR p_succeeded IS NULL
       OR p_retry_seconds NOT BETWEEN 1 AND 86400 THEN
        RAISE EXCEPTION 'invalid StageArtifact deletion outcome' USING ERRCODE = '22023';
    END IF;
    PERFORM 1 FROM stage_artifact_deletions
    WHERE stage_artifact_id = p_artifact_id FOR UPDATE;
    v_now := clock_timestamp();
    IF NOT EXISTS (SELECT 1 FROM stage_artifact_deletions
                   WHERE stage_artifact_id = p_artifact_id AND claim_id = p_claim_id
                     AND completed_at IS NULL AND claim_expires_at > v_now) THEN
        RETURN false;
    END IF;
    IF p_succeeded THEN
        UPDATE stage_artifacts SET state = 'DELETED', deleted_at = v_now
        WHERE id = p_artifact_id AND state = 'DELETION_BLOCKED';
    END IF;
    UPDATE stage_artifact_deletions
    SET completed_at = CASE WHEN p_succeeded THEN v_now END,
        retry_at = CASE WHEN NOT p_succeeded THEN v_now + make_interval(secs => p_retry_seconds) END,
        claim_id = NULL, claim_expires_at = NULL
    WHERE stage_artifact_id = p_artifact_id;
    RETURN true;
END
$$;
ALTER FUNCTION vela_finish_stage_artifact_deletion(uuid, uuid, boolean, integer)
    OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_finish_stage_artifact_deletion(uuid, uuid, boolean, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_finish_stage_artifact_deletion(uuid, uuid, boolean, integer)
    TO vela_retention;

CREATE FUNCTION vela_claim_stage_materialization_deletion(p_claim_id uuid, p_claim_seconds integer)
RETURNS TABLE (lease_id uuid, object_key text, object_version text, resolved boolean, sha256 bytea, size_bytes bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE v_id uuid; v_now timestamptz := clock_timestamp();
BEGIN
    IF p_claim_id IS NULL OR p_claim_seconds NOT BETWEEN 1 AND 3600 THEN
        RAISE EXCEPTION 'invalid materialization deletion claim' USING ERRCODE = '22023';
    END IF;
    SELECT lease.id INTO v_id
    FROM stage_materialization_deletions AS deletion
    JOIN stage_materialization_leases AS lease ON lease.id = deletion.materialization_lease_id
    WHERE deletion.completed_at IS NULL
      AND (deletion.claim_expires_at IS NULL OR deletion.claim_expires_at <= v_now)
      AND (deletion.retry_at IS NULL OR deletion.retry_at <= v_now)
      AND lease.state IN ('REVOKED', 'EXPIRED')
      -- A revoked publisher can still be in flight until its original upload deadline.
      AND lease.expires_at <= v_now
      AND NOT EXISTS (SELECT 1 FROM stage_artifacts WHERE id = lease.artifact_id)
    ORDER BY deletion.requested_at, lease.id LIMIT 1
    FOR UPDATE OF deletion, lease SKIP LOCKED;
    IF NOT FOUND THEN RETURN; END IF;
    UPDATE stage_materialization_deletions
    SET claim_id = p_claim_id, claim_expires_at = v_now + make_interval(secs => p_claim_seconds),
        retry_at = NULL, attempt_count = attempt_count + 1
    WHERE materialization_lease_id = v_id;
    RETURN QUERY SELECT lease.id, lease.object_key, deletion.object_version, deletion.resolved, lease.expected_sha256, lease.expected_size_bytes
    FROM stage_materialization_leases AS lease
    JOIN stage_materialization_deletions AS deletion ON deletion.materialization_lease_id = lease.id
    WHERE lease.id = v_id;
END
$$;
ALTER FUNCTION vela_claim_stage_materialization_deletion(uuid, integer) OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_claim_stage_materialization_deletion(uuid, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_claim_stage_materialization_deletion(uuid, integer) TO vela_retention;

CREATE FUNCTION vela_resolve_stage_materialization_deletion(p_lease_id uuid, p_claim_id uuid, p_object_version text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF p_lease_id IS NULL OR p_claim_id IS NULL
       OR (p_object_version IS NOT NULL AND length(p_object_version) NOT BETWEEN 1 AND 1000) THEN
        RAISE EXCEPTION 'invalid materialization deletion version' USING ERRCODE = '22023';
    END IF;
    PERFORM 1 FROM stage_materialization_deletions
    WHERE materialization_lease_id = p_lease_id FOR UPDATE;
    UPDATE stage_materialization_deletions
    SET resolved = true, object_version = p_object_version
    WHERE materialization_lease_id = p_lease_id AND claim_id = p_claim_id
      AND claim_expires_at > clock_timestamp() AND completed_at IS NULL
      AND (NOT resolved OR object_version IS NOT DISTINCT FROM p_object_version);
    RETURN FOUND;
END
$$;
ALTER FUNCTION vela_resolve_stage_materialization_deletion(uuid, uuid, text) OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_resolve_stage_materialization_deletion(uuid, uuid, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_resolve_stage_materialization_deletion(uuid, uuid, text) TO vela_retention;

CREATE FUNCTION vela_finish_stage_materialization_deletion(
    p_lease_id uuid, p_claim_id uuid, p_succeeded boolean, p_retry_seconds integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF p_lease_id IS NULL OR p_claim_id IS NULL OR p_succeeded IS NULL
       OR p_retry_seconds NOT BETWEEN 1 AND 86400 THEN
        RAISE EXCEPTION 'invalid materialization deletion outcome' USING ERRCODE = '22023';
    END IF;
    PERFORM 1 FROM stage_materialization_deletions
    WHERE materialization_lease_id = p_lease_id FOR UPDATE;
    UPDATE stage_materialization_deletions
    SET completed_at = CASE WHEN p_succeeded THEN clock_timestamp() END,
        retry_at = CASE WHEN NOT p_succeeded THEN clock_timestamp() + make_interval(secs => p_retry_seconds) END,
        claim_id = NULL, claim_expires_at = NULL
    WHERE materialization_lease_id = p_lease_id AND claim_id = p_claim_id
      AND claim_expires_at > clock_timestamp() AND completed_at IS NULL
      AND (NOT p_succeeded OR resolved);
    RETURN FOUND;
END
$$;
ALTER FUNCTION vela_finish_stage_materialization_deletion(uuid, uuid, boolean, integer) OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_finish_stage_materialization_deletion(uuid, uuid, boolean, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_finish_stage_materialization_deletion(uuid, uuid, boolean, integer) TO vela_retention;

CREATE FUNCTION vela_guard_stage_content_deletion_completion() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NEW.source = 'CUSTOMER' AND NEW.state = 'COMPLETED' AND (EXISTS (
        SELECT 1 FROM stage_artifacts AS artifact
        WHERE artifact.job_id = NEW.job_id AND artifact.state <> 'DELETED'
    ) OR EXISTS (
        SELECT 1 FROM stage_materialization_leases AS lease
        WHERE lease.job_id = NEW.job_id AND lease.state <> 'COMMITTED'
          AND NOT EXISTS (SELECT 1 FROM stage_materialization_deletions AS deletion
                          WHERE deletion.materialization_lease_id = lease.id
                            AND deletion.completed_at IS NOT NULL)
    )) THEN
        RAISE EXCEPTION USING ERRCODE = '40001',
            CONSTRAINT = 'content_deletion_stage_artifacts_pending',
            MESSAGE = 'Content Deletion still has retained StageArtifact versions';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_guard_stage_content_deletion_completion() OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_guard_stage_content_deletion_completion() FROM PUBLIC;
CREATE TRIGGER content_deletion_requests_guard_stage_artifacts
BEFORE UPDATE OF state ON content_deletion_requests
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_content_deletion_completion();

DO $$
DECLARE v_definition text; v_old text; v_new text;
BEGIN
    v_definition := pg_get_functiondef('vela_admit_stage_cache_entry(jsonb)'::regprocedure);
    v_old := 'IF v_artifact.state <> ''COMMITTED'' OR v_artifact.expires_at <= v_expires_at';
    v_new := v_old || E'\n       OR EXISTS (SELECT 1 FROM stage_artifact_deletions WHERE stage_artifact_id = v_artifact.id)';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'Stage cache admission lifecycle drift'; END IF;
    EXECUTE replace(v_definition, v_old, v_new);
    v_definition := pg_get_functiondef('vela_hit_stage_cache(jsonb)'::regprocedure);
    v_old := 'OR v_artifact.state <> ''COMMITTED''';
    v_new := v_old || E'\n       OR EXISTS (SELECT 1 FROM stage_artifact_deletions WHERE stage_artifact_id = v_artifact.id)';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'Stage cache hit lifecycle drift'; END IF;
    EXECUTE replace(v_definition, v_old, v_new);
    v_definition := pg_get_functiondef('vela_claim_content_deletion_target(text,uuid,integer)'::regprocedure);
    v_old := 'AND target.state <> ''COMPLETED''';
    v_new := v_old || E'\n      AND NOT EXISTS (SELECT 1 FROM stage_artifact_deletions AS stage_deletion\n'
        || E'          JOIN stage_artifacts AS stage_artifact ON stage_artifact.id = stage_deletion.stage_artifact_id\n'
        || E'          WHERE stage_artifact.job_id = deletion.job_id AND stage_deletion.completed_at IS NULL)\n'
        || E'      AND NOT EXISTS (SELECT 1 FROM stage_materialization_deletions AS orphan_deletion\n'
        || E'          JOIN stage_materialization_leases AS orphan ON orphan.id = orphan_deletion.materialization_lease_id\n'
        || E'          WHERE orphan.job_id = deletion.job_id AND orphan_deletion.completed_at IS NULL)\n'
        || E'      AND NOT EXISTS (SELECT 1 FROM stage_artifacts AS stage_artifact\n'
        || E'          JOIN stage_artifact_pins AS pin ON pin.stage_artifact_id = stage_artifact.id\n'
        || E'          WHERE stage_artifact.object_key = target.object_key AND pin.state = ''ACTIVE'')';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'Content Deletion claim lifecycle drift'; END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE v_definition text; v_old text; v_new text;
BEGIN
    LOCK TABLE stage_artifact_deletions, stage_materialization_deletions, stage_artifact_pins, stage_cache_entries
        IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM stage_artifact_deletions)
       OR EXISTS (SELECT 1 FROM stage_materialization_deletions)
       OR EXISTS (SELECT 1 FROM stage_artifact_pins WHERE release_reason = 'OWNER_JOB_TERMINAL')
       OR EXISTS (SELECT 1 FROM stage_cache_entries WHERE state = 'EXPIRED') THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'stage_artifact_lifecycle_rollback_is_unsafe',
            MESSAGE = 'StageArtifact deletion and terminal cleanup evidence prevents rollback';
    END IF;
    v_definition := pg_get_functiondef('vela_admit_stage_cache_entry(jsonb)'::regprocedure);
    v_old := 'IF v_artifact.state <> ''COMMITTED'' OR v_artifact.expires_at <= v_expires_at';
    v_new := v_old || E'\n       OR EXISTS (SELECT 1 FROM stage_artifact_deletions WHERE stage_artifact_id = v_artifact.id)';
    IF strpos(v_definition, v_new) = 0 THEN RAISE EXCEPTION 'Stage cache admission lifecycle rollback drift'; END IF;
    EXECUTE replace(v_definition, v_new, v_old);
    v_definition := pg_get_functiondef('vela_hit_stage_cache(jsonb)'::regprocedure);
    v_old := 'OR v_artifact.state <> ''COMMITTED''';
    v_new := v_old || E'\n       OR EXISTS (SELECT 1 FROM stage_artifact_deletions WHERE stage_artifact_id = v_artifact.id)';
    IF strpos(v_definition, v_new) = 0 THEN RAISE EXCEPTION 'Stage cache hit lifecycle rollback drift'; END IF;
    EXECUTE replace(v_definition, v_new, v_old);
    v_definition := pg_get_functiondef('vela_claim_content_deletion_target(text,uuid,integer)'::regprocedure);
    v_old := 'AND target.state <> ''COMPLETED''';
    v_new := v_old || E'\n      AND NOT EXISTS (SELECT 1 FROM stage_artifact_deletions AS stage_deletion\n'
        || E'          JOIN stage_artifacts AS stage_artifact ON stage_artifact.id = stage_deletion.stage_artifact_id\n'
        || E'          WHERE stage_artifact.job_id = deletion.job_id AND stage_deletion.completed_at IS NULL)\n'
        || E'      AND NOT EXISTS (SELECT 1 FROM stage_materialization_deletions AS orphan_deletion\n'
        || E'          JOIN stage_materialization_leases AS orphan ON orphan.id = orphan_deletion.materialization_lease_id\n'
        || E'          WHERE orphan.job_id = deletion.job_id AND orphan_deletion.completed_at IS NULL)\n'
        || E'      AND NOT EXISTS (SELECT 1 FROM stage_artifacts AS stage_artifact\n'
        || E'          JOIN stage_artifact_pins AS pin ON pin.stage_artifact_id = stage_artifact.id\n'
        || E'          WHERE stage_artifact.object_key = target.object_key AND pin.state = ''ACTIVE'')';
    IF strpos(v_definition, v_new) = 0 THEN RAISE EXCEPTION 'Content Deletion claim lifecycle rollback drift'; END IF;
    EXECUTE replace(v_definition, v_new, v_old);
END
$$;
DROP TRIGGER content_deletion_requests_guard_stage_artifacts ON content_deletion_requests;
DROP FUNCTION vela_guard_stage_content_deletion_completion();
DROP TRIGGER content_deletion_requests_attach_stage_artifacts ON content_deletion_requests;
DROP FUNCTION vela_attach_content_deletion_stage_artifacts();
DROP FUNCTION vela_claim_stage_artifact_deletion(uuid, integer);
DROP FUNCTION vela_finish_stage_artifact_deletion(uuid, uuid, boolean, integer);
DROP FUNCTION vela_prepare_stage_artifact_lifecycle(integer);
DROP FUNCTION vela_enqueue_job_stage_artifact_deletion(uuid);
DROP FUNCTION vela_claim_stage_materialization_deletion(uuid, integer);
DROP FUNCTION vela_resolve_stage_materialization_deletion(uuid, uuid, text);
DROP FUNCTION vela_finish_stage_materialization_deletion(uuid, uuid, boolean, integer);
DROP TABLE stage_materialization_deletions;
REVOKE SELECT ON stage_materialization_leases FROM vela_retention_owner;
REVOKE UPDATE (state, revoked_at, revoke_reason) ON stage_materialization_leases FROM vela_retention_owner;
DROP INDEX stage_artifact_pins_active_owner_idx;
DROP INDEX stage_artifacts_expiry_idx;
DROP TRIGGER stage_artifacts_identity_immutable ON stage_artifacts;
CREATE TRIGGER stage_artifacts_identity_immutable
BEFORE UPDATE OR DELETE ON stage_artifacts
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_artifact_immutable_mutation();
DROP FUNCTION vela_validate_stage_artifact_lifecycle();
DROP TABLE stage_artifact_deletions;
REVOKE SELECT ON stage_artifacts, stage_artifact_pins, stage_cache_entries,
    stage_cache_references, edge_buffer_credits FROM vela_retention_owner;
REVOKE UPDATE (state, deletion_fence, deleted_at) ON stage_artifacts FROM vela_retention_owner;
REVOKE UPDATE (state, released_at, release_reason) ON stage_artifact_pins,
    stage_cache_references FROM vela_retention_owner;
REVOKE UPDATE (state, released_at) ON edge_buffer_credits FROM vela_retention_owner;
REVOKE UPDATE (state, deletion_requested_at, terminal_at) ON stage_cache_entries
    FROM vela_retention_owner;
REVOKE SELECT (source_stage_artifact_id) ON artifacts FROM vela_retention_owner;
-- +goose StatementEnd
