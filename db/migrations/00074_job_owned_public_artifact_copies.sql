-- +goose Up
-- +goose StatementBegin
ALTER TABLE artifacts DROP CONSTRAINT artifacts_source_stage_artifact_id_key;
CREATE INDEX artifacts_stage_source_idx ON artifacts(source_stage_artifact_id)
    WHERE source_stage_artifact_id IS NOT NULL;
GRANT SELECT (expires_at) ON artifacts TO vela_retention_owner;

CREATE FUNCTION vela_stage_publication_deletion_identity(p_target_id uuid)
RETURNS TABLE (sha256 bytea, size_bytes bigint)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT source.sha256, source.size_bytes
    FROM content_deletion_targets AS target
    JOIN artifacts AS publication ON publication.id = target.artifact_id
    JOIN stage_artifacts AS source ON source.id = publication.source_stage_artifact_id
    WHERE target.id = p_target_id AND publication.object_key <> source.object_key;
$$;
ALTER FUNCTION vela_stage_publication_deletion_identity(uuid) OWNER TO vela_retention_owner;
REVOKE ALL ON FUNCTION vela_stage_publication_deletion_identity(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_stage_publication_deletion_identity(uuid) TO vela_retention;

DO $$
DECLARE v_definition text; v_old text; v_new text;
BEGIN
    v_definition := pg_get_functiondef('vela_prepare_stage_artifact_lifecycle(integer)'::regprocedure);
    v_old := 'WHERE visible.source_stage_artifact_id = artifact.id';
    v_new := v_old || E'\n                        AND visible.object_key = artifact.object_key';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'StageArtifact public retention drift'; END IF;
    EXECUTE replace(v_definition, v_old, v_new);
    v_definition := pg_get_functiondef('vela_claim_content_deletion_target(text,uuid,integer)'::regprocedure);
    v_old := 'AND target.state <> ''COMPLETED''';
    v_new := v_old || E'\n      AND NOT EXISTS (SELECT 1 FROM artifacts AS publication\n'
        || E'          WHERE publication.id = target.artifact_id\n'
        || E'            AND publication.source_stage_artifact_id IS NOT NULL\n'
        || E'            AND publication.state = ''STAGING'' AND publication.expires_at > v_claimed_at)';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'Public copy deletion claim drift'; END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE v_definition text; v_old text; v_new text;
BEGIN
    LOCK TABLE artifacts IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM artifacts AS artifact
               JOIN stage_artifacts AS source ON source.id = artifact.source_stage_artifact_id
               WHERE artifact.object_key <> source.object_key) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'public_artifact_copy_rollback_is_unsafe',
            MESSAGE = 'Job-owned public Artifact copies prevent rollback';
    END IF;
    v_definition := pg_get_functiondef('vela_prepare_stage_artifact_lifecycle(integer)'::regprocedure);
    v_old := 'WHERE visible.source_stage_artifact_id = artifact.id';
    v_new := v_old || E'\n                        AND visible.object_key = artifact.object_key';
    IF strpos(v_definition, v_new) = 0 THEN RAISE EXCEPTION 'StageArtifact public retention rollback drift'; END IF;
    EXECUTE replace(v_definition, v_new, v_old);
    v_definition := pg_get_functiondef('vela_claim_content_deletion_target(text,uuid,integer)'::regprocedure);
    v_old := 'AND target.state <> ''COMPLETED''';
    v_new := v_old || E'\n      AND NOT EXISTS (SELECT 1 FROM artifacts AS publication\n'
        || E'          WHERE publication.id = target.artifact_id\n'
        || E'            AND publication.source_stage_artifact_id IS NOT NULL\n'
        || E'            AND publication.state = ''STAGING'' AND publication.expires_at > v_claimed_at)';
    IF strpos(v_definition, v_new) = 0 THEN RAISE EXCEPTION 'Public copy deletion claim rollback drift'; END IF;
    EXECUTE replace(v_definition, v_new, v_old);
END
$$;
DROP INDEX artifacts_stage_source_idx;
DROP FUNCTION vela_stage_publication_deletion_identity(uuid);
REVOKE SELECT (expires_at) ON artifacts FROM vela_retention_owner;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_source_stage_artifact_id_key UNIQUE (source_stage_artifact_id);
-- +goose StatementEnd
