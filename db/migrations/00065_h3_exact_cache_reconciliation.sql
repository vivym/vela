-- +goose Up
-- +goose StatementBegin
CREATE TABLE h3_exact_cache_admission_receipts (
    stage_artifact_id uuid PRIMARY KEY REFERENCES stage_artifacts(id),
    command_id uuid NOT NULL UNIQUE REFERENCES stage_cache_commands(command_id),
    stage_cache_entry_id uuid NOT NULL REFERENCES stage_cache_entries(id),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE h3_exact_cache_hit_cursors (
    action text PRIMARY KEY CHECK (action = 'HIT'),
    last_stage_run_id uuid,
    last_stage_profile_revision_id uuid,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (last_stage_run_id IS NULL) =
        (last_stage_profile_revision_id IS NULL)
    )
);
INSERT INTO h3_exact_cache_hit_cursors (action) VALUES ('HIT');

CREATE FUNCTION vela_reject_h3_exact_cache_admission_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'h3_exact_cache_admission_receipt_immutable',
        MESSAGE = 'H3 exact cache admission receipt is immutable';
END
$$;
ALTER FUNCTION vela_reject_h3_exact_cache_admission_receipt_mutation()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_reject_h3_exact_cache_admission_receipt_mutation()
    FROM PUBLIC;
CREATE TRIGGER h3_exact_cache_admission_receipts_immutable
BEFORE UPDATE OR DELETE ON h3_exact_cache_admission_receipts
FOR EACH ROW EXECUTE FUNCTION vela_reject_h3_exact_cache_admission_receipt_mutation();

CREATE FUNCTION vela_admit_h3_exact_cache_entry(p_command jsonb)
RETURNS TABLE (
    entry_id uuid, artifact_id uuid, object_version text,
    expires_at timestamptz, replayed boolean, deduplicated boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_requested_artifact_id uuid := (p_command ->> 'artifact_id')::uuid;
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_decision record;
    v_receipt h3_exact_cache_admission_receipts%ROWTYPE;
BEGIN
    IF v_requested_artifact_id IS NULL OR v_command_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'h3_exact_cache_admit_invalid',
            MESSAGE = 'H3 exact cache admission command is invalid';
    END IF;

    SELECT * INTO v_decision
    FROM vela_admit_stage_cache_entry(p_command);

    INSERT INTO h3_exact_cache_admission_receipts (
        stage_artifact_id, command_id, stage_cache_entry_id
    ) VALUES (
        v_requested_artifact_id, v_command_id, v_decision.entry_id
    )
    ON CONFLICT (stage_artifact_id) DO NOTHING;

    SELECT receipt.* INTO STRICT v_receipt
    FROM h3_exact_cache_admission_receipts AS receipt
    WHERE receipt.stage_artifact_id = v_requested_artifact_id;
    IF v_receipt.command_id <> v_command_id
       OR v_receipt.stage_cache_entry_id <> v_decision.entry_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'h3_exact_cache_admission_receipt_mismatch',
            MESSAGE = 'H3 exact cache admission receipt does not match replay';
    END IF;

    RETURN QUERY SELECT
        v_decision.entry_id,
        v_decision.artifact_id,
        v_decision.object_version,
        v_decision.expires_at,
        v_decision.replayed,
        v_decision.deduplicated;
END
$$;
ALTER FUNCTION vela_admit_h3_exact_cache_entry(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_admit_h3_exact_cache_entry(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_admit_h3_exact_cache_entry(jsonb)
    TO vela_attempt_coordinator;

CREATE FUNCTION vela_read_h3_exact_cache_candidates(
    p_action text,
    p_limit integer
) RETURNS SETOF jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cursor_stage_run_id uuid;
    v_cursor_profile_id uuid;
    v_candidate record;
    v_returned integer := 0;
BEGIN
    IF p_action NOT IN ('ADMIT', 'HIT') OR p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'h3_exact_cache_candidate_request_invalid',
            MESSAGE = 'H3 exact cache candidate request is invalid';
    END IF;

    IF p_action = 'ADMIT' THEN
        RETURN QUERY
        SELECT jsonb_build_object(
            'action', 'ADMIT',
            'organization_id', run.organization_id,
            'project_id', run.project_id,
            'attempt_id', run.attempt_id,
            'attempt_fence', attempt.fence,
            'stage_run_id', run.id,
            'stage_fence', run.fence,
            'stage_version', run.version,
            'stage_key', run.stage_key,
            'stage_kind', definition.stage_kind,
            'cache_policy_revision_id', policy.id,
            'cache_ttl_seconds', policy.ttl_seconds,
            'stage_profile_revision_id', profile.id,
            'result_equivalence_revision_id', profile.result_equivalence_revision_id,
            'stage_profile_content_digest', encode(profile.content_digest, 'hex'),
            'request_content', job.request_content,
            'request_hash', encode(job.request_hash, 'hex'),
            'dependency_digests', dependencies.digests,
            'artifact_id', artifact.id,
            'artifact_committed_at', artifact.committed_at,
            'artifact_expires_at', artifact.expires_at
        )
        FROM stage_runs AS run
        JOIN attempts AS attempt ON attempt.id = run.attempt_id
        JOIN jobs AS job ON job.id = attempt.job_id
        JOIN stage_definition_revisions AS definition
          ON definition.id = run.stage_definition_revision_id
        JOIN stage_cache_policy_revisions AS policy
          ON policy.id = definition.cache_policy_revision_id
        JOIN stage_attempts AS physical ON physical.id = run.winner_stage_attempt_id
        JOIN stage_profile_revisions AS profile
          ON profile.id = physical.selected_stage_profile_revision_id
        JOIN stage_result_equivalence_revisions AS equivalence
          ON equivalence.id = profile.result_equivalence_revision_id
        JOIN stage_artifacts AS artifact ON artifact.id = run.winner_stage_artifact_id
        JOIN project_stage_cache_controls AS control
          ON control.organization_id = run.organization_id
         AND control.project_id = run.project_id
         AND control.cache_policy_revision_id = policy.id
         AND control.enabled
        LEFT JOIN LATERAL (
            SELECT
                count(*) AS total,
                count(source_artifact.id) AS resolved,
                COALESCE(
                    jsonb_agg(encode(source_artifact.sha256, 'hex')
                              ORDER BY source.stage_key, dependency.source_port)
                        FILTER (WHERE source_artifact.id IS NOT NULL),
                    '[]'::jsonb
                ) AS digests
            FROM stage_dependencies AS dependency
            JOIN stage_runs AS source ON source.id = dependency.source_stage_run_id
            LEFT JOIN stage_run_output_bindings AS source_binding
              ON source_binding.attempt_id = dependency.attempt_id
             AND source_binding.stage_run_id = source.id
             AND source_binding.output_port = dependency.source_port
            LEFT JOIN stage_artifacts AS source_artifact
              ON source_artifact.id = source_binding.stage_artifact_id
             AND source_artifact.state = 'COMMITTED'
            WHERE dependency.destination_stage_run_id = run.id
        ) AS dependencies ON true
        WHERE run.state = 'SUCCEEDED'
          AND run.stage_key IN ('encoder', 'dit')
          AND definition.stage_kind IN ('ENCODER', 'DIT')
          AND policy.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          AND run.stage_key = ANY(policy.allowed_stage_keys)
          AND policy.scope_ceiling IN ('PROJECT', 'ORGANIZATION')
          AND equivalence.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          AND equivalence.exact_contract ->> 'rng' = 'exact'
          AND job.request_content_deleted_at IS NULL
          AND artifact.state = 'COMMITTED'
          AND artifact.expires_at > clock_timestamp()
          AND dependencies.total = dependencies.resolved
          AND NOT EXISTS (
              SELECT 1 FROM stage_cache_entries AS entry
              WHERE entry.stage_artifact_id = artifact.id
                AND entry.state IN ('LIVE', 'EVICTING')
          )
          AND NOT EXISTS (
              SELECT 1
              FROM h3_exact_cache_admission_receipts AS receipt
              WHERE receipt.stage_artifact_id = artifact.id
          )
        ORDER BY artifact.committed_at, run.id
        LIMIT p_limit;
        RETURN;
    END IF;

    SELECT cursor.last_stage_run_id, cursor.last_stage_profile_revision_id
    INTO v_cursor_stage_run_id, v_cursor_profile_id
    FROM h3_exact_cache_hit_cursors AS cursor
    WHERE cursor.action = 'HIT'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'h3_exact_cache_hit_cursor_missing',
            MESSAGE = 'H3 exact cache HIT cursor is missing';
    END IF;

    FOR v_candidate IN
        WITH candidates AS (
            SELECT
                run.id AS stage_run_id,
                profile.id AS stage_profile_revision_id,
                jsonb_build_object(
                    'action', 'HIT',
                    'organization_id', run.organization_id,
                    'project_id', run.project_id,
                    'attempt_id', run.attempt_id,
                    'attempt_fence', attempt.fence,
                    'stage_run_id', run.id,
                    'stage_fence', run.fence,
                    'stage_version', run.version,
                    'stage_key', run.stage_key,
                    'stage_kind', definition.stage_kind,
                    'cache_policy_revision_id', policy.id,
                    'cache_ttl_seconds', policy.ttl_seconds,
                    'stage_profile_revision_id', profile.id,
                    'result_equivalence_revision_id', profile.result_equivalence_revision_id,
                    'stage_profile_content_digest', encode(profile.content_digest, 'hex'),
                    'request_content', job.request_content,
                    'request_hash', encode(job.request_hash, 'hex'),
                    'dependency_digests', dependencies.digests
                ) AS payload
            FROM stage_runs AS run
            JOIN attempts AS attempt ON attempt.id = run.attempt_id
            JOIN jobs AS job ON job.id = attempt.job_id
            JOIN stage_definition_revisions AS definition
              ON definition.id = run.stage_definition_revision_id
            JOIN stage_cache_policy_revisions AS policy
              ON policy.id = definition.cache_policy_revision_id
            JOIN stage_profile_revisions AS profile
              ON profile.id = ANY(run.allowed_stage_profile_revision_ids)
            JOIN stage_result_equivalence_revisions AS equivalence
              ON equivalence.id = profile.result_equivalence_revision_id
            JOIN project_stage_cache_controls AS control
              ON control.organization_id = run.organization_id
             AND control.project_id = run.project_id
             AND control.cache_policy_revision_id = policy.id
             AND control.enabled
            LEFT JOIN LATERAL (
                SELECT
                    count(*) AS total,
                    count(source_artifact.id) AS resolved,
                    COALESCE(
                        jsonb_agg(encode(source_artifact.sha256, 'hex')
                                  ORDER BY source.stage_key, dependency.source_port)
                            FILTER (WHERE source_artifact.id IS NOT NULL),
                        '[]'::jsonb
                    ) AS digests
                FROM stage_dependencies AS dependency
                JOIN stage_runs AS source ON source.id = dependency.source_stage_run_id
                LEFT JOIN stage_run_output_bindings AS source_binding
                  ON source_binding.attempt_id = dependency.attempt_id
                 AND source_binding.stage_run_id = source.id
                 AND source_binding.output_port = dependency.source_port
                LEFT JOIN stage_artifacts AS source_artifact
                  ON source_artifact.id = source_binding.stage_artifact_id
                 AND source_artifact.state = 'COMMITTED'
                WHERE dependency.destination_stage_run_id = run.id
            ) AS dependencies ON true
            WHERE run.state = 'READY'
              AND run.stage_key IN ('encoder', 'dit')
              AND definition.stage_kind IN ('ENCODER', 'DIT')
              AND attempt.graph_state IN ('QUEUED', 'RUNNING')
              AND job.state IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
              AND policy.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
              AND run.stage_key = ANY(policy.allowed_stage_keys)
              AND equivalence.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
              AND equivalence.exact_contract ->> 'rng' = 'exact'
              AND job.request_content_deleted_at IS NULL
              AND dependencies.total = dependencies.resolved
        )
        SELECT candidate.*
        FROM candidates AS candidate
        ORDER BY
            CASE
                WHEN v_cursor_stage_run_id IS NULL
                  OR (candidate.stage_run_id, candidate.stage_profile_revision_id) >
                     (v_cursor_stage_run_id, v_cursor_profile_id)
                THEN 0 ELSE 1
            END,
            candidate.stage_run_id,
            candidate.stage_profile_revision_id
        LIMIT p_limit
    LOOP
        v_cursor_stage_run_id := v_candidate.stage_run_id;
        v_cursor_profile_id := v_candidate.stage_profile_revision_id;
        v_returned := v_returned + 1;
        RETURN NEXT v_candidate.payload;
    END LOOP;

    IF v_returned > 0 THEN
        UPDATE h3_exact_cache_hit_cursors
        SET last_stage_run_id = v_cursor_stage_run_id,
            last_stage_profile_revision_id = v_cursor_profile_id,
            updated_at = clock_timestamp()
        WHERE action = 'HIT';
    END IF;
END
$$;
ALTER FUNCTION vela_read_h3_exact_cache_candidates(text, integer)
    OWNER TO vela_attempt_coordinator_owner;
GRANT SELECT ON stage_result_equivalence_revisions
    TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_h3_exact_cache_candidates(text, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_h3_exact_cache_candidates(text, integer)
    TO vela_attempt_coordinator;

CREATE FUNCTION vela_find_h3_exact_cache_entry(
    p_organization_id uuid,
    p_project_id uuid,
    p_cache_policy_revision_id uuid,
    p_stage_profile_revision_id uuid,
    p_result_equivalence_revision_id uuid,
    p_stage_key text,
    p_cache_key_digest bytea,
    p_observed_at timestamptz
) RETURNS TABLE (entry_id uuid)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT entry.id
    FROM stage_cache_entries AS entry
    WHERE entry.organization_id = p_organization_id
      AND entry.scope = 'PROJECT'
      AND entry.scope_project_id = p_project_id
      AND entry.cache_policy_revision_id = p_cache_policy_revision_id
      AND entry.stage_profile_revision_id = p_stage_profile_revision_id
      AND entry.result_equivalence_revision_id = p_result_equivalence_revision_id
      AND entry.stage_key = p_stage_key
      AND entry.cache_key_digest = p_cache_key_digest
      AND entry.state = 'LIVE'
      AND entry.expires_at > p_observed_at
    ORDER BY entry.admitted_at, entry.id
    LIMIT 1
$$;
ALTER FUNCTION vela_find_h3_exact_cache_entry(
    uuid, uuid, uuid, uuid, uuid, text, bytea, timestamptz
) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_find_h3_exact_cache_entry(
    uuid, uuid, uuid, uuid, uuid, text, bytea, timestamptz
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_find_h3_exact_cache_entry(
    uuid, uuid, uuid, uuid, uuid, text, bytea, timestamptz
) TO vela_attempt_coordinator;

GRANT SELECT, INSERT ON h3_exact_cache_admission_receipts
    TO vela_attempt_coordinator_owner;
GRANT SELECT, UPDATE ON h3_exact_cache_hit_cursors
    TO vela_attempt_coordinator_owner;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM h3_exact_cache_admission_receipts) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'h3_exact_cache_reconciliation_rollback_unsafe',
            MESSAGE = 'cannot remove durable H3 exact cache admission receipts';
    END IF;
END
$$;
REVOKE EXECUTE ON FUNCTION vela_find_h3_exact_cache_entry(
    uuid, uuid, uuid, uuid, uuid, text, bytea, timestamptz
) FROM vela_attempt_coordinator;
DROP FUNCTION vela_find_h3_exact_cache_entry(
    uuid, uuid, uuid, uuid, uuid, text, bytea, timestamptz
);
REVOKE EXECUTE ON FUNCTION vela_read_h3_exact_cache_candidates(text, integer)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_read_h3_exact_cache_candidates(text, integer);
REVOKE SELECT ON stage_result_equivalence_revisions
    FROM vela_attempt_coordinator_owner;
REVOKE EXECUTE ON FUNCTION vela_admit_h3_exact_cache_entry(jsonb)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_admit_h3_exact_cache_entry(jsonb);
DROP TRIGGER h3_exact_cache_admission_receipts_immutable
    ON h3_exact_cache_admission_receipts;
DROP FUNCTION vela_reject_h3_exact_cache_admission_receipt_mutation();
DROP TABLE h3_exact_cache_hit_cursors;
DROP TABLE h3_exact_cache_admission_receipts;
-- +goose StatementEnd
