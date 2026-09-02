-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_read_h3_exact_cache_candidates(
    p_action text,
    p_limit integer
) RETURNS SETOF jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
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
        ORDER BY artifact.committed_at, run.id
        LIMIT p_limit;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT jsonb_build_object(
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
    )
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
    ORDER BY run.updated_at, run.id,
             array_position(run.allowed_stage_profile_revision_ids, profile.id)
    LIMIT p_limit;
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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
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
-- +goose StatementEnd
