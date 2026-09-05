WITH selected_jobs AS (
    SELECT * FROM jobs WHERE id IN ($1::uuid, $2::uuid)
), source_entries AS (
    SELECT entry.*, receipt.command_id
    FROM stage_cache_entries AS entry
    JOIN h3_exact_cache_admission_receipts AS receipt
      ON receipt.stage_cache_entry_id = entry.id
     AND receipt.stage_artifact_id = entry.stage_artifact_id
    WHERE entry.source_job_id = $1
      AND entry.stage_key IN ('encoder', 'dit')
      AND entry.scope = 'PROJECT' AND entry.scope_project_id = entry.source_project_id
      AND entry.state = 'LIVE' AND entry.expires_at > clock_timestamp()
), target_hits AS (
    SELECT run.stage_key, binding.stage_artifact_id, artifact.object_version,
           entry.id AS cache_entry_id, entry.hit_count, pin.id AS pin_id,
           reference.id AS reference_id
    FROM stage_runs AS run
    JOIN attempts AS attempt ON attempt.id = run.attempt_id
    JOIN selected_jobs AS job ON job.id = attempt.job_id AND job.id = $2
    JOIN stage_run_output_bindings AS binding
      ON binding.stage_run_id = run.id AND binding.source_kind = 'EXACT_CACHE'
    JOIN stage_cache_references AS reference
      ON reference.id = binding.stage_cache_reference_id
     AND reference.owner_job_id = job.id AND reference.owner_stage_run_id = run.id
     AND reference.stage_artifact_id = binding.stage_artifact_id
    JOIN source_entries AS entry
      ON entry.id = reference.stage_cache_entry_id AND entry.stage_key = run.stage_key
     AND entry.organization_id = job.organization_id AND entry.scope_project_id = job.project_id
    JOIN stage_artifacts AS artifact
      ON artifact.id = entry.stage_artifact_id AND artifact.id = binding.stage_artifact_id
     AND artifact.object_version = entry.exact_object_version AND artifact.sha256 = entry.sha256
    JOIN stage_artifact_pins AS pin
      ON pin.id = reference.execution_pin_id AND pin.stage_artifact_id = artifact.id
     AND pin.owner_job_id = job.id AND pin.owner_stage_run_id = run.id
    WHERE run.stage_key IN ('encoder', 'dit') AND run.state = 'SUCCEEDED'
      AND entry.hit_count >= 1
), job_results AS (
    SELECT job.id, job.state,
           (SELECT count(*) FROM attempts WHERE job_id = job.id) AS attempts,
           (SELECT count(*) FROM stage_runs AS run
            JOIN attempts AS attempt ON attempt.id = run.attempt_id
            WHERE attempt.job_id = job.id) AS stages,
           (SELECT count(*) FROM stage_runs AS run
            JOIN attempts AS attempt ON attempt.id = run.attempt_id
            WHERE attempt.job_id = job.id AND run.state = 'SUCCEEDED') AS succeeded_stages,
           (SELECT count(*) FROM stage_attempts AS physical
            JOIN stage_runs AS run ON run.id = physical.stage_run_id
            JOIN attempts AS attempt ON attempt.id = run.attempt_id
            WHERE attempt.job_id = job.id AND run.stage_key IN ('encoder', 'dit')) AS cache_stage_executions,
           (SELECT count(*) FROM visible_completions WHERE job_id = job.id) AS completions,
           (SELECT count(*) FROM charges
            WHERE job_id = job.id AND amount_minor = job.pricing_quoted_amount_minor
              AND currency = job.pricing_currency) AS fixed_price_charges
    FROM selected_jobs AS job
)
SELECT jsonb_build_object(
    'schema_version', 1,
    'environment', 'non-production-lab',
    'production_gate_evidence', false,
    'source_job_id', $1::uuid,
    'target_job_id', $2::uuid,
    'database_time', clock_timestamp(),
    'database_snapshot', pg_current_snapshot()::text,
    'source_ready', EXISTS (
        SELECT 1 FROM job_results WHERE id = $1 AND state = 'SUCCEEDED'
          AND attempts = 1 AND stages = succeeded_stages AND stages >= 3
          AND cache_stage_executions = 2 AND completions = 1 AND fixed_price_charges = 1
    ) AND (SELECT count(DISTINCT stage_key) FROM source_entries) = 2,
    'equivalent_requests', EXISTS (
        SELECT 1 FROM selected_jobs AS source JOIN selected_jobs AS target
          ON target.id = $2 AND source.id = $1 AND source.id <> target.id
         AND source.organization_id = target.organization_id AND source.project_id = target.project_id
         AND source.request_hash = target.request_hash
         AND source.pricing_quoted_amount_minor = target.pricing_quoted_amount_minor
         AND source.pricing_currency = target.pricing_currency
    ),
    'target_reused', EXISTS (
        SELECT 1 FROM job_results WHERE id = $2 AND state = 'SUCCEEDED'
          AND attempts = 1 AND stages = succeeded_stages AND stages >= 3
          AND cache_stage_executions = 0 AND completions = 1 AND fixed_price_charges = 1
    ) AND (SELECT count(DISTINCT stage_key) FROM target_hits) = 2,
    'jobs', (SELECT COALESCE(jsonb_agg(to_jsonb(result) ORDER BY result.id), '[]') FROM job_results AS result),
    'hits', (SELECT COALESCE(jsonb_agg(to_jsonb(hit) ORDER BY hit.stage_key), '[]') FROM target_hits AS hit),
    'usage_ledger_records', (SELECT count(*) FROM resource_usage_records WHERE job_id IN ($1, $2))
);
