-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_private.vela_begin_stage_graph_finalization(
    p_attempt_id uuid,
    p_stage_run_id uuid,
    p_completed_at timestamptz
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_graph_revision_id uuid;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.stage_dependencies AS outgoing
        WHERE outgoing.attempt_id = p_attempt_id
          AND outgoing.source_stage_run_id = p_stage_run_id
    ) THEN
        RETURN;
    END IF;

    SELECT run.execution_graph_revision_id INTO v_graph_revision_id
    FROM public.stage_runs AS run
    WHERE run.attempt_id = p_attempt_id
      AND run.id = p_stage_run_id
      AND run.state = 'SUCCEEDED';
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT attempt.* INTO STRICT v_attempt
    FROM public.attempts AS attempt
    WHERE attempt.id = p_attempt_id;
    SELECT job.* INTO STRICT v_job
    FROM public.jobs AS job
    WHERE job.id = v_attempt.job_id;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS graph_stage
        LEFT JOIN public.stage_runs AS run
          ON run.attempt_id = v_attempt.id
         AND run.execution_graph_revision_id = graph_stage.execution_graph_revision_id
         AND run.stage_key = graph_stage.stage_key
        WHERE graph_stage.execution_graph_revision_id = v_graph_revision_id
          AND graph_stage.required
          AND (run.id IS NULL OR run.state <> 'SUCCEEDED')
    ) OR EXISTS (
        SELECT 1
        FROM public.execution_graph_outputs AS graph_output
        LEFT JOIN public.stage_runs AS run
          ON run.attempt_id = v_attempt.id
         AND run.execution_graph_revision_id = graph_output.execution_graph_revision_id
         AND run.stage_key = graph_output.source_stage_key
        LEFT JOIN public.stage_artifacts AS artifact
          ON artifact.id = run.winner_stage_artifact_id
         AND artifact.producer_stage_run_id = run.id
         AND artifact.output_port = graph_output.source_port
         AND artifact.stage_interface_revision_id = graph_output.interface_revision_id
         AND artifact.state = 'COMMITTED'
        WHERE graph_output.execution_graph_revision_id = v_graph_revision_id
          AND graph_output.required
          AND artifact.id IS NULL
    ) THEN
        RETURN;
    END IF;

    UPDATE public.attempts AS attempt
    SET state = 'FINALIZING', graph_state = 'FINALIZING',
        finalization_started_at = p_completed_at,
        finalization_deadline_at = p_completed_at
            + make_interval(secs => v_job.execution_max_finalization_seconds_per_attempt),
        updated_at = p_completed_at
    WHERE attempt.id = v_attempt.id
      AND attempt.state = 'RUNNING'
      AND attempt.graph_state = 'RUNNING';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_final_output_transition_stale',
            MESSAGE = 'Parent Attempt changed before finalization';
    END IF;

    UPDATE public.jobs AS job
    SET state = 'FINALIZING', version = job.version + 1,
        updated_at = p_completed_at
    WHERE job.id = v_job.id
      AND job.state = 'RUNNING'
      AND job.current_fence = v_attempt.fence;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_final_output_transition_stale',
            MESSAGE = 'Job changed before finalization';
    END IF;
END
$$;

GRANT SELECT ON execution_graph_outputs TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_complete_stage_graph_visible_completion_attempt(
    p_attempt_id uuid,
    p_attempt_fence bigint,
    p_completed_at timestamptz
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    UPDATE public.attempts AS attempt
    SET state = 'SUCCEEDED',
        graph_state = 'SUCCEEDED',
        ended_at = p_completed_at,
        updated_at = p_completed_at
    WHERE attempt.id = p_attempt_id
      AND attempt.worker_id IS NULL
      AND attempt.fence = p_attempt_fence
      AND attempt.execution_authority_kind = 'STAGE_GRAPH'
      AND attempt.state = 'FINALIZING'
      AND attempt.graph_state = 'FINALIZING'
      AND attempt.ended_at IS NULL;
    RETURN FOUND;
END
$$;
ALTER FUNCTION vela_complete_stage_graph_visible_completion_attempt(
    uuid, bigint, timestamptz
) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_complete_stage_graph_visible_completion_attempt(
    uuid, bigint, timestamptz
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_complete_stage_graph_visible_completion_attempt(
    uuid, bigint, timestamptz
) TO vela_internal;

ALTER TABLE stage_artifacts
    ADD CONSTRAINT stage_artifacts_visible_completion_source_identity
    UNIQUE (organization_id, project_id, attempt_id, id);
ALTER TABLE artifacts
    ADD COLUMN source_stage_artifact_id uuid UNIQUE,
    ADD CONSTRAINT artifacts_source_stage_artifact_identity
        FOREIGN KEY (
            organization_id, project_id, attempt_id,
            source_stage_artifact_id
        ) REFERENCES stage_artifacts(
            organization_id, project_id, attempt_id, id
        );

CREATE FUNCTION vela_reject_artifact_source_stage_artifact_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'artifact_source_stage_artifact_immutable',
        MESSAGE = 'Artifact StageArtifact provenance is immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_artifact_source_stage_artifact_mutation()
    FROM PUBLIC;
ALTER FUNCTION vela_reject_artifact_source_stage_artifact_mutation()
    OWNER TO vela_internal;
CREATE TRIGGER artifacts_source_stage_artifact_immutable
BEFORE UPDATE OF source_stage_artifact_id ON artifacts
FOR EACH ROW
WHEN (OLD.source_stage_artifact_id IS DISTINCT FROM NEW.source_stage_artifact_id)
EXECUTE FUNCTION vela_reject_artifact_source_stage_artifact_mutation();

ALTER TABLE visible_completions
    ALTER COLUMN authority_lease_id DROP NOT NULL,
    ADD COLUMN authority_stage_graph_finalization_claim_id uuid UNIQUE,
    ADD CONSTRAINT visible_completions_stage_graph_claim_authority
        FOREIGN KEY (
            organization_id, project_id,
            authority_stage_graph_finalization_claim_id
        ) REFERENCES stage_graph_finalization_claims(
            organization_id, project_id, id
        ),
    ADD CONSTRAINT visible_completions_exactly_one_authority CHECK (
        num_nonnulls(
            authority_lease_id,
            authority_stage_graph_finalization_claim_id
        ) = 1
    );
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE visible_completions, artifacts IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM visible_completions
        WHERE authority_stage_graph_finalization_claim_id IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM artifacts
        WHERE source_stage_artifact_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_visible_completion_rollback_is_unsafe',
            MESSAGE = 'Migration 00046 cannot contract while Stage graph Visible Completion evidence remains';
    END IF;
END
$$;

ALTER TABLE visible_completions
    DROP CONSTRAINT visible_completions_exactly_one_authority,
    DROP CONSTRAINT visible_completions_stage_graph_claim_authority,
    DROP COLUMN authority_stage_graph_finalization_claim_id,
    ALTER COLUMN authority_lease_id SET NOT NULL;

DROP TRIGGER artifacts_source_stage_artifact_immutable ON artifacts;
DROP FUNCTION vela_reject_artifact_source_stage_artifact_mutation();
ALTER TABLE artifacts DROP COLUMN source_stage_artifact_id;
ALTER TABLE stage_artifacts
    DROP CONSTRAINT stage_artifacts_visible_completion_source_identity;

REVOKE EXECUTE ON FUNCTION vela_complete_stage_graph_visible_completion_attempt(
    uuid, bigint, timestamptz
) FROM vela_internal;
DROP FUNCTION vela_complete_stage_graph_visible_completion_attempt(
    uuid, bigint, timestamptz
);

REVOKE SELECT ON execution_graph_outputs FROM vela_attempt_coordinator_owner;

CREATE OR REPLACE FUNCTION vela_private.vela_begin_stage_graph_finalization(
    p_attempt_id uuid,
    p_stage_run_id uuid,
    p_completed_at timestamptz
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.stage_dependencies AS outgoing
        WHERE outgoing.attempt_id = p_attempt_id
          AND outgoing.source_stage_run_id = p_stage_run_id
    ) THEN
        RETURN;
    END IF;

    SELECT attempt.* INTO STRICT v_attempt
    FROM public.attempts AS attempt
    WHERE attempt.id = p_attempt_id;
    SELECT job.* INTO STRICT v_job
    FROM public.jobs AS job
    WHERE job.id = v_attempt.job_id;

    UPDATE public.attempts AS attempt
    SET state = 'FINALIZING', graph_state = 'FINALIZING',
        finalization_started_at = p_completed_at,
        finalization_deadline_at = p_completed_at
            + make_interval(secs => v_job.execution_max_finalization_seconds_per_attempt),
        updated_at = p_completed_at
    WHERE attempt.id = v_attempt.id
      AND attempt.state = 'RUNNING'
      AND attempt.graph_state = 'RUNNING';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_final_output_transition_stale',
            MESSAGE = 'Parent Attempt changed before finalization';
    END IF;

    UPDATE public.jobs AS job
    SET state = 'FINALIZING', version = job.version + 1,
        updated_at = p_completed_at
    WHERE job.id = v_job.id
      AND job.state = 'RUNNING'
      AND job.current_fence = v_attempt.fence;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_final_output_transition_stale',
            MESSAGE = 'Job changed before finalization';
    END IF;
END
$$;
-- +goose StatementEnd
