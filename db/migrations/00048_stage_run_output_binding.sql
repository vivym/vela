-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_run_output_source_kind AS ENUM ('PHYSICAL', 'EXACT_CACHE');

ALTER TABLE stage_cache_references
    ADD CONSTRAINT stage_cache_references_output_binding_identity
    UNIQUE (id, stage_artifact_id, owner_job_id, owner_stage_run_id);

CREATE TABLE stage_run_output_bindings (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    stage_run_id uuid NOT NULL,
    output_port text NOT NULL CHECK (
        length(output_port) BETWEEN 1 AND 100
        AND output_port ~ '^[a-z][a-z0-9_-]*$'
    ),
    stage_interface_revision_id uuid NOT NULL
        REFERENCES stage_interface_revisions(id),
    stage_artifact_id uuid NOT NULL REFERENCES stage_artifacts(id),
    source_kind stage_run_output_source_kind NOT NULL,
    physical_producer_stage_run_id uuid,
    stage_cache_reference_id uuid,
    bound_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stage_run_id, output_port),
    UNIQUE (stage_run_id, stage_artifact_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (attempt_id, stage_run_id)
        REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (physical_producer_stage_run_id, stage_artifact_id)
        REFERENCES stage_artifacts(producer_stage_run_id, id),
    FOREIGN KEY (
        stage_cache_reference_id, stage_artifact_id, job_id, stage_run_id
    ) REFERENCES stage_cache_references(
        id, stage_artifact_id, owner_job_id, owner_stage_run_id
    ),
    CHECK (
        (
            source_kind = 'PHYSICAL'
            AND physical_producer_stage_run_id = stage_run_id
            AND stage_cache_reference_id IS NULL
        ) OR (
            source_kind = 'EXACT_CACHE'
            AND physical_producer_stage_run_id IS NULL
            AND stage_cache_reference_id IS NOT NULL
        )
    )
);

INSERT INTO stage_run_output_bindings (
    organization_id, project_id, job_id, attempt_id, stage_run_id,
    output_port, stage_interface_revision_id, stage_artifact_id,
    source_kind, physical_producer_stage_run_id, bound_at
)
SELECT
    job.organization_id, job.project_id, job.id, attempt.id, run.id,
    artifact.output_port, artifact.stage_interface_revision_id, artifact.id,
    'PHYSICAL', run.id, artifact.committed_at
FROM stage_runs AS run
JOIN attempts AS attempt ON attempt.id = run.attempt_id
JOIN jobs AS job ON job.id = attempt.job_id
JOIN stage_artifacts AS artifact
  ON artifact.id = run.winner_stage_artifact_id
 AND artifact.producer_stage_run_id = run.id;

DO $$
DECLARE
    v_constraint name;
BEGIN
    SELECT constraint_row.conname INTO STRICT v_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'stage_graph_finalization_claims'::regclass
      AND constraint_row.contype = 'f'
      AND pg_get_constraintdef(constraint_row.oid) LIKE
          'FOREIGN KEY (final_stage_run_id, final_stage_artifact_id) REFERENCES stage_artifacts%';
    EXECUTE format(
        'ALTER TABLE stage_graph_finalization_claims DROP CONSTRAINT %I',
        v_constraint
    );

    SELECT constraint_row.conname INTO STRICT v_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'stage_graph_finalization_claim_outputs'::regclass
      AND constraint_row.contype = 'f'
      AND pg_get_constraintdef(constraint_row.oid) LIKE
          'FOREIGN KEY (stage_run_id, stage_artifact_id) REFERENCES stage_artifacts%';
    EXECUTE format(
        'ALTER TABLE stage_graph_finalization_claim_outputs DROP CONSTRAINT %I',
        v_constraint
    );

    SELECT constraint_row.conname INTO STRICT v_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'artifacts'::regclass
      AND constraint_row.contype = 'f'
      AND pg_get_constraintdef(constraint_row.oid) LIKE
          'FOREIGN KEY (organization_id, project_id, attempt_id, source_stage_artifact_id)%';
    EXECUTE format('ALTER TABLE artifacts DROP CONSTRAINT %I', v_constraint);
END
$$;

ALTER TABLE stage_graph_finalization_claims
    ADD CONSTRAINT stage_graph_finalization_claims_output_binding_fk
    FOREIGN KEY (final_stage_run_id, final_stage_artifact_id)
    REFERENCES stage_run_output_bindings(stage_run_id, stage_artifact_id);
ALTER TABLE stage_graph_finalization_claim_outputs
    ADD CONSTRAINT stage_graph_finalization_claim_outputs_output_binding_fk
    FOREIGN KEY (stage_run_id, stage_artifact_id)
    REFERENCES stage_run_output_bindings(stage_run_id, stage_artifact_id);

ALTER TABLE stage_artifacts
    ADD CONSTRAINT stage_artifacts_organization_artifact_identity
    UNIQUE (organization_id, id);
ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_source_stage_artifact_organization_fk
    FOREIGN KEY (organization_id, source_stage_artifact_id)
    REFERENCES stage_artifacts(organization_id, id);

CREATE FUNCTION vela_reject_stage_run_output_binding_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'stage_run_output_binding_immutable',
        MESSAGE = 'StageRun output binding evidence is immutable';
END
$$;
ALTER FUNCTION vela_reject_stage_run_output_binding_mutation()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_reject_stage_run_output_binding_mutation() FROM PUBLIC;
CREATE TRIGGER stage_run_output_bindings_immutable
BEFORE UPDATE OR DELETE ON stage_run_output_bindings
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_run_output_binding_mutation();

CREATE FUNCTION vela_private.vela_bind_physical_stage_run_output()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_artifact public.stage_artifacts%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_job public.jobs%ROWTYPE;
BEGIN
    IF NEW.winner_stage_artifact_id IS NULL
       OR NEW.winner_stage_artifact_id IS NOT DISTINCT FROM OLD.winner_stage_artifact_id
    THEN
        RETURN NEW;
    END IF;
    SELECT artifact.* INTO STRICT v_artifact
    FROM public.stage_artifacts AS artifact
    WHERE artifact.id = NEW.winner_stage_artifact_id
      AND artifact.producer_stage_run_id = NEW.id;
    SELECT attempt.* INTO STRICT v_attempt
    FROM public.attempts AS attempt WHERE attempt.id = NEW.attempt_id;
    SELECT job.* INTO STRICT v_job
    FROM public.jobs AS job WHERE job.id = v_attempt.job_id;

    INSERT INTO public.stage_run_output_bindings (
        organization_id, project_id, job_id, attempt_id, stage_run_id,
        output_port, stage_interface_revision_id, stage_artifact_id,
        source_kind, physical_producer_stage_run_id, bound_at
    ) VALUES (
        v_job.organization_id, v_job.project_id, v_job.id, v_attempt.id, NEW.id,
        v_artifact.output_port, v_artifact.stage_interface_revision_id,
        v_artifact.id, 'PHYSICAL', NEW.id, v_artifact.committed_at
    );
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_private.vela_bind_physical_stage_run_output()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_private.vela_bind_physical_stage_run_output() FROM PUBLIC;
CREATE TRIGGER stage_runs_bind_physical_output
AFTER UPDATE OF winner_stage_artifact_id ON stage_runs
FOR EACH ROW EXECUTE FUNCTION vela_private.vela_bind_physical_stage_run_output();

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
    FROM public.attempts AS attempt WHERE attempt.id = p_attempt_id;
    SELECT job.* INTO STRICT v_job
    FROM public.jobs AS job WHERE job.id = v_attempt.job_id;

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
        LEFT JOIN public.stage_run_output_bindings AS binding
          ON binding.attempt_id = v_attempt.id
         AND binding.stage_run_id = run.id
         AND binding.output_port = graph_output.source_port
         AND binding.stage_interface_revision_id = graph_output.interface_revision_id
        LEFT JOIN public.stage_artifacts AS artifact
          ON artifact.id = binding.stage_artifact_id
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

CREATE FUNCTION vela_private.vela_bind_cached_stage_run_output()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_artifact public.stage_artifacts%ROWTYPE;
    v_run public.stage_runs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_job public.jobs%ROWTYPE;
BEGIN
    IF NEW.owner_stage_run_id IS NULL OR NEW.execution_pin_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT artifact.* INTO STRICT v_artifact
    FROM public.stage_artifacts AS artifact
    WHERE artifact.id = NEW.stage_artifact_id
      AND artifact.state = 'COMMITTED';
    SELECT run.* INTO STRICT v_run
    FROM public.stage_runs AS run WHERE run.id = NEW.owner_stage_run_id;
    SELECT attempt.* INTO STRICT v_attempt
    FROM public.attempts AS attempt WHERE attempt.id = v_run.attempt_id;
    SELECT job.* INTO STRICT v_job
    FROM public.jobs AS job
    WHERE job.id = v_attempt.job_id AND job.id = NEW.owner_job_id;

    INSERT INTO public.stage_run_output_bindings (
        organization_id, project_id, job_id, attempt_id, stage_run_id,
        output_port, stage_interface_revision_id, stage_artifact_id,
        source_kind, stage_cache_reference_id, bound_at
    ) VALUES (
        v_job.organization_id, v_job.project_id, v_job.id, v_attempt.id, v_run.id,
        v_artifact.output_port, v_artifact.stage_interface_revision_id,
        v_artifact.id, 'EXACT_CACHE', NEW.id, NEW.acquired_at
    );
    PERFORM vela_private.vela_begin_stage_graph_finalization(
        v_attempt.id, v_run.id, NEW.acquired_at
    );
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_private.vela_bind_cached_stage_run_output()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_private.vela_bind_cached_stage_run_output() FROM PUBLIC;
CREATE TRIGGER stage_cache_references_bind_output
AFTER INSERT ON stage_cache_references
FOR EACH ROW EXECUTE FUNCTION vela_private.vela_bind_cached_stage_run_output();

ALTER TABLE stage_run_output_bindings OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE stage_run_output_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_run_output_bindings FORCE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE stage_run_output_bindings FROM PUBLIC;
GRANT SELECT ON stage_run_output_bindings TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE stage_run_output_bindings, stage_graph_finalization_claims,
    stage_graph_finalization_claim_outputs, artifacts IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM stage_run_output_bindings
        WHERE source_kind = 'EXACT_CACHE'
    ) OR EXISTS (
        SELECT 1
        FROM artifacts AS artifact
        JOIN stage_artifacts AS source
          ON source.id = artifact.source_stage_artifact_id
        WHERE artifact.source_stage_artifact_id IS NOT NULL
          AND (
              source.project_id <> artifact.project_id
              OR source.attempt_id <> artifact.attempt_id
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_run_output_binding_rollback_is_unsafe',
            MESSAGE = 'Migration 00048 cannot remove exact-cache StageRun output evidence';
    END IF;
END
$$;

DROP TRIGGER stage_cache_references_bind_output ON stage_cache_references;
DROP FUNCTION vela_private.vela_bind_cached_stage_run_output();
DROP TRIGGER stage_runs_bind_physical_output ON stage_runs;
DROP FUNCTION vela_private.vela_bind_physical_stage_run_output();

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
        SELECT 1 FROM public.stage_dependencies AS outgoing
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
    FROM public.attempts AS attempt WHERE attempt.id = p_attempt_id;
    SELECT job.* INTO STRICT v_job
    FROM public.jobs AS job WHERE job.id = v_attempt.job_id;
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
            ERRCODE = '40001', CONSTRAINT = 'stage_final_output_transition_stale',
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
            ERRCODE = '40001', CONSTRAINT = 'stage_final_output_transition_stale',
            MESSAGE = 'Job changed before finalization';
    END IF;
END
$$;

ALTER TABLE stage_graph_finalization_claims
    DROP CONSTRAINT stage_graph_finalization_claims_output_binding_fk,
    ADD CONSTRAINT stage_graph_finalization_claims_final_output_fk
    FOREIGN KEY (final_stage_run_id, final_stage_artifact_id)
    REFERENCES stage_artifacts(producer_stage_run_id, id);
ALTER TABLE stage_graph_finalization_claim_outputs
    DROP CONSTRAINT stage_graph_finalization_claim_outputs_output_binding_fk,
    ADD CONSTRAINT stage_graph_finalization_claim_outputs_final_output_fk
    FOREIGN KEY (stage_run_id, stage_artifact_id)
    REFERENCES stage_artifacts(producer_stage_run_id, id);

ALTER TABLE artifacts
    DROP CONSTRAINT artifacts_source_stage_artifact_organization_fk,
    ADD CONSTRAINT artifacts_source_stage_artifact_identity
    FOREIGN KEY (
        organization_id, project_id, attempt_id, source_stage_artifact_id
    ) REFERENCES stage_artifacts(
        organization_id, project_id, attempt_id, id
    );
ALTER TABLE stage_artifacts
    DROP CONSTRAINT stage_artifacts_organization_artifact_identity;

DROP TRIGGER stage_run_output_bindings_immutable ON stage_run_output_bindings;
DROP FUNCTION vela_reject_stage_run_output_binding_mutation();
DROP TABLE stage_run_output_bindings;
ALTER TABLE stage_cache_references
    DROP CONSTRAINT stage_cache_references_output_binding_identity;
DROP TYPE stage_run_output_source_kind;
-- +goose StatementEnd
