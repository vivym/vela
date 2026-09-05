-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_pin_cached_stage_output_consumers(p_stage_run_id uuid, p_output_port text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO public.stage_artifact_pins (
        id, stage_artifact_id, exact_object_version, pin_kind,
        owner_job_id, owner_stage_run_id, acquired_at
    )
    SELECT DISTINCT gen_random_uuid(), binding.stage_artifact_id, artifact.object_version,
        'EXECUTION'::public.stage_artifact_pin_kind,
        binding.job_id, dependency.destination_stage_run_id, binding.bound_at
    FROM public.stage_run_output_bindings AS binding
    JOIN public.stage_artifacts AS artifact ON artifact.id = binding.stage_artifact_id
    JOIN public.stage_dependencies AS dependency
      ON dependency.source_stage_run_id = binding.stage_run_id
     AND dependency.source_port = binding.output_port
    JOIN public.stage_runs AS destination ON destination.id = dependency.destination_stage_run_id
    JOIN public.jobs AS job ON job.id = binding.job_id
    WHERE binding.stage_run_id = p_stage_run_id AND binding.output_port = p_output_port
      AND binding.source_kind = 'EXACT_CACHE' AND artifact.state = 'COMMITTED'
      AND job.state IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
      AND destination.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
    ON CONFLICT DO NOTHING;

    INSERT INTO public.edge_buffer_credits (
        id, attempt_id, source_stage_run_id, destination_stage_run_id,
        destination_port, buffer_class, stage_artifact_id, held_bytes, acquired_at
    )
    SELECT gen_random_uuid(), binding.attempt_id, binding.stage_run_id,
        destination.id, dependency.destination_port, edge.buffer_class,
        artifact.id, artifact.size_bytes, binding.bound_at
    FROM public.stage_run_output_bindings AS binding
    JOIN public.stage_artifacts AS artifact ON artifact.id = binding.stage_artifact_id
    JOIN public.stage_runs AS source ON source.id = binding.stage_run_id
    JOIN public.stage_dependencies AS dependency
      ON dependency.source_stage_run_id = binding.stage_run_id
     AND dependency.source_port = binding.output_port
    JOIN public.stage_runs AS destination ON destination.id = dependency.destination_stage_run_id
    JOIN public.jobs AS job ON job.id = binding.job_id
    JOIN public.execution_graph_edges AS edge
      ON edge.execution_graph_revision_id = source.execution_graph_revision_id
     AND edge.source_stage_key = source.stage_key AND edge.source_port = binding.output_port
     AND edge.destination_stage_key = destination.stage_key
     AND edge.destination_port = dependency.destination_port
    WHERE binding.stage_run_id = p_stage_run_id AND binding.output_port = p_output_port
      AND binding.source_kind = 'EXACT_CACHE' AND artifact.state = 'COMMITTED'
      AND job.state IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
      AND destination.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
    ON CONFLICT (destination_stage_run_id, destination_port) DO NOTHING;
END
$$;
ALTER FUNCTION vela_pin_cached_stage_output_consumers(uuid, text) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_pin_cached_stage_output_consumers(uuid, text) FROM PUBLIC;

CREATE FUNCTION vela_pin_cached_stage_output_binding() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.source_kind = 'EXACT_CACHE' THEN
        PERFORM public.vela_pin_cached_stage_output_consumers(NEW.stage_run_id, NEW.output_port);
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_pin_cached_stage_output_binding() OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_pin_cached_stage_output_binding() FROM PUBLIC;
CREATE TRIGGER stage_run_output_bindings_pin_cached_consumers
AFTER INSERT ON stage_run_output_bindings
FOR EACH ROW EXECUTE FUNCTION vela_pin_cached_stage_output_binding();

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
    v_binding record;
BEGIN
    v_definition := pg_get_functiondef('public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure);
    v_old := E'        JOIN stage_artifacts AS artifact ON artifact.id = source.winner_stage_artifact_id\n';
    v_new := E'        JOIN stage_run_output_bindings AS binding\n'
        || E'          ON binding.stage_run_id = source.id AND binding.output_port = dependency.source_port\n'
        || E'        JOIN stage_artifacts AS artifact ON artifact.id = binding.stage_artifact_id\n';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <> length(v_old) THEN
        RAISE EXCEPTION 'Stage assignment input binding dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_commit_stage_artifact(jsonb)'::regprocedure);
    v_old := E'        source.winner_stage_artifact_id\n'
        || E'    FROM stage_dependencies AS dependency\n'
        || E'    JOIN stage_runs AS source ON source.id = dependency.source_stage_run_id\n'
        || E'    WHERE dependency.destination_stage_run_id = v_run.id\n'
        || E'      AND source.winner_stage_artifact_id IS NOT NULL;';
    v_new := E'        binding.stage_artifact_id\n'
        || E'    FROM stage_dependencies AS dependency\n'
        || E'    JOIN stage_run_output_bindings AS binding\n'
        || E'      ON binding.stage_run_id = dependency.source_stage_run_id\n'
        || E'     AND binding.output_port = dependency.source_port\n'
        || E'    WHERE dependency.destination_stage_run_id = v_run.id;';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <> length(v_old) THEN
        RAISE EXCEPTION 'StageArtifact lineage input binding dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    FOR v_binding IN SELECT stage_run_id, output_port FROM public.stage_run_output_bindings
        WHERE source_kind = 'EXACT_CACHE'
    LOOP
        PERFORM public.vela_pin_cached_stage_output_consumers(v_binding.stage_run_id, v_binding.output_port);
    END LOOP;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE v_definition text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure);
    EXECUTE replace(v_definition,
        E'        JOIN stage_run_output_bindings AS binding\n'
            || E'          ON binding.stage_run_id = source.id AND binding.output_port = dependency.source_port\n'
            || E'        JOIN stage_artifacts AS artifact ON artifact.id = binding.stage_artifact_id\n',
        E'        JOIN stage_artifacts AS artifact ON artifact.id = source.winner_stage_artifact_id\n');
    v_definition := pg_get_functiondef('public.vela_commit_stage_artifact(jsonb)'::regprocedure);
    EXECUTE replace(v_definition,
        E'        binding.stage_artifact_id\n'
            || E'    FROM stage_dependencies AS dependency\n'
            || E'    JOIN stage_run_output_bindings AS binding\n'
            || E'      ON binding.stage_run_id = dependency.source_stage_run_id\n'
            || E'     AND binding.output_port = dependency.source_port\n'
            || E'    WHERE dependency.destination_stage_run_id = v_run.id;',
        E'        source.winner_stage_artifact_id\n'
            || E'    FROM stage_dependencies AS dependency\n'
            || E'    JOIN stage_runs AS source ON source.id = dependency.source_stage_run_id\n'
            || E'    WHERE dependency.destination_stage_run_id = v_run.id\n'
            || E'      AND source.winner_stage_artifact_id IS NOT NULL;');
END
$$;
DROP TRIGGER stage_run_output_bindings_pin_cached_consumers ON stage_run_output_bindings;
DROP FUNCTION vela_pin_cached_stage_output_binding();
DROP FUNCTION vela_pin_cached_stage_output_consumers(uuid, text);
-- +goose StatementEnd
