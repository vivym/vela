-- +goose Up
-- +goose StatementBegin
CREATE INDEX capacity_observations_epoch_sequence_idx
    ON capacity_observations(worker_instance_id, worker_instance_epoch, observation_sequence DESC);

-- Capacity leases are transient. Keep every live lease and each epoch/source's
-- highest sequence, even after expiry, so cleanup cannot revive stale capacity
-- or undo the Stage Worker's takeover from Node Agent capacity reporting.
CREATE FUNCTION vela_prune_worker_capacity_observations(p_worker_instance_id uuid)
RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_deleted integer;
BEGIN
    WITH expired AS (
        SELECT observation.worker_instance_id, observation.observation_sequence
        FROM public.capacity_observations AS observation
        WHERE observation.worker_instance_id = p_worker_instance_id
          AND observation.expires_at <= v_now
          AND EXISTS (
              SELECT 1 FROM public.capacity_observations AS newer
              WHERE newer.worker_instance_id = observation.worker_instance_id
                AND newer.worker_instance_epoch = observation.worker_instance_epoch
                AND newer.observation_sequence > observation.observation_sequence
                AND (newer.observation_sequence::numeric > newer.worker_instance_epoch::numeric * 4294967296
                     AND newer.observation_sequence::numeric <= newer.worker_instance_epoch::numeric * 4294967296 + 4294967295)
                    = (observation.observation_sequence::numeric > observation.worker_instance_epoch::numeric * 4294967296
                       AND observation.observation_sequence::numeric <= observation.worker_instance_epoch::numeric * 4294967296 + 4294967295)
          )
        ORDER BY observation.expires_at, observation.observation_sequence
        LIMIT 256
    )
    DELETE FROM public.capacity_observations AS observation
    USING expired
    WHERE observation.worker_instance_id = expired.worker_instance_id
      AND observation.observation_sequence = expired.observation_sequence;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END
$$;
ALTER FUNCTION vela_prune_worker_capacity_observations(uuid) OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_prune_worker_capacity_observations(uuid) FROM PUBLIC;

-- Both entrypoints already hold the Worker row lock before these positions.
-- Preserve their identities and ACLs, and make rejection roll back maintenance.
DO $migration$
DECLARE
    v_signature text;
    v_definition text;
    v_marker text;
    v_call text := E'    PERFORM public.vela_prune_worker_capacity_observations(v_worker.id);\n';
BEGIN
    FOREACH v_signature IN ARRAY ARRAY[
        'vela_observe_worker_instance(jsonb)',
        'vela_report_stage_worker_capacity_v66(jsonb)'
    ] LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        v_marker := CASE v_signature
            WHEN 'vela_observe_worker_instance(jsonb)'
                THEN E'    v_capacity := p_evidence -> ''capacity'';\n'
            ELSE E'    v_observed_by := ''stage-worker-control/'' || v_member_id::text;\n'
        END;
        IF strpos(v_definition, v_marker) = 0 OR strpos(v_definition, v_call) <> 0 THEN
            RAISE EXCEPTION 'Capacity observation lifecycle dependency changed: %', v_signature;
        END IF;
        EXECUTE replace(v_definition, v_marker, v_marker || v_call);
    END LOOP;
END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $migration$
DECLARE
    v_signature text;
    v_definition text;
    v_call text := E'    PERFORM public.vela_prune_worker_capacity_observations(v_worker.id);\n';
BEGIN
    FOREACH v_signature IN ARRAY ARRAY[
        'vela_observe_worker_instance(jsonb)',
        'vela_report_stage_worker_capacity_v66(jsonb)'
    ] LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        IF strpos(v_definition, v_call) = 0 THEN
            RAISE EXCEPTION 'Capacity observation lifecycle rollback dependency changed: %', v_signature;
        END IF;
        EXECUTE replace(v_definition, v_call, '');
    END LOOP;
END
$migration$;
DROP FUNCTION vela_prune_worker_capacity_observations(uuid);
DROP INDEX capacity_observations_epoch_sequence_idx;
-- Expired superseded leases stay retired; rollback never invents old capacity.
-- +goose StatementEnd
