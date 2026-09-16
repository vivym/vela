-- +goose Up
-- +goose StatementBegin
-- Capacity observations are fresh scheduling inputs. A committed allocation
-- retains the exact validated sequence even after observation GC. Historical
-- allocations remain NULL; never invent an authority binding for old work.
LOCK TABLE public.stage_allocations IN ACCESS EXCLUSIVE MODE;
DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM public.stage_allocations WHERE state = 'ALLOCATED') THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'stage_capacity_binding_requires_quiescence',
            MESSAGE = 'Drain active StageAllocations before installing capacity bindings';
    END IF;
END
$migration$;
ALTER TABLE public.stage_allocations
    ADD COLUMN capacity_observation_sequence bigint CHECK (capacity_observation_sequence > 0);

CREATE FUNCTION public.vela_bind_stage_allocation_capacity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM public.vela_lock_stage_worker_control_session(
        NEW.worker_instance_id, NEW.worker_instance_epoch, NULL
    );
    IF NEW.capacity_observation_sequence IS NULL
       OR NOT public.vela_lock_exact_capacity_observation(
           NEW.worker_instance_id, NEW.worker_instance_epoch,
           NEW.capacity_observation_sequence, NEW.capacity_vector
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'stage_allocation_capacity_binding_invalid',
            MESSAGE = 'New StageAllocation requires exact fresh capacity evidence';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION public.vela_bind_stage_allocation_capacity() OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION public.vela_bind_stage_allocation_capacity() FROM PUBLIC;
CREATE TRIGGER stage_allocations_bind_capacity
    BEFORE INSERT ON public.stage_allocations
    FOR EACH ROW EXECUTE FUNCTION public.vela_bind_stage_allocation_capacity();

-- The existing authority predicate remains the scheduling eligibility gate.
-- This narrower predicate checks the same lifecycle, topology and residency
-- identities for work already held by an allocation and bounded lease.
DO $migration$
DECLARE
    v_definition text;
    v_capacity text := $capacity$          AND EXISTS (
              SELECT 1
              FROM public.capacity_observations AS observation
              WHERE observation.worker_instance_id = worker.id
                AND observation.worker_instance_epoch = worker.instance_epoch
                AND observation.expires_at > statement_timestamp()
          )
$capacity$;
BEGIN
    v_definition := pg_get_functiondef('public.vela_worker_instance_authority_matches(uuid,bigint,bytea,bytea,uuid,bigint)'::regprocedure);
    IF strpos(v_definition, v_capacity) = 0 THEN
        RAISE EXCEPTION 'Worker execution identity dependency changed';
    END IF;
    v_definition := replace(v_definition, v_capacity, '');
    EXECUTE replace(v_definition, 'vela_worker_instance_authority_matches(',
                    'vela_worker_instance_execution_identity_matches(');
END
$migration$;
ALTER FUNCTION public.vela_worker_instance_execution_identity_matches(uuid,bigint,bytea,bytea,uuid,bigint)
    OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION public.vela_worker_instance_execution_identity_matches(uuid,bigint,bytea,bytea,uuid,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.vela_worker_instance_execution_identity_matches(uuid,bigint,bytea,bytea,uuid,bigint)
    TO vela_attempt_coordinator_owner;

DO $migration$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_enforce_stage_allocation_authority()'::regprocedure);
    v_old := $old$NEW.model_runtime_epoch, NEW.capacity_vector, NEW.allocated_at, NEW.created_at, NEW.execution_sequence$old$;
    v_new := $new$NEW.model_runtime_epoch, NEW.capacity_vector, NEW.allocated_at, NEW.created_at, NEW.execution_sequence, NEW.capacity_observation_sequence$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_enforce_stage_allocation_authority()';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_enforce_stage_allocation_authority()'::regprocedure);
    v_old := $old$OLD.model_runtime_epoch, OLD.capacity_vector, OLD.allocated_at, OLD.created_at, OLD.execution_sequence$old$;
    v_new := $new$OLD.model_runtime_epoch, OLD.capacity_vector, OLD.allocated_at, OLD.created_at, OLD.execution_sequence, OLD.capacity_observation_sequence$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_enforce_stage_allocation_authority()';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$        capacity_vector, state, allocated_at
$old$;
    v_new := $new$        capacity_vector, state, allocated_at, capacity_observation_sequence
$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$        p_command -> 'capacity_vector', 'ALLOCATED', v_issued_at
$old$;
    v_new := $new$        p_command -> 'capacity_vector', 'ALLOCATED', v_issued_at, v_capacity_observation_sequence
$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_read_stage_authority_snapshot(uuid,bigint)'::regprocedure);
    v_old := $old$           EXISTS (
               SELECT 1 FROM capacity_observations AS observation
               WHERE observation.worker_instance_id = worker.id
                 AND observation.worker_instance_epoch = worker.instance_epoch
                 AND observation.observation_sequence =
                     p_capacity_observation_sequence
                 AND observation.capacity_vector = allocation.capacity_vector
                 AND observation.expires_at > statement_timestamp()
                 AND NOT EXISTS (
                     SELECT 1 FROM capacity_observations AS newer
                     WHERE newer.worker_instance_id = worker.id
                       AND newer.observation_sequence >
                           p_capacity_observation_sequence
                 )
           ),
$old$;
    v_new := $new$           COALESCE(allocation.capacity_observation_sequence =
                    p_capacity_observation_sequence, false),
$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_read_stage_authority_snapshot(uuid,bigint)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_start_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );$old$;
    v_new := $new$    -- ASSIGN fixed this identity under the fresh-capacity and Worker locks.
    -- Execution remains governed by its lease, allocation and epoch checks,
    -- not by the lifetime of the scheduler's disposable observation.
    v_capacity_observation_active := COALESCE(
        v_allocation.capacity_observation_sequence = v_capacity_observation_sequence,
        false
    );$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_start_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_heartbeat_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );$old$;
    v_new := $new$    -- ASSIGN fixed this identity under the fresh-capacity and Worker locks.
    -- Execution remains governed by its lease, allocation and epoch checks,
    -- not by the lifetime of the scheduler's disposable observation.
    v_capacity_observation_active := COALESCE(
        v_allocation.capacity_observation_sequence = v_capacity_observation_sequence,
        false
    );$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_heartbeat_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_reattach_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );$old$;
    v_new := $new$    -- ASSIGN fixed this identity under the fresh-capacity and Worker locks.
    -- Execution remains governed by its lease, allocation and epoch checks,
    -- not by the lifetime of the scheduler's disposable observation.
    v_capacity_observation_active := COALESCE(
        v_allocation.capacity_observation_sequence = v_capacity_observation_sequence,
        false
    );$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_reattach_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_start_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$vela_worker_instance_authority_matches($old$;
    v_new := $new$vela_worker_instance_execution_identity_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_start_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_heartbeat_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$vela_worker_instance_authority_matches($old$;
    v_new := $new$vela_worker_instance_execution_identity_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_heartbeat_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_reattach_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$vela_worker_instance_authority_matches($old$;
    v_new := $new$vela_worker_instance_execution_identity_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_reattach_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$public.vela_worker_instance_authority_matches(
               v_stage_lease.worker_instance_id,$old$;
    v_new := $new$public.vela_worker_instance_execution_identity_matches(
               v_stage_lease.worker_instance_id,$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure);
    v_old := $old$vela_worker_instance_authority_matches($old$;
    v_new := $new$vela_worker_instance_execution_identity_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_read_stage_assignment_execution_v1(uuid,uuid)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE public.stage_allocations IN ACCESS EXCLUSIVE MODE;
DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM public.stage_allocations
               WHERE capacity_observation_sequence IS NOT NULL OR state = 'ALLOCATED') THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'stage_capacity_binding_downgrade_unsafe',
            MESSAGE = 'Cannot remove committed StageAllocation capacity bindings';
    END IF;
END
$migration$;
DROP TRIGGER stage_allocations_bind_capacity ON public.stage_allocations;
DROP FUNCTION public.vela_bind_stage_allocation_capacity();

DO $migration$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure);
    v_old := $old$vela_worker_instance_execution_identity_matches($old$;
    v_new := $new$vela_worker_instance_authority_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_read_stage_assignment_execution_v1(uuid,uuid)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$public.vela_worker_instance_execution_identity_matches(
               v_stage_lease.worker_instance_id,$old$;
    v_new := $new$public.vela_worker_instance_authority_matches(
               v_stage_lease.worker_instance_id,$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_reattach_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$vela_worker_instance_execution_identity_matches($old$;
    v_new := $new$vela_worker_instance_authority_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_reattach_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_heartbeat_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$vela_worker_instance_execution_identity_matches($old$;
    v_new := $new$vela_worker_instance_authority_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_heartbeat_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_start_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$vela_worker_instance_execution_identity_matches($old$;
    v_new := $new$vela_worker_instance_authority_matches($new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_start_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_reattach_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$    -- ASSIGN fixed this identity under the fresh-capacity and Worker locks.
    -- Execution remains governed by its lease, allocation and epoch checks,
    -- not by the lifetime of the scheduler's disposable observation.
    v_capacity_observation_active := COALESCE(
        v_allocation.capacity_observation_sequence = v_capacity_observation_sequence,
        false
    );$old$;
    v_new := $new$    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_reattach_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_heartbeat_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$    -- ASSIGN fixed this identity under the fresh-capacity and Worker locks.
    -- Execution remains governed by its lease, allocation and epoch checks,
    -- not by the lifetime of the scheduler's disposable observation.
    v_capacity_observation_active := COALESCE(
        v_allocation.capacity_observation_sequence = v_capacity_observation_sequence,
        false
    );$old$;
    v_new := $new$    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_heartbeat_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_start_stage_worker_command(jsonb)'::regprocedure);
    v_old := $old$    -- ASSIGN fixed this identity under the fresh-capacity and Worker locks.
    -- Execution remains governed by its lease, allocation and epoch checks,
    -- not by the lifetime of the scheduler's disposable observation.
    v_capacity_observation_active := COALESCE(
        v_allocation.capacity_observation_sequence = v_capacity_observation_sequence,
        false
    );$old$;
    v_new := $new$    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_start_stage_worker_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_read_stage_authority_snapshot(uuid,bigint)'::regprocedure);
    v_old := $old$           COALESCE(allocation.capacity_observation_sequence =
                    p_capacity_observation_sequence, false),
$old$;
    v_new := $new$           EXISTS (
               SELECT 1 FROM capacity_observations AS observation
               WHERE observation.worker_instance_id = worker.id
                 AND observation.worker_instance_epoch = worker.instance_epoch
                 AND observation.observation_sequence =
                     p_capacity_observation_sequence
                 AND observation.capacity_vector = allocation.capacity_vector
                 AND observation.expires_at > statement_timestamp()
                 AND NOT EXISTS (
                     SELECT 1 FROM capacity_observations AS newer
                     WHERE newer.worker_instance_id = worker.id
                       AND newer.observation_sequence >
                           p_capacity_observation_sequence
                 )
           ),
$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_read_stage_authority_snapshot(uuid,bigint)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$        p_command -> 'capacity_vector', 'ALLOCATED', v_issued_at, v_capacity_observation_sequence
$old$;
    v_new := $new$        p_command -> 'capacity_vector', 'ALLOCATED', v_issued_at
$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$        capacity_vector, state, allocated_at, capacity_observation_sequence
$old$;
    v_new := $new$        capacity_vector, state, allocated_at
$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_enforce_stage_allocation_authority()'::regprocedure);
    v_old := $old$OLD.model_runtime_epoch, OLD.capacity_vector, OLD.allocated_at, OLD.created_at, OLD.execution_sequence, OLD.capacity_observation_sequence$old$;
    v_new := $new$OLD.model_runtime_epoch, OLD.capacity_vector, OLD.allocated_at, OLD.created_at, OLD.execution_sequence$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_enforce_stage_allocation_authority()';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_enforce_stage_allocation_authority()'::regprocedure);
    v_old := $old$NEW.model_runtime_epoch, NEW.capacity_vector, NEW.allocated_at, NEW.created_at, NEW.execution_sequence, NEW.capacity_observation_sequence$old$;
    v_new := $new$NEW.model_runtime_epoch, NEW.capacity_vector, NEW.allocated_at, NEW.created_at, NEW.execution_sequence$new$;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation capacity binding dependency changed: public.vela_enforce_stage_allocation_authority()';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

END
$migration$;
DROP FUNCTION public.vela_worker_instance_execution_identity_matches(uuid,bigint,bytea,bytea,uuid,bigint);
ALTER TABLE public.stage_allocations DROP COLUMN capacity_observation_sequence;
-- +goose StatementEnd
