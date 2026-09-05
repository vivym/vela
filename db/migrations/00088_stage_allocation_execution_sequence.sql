-- +goose Up
-- +goose StatementBegin
-- Historical V1 allocations remain NULL: no ordering is invented for an
-- authority that was signed before execution sequences existed.
ALTER TABLE public.stage_allocations
    ADD COLUMN execution_sequence bigint CHECK (execution_sequence > 0);
CREATE UNIQUE INDEX stage_allocations_execution_sequence_idx
    ON public.stage_allocations(execution_sequence)
    WHERE execution_sequence IS NOT NULL;
CREATE SEQUENCE public.stage_allocation_execution_sequence
    AS bigint MINVALUE 1 NO MAXVALUE START WITH 1 INCREMENT BY 1 CACHE 1 NO CYCLE;
ALTER SEQUENCE public.stage_allocation_execution_sequence
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON SEQUENCE public.stage_allocation_execution_sequence FROM PUBLIC;

CREATE FUNCTION public.vela_assign_stage_allocation_execution_sequence()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.execution_sequence IS NOT NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_allocation_execution_sequence_database_assigned',
            MESSAGE = 'StageAllocation execution sequence must be assigned by PostgreSQL';
    END IF;
    -- ASSIGN already holds this Worker lock before its residency gate.
    -- Reacquiring it also protects direct inserts from allocating ahead of it.
    PERFORM public.vela_lock_stage_worker_control_session(
        NEW.worker_instance_id, NEW.worker_instance_epoch, NULL
    );
    NEW.execution_sequence := nextval('public.stage_allocation_execution_sequence'::regclass);
    RETURN NEW;
END
$$;
ALTER FUNCTION public.vela_assign_stage_allocation_execution_sequence()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION public.vela_assign_stage_allocation_execution_sequence() FROM PUBLIC;
CREATE TRIGGER stage_allocations_assign_execution_sequence
    BEFORE INSERT ON public.stage_allocations
    FOR EACH ROW EXECUTE FUNCTION public.vela_assign_stage_allocation_execution_sequence();

DO $migration$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_enforce_stage_allocation_authority()'::regprocedure);
    v_old := 'NEW.model_runtime_epoch, NEW.capacity_vector, NEW.allocated_at, NEW.created_at';
    v_new := v_old || ', NEW.execution_sequence';
    IF strpos(v_definition, v_old) = 0 OR strpos(v_definition, 'NEW.execution_sequence') <> 0 THEN
        RAISE EXCEPTION 'StageAllocation immutable authority dependency changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := 'OLD.model_runtime_epoch, OLD.capacity_vector, OLD.allocated_at, OLD.created_at';
    v_new := v_old || ', OLD.execution_sequence';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation previous immutable authority dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure);
    v_old := E'        ''stage_allocation_id'', v_allocation.id,\n';
    v_new := v_old || E'        ''execution_sequence'', v_allocation.execution_sequence,\n';
    IF strpos(v_definition, v_old) = 0 OR strpos(v_definition, '''execution_sequence''') <> 0 THEN
        RAISE EXCEPTION 'StageAssignment allocation snapshot dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$migration$;

CREATE FUNCTION public.vela_read_stage_allocation_execution_sequence(
    p_stage_lease_id uuid,
    p_stage_allocation_id uuid
) RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT allocation.execution_sequence
    FROM public.stage_leases AS lease
    JOIN public.stage_allocations AS allocation
      ON allocation.id = lease.stage_allocation_id
     AND allocation.stage_attempt_id = lease.stage_attempt_id
     AND allocation.stage_run_id = lease.stage_run_id
     AND allocation.attempt_id = lease.attempt_id
    WHERE lease.id = p_stage_lease_id
      AND allocation.id = p_stage_allocation_id;
$$;
ALTER FUNCTION public.vela_read_stage_allocation_execution_sequence(uuid,uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION public.vela_read_stage_allocation_execution_sequence(uuid,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.vela_read_stage_allocation_execution_sequence(uuid,uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Remember issuance even after transaction rollback or future history deletion.
-- Resetting an issued sequence can make delayed signed authorities appear new.
LOCK TABLE public.stage_allocations IN ACCESS EXCLUSIVE MODE;
DO $migration$
BEGIN
    IF (SELECT is_called FROM public.stage_allocation_execution_sequence)
       OR EXISTS (SELECT 1 FROM public.stage_allocations WHERE execution_sequence IS NOT NULL) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_allocation_execution_sequence_downgrade_unsafe',
            MESSAGE = 'Cannot remove execution sequences after an execution number has been issued';
    END IF;
END
$migration$;
DROP FUNCTION public.vela_read_stage_allocation_execution_sequence(uuid,uuid);
DROP TRIGGER stage_allocations_assign_execution_sequence ON public.stage_allocations;
DROP FUNCTION public.vela_assign_stage_allocation_execution_sequence();

DO $migration$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_enforce_stage_allocation_authority()'::regprocedure);
    v_old := 'NEW.model_runtime_epoch, NEW.capacity_vector, NEW.allocated_at, NEW.created_at, NEW.execution_sequence';
    v_new := 'NEW.model_runtime_epoch, NEW.capacity_vector, NEW.allocated_at, NEW.created_at';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation immutable authority rollback dependency changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := 'OLD.model_runtime_epoch, OLD.capacity_vector, OLD.allocated_at, OLD.created_at, OLD.execution_sequence';
    v_new := 'OLD.model_runtime_epoch, OLD.capacity_vector, OLD.allocated_at, OLD.created_at';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAllocation previous immutable authority rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure);
    v_old := E'        ''stage_allocation_id'', v_allocation.id,\n'
        || E'        ''execution_sequence'', v_allocation.execution_sequence,\n';
    v_new := E'        ''stage_allocation_id'', v_allocation.id,\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageAssignment allocation snapshot rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$migration$;
DROP SEQUENCE public.stage_allocation_execution_sequence;
DROP INDEX public.stage_allocations_execution_sequence_idx;
ALTER TABLE public.stage_allocations DROP COLUMN execution_sequence;
-- +goose StatementEnd
