-- +goose Up
-- +goose StatementBegin
-- A completed graph no longer reserves future L2 capacity. The byte counters
-- and independently retained objects/Usage remain evidence of past execution.
-- Finalization holds ROW SHARE on jobs before writing attempts, then upgrades
-- its Job lock. Wait for that entire transaction before locking child tables.
LOCK TABLE jobs IN EXCLUSIVE MODE;
LOCK TABLE attempts, stage_storage_reservations IN SHARE ROW EXCLUSIVE MODE;

DO $migration$
DECLARE
    v_definition text;
    v_old text := '    RETURN FOUND;';
    v_new text := $body$    IF NOT FOUND THEN RETURN false; END IF;
    UPDATE public.stage_storage_reservations AS reservation
    SET state = 'CONSUMED', updated_at = p_completed_at
    WHERE reservation.attempt_id = p_attempt_id AND reservation.state = 'RESERVED';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '40001',
            CONSTRAINT = 'stage_storage_completion_authority_missing',
            MESSAGE = 'Successful Stage graph requires its active storage reservation';
    END IF;
    RETURN true;$body$;
BEGIN
    v_definition := pg_get_functiondef(
        'vela_complete_stage_graph_visible_completion_attempt(uuid,bigint,timestamptz)'::regprocedure);
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'Stage successful Attempt completion dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$migration$;

DO $migration$
DECLARE
    v_migration_role text := current_user;
    v_reservation_ids uuid[];
BEGIN
    SELECT array_agg(reservation.id) INTO v_reservation_ids
    FROM public.stage_storage_reservations AS reservation
    JOIN public.visible_completions AS completion
      ON completion.organization_id = reservation.organization_id
     AND completion.project_id = reservation.project_id
     AND completion.job_id = reservation.job_id
     AND completion.attempt_id = reservation.attempt_id
    JOIN public.stage_graph_finalization_claims AS claim
      ON claim.id = completion.authority_stage_graph_finalization_claim_id
     AND claim.organization_id = completion.organization_id
     AND claim.project_id = completion.project_id
     AND claim.job_id = completion.job_id
     AND claim.attempt_id = completion.attempt_id
     AND claim.attempt_fence = completion.attempt_fence
     AND claim.state = 'COMPLETED'
    WHERE reservation.state = 'RESERVED';

    -- Bind the migration's exact evidence set before entering the established
    -- writer role; no new runtime privilege or table-write exception is needed.
    SET LOCAL ROLE vela_attempt_coordinator_owner;
    UPDATE public.stage_storage_reservations
    SET state = 'CONSUMED', updated_at = clock_timestamp()
    WHERE id = ANY(v_reservation_ids) AND state = 'RESERVED';
    PERFORM set_config('role', v_migration_role, true);
END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- CONSUMED already exists in schema 81. Preserve completed history on rollback;
-- reactivating its old reservation would invent outstanding execution demand.
DO $migration$
DECLARE
    v_definition text;
    v_old text := '    RETURN FOUND;';
    v_new text := $body$    IF NOT FOUND THEN RETURN false; END IF;
    UPDATE public.stage_storage_reservations AS reservation
    SET state = 'CONSUMED', updated_at = p_completed_at
    WHERE reservation.attempt_id = p_attempt_id AND reservation.state = 'RESERVED';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '40001',
            CONSTRAINT = 'stage_storage_completion_authority_missing',
            MESSAGE = 'Successful Stage graph requires its active storage reservation';
    END IF;
    RETURN true;$body$;
BEGIN
    v_definition := pg_get_functiondef(
        'vela_complete_stage_graph_visible_completion_attempt(uuid,bigint,timestamptz)'::regprocedure);
    IF strpos(v_definition, v_new) = 0 THEN
        RAISE EXCEPTION 'Stage successful Attempt completion rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_new, v_old);
END
$migration$;
-- +goose StatementEnd
