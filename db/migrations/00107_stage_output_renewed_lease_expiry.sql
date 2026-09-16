-- +goose Up
-- +goose StatementBegin
-- Both commands already hold the StageLease row lock, shared with renewal and
-- expiry recovery. Honor only committed renewals; never extend an expired or
-- revoked lease. The server clock also prevents backdated completion after
-- the effective execution deadline.
DO $migration$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_seal_stage_output(jsonb)'::regprocedure);
    v_old := $old$OR v_sealed_at < v_physical.started_at OR v_sealed_at >= v_lease.expires_at$old$;
    v_new := $new$OR v_sealed_at < v_physical.started_at
       OR v_sealed_at >= public.vela_stage_lease_effective_expires_at(v_lease.id)
       OR clock_timestamp() >= public.vela_stage_lease_effective_expires_at(v_lease.id)$new$;
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <> length(v_old) THEN
        RAISE EXCEPTION 'Stage output expiry dependency changed: public.vela_seal_stage_output(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$OR v_completed_at >= v_stage_lease.expires_at$old$;
    v_new := $new$OR v_completed_at >= public.vela_stage_lease_effective_expires_at(v_stage_lease.id)
           OR clock_timestamp() >= public.vela_stage_lease_effective_expires_at(v_stage_lease.id)$new$;
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <> length(v_old) THEN
        RAISE EXCEPTION 'Stage output expiry dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM public.stage_allocations WHERE state = 'ALLOCATED') THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'stage_output_expiry_downgrade_requires_quiescence',
            MESSAGE = 'Drain active stages before restoring legacy output expiry checks';
    END IF;
END
$migration$;
DO $migration$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := $old$OR v_completed_at >= public.vela_stage_lease_effective_expires_at(v_stage_lease.id)
           OR clock_timestamp() >= public.vela_stage_lease_effective_expires_at(v_stage_lease.id)$old$;
    v_new := $new$OR v_completed_at >= v_stage_lease.expires_at$new$;
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <> length(v_old) THEN
        RAISE EXCEPTION 'Stage output expiry dependency changed: public.vela_apply_stage_command(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_seal_stage_output(jsonb)'::regprocedure);
    v_old := $old$OR v_sealed_at < v_physical.started_at
       OR v_sealed_at >= public.vela_stage_lease_effective_expires_at(v_lease.id)
       OR clock_timestamp() >= public.vela_stage_lease_effective_expires_at(v_lease.id)$old$;
    v_new := $new$OR v_sealed_at < v_physical.started_at OR v_sealed_at >= v_lease.expires_at$new$;
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <> length(v_old) THEN
        RAISE EXCEPTION 'Stage output expiry dependency changed: public.vela_seal_stage_output(jsonb)';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

END
$migration$;
-- +goose StatementEnd
