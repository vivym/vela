-- +goose Up
-- +goose StatementBegin
DO $migration$
DECLARE
    v_definition text := pg_get_functiondef('public.vela_report_stage_worker_capacity_v66(jsonb)'::regprocedure);
    v_before text := $before$        LEFT JOIN jsonb_each_text(v_profile.capacity_limits) AS certified(resource_name, quantity)
          USING (resource_name)
$before$;
    v_after text := $after$        -- concurrency is the legacy active_stage_slots limit under its canonical
        -- name. An explicit certified concurrency always wins; no other resource
        -- alias is accepted and the numerical upper bound remains unchanged.
        LEFT JOIN jsonb_each_text(
            CASE WHEN NOT (v_profile.capacity_limits ? 'concurrency')
                      AND v_profile.capacity_limits ? 'active_stage_slots'
                 THEN v_profile.capacity_limits || jsonb_build_object(
                     'concurrency', v_profile.capacity_limits -> 'active_stage_slots')
                 ELSE v_profile.capacity_limits
            END
        ) AS certified(resource_name, quantity)
          USING (resource_name)
$after$;
BEGIN
    IF strpos(v_definition, v_before) = 0 THEN
        RAISE EXCEPTION 'Legacy capacity limit dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_before, v_after);
END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $migration$
DECLARE
    v_definition text := pg_get_functiondef('public.vela_report_stage_worker_capacity_v66(jsonb)'::regprocedure);
    v_before text := $before$        -- concurrency is the legacy active_stage_slots limit under its canonical
        -- name. An explicit certified concurrency always wins; no other resource
        -- alias is accepted and the numerical upper bound remains unchanged.
        LEFT JOIN jsonb_each_text(
            CASE WHEN NOT (v_profile.capacity_limits ? 'concurrency')
                      AND v_profile.capacity_limits ? 'active_stage_slots'
                 THEN v_profile.capacity_limits || jsonb_build_object(
                     'concurrency', v_profile.capacity_limits -> 'active_stage_slots')
                 ELSE v_profile.capacity_limits
            END
        ) AS certified(resource_name, quantity)
          USING (resource_name)
$before$;
    v_after text := $after$        LEFT JOIN jsonb_each_text(v_profile.capacity_limits) AS certified(resource_name, quantity)
          USING (resource_name)
$after$;
BEGIN
    IF strpos(v_definition, v_before) = 0 THEN
        RAISE EXCEPTION 'Legacy capacity limit dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_before, v_after);
END
$migration$;
-- +goose StatementEnd
