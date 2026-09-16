-- +goose Up
-- +goose StatementBegin
DO $migration$
DECLARE
    v_definition text := pg_get_functiondef('public.vela_observe_worker_instance(jsonb)'::regprocedure);
    v_before text := $before$    INSERT INTO public.device_sets (
        id, membership_digest, topology_digest, device_count
    ) VALUES (
        v_device_set_id, v_membership_digest, v_topology_digest, v_device_count
    )
    ON CONFLICT (id) DO NOTHING;
$before$;
    v_after text := $after$    INSERT INTO public.device_sets (
        id, membership_digest, topology_digest, device_count
    ) VALUES (
        v_device_set_id, v_membership_digest, v_topology_digest, v_device_count
    )
    ON CONFLICT DO NOTHING;

    -- DeviceSet identifies physical membership/topology, not a Worker lifetime.
    -- Preserve rejection of an already-known ID with changed contents before
    -- resolving a fresh candidate ID to the canonical physical set.
    IF EXISTS (
        SELECT 1 FROM public.device_sets AS device_set
        WHERE device_set.id = v_device_set_id
          AND (device_set.membership_digest <> v_membership_digest
               OR device_set.topology_digest <> v_topology_digest
               OR device_set.device_count <> v_device_count)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'device_set_identity_conflict',
            MESSAGE = 'DeviceSet identity cannot change';
    END IF;
    SELECT device_set.id INTO STRICT v_device_set_id
    FROM public.device_sets AS device_set
    WHERE device_set.membership_digest = v_membership_digest
      AND device_set.topology_digest = v_topology_digest;
$after$;
BEGIN
    IF strpos(v_definition, v_before) = 0 THEN
        RAISE EXCEPTION 'DeviceSet canonicalization dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_before, v_after);
END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $migration$
DECLARE
    v_definition text := pg_get_functiondef('public.vela_observe_worker_instance(jsonb)'::regprocedure);
    v_before text := $before$    INSERT INTO public.device_sets (
        id, membership_digest, topology_digest, device_count
    ) VALUES (
        v_device_set_id, v_membership_digest, v_topology_digest, v_device_count
    )
    ON CONFLICT DO NOTHING;

    -- DeviceSet identifies physical membership/topology, not a Worker lifetime.
    -- Preserve rejection of an already-known ID with changed contents before
    -- resolving a fresh candidate ID to the canonical physical set.
    IF EXISTS (
        SELECT 1 FROM public.device_sets AS device_set
        WHERE device_set.id = v_device_set_id
          AND (device_set.membership_digest <> v_membership_digest
               OR device_set.topology_digest <> v_topology_digest
               OR device_set.device_count <> v_device_count)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'device_set_identity_conflict',
            MESSAGE = 'DeviceSet identity cannot change';
    END IF;
    SELECT device_set.id INTO STRICT v_device_set_id
    FROM public.device_sets AS device_set
    WHERE device_set.membership_digest = v_membership_digest
      AND device_set.topology_digest = v_topology_digest;
$before$;
    v_after text := $after$    INSERT INTO public.device_sets (
        id, membership_digest, topology_digest, device_count
    ) VALUES (
        v_device_set_id, v_membership_digest, v_topology_digest, v_device_count
    )
    ON CONFLICT (id) DO NOTHING;
$after$;
BEGIN
    IF strpos(v_definition, v_before) = 0 THEN
        RAISE EXCEPTION 'DeviceSet canonicalization dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_before, v_after);
END
$migration$;
-- +goose StatementEnd
