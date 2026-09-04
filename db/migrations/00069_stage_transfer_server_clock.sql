-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_resolve_stage_transfer_ticket(jsonb)'::regprocedure
    );
    v_old := E'    v_resolved_at timestamptz := (p_command ->> ''resolved_at'')::timestamptz;\n'
        || E'    v_ticket transfer_tickets%ROWTYPE;';
    v_new := E'    v_resolved_at timestamptz := (p_command ->> ''resolved_at'')::timestamptz;\n'
        || E'    v_server_now timestamptz;\n'
        || E'    v_ticket transfer_tickets%ROWTYPE;';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_clock_declaration_drift',
            MESSAGE = 'Stage TransferTicket resolve clock declaration changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR SHARE;\n'
        || E'    IF v_ticket.state NOT IN (''ACTIVE'', ''CONSUMED'')';
    v_new := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR SHARE;\n'
        || E'    v_server_now := clock_timestamp();\n'
        || E'    IF v_ticket.state NOT IN (''ACTIVE'', ''CONSUMED'')';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_clock_sample_drift',
            MESSAGE = 'Stage TransferTicket resolve clock sample changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := 'OR v_resolved_at < v_ticket.issued_at OR v_resolved_at >= v_ticket.expires_at';
    v_new := v_old || E'\n       OR v_server_now < v_ticket.issued_at\n'
        || E'       OR v_server_now >= v_ticket.expires_at';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_clock_condition_drift',
            MESSAGE = 'Stage TransferTicket resolve clock condition changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'       OR NOT vela_worker_instance_authority_matches(\n'
        || E'           v_worker_id, v_worker_epoch,\n'
        || E'           (SELECT epoch.device_set_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           (SELECT epoch.membership_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           v_residency_id, v_runtime_epoch\n'
        || E'       ) THEN';
    v_new := left(v_old, length(v_old) - length(' THEN')) || E'\n'
        || E'       OR NOT EXISTS (\n'
        || E'           SELECT 1 FROM capacity_observations AS observation\n'
        || E'           WHERE observation.worker_instance_id = v_worker_id\n'
        || E'             AND observation.worker_instance_epoch = v_worker_epoch\n'
        || E'             AND observation.expires_at > v_server_now\n'
        || E'       ) THEN';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_capacity_clock_condition_drift',
            MESSAGE = 'Stage TransferTicket resolve capacity clock condition changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef(
        'public.vela_consume_stage_transfer_ticket(jsonb)'::regprocedure
    );
    v_old := E'    v_consumed_at timestamptz := (p_command ->> ''consumed_at'')::timestamptz;\n'
        || E'    v_request_digest bytea;';
    v_new := E'    v_consumed_at timestamptz := (p_command ->> ''consumed_at'')::timestamptz;\n'
        || E'    v_server_now timestamptz;\n'
        || E'    v_request_digest bytea;';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_clock_declaration_drift',
            MESSAGE = 'Stage TransferTicket consume clock declaration changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR UPDATE;\n'
        || E'    IF v_ticket.state <> ''ACTIVE''';
    v_new := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR UPDATE;\n'
        || E'    v_server_now := clock_timestamp();\n'
        || E'    IF v_ticket.state <> ''ACTIVE''';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_clock_sample_drift',
            MESSAGE = 'Stage TransferTicket consume clock sample changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := 'OR v_consumed_at < v_ticket.issued_at OR v_consumed_at >= v_ticket.expires_at';
    v_new := v_old || E'\n       OR v_server_now < v_ticket.issued_at\n'
        || E'       OR v_server_now >= v_ticket.expires_at';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_clock_condition_drift',
            MESSAGE = 'Stage TransferTicket consume clock condition changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'       OR NOT vela_worker_instance_authority_matches(\n'
        || E'           v_worker_id, v_worker_epoch,\n'
        || E'           (SELECT epoch.device_set_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           (SELECT epoch.membership_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           v_residency_id, v_runtime_epoch\n'
        || E'       ) THEN';
    v_new := left(v_old, length(v_old) - length(' THEN')) || E'\n'
        || E'       OR NOT EXISTS (\n'
        || E'           SELECT 1 FROM capacity_observations AS observation\n'
        || E'           WHERE observation.worker_instance_id = v_worker_id\n'
        || E'             AND observation.worker_instance_epoch = v_worker_epoch\n'
        || E'             AND observation.expires_at > v_server_now\n'
        || E'       ) THEN';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_capacity_clock_condition_drift',
            MESSAGE = 'Stage TransferTicket consume capacity clock condition changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    LOCK TABLE public.transfer_tickets IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM transfer_tickets) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_server_clock_rollback_is_unsafe',
            MESSAGE = 'TransferTicket evidence prevents server-clock rollback';
    END IF;

    v_definition := pg_get_functiondef(
        'public.vela_resolve_stage_transfer_ticket(jsonb)'::regprocedure
    );
    v_old := E'    v_resolved_at timestamptz := (p_command ->> ''resolved_at'')::timestamptz;\n'
        || E'    v_ticket transfer_tickets%ROWTYPE;';
    v_new := E'    v_resolved_at timestamptz := (p_command ->> ''resolved_at'')::timestamptz;\n'
        || E'    v_server_now timestamptz;\n'
        || E'    v_ticket transfer_tickets%ROWTYPE;';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_clock_declaration_rollback_drift',
            MESSAGE = 'Stage TransferTicket resolve clock declaration rollback changed';
    END IF;
    v_definition := replace(v_definition, v_new, v_old);
    v_old := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR SHARE;\n'
        || E'    IF v_ticket.state NOT IN (''ACTIVE'', ''CONSUMED'')';
    v_new := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR SHARE;\n'
        || E'    v_server_now := clock_timestamp();\n'
        || E'    IF v_ticket.state NOT IN (''ACTIVE'', ''CONSUMED'')';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_clock_sample_rollback_drift',
            MESSAGE = 'Stage TransferTicket resolve clock sample rollback changed';
    END IF;
    v_definition := replace(v_definition, v_new, v_old);
    v_old := 'OR v_resolved_at < v_ticket.issued_at OR v_resolved_at >= v_ticket.expires_at';
    v_new := v_old || E'\n       OR v_server_now < v_ticket.issued_at\n'
        || E'       OR v_server_now >= v_ticket.expires_at';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_clock_condition_rollback_drift',
            MESSAGE = 'Stage TransferTicket resolve clock condition rollback changed';
    END IF;
    v_definition := replace(v_definition, v_new, v_old);
    v_old := E'       OR NOT vela_worker_instance_authority_matches(\n'
        || E'           v_worker_id, v_worker_epoch,\n'
        || E'           (SELECT epoch.device_set_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           (SELECT epoch.membership_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           v_residency_id, v_runtime_epoch\n'
        || E'       ) THEN';
    v_new := left(v_old, length(v_old) - length(' THEN')) || E'\n'
        || E'       OR NOT EXISTS (\n'
        || E'           SELECT 1 FROM capacity_observations AS observation\n'
        || E'           WHERE observation.worker_instance_id = v_worker_id\n'
        || E'             AND observation.worker_instance_epoch = v_worker_epoch\n'
        || E'             AND observation.expires_at > v_server_now\n'
        || E'       ) THEN';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_resolve_capacity_clock_condition_rollback_drift',
            MESSAGE = 'Stage TransferTicket resolve capacity clock rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_new, v_old);

    v_definition := pg_get_functiondef(
        'public.vela_consume_stage_transfer_ticket(jsonb)'::regprocedure
    );
    v_old := E'    v_consumed_at timestamptz := (p_command ->> ''consumed_at'')::timestamptz;\n'
        || E'    v_request_digest bytea;';
    v_new := E'    v_consumed_at timestamptz := (p_command ->> ''consumed_at'')::timestamptz;\n'
        || E'    v_server_now timestamptz;\n'
        || E'    v_request_digest bytea;';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_clock_declaration_rollback_drift',
            MESSAGE = 'Stage TransferTicket consume clock declaration rollback changed';
    END IF;
    v_definition := replace(v_definition, v_new, v_old);
    v_old := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR UPDATE;\n'
        || E'    IF v_ticket.state <> ''ACTIVE''';
    v_new := E'    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin\n'
        || E'    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR UPDATE;\n'
        || E'    v_server_now := clock_timestamp();\n'
        || E'    IF v_ticket.state <> ''ACTIVE''';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_clock_sample_rollback_drift',
            MESSAGE = 'Stage TransferTicket consume clock sample rollback changed';
    END IF;
    v_definition := replace(v_definition, v_new, v_old);
    v_old := 'OR v_consumed_at < v_ticket.issued_at OR v_consumed_at >= v_ticket.expires_at';
    v_new := v_old || E'\n       OR v_server_now < v_ticket.issued_at\n'
        || E'       OR v_server_now >= v_ticket.expires_at';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_clock_condition_rollback_drift',
            MESSAGE = 'Stage TransferTicket consume clock condition rollback changed';
    END IF;
    v_definition := replace(v_definition, v_new, v_old);
    v_old := E'       OR NOT vela_worker_instance_authority_matches(\n'
        || E'           v_worker_id, v_worker_epoch,\n'
        || E'           (SELECT epoch.device_set_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           (SELECT epoch.membership_digest FROM worker_instance_epochs AS epoch\n'
        || E'            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),\n'
        || E'           v_residency_id, v_runtime_epoch\n'
        || E'       ) THEN';
    v_new := left(v_old, length(v_old) - length(' THEN')) || E'\n'
        || E'       OR NOT EXISTS (\n'
        || E'           SELECT 1 FROM capacity_observations AS observation\n'
        || E'           WHERE observation.worker_instance_id = v_worker_id\n'
        || E'             AND observation.worker_instance_epoch = v_worker_epoch\n'
        || E'             AND observation.expires_at > v_server_now\n'
        || E'       ) THEN';
    IF (length(v_definition) - length(replace(v_definition, v_new, ''))) <>
       length(v_new) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_capacity_clock_condition_rollback_drift',
            MESSAGE = 'Stage TransferTicket consume capacity clock rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_new, v_old);
END
$$;
-- +goose StatementEnd
