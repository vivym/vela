-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_consume_stage_transfer_ticket(p_command jsonb)
RETURNS TABLE (ticket_id uuid, consumed_at timestamptz, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_ticket_id uuid := (p_command ->> 'ticket_id')::uuid;
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_worker_id uuid := (p_command ->> 'destination_worker_instance_id')::uuid;
    v_worker_epoch bigint := (p_command ->> 'destination_worker_instance_epoch')::bigint;
    v_residency_id uuid := (p_command ->> 'destination_model_residency_id')::uuid;
    v_runtime_epoch bigint := (p_command ->> 'destination_model_runtime_epoch')::bigint;
    v_connector_id uuid := (p_command ->> 'connector_revision_id')::uuid;
    v_outcome_digest bytea := decode(p_command ->> 'outcome_digest', 'hex');
    v_consumed_at timestamptz := (p_command ->> 'consumed_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_artifact_commands%ROWTYPE;
    v_ticket transfer_tickets%ROWTYPE;
    v_artifact stage_artifacts%ROWTYPE;
    v_pin stage_artifact_pins%ROWTYPE;
BEGIN
    IF v_command_id IS NULL OR v_ticket_id IS NULL
       OR octet_length(v_token_digest) <> 32 OR v_worker_id IS NULL
       OR v_worker_epoch <= 0 OR v_residency_id IS NULL OR v_runtime_epoch <= 0
       OR v_connector_id IS NULL OR octet_length(v_outcome_digest) <> 32
       OR v_consumed_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_transfer_consume_command_invalid',
            MESSAGE = 'Stage TransferTicket consume command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_artifact_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'CONSUME_TRANSFER'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_artifact_command_replay_mismatch',
                MESSAGE = 'StageArtifact command replay does not match';
        END IF;
        RETURN QUERY SELECT (v_existing.result ->> 'ticket_id')::uuid,
            (v_existing.result ->> 'consumed_at')::timestamptz, true;
        RETURN;
    END IF;
    SELECT ticket.* INTO v_ticket FROM transfer_tickets AS ticket
    WHERE ticket.id = v_ticket_id FOR UPDATE;
    SELECT artifact.* INTO v_artifact FROM stage_artifacts AS artifact
    WHERE artifact.id = v_ticket.stage_artifact_id FOR SHARE;
    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin
    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR UPDATE;
    IF v_ticket.state <> 'ACTIVE' OR v_ticket.token_digest <> v_token_digest
       OR v_ticket.destination_worker_instance_id <> v_worker_id
       OR v_ticket.destination_worker_instance_epoch <> v_worker_epoch
       OR v_ticket.destination_model_residency_id <> v_residency_id
       OR v_ticket.destination_model_runtime_epoch <> v_runtime_epoch
       OR v_ticket.connector_revision_id <> v_connector_id
       OR v_consumed_at < v_ticket.issued_at OR v_consumed_at >= v_ticket.expires_at
       OR v_artifact.state <> 'COMMITTED'
       OR v_artifact.object_version <> v_ticket.exact_object_version
       OR v_pin.state <> 'ACTIVE' OR v_pin.stage_artifact_id <> v_artifact.id
       OR v_pin.exact_object_version <> v_artifact.object_version
       OR NOT vela_worker_instance_authority_matches(
           v_worker_id, v_worker_epoch,
           (SELECT epoch.device_set_digest FROM worker_instance_epochs AS epoch
            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),
           (SELECT epoch.membership_digest FROM worker_instance_epochs AS epoch
            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),
           v_residency_id, v_runtime_epoch
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_transfer_ticket_consume_stale',
            MESSAGE = 'Stage TransferTicket is stale, expired, revoked, or for another destination';
    END IF;
    UPDATE transfer_tickets SET state = 'CONSUMED', consumed_at = v_consumed_at,
        outcome_digest = v_outcome_digest WHERE id = v_ticket.id AND state = 'ACTIVE';
    UPDATE edge_buffer_credits SET state = 'RELEASED', released_at = v_consumed_at
    WHERE stage_artifact_id = v_artifact.id
      AND destination_stage_run_id = v_pin.owner_stage_run_id
      AND state = 'HELD';
    INSERT INTO stage_artifact_commands (
        command_id, command_kind, attempt_id, stage_run_id, request_digest, result
    ) VALUES (
        v_command_id, 'CONSUME_TRANSFER', v_artifact.attempt_id,
        v_artifact.producer_stage_run_id, v_request_digest,
        jsonb_build_object('ticket_id', v_ticket.id, 'consumed_at', v_consumed_at)
    );
    RETURN QUERY SELECT v_ticket.id, v_consumed_at, false;
END
$$;
ALTER FUNCTION vela_consume_stage_transfer_ticket(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_consume_stage_transfer_ticket(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_consume_stage_transfer_ticket(jsonb) TO vela_stage_artifact;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM transfer_tickets)
       OR EXISTS (
           SELECT 1 FROM stage_artifact_commands
           WHERE command_kind = 'CONSUME_TRANSFER'
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_transfer_consume_destination_rollback_is_unsafe',
            MESSAGE = 'TransferTicket evidence prevents destination-fence rollback';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION vela_consume_stage_transfer_ticket(p_command jsonb)
RETURNS TABLE (ticket_id uuid, consumed_at timestamptz, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_ticket_id uuid := (p_command ->> 'ticket_id')::uuid;
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_outcome_digest bytea := decode(p_command ->> 'outcome_digest', 'hex');
    v_consumed_at timestamptz := (p_command ->> 'consumed_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_artifact_commands%ROWTYPE;
    v_ticket transfer_tickets%ROWTYPE;
    v_artifact stage_artifacts%ROWTYPE;
    v_pin stage_artifact_pins%ROWTYPE;
BEGIN
    IF v_command_id IS NULL OR v_ticket_id IS NULL
       OR octet_length(v_token_digest) <> 32 OR octet_length(v_outcome_digest) <> 32
       OR v_consumed_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_transfer_consume_command_invalid',
            MESSAGE = 'Stage TransferTicket consume command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_artifact_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'CONSUME_TRANSFER'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_artifact_command_replay_mismatch',
                MESSAGE = 'StageArtifact command replay does not match';
        END IF;
        RETURN QUERY SELECT (v_existing.result ->> 'ticket_id')::uuid,
            (v_existing.result ->> 'consumed_at')::timestamptz, true;
        RETURN;
    END IF;
    SELECT ticket.* INTO v_ticket FROM transfer_tickets AS ticket
    WHERE ticket.id = v_ticket_id FOR UPDATE;
    SELECT artifact.* INTO v_artifact FROM stage_artifacts AS artifact
    WHERE artifact.id = v_ticket.stage_artifact_id FOR SHARE;
    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin
    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR UPDATE;
    IF v_ticket.state <> 'ACTIVE' OR v_ticket.token_digest <> v_token_digest
       OR v_consumed_at < v_ticket.issued_at OR v_consumed_at >= v_ticket.expires_at
       OR v_artifact.state <> 'COMMITTED'
       OR v_artifact.object_version <> v_ticket.exact_object_version
       OR v_pin.state <> 'ACTIVE' OR v_pin.stage_artifact_id <> v_artifact.id
       OR v_pin.exact_object_version <> v_artifact.object_version THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_transfer_ticket_consume_stale',
            MESSAGE = 'Stage TransferTicket is stale, expired, or revoked';
    END IF;
    UPDATE transfer_tickets SET state = 'CONSUMED', consumed_at = v_consumed_at,
        outcome_digest = v_outcome_digest WHERE id = v_ticket.id AND state = 'ACTIVE';
    UPDATE edge_buffer_credits SET state = 'RELEASED', released_at = v_consumed_at
    WHERE stage_artifact_id = v_artifact.id
      AND destination_stage_run_id = v_pin.owner_stage_run_id
      AND state = 'HELD';
    INSERT INTO stage_artifact_commands (
        command_id, command_kind, attempt_id, stage_run_id, request_digest, result
    ) VALUES (
        v_command_id, 'CONSUME_TRANSFER', v_artifact.attempt_id,
        v_artifact.producer_stage_run_id, v_request_digest,
        jsonb_build_object('ticket_id', v_ticket.id, 'consumed_at', v_consumed_at)
    );
    RETURN QUERY SELECT v_ticket.id, v_consumed_at, false;
END
$$;
ALTER FUNCTION vela_consume_stage_transfer_ticket(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_consume_stage_transfer_ticket(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_consume_stage_transfer_ticket(jsonb) TO vela_stage_artifact;
-- +goose StatementEnd
