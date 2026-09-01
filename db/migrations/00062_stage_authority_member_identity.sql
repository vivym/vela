-- +goose Up
-- +goose StatementBegin
ALTER FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    RENAME TO vela_read_stage_worker_acquire_authority_v2;
REVOKE EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority_v2(uuid)
    FROM vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_worker_acquire_authority(p_command_id uuid)
RETURNS TABLE (decision text, reason text, authority jsonb)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_decision text;
    v_reason text;
    v_authority jsonb;
    v_members jsonb;
BEGIN
    SELECT legacy.decision, legacy.reason, legacy.authority
    INTO STRICT v_decision, v_reason, v_authority
    FROM vela_read_stage_worker_acquire_authority_v2(p_command_id) AS legacy;

    IF v_decision = 'AUTHORIZED' THEN
        SELECT jsonb_agg(
            entry.value || jsonb_build_object(
                'identity_digest', encode(member.identity_digest, 'hex')
            )
            ORDER BY entry.position
        ) INTO v_members
        FROM jsonb_array_elements(v_authority -> 'members')
             WITH ORDINALITY AS entry(value, position)
        JOIN worker_members AS member
          ON member.id = (entry.value ->> 'worker_member_id')::uuid
         AND octet_length(member.identity_digest) = 32;

        IF v_members IS NULL
           OR jsonb_array_length(v_members) <>
              jsonb_array_length(v_authority -> 'members') THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_worker_acquire_member_identity_stale',
                MESSAGE = 'Stage Worker assignment member identity is stale';
        END IF;
        v_authority := jsonb_set(v_authority, '{members}', v_members, true);
    END IF;

    RETURN QUERY SELECT v_decision, v_reason, v_authority;
END
$$;
ALTER FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_worker_acquire_authority(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    TO vela_stage_worker_control;

ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution_v2;
REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution_v2(uuid,uuid)
    FROM vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_assignment_execution(
    p_command_id uuid,
    p_claim_id uuid
) RETURNS TABLE (snapshot jsonb)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_snapshot jsonb;
    v_members jsonb;
BEGIN
    SELECT legacy.snapshot INTO STRICT v_snapshot
    FROM vela_read_stage_assignment_execution_v2(p_command_id, p_claim_id) AS legacy;

    SELECT jsonb_agg(
        entry.value || jsonb_build_object(
            'identity_digest', encode(member.identity_digest, 'hex')
        )
        ORDER BY entry.position
    ) INTO v_members
    FROM jsonb_array_elements(v_snapshot -> 'members')
         WITH ORDINALITY AS entry(value, position)
    JOIN worker_members AS member
      ON member.id = (entry.value ->> 'worker_member_id')::uuid
     AND octet_length(member.identity_digest) = 32;

    IF v_members IS NULL
       OR jsonb_array_length(v_members) <>
          jsonb_array_length(v_snapshot -> 'members') THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_worker_execution_member_identity_stale',
            MESSAGE = 'Stage Worker execution member identity is stale';
    END IF;
    v_snapshot := jsonb_set(v_snapshot, '{members}', v_members, true);
    RETURN QUERY SELECT v_snapshot;
END
$$;
ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_read_stage_assignment_execution(uuid,uuid);
ALTER FUNCTION vela_read_stage_assignment_execution_v2(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;

REVOKE EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_read_stage_worker_acquire_authority(uuid);
ALTER FUNCTION vela_read_stage_worker_acquire_authority_v2(uuid)
    RENAME TO vela_read_stage_worker_acquire_authority;
GRANT EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd
