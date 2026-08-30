-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_read_stage_authority_snapshot(
    p_stage_lease_id uuid,
    p_capacity_observation_sequence bigint
) RETURNS TABLE (
    job_id uuid,
    attempt_id uuid,
    attempt_fence bigint,
    attempt_state text,
    stage_run_id uuid,
    stage_fence bigint,
    stage_version bigint,
    stage_run_state text,
    stage_attempt_id uuid,
    stage_attempt_state text,
    stage_profile_revision_id uuid,
    stage_allocation_id uuid,
    stage_allocation_state text,
    capacity_vector jsonb,
    stage_lease_id uuid,
    stage_lease_state text,
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    device_set_digest bytea,
    membership_digest bytea,
    model_residency_id uuid,
    model_runtime_epoch bigint,
    token_digest bytea,
    signing_key_id text,
    execution_nonce bytea,
    issued_at timestamptz,
    expires_at timestamptz,
    local_deadline_at timestamptz,
    control_session_epoch bigint,
    worker_lifecycle_state text,
    worker_reachability_state text,
    runtime_identity text,
    residency_state text,
    capacity_observation_active boolean,
    members jsonb,
    devices jsonb
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT attempt.job_id, attempt.id, attempt.fence, attempt.state::text,
           run.id, run.fence, run.version, run.state::text,
           physical.id, physical.state::text,
           physical.selected_stage_profile_revision_id,
           allocation.id, allocation.state::text, allocation.capacity_vector,
           lease.id, lease.state::text,
           lease.worker_instance_id, lease.worker_instance_epoch,
           lease.device_set_digest, lease.membership_digest,
           lease.model_residency_id, lease.model_runtime_epoch,
           lease.token_digest, lease.signing_key_id, lease.execution_nonce,
           lease.issued_at, lease.expires_at, lease.local_deadline_at,
           worker.control_session_epoch, worker.lifecycle_state::text,
           worker.reachability_state::text, residency.runtime_identity,
           residency.state::text,
           EXISTS (
               SELECT 1 FROM capacity_observations AS observation
               WHERE observation.worker_instance_id = worker.id
                 AND observation.worker_instance_epoch = worker.instance_epoch
                 AND observation.observation_sequence =
                     p_capacity_observation_sequence
                 AND observation.capacity_vector = allocation.capacity_vector
                 AND observation.expires_at > statement_timestamp()
           ),
           COALESCE(member_snapshot.members, '[]'::jsonb),
           COALESCE(device_snapshot.devices, '[]'::jsonb)
    FROM stage_leases AS lease
    JOIN attempts AS attempt ON attempt.id = lease.attempt_id
    JOIN stage_runs AS run ON run.id = lease.stage_run_id
    JOIN stage_attempts AS physical ON physical.id = lease.stage_attempt_id
    JOIN stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
    JOIN worker_instances AS worker ON worker.id = lease.worker_instance_id
    JOIN model_residencies AS residency ON residency.id = lease.model_residency_id
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'id', member.id,
                'epoch', member.member_epoch,
                'identity_digest', encode(member.identity_digest, 'hex'),
                'readiness', member.readiness::text
            ) ORDER BY member.id
        ) AS members
        FROM worker_members AS member
        WHERE member.worker_instance_id = worker.id
          AND member.worker_instance_epoch = worker.instance_epoch
    ) AS member_snapshot ON true
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'id', binding.device_id,
                'epoch', binding.device_epoch
            ) ORDER BY binding.device_id
        ) AS devices
        FROM active_device_bindings AS binding
        WHERE binding.worker_instance_id = worker.id
          AND binding.worker_instance_epoch = worker.instance_epoch
    ) AS device_snapshot ON true
    WHERE lease.id = p_stage_lease_id;
$$;
ALTER FUNCTION vela_read_stage_authority_snapshot(uuid,bigint)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_authority_snapshot(uuid,bigint) FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO vela_stage_worker_control;
GRANT EXECUTE ON FUNCTION vela_read_stage_authority_snapshot(uuid,bigint)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE EXECUTE ON FUNCTION vela_read_stage_authority_snapshot(uuid,bigint)
    FROM vela_stage_worker_control;
REVOKE USAGE ON SCHEMA public FROM vela_stage_worker_control;
DROP FUNCTION vela_read_stage_authority_snapshot(uuid,bigint);
-- +goose StatementEnd
