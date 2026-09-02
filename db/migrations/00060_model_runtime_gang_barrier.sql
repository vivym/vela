-- +goose Up
-- +goose StatementBegin
CREATE TYPE model_runtime_barrier_state AS ENUM ('WAITING', 'READY', 'SUPERSEDED');

CREATE TABLE model_runtime_barriers (
    model_residency_id uuid NOT NULL REFERENCES model_residencies(id),
    barrier_generation bigint NOT NULL CHECK (barrier_generation > 0),
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    expected_member_count integer NOT NULL CHECK (expected_member_count > 0),
    leader_worker_member_id uuid NOT NULL REFERENCES worker_members(id),
    state model_runtime_barrier_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ready_at timestamptz,
    superseded_at timestamptz,
    PRIMARY KEY (model_residency_id, barrier_generation),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    CHECK ((state = 'READY') = (ready_at IS NOT NULL)),
    CHECK ((state = 'SUPERSEDED') = (superseded_at IS NOT NULL)),
    CHECK (ready_at IS NULL OR ready_at >= created_at),
    CHECK (superseded_at IS NULL OR superseded_at >= created_at)
);
ALTER TABLE model_runtime_barriers OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON TABLE model_runtime_barriers FROM PUBLIC;
GRANT SELECT ON worker_member_devices TO vela_attempt_coordinator_owner;

ALTER TABLE model_runtime_epoch_registrations
    RENAME COLUMN model_runtime_epoch TO barrier_generation;
ALTER TABLE model_runtime_epoch_registrations
    ADD COLUMN local_model_runtime_epoch bigint,
    ADD COLUMN device_subset_digest bytea;

UPDATE model_runtime_epoch_registrations AS registration
SET local_model_runtime_epoch = registration.barrier_generation,
    device_subset_digest = member.device_subset_digest
FROM worker_members AS member
WHERE member.id = registration.worker_member_id
  AND member.worker_instance_id = registration.worker_instance_id
  AND member.worker_instance_epoch = registration.worker_instance_epoch;

INSERT INTO model_runtime_barriers (
    model_residency_id, barrier_generation,
    worker_instance_id, worker_instance_epoch,
    expected_member_count, leader_worker_member_id,
    state, created_at, ready_at
)
SELECT registration.model_residency_id, registration.barrier_generation,
       registration.worker_instance_id, registration.worker_instance_epoch,
       worker.desired_member_count, min(registration.worker_member_id::text)::uuid,
       'READY', min(registration.registered_at), max(registration.registered_at)
FROM model_runtime_epoch_registrations AS registration
JOIN worker_instances AS worker ON worker.id = registration.worker_instance_id
GROUP BY registration.model_residency_id, registration.barrier_generation,
         registration.worker_instance_id, registration.worker_instance_epoch,
         worker.desired_member_count;

ALTER TABLE model_runtime_epoch_registrations
    ALTER COLUMN local_model_runtime_epoch SET NOT NULL,
    ALTER COLUMN device_subset_digest SET NOT NULL,
    ADD CONSTRAINT model_runtime_epoch_registrations_local_epoch_check
        CHECK (local_model_runtime_epoch > 0),
    ADD CONSTRAINT model_runtime_epoch_registrations_device_subset_digest_check
        CHECK (octet_length(device_subset_digest) = 32),
    ADD CONSTRAINT model_runtime_epoch_registrations_barrier_fk
        FOREIGN KEY (model_residency_id, barrier_generation)
        REFERENCES model_runtime_barriers(model_residency_id, barrier_generation);

ALTER FUNCTION vela_register_stage_worker_runtime(jsonb)
    RENAME TO vela_register_stage_worker_runtime_v1;
REVOKE EXECUTE ON FUNCTION vela_register_stage_worker_runtime_v1(jsonb)
    FROM vela_stage_worker_control;

CREATE FUNCTION vela_verify_stage_worker_member_registration(p_evidence jsonb)
RETURNS TABLE (
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    ready boolean,
    reason text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_model_residency_id uuid;
    v_local_model_runtime_epoch bigint;
    v_stage_profile_revision_id uuid;
    v_worker_member_id uuid;
    v_worker_member_epoch bigint;
    v_capacity_observation_sequence bigint;
    v_device_set_digest bytea;
    v_membership_digest bytea;
    v_spiffe_id_digest bytea;
    v_readiness_evidence_digest bytea;
    v_runtime_identity text;
    v_devices jsonb;
    v_members jsonb;
    v_ready boolean;
BEGIN
    IF p_evidence IS NULL OR jsonb_typeof(p_evidence) <> 'object'
       OR (p_evidence ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_member_registration_invalid',
            MESSAGE = 'ModelRuntime member registration evidence is invalid';
    END IF;
    v_worker_instance_id := (p_evidence ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_evidence ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_evidence ->> 'control_session_epoch')::bigint;
    v_model_residency_id := (p_evidence ->> 'model_residency_id')::uuid;
    v_local_model_runtime_epoch := (p_evidence ->> 'model_runtime_epoch')::bigint;
    v_stage_profile_revision_id :=
        (p_evidence ->> 'stage_profile_revision_id')::uuid;
    v_worker_member_id := (p_evidence ->> 'worker_member_id')::uuid;
    v_worker_member_epoch := (p_evidence ->> 'worker_member_epoch')::bigint;
    v_capacity_observation_sequence :=
        (p_evidence ->> 'capacity_observation_sequence')::bigint;
    v_device_set_digest := decode(p_evidence ->> 'device_set_digest', 'hex');
    v_membership_digest := decode(p_evidence ->> 'membership_digest', 'hex');
    v_spiffe_id_digest := decode(p_evidence ->> 'spiffe_id_digest', 'hex');
    v_readiness_evidence_digest :=
        decode(p_evidence ->> 'readiness_evidence_digest', 'hex');
    v_runtime_identity := p_evidence ->> 'runtime_identity';
    v_devices := p_evidence -> 'devices';
    v_members := p_evidence -> 'members';

    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_control_session_epoch <= 0 OR v_model_residency_id IS NULL
       OR v_local_model_runtime_epoch <= 0 OR v_stage_profile_revision_id IS NULL
       OR v_worker_member_id IS NULL OR v_worker_member_epoch <= 0
       OR v_capacity_observation_sequence <= 0
       OR octet_length(v_device_set_digest) <> 32
       OR octet_length(v_membership_digest) <> 32
       OR octet_length(v_spiffe_id_digest) <> 32
       OR octet_length(v_readiness_evidence_digest) <> 32
       OR v_runtime_identity IS NULL OR length(v_runtime_identity) NOT BETWEEN 1 AND 500
       OR jsonb_typeof(v_devices) <> 'array' OR jsonb_array_length(v_devices) = 0
       OR jsonb_array_length(v_devices) > 64
       OR jsonb_typeof(v_members) <> 'array' OR jsonb_array_length(v_members) <> 1
       OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(v_devices) AS requested(device)
           GROUP BY requested.device ->> 'device_id' HAVING count(*) > 1
       )
       OR (v_members #>> '{0,worker_member_id}')::uuid <> v_worker_member_id
       OR (v_members #>> '{0,member_epoch}')::bigint <> v_worker_member_epoch
       OR (v_members #>> '{0,model_runtime_epoch}')::bigint <>
          v_local_model_runtime_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_member_registration_invalid',
            MESSAGE = 'ModelRuntime member registration fields are invalid';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM worker_instances AS worker
        JOIN worker_instance_epochs AS epoch
          ON epoch.worker_instance_id = worker.id
         AND epoch.epoch = worker.instance_epoch
        JOIN capacity_pools AS pool ON pool.id = worker.capacity_pool_id
        JOIN stage_profile_revisions AS profile
          ON profile.id = v_stage_profile_revision_id
        JOIN model_residencies AS residency
          ON residency.id = v_model_residency_id
         AND residency.worker_instance_id = worker.id
         AND residency.worker_instance_epoch = worker.instance_epoch
        JOIN worker_members AS caller
          ON caller.id = v_worker_member_id
         AND caller.worker_instance_id = worker.id
         AND caller.worker_instance_epoch = worker.instance_epoch
        JOIN capacity_observations AS observation
          ON observation.worker_instance_id = worker.id
         AND observation.worker_instance_epoch = worker.instance_epoch
         AND observation.observation_sequence = v_capacity_observation_sequence
        WHERE worker.id = v_worker_instance_id
          AND worker.instance_epoch = v_worker_instance_epoch
          AND worker.control_session_epoch = v_control_session_epoch
          AND worker.lifecycle_state = 'READY'
          AND worker.reachability_state = 'CONNECTED'
          AND worker.device_set_digest = v_device_set_digest
          AND worker.membership_digest = v_membership_digest
          AND epoch.device_set_digest = v_device_set_digest
          AND epoch.membership_digest = v_membership_digest
          AND epoch.ended_at IS NULL
          AND worker.worker_profile_revision_id = profile.worker_profile_revision_id
          AND pool.stage_profile_revision_id = profile.id
          AND pool.state = 'ACTIVE'
          AND profile.state IN ('CERTIFIED', 'ACTIVE')
          AND residency.model_component_revision = profile.model_component_revision
          AND residency.runtime_image_digest = profile.runtime_image_digest
          AND residency.runtime_identity = v_runtime_identity
          AND residency.state IN ('READY', 'WARMING')
          AND (
              residency.warmup_evidence_digest = v_readiness_evidence_digest
              OR residency.canary_evidence_digest = v_readiness_evidence_digest
          )
          AND caller.member_epoch = v_worker_member_epoch
          AND caller.identity_digest = v_spiffe_id_digest
          AND caller.readiness = 'READY'
          AND observation.expires_at > clock_timestamp()
          AND jsonb_array_length(v_devices) = (
              SELECT count(*) FROM worker_member_devices AS assigned
              WHERE assigned.worker_instance_id = worker.id
                AND assigned.worker_member_id = caller.id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM jsonb_array_elements(v_devices) AS requested(device)
              LEFT JOIN worker_member_devices AS assigned
                ON assigned.worker_instance_id = worker.id
               AND assigned.worker_member_id = caller.id
               AND assigned.device_id = (requested.device ->> 'device_id')::uuid
               AND assigned.device_epoch = (requested.device ->> 'device_epoch')::bigint
              LEFT JOIN active_device_bindings AS binding
                ON binding.worker_instance_id = worker.id
               AND binding.worker_instance_epoch = worker.instance_epoch
               AND binding.device_id = assigned.device_id
               AND binding.device_epoch = assigned.device_epoch
              WHERE assigned.device_id IS NULL OR binding.device_id IS NULL
          )
          AND (
              SELECT count(*) FROM worker_members AS member
              WHERE member.worker_instance_id = worker.id
                AND member.worker_instance_epoch = worker.instance_epoch
                AND member.readiness = 'READY'
          ) = worker.desired_member_count
          AND (
              SELECT count(*) FROM active_device_bindings AS binding
              WHERE binding.worker_instance_id = worker.id
                AND binding.worker_instance_epoch = worker.instance_epoch
          ) = worker.desired_device_count
    ) INTO v_ready;

    RETURN QUERY SELECT
        v_worker_instance_id, v_worker_instance_epoch, v_ready,
        CASE WHEN v_ready THEN 'member Fleet evidence verified'
             ELSE 'member Fleet evidence does not match durable authority' END;
END
$$;
ALTER FUNCTION vela_verify_stage_worker_member_registration(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_verify_stage_worker_member_registration(jsonb) FROM PUBLIC;

CREATE FUNCTION vela_fence_model_runtime_barrier(
    p_model_residency_id uuid,
    p_old_barrier_generation bigint,
    p_new_barrier_generation bigint,
    p_fenced_at timestamptz
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_active record;
    v_consumed_resource_units bigint;
    v_failure_command jsonb;
BEGIN
    IF p_model_residency_id IS NULL OR p_old_barrier_generation <= 0
       OR p_new_barrier_generation <= p_old_barrier_generation
       OR p_fenced_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_barrier_fence_invalid',
            MESSAGE = 'ModelRuntime barrier fence is invalid';
    END IF;
    FOR v_active IN
        SELECT lease.id AS lease_id,
               lease.attempt_id,
               lease.stage_run_id,
               lease.stage_attempt_id,
               lease.attempt_fence,
               lease.stage_fence,
               run.version AS stage_version,
               physical.state AS physical_state,
               physical.started_at,
               allocation.capacity_vector
        FROM stage_leases AS lease
        JOIN attempts AS attempt ON attempt.id = lease.attempt_id
        JOIN jobs AS job ON job.id = attempt.job_id
        JOIN stage_runs AS run ON run.id = lease.stage_run_id
        JOIN stage_attempts AS physical ON physical.id = lease.stage_attempt_id
        JOIN stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
        WHERE lease.model_residency_id = p_model_residency_id
          AND lease.model_runtime_epoch = p_old_barrier_generation
          AND lease.state = 'ACTIVE'
        ORDER BY job.id, attempt.id, run.id, lease.id
        FOR UPDATE OF job, attempt, run, physical, lease, allocation
    LOOP
        IF v_active.physical_state = 'ASSIGNED' THEN
            PERFORM vela_fence_assigned_stage_for_runtime_epoch(
                v_active.lease_id,
                p_model_residency_id,
                p_old_barrier_generation,
                p_new_barrier_generation,
                p_fenced_at
            );
            CONTINUE;
        END IF;
        IF v_active.started_at IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'model_runtime_active_stage_start_missing',
                MESSAGE = 'Active Stage has no Billable Start for runtime barrier fencing';
        END IF;
        v_consumed_resource_units := GREATEST(
            1,
            CEIL(EXTRACT(EPOCH FROM (p_fenced_at - v_active.started_at)))::bigint
            * GREATEST(
                1,
                COALESCE((v_active.capacity_vector ->> 'gpu_count')::bigint, 1)
            )
        );
        v_failure_command := jsonb_build_object(
            'schema_version', 1,
            'command_kind', 'FAIL',
            'command_id', gen_random_uuid(),
            'attempt_id', v_active.attempt_id,
            'stage_run_id', v_active.stage_run_id,
            'expected_attempt_fence', v_active.attempt_fence,
            'expected_stage_fence', v_active.stage_fence,
            'expected_stage_version', v_active.stage_version,
            'stage_attempt_id', v_active.stage_attempt_id,
            'stage_lease_id', v_active.lease_id,
            'failure_class', 'WORKER_LOST',
            'failure_fingerprint', encode(sha256(convert_to(
                p_model_residency_id::text || ':'
                || p_old_barrier_generation::text || ':'
                || p_new_barrier_generation::text,
                'UTF8'
            )), 'hex'),
            'consumed_resource_units', v_consumed_resource_units,
            'failed_at', p_fenced_at,
            'retry_at', p_fenced_at + interval '1 second'
        );
        PERFORM * FROM vela_apply_stage_command(v_failure_command);
    END LOOP;
END
$$;
ALTER FUNCTION vela_fence_model_runtime_barrier(uuid,bigint,bigint,timestamptz)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_fence_model_runtime_barrier(uuid,bigint,bigint,timestamptz)
    FROM PUBLIC;

CREATE FUNCTION vela_register_stage_worker_runtime(p_evidence jsonb)
RETURNS TABLE (
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    ready boolean,
    reason text,
    barrier_generation bigint,
    leader_worker_member_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_residency model_residencies%ROWTYPE;
    v_worker worker_instances%ROWTYPE;
    v_barrier model_runtime_barriers%ROWTYPE;
    v_registration model_runtime_epoch_registrations%ROWTYPE;
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_worker_member_id uuid;
    v_worker_member_epoch bigint;
    v_local_model_runtime_epoch bigint;
    v_readiness_digest bytea;
    v_spiffe_digest bytea;
    v_device_subset_digest bytea;
    v_prevalidated boolean;
    v_complete boolean;
    v_expected_member_count integer;
    v_leader_worker_member_id uuid;
    v_generation bigint;
    v_max_generation bigint;
    v_max_local_epoch bigint;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_evidence IS NULL OR jsonb_typeof(p_evidence) <> 'object'
       OR (p_evidence ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_epoch_registration_invalid',
            MESSAGE = 'ModelRuntime epoch registration is invalid';
    END IF;
    v_worker_instance_id := (p_evidence ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_evidence ->> 'worker_instance_epoch')::bigint;
    v_worker_member_id := (p_evidence ->> 'worker_member_id')::uuid;
    v_worker_member_epoch := (p_evidence ->> 'worker_member_epoch')::bigint;
    v_local_model_runtime_epoch := (p_evidence ->> 'model_runtime_epoch')::bigint;
    v_readiness_digest := decode(p_evidence ->> 'readiness_evidence_digest', 'hex');
    v_spiffe_digest := decode(p_evidence ->> 'spiffe_id_digest', 'hex');
    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_worker_member_id IS NULL OR v_worker_member_epoch <= 0
       OR v_local_model_runtime_epoch <= 0
       OR octet_length(v_readiness_digest) <> 32
       OR octet_length(v_spiffe_digest) <> 32
       OR jsonb_typeof(p_evidence -> 'devices') <> 'array'
       OR jsonb_array_length(p_evidence -> 'devices') = 0
       OR jsonb_typeof(p_evidence -> 'members') <> 'array'
       OR jsonb_array_length(p_evidence -> 'members') <> 1
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(p_evidence -> 'devices') AS requested(device)
           GROUP BY requested.device ->> 'device_id'
           HAVING count(*) > 1
       )
       OR (p_evidence #>> '{members,0,worker_member_id}')::uuid <>
          v_worker_member_id
       OR (p_evidence #>> '{members,0,member_epoch}')::bigint <>
          v_worker_member_epoch
       OR (p_evidence #>> '{members,0,model_runtime_epoch}')::bigint <>
          v_local_model_runtime_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_epoch_registration_invalid',
            MESSAGE = 'ModelRuntime epoch registration fields are invalid';
    END IF;

    PERFORM vela_lock_model_runtime_epoch_gate(
        (p_evidence ->> 'model_residency_id')::uuid
    );
    SELECT residency.* INTO v_residency
    FROM model_residencies AS residency
    WHERE residency.id = (p_evidence ->> 'model_residency_id')::uuid
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'ModelRuntime residency is not durable Fleet authority'::text,
            NULL::bigint, NULL::uuid;
        RETURN;
    END IF;
    SELECT worker.* INTO v_worker
    FROM worker_instances AS worker
    WHERE worker.id = v_residency.worker_instance_id;
    SELECT member.device_subset_digest INTO v_device_subset_digest
    FROM worker_members AS member
    WHERE member.id = v_worker_member_id
      AND member.worker_instance_id = v_worker_instance_id
      AND member.worker_instance_epoch = v_worker_instance_epoch;
    SELECT count(*), min(member.id::text)::uuid
      INTO v_expected_member_count, v_leader_worker_member_id
    FROM worker_members AS member
    WHERE member.worker_instance_id = v_worker_instance_id
      AND member.worker_instance_epoch = v_worker_instance_epoch
      AND member.readiness = 'READY';

    SELECT verified.ready INTO v_prevalidated
    FROM vela_verify_stage_worker_member_registration(p_evidence) AS verified;
    IF NOT COALESCE(v_prevalidated, false)
       OR v_worker.id IS NULL
       OR v_worker.id <> v_worker_instance_id
       OR v_expected_member_count IS DISTINCT FROM v_worker.desired_member_count
       OR v_device_subset_digest IS NULL THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'ModelRuntime member evidence does not match durable Fleet authority'::text,
            NULL::bigint, NULL::uuid;
        RETURN;
    END IF;

    SELECT max(registration.local_model_runtime_epoch)
      INTO v_max_local_epoch
    FROM model_runtime_epoch_registrations AS registration
    WHERE registration.model_residency_id = v_residency.id
      AND registration.worker_member_id = v_worker_member_id;
    IF v_max_local_epoch IS NOT NULL
       AND v_local_model_runtime_epoch < v_max_local_epoch THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'ModelRuntime member epoch is stale'::text,
            v_residency.model_runtime_epoch, v_leader_worker_member_id;
        RETURN;
    END IF;

    SELECT registration.* INTO v_registration
    FROM model_runtime_epoch_registrations AS registration
    JOIN model_runtime_barriers AS barrier
      ON barrier.model_residency_id = registration.model_residency_id
     AND barrier.barrier_generation = registration.barrier_generation
    WHERE registration.model_residency_id = v_residency.id
      AND registration.barrier_generation = v_residency.model_runtime_epoch
      AND registration.worker_member_id = v_worker_member_id
      AND barrier.state = 'READY'
      AND v_residency.state = 'READY';
    IF FOUND THEN
        IF v_registration.worker_instance_id = v_worker_instance_id
           AND v_registration.worker_instance_epoch = v_worker_instance_epoch
           AND v_registration.worker_member_epoch = v_worker_member_epoch
           AND v_registration.local_model_runtime_epoch = v_local_model_runtime_epoch
           AND v_registration.readiness_evidence_digest = v_readiness_digest
           AND v_registration.spiffe_id_digest = v_spiffe_digest
           AND v_registration.device_subset_digest = v_device_subset_digest THEN
            RETURN QUERY SELECT
                v_worker_instance_id, v_worker_instance_epoch, true,
                'ModelRuntime barrier registration replayed'::text,
                v_residency.model_runtime_epoch, v_leader_worker_member_id;
            RETURN;
        END IF;
        IF v_local_model_runtime_epoch = v_registration.local_model_runtime_epoch THEN
            RETURN QUERY SELECT
                v_worker_instance_id, v_worker_instance_epoch, false,
                'ModelRuntime member registration replay does not match'::text,
                v_residency.model_runtime_epoch, v_leader_worker_member_id;
            RETURN;
        END IF;
    END IF;

    SELECT barrier.* INTO v_barrier
    FROM model_runtime_barriers AS barrier
    WHERE barrier.model_residency_id = v_residency.id
      AND barrier.state = 'WAITING'
    ORDER BY barrier.barrier_generation DESC
    LIMIT 1
    FOR UPDATE;
    IF FOUND THEN
        SELECT registration.* INTO v_registration
        FROM model_runtime_epoch_registrations AS registration
        WHERE registration.model_residency_id = v_residency.id
          AND registration.barrier_generation = v_barrier.barrier_generation
          AND registration.worker_member_id = v_worker_member_id;
        IF FOUND THEN
            IF v_registration.worker_instance_id = v_worker_instance_id
               AND v_registration.worker_instance_epoch = v_worker_instance_epoch
               AND v_registration.worker_member_epoch = v_worker_member_epoch
               AND v_registration.local_model_runtime_epoch = v_local_model_runtime_epoch
               AND v_registration.readiness_evidence_digest = v_readiness_digest
               AND v_registration.spiffe_id_digest = v_spiffe_digest
               AND v_registration.device_subset_digest = v_device_subset_digest THEN
                RETURN QUERY SELECT
                    v_worker_instance_id, v_worker_instance_epoch, false,
                    'ModelRuntime gang barrier is waiting for required members'::text,
                    v_barrier.barrier_generation, v_barrier.leader_worker_member_id;
                RETURN;
            END IF;
            IF v_local_model_runtime_epoch <=
               v_registration.local_model_runtime_epoch THEN
                RETURN QUERY SELECT
                    v_worker_instance_id, v_worker_instance_epoch, false,
                    'ModelRuntime member registration replay does not match'::text,
                    v_barrier.barrier_generation, v_barrier.leader_worker_member_id;
                RETURN;
            END IF;
            UPDATE model_runtime_barriers AS barrier
            SET state = 'SUPERSEDED', superseded_at = v_now
            WHERE barrier.model_residency_id = v_barrier.model_residency_id
              AND barrier.barrier_generation = v_barrier.barrier_generation
              AND barrier.state = 'WAITING';
            v_barrier.model_residency_id := NULL;
        END IF;
    END IF;

    IF v_barrier.model_residency_id IS NULL THEN
        SELECT max(barrier.barrier_generation) INTO v_max_generation
        FROM model_runtime_barriers AS barrier
        WHERE barrier.model_residency_id = v_residency.id;
        IF v_expected_member_count = 1
           AND v_max_generation IS NULL
           AND v_local_model_runtime_epoch = v_residency.model_runtime_epoch THEN
            v_generation := v_residency.model_runtime_epoch;
        ELSE
            v_generation := GREATEST(
                v_residency.model_runtime_epoch + 1,
                COALESCE(v_max_generation + 1, v_residency.model_runtime_epoch + 1)
            );
        END IF;
        INSERT INTO model_runtime_barriers (
            model_residency_id, barrier_generation,
            worker_instance_id, worker_instance_epoch,
            expected_member_count, leader_worker_member_id,
            state, created_at
        ) VALUES (
            v_residency.id, v_generation,
            v_worker_instance_id, v_worker_instance_epoch,
            v_expected_member_count, v_leader_worker_member_id,
            'WAITING', v_now
        ) RETURNING * INTO v_barrier;
        IF v_generation > v_residency.model_runtime_epoch THEN
            PERFORM vela_fence_model_runtime_barrier(
                v_residency.id,
                v_residency.model_runtime_epoch,
                v_generation,
                v_now
            );
            UPDATE model_residencies AS residency
            SET state = 'WARMING', observed_at = v_now,
                observed_by = 'stage-worker-runtime-barrier/'
                    || v_worker_member_id::text
            WHERE residency.id = v_residency.id
              AND residency.model_runtime_epoch = v_residency.model_runtime_epoch;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'model_runtime_barrier_registration_raced',
                    MESSAGE = 'ModelRuntime barrier authority changed during registration';
            END IF;
        END IF;
    END IF;

    INSERT INTO model_runtime_epoch_registrations (
        model_residency_id, barrier_generation,
        worker_instance_id, worker_instance_epoch,
        worker_member_id, worker_member_epoch,
        local_model_runtime_epoch, device_subset_digest,
        readiness_evidence_digest, spiffe_id_digest, registered_at
    ) VALUES (
        v_residency.id, v_barrier.barrier_generation,
        v_worker_instance_id, v_worker_instance_epoch,
        v_worker_member_id, v_worker_member_epoch,
        v_local_model_runtime_epoch, v_device_subset_digest,
        v_readiness_digest, v_spiffe_digest, v_now
    );

    SELECT count(*) = v_barrier.expected_member_count
           AND NOT EXISTS (
               SELECT 1
               FROM worker_members AS member
               LEFT JOIN model_runtime_epoch_registrations AS registration
                 ON registration.model_residency_id = v_barrier.model_residency_id
                AND registration.barrier_generation = v_barrier.barrier_generation
                AND registration.worker_member_id = member.id
                AND registration.worker_member_epoch = member.member_epoch
                AND registration.spiffe_id_digest = member.identity_digest
                AND registration.device_subset_digest = member.device_subset_digest
               WHERE member.worker_instance_id = v_barrier.worker_instance_id
                 AND member.worker_instance_epoch = v_barrier.worker_instance_epoch
                 AND member.readiness = 'READY'
                 AND registration.worker_member_id IS NULL
           )
      INTO v_complete
    FROM model_runtime_epoch_registrations AS registration
    WHERE registration.model_residency_id = v_barrier.model_residency_id
      AND registration.barrier_generation = v_barrier.barrier_generation;

    IF NOT COALESCE(v_complete, false) THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'ModelRuntime gang barrier is waiting for required members'::text,
            v_barrier.barrier_generation, v_barrier.leader_worker_member_id;
        RETURN;
    END IF;

    UPDATE model_runtime_barriers AS barrier
    SET state = 'READY', ready_at = v_now
    WHERE barrier.model_residency_id = v_barrier.model_residency_id
      AND barrier.barrier_generation = v_barrier.barrier_generation
      AND barrier.state = 'WAITING';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'model_runtime_barrier_registration_raced',
            MESSAGE = 'ModelRuntime barrier changed during completion';
    END IF;
    UPDATE model_residencies AS residency
    SET model_runtime_epoch = v_barrier.barrier_generation,
        state = 'READY', ready_at = v_now, observed_at = v_now,
        observed_by = 'stage-worker-runtime-barrier/' || v_worker_member_id::text
    WHERE residency.id = v_residency.id
      AND residency.model_runtime_epoch = v_residency.model_runtime_epoch
      AND residency.state IN ('READY', 'WARMING');
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'model_runtime_barrier_registration_raced',
            MESSAGE = 'ModelRuntime residency changed during barrier completion';
    END IF;

    RETURN QUERY SELECT
        v_worker_instance_id, v_worker_instance_epoch, true,
        'ModelRuntime gang barrier is complete'::text,
        v_barrier.barrier_generation, v_barrier.leader_worker_member_id;
END
$$;
ALTER FUNCTION vela_register_stage_worker_runtime(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_register_stage_worker_runtime(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_register_stage_worker_runtime(jsonb)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE EXECUTE ON FUNCTION vela_register_stage_worker_runtime(jsonb)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_register_stage_worker_runtime(jsonb);
DROP FUNCTION vela_fence_model_runtime_barrier(uuid,bigint,bigint,timestamptz);
DROP FUNCTION vela_verify_stage_worker_member_registration(jsonb);

ALTER TABLE model_runtime_epoch_registrations
    DROP CONSTRAINT model_runtime_epoch_registrations_barrier_fk,
    DROP CONSTRAINT model_runtime_epoch_registrations_device_subset_digest_check,
    DROP CONSTRAINT model_runtime_epoch_registrations_local_epoch_check,
    DROP COLUMN device_subset_digest,
    DROP COLUMN local_model_runtime_epoch;
ALTER TABLE model_runtime_epoch_registrations
    RENAME COLUMN barrier_generation TO model_runtime_epoch;

DROP TABLE model_runtime_barriers;
DROP TYPE model_runtime_barrier_state;
REVOKE SELECT ON worker_member_devices FROM vela_attempt_coordinator_owner;

ALTER FUNCTION vela_register_stage_worker_runtime_v1(jsonb)
    RENAME TO vela_register_stage_worker_runtime;
GRANT EXECUTE ON FUNCTION vela_register_stage_worker_runtime(jsonb)
    TO vela_stage_worker_control;
-- +goose StatementEnd
