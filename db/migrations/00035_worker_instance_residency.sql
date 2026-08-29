-- +goose Up
-- +goose StatementBegin
CREATE TYPE compute_node_lifecycle_state AS ENUM ('ACTIVE', 'DRAINING', 'FENCED', 'RETIRED');
CREATE TYPE device_kind AS ENUM ('GPU', 'CPU');
CREATE TYPE device_health_state AS ENUM ('HEALTHY', 'DEGRADED', 'FENCED', 'MISSING');
CREATE TYPE capacity_pool_state AS ENUM ('ACTIVE', 'DRAINING', 'RETIRED');
CREATE TYPE worker_bundle_state AS ENUM (
    'PROPOSED', 'APPLYING', 'READY', 'DRAINING', 'FAILED', 'RETIRED'
);
CREATE TYPE worker_instance_lifecycle_state AS ENUM (
    'PROVISIONING', 'WARMING', 'READY', 'DRAINING', 'FENCED', 'RETIRED'
);
CREATE TYPE worker_instance_reachability_state AS ENUM (
    'CONNECTED', 'DISCONNECTED', 'UNREACHABLE'
);
CREATE TYPE worker_member_readiness_state AS ENUM ('JOINING', 'READY', 'FAILED', 'FENCED');
CREATE TYPE model_residency_state AS ENUM (
    'LOADING', 'WARMING', 'READY', 'DRAINING', 'RELEASED', 'FAILED'
);
CREATE TYPE model_residency_release_reason AS ENUM (
    'SHUTDOWN', 'HARDWARE_RESPONSE', 'SECURITY_RESPONSE',
    'REVISION_ROLLOUT', 'CAPACITY_CHANGE'
);
CREATE TYPE model_residency_release_state AS ENUM ('APPROVED', 'COMPLETED', 'CANCELED');

CREATE TABLE capacity_pools (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    stage_profile_revision_id uuid NOT NULL REFERENCES stage_profile_revisions(id),
    resource_class text NOT NULL CHECK (resource_class IN ('GPU', 'CPU')),
    security_class text NOT NULL CHECK (length(security_class) BETWEEN 1 AND 100),
    region text NOT NULL CHECK (length(region) BETWEEN 1 AND 100),
    max_ready_queue_depth integer NOT NULL CHECK (max_ready_queue_depth > 0),
    state capacity_pool_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id),
    UNIQUE (id, stage_profile_revision_id)
);

CREATE TABLE residency_proposals (
    id uuid PRIMARY KEY,
    schema_version integer NOT NULL CHECK (schema_version = 1),
    input_digest bytea NOT NULL CHECK (octet_length(input_digest) = 32),
    confidence_ppm integer NOT NULL CHECK (confidence_ppm BETWEEN 0 AND 1000000),
    expires_at timestamptz NOT NULL,
    min_capacity jsonb NOT NULL CHECK (jsonb_typeof(min_capacity) = 'object'),
    desired_capacity jsonb NOT NULL CHECK (jsonb_typeof(desired_capacity) = 'object'),
    max_capacity jsonb NOT NULL CHECK (jsonb_typeof(max_capacity) = 'object'),
    cooldown_seconds bigint NOT NULL CHECK (cooldown_seconds > 0),
    budget_micro_units bigint NOT NULL CHECK (budget_micro_units >= 0),
    reason_codes text[] NOT NULL CHECK (cardinality(reason_codes) > 0),
    proposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    proposed_by text NOT NULL CHECK (length(proposed_by) BETWEEN 1 AND 500),
    CHECK (expires_at > proposed_at)
);

CREATE TABLE residency_plan_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    source_proposal_id uuid REFERENCES residency_proposals(id),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    approval_evidence_digest bytea NOT NULL CHECK (
        octet_length(approval_evidence_digest) = 32
    ),
    approved_at timestamptz NOT NULL,
    approved_by text NOT NULL CHECK (length(approved_by) BETWEEN 1 AND 500),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

ALTER TABLE capacity_pools
    ADD COLUMN residency_plan_revision_id uuid REFERENCES residency_plan_revisions(id);

CREATE FUNCTION vela_reject_residency_planning_history_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'residency_planning_history_immutable',
        MESSAGE = TG_TABLE_NAME || ' is append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_residency_planning_history_mutation() FROM PUBLIC;
CREATE TRIGGER residency_proposals_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON residency_proposals
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_residency_planning_history_mutation();
CREATE TRIGGER residency_plan_revisions_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON residency_plan_revisions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_residency_planning_history_mutation();

CREATE TABLE worker_bundles (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    plan_revision text NOT NULL CHECK (length(plan_revision) BETWEEN 1 AND 300),
    residency_plan_revision_id uuid REFERENCES residency_plan_revisions(id),
    compute_node_id uuid,
    desired_generation bigint NOT NULL CHECK (desired_generation > 0),
    observed_generation bigint NOT NULL CHECK (
        observed_generation >= 0 AND observed_generation <= desired_generation
    ),
    lifecycle_state worker_bundle_state NOT NULL,
    layout_digest bytea NOT NULL CHECK (octet_length(layout_digest) = 32),
    approved_by text NOT NULL CHECK (length(approved_by) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, plan_revision)
);

CREATE TABLE compute_nodes (
    id uuid PRIMARY KEY,
    node_identity text NOT NULL CHECK (length(node_identity) BETWEEN 1 AND 500),
    region text NOT NULL CHECK (length(region) BETWEEN 1 AND 100),
    network_domain text NOT NULL CHECK (length(network_domain) BETWEEN 1 AND 200),
    fault_domain text NOT NULL CHECK (length(fault_domain) BETWEEN 1 AND 200),
    lifecycle_state compute_node_lifecycle_state NOT NULL DEFAULT 'ACTIVE',
    lifecycle_epoch bigint NOT NULL CHECK (lifecycle_epoch > 0),
    agent_session_epoch bigint NOT NULL CHECK (agent_session_epoch > 0),
    attestation_digest bytea NOT NULL CHECK (octet_length(attestation_digest) = 32),
    observed_at timestamptz NOT NULL,
    observed_by text NOT NULL CHECK (length(observed_by) BETWEEN 1 AND 500),
    UNIQUE (node_identity),
    UNIQUE (id, lifecycle_epoch)
);

ALTER TABLE worker_bundles
    ADD CONSTRAINT worker_bundles_compute_node
    FOREIGN KEY (compute_node_id) REFERENCES compute_nodes(id);

CREATE TABLE devices (
    id uuid PRIMARY KEY,
    compute_node_id uuid NOT NULL REFERENCES compute_nodes(id),
    kind device_kind NOT NULL,
    gpu_uuid text,
    pci_bdf text,
    device_epoch bigint NOT NULL CHECK (device_epoch > 0),
    health device_health_state NOT NULL,
    attestation_digest bytea NOT NULL CHECK (octet_length(attestation_digest) = 32),
    observed_at timestamptz NOT NULL,
    observed_by text NOT NULL CHECK (length(observed_by) BETWEEN 1 AND 500),
    CHECK (
        (kind = 'GPU'
         AND gpu_uuid ~ '^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$'
         AND pci_bdf ~ '^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}[.][0-7]$')
        OR (kind = 'CPU' AND gpu_uuid IS NULL AND pci_bdf IS NULL)
    ),
    UNIQUE (gpu_uuid),
    UNIQUE (compute_node_id, pci_bdf),
    UNIQUE (id, device_epoch)
);

CREATE TABLE device_sets (
    id uuid PRIMARY KEY,
    membership_digest bytea NOT NULL CHECK (octet_length(membership_digest) = 32),
    topology_digest bytea NOT NULL CHECK (octet_length(topology_digest) = 32),
    device_count integer NOT NULL CHECK (device_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (membership_digest, topology_digest),
    UNIQUE (id, membership_digest)
);

CREATE TABLE device_set_members (
    device_set_id uuid NOT NULL REFERENCES device_sets(id),
    device_id uuid NOT NULL REFERENCES devices(id),
    device_epoch bigint NOT NULL CHECK (device_epoch > 0),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (device_set_id, device_id),
    UNIQUE (device_set_id, ordinal),
    UNIQUE (device_set_id, device_id, device_epoch),
    FOREIGN KEY (device_id, device_epoch) REFERENCES devices(id, device_epoch)
);

CREATE TABLE worker_instances (
    id uuid PRIMARY KEY,
    worker_profile_revision_id uuid NOT NULL REFERENCES worker_profile_revisions(id),
    residency_plan_revision_id uuid REFERENCES residency_plan_revisions(id),
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    worker_bundle_id uuid REFERENCES worker_bundles(id),
    device_set_id uuid REFERENCES device_sets(id),
    lifecycle_state worker_instance_lifecycle_state NOT NULL,
    reachability_state worker_instance_reachability_state NOT NULL,
    instance_epoch bigint NOT NULL CHECK (instance_epoch > 0),
    control_session_epoch bigint NOT NULL CHECK (control_session_epoch > 0),
    desired_member_count integer NOT NULL CHECK (desired_member_count > 0),
    desired_device_count integer NOT NULL CHECK (desired_device_count > 0),
    membership_digest bytea CHECK (
        membership_digest IS NULL OR octet_length(membership_digest) = 32
    ),
    device_set_digest bytea CHECK (
        device_set_digest IS NULL OR octet_length(device_set_digest) = 32
    ),
    control_session_id text CHECK (
        control_session_id IS NULL OR length(control_session_id) BETWEEN 1 AND 500
    ),
    observed_at timestamptz,
    observed_by text CHECK (observed_by IS NULL OR length(observed_by) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        lifecycle_state IN ('PROVISIONING', 'FENCED', 'RETIRED')
        OR (device_set_id IS NOT NULL AND membership_digest IS NOT NULL
            AND device_set_digest IS NOT NULL AND observed_at IS NOT NULL)
    )
);

CREATE TABLE worker_instance_epochs (
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    epoch bigint NOT NULL CHECK (epoch > 0),
    device_set_id uuid NOT NULL REFERENCES device_sets(id),
    membership_digest bytea NOT NULL CHECK (octet_length(membership_digest) = 32),
    device_set_digest bytea NOT NULL CHECK (octet_length(device_set_digest) = 32),
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    end_reason text CHECK (end_reason IS NULL OR length(end_reason) BETWEEN 1 AND 500),
    PRIMARY KEY (worker_instance_id, epoch),
    UNIQUE (worker_instance_id, device_set_id, epoch),
    CHECK ((ended_at IS NULL) = (end_reason IS NULL)),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE TABLE worker_members (
    id uuid PRIMARY KEY,
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    member_key text NOT NULL CHECK (
        length(member_key) BETWEEN 1 AND 100
        AND member_key ~ '^[a-z][a-z0-9_-]*$'
    ),
    compute_node_id uuid NOT NULL REFERENCES compute_nodes(id),
    worker_bundle_id uuid REFERENCES worker_bundles(id),
    member_epoch bigint NOT NULL CHECK (member_epoch > 0),
    device_subset_digest bytea NOT NULL CHECK (octet_length(device_subset_digest) = 32),
    identity_digest bytea NOT NULL CHECK (octet_length(identity_digest) = 32),
    readiness worker_member_readiness_state NOT NULL,
    observed_at timestamptz NOT NULL,
    observed_by text NOT NULL CHECK (length(observed_by) BETWEEN 1 AND 500),
    UNIQUE (worker_instance_id, member_key),
    UNIQUE (worker_instance_id, id),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch)
);

CREATE TABLE worker_member_devices (
    worker_instance_id uuid NOT NULL,
    worker_member_id uuid NOT NULL,
    device_set_id uuid NOT NULL,
    device_id uuid NOT NULL,
    device_epoch bigint NOT NULL CHECK (device_epoch > 0),
    PRIMARY KEY (worker_member_id, device_id),
    UNIQUE (worker_instance_id, device_id),
    FOREIGN KEY (worker_instance_id, worker_member_id)
        REFERENCES worker_members(worker_instance_id, id),
    FOREIGN KEY (device_set_id, device_id, device_epoch)
        REFERENCES device_set_members(device_set_id, device_id, device_epoch)
);

CREATE TABLE active_device_bindings (
    device_id uuid PRIMARY KEY,
    device_epoch bigint NOT NULL CHECK (device_epoch > 0),
    device_set_id uuid NOT NULL,
    worker_instance_id uuid NOT NULL,
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    bound_at timestamptz NOT NULL,
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    FOREIGN KEY (device_set_id, device_id, device_epoch)
        REFERENCES device_set_members(device_set_id, device_id, device_epoch),
    FOREIGN KEY (worker_instance_id, device_set_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, device_set_id, epoch),
    UNIQUE (worker_instance_id, device_id)
);

CREATE TABLE model_residencies (
    id uuid PRIMARY KEY,
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    model_component_revision text NOT NULL CHECK (
        length(model_component_revision) BETWEEN 1 AND 300
    ),
    runtime_identity text NOT NULL CHECK (length(runtime_identity) BETWEEN 1 AND 500),
    runtime_image_digest text NOT NULL CHECK (
        runtime_image_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    model_runtime_epoch bigint NOT NULL CHECK (model_runtime_epoch > 0),
    state model_residency_state NOT NULL,
    warmup_evidence_digest bytea NOT NULL CHECK (octet_length(warmup_evidence_digest) = 32),
    canary_evidence_digest bytea NOT NULL CHECK (octet_length(canary_evidence_digest) = 32),
    ready_at timestamptz,
    released_at timestamptz,
    observed_at timestamptz NOT NULL,
    observed_by text NOT NULL CHECK (length(observed_by) BETWEEN 1 AND 500),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    CHECK (state <> 'READY' OR ready_at IS NOT NULL),
    CHECK ((state = 'RELEASED') = (released_at IS NOT NULL))
);

CREATE UNIQUE INDEX model_residencies_one_live_component_idx
    ON model_residencies (worker_instance_id, model_component_revision)
    WHERE state <> 'RELEASED';

CREATE TABLE worker_instance_drain_operations (
    worker_instance_id uuid NOT NULL,
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
    requested_by text NOT NULL CHECK (length(requested_by) BETWEEN 1 AND 500),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (worker_instance_id, worker_instance_epoch),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch)
);

CREATE TABLE model_residency_release_operations (
    id uuid PRIMARY KEY,
    model_residency_id uuid NOT NULL REFERENCES model_residencies(id),
    worker_instance_id uuid NOT NULL,
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    reason model_residency_release_reason NOT NULL,
    approved_plan_revision text NOT NULL CHECK (
        length(approved_plan_revision) BETWEEN 1 AND 300
    ),
    approved_by text NOT NULL CHECK (length(approved_by) BETWEEN 1 AND 500),
    planned_offline_seconds bigint NOT NULL CHECK (planned_offline_seconds > 0),
    measured_reload_seconds bigint NOT NULL CHECK (measured_reload_seconds > 0),
    break_even_evidence_digest bytea NOT NULL CHECK (
        octet_length(break_even_evidence_digest) = 32
    ),
    state model_residency_release_state NOT NULL,
    approved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    completion_evidence_digest bytea CHECK (
        completion_evidence_digest IS NULL OR octet_length(completion_evidence_digest) = 32
    ),
    completed_by text CHECK (
        completed_by IS NULL OR length(completed_by) BETWEEN 1 AND 500
    ),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    CHECK (
        reason <> 'CAPACITY_CHANGE'
        OR planned_offline_seconds > measured_reload_seconds
    ),
    CHECK (
        (state = 'COMPLETED') =
        (completed_at IS NOT NULL AND completion_evidence_digest IS NOT NULL
         AND completed_by IS NOT NULL)
    )
);

CREATE UNIQUE INDEX model_residency_release_one_active_idx
    ON model_residency_release_operations (model_residency_id)
    WHERE state = 'APPROVED';

CREATE TABLE worker_instance_pod_mutation_authorizations (
    request_uid text PRIMARY KEY CHECK (
        length(request_uid) BETWEEN 1 AND 200 AND btrim(request_uid) = request_uid
    ),
    actor_identity text NOT NULL CHECK (
        length(actor_identity) BETWEEN 1 AND 500 AND btrim(actor_identity) = actor_identity
    ),
    operation fleet_mutation_operation NOT NULL CHECK (
        operation IN ('DELETE', 'REMOVE_FINALIZER')
    ),
    kubernetes_uid text NOT NULL CHECK (
        length(kubernetes_uid) BETWEEN 1 AND 200 AND btrim(kubernetes_uid) = kubernetes_uid
    ),
    namespace text NOT NULL CHECK (
        length(namespace) BETWEEN 1 AND 253 AND btrim(namespace) = namespace
    ),
    name text NOT NULL CHECK (
        length(name) BETWEEN 1 AND 253 AND btrim(name) = name
    ),
    worker_instance_id uuid NOT NULL,
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    residency_plan_revision_id uuid NOT NULL REFERENCES residency_plan_revisions(id),
    worker_bundle_id uuid NOT NULL REFERENCES worker_bundles(id),
    worker_member_id uuid NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    authorized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    FOREIGN KEY (worker_instance_id, worker_member_id)
        REFERENCES worker_members(worker_instance_id, id)
);

CREATE FUNCTION vela_reject_worker_instance_pod_mutation_authorization_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'worker_instance_pod_mutation_authorization_immutable',
        MESSAGE = 'WorkerInstance Pod mutation authorization receipts are append-only';
END
$$;
REVOKE ALL ON FUNCTION
    vela_reject_worker_instance_pod_mutation_authorization_mutation() FROM PUBLIC;
CREATE TRIGGER worker_instance_pod_mutation_authorizations_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_instance_pod_mutation_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION
    vela_reject_worker_instance_pod_mutation_authorization_mutation();

CREATE TABLE capacity_observations (
    worker_instance_id uuid NOT NULL,
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    observation_sequence bigint NOT NULL CHECK (observation_sequence > 0),
    capacity_vector jsonb NOT NULL CHECK (jsonb_typeof(capacity_vector) = 'object'),
    observed_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    observed_by text NOT NULL CHECK (length(observed_by) BETWEEN 1 AND 500),
    PRIMARY KEY (worker_instance_id, observation_sequence),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    CHECK (expires_at > observed_at)
);

CREATE INDEX capacity_observations_fresh_idx
    ON capacity_observations (worker_instance_id, expires_at DESC);

CREATE FUNCTION vela_record_residency_proposal(p_proposal jsonb)
RETURNS TABLE (proposal_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_id uuid;
    v_input_digest bytea;
    v_reason_codes text[];
    v_existing public.residency_proposals%ROWTYPE;
BEGIN
    IF p_proposal IS NULL OR jsonb_typeof(p_proposal) <> 'object'
       OR (p_proposal ->> 'schema_version')::integer <> 1
       OR p_proposal ->> 'input_digest' IS NULL
       OR p_proposal ->> 'expires_at' IS NULL
       OR jsonb_typeof(p_proposal -> 'min_capacity') <> 'object'
       OR jsonb_typeof(p_proposal -> 'desired_capacity') <> 'object'
       OR jsonb_typeof(p_proposal -> 'max_capacity') <> 'object'
       OR jsonb_typeof(p_proposal -> 'reason_codes') <> 'array' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'residency_proposal_invalid',
            MESSAGE = 'ResidencyProposal schema is invalid';
    END IF;
    v_id := (p_proposal ->> 'id')::uuid;
    v_input_digest := decode(p_proposal ->> 'input_digest', 'hex');
    SELECT array_agg(reason ORDER BY reason) INTO v_reason_codes
    FROM jsonb_array_elements_text(p_proposal -> 'reason_codes') AS reason;
    IF v_id IS NULL OR octet_length(v_input_digest) <> 32
       OR (p_proposal ->> 'confidence_ppm')::integer NOT BETWEEN 0 AND 1000000
       OR (p_proposal ->> 'cooldown_seconds')::bigint <= 0
       OR (p_proposal ->> 'budget_micro_units')::bigint < 0
       OR cardinality(v_reason_codes) = 0
       OR p_proposal ->> 'proposed_by' IS NULL
       OR length(p_proposal ->> 'proposed_by') NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'residency_proposal_invalid',
            MESSAGE = 'ResidencyProposal fields are invalid';
    END IF;

    SELECT proposal.* INTO v_existing
    FROM public.residency_proposals AS proposal
    WHERE proposal.id = v_id;
    IF FOUND THEN
        IF v_existing.input_digest <> v_input_digest
           OR v_existing.confidence_ppm <> (p_proposal ->> 'confidence_ppm')::integer
           OR v_existing.expires_at <> (p_proposal ->> 'expires_at')::timestamptz
           OR v_existing.min_capacity <> p_proposal -> 'min_capacity'
           OR v_existing.desired_capacity <> p_proposal -> 'desired_capacity'
           OR v_existing.max_capacity <> p_proposal -> 'max_capacity'
           OR v_existing.cooldown_seconds <> (p_proposal ->> 'cooldown_seconds')::bigint
           OR v_existing.budget_micro_units <> (p_proposal ->> 'budget_micro_units')::bigint
           OR v_existing.reason_codes <> v_reason_codes
           OR v_existing.proposed_by <> p_proposal ->> 'proposed_by' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'residency_proposal_replay_conflict',
                MESSAGE = 'ResidencyProposal replay does not match';
        END IF;
        RETURN QUERY SELECT v_existing.id;
        RETURN;
    END IF;

    INSERT INTO public.residency_proposals (
        id, schema_version, input_digest, confidence_ppm, expires_at,
        min_capacity, desired_capacity, max_capacity, cooldown_seconds,
        budget_micro_units, reason_codes, proposed_by
    ) VALUES (
        v_id, 1, v_input_digest, (p_proposal ->> 'confidence_ppm')::integer,
        (p_proposal ->> 'expires_at')::timestamptz,
        p_proposal -> 'min_capacity', p_proposal -> 'desired_capacity',
        p_proposal -> 'max_capacity', (p_proposal ->> 'cooldown_seconds')::bigint,
        (p_proposal ->> 'budget_micro_units')::bigint, v_reason_codes,
        p_proposal ->> 'proposed_by'
    );
    RETURN QUERY SELECT v_id;
END
$$;

CREATE FUNCTION vela_apply_residency_plan(p_plan jsonb)
RETURNS TABLE (
    plan_revision_id uuid,
    worker_instance_count integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_id uuid;
    v_source_proposal_id uuid;
    v_content_digest bytea;
    v_approval_digest bytea;
    v_existing public.residency_plan_revisions%ROWTYPE;
    v_pool jsonb;
    v_bundle jsonb;
    v_planned_worker jsonb;
    v_profile public.worker_profile_revisions%ROWTYPE;
    v_worker_count integer;
    v_plan_revision_text text;
BEGIN
    IF p_plan IS NULL OR jsonb_typeof(p_plan) <> 'object'
       OR (p_plan ->> 'schema_version')::integer <> 1
       OR p_plan ->> 'approval_evidence_digest' IS NULL
       OR p_plan ->> 'approved_by' IS NULL
       OR p_plan ->> 'approved_at' IS NULL
       OR jsonb_typeof(p_plan -> 'capacity_pools') <> 'array'
       OR jsonb_typeof(p_plan -> 'worker_bundles') <> 'array'
       OR jsonb_typeof(p_plan -> 'worker_instances') <> 'array' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'residency_plan_approval_required',
            MESSAGE = 'ResidencyPlan requires explicit approval evidence';
    END IF;
    v_id := (p_plan ->> 'id')::uuid;
    v_content_digest := decode(p_plan ->> 'content_digest', 'hex');
    v_approval_digest := decode(p_plan ->> 'approval_evidence_digest', 'hex');
    v_source_proposal_id := NULLIF(p_plan ->> 'source_proposal_id', '')::uuid;
    v_worker_count := jsonb_array_length(p_plan -> 'worker_instances');
    v_plan_revision_text := (p_plan ->> 'stable_id') || '@' || (p_plan ->> 'revision');
    IF v_id IS NULL OR octet_length(v_content_digest) <> 32
       OR octet_length(v_approval_digest) <> 32
       OR p_plan ->> 'stable_id' IS NULL
       OR length(p_plan ->> 'stable_id') NOT BETWEEN 1 AND 100
       OR (p_plan ->> 'revision')::integer <= 0
       OR length(p_plan ->> 'approved_by') NOT BETWEEN 1 AND 500
       OR v_worker_count <= 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'residency_plan_approval_required',
            MESSAGE = 'ResidencyPlan approval or layout is incomplete';
    END IF;
    IF v_source_proposal_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM public.residency_proposals AS proposal
        WHERE proposal.id = v_source_proposal_id
          AND proposal.expires_at > statement_timestamp()
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'residency_plan_proposal_stale',
            MESSAGE = 'ResidencyPlan source proposal is missing or expired';
    END IF;

    SELECT plan.* INTO v_existing
    FROM public.residency_plan_revisions AS plan
    WHERE plan.id = v_id;
    IF FOUND THEN
        IF v_existing.stable_id <> p_plan ->> 'stable_id'
           OR v_existing.revision <> (p_plan ->> 'revision')::integer
           OR v_existing.source_proposal_id IS DISTINCT FROM v_source_proposal_id
           OR v_existing.content_digest <> v_content_digest
           OR v_existing.approval_evidence_digest <> v_approval_digest
           OR v_existing.approved_at <> (p_plan ->> 'approved_at')::timestamptz
           OR v_existing.approved_by <> p_plan ->> 'approved_by' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'residency_plan_replay_conflict',
                MESSAGE = 'ResidencyPlan replay does not match';
        END IF;
        RETURN QUERY SELECT
            v_existing.id,
            (SELECT count(*)::integer FROM public.worker_instances AS worker
             WHERE worker.residency_plan_revision_id = v_existing.id);
        RETURN;
    END IF;

    INSERT INTO public.residency_plan_revisions (
        id, stable_id, revision, source_proposal_id, content_digest,
        approval_evidence_digest, approved_at, approved_by
    ) VALUES (
        v_id, p_plan ->> 'stable_id', (p_plan ->> 'revision')::integer,
        v_source_proposal_id, v_content_digest, v_approval_digest,
        (p_plan ->> 'approved_at')::timestamptz, p_plan ->> 'approved_by'
    );

    FOR v_pool IN SELECT value FROM jsonb_array_elements(p_plan -> 'capacity_pools') LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM public.stage_profile_revisions AS profile
            JOIN public.stage_definition_revisions AS definition
              ON definition.id = profile.stage_definition_revision_id
            WHERE profile.id = (v_pool ->> 'stage_profile_revision_id')::uuid
              AND profile.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
              AND definition.resource_class = v_pool ->> 'resource_class'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'capacity_pool_profile_not_certified',
                MESSAGE = 'CapacityPool StageProfile is not certified for its resource class';
        END IF;
        INSERT INTO public.capacity_pools (
            id, stable_id, stage_profile_revision_id, resource_class,
            security_class, region, max_ready_queue_depth, state,
            residency_plan_revision_id
        ) VALUES (
            (v_pool ->> 'id')::uuid, v_pool ->> 'stable_id',
            (v_pool ->> 'stage_profile_revision_id')::uuid,
            v_pool ->> 'resource_class', v_pool ->> 'security_class',
            v_pool ->> 'region', (v_pool ->> 'max_ready_queue_depth')::integer,
            'ACTIVE', v_id
        );
    END LOOP;

    FOR v_bundle IN SELECT value FROM jsonb_array_elements(p_plan -> 'worker_bundles') LOOP
        INSERT INTO public.worker_bundles (
            id, stable_id, plan_revision, residency_plan_revision_id,
            desired_generation, observed_generation, lifecycle_state,
            layout_digest, approved_by
        ) VALUES (
            (v_bundle ->> 'id')::uuid, v_bundle ->> 'stable_id',
            v_plan_revision_text, v_id,
            (v_bundle ->> 'desired_generation')::bigint, 0, 'APPLYING',
            decode(v_bundle ->> 'layout_digest', 'hex'), p_plan ->> 'approved_by'
        );
    END LOOP;

    FOR v_planned_worker IN SELECT value
        FROM jsonb_array_elements(p_plan -> 'worker_instances')
    LOOP
        SELECT profile.* INTO v_profile
        FROM public.worker_profile_revisions AS profile
        JOIN public.capacity_pools AS pool
          ON pool.id = (v_planned_worker ->> 'capacity_pool_id')::uuid
        JOIN public.stage_profile_revisions AS stage_profile
          ON stage_profile.id = pool.stage_profile_revision_id
         AND stage_profile.worker_profile_revision_id = profile.id
        WHERE profile.id = (v_planned_worker ->> 'worker_profile_revision_id')::uuid
          AND profile.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          AND pool.residency_plan_revision_id = v_id;
        IF NOT FOUND
           OR v_profile.member_count <> (v_planned_worker ->> 'desired_member_count')::integer
           OR v_profile.device_count <> (v_planned_worker ->> 'desired_device_count')::integer THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'worker_instance_plan_profile_mismatch',
                MESSAGE = 'WorkerInstance plan does not match its certified WorkerProfile';
        END IF;
        INSERT INTO public.worker_instances (
            id, worker_profile_revision_id, residency_plan_revision_id,
            capacity_pool_id, worker_bundle_id, lifecycle_state, reachability_state,
            instance_epoch, control_session_epoch, desired_member_count,
            desired_device_count
        ) VALUES (
            (v_planned_worker ->> 'id')::uuid, v_profile.id, v_id,
            (v_planned_worker ->> 'capacity_pool_id')::uuid,
            (v_planned_worker ->> 'worker_bundle_id')::uuid,
            'PROVISIONING', 'DISCONNECTED', 1, 1,
            v_profile.member_count, v_profile.device_count
        );
    END LOOP;

    RETURN QUERY SELECT v_id, v_worker_count;
END
$$;

CREATE FUNCTION vela_observe_worker_instance(p_evidence jsonb)
RETURNS TABLE (
    worker_instance_id uuid,
    instance_epoch bigint,
    control_session_epoch bigint,
    model_runtime_epoch bigint,
    readiness worker_instance_lifecycle_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_profile public.worker_profile_revisions%ROWTYPE;
    v_device_set_id uuid;
    v_membership_digest bytea;
    v_topology_digest bytea;
    v_device_set_digest bytea;
    v_device jsonb;
    v_member jsonb;
    v_residency jsonb;
    v_capacity jsonb;
    v_device_id uuid;
    v_device_epoch bigint;
    v_node_id uuid;
    v_member_id uuid;
    v_member_device_id_text text;
    v_model_runtime_epoch bigint := 0;
    v_bound boolean;
    v_observed_at timestamptz;
    v_observed_by text;
    v_device_count integer;
    v_member_count integer;
BEGIN
    IF p_evidence IS NULL OR jsonb_typeof(p_evidence) <> 'object'
       OR (p_evidence ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_evidence_invalid',
            MESSAGE = 'WorkerInstance evidence must use schema version 1';
    END IF;

    v_observed_at := (p_evidence ->> 'observed_at')::timestamptz;
    v_observed_by := p_evidence ->> 'observed_by';
    IF v_observed_at IS NULL OR v_observed_by IS NULL
       OR length(v_observed_by) NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_evidence_invalid',
            MESSAGE = 'WorkerInstance observation identity is incomplete';
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = (p_evidence ->> 'worker_instance_id')::uuid
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'worker_instance_plan_required',
            MESSAGE = 'WorkerInstance must exist in an approved Fleet plan';
    END IF;
    IF v_worker.lifecycle_state IN ('FENCED', 'RETIRED')
       OR v_worker.instance_epoch <> (p_evidence ->> 'instance_epoch')::bigint
       OR v_worker.control_session_epoch <> (p_evidence ->> 'control_session_epoch')::bigint THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_epoch_conflict',
            MESSAGE = 'WorkerInstance evidence is stale';
    END IF;

    SELECT profile.* INTO STRICT v_profile
    FROM public.worker_profile_revisions AS profile
    WHERE profile.id = v_worker.worker_profile_revision_id
      AND profile.state IN ('CERTIFIED', 'CANARY', 'ACTIVE');

    IF jsonb_typeof(p_evidence -> 'device_set') <> 'object'
       OR jsonb_typeof(p_evidence #> '{device_set,devices}') <> 'array'
       OR jsonb_typeof(p_evidence -> 'members') <> 'array'
       OR jsonb_typeof(p_evidence -> 'residencies') <> 'array'
       OR jsonb_typeof(p_evidence -> 'capacity') <> 'object' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_evidence_invalid',
            MESSAGE = 'WorkerInstance evidence collections are incomplete';
    END IF;

    v_device_count := jsonb_array_length(p_evidence #> '{device_set,devices}');
    v_member_count := jsonb_array_length(p_evidence -> 'members');
    IF v_device_count <> v_worker.desired_device_count
       OR v_device_count <> v_profile.device_count
       OR v_member_count <> v_worker.desired_member_count
       OR v_member_count <> v_profile.member_count THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'worker_instance_membership_incomplete',
            MESSAGE = 'WorkerInstance evidence does not cover its certified membership';
    END IF;

    v_device_set_id := (p_evidence #>> '{device_set,id}')::uuid;
    v_membership_digest := decode(p_evidence #>> '{device_set,membership_digest}', 'hex');
    v_topology_digest := decode(p_evidence #>> '{device_set,topology_digest}', 'hex');
    v_device_set_digest := pg_catalog.sha256(v_membership_digest || v_topology_digest);
    IF octet_length(v_membership_digest) <> 32 OR octet_length(v_topology_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_evidence_invalid',
            MESSAGE = 'DeviceSet digests must be SHA-256 values';
    END IF;

    INSERT INTO public.device_sets (
        id, membership_digest, topology_digest, device_count
    ) VALUES (
        v_device_set_id, v_membership_digest, v_topology_digest, v_device_count
    )
    ON CONFLICT (id) DO NOTHING;
    IF NOT EXISTS (
        SELECT 1 FROM public.device_sets AS device_set
        WHERE device_set.id = v_device_set_id
          AND device_set.membership_digest = v_membership_digest
          AND device_set.topology_digest = v_topology_digest
          AND device_set.device_count = v_device_count
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'device_set_identity_conflict',
            MESSAGE = 'DeviceSet identity cannot change';
    END IF;

    FOR v_device IN SELECT value FROM jsonb_array_elements(
        p_evidence #> '{device_set,devices}'
    ) LOOP
        v_device_id := (v_device ->> 'id')::uuid;
        v_node_id := (v_device ->> 'compute_node_id')::uuid;
        v_device_epoch := (v_device ->> 'device_epoch')::bigint;

        INSERT INTO public.compute_nodes (
            id, node_identity, region, network_domain, fault_domain,
            lifecycle_epoch, agent_session_epoch, attestation_digest,
            observed_at, observed_by
        ) VALUES (
            v_node_id, v_device ->> 'node_identity', v_device ->> 'region',
            v_device ->> 'network_domain', v_device ->> 'fault_domain',
            (v_device ->> 'node_epoch')::bigint,
            (v_device ->> 'agent_session_epoch')::bigint,
            decode(v_device ->> 'node_attestation_digest', 'hex'),
            v_observed_at, v_observed_by
        )
        ON CONFLICT (id) DO UPDATE SET
            agent_session_epoch = GREATEST(
                public.compute_nodes.agent_session_epoch,
                EXCLUDED.agent_session_epoch
            ),
            observed_at = GREATEST(public.compute_nodes.observed_at, EXCLUDED.observed_at),
            observed_by = EXCLUDED.observed_by
        WHERE public.compute_nodes.node_identity = EXCLUDED.node_identity
          AND public.compute_nodes.region = EXCLUDED.region
          AND public.compute_nodes.network_domain = EXCLUDED.network_domain
          AND public.compute_nodes.fault_domain = EXCLUDED.fault_domain
          AND public.compute_nodes.lifecycle_epoch = EXCLUDED.lifecycle_epoch
          AND public.compute_nodes.attestation_digest = EXCLUDED.attestation_digest;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'compute_node_identity_conflict',
                MESSAGE = 'ComputeNode identity changed without a lifecycle epoch';
        END IF;

        INSERT INTO public.devices (
            id, compute_node_id, kind, gpu_uuid, pci_bdf, device_epoch, health,
            attestation_digest, observed_at, observed_by
        ) VALUES (
            v_device_id, v_node_id, (v_device ->> 'kind')::public.device_kind,
            v_device ->> 'gpu_uuid', v_device ->> 'pci_bdf', v_device_epoch,
            (v_device ->> 'health')::public.device_health_state,
            decode(v_device ->> 'attestation_digest', 'hex'), v_observed_at, v_observed_by
        )
        ON CONFLICT (id) DO UPDATE SET
            health = EXCLUDED.health,
            observed_at = GREATEST(public.devices.observed_at, EXCLUDED.observed_at),
            observed_by = EXCLUDED.observed_by
        WHERE public.devices.compute_node_id = EXCLUDED.compute_node_id
          AND public.devices.kind = EXCLUDED.kind
          AND public.devices.gpu_uuid IS NOT DISTINCT FROM EXCLUDED.gpu_uuid
          AND public.devices.pci_bdf IS NOT DISTINCT FROM EXCLUDED.pci_bdf
          AND public.devices.device_epoch = EXCLUDED.device_epoch
          AND public.devices.attestation_digest = EXCLUDED.attestation_digest;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'device_identity_conflict',
                MESSAGE = 'Device identity changed without a device epoch';
        END IF;

        INSERT INTO public.device_set_members (
            device_set_id, device_id, device_epoch, ordinal
        ) VALUES (
            v_device_set_id, v_device_id, v_device_epoch,
            (v_device ->> 'ordinal')::integer
        );
    END LOOP;

    UPDATE public.worker_instances AS worker
    SET device_set_id = v_device_set_id,
        membership_digest = v_membership_digest,
        device_set_digest = v_device_set_digest,
        observed_at = v_observed_at,
        observed_by = v_observed_by
    WHERE worker.id = v_worker.id;

    INSERT INTO public.worker_instance_epochs (
        worker_instance_id, epoch, device_set_id, membership_digest,
        device_set_digest, started_at
    ) VALUES (
        v_worker.id, v_worker.instance_epoch, v_device_set_id, v_membership_digest,
        v_device_set_digest, v_observed_at
    );

    FOR v_member IN SELECT value FROM jsonb_array_elements(p_evidence -> 'members') LOOP
        v_member_id := (v_member ->> 'id')::uuid;
        INSERT INTO public.worker_members (
            id, worker_instance_id, worker_instance_epoch, member_key,
            compute_node_id, worker_bundle_id, member_epoch, device_subset_digest,
            identity_digest, readiness, observed_at, observed_by
        ) VALUES (
            v_member_id, v_worker.id, v_worker.instance_epoch,
            v_member ->> 'member_key', (v_member ->> 'compute_node_id')::uuid,
            v_worker.worker_bundle_id, (v_member ->> 'member_epoch')::bigint,
            decode(v_member ->> 'device_subset_digest', 'hex'),
            decode(v_member ->> 'identity_digest', 'hex'),
            (v_member ->> 'readiness')::public.worker_member_readiness_state,
            v_observed_at, v_observed_by
        );
        FOR v_member_device_id_text IN SELECT value #>> '{}'
            FROM jsonb_array_elements(v_member -> 'device_ids')
        LOOP
            SELECT member.device_epoch INTO STRICT v_device_epoch
            FROM public.device_set_members AS member
            WHERE member.device_set_id = v_device_set_id
              AND member.device_id = v_member_device_id_text::uuid;
            INSERT INTO public.worker_member_devices (
                worker_instance_id, worker_member_id, device_set_id, device_id, device_epoch
            ) VALUES (
                v_worker.id, v_member_id, v_device_set_id,
                v_member_device_id_text::uuid, v_device_epoch
            );
        END LOOP;
    END LOOP;

    IF (
        SELECT count(*) FROM public.worker_member_devices AS member_device
        WHERE member_device.worker_instance_id = v_worker.id
    ) <> v_device_count THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'worker_instance_membership_incomplete',
            MESSAGE = 'WorkerMember device coverage is incomplete';
    END IF;

    FOR v_device IN SELECT value FROM jsonb_array_elements(
        p_evidence #> '{device_set,devices}'
    ) LOOP
        v_device_id := (v_device ->> 'id')::uuid;
        v_device_epoch := (v_device ->> 'device_epoch')::bigint;
        INSERT INTO public.active_device_bindings (
            device_id, device_epoch, device_set_id, worker_instance_id,
            worker_instance_epoch, bound_at, evidence_digest
        ) VALUES (
            v_device_id, v_device_epoch, v_device_set_id, v_worker.id,
            v_worker.instance_epoch, v_observed_at, v_membership_digest
        )
        ON CONFLICT (device_id) DO UPDATE SET
            bound_at = GREATEST(public.active_device_bindings.bound_at, EXCLUDED.bound_at),
            evidence_digest = EXCLUDED.evidence_digest
        WHERE public.active_device_bindings.worker_instance_id = EXCLUDED.worker_instance_id
          AND public.active_device_bindings.worker_instance_epoch = EXCLUDED.worker_instance_epoch
          AND public.active_device_bindings.device_set_id = EXCLUDED.device_set_id
          AND public.active_device_bindings.device_epoch = EXCLUDED.device_epoch
        RETURNING true INTO v_bound;
        IF NOT COALESCE(v_bound, false) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'device_already_bound_to_worker_instance',
                MESSAGE = 'Device is already owned by another WorkerInstance';
        END IF;
        v_bound := false;
    END LOOP;

    FOR v_residency IN SELECT value FROM jsonb_array_elements(p_evidence -> 'residencies') LOOP
        IF NOT v_profile.resident_model_revisions ? (v_residency ->> 'model_component_revision') THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'model_residency_not_certified_for_worker_profile',
                MESSAGE = 'ModelResidency is not allowed by WorkerProfileRevision';
        END IF;
        INSERT INTO public.model_residencies (
            id, worker_instance_id, worker_instance_epoch, model_component_revision,
            runtime_identity, runtime_image_digest, model_runtime_epoch, state,
            warmup_evidence_digest, canary_evidence_digest, ready_at, released_at,
            observed_at, observed_by
        ) VALUES (
            (v_residency ->> 'id')::uuid, v_worker.id, v_worker.instance_epoch,
            v_residency ->> 'model_component_revision', v_residency ->> 'runtime_identity',
            v_residency ->> 'runtime_image_digest',
            (v_residency ->> 'model_runtime_epoch')::bigint,
            (v_residency ->> 'state')::public.model_residency_state,
            decode(v_residency ->> 'warmup_evidence_digest', 'hex'),
            decode(v_residency ->> 'canary_evidence_digest', 'hex'),
            CASE WHEN v_residency ->> 'state' = 'READY' THEN v_observed_at END,
            CASE WHEN v_residency ->> 'state' = 'RELEASED' THEN v_observed_at END,
            v_observed_at, v_observed_by
        );
        v_model_runtime_epoch := GREATEST(
            v_model_runtime_epoch,
            (v_residency ->> 'model_runtime_epoch')::bigint
        );
    END LOOP;
    IF jsonb_array_length(p_evidence -> 'residencies') = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'worker_instance_model_residency_required',
            MESSAGE = 'WorkerInstance has no resident ModelRuntime';
    END IF;

    v_capacity := p_evidence -> 'capacity';
    IF EXISTS (
        SELECT 1
        FROM jsonb_each_text(v_capacity -> 'vector') AS observed(resource_name, quantity)
        LEFT JOIN jsonb_each_text(v_profile.capacity_limits) AS certified(resource_name, quantity)
          USING (resource_name)
        WHERE certified.resource_name IS NULL
           OR observed.quantity::numeric < 0
           OR observed.quantity::numeric > certified.quantity::numeric
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'capacity_observation_exceeds_worker_profile',
            MESSAGE = 'CapacityObservation exceeds certified WorkerProfile limits';
    END IF;
    INSERT INTO public.capacity_observations (
        worker_instance_id, worker_instance_epoch, observation_sequence,
        capacity_vector, observed_at, expires_at, observed_by
    ) VALUES (
        v_worker.id, v_worker.instance_epoch, (v_capacity ->> 'sequence')::bigint,
        v_capacity -> 'vector', (v_capacity ->> 'observed_at')::timestamptz,
        (v_capacity ->> 'expires_at')::timestamptz, v_observed_by
    );

    UPDATE public.worker_instances AS worker
    SET lifecycle_state = 'READY', reachability_state = 'CONNECTED',
        observed_at = v_observed_at, observed_by = v_observed_by
    WHERE worker.id = v_worker.id;
    UPDATE public.worker_bundles AS bundle
    SET observed_generation = desired_generation, lifecycle_state = 'READY',
        updated_at = v_observed_at
    WHERE bundle.id = v_worker.worker_bundle_id;

    RETURN QUERY SELECT
        v_worker.id, v_worker.instance_epoch, v_worker.control_session_epoch,
        v_model_runtime_epoch, 'READY'::public.worker_instance_lifecycle_state;
END
$$;

CREATE FUNCTION vela_worker_instance_authority_matches(
    p_worker_instance_id uuid,
    p_instance_epoch bigint,
    p_device_set_digest bytea,
    p_membership_digest bytea,
    p_model_residency_id uuid,
    p_model_runtime_epoch bigint
) RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM public.worker_instances AS worker
        JOIN public.worker_instance_epochs AS epoch
          ON epoch.worker_instance_id = worker.id
         AND epoch.epoch = worker.instance_epoch
        JOIN public.model_residencies AS residency
          ON residency.id = p_model_residency_id
         AND residency.worker_instance_id = worker.id
         AND residency.worker_instance_epoch = worker.instance_epoch
        WHERE worker.id = p_worker_instance_id
          AND worker.instance_epoch = p_instance_epoch
          AND worker.lifecycle_state = 'READY'
          AND worker.reachability_state = 'CONNECTED'
          AND worker.device_set_digest = p_device_set_digest
          AND worker.membership_digest = p_membership_digest
          AND epoch.device_set_digest = p_device_set_digest
          AND epoch.membership_digest = p_membership_digest
          AND epoch.ended_at IS NULL
          AND residency.model_runtime_epoch = p_model_runtime_epoch
          AND residency.state = 'READY'
          AND (
              SELECT count(*)
              FROM public.worker_members AS member
              WHERE member.worker_instance_id = worker.id
                AND member.worker_instance_epoch = worker.instance_epoch
                AND member.readiness = 'READY'
          ) = worker.desired_member_count
          AND (
              SELECT count(*)
              FROM public.active_device_bindings AS binding
              WHERE binding.worker_instance_id = worker.id
                AND binding.worker_instance_epoch = worker.instance_epoch
          ) = worker.desired_device_count
          AND EXISTS (
              SELECT 1
              FROM public.capacity_observations AS observation
              WHERE observation.worker_instance_id = worker.id
                AND observation.worker_instance_epoch = worker.instance_epoch
                AND observation.expires_at > statement_timestamp()
          )
    )
$$;

CREATE FUNCTION vela_reconnect_worker_instance(
    p_worker_instance_id uuid,
    p_expected_instance_epoch bigint,
    p_expected_control_session_epoch bigint,
    p_control_session_id text,
    p_observed_at timestamptz,
    p_observed_by text
) RETURNS TABLE (
    instance_epoch bigint,
    control_session_epoch bigint,
    model_runtime_epoch bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_model_runtime_epoch bigint;
BEGIN
    IF p_worker_instance_id IS NULL OR p_expected_instance_epoch <= 0
       OR p_expected_control_session_epoch <= 0 OR p_observed_at IS NULL
       OR p_control_session_id IS NULL OR length(p_control_session_id) NOT BETWEEN 1 AND 500
       OR p_observed_by IS NULL OR length(p_observed_by) NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_reconnect_invalid',
            MESSAGE = 'WorkerInstance reconnect evidence is invalid';
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    IF NOT FOUND OR v_worker.instance_epoch <> p_expected_instance_epoch
       OR v_worker.lifecycle_state <> 'READY' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_epoch_conflict',
            MESSAGE = 'WorkerInstance reconnect uses stale execution identity';
    END IF;

    SELECT max(residency.model_runtime_epoch) INTO v_model_runtime_epoch
    FROM public.model_residencies AS residency
    WHERE residency.worker_instance_id = v_worker.id
      AND residency.worker_instance_epoch = v_worker.instance_epoch
      AND residency.state = 'READY';
    IF v_model_runtime_epoch IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_model_residency_required',
            MESSAGE = 'WorkerInstance reconnect requires a READY ModelRuntime';
    END IF;

    IF v_worker.control_session_epoch = p_expected_control_session_epoch + 1
       AND v_worker.control_session_id = p_control_session_id THEN
        RETURN QUERY SELECT
            v_worker.instance_epoch, v_worker.control_session_epoch, v_model_runtime_epoch;
        RETURN;
    END IF;
    IF v_worker.control_session_epoch <> p_expected_control_session_epoch
       OR p_observed_at < COALESCE(v_worker.observed_at, '-infinity'::timestamptz) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_control_session_conflict',
            MESSAGE = 'WorkerInstance control session is stale';
    END IF;

    UPDATE public.worker_instances AS worker
    SET control_session_epoch = p_expected_control_session_epoch + 1,
        control_session_id = p_control_session_id,
        reachability_state = 'CONNECTED',
        observed_at = p_observed_at,
        observed_by = p_observed_by
    WHERE worker.id = v_worker.id;

    RETURN QUERY SELECT
        v_worker.instance_epoch, p_expected_control_session_epoch + 1,
        v_model_runtime_epoch;
END
$$;

CREATE FUNCTION vela_fence_worker_instance(
    p_worker_instance_id uuid,
    p_expected_instance_epoch bigint,
    p_reason text,
    p_observed_by text
) RETURNS TABLE (
    instance_epoch bigint,
    lifecycle_state worker_instance_lifecycle_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_worker_instance_id IS NULL OR p_expected_instance_epoch <= 0
       OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 500
       OR p_observed_by IS NULL OR length(p_observed_by) NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_fence_invalid',
            MESSAGE = 'WorkerInstance fence request is invalid';
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'worker_instance_not_found',
            MESSAGE = 'WorkerInstance does not exist';
    END IF;
    IF v_worker.lifecycle_state = 'FENCED'
       AND v_worker.instance_epoch = p_expected_instance_epoch + 1 THEN
        RETURN QUERY SELECT v_worker.instance_epoch, v_worker.lifecycle_state;
        RETURN;
    END IF;
    IF v_worker.instance_epoch <> p_expected_instance_epoch
       OR v_worker.lifecycle_state IN ('FENCED', 'RETIRED') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_epoch_conflict',
            MESSAGE = 'WorkerInstance fence uses stale execution identity';
    END IF;

    UPDATE public.worker_instance_epochs AS epoch
    SET ended_at = v_now, end_reason = p_reason
    WHERE epoch.worker_instance_id = v_worker.id
      AND epoch.epoch = v_worker.instance_epoch
      AND epoch.ended_at IS NULL;
    DELETE FROM public.active_device_bindings AS binding
    WHERE binding.worker_instance_id = v_worker.id
      AND binding.worker_instance_epoch = v_worker.instance_epoch;
    UPDATE public.model_residencies AS residency
    SET state = 'DRAINING', observed_at = v_now, observed_by = p_observed_by
    WHERE residency.worker_instance_id = v_worker.id
      AND residency.worker_instance_epoch = v_worker.instance_epoch
      AND residency.state IN ('LOADING', 'WARMING', 'READY');
    UPDATE public.worker_instances AS worker
    SET instance_epoch = v_worker.instance_epoch + 1,
        lifecycle_state = 'FENCED',
        reachability_state = 'UNREACHABLE',
        device_set_id = NULL,
        membership_digest = NULL,
        device_set_digest = NULL,
        control_session_id = NULL,
        observed_at = v_now,
        observed_by = p_observed_by
    WHERE worker.id = v_worker.id;

    RETURN QUERY SELECT
        v_worker.instance_epoch + 1, 'FENCED'::public.worker_instance_lifecycle_state;
END
$$;

CREATE FUNCTION vela_begin_worker_instance_drain(
    p_worker_instance_id uuid,
    p_expected_instance_epoch bigint,
    p_reason text,
    p_requested_by text
) RETURNS TABLE (
    instance_epoch bigint,
    lifecycle_state worker_instance_lifecycle_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_existing public.worker_instance_drain_operations%ROWTYPE;
BEGIN
    IF p_worker_instance_id IS NULL OR p_expected_instance_epoch <= 0
       OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 500
       OR p_requested_by IS NULL OR length(p_requested_by) NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_drain_invalid',
            MESSAGE = 'WorkerInstance drain request is invalid';
    END IF;
    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    IF NOT FOUND OR v_worker.instance_epoch <> p_expected_instance_epoch
       OR v_worker.lifecycle_state IN ('FENCED', 'RETIRED') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_epoch_conflict',
            MESSAGE = 'WorkerInstance drain uses stale execution identity';
    END IF;

    SELECT operation.* INTO v_existing
    FROM public.worker_instance_drain_operations AS operation
    WHERE operation.worker_instance_id = v_worker.id
      AND operation.worker_instance_epoch = v_worker.instance_epoch;
    IF FOUND THEN
        IF v_existing.reason <> p_reason OR v_existing.requested_by <> p_requested_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_instance_drain_replay_conflict',
                MESSAGE = 'WorkerInstance drain replay does not match';
        END IF;
    ELSE
        INSERT INTO public.worker_instance_drain_operations (
            worker_instance_id, worker_instance_epoch, reason, requested_by
        ) VALUES (
            v_worker.id, v_worker.instance_epoch, p_reason, p_requested_by
        );
    END IF;
    UPDATE public.worker_instances AS worker
    SET lifecycle_state = 'DRAINING', observed_at = clock_timestamp(),
        observed_by = p_requested_by
    WHERE worker.id = v_worker.id;
    RETURN QUERY SELECT
        v_worker.instance_epoch, 'DRAINING'::public.worker_instance_lifecycle_state;
END
$$;

CREATE FUNCTION vela_approve_model_residency_release(
    p_operation_id uuid,
    p_model_residency_id uuid,
    p_expected_worker_instance_epoch bigint,
    p_reason model_residency_release_reason,
    p_approved_plan_revision text,
    p_approved_by text,
    p_planned_offline_seconds bigint,
    p_measured_reload_seconds bigint,
    p_break_even_evidence_digest bytea
) RETURNS TABLE (
    operation_id uuid,
    state model_residency_release_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_residency public.model_residencies%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_existing public.model_residency_release_operations%ROWTYPE;
BEGIN
    IF p_operation_id IS NULL OR p_model_residency_id IS NULL
       OR p_expected_worker_instance_epoch <= 0 OR p_reason IS NULL
       OR p_approved_plan_revision IS NULL
       OR length(p_approved_plan_revision) NOT BETWEEN 1 AND 300
       OR p_approved_by IS NULL OR length(p_approved_by) NOT BETWEEN 1 AND 500
       OR p_planned_offline_seconds <= 0 OR p_measured_reload_seconds <= 0
       OR octet_length(p_break_even_evidence_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_residency_release_invalid',
            MESSAGE = 'ModelResidency release approval is incomplete';
    END IF;
    IF p_reason = 'CAPACITY_CHANGE'
       AND p_planned_offline_seconds <= p_measured_reload_seconds THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'model_residency_release_below_break_even',
            MESSAGE = 'Capacity release cannot beat measured reload break-even';
    END IF;

    SELECT operation.* INTO v_existing
    FROM public.model_residency_release_operations AS operation
    WHERE operation.id = p_operation_id;
    IF FOUND THEN
        IF v_existing.model_residency_id <> p_model_residency_id
           OR v_existing.worker_instance_epoch <> p_expected_worker_instance_epoch
           OR v_existing.reason <> p_reason
           OR v_existing.approved_plan_revision <> p_approved_plan_revision
           OR v_existing.approved_by <> p_approved_by
           OR v_existing.planned_offline_seconds <> p_planned_offline_seconds
           OR v_existing.measured_reload_seconds <> p_measured_reload_seconds
           OR v_existing.break_even_evidence_digest <> p_break_even_evidence_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'model_residency_release_replay_conflict',
                MESSAGE = 'ModelResidency release replay does not match';
        END IF;
        RETURN QUERY SELECT v_existing.id, v_existing.state;
        RETURN;
    END IF;

    SELECT residency.* INTO v_residency
    FROM public.model_residencies AS residency
    WHERE residency.id = p_model_residency_id
    FOR UPDATE;
    IF NOT FOUND OR v_residency.worker_instance_epoch <> p_expected_worker_instance_epoch
       OR v_residency.state NOT IN ('READY', 'DRAINING') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_residency_release_epoch_conflict',
            MESSAGE = 'ModelResidency release uses stale runtime authority';
    END IF;
    SELECT worker.* INTO STRICT v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_residency.worker_instance_id
    FOR UPDATE;
    IF NOT (
        (v_worker.lifecycle_state = 'DRAINING'
         AND v_worker.instance_epoch = p_expected_worker_instance_epoch)
        OR (v_worker.lifecycle_state = 'FENCED'
            AND v_worker.instance_epoch = p_expected_worker_instance_epoch + 1)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_residency_release_requires_drain',
            MESSAGE = 'Healthy READY ModelRuntime cannot release outside drain or fence';
    END IF;

    INSERT INTO public.model_residency_release_operations (
        id, model_residency_id, worker_instance_id, worker_instance_epoch,
        reason, approved_plan_revision, approved_by, planned_offline_seconds,
        measured_reload_seconds, break_even_evidence_digest, state
    ) VALUES (
        p_operation_id, v_residency.id, v_residency.worker_instance_id,
        v_residency.worker_instance_epoch, p_reason, p_approved_plan_revision,
        p_approved_by, p_planned_offline_seconds, p_measured_reload_seconds,
        p_break_even_evidence_digest, 'APPROVED'
    );
    RETURN QUERY SELECT
        p_operation_id, 'APPROVED'::public.model_residency_release_state;
END
$$;

CREATE FUNCTION vela_complete_model_residency_release(
    p_operation_id uuid,
    p_expected_worker_instance_epoch bigint,
    p_completion_evidence_digest bytea,
    p_completed_by text
) RETURNS TABLE (
    operation_id uuid,
    state model_residency_release_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.model_residency_release_operations%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_operation_id IS NULL OR p_expected_worker_instance_epoch <= 0
       OR octet_length(p_completion_evidence_digest) <> 32
       OR p_completed_by IS NULL OR length(p_completed_by) NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_residency_release_completion_invalid',
            MESSAGE = 'ModelResidency release completion evidence is invalid';
    END IF;
    SELECT operation.* INTO v_operation
    FROM public.model_residency_release_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    IF NOT FOUND OR v_operation.worker_instance_epoch <> p_expected_worker_instance_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_residency_release_epoch_conflict',
            MESSAGE = 'ModelResidency release completion uses stale authority';
    END IF;
    IF v_operation.state = 'COMPLETED' THEN
        IF v_operation.completion_evidence_digest <> p_completion_evidence_digest
           OR v_operation.completed_by <> p_completed_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'model_residency_release_replay_conflict',
                MESSAGE = 'ModelResidency release completion replay does not match';
        END IF;
        RETURN QUERY SELECT v_operation.id, v_operation.state;
        RETURN;
    END IF;
    IF v_operation.state <> 'APPROVED' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_residency_release_not_approved',
            MESSAGE = 'ModelResidency release is not approved';
    END IF;

    UPDATE public.model_residencies AS residency
    SET state = 'RELEASED', released_at = v_now,
        observed_at = v_now, observed_by = p_completed_by
    WHERE residency.id = v_operation.model_residency_id
      AND residency.worker_instance_id = v_operation.worker_instance_id
      AND residency.worker_instance_epoch = v_operation.worker_instance_epoch
      AND residency.state IN ('READY', 'DRAINING');
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_residency_release_epoch_conflict',
            MESSAGE = 'ModelResidency release target changed';
    END IF;
    UPDATE public.model_residency_release_operations AS operation
    SET state = 'COMPLETED', completed_at = v_now,
        completion_evidence_digest = p_completion_evidence_digest,
        completed_by = p_completed_by
    WHERE operation.id = v_operation.id;
    RETURN QUERY SELECT
        v_operation.id, 'COMPLETED'::public.model_residency_release_state;
END
$$;

CREATE FUNCTION vela_authorize_worker_instance_pod_mutation(
    p_request_uid text,
    p_actor_identity text,
    p_operation fleet_mutation_operation,
    p_kubernetes_uid text,
    p_namespace text,
    p_name text,
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_residency_plan_revision_id uuid,
    p_worker_bundle_id uuid,
    p_worker_member_id uuid,
    p_request_digest bytea
) RETURNS TABLE (
    request_uid text,
    replayed boolean,
    authorized boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_existing public.worker_instance_pod_mutation_authorizations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_member public.worker_members%ROWTYPE;
BEGIN
    IF p_request_uid IS NULL OR length(p_request_uid) NOT BETWEEN 1 AND 200
       OR btrim(p_request_uid) <> p_request_uid
       OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_actor_identity) <> p_actor_identity
       OR p_operation IS NULL OR p_operation NOT IN ('DELETE', 'REMOVE_FINALIZER')
       OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 200
       OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
       OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253
       OR btrim(p_namespace) <> p_namespace
       OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253
       OR btrim(p_name) <> p_name
       OR p_worker_instance_id IS NULL OR p_worker_instance_epoch <= 0
       OR p_residency_plan_revision_id IS NULL OR p_worker_bundle_id IS NULL
       OR p_worker_member_id IS NULL OR octet_length(p_request_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_pod_mutation_authorization_invalid',
            MESSAGE = 'WorkerInstance Pod mutation authorization request is invalid';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_request_uid, 49035)
    );
    SELECT receipt.* INTO v_existing
    FROM public.worker_instance_pod_mutation_authorizations AS receipt
    WHERE receipt.request_uid = p_request_uid;
    IF FOUND THEN
        IF v_existing.actor_identity <> p_actor_identity
           OR v_existing.operation <> p_operation
           OR v_existing.kubernetes_uid <> p_kubernetes_uid
           OR v_existing.namespace <> p_namespace
           OR v_existing.name <> p_name
           OR v_existing.worker_instance_id <> p_worker_instance_id
           OR v_existing.worker_instance_epoch <> p_worker_instance_epoch
           OR v_existing.residency_plan_revision_id <> p_residency_plan_revision_id
           OR v_existing.worker_bundle_id <> p_worker_bundle_id
           OR v_existing.worker_member_id <> p_worker_member_id
           OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_instance_pod_mutation_authorization_conflict',
                MESSAGE = 'WorkerInstance Pod mutation request UID conflicts with existing authorization';
        END IF;
        RETURN QUERY SELECT p_request_uid, true, true;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.fleet_mutation_authorizations AS receipt
        WHERE receipt.request_uid = p_request_uid
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_pod_mutation_authorization_conflict',
            MESSAGE = 'WorkerInstance Pod mutation request UID conflicts with legacy authorization';
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    SELECT member.* INTO v_member
    FROM public.worker_members AS member
    WHERE member.worker_instance_id = p_worker_instance_id
      AND member.id = p_worker_member_id
    FOR UPDATE;
    IF v_worker.id IS NULL OR v_member.id IS NULL
       OR v_worker.residency_plan_revision_id IS DISTINCT FROM p_residency_plan_revision_id
       OR v_worker.worker_bundle_id IS DISTINCT FROM p_worker_bundle_id
       OR v_member.worker_instance_epoch <> p_worker_instance_epoch
       OR v_member.worker_bundle_id IS DISTINCT FROM p_worker_bundle_id
       OR p_name <> 'wi-'
            || pg_catalog.replace(p_worker_instance_id::text, '-', '')
            || '-'
            || pg_catalog.encode(
                pg_catalog.substr(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(v_member.member_key, 'UTF8')
                    ),
                    1,
                    4
                ),
                'hex'
            ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_pod_mutation_authority_conflict',
            MESSAGE = 'WorkerInstance Pod mutation authority is stale or conflicting';
    END IF;

    IF v_worker.lifecycle_state = 'DRAINING'
       AND v_worker.instance_epoch = p_worker_instance_epoch THEN
        IF NOT EXISTS (
            SELECT 1
            FROM public.worker_instance_drain_operations AS drain
            WHERE drain.worker_instance_id = p_worker_instance_id
              AND drain.worker_instance_epoch = p_worker_instance_epoch
        ) OR NOT EXISTS (
            SELECT 1
            FROM public.model_residencies AS residency
            WHERE residency.worker_instance_id = p_worker_instance_id
              AND residency.worker_instance_epoch = p_worker_instance_epoch
        ) OR EXISTS (
            SELECT 1
            FROM public.model_residencies AS residency
            WHERE residency.worker_instance_id = p_worker_instance_id
              AND residency.worker_instance_epoch = p_worker_instance_epoch
              AND residency.state <> 'RELEASED'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_instance_pod_mutation_release_incomplete',
                MESSAGE = 'WorkerInstance Pod mutation requires drain and complete residency release';
        END IF;
    ELSIF NOT (
        v_worker.lifecycle_state = 'FENCED'
        AND v_worker.instance_epoch = p_worker_instance_epoch + 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_pod_mutation_lifecycle_conflict',
            MESSAGE = 'WorkerInstance Pod mutation requires released drain or fenced old epoch';
    END IF;

    INSERT INTO public.worker_instance_pod_mutation_authorizations (
        request_uid, actor_identity, operation, kubernetes_uid, namespace, name,
        worker_instance_id, worker_instance_epoch, residency_plan_revision_id,
        worker_bundle_id, worker_member_id, request_digest
    ) VALUES (
        p_request_uid, p_actor_identity, p_operation, p_kubernetes_uid,
        p_namespace, p_name, p_worker_instance_id, p_worker_instance_epoch,
        p_residency_plan_revision_id, p_worker_bundle_id, p_worker_member_id,
        p_request_digest
    );
    RETURN QUERY SELECT p_request_uid, false, true;
END
$$;

ALTER TABLE capacity_pools OWNER TO vela_fleet_owner;
ALTER TABLE residency_proposals OWNER TO vela_fleet_owner;
ALTER TABLE residency_plan_revisions OWNER TO vela_fleet_owner;
ALTER TABLE worker_bundles OWNER TO vela_fleet_owner;
ALTER TABLE compute_nodes OWNER TO vela_fleet_owner;
ALTER TABLE devices OWNER TO vela_fleet_owner;
ALTER TABLE device_sets OWNER TO vela_fleet_owner;
ALTER TABLE device_set_members OWNER TO vela_fleet_owner;
ALTER TABLE worker_instances OWNER TO vela_fleet_owner;
ALTER TABLE worker_instance_epochs OWNER TO vela_fleet_owner;
ALTER TABLE worker_members OWNER TO vela_fleet_owner;
ALTER TABLE worker_member_devices OWNER TO vela_fleet_owner;
ALTER TABLE active_device_bindings OWNER TO vela_fleet_owner;
ALTER TABLE model_residencies OWNER TO vela_fleet_owner;
ALTER TABLE worker_instance_drain_operations OWNER TO vela_fleet_owner;
ALTER TABLE model_residency_release_operations OWNER TO vela_fleet_owner;
ALTER TABLE worker_instance_pod_mutation_authorizations OWNER TO vela_fleet_owner;
ALTER TABLE capacity_observations OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_observe_worker_instance(jsonb) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_record_residency_proposal(jsonb) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_apply_residency_plan(jsonb) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reject_residency_planning_history_mutation()
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_worker_instance_authority_matches(
    uuid, bigint, bytea, bytea, uuid, bigint
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reconnect_worker_instance(
    uuid, bigint, bigint, text, timestamptz, text
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_fence_worker_instance(uuid, bigint, text, text)
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_begin_worker_instance_drain(uuid, bigint, text, text)
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_approve_model_residency_release(
    uuid, uuid, bigint, model_residency_release_reason, text, text,
    bigint, bigint, bytea
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_complete_model_residency_release(uuid, bigint, bytea, text)
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_authorize_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, bytea
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reject_worker_instance_pod_mutation_authorization_mutation()
    OWNER TO vela_fleet_owner;

REVOKE ALL ON TABLE capacity_pools, residency_proposals, residency_plan_revisions,
    worker_bundles, compute_nodes, devices,
    device_sets, device_set_members, worker_instances, worker_instance_epochs, worker_members,
    worker_member_devices, active_device_bindings, model_residencies,
    worker_instance_drain_operations, model_residency_release_operations,
    worker_instance_pod_mutation_authorizations, capacity_observations FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_observe_worker_instance(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_record_residency_proposal(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_apply_residency_plan(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_reject_residency_planning_history_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_worker_instance_authority_matches(
    uuid, bigint, bytea, bytea, uuid, bigint
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_reconnect_worker_instance(
    uuid, bigint, bigint, text, timestamptz, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_fence_worker_instance(uuid, bigint, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_begin_worker_instance_drain(uuid, bigint, text, text)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_approve_model_residency_release(
    uuid, uuid, bigint, model_residency_release_reason, text, text,
    bigint, bigint, bytea
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_complete_model_residency_release(
    uuid, bigint, bytea, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_authorize_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, bytea
) FROM PUBLIC;
REVOKE ALL ON FUNCTION
    vela_reject_worker_instance_pod_mutation_authorization_mutation() FROM PUBLIC;

GRANT SELECT ON worker_profile_revisions, stage_profile_revisions,
    stage_definition_revisions TO vela_fleet_owner;
GRANT EXECUTE ON FUNCTION vela_observe_worker_instance(jsonb) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_record_residency_proposal(jsonb) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_apply_residency_plan(jsonb) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_worker_instance_authority_matches(
    uuid, bigint, bytea, bytea, uuid, bigint
) TO vela_fleet, vela_scheduler;
GRANT EXECUTE ON FUNCTION vela_reconnect_worker_instance(
    uuid, bigint, bigint, text, timestamptz, text
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_fence_worker_instance(uuid, bigint, text, text)
    TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_begin_worker_instance_drain(uuid, bigint, text, text)
    TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_approve_model_residency_release(
    uuid, uuid, bigint, model_residency_release_reason, text, text,
    bigint, bigint, bytea
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_complete_model_residency_release(
    uuid, bigint, bytea, text
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_authorize_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, bytea
) TO vela_fleet;
GRANT SELECT ON capacity_pools, residency_proposals, residency_plan_revisions,
    worker_bundles, compute_nodes, devices,
    device_sets, device_set_members, worker_instances, worker_instance_epochs, worker_members,
    worker_member_devices, active_device_bindings, model_residencies,
    worker_instance_drain_operations, model_residency_release_operations,
    worker_instance_pod_mutation_authorizations, capacity_observations TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    capacity_pools, residency_proposals, residency_plan_revisions,
    worker_bundles, compute_nodes, devices, device_sets,
    device_set_members, worker_instances, worker_instance_epochs, worker_members, worker_member_devices,
    active_device_bindings, model_residencies, capacity_observations
    , worker_instance_drain_operations, model_residency_release_operations,
    worker_instance_pod_mutation_authorizations
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM worker_instances)
       OR EXISTS (SELECT 1 FROM devices)
       OR EXISTS (SELECT 1 FROM model_residencies)
       OR EXISTS (SELECT 1 FROM capacity_observations)
       OR EXISTS (SELECT 1 FROM worker_instance_pod_mutation_authorizations)
       OR EXISTS (SELECT 1 FROM residency_proposals)
       OR EXISTS (SELECT 1 FROM residency_plan_revisions) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_registry_rollback_is_unsafe',
            MESSAGE = 'Worker Registry contains resource or residency authority';
    END IF;
END
$$;

REVOKE SELECT ON worker_profile_revisions, stage_profile_revisions,
    stage_definition_revisions FROM vela_fleet_owner;
REVOKE EXECUTE ON FUNCTION vela_observe_worker_instance(jsonb) FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_record_residency_proposal(jsonb) FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_apply_residency_plan(jsonb) FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_worker_instance_authority_matches(
    uuid, bigint, bytea, bytea, uuid, bigint
) FROM vela_fleet, vela_scheduler;
REVOKE EXECUTE ON FUNCTION vela_reconnect_worker_instance(
    uuid, bigint, bigint, text, timestamptz, text
) FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_fence_worker_instance(uuid, bigint, text, text)
    FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_begin_worker_instance_drain(uuid, bigint, text, text)
    FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_approve_model_residency_release(
    uuid, uuid, bigint, model_residency_release_reason, text, text,
    bigint, bigint, bytea
) FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_complete_model_residency_release(
    uuid, bigint, bytea, text
) FROM vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_authorize_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, bytea
) FROM vela_fleet;
REVOKE SELECT ON capacity_pools, residency_proposals, residency_plan_revisions,
    worker_bundles, compute_nodes, devices,
    device_sets, device_set_members, worker_instances, worker_instance_epochs, worker_members,
    worker_member_devices, active_device_bindings, model_residencies,
    worker_instance_drain_operations, model_residency_release_operations,
    worker_instance_pod_mutation_authorizations, capacity_observations FROM vela_internal;
DROP FUNCTION vela_authorize_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, bytea
);
DROP TRIGGER worker_instance_pod_mutation_authorizations_immutable
    ON worker_instance_pod_mutation_authorizations;
DROP FUNCTION vela_reject_worker_instance_pod_mutation_authorization_mutation();
DROP TABLE worker_instance_pod_mutation_authorizations;
DROP FUNCTION vela_complete_model_residency_release(uuid, bigint, bytea, text);
DROP FUNCTION vela_approve_model_residency_release(
    uuid, uuid, bigint, model_residency_release_reason, text, text,
    bigint, bigint, bytea
);
DROP FUNCTION vela_begin_worker_instance_drain(uuid, bigint, text, text);
DROP FUNCTION vela_fence_worker_instance(uuid, bigint, text, text);
DROP FUNCTION vela_reconnect_worker_instance(
    uuid, bigint, bigint, text, timestamptz, text
);
DROP FUNCTION vela_worker_instance_authority_matches(
    uuid, bigint, bytea, bytea, uuid, bigint
);
DROP FUNCTION vela_observe_worker_instance(jsonb);
DROP FUNCTION vela_apply_residency_plan(jsonb);
DROP FUNCTION vela_record_residency_proposal(jsonb);
DROP TABLE capacity_observations;
DROP TABLE model_residency_release_operations;
DROP TABLE worker_instance_drain_operations;
DROP TABLE model_residencies;
DROP TABLE active_device_bindings;
DROP TABLE worker_member_devices;
DROP TABLE worker_members;
DROP TABLE worker_instance_epochs;
DROP TABLE worker_instances;
DROP TABLE device_set_members;
DROP TABLE device_sets;
DROP TABLE devices;
ALTER TABLE worker_bundles DROP CONSTRAINT worker_bundles_compute_node;
DROP TABLE compute_nodes;
DROP TABLE worker_bundles;
DROP TABLE capacity_pools;
DROP TRIGGER residency_plan_revisions_immutable ON residency_plan_revisions;
DROP TRIGGER residency_proposals_immutable ON residency_proposals;
DROP FUNCTION vela_reject_residency_planning_history_mutation();
DROP TABLE residency_plan_revisions;
DROP TABLE residency_proposals;
DROP TYPE model_residency_state;
DROP TYPE model_residency_release_state;
DROP TYPE model_residency_release_reason;
DROP TYPE worker_member_readiness_state;
DROP TYPE worker_instance_reachability_state;
DROP TYPE worker_instance_lifecycle_state;
DROP TYPE worker_bundle_state;
DROP TYPE capacity_pool_state;
DROP TYPE device_health_state;
DROP TYPE device_kind;
DROP TYPE compute_node_lifecycle_state;
-- +goose StatementEnd
