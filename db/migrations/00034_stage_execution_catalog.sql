-- +goose Up
-- +goose StatementBegin
CREATE TABLE stage_interface_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    payload_kind text NOT NULL CHECK (length(payload_kind) BETWEEN 1 AND 100),
    dtype text NOT NULL,
    layout text NOT NULL,
    shape_contract jsonb NOT NULL CHECK (jsonb_typeof(shape_contract) = 'object'),
    serialization text NOT NULL CHECK (length(serialization) BETWEEN 1 AND 100),
    max_bytes bigint NOT NULL CHECK (max_bytes > 0),
    digest_algorithm text NOT NULL CHECK (digest_algorithm = 'sha256'),
    schema_digest bytea NOT NULL CHECK (octet_length(schema_digest) = 32),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE stage_result_equivalence_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    exact_contract jsonb NOT NULL CHECK (jsonb_typeof(exact_contract) = 'object'),
    evidence_receipt_ref text NOT NULL CHECK (length(evidence_receipt_ref) BETWEEN 1 AND 2000),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE input_canonicalization_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    encoding_contract jsonb NOT NULL CHECK (jsonb_typeof(encoding_contract) = 'object'),
    exact_equivalence_rules jsonb NOT NULL CHECK (jsonb_typeof(exact_equivalence_rules) = 'object'),
    test_corpus_digest bytea NOT NULL CHECK (octet_length(test_corpus_digest) = 32),
    activation_evidence_digest bytea NOT NULL CHECK (octet_length(activation_evidence_digest) = 32),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE worker_profile_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    device_count integer NOT NULL CHECK (device_count > 0),
    member_count integer NOT NULL CHECK (member_count > 0 AND member_count <= device_count),
    device_set_shape jsonb NOT NULL CHECK (jsonb_typeof(device_set_shape) = 'object'),
    resident_model_revisions jsonb NOT NULL CHECK (jsonb_typeof(resident_model_revisions) = 'array'),
    capacity_limits jsonb NOT NULL CHECK (jsonb_typeof(capacity_limits) = 'object'),
    readiness_checks jsonb NOT NULL CHECK (jsonb_typeof(readiness_checks) = 'object'),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE stage_runtime_model_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    request_cohort_schema jsonb NOT NULL CHECK (jsonb_typeof(request_cohort_schema) = 'object'),
    service_distributions jsonb NOT NULL CHECK (jsonb_typeof(service_distributions) = 'object'),
    output_distributions jsonb NOT NULL CHECK (jsonb_typeof(output_distributions) = 'object'),
    transfer_model jsonb NOT NULL CHECK (jsonb_typeof(transfer_model) = 'object'),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE stage_cache_policy_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    allowed_stage_keys text[] NOT NULL CHECK (cardinality(allowed_stage_keys) > 0),
    scope_ceiling text NOT NULL CHECK (scope_ceiling IN ('PROJECT', 'ORGANIZATION')),
    ttl_seconds bigint NOT NULL CHECK (ttl_seconds > 0),
    quota_policy jsonb NOT NULL CHECK (jsonb_typeof(quota_policy) = 'object'),
    encryption_policy jsonb NOT NULL CHECK (jsonb_typeof(encryption_policy) = 'object'),
    deletion_policy jsonb NOT NULL CHECK (jsonb_typeof(deletion_policy) = 'object'),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE checkpoint_policy_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    resume_format text NOT NULL CHECK (length(resume_format) BETWEEN 1 AND 100),
    compatibility_contract jsonb NOT NULL CHECK (jsonb_typeof(compatibility_contract) = 'object'),
    interval_policy jsonb NOT NULL CHECK (jsonb_typeof(interval_policy) = 'object'),
    max_overhead_ppm integer NOT NULL CHECK (max_overhead_ppm BETWEEN 0 AND 1000000),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE cost_model_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    effective_at timestamptz NOT NULL,
    expires_at timestamptz,
    resource_valuations jsonb NOT NULL CHECK (jsonb_typeof(resource_valuations) = 'object'),
    allocation_method text NOT NULL CHECK (length(allocation_method) BETWEEN 1 AND 100),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at IS NULL OR expires_at > effective_at),
    UNIQUE (stable_id, revision)
);

CREATE TABLE stage_definition_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    stage_kind text NOT NULL CHECK (length(stage_kind) BETWEEN 1 AND 100),
    input_ports jsonb NOT NULL CHECK (jsonb_typeof(input_ports) = 'object'),
    output_ports jsonb NOT NULL CHECK (jsonb_typeof(output_ports) = 'object'),
    required_input_ports text[] NOT NULL,
    required_output_ports text[] NOT NULL,
    resource_class text NOT NULL CHECK (resource_class IN ('GPU', 'CPU')),
    retry_class text NOT NULL CHECK (length(retry_class) BETWEEN 1 AND 100),
    cache_policy_revision_id uuid REFERENCES stage_cache_policy_revisions(id),
    checkpoint_policy_revision_id uuid REFERENCES checkpoint_policy_revisions(id),
    public_phase execution_phase NOT NULL,
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (input_ports <> '{}'::jsonb OR output_ports <> '{}'::jsonb),
    UNIQUE (stable_id, revision)
);

CREATE TABLE stage_profile_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    stage_definition_revision_id uuid NOT NULL REFERENCES stage_definition_revisions(id),
    model_component_revision text NOT NULL CHECK (length(model_component_revision) BETWEEN 1 AND 300),
    runtime_image_digest text NOT NULL CHECK (length(runtime_image_digest) BETWEEN 1 AND 300),
    worker_profile_revision_id uuid NOT NULL REFERENCES worker_profile_revisions(id),
    result_equivalence_revision_id uuid NOT NULL REFERENCES stage_result_equivalence_revisions(id),
    certified_capacity_vector jsonb NOT NULL CHECK (jsonb_typeof(certified_capacity_vector) = 'object'),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision),
    UNIQUE (id, stage_definition_revision_id)
);

CREATE TABLE connector_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    source_interface_revision_id uuid NOT NULL REFERENCES stage_interface_revisions(id),
    destination_interface_revision_id uuid NOT NULL REFERENCES stage_interface_revisions(id),
    transport text NOT NULL CHECK (length(transport) BETWEEN 1 AND 100),
    durable_fallback boolean NOT NULL,
    topology_policy jsonb NOT NULL CHECK (jsonb_typeof(topology_policy) = 'object'),
    integrity_policy jsonb NOT NULL CHECK (jsonb_typeof(integrity_policy) = 'object'),
    security_policy jsonb NOT NULL CHECK (jsonb_typeof(security_policy) = 'object'),
    limits jsonb NOT NULL CHECK (jsonb_typeof(limits) = 'object'),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE execution_graph_revisions (
    id uuid PRIMARY KEY,
    model_revision_id uuid NOT NULL REFERENCES model_revisions(id),
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    schema_version integer NOT NULL CHECK (schema_version = 1),
    state catalog_state NOT NULL,
    final_output_contract jsonb NOT NULL CHECK (jsonb_typeof(final_output_contract) = 'object'),
    public_phase_map jsonb NOT NULL CHECK (jsonb_typeof(public_phase_map) = 'object'),
    topological_order text[],
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        state NOT IN ('ACTIVE', 'DRAINING', 'RETIRED')
        OR cardinality(topological_order) > 0
    ),
    UNIQUE (model_revision_id, stable_id, revision)
);

CREATE UNIQUE INDEX execution_graph_revisions_one_active_idx
    ON execution_graph_revisions (model_revision_id, stable_id)
    WHERE state = 'ACTIVE';

CREATE TABLE execution_graph_stages (
    execution_graph_revision_id uuid NOT NULL REFERENCES execution_graph_revisions(id),
    stage_key text NOT NULL CHECK (
        length(stage_key) BETWEEN 1 AND 100
        AND stage_key ~ '^[a-z][a-z0-9_-]*$'
    ),
    stage_definition_revision_id uuid NOT NULL REFERENCES stage_definition_revisions(id),
    required boolean NOT NULL,
    max_fan_out integer NOT NULL CHECK (max_fan_out BETWEEN 1 AND 64),
    PRIMARY KEY (execution_graph_revision_id, stage_key)
);

CREATE TABLE execution_graph_edges (
    id uuid PRIMARY KEY,
    execution_graph_revision_id uuid NOT NULL REFERENCES execution_graph_revisions(id),
    source_stage_key text NOT NULL,
    source_port text NOT NULL CHECK (length(source_port) BETWEEN 1 AND 100),
    destination_stage_key text NOT NULL,
    destination_port text NOT NULL CHECK (length(destination_port) BETWEEN 1 AND 100),
    buffer_class text NOT NULL CHECK (length(buffer_class) BETWEEN 1 AND 100),
    CHECK (source_stage_key <> destination_stage_key),
    FOREIGN KEY (execution_graph_revision_id, source_stage_key)
        REFERENCES execution_graph_stages(execution_graph_revision_id, stage_key),
    FOREIGN KEY (execution_graph_revision_id, destination_stage_key)
        REFERENCES execution_graph_stages(execution_graph_revision_id, stage_key),
    UNIQUE (
        execution_graph_revision_id, source_stage_key, source_port,
        destination_stage_key, destination_port
    ),
    UNIQUE (id, execution_graph_revision_id)
);

CREATE TABLE execution_graph_inputs (
    execution_graph_revision_id uuid NOT NULL REFERENCES execution_graph_revisions(id),
    input_key text NOT NULL CHECK (length(input_key) BETWEEN 1 AND 100),
    interface_revision_id uuid NOT NULL REFERENCES stage_interface_revisions(id),
    destination_stage_key text NOT NULL,
    destination_port text NOT NULL CHECK (length(destination_port) BETWEEN 1 AND 100),
    PRIMARY KEY (execution_graph_revision_id, input_key),
    FOREIGN KEY (execution_graph_revision_id, destination_stage_key)
        REFERENCES execution_graph_stages(execution_graph_revision_id, stage_key),
    UNIQUE (execution_graph_revision_id, destination_stage_key, destination_port)
);

CREATE TABLE execution_graph_outputs (
    execution_graph_revision_id uuid NOT NULL REFERENCES execution_graph_revisions(id),
    output_key text NOT NULL CHECK (length(output_key) BETWEEN 1 AND 100),
    interface_revision_id uuid NOT NULL REFERENCES stage_interface_revisions(id),
    source_stage_key text NOT NULL,
    source_port text NOT NULL CHECK (length(source_port) BETWEEN 1 AND 100),
    required boolean NOT NULL,
    PRIMARY KEY (execution_graph_revision_id, output_key),
    FOREIGN KEY (execution_graph_revision_id, source_stage_key)
        REFERENCES execution_graph_stages(execution_graph_revision_id, stage_key),
    UNIQUE (execution_graph_revision_id, source_stage_key, source_port)
);

ALTER TABLE execution_profile_revisions
    ADD COLUMN execution_graph_revision_id uuid REFERENCES execution_graph_revisions(id);

CREATE TABLE execution_profile_stage_options (
    execution_profile_revision_id uuid NOT NULL REFERENCES execution_profile_revisions(id),
    execution_graph_revision_id uuid NOT NULL REFERENCES execution_graph_revisions(id),
    stage_key text NOT NULL,
    stage_profile_revision_id uuid NOT NULL REFERENCES stage_profile_revisions(id),
    preference integer NOT NULL DEFAULT 0,
    eligibility_metadata jsonb NOT NULL CHECK (jsonb_typeof(eligibility_metadata) = 'object'),
    PRIMARY KEY (execution_profile_revision_id, stage_key, stage_profile_revision_id),
    FOREIGN KEY (execution_graph_revision_id, stage_key)
        REFERENCES execution_graph_stages(execution_graph_revision_id, stage_key)
);

CREATE TABLE execution_profile_connector_options (
    execution_profile_revision_id uuid NOT NULL REFERENCES execution_profile_revisions(id),
    execution_graph_revision_id uuid NOT NULL REFERENCES execution_graph_revisions(id),
    execution_graph_edge_id uuid NOT NULL,
    connector_revision_id uuid NOT NULL REFERENCES connector_revisions(id),
    required_topology_policy jsonb NOT NULL CHECK (jsonb_typeof(required_topology_policy) = 'object'),
    preference integer NOT NULL DEFAULT 0,
    PRIMARY KEY (
        execution_profile_revision_id, execution_graph_edge_id, connector_revision_id
    ),
    FOREIGN KEY (execution_graph_edge_id, execution_graph_revision_id)
        REFERENCES execution_graph_edges(id, execution_graph_revision_id)
);

CREATE UNIQUE INDEX stage_interface_revisions_one_active_idx
    ON stage_interface_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX stage_result_equivalence_revisions_one_active_idx
    ON stage_result_equivalence_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX input_canonicalization_revisions_one_active_idx
    ON input_canonicalization_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX worker_profile_revisions_one_active_idx
    ON worker_profile_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX stage_runtime_model_revisions_one_active_idx
    ON stage_runtime_model_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX stage_cache_policy_revisions_one_active_idx
    ON stage_cache_policy_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX checkpoint_policy_revisions_one_active_idx
    ON checkpoint_policy_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX cost_model_revisions_one_active_idx
    ON cost_model_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX stage_definition_revisions_one_active_idx
    ON stage_definition_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX stage_profile_revisions_one_active_idx
    ON stage_profile_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX connector_revisions_one_active_idx
    ON connector_revisions (stable_id) WHERE state = 'ACTIVE';

CREATE FUNCTION vela_guard_stage_catalog_revision() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.state IN ('ACTIVE', 'DRAINING', 'RETIRED') THEN
        IF TG_OP = 'DELETE'
           OR (to_jsonb(NEW) - 'state') IS DISTINCT FROM (to_jsonb(OLD) - 'state') THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_catalog_revision_is_immutable',
                MESSAGE = TG_TABLE_NAME || ' activated revision is immutable';
        END IF;
        IF OLD.state = 'RETIRED' AND NEW.state <> 'RETIRED'
           OR OLD.state = 'DRAINING' AND NEW.state NOT IN ('DRAINING', 'RETIRED')
           OR OLD.state = 'ACTIVE' AND NEW.state NOT IN ('ACTIVE', 'DRAINING', 'RETIRED') THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_catalog_lifecycle_is_monotonic',
                MESSAGE = TG_TABLE_NAME || ' activated lifecycle cannot move backward';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_stage_catalog_revision() FROM PUBLIC;
ALTER FUNCTION vela_guard_stage_catalog_revision() OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_guard_active_execution_graph_member() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_old_state public.catalog_state;
    v_new_state public.catalog_state;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        SELECT state INTO v_old_state FROM public.execution_graph_revisions
        WHERE id = OLD.execution_graph_revision_id;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        SELECT state INTO v_new_state FROM public.execution_graph_revisions
        WHERE id = NEW.execution_graph_revision_id;
    END IF;
    IF v_old_state IN ('ACTIVE', 'DRAINING', 'RETIRED')
       OR v_new_state IN ('ACTIVE', 'DRAINING', 'RETIRED') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'active_execution_graph_is_immutable',
            MESSAGE = TG_TABLE_NAME || ' cannot mutate activated graph membership';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_active_execution_graph_member() FROM PUBLIC;
ALTER FUNCTION vela_guard_active_execution_graph_member() OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_guard_active_execution_graph_truncate() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.execution_graph_revisions
        WHERE state IN ('ACTIVE', 'DRAINING', 'RETIRED')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'active_execution_graph_is_immutable',
            MESSAGE = TG_TABLE_NAME || ' cannot truncate activated graph membership';
    END IF;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_active_execution_graph_truncate() FROM PUBLIC;
ALTER FUNCTION vela_guard_active_execution_graph_truncate() OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_guard_active_execution_profile_member() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_old_state public.catalog_state;
    v_new_state public.catalog_state;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        SELECT state INTO v_old_state FROM public.execution_profile_revisions
        WHERE id = OLD.execution_profile_revision_id;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        SELECT state INTO v_new_state FROM public.execution_profile_revisions
        WHERE id = NEW.execution_profile_revision_id;
    END IF;
    IF v_old_state IN ('ACTIVE', 'DRAINING', 'RETIRED')
       OR v_new_state IN ('ACTIVE', 'DRAINING', 'RETIRED') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'active_execution_profile_is_immutable',
            MESSAGE = TG_TABLE_NAME || ' cannot mutate activated profile options';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_active_execution_profile_member() FROM PUBLIC;
ALTER FUNCTION vela_guard_active_execution_profile_member()
    OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_require_execution_graph_activation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.state = 'ACTIVE' AND OLD.state <> 'ACTIVE'
       AND current_setting('vela.execution_graph_activation', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_graph_activation_function_required',
            MESSAGE = 'ExecutionGraphRevision must activate through vela_activate_execution_graph';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_require_execution_graph_activation() FROM PUBLIC;
ALTER FUNCTION vela_require_execution_graph_activation() OWNER TO vela_catalog_promotion_owner;

CREATE TRIGGER stage_interface_revision_definition_immutable
BEFORE UPDATE ON stage_interface_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER stage_result_equivalence_definition_immutable
BEFORE UPDATE ON stage_result_equivalence_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER input_canonicalization_definition_immutable
BEFORE UPDATE ON input_canonicalization_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER worker_profile_revision_definition_immutable
BEFORE UPDATE ON worker_profile_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER stage_runtime_model_definition_immutable
BEFORE UPDATE ON stage_runtime_model_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER stage_cache_policy_definition_immutable
BEFORE UPDATE ON stage_cache_policy_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER checkpoint_policy_definition_immutable
BEFORE UPDATE ON checkpoint_policy_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER cost_model_definition_immutable
BEFORE UPDATE ON cost_model_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER stage_definition_definition_immutable
BEFORE UPDATE ON stage_definition_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER stage_profile_definition_immutable
BEFORE UPDATE ON stage_profile_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER connector_definition_immutable
BEFORE UPDATE ON connector_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');
CREATE TRIGGER execution_graph_definition_immutable
BEFORE UPDATE ON execution_graph_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state', 'topological_order');

CREATE TRIGGER a_stage_interface_revision_lifecycle
BEFORE UPDATE OR DELETE ON stage_interface_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_stage_result_equivalence_lifecycle
BEFORE UPDATE OR DELETE ON stage_result_equivalence_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_input_canonicalization_lifecycle
BEFORE UPDATE OR DELETE ON input_canonicalization_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_worker_profile_revision_lifecycle
BEFORE UPDATE OR DELETE ON worker_profile_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_stage_runtime_model_lifecycle
BEFORE UPDATE OR DELETE ON stage_runtime_model_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_stage_cache_policy_lifecycle
BEFORE UPDATE OR DELETE ON stage_cache_policy_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_checkpoint_policy_lifecycle
BEFORE UPDATE OR DELETE ON checkpoint_policy_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_cost_model_lifecycle
BEFORE UPDATE OR DELETE ON cost_model_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_stage_definition_lifecycle
BEFORE UPDATE OR DELETE ON stage_definition_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_stage_profile_lifecycle
BEFORE UPDATE OR DELETE ON stage_profile_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_connector_lifecycle
BEFORE UPDATE OR DELETE ON connector_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER a_execution_graph_lifecycle
BEFORE UPDATE OR DELETE ON execution_graph_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_catalog_revision();
CREATE TRIGGER z_execution_graph_activation_required
BEFORE UPDATE OF state ON execution_graph_revisions
FOR EACH ROW EXECUTE FUNCTION vela_require_execution_graph_activation();

CREATE TRIGGER execution_graph_stages_active_guard
BEFORE INSERT OR UPDATE OR DELETE ON execution_graph_stages
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_execution_graph_member();
CREATE TRIGGER execution_graph_stages_truncate_guard
BEFORE TRUNCATE ON execution_graph_stages
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_active_execution_graph_truncate();
CREATE TRIGGER execution_graph_edges_active_guard
BEFORE INSERT OR UPDATE OR DELETE ON execution_graph_edges
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_execution_graph_member();
CREATE TRIGGER execution_graph_edges_truncate_guard
BEFORE TRUNCATE ON execution_graph_edges
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_active_execution_graph_truncate();
CREATE TRIGGER execution_graph_inputs_active_guard
BEFORE INSERT OR UPDATE OR DELETE ON execution_graph_inputs
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_execution_graph_member();
CREATE TRIGGER execution_graph_inputs_truncate_guard
BEFORE TRUNCATE ON execution_graph_inputs
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_active_execution_graph_truncate();
CREATE TRIGGER execution_graph_outputs_active_guard
BEFORE INSERT OR UPDATE OR DELETE ON execution_graph_outputs
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_execution_graph_member();
CREATE TRIGGER execution_graph_outputs_truncate_guard
BEFORE TRUNCATE ON execution_graph_outputs
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_active_execution_graph_truncate();
CREATE TRIGGER execution_profile_stage_options_active_guard
BEFORE INSERT OR UPDATE OR DELETE ON execution_profile_stage_options
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_execution_profile_member();
CREATE TRIGGER execution_profile_connector_options_active_guard
BEFORE INSERT OR UPDATE OR DELETE ON execution_profile_connector_options
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_execution_profile_member();

CREATE FUNCTION vela_activate_execution_graph(
    p_execution_graph_revision_id uuid,
    p_expected_content_digest bytea
) RETURNS TABLE(state catalog_state, topological_order text[])
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_graph public.execution_graph_revisions%ROWTYPE;
    v_stage_count integer;
    v_next_stage text;
    v_order text[] := ARRAY[]::text[];
BEGIN
    SELECT graph.* INTO v_graph
    FROM public.execution_graph_revisions AS graph
    WHERE graph.id = p_execution_graph_revision_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            CONSTRAINT = 'execution_graph_revision_not_found',
            MESSAGE = 'ExecutionGraphRevision does not exist';
    END IF;
    IF octet_length(p_expected_content_digest) <> 32
       OR v_graph.content_digest <> p_expected_content_digest THEN
        RAISE EXCEPTION USING
            ERRCODE = '22000',
            CONSTRAINT = 'execution_graph_content_digest_mismatch',
            MESSAGE = 'ExecutionGraphRevision content digest does not match activation request';
    END IF;
    IF v_graph.state = 'ACTIVE' THEN
        RETURN QUERY SELECT v_graph.state, v_graph.topological_order;
        RETURN;
    END IF;
    IF v_graph.state NOT IN ('CERTIFIED', 'CANARY') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_graph_not_certified',
            MESSAGE = 'ExecutionGraphRevision must be CERTIFIED or CANARY before activation';
    END IF;

    SELECT count(*) INTO v_stage_count
    FROM public.execution_graph_stages
    WHERE execution_graph_revision_id = p_execution_graph_revision_id;
    IF v_stage_count = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_has_no_stages',
            MESSAGE = 'ExecutionGraphRevision has no stages';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS stage
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        CROSS JOIN LATERAL unnest(definition.required_input_ports) AS required(port)
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND NOT definition.input_ports ? required.port
    ) OR EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS stage
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        CROSS JOIN LATERAL unnest(definition.required_output_ports) AS required(port)
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND NOT definition.output_ports ? required.port
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_definition_required_port_missing',
            MESSAGE = 'StageDefinition required port is absent from its interface map';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_edges AS edge
        JOIN public.execution_graph_stages AS source_stage
          ON source_stage.execution_graph_revision_id = edge.execution_graph_revision_id
         AND source_stage.stage_key = edge.source_stage_key
        JOIN public.stage_definition_revisions AS source_definition
          ON source_definition.id = source_stage.stage_definition_revision_id
        JOIN public.execution_graph_stages AS destination_stage
          ON destination_stage.execution_graph_revision_id = edge.execution_graph_revision_id
         AND destination_stage.stage_key = edge.destination_stage_key
        JOIN public.stage_definition_revisions AS destination_definition
          ON destination_definition.id = destination_stage.stage_definition_revision_id
        WHERE edge.execution_graph_revision_id = p_execution_graph_revision_id
          AND (
              NOT source_definition.output_ports ? edge.source_port
              OR NOT destination_definition.input_ports ? edge.destination_port
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_edge_port_missing',
            MESSAGE = 'ExecutionGraph edge references an unknown StageDefinition port';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_edges AS edge
        JOIN public.execution_graph_stages AS source_stage
          ON source_stage.execution_graph_revision_id = edge.execution_graph_revision_id
         AND source_stage.stage_key = edge.source_stage_key
        JOIN public.stage_definition_revisions AS source_definition
          ON source_definition.id = source_stage.stage_definition_revision_id
        JOIN public.execution_graph_stages AS destination_stage
          ON destination_stage.execution_graph_revision_id = edge.execution_graph_revision_id
         AND destination_stage.stage_key = edge.destination_stage_key
        JOIN public.stage_definition_revisions AS destination_definition
          ON destination_definition.id = destination_stage.stage_definition_revision_id
        WHERE edge.execution_graph_revision_id = p_execution_graph_revision_id
          AND source_definition.output_ports ->> edge.source_port
              <> destination_definition.input_ports ->> edge.destination_port
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_edge_interface_incompatible',
            MESSAGE = 'ExecutionGraph edge interfaces are incompatible';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_inputs AS input
        JOIN public.execution_graph_stages AS stage
          ON stage.execution_graph_revision_id = input.execution_graph_revision_id
         AND stage.stage_key = input.destination_stage_key
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        WHERE input.execution_graph_revision_id = p_execution_graph_revision_id
          AND (
              NOT definition.input_ports ? input.destination_port
              OR definition.input_ports ->> input.destination_port <> input.interface_revision_id::text
          )
    ) OR EXISTS (
        SELECT 1
        FROM public.execution_graph_outputs AS output
        JOIN public.execution_graph_stages AS stage
          ON stage.execution_graph_revision_id = output.execution_graph_revision_id
         AND stage.stage_key = output.source_stage_key
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        WHERE output.execution_graph_revision_id = p_execution_graph_revision_id
          AND (
              NOT definition.output_ports ? output.source_port
              OR definition.output_ports ->> output.source_port <> output.interface_revision_id::text
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_boundary_interface_incompatible',
            MESSAGE = 'ExecutionGraph boundary interface is incompatible';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS stage
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        CROSS JOIN LATERAL unnest(definition.required_input_ports) AS required(port)
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND NOT EXISTS (
              SELECT 1 FROM public.execution_graph_edges AS edge
              WHERE edge.execution_graph_revision_id = stage.execution_graph_revision_id
                AND edge.destination_stage_key = stage.stage_key
                AND edge.destination_port = required.port
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.execution_graph_inputs AS input
              WHERE input.execution_graph_revision_id = stage.execution_graph_revision_id
                AND input.destination_stage_key = stage.stage_key
                AND input.destination_port = required.port
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_required_input_missing',
            MESSAGE = 'ExecutionGraph required Stage input has no source';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS stage
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        CROSS JOIN LATERAL unnest(definition.required_output_ports) AS required(port)
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND NOT EXISTS (
              SELECT 1 FROM public.execution_graph_edges AS edge
              WHERE edge.execution_graph_revision_id = stage.execution_graph_revision_id
                AND edge.source_stage_key = stage.stage_key
                AND edge.source_port = required.port
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.execution_graph_outputs AS output
              WHERE output.execution_graph_revision_id = stage.execution_graph_revision_id
                AND output.source_stage_key = stage.stage_key
                AND output.source_port = required.port
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_required_output_missing',
            MESSAGE = 'ExecutionGraph required Stage output has no destination';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS stage
        LEFT JOIN public.execution_graph_edges AS edge
          ON edge.execution_graph_revision_id = stage.execution_graph_revision_id
         AND edge.source_stage_key = stage.stage_key
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
        GROUP BY stage.stage_key, stage.max_fan_out
        HAVING count(edge.id) > stage.max_fan_out
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_fan_out_exceeded',
            MESSAGE = 'ExecutionGraph Stage fan-out exceeds its declared bound';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS stage
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND (
              definition.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE')
              OR NOT EXISTS (
                  SELECT 1
                  FROM public.stage_profile_revisions AS profile
                  JOIN public.worker_profile_revisions AS worker_profile
                    ON worker_profile.id = profile.worker_profile_revision_id
                  JOIN public.stage_result_equivalence_revisions AS equivalence
                    ON equivalence.id = profile.result_equivalence_revision_id
                  WHERE profile.stage_definition_revision_id = stage.stage_definition_revision_id
                    AND profile.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
                    AND worker_profile.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
                    AND equivalence.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
                    AND worker_profile.device_count > 0
              )
              OR definition.cache_policy_revision_id IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM public.stage_cache_policy_revisions AS policy
                     WHERE policy.id = definition.cache_policy_revision_id
                       AND policy.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
                 )
              OR definition.checkpoint_policy_revision_id IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM public.checkpoint_policy_revisions AS policy
                     WHERE policy.id = definition.checkpoint_policy_revision_id
                       AND policy.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
                 )
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_graph_stage_certification_incomplete',
            MESSAGE = 'ExecutionGraph Stage has no complete certified profile';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS stage
        JOIN public.stage_definition_revisions AS definition
          ON definition.id = stage.stage_definition_revision_id
        CROSS JOIN LATERAL jsonb_each_text(
            definition.input_ports || definition.output_ports
        ) AS port(name, interface_id)
        LEFT JOIN public.stage_interface_revisions AS interface
          ON interface.id = port.interface_id::uuid
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND (
              interface.id IS NULL
              OR interface.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          )
    ) OR EXISTS (
        SELECT 1
        FROM public.execution_graph_inputs AS input
        JOIN public.stage_interface_revisions AS interface
          ON interface.id = input.interface_revision_id
        WHERE input.execution_graph_revision_id = p_execution_graph_revision_id
          AND interface.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE')
    ) OR EXISTS (
        SELECT 1
        FROM public.execution_graph_outputs AS output
        JOIN public.stage_interface_revisions AS interface
          ON interface.id = output.interface_revision_id
        WHERE output.execution_graph_revision_id = p_execution_graph_revision_id
          AND interface.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_graph_interface_certification_incomplete',
            MESSAGE = 'ExecutionGraph interface certification is incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_edges AS edge
        JOIN public.execution_graph_stages AS source_stage
          ON source_stage.execution_graph_revision_id = edge.execution_graph_revision_id
         AND source_stage.stage_key = edge.source_stage_key
        JOIN public.stage_definition_revisions AS source_definition
          ON source_definition.id = source_stage.stage_definition_revision_id
        JOIN public.execution_graph_stages AS destination_stage
          ON destination_stage.execution_graph_revision_id = edge.execution_graph_revision_id
         AND destination_stage.stage_key = edge.destination_stage_key
        JOIN public.stage_definition_revisions AS destination_definition
          ON destination_definition.id = destination_stage.stage_definition_revision_id
        WHERE edge.execution_graph_revision_id = p_execution_graph_revision_id
          AND NOT EXISTS (
              SELECT 1 FROM public.connector_revisions AS connector
              WHERE connector.source_interface_revision_id =
                    (source_definition.output_ports ->> edge.source_port)::uuid
                AND connector.destination_interface_revision_id =
                    (destination_definition.input_ports ->> edge.destination_port)::uuid
                AND connector.durable_fallback
                AND connector.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_graph_connector_fallback_missing',
            MESSAGE = 'ExecutionGraph edge has no certified durable fallback connector';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.execution_graph_outputs AS output
        WHERE output.execution_graph_revision_id = p_execution_graph_revision_id
          AND output.required
          AND output.output_key = v_graph.final_output_contract ->> 'output_key'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'execution_graph_final_output_missing',
            MESSAGE = 'ExecutionGraph final output contract is not connected';
    END IF;

    WHILE cardinality(v_order) < v_stage_count LOOP
        SELECT min(stage.stage_key) INTO v_next_stage
        FROM public.execution_graph_stages AS stage
        WHERE stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND NOT stage.stage_key = ANY(v_order)
          AND NOT EXISTS (
              SELECT 1 FROM public.execution_graph_edges AS edge
              WHERE edge.execution_graph_revision_id = p_execution_graph_revision_id
                AND edge.destination_stage_key = stage.stage_key
                AND NOT edge.source_stage_key = ANY(v_order)
          );
        IF v_next_stage IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'execution_graph_cycle',
                MESSAGE = 'ExecutionGraph contains a cycle';
        END IF;
        v_order := array_append(v_order, v_next_stage);
    END LOOP;

    PERFORM set_config('vela.execution_graph_activation', 'on', true);
    UPDATE public.execution_graph_revisions AS graph
    SET state = 'ACTIVE', topological_order = v_order
    WHERE graph.id = p_execution_graph_revision_id
    RETURNING graph.* INTO v_graph;
    RETURN QUERY SELECT v_graph.state, v_graph.topological_order;
END
$$;
REVOKE ALL ON FUNCTION vela_activate_execution_graph(uuid, bytea) FROM PUBLIC;
ALTER FUNCTION vela_activate_execution_graph(uuid, bytea)
    OWNER TO vela_catalog_promotion_owner;

ALTER TABLE stage_interface_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_result_equivalence_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE input_canonicalization_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE worker_profile_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_runtime_model_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_cache_policy_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE checkpoint_policy_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE cost_model_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_definition_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_profile_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE connector_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE execution_graph_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE execution_graph_stages OWNER TO vela_catalog_promotion_owner;
ALTER TABLE execution_graph_edges OWNER TO vela_catalog_promotion_owner;
ALTER TABLE execution_graph_inputs OWNER TO vela_catalog_promotion_owner;
ALTER TABLE execution_graph_outputs OWNER TO vela_catalog_promotion_owner;
ALTER TABLE execution_profile_stage_options OWNER TO vela_catalog_promotion_owner;
ALTER TABLE execution_profile_connector_options OWNER TO vela_catalog_promotion_owner;

GRANT USAGE ON SCHEMA public TO vela_stage_catalog_activation;
GRANT EXECUTE ON FUNCTION vela_activate_execution_graph(uuid, bytea)
    TO vela_stage_catalog_activation;
GRANT SELECT ON stage_interface_revisions, stage_result_equivalence_revisions,
    input_canonicalization_revisions, worker_profile_revisions,
    stage_runtime_model_revisions, stage_cache_policy_revisions,
    checkpoint_policy_revisions, cost_model_revisions,
    stage_definition_revisions, stage_profile_revisions, connector_revisions,
    execution_graph_revisions, execution_graph_stages, execution_graph_edges,
    execution_graph_inputs, execution_graph_outputs,
    execution_profile_stage_options, execution_profile_connector_options
    TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    LOCK TABLE execution_graph_revisions IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM execution_graph_revisions)
       OR EXISTS (SELECT 1 FROM stage_interface_revisions)
       OR EXISTS (SELECT 1 FROM stage_definition_revisions)
       OR EXISTS (SELECT 1 FROM stage_profile_revisions)
       OR EXISTS (SELECT 1 FROM connector_revisions)
       OR EXISTS (SELECT 1 FROM worker_profile_revisions)
       OR EXISTS (SELECT 1 FROM stage_result_equivalence_revisions)
       OR EXISTS (SELECT 1 FROM input_canonicalization_revisions)
       OR EXISTS (SELECT 1 FROM stage_runtime_model_revisions)
       OR EXISTS (SELECT 1 FROM stage_cache_policy_revisions)
       OR EXISTS (SELECT 1 FROM checkpoint_policy_revisions)
       OR EXISTS (SELECT 1 FROM cost_model_revisions) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_execution_catalog_rollback_is_unsafe',
            MESSAGE = 'Stage Execution Catalog revisions or authority exist';
    END IF;
END
$$;

REVOKE SELECT ON stage_interface_revisions, stage_result_equivalence_revisions,
    input_canonicalization_revisions, worker_profile_revisions,
    stage_runtime_model_revisions, stage_cache_policy_revisions,
    checkpoint_policy_revisions, cost_model_revisions,
    stage_definition_revisions, stage_profile_revisions, connector_revisions,
    execution_graph_revisions, execution_graph_stages, execution_graph_edges,
    execution_graph_inputs, execution_graph_outputs,
    execution_profile_stage_options, execution_profile_connector_options
    FROM vela_internal;
REVOKE EXECUTE ON FUNCTION vela_activate_execution_graph(uuid, bytea)
    FROM vela_stage_catalog_activation;
REVOKE USAGE ON SCHEMA public FROM vela_stage_catalog_activation;

DROP FUNCTION vela_activate_execution_graph(uuid, bytea);
DROP TRIGGER execution_profile_connector_options_active_guard
    ON execution_profile_connector_options;
DROP TRIGGER execution_profile_stage_options_active_guard
    ON execution_profile_stage_options;
DROP TRIGGER execution_graph_outputs_truncate_guard ON execution_graph_outputs;
DROP TRIGGER execution_graph_outputs_active_guard ON execution_graph_outputs;
DROP TRIGGER execution_graph_inputs_truncate_guard ON execution_graph_inputs;
DROP TRIGGER execution_graph_inputs_active_guard ON execution_graph_inputs;
DROP TRIGGER execution_graph_edges_truncate_guard ON execution_graph_edges;
DROP TRIGGER execution_graph_edges_active_guard ON execution_graph_edges;
DROP TRIGGER execution_graph_stages_truncate_guard ON execution_graph_stages;
DROP TRIGGER execution_graph_stages_active_guard ON execution_graph_stages;

DROP TRIGGER z_execution_graph_activation_required ON execution_graph_revisions;
DROP TRIGGER a_execution_graph_lifecycle ON execution_graph_revisions;
DROP TRIGGER a_connector_lifecycle ON connector_revisions;
DROP TRIGGER a_stage_profile_lifecycle ON stage_profile_revisions;
DROP TRIGGER a_stage_definition_lifecycle ON stage_definition_revisions;
DROP TRIGGER a_cost_model_lifecycle ON cost_model_revisions;
DROP TRIGGER a_checkpoint_policy_lifecycle ON checkpoint_policy_revisions;
DROP TRIGGER a_stage_cache_policy_lifecycle ON stage_cache_policy_revisions;
DROP TRIGGER a_stage_runtime_model_lifecycle ON stage_runtime_model_revisions;
DROP TRIGGER a_worker_profile_revision_lifecycle ON worker_profile_revisions;
DROP TRIGGER a_input_canonicalization_lifecycle ON input_canonicalization_revisions;
DROP TRIGGER a_stage_result_equivalence_lifecycle ON stage_result_equivalence_revisions;
DROP TRIGGER a_stage_interface_revision_lifecycle ON stage_interface_revisions;

DROP TRIGGER execution_graph_definition_immutable ON execution_graph_revisions;
DROP TRIGGER connector_definition_immutable ON connector_revisions;
DROP TRIGGER stage_profile_definition_immutable ON stage_profile_revisions;
DROP TRIGGER stage_definition_definition_immutable ON stage_definition_revisions;
DROP TRIGGER cost_model_definition_immutable ON cost_model_revisions;
DROP TRIGGER checkpoint_policy_definition_immutable ON checkpoint_policy_revisions;
DROP TRIGGER stage_cache_policy_definition_immutable ON stage_cache_policy_revisions;
DROP TRIGGER stage_runtime_model_definition_immutable ON stage_runtime_model_revisions;
DROP TRIGGER worker_profile_revision_definition_immutable ON worker_profile_revisions;
DROP TRIGGER input_canonicalization_definition_immutable ON input_canonicalization_revisions;
DROP TRIGGER stage_result_equivalence_definition_immutable ON stage_result_equivalence_revisions;
DROP TRIGGER stage_interface_revision_definition_immutable ON stage_interface_revisions;

DROP FUNCTION vela_require_execution_graph_activation();
DROP FUNCTION vela_guard_active_execution_profile_member();
DROP FUNCTION vela_guard_active_execution_graph_truncate();
DROP FUNCTION vela_guard_active_execution_graph_member();
DROP FUNCTION vela_guard_stage_catalog_revision();

DROP TABLE execution_profile_connector_options;
DROP TABLE execution_profile_stage_options;
ALTER TABLE execution_profile_revisions DROP COLUMN execution_graph_revision_id;
DROP TABLE execution_graph_outputs;
DROP TABLE execution_graph_inputs;
DROP TABLE execution_graph_edges;
DROP TABLE execution_graph_stages;
DROP TABLE execution_graph_revisions;
DROP TABLE connector_revisions;
DROP TABLE stage_profile_revisions;
DROP TABLE stage_definition_revisions;
DROP TABLE cost_model_revisions;
DROP TABLE checkpoint_policy_revisions;
DROP TABLE stage_cache_policy_revisions;
DROP TABLE stage_runtime_model_revisions;
DROP TABLE worker_profile_revisions;
DROP TABLE input_canonicalization_revisions;
DROP TABLE stage_result_equivalence_revisions;
DROP TABLE stage_interface_revisions;
-- +goose StatementEnd
