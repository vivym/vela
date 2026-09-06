-- +goose Up
CREATE TABLE worker_bootstrap_manifests (
    worker_bundle_id uuid PRIMARY KEY REFERENCES worker_bundles(id),
    layout_digest bytea NOT NULL CHECK (octet_length(layout_digest) = 32),
    manifest bytea NOT NULL CHECK (octet_length(manifest) BETWEEN 1 AND 4194304),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (sha256(manifest) = layout_digest)
);

-- Member UUIDs are never reused for a second first-use claim, even at a new epoch.
CREATE TABLE worker_bootstrap_claims (
    request_id uuid PRIMARY KEY CHECK (request_id <> '00000000-0000-0000-0000-000000000000'),
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    worker_member_id uuid NOT NULL UNIQUE CHECK (worker_member_id <> '00000000-0000-0000-0000-000000000000'),
    worker_member_epoch bigint NOT NULL CHECK (worker_member_epoch > 0),
    worker_bundle_id uuid NOT NULL REFERENCES worker_bootstrap_manifests(worker_bundle_id),
    node_identity text NOT NULL CHECK (length(node_identity) BETWEEN 1 AND 253),
    actor_identity text NOT NULL CHECK (length(actor_identity) BETWEEN 1 AND 500),
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE worker_bootstrap_receipts (
    request_id uuid PRIMARY KEY REFERENCES worker_bootstrap_claims(request_id),
    worker_journal_id uuid NOT NULL UNIQUE CHECK (worker_journal_id <> '00000000-0000-0000-0000-000000000000'),
    worker_scope bytea NOT NULL CHECK (octet_length(worker_scope) = 32),
    runtime_journal_id uuid NOT NULL UNIQUE CHECK (runtime_journal_id <> '00000000-0000-0000-0000-000000000000'),
    runtime_scope bytea NOT NULL CHECK (octet_length(runtime_scope) = 32),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (worker_journal_id <> runtime_journal_id)
);

CREATE CONSTRAINT TRIGGER worker_bootstrap_claims_require_synchronous_quorum
AFTER INSERT ON worker_bootstrap_claims DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();
CREATE CONSTRAINT TRIGGER worker_bootstrap_receipts_require_synchronous_quorum
AFTER INSERT ON worker_bootstrap_receipts DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();
CREATE TRIGGER worker_bootstrap_recovery_admission_gate
BEFORE INSERT ON worker_bootstrap_claims
FOR EACH ROW EXECUTE FUNCTION vela_require_admission_open();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_recovery_inventory() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT jsonb_build_object(
        'jobs', (SELECT count(*) FROM public.jobs WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
        'attempts', (SELECT count(*) FROM public.attempts WHERE graph_state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
        'stage_runs', (SELECT count(*) FROM public.stage_runs WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
        'stage_attempts', (SELECT count(*) FROM public.stage_attempts WHERE state NOT IN ('SUCCEEDED','FAILED','LOST','CANCELED')),
        'stage_leases', (SELECT count(*) FROM public.stage_leases WHERE state = 'ACTIVE'),
        'stage_allocations', (SELECT count(*) FROM public.stage_allocations WHERE state = 'ALLOCATED'),
        'materialization_leases', (SELECT count(*) FROM public.stage_materialization_leases WHERE state = 'ACTIVE'),
        'transfer_tickets', (SELECT count(*) FROM public.transfer_tickets WHERE state = 'ACTIVE'),
        'finalization_claims', (SELECT count(*) FROM public.stage_graph_finalization_claims WHERE state = 'ACTIVE'),
        'execution_pins', (SELECT count(*) FROM public.stage_artifact_pins WHERE state = 'ACTIVE' AND pin_kind IN ('EXECUTION','FINALIZATION')),
        'edge_buffer_credits', (SELECT count(*) FROM public.edge_buffer_credits WHERE state = 'HELD'),
        'storage_reservations', (SELECT count(*) FROM public.stage_storage_reservations WHERE state = 'RESERVED'),
        'worker_bootstrap_claims', (SELECT count(*) FROM public.worker_bootstrap_claims AS claim
            WHERE NOT EXISTS (SELECT 1 FROM public.worker_bootstrap_receipts AS receipt WHERE receipt.request_id = claim.request_id))
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION vela_reject_worker_bootstrap_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = 'Worker bootstrap history is immutable';
END
$$;
-- +goose StatementEnd
CREATE TRIGGER worker_bootstrap_manifests_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_bootstrap_manifests
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_bootstrap_mutation();
CREATE TRIGGER worker_bootstrap_claims_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_bootstrap_claims
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_bootstrap_mutation();
CREATE TRIGGER worker_bootstrap_receipts_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_bootstrap_receipts
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_bootstrap_mutation();

-- +goose StatementBegin
CREATE FUNCTION vela_claim_worker_bootstrap(
    p_request_id uuid, p_worker_id uuid, p_worker_epoch bigint, p_member_id uuid,
    p_manifest bytea, p_actor text
) RETURNS TABLE (
    request_id uuid, fresh boolean, worker_instance_id uuid, worker_instance_epoch bigint,
    worker_member_id uuid, worker_member_epoch bigint, node_identity text,
    bundle_digest bytea, claimed_at timestamptz
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_bundle public.worker_bundles%ROWTYPE;
    v_claim public.worker_bootstrap_claims%ROWTYPE;
    v_payload jsonb;
    v_launch jsonb;
    v_member jsonb;
    v_layout bytea;
    v_count integer;
BEGIN
    IF p_request_id IS NULL OR p_request_id = '00000000-0000-0000-0000-000000000000'
       OR p_worker_id IS NULL OR p_member_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
       OR p_actor IS NULL OR length(p_actor) NOT BETWEEN 1 AND 500 OR btrim(p_actor) <> p_actor
       OR p_manifest IS NULL OR octet_length(p_manifest) NOT BETWEEN 1 AND 4194304 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker bootstrap request is invalid';
    END IF;
    v_payload := convert_from(p_manifest, 'UTF8')::jsonb;
    v_layout := sha256(p_manifest);
    IF v_payload ->> 'schema' IS DISTINCT FROM 'vela.worker-bundle-actuation/v2'
       OR v_payload #>> '{bundle,schema_version}' IS DISTINCT FROM '2'
       OR v_payload #>> '{bundle,revision_digest}' IS DISTINCT FROM '' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker bootstrap manifest format is invalid';
    END IF;

    -- Serialize with observation/fencing and every competing claim for this Worker.
    SELECT * INTO v_worker FROM public.worker_instances WHERE id = p_worker_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker bootstrap target does not exist';
    END IF;
    SELECT * INTO v_bundle FROM public.worker_bundles WHERE id = v_worker.worker_bundle_id FOR SHARE;
    IF NOT FOUND OR v_bundle.layout_digest IS DISTINCT FROM v_layout
       OR v_bundle.id::text IS DISTINCT FROM v_payload #>> '{bundle,worker_bundle_id}'
       OR v_bundle.residency_plan_revision_id::text IS DISTINCT FROM v_payload #>> '{bundle,plan_revision_id}'
       OR v_worker.residency_plan_revision_id IS DISTINCT FROM v_bundle.residency_plan_revision_id THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap manifest is not the approved bundle';
    END IF;
    SELECT count(*) INTO v_count FROM jsonb_array_elements(v_payload #> '{bundle,worker_instances}') AS entry
        WHERE entry ->> 'id' = p_worker_id::text;
    IF v_count <> 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker bootstrap manifest must contain exactly one target';
    END IF;
    SELECT entry INTO v_launch FROM jsonb_array_elements(v_payload #> '{bundle,worker_instances}') AS entry
        WHERE entry ->> 'id' = p_worker_id::text;
    IF v_launch ->> 'instance_epoch' IS DISTINCT FROM p_worker_epoch::text
       OR v_launch ->> 'worker_profile_revision_id' IS DISTINCT FROM v_worker.worker_profile_revision_id::text
       OR v_launch ->> 'capacity_pool_id' IS DISTINCT FROM v_worker.capacity_pool_id::text
       OR jsonb_array_length(v_launch -> 'members') IS DISTINCT FROM v_worker.desired_member_count THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap topology differs from approved authority';
    END IF;
    SELECT count(*) INTO v_count FROM jsonb_array_elements(v_launch -> 'members') AS entry
        WHERE entry ->> 'id' = p_member_id::text;
    IF v_count <> 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker bootstrap member is not in the approved topology';
    END IF;
    SELECT entry INTO v_member FROM jsonb_array_elements(v_launch -> 'members') AS entry
        WHERE entry ->> 'id' = p_member_id::text;

    SELECT * INTO v_claim FROM public.worker_bootstrap_claims WHERE worker_bootstrap_claims.request_id = p_request_id;
    IF FOUND THEN
        IF v_claim.worker_instance_id <> p_worker_id OR v_claim.worker_instance_epoch <> p_worker_epoch
           OR v_claim.worker_member_id <> p_member_id OR v_claim.actor_identity <> p_actor
           OR v_claim.worker_bundle_id <> v_bundle.id THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap request identity was reused';
        END IF;
        RETURN QUERY SELECT v_claim.request_id, false, v_claim.worker_instance_id, v_claim.worker_instance_epoch,
            v_claim.worker_member_id, v_claim.worker_member_epoch, v_claim.node_identity, v_layout, v_claim.claimed_at;
        RETURN;
    END IF;
    IF v_worker.lifecycle_state <> 'PROVISIONING' OR v_worker.instance_epoch <> p_worker_epoch
       OR v_worker.observed_at IS NOT NULL OR v_worker.device_set_id IS NOT NULL
       OR v_worker.control_session_epoch <> 1 OR v_worker.control_session_id IS NOT NULL
       OR EXISTS (SELECT 1 FROM public.worker_instance_epochs WHERE worker_instance_epochs.worker_instance_id = p_worker_id)
       OR EXISTS (SELECT 1 FROM public.worker_members WHERE worker_members.worker_instance_id = p_worker_id)
       OR EXISTS (SELECT 1 FROM public.model_residencies WHERE model_residencies.worker_instance_id = p_worker_id) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap requires unobserved first-use authority';
    END IF;

    INSERT INTO public.worker_bootstrap_manifests(worker_bundle_id, layout_digest, manifest)
    VALUES (v_bundle.id, v_layout, p_manifest) ON CONFLICT DO NOTHING;
    IF NOT EXISTS (SELECT 1 FROM public.worker_bootstrap_manifests AS manifest
        WHERE manifest.worker_bundle_id = v_bundle.id AND manifest.manifest = p_manifest) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap manifest history changed';
    END IF;
    INSERT INTO public.worker_bootstrap_claims(request_id, worker_instance_id, worker_instance_epoch,
        worker_member_id, worker_member_epoch, worker_bundle_id, node_identity, actor_identity)
    VALUES (p_request_id, p_worker_id, p_worker_epoch, p_member_id, (v_member ->> 'member_epoch')::bigint,
        v_bundle.id, v_member ->> 'node_identity', p_actor) RETURNING * INTO v_claim;
    RETURN QUERY SELECT v_claim.request_id, true, v_claim.worker_instance_id, v_claim.worker_instance_epoch,
        v_claim.worker_member_id, v_claim.worker_member_epoch, v_claim.node_identity, v_layout, v_claim.claimed_at;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION vela_record_worker_bootstrap_receipt(
    p_request_id uuid, p_worker_journal_id uuid, p_worker_scope bytea,
    p_runtime_journal_id uuid, p_runtime_scope bytea, p_actor text
) RETURNS timestamptz
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_claim public.worker_bootstrap_claims%ROWTYPE;
    v_receipt public.worker_bootstrap_receipts%ROWTYPE;
BEGIN
    IF p_request_id IS NULL OR p_worker_journal_id IS NULL OR p_runtime_journal_id IS NULL
       OR p_worker_scope IS NULL OR octet_length(p_worker_scope) <> 32
       OR p_runtime_scope IS NULL OR octet_length(p_runtime_scope) <> 32 OR p_actor IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker bootstrap receipt is invalid';
    END IF;
    SELECT * INTO v_claim FROM public.worker_bootstrap_claims WHERE request_id = p_request_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker bootstrap claim does not exist';
    END IF;
    IF v_claim.actor_identity <> p_actor THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap receipt actor differs from claim';
    END IF;
    INSERT INTO public.worker_bootstrap_receipts(request_id, worker_journal_id, worker_scope, runtime_journal_id, runtime_scope)
    VALUES (p_request_id, p_worker_journal_id, p_worker_scope, p_runtime_journal_id, p_runtime_scope)
    ON CONFLICT (request_id) DO NOTHING;
    SELECT * INTO v_receipt FROM public.worker_bootstrap_receipts WHERE request_id = p_request_id;
    IF v_receipt.worker_journal_id IS DISTINCT FROM p_worker_journal_id
       OR v_receipt.worker_scope IS DISTINCT FROM p_worker_scope
       OR v_receipt.runtime_journal_id IS DISTINCT FROM p_runtime_journal_id
       OR v_receipt.runtime_scope IS DISTINCT FROM p_runtime_scope THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap receipt identity was changed';
    END IF;
    RETURN v_receipt.recorded_at;
END
$$;
-- +goose StatementEnd

ALTER TABLE worker_bootstrap_manifests OWNER TO vela_fleet_owner;
ALTER TABLE worker_bootstrap_claims OWNER TO vela_fleet_owner;
ALTER TABLE worker_bootstrap_receipts OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reject_worker_bootstrap_mutation() OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_claim_worker_bootstrap(uuid, uuid, bigint, uuid, bytea, text) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text) OWNER TO vela_fleet_owner;
REVOKE ALL ON TABLE worker_bootstrap_manifests, worker_bootstrap_claims, worker_bootstrap_receipts FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_reject_worker_bootstrap_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_claim_worker_bootstrap(uuid, uuid, bigint, uuid, bytea, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_claim_worker_bootstrap(uuid, uuid, bigint, uuid, bytea, text) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text) TO vela_fleet;
GRANT SELECT ON worker_bootstrap_claims, worker_bootstrap_receipts TO vela_recovery_owner;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM worker_bootstrap_claims) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap history prohibits rollback';
    END IF;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_recovery_inventory() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT jsonb_build_object(
        'jobs', (SELECT count(*) FROM public.jobs WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
        'attempts', (SELECT count(*) FROM public.attempts WHERE graph_state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
        'stage_runs', (SELECT count(*) FROM public.stage_runs WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
        'stage_attempts', (SELECT count(*) FROM public.stage_attempts WHERE state NOT IN ('SUCCEEDED','FAILED','LOST','CANCELED')),
        'stage_leases', (SELECT count(*) FROM public.stage_leases WHERE state = 'ACTIVE'),
        'stage_allocations', (SELECT count(*) FROM public.stage_allocations WHERE state = 'ALLOCATED'),
        'materialization_leases', (SELECT count(*) FROM public.stage_materialization_leases WHERE state = 'ACTIVE'),
        'transfer_tickets', (SELECT count(*) FROM public.transfer_tickets WHERE state = 'ACTIVE'),
        'finalization_claims', (SELECT count(*) FROM public.stage_graph_finalization_claims WHERE state = 'ACTIVE'),
        'execution_pins', (SELECT count(*) FROM public.stage_artifact_pins WHERE state = 'ACTIVE' AND pin_kind IN ('EXECUTION','FINALIZATION')),
        'edge_buffer_credits', (SELECT count(*) FROM public.edge_buffer_credits WHERE state = 'HELD'),
        'storage_reservations', (SELECT count(*) FROM public.stage_storage_reservations WHERE state = 'RESERVED')
    );
$$;
-- +goose StatementEnd
DROP FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text);
DROP FUNCTION vela_claim_worker_bootstrap(uuid, uuid, bigint, uuid, bytea, text);
DROP TABLE worker_bootstrap_receipts;
DROP TABLE worker_bootstrap_claims;
DROP TABLE worker_bootstrap_manifests;
DROP FUNCTION vela_reject_worker_bootstrap_mutation();
