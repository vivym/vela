-- +goose Up
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vela_recovery') THEN
        CREATE ROLE vela_recovery NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vela_recovery_owner') THEN
        CREATE ROLE vela_recovery_owner NOLOGIN BYPASSRLS;
    END IF;
END
$$;

CREATE TABLE recovery_admission_control (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    database_identity uuid NOT NULL DEFAULT gen_random_uuid(),
    generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    admission_open boolean NOT NULL DEFAULT true,
    operation_id uuid,
    CHECK (admission_open = (operation_id IS NULL))
);
INSERT INTO recovery_admission_control (singleton) VALUES (true);

CREATE TABLE recovery_operations (
    id uuid PRIMARY KEY,
    database_identity uuid NOT NULL,
    generation bigint NOT NULL UNIQUE CHECK (generation > 1),
    system_identifier text NOT NULL,
    database_name text NOT NULL,
    database_oid oid NOT NULL,
    actor text NOT NULL,
    closed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    reopened_at timestamptz
);
ALTER TABLE recovery_admission_control ADD FOREIGN KEY (operation_id)
    REFERENCES recovery_operations(id);

CREATE TABLE recovery_quiescence_receipts (
    operation_id uuid PRIMARY KEY REFERENCES recovery_operations(id),
    receipt jsonb NOT NULL CHECK (jsonb_typeof(receipt) = 'object'),
    sealed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION vela_reject_recovery_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '55000',
        CONSTRAINT = 'recovery_receipt_immutable',
        MESSAGE = 'Recovery quiescence receipt is immutable';
END
$$;
CREATE TRIGGER recovery_quiescence_receipts_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON recovery_quiescence_receipts
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_recovery_receipt_mutation();

CREATE FUNCTION vela_require_admission_open() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_open boolean;
BEGIN
    SELECT admission_open INTO STRICT v_open
    FROM public.recovery_admission_control WHERE singleton FOR SHARE;
    IF NOT v_open THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            CONSTRAINT = 'recovery_admission_closed',
            MESSAGE = 'Admission is closed for recovery';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER jobs_recovery_admission_gate
BEFORE INSERT ON jobs FOR EACH ROW EXECUTE FUNCTION vela_require_admission_open();

CREATE FUNCTION vela_recovery_inventory() RETURNS jsonb
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

CREATE FUNCTION vela_close_recovery_admission(p_operation_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_control public.recovery_admission_control%ROWTYPE;
    v_operation public.recovery_operations%ROWTYPE;
BEGIN
    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION 'Recovery operation identity is required' USING ERRCODE = '22023';
    END IF;
    -- No Project, Organization, or Job locks are acquired while holding the
    -- gate. Admission may already hold those locks before its INSERT trigger.
    SELECT * INTO STRICT v_control FROM public.recovery_admission_control
    WHERE singleton FOR UPDATE;
    SELECT * INTO v_operation FROM public.recovery_operations WHERE id = p_operation_id;
    IF v_operation.id IS NOT NULL THEN
        IF v_control.operation_id IS DISTINCT FROM p_operation_id
           OR v_operation.reopened_at IS NOT NULL
           OR v_operation.system_identifier <> (SELECT system_identifier::text FROM pg_control_system()) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'recovery_operation_stale',
                MESSAGE = 'Recovery operation no longer owns this database gate';
        END IF;
        RETURN to_jsonb(v_operation);
    END IF;
    IF NOT v_control.admission_open THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'recovery_operation_conflict',
            MESSAGE = 'Another recovery operation owns the closed Admission gate';
    END IF;
    INSERT INTO public.recovery_operations (
        id, database_identity, generation, system_identifier, database_name, database_oid, actor
    ) SELECT p_operation_id, v_control.database_identity, v_control.generation + 1,
        system_identifier::text, current_database(),
        (SELECT oid FROM pg_database WHERE datname = current_database()), session_user
    FROM pg_control_system() RETURNING * INTO v_operation;
    UPDATE public.recovery_admission_control SET admission_open = false,
        generation = v_operation.generation, operation_id = p_operation_id WHERE singleton;
    RETURN to_jsonb(v_operation);
END
$$;

CREATE FUNCTION vela_seal_recovery_quiescence(p_operation_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_control public.recovery_admission_control%ROWTYPE;
    v_operation public.recovery_operations%ROWTYPE;
    v_inventory jsonb;
    v_receipt jsonb;
BEGIN
    SELECT * INTO STRICT v_control FROM public.recovery_admission_control
    WHERE singleton FOR UPDATE;
    SELECT * INTO v_operation FROM public.recovery_operations WHERE id = p_operation_id;
    IF v_operation.id IS NULL OR v_control.operation_id IS DISTINCT FROM p_operation_id
       OR v_control.admission_open OR v_operation.reopened_at IS NOT NULL
       OR v_operation.system_identifier <> (SELECT system_identifier::text FROM pg_control_system()) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'recovery_operation_stale',
            MESSAGE = 'Recovery operation does not own this source database gate';
    END IF;
    v_inventory := public.vela_recovery_inventory();
    IF EXISTS (SELECT 1 FROM jsonb_each_text(v_inventory) WHERE value::bigint <> 0) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'recovery_authority_not_quiescent',
            MESSAGE = 'Nonterminal execution or storage authority remains';
    END IF;
    SELECT receipt INTO v_receipt FROM public.recovery_quiescence_receipts WHERE operation_id = p_operation_id;
    IF v_receipt IS NOT NULL THEN RETURN v_receipt; END IF;
    v_receipt := to_jsonb(v_operation) || jsonb_build_object(
        'schema', 'vela-db-quiescence-v1', 'production_gate', false,
        'scope', 'DATABASE_ONLY', 'schema_version',
        (SELECT max(version_id) FROM public.goose_db_version WHERE is_applied),
        'inventory', v_inventory, 'sealed_at', clock_timestamp()
    );
    INSERT INTO public.recovery_quiescence_receipts (operation_id, receipt) VALUES (p_operation_id, v_receipt);
    RETURN v_receipt;
END
$$;

CREATE FUNCTION vela_reopen_recovery_admission(p_operation_id uuid, p_generation bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_control public.recovery_admission_control%ROWTYPE;
    v_operation public.recovery_operations%ROWTYPE;
BEGIN
    SELECT * INTO STRICT v_control FROM public.recovery_admission_control WHERE singleton FOR UPDATE;
    SELECT * INTO v_operation FROM public.recovery_operations WHERE id = p_operation_id;
    IF v_operation.id IS NULL OR v_control.operation_id IS DISTINCT FROM p_operation_id
       OR v_control.generation IS DISTINCT FROM p_generation
       OR v_operation.system_identifier <> (SELECT system_identifier::text FROM pg_control_system()) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'recovery_operation_stale',
            MESSAGE = 'Reopening requires the exact operation on its original source database';
    END IF;
    UPDATE public.recovery_operations SET reopened_at = clock_timestamp() WHERE id = p_operation_id;
    UPDATE public.recovery_admission_control SET admission_open = true,
        generation = generation + 1, operation_id = NULL WHERE singleton;
END
$$;

CREATE FUNCTION vela_recovery_status() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT jsonb_build_object('schema','vela-db-recovery-status-v1','production_gate',false,
        'scope','DATABASE_ONLY','gate',to_jsonb(control),'operation',to_jsonb(operation),
        'receipt',receipt.receipt,'inventory',public.vela_recovery_inventory())
    FROM public.recovery_admission_control control
    LEFT JOIN public.recovery_operations operation ON operation.id=control.operation_id
    LEFT JOIN public.recovery_quiescence_receipts receipt ON receipt.operation_id=control.operation_id
    WHERE control.singleton;
$$;

ALTER TABLE recovery_admission_control OWNER TO vela_recovery_owner;
ALTER TABLE recovery_operations OWNER TO vela_recovery_owner;
ALTER TABLE recovery_quiescence_receipts OWNER TO vela_recovery_owner;
ALTER TABLE recovery_admission_control ENABLE ROW LEVEL SECURITY;
ALTER TABLE recovery_admission_control FORCE ROW LEVEL SECURITY;
ALTER TABLE recovery_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE recovery_operations FORCE ROW LEVEL SECURITY;
ALTER TABLE recovery_quiescence_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE recovery_quiescence_receipts FORCE ROW LEVEL SECURITY;
GRANT USAGE ON SCHEMA public TO vela_recovery, vela_recovery_owner;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO vela_recovery_owner;
GRANT SELECT ON jobs, attempts, stage_runs, stage_attempts, stage_leases, stage_allocations,
    stage_materialization_leases, transfer_tickets, stage_graph_finalization_claims,
    stage_artifact_pins, edge_buffer_credits, stage_storage_reservations, goose_db_version
TO vela_recovery_owner;
ALTER FUNCTION vela_reject_recovery_receipt_mutation() OWNER TO vela_recovery_owner;
ALTER FUNCTION vela_require_admission_open() OWNER TO vela_recovery_owner;
ALTER FUNCTION vela_recovery_inventory() OWNER TO vela_recovery_owner;
ALTER FUNCTION vela_close_recovery_admission(uuid) OWNER TO vela_recovery_owner;
ALTER FUNCTION vela_seal_recovery_quiescence(uuid) OWNER TO vela_recovery_owner;
ALTER FUNCTION vela_reopen_recovery_admission(uuid,bigint) OWNER TO vela_recovery_owner;
ALTER FUNCTION vela_recovery_status() OWNER TO vela_recovery_owner;
REVOKE ALL ON FUNCTION vela_reject_recovery_receipt_mutation(), vela_require_admission_open(),
    vela_recovery_inventory(), vela_close_recovery_admission(uuid),
    vela_seal_recovery_quiescence(uuid), vela_reopen_recovery_admission(uuid,bigint), vela_recovery_status() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_recovery_inventory(), vela_close_recovery_admission(uuid),
    vela_seal_recovery_quiescence(uuid), vela_reopen_recovery_admission(uuid,bigint), vela_recovery_status() TO vela_recovery;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM recovery_operations) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'recovery_rollback_unsafe',
            MESSAGE = 'Recovery evidence prevents removing the Admission gate';
    END IF;
END
$$;
DROP TRIGGER jobs_recovery_admission_gate ON jobs;
DROP FUNCTION vela_recovery_status();
DROP FUNCTION vela_reopen_recovery_admission(uuid,bigint);
DROP FUNCTION vela_seal_recovery_quiescence(uuid);
DROP FUNCTION vela_close_recovery_admission(uuid);
DROP FUNCTION vela_recovery_inventory();
DROP FUNCTION vela_require_admission_open();
DROP TABLE recovery_quiescence_receipts;
DROP FUNCTION vela_reject_recovery_receipt_mutation();
DROP TABLE recovery_admission_control;
DROP TABLE recovery_operations;
REVOKE SELECT ON jobs, attempts, stage_runs, stage_attempts, stage_leases, stage_allocations,
    stage_materialization_leases, transfer_tickets, stage_graph_finalization_claims,
    stage_artifact_pins, edge_buffer_credits, stage_storage_reservations, goose_db_version
FROM vela_recovery_owner;
REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM vela_recovery_owner;
-- +goose StatementEnd
