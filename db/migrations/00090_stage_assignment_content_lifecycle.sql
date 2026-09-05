-- +goose Up
-- +goose StatementBegin
-- Fail without waiting if an old writer holds any authority table. Waiting
-- after acquiring a subset could invert a pre-upgrade transaction's lock order.
LOCK TABLE public.jobs, public.non_content_job_roots, public.stage_worker_acquire_intents,
    public.stage_worker_acquire_results, public.stage_leases, public.content_deletion_requests
    IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vela_assignment_history_migration') THEN
        CREATE ROLE vela_assignment_history_migration NOLOGIN;
    END IF;
END
$$;
GRANT UPDATE (id) ON public.non_content_job_roots TO vela_attempt_coordinator_owner;

ALTER TABLE public.stage_worker_acquire_results
    ADD COLUMN assignment_digest bytea CHECK (octet_length(assignment_digest) = 32),
    ADD COLUMN assignment_retired_at timestamptz;
DROP TRIGGER stage_worker_acquire_results_immutable ON public.stage_worker_acquire_results;
-- Schema42's nullable CHECK permitted a missing ASSIGNMENT wire. There are no
-- delivery bytes to recover; record the empty-byte digest and an explicit reason.
UPDATE public.stage_worker_acquire_results
SET assignment_digest = sha256(COALESCE(assignment_wire, ''::bytea)),
    assignment_retired_at = CASE WHEN assignment_wire IS NULL THEN clock_timestamp() END
WHERE result_kind = 'ASSIGNMENT';
ALTER TABLE public.stage_worker_acquire_results DROP CONSTRAINT stage_worker_acquire_results_check;
ALTER TABLE public.stage_worker_acquire_results ADD CONSTRAINT stage_worker_acquire_result_content_shape CHECK (
    (result_kind = 'ASSIGNMENT' AND assignment_digest IS NOT NULL
        AND retry_after_ms IS NULL AND detail IS NULL
        AND ((assignment_wire IS NOT NULL AND octet_length(assignment_wire) BETWEEN 1 AND 4194304
                AND sha256(assignment_wire) = assignment_digest AND assignment_retired_at IS NULL)
            OR (assignment_wire IS NULL AND assignment_retired_at IS NOT NULL)))
    OR (result_kind = 'NO_WORK' AND assignment_wire IS NULL AND assignment_digest IS NULL
        AND assignment_retired_at IS NULL
        AND retry_after_ms BETWEEN 1 AND 3600000 AND detail IS NULL)
    OR (result_kind IN ('STALE', 'REJECTED') AND assignment_wire IS NULL AND assignment_digest IS NULL
        AND assignment_retired_at IS NULL AND retry_after_ms IS NULL AND detail IS NOT NULL)
);

CREATE TABLE public.stage_assignment_authority_receipts (
    command_id uuid PRIMARY KEY REFERENCES public.stage_worker_acquire_results(command_id),
    assignment_digest bytea NOT NULL CHECK (octet_length(assignment_digest) = 32),
    authority_digest bytea NOT NULL CHECK (octet_length(authority_digest) = 32),
    authority_wire bytea NOT NULL CHECK (octet_length(authority_wire) BETWEEN 1 AND 4194304
        AND sha256(authority_wire) = authority_digest),
    job_id uuid NOT NULL REFERENCES public.non_content_job_roots(id),
    stage_lease_id uuid NOT NULL REFERENCES public.stage_leases(id),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX stage_assignment_authority_job ON public.stage_assignment_authority_receipts(job_id, command_id);
CREATE TABLE public.stage_assignment_unverifiable_tombstones (
    command_id uuid PRIMARY KEY REFERENCES public.stage_worker_acquire_results(command_id),
    assignment_digest bytea NOT NULL CHECK (octet_length(assignment_digest) = 32),
    reason text NOT NULL CHECK (reason IN ('MISSING_ASSIGNMENT_WIRE', 'INVALID_ASSIGNMENT_PROTOBUF', 'UNKNOWN_AUTHORITY_FIELDS',
        'UNKNOWN_SIGNING_KEY', 'INVALID_AUTHORITY_SIGNATURE', 'FUTURE_ISSUED_AUTHORITY',
        'INVALID_AUTHORITY', 'AUTHORITY_MAPPING_MISMATCH')),
    retired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- This finite upgrade worklist cannot grow after the legacy writer is fenced.
CREATE TABLE public.stage_assignment_history_pending (
    command_id uuid PRIMARY KEY REFERENCES public.stage_worker_acquire_results(command_id)
);
INSERT INTO public.stage_assignment_history_pending SELECT command_id
FROM public.stage_worker_acquire_results WHERE result_kind = 'ASSIGNMENT' AND assignment_retired_at IS NULL;
INSERT INTO public.stage_assignment_unverifiable_tombstones(command_id, assignment_digest, reason, retired_at)
SELECT command_id, assignment_digest, 'MISSING_ASSIGNMENT_WIRE', assignment_retired_at
FROM public.stage_worker_acquire_results WHERE result_kind = 'ASSIGNMENT' AND assignment_retired_at IS NOT NULL;
ALTER TABLE public.stage_assignment_authority_receipts OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE public.stage_assignment_unverifiable_tombstones OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE public.stage_assignment_history_pending OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE public.stage_assignment_authority_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stage_assignment_authority_receipts FORCE ROW LEVEL SECURITY;
ALTER TABLE public.stage_assignment_unverifiable_tombstones ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stage_assignment_unverifiable_tombstones FORCE ROW LEVEL SECURITY;
ALTER TABLE public.stage_assignment_history_pending ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stage_assignment_history_pending FORCE ROW LEVEL SECURITY;
CREATE TRIGGER stage_assignment_authority_immutable
BEFORE UPDATE OR DELETE ON public.stage_assignment_authority_receipts
FOR EACH ROW EXECUTE FUNCTION public.vela_reject_stage_worker_evidence_mutation();
CREATE TRIGGER stage_assignment_tombstones_immutable
BEFORE UPDATE OR DELETE ON public.stage_assignment_unverifiable_tombstones
FOR EACH ROW EXECUTE FUNCTION public.vela_reject_stage_worker_evidence_mutation();

CREATE FUNCTION public.vela_guard_stage_assignment_content() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.result_kind = 'ASSIGNMENT'
       AND OLD.assignment_retired_at IS NULL AND NEW.assignment_retired_at IS NOT NULL
       AND NEW.assignment_wire IS NULL
       AND (to_jsonb(NEW) - 'assignment_wire' - 'assignment_retired_at') =
           (to_jsonb(OLD) - 'assignment_wire' - 'assignment_retired_at') THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'stage_assignment_content_immutable',
        MESSAGE = 'Stage assignment results permit only irreversible delivery retirement';
END
$$;
CREATE TRIGGER stage_worker_acquire_results_immutable
BEFORE UPDATE OR DELETE ON public.stage_worker_acquire_results
FOR EACH ROW EXECUTE FUNCTION public.vela_guard_stage_assignment_content();

CREATE FUNCTION public.vela_stage_assignment_history_ready() RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT NOT EXISTS (SELECT 1 FROM public.stage_assignment_history_pending)
$$;

CREATE FUNCTION public.vela_retire_job_stage_assignment_content() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.request_content_deleted_at IS NULL THEN RETURN NEW; END IF;
    IF NOT public.vela_stage_assignment_history_ready() THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'stage_assignment_history_backfill_required',
            MESSAGE = 'Stage assignment history backfill must finish before content deletion';
    END IF;
    -- The caller already owns the Job row lock. Completion and replay lock the
    -- same parent before touching delivery rows, including multi-transaction Acquire.
    UPDATE public.stage_worker_acquire_results AS result
    SET assignment_wire = NULL, assignment_retired_at = clock_timestamp()
    FROM public.stage_assignment_authority_receipts AS receipt
    WHERE receipt.job_id = OLD.id AND result.command_id = receipt.command_id
      AND result.assignment_retired_at IS NULL;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER jobs_retire_stage_assignment_content
BEFORE UPDATE OF request_content_deleted_at OR DELETE ON public.jobs
FOR EACH ROW EXECUTE FUNCTION public.vela_retire_job_stage_assignment_content();

CREATE FUNCTION public.vela_guard_assignment_content_deletion_completion() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NEW.state = 'COMPLETED' AND NOT public.vela_stage_assignment_history_ready() THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'stage_assignment_history_backfill_required',
            MESSAGE = 'Stage assignment history backfill must finish before content deletion completion';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER content_deletion_assignment_history_complete
BEFORE INSERT OR UPDATE OF state ON public.content_deletion_requests
FOR EACH ROW EXECUTE FUNCTION public.vela_guard_assignment_content_deletion_completion();

CREATE FUNCTION public.vela_read_stage_assignment_history_backfill(p_limit integer)
RETURNS TABLE (command_id uuid, assignment_wire bytea)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Assignment history batch size is invalid';
    END IF;
    RETURN QUERY SELECT result.command_id, result.assignment_wire
    FROM public.stage_assignment_history_pending AS pending
    JOIN public.stage_worker_acquire_results AS result USING (command_id)
    ORDER BY pending.command_id LIMIT p_limit;
END
$$;

CREATE FUNCTION public.vela_record_stage_assignment_authority(p_evidence jsonb) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_command_id uuid := (p_evidence ->> 'command_id')::uuid;
    v_job_id uuid := (p_evidence ->> 'job_id')::uuid;
    v_lease_id uuid := (p_evidence ->> 'stage_lease_id')::uuid;
    v_assignment_digest bytea := decode(p_evidence ->> 'assignment_digest', 'hex');
    v_authority_digest bytea := decode(p_evidence ->> 'authority_digest', 'hex');
    v_authority_wire bytea := decode(p_evidence ->> 'authority_wire', 'hex');
    v_deleted_at timestamptz;
    v_expires_at timestamptz;
    v_result public.stage_worker_acquire_results%ROWTYPE;
    v_existing public.stage_assignment_authority_receipts%ROWTYPE;
    v_new boolean;
BEGIN
    IF jsonb_typeof(p_evidence) IS DISTINCT FROM 'object'
       OR (p_evidence ->> 'schema_version')::integer IS DISTINCT FROM 1
       OR v_command_id IS NULL OR v_job_id IS NULL OR v_lease_id IS NULL
       OR octet_length(v_assignment_digest) IS DISTINCT FROM 32
       OR octet_length(v_authority_digest) IS DISTINCT FROM 32
       OR COALESCE(octet_length(v_authority_wire), 0) NOT BETWEEN 1 AND 4194304
       OR sha256(v_authority_wire) IS DISTINCT FROM v_authority_digest THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Assignment authority evidence is invalid';
    END IF;
    -- Metadata expiry locks the retained root before the live Job. Acquire its
    -- FK-compatible lock first so receipt insertion cannot reverse that order.
    PERFORM 1 FROM public.non_content_job_roots WHERE id = v_job_id FOR KEY SHARE;
    SELECT job.request_content_deleted_at, job.request_content_expires_at
      INTO v_deleted_at, v_expires_at FROM public.jobs AS job WHERE job.id = v_job_id FOR SHARE;
    PERFORM 1 FROM public.stage_worker_acquire_intents WHERE command_id = v_command_id FOR UPDATE;
    SELECT * INTO v_result FROM public.stage_worker_acquire_results WHERE command_id = v_command_id;
    IF v_result.result_kind IS DISTINCT FROM 'ASSIGNMENT'
       OR v_result.assignment_digest IS DISTINCT FROM v_assignment_digest THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Assignment authority delivery digest does not match';
    END IF;
    IF EXISTS (SELECT 1 FROM public.stage_assignment_unverifiable_tombstones WHERE command_id = v_command_id) THEN
        RETURN false;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.stage_leases AS lease
        JOIN public.stage_runs AS run ON run.id = lease.stage_run_id AND run.attempt_id = lease.attempt_id
        JOIN public.non_content_attempt_roots AS attempt ON attempt.id = run.attempt_id
          AND attempt.organization_id = run.organization_id AND attempt.project_id = run.project_id
        JOIN public.non_content_job_roots AS job ON job.id = attempt.job_id
          AND job.organization_id = attempt.organization_id AND job.project_id = attempt.project_id
        JOIN public.stage_worker_acquire_intents AS intent ON intent.command_id = v_command_id
        JOIN public.stage_attempts AS physical ON physical.id = lease.stage_attempt_id
        WHERE job.id = v_job_id AND lease.id = v_lease_id
          AND lease.attempt_id = (p_evidence ->> 'attempt_id')::uuid
          AND lease.stage_run_id = (p_evidence ->> 'stage_run_id')::uuid
          AND lease.stage_attempt_id = (p_evidence ->> 'stage_attempt_id')::uuid
          AND lease.stage_allocation_id = (p_evidence ->> 'stage_allocation_id')::uuid
          AND lease.worker_instance_id = (p_evidence ->> 'worker_instance_id')::uuid
          AND lease.worker_instance_epoch = (p_evidence ->> 'worker_instance_epoch')::bigint
          AND lease.token_digest = decode(p_evidence ->> 'token_digest', 'hex')
          AND lease.execution_nonce = decode(p_evidence ->> 'execution_nonce', 'hex')
          AND intent.worker_instance_id = lease.worker_instance_id
          AND intent.worker_instance_epoch = lease.worker_instance_epoch
          AND intent.model_residency_id = lease.model_residency_id
          AND intent.model_runtime_epoch = lease.model_runtime_epoch
          AND intent.stage_profile_revision_id = physical.selected_stage_profile_revision_id
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'stage_assignment_authority_mapping_mismatch',
            MESSAGE = 'Assignment authority does not match retained execution identity';
    END IF;
    INSERT INTO public.stage_assignment_authority_receipts (
        command_id, assignment_digest, authority_digest, authority_wire, job_id, stage_lease_id
    ) VALUES (v_command_id, v_assignment_digest, v_authority_digest, v_authority_wire, v_job_id, v_lease_id)
    ON CONFLICT (command_id) DO NOTHING;
    v_new := FOUND;
    SELECT * INTO STRICT v_existing FROM public.stage_assignment_authority_receipts WHERE command_id = v_command_id;
    IF ROW(v_existing.assignment_digest, v_existing.authority_digest, v_existing.authority_wire,
           v_existing.job_id, v_existing.stage_lease_id)
       IS DISTINCT FROM ROW(v_assignment_digest, v_authority_digest, v_authority_wire, v_job_id, v_lease_id) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Assignment authority receipt replay does not match';
    END IF;
    IF v_expires_at IS NULL OR v_deleted_at IS NOT NULL OR v_expires_at <= clock_timestamp() THEN
        UPDATE public.stage_worker_acquire_results SET assignment_wire = NULL, assignment_retired_at = clock_timestamp()
        WHERE command_id = v_command_id AND assignment_retired_at IS NULL;
    END IF;
    DELETE FROM public.stage_assignment_history_pending WHERE command_id = v_command_id;
    RETURN v_new;
END
$$;

CREATE FUNCTION public.vela_retire_unverifiable_stage_assignment(p_request jsonb) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_command_id uuid := (p_request ->> 'command_id')::uuid;
    v_digest bytea := decode(p_request ->> 'assignment_digest', 'hex');
    v_reason text := p_request ->> 'reason';
BEGIN
    IF jsonb_typeof(p_request) IS DISTINCT FROM 'object'
       OR (p_request ->> 'schema_version')::integer IS DISTINCT FROM 1
       OR v_command_id IS NULL OR octet_length(v_digest) IS DISTINCT FROM 32
       OR v_reason IS NULL OR v_reason NOT IN ('INVALID_ASSIGNMENT_PROTOBUF', 'UNKNOWN_AUTHORITY_FIELDS',
            'UNKNOWN_SIGNING_KEY', 'INVALID_AUTHORITY_SIGNATURE', 'FUTURE_ISSUED_AUTHORITY',
            'INVALID_AUTHORITY', 'AUTHORITY_MAPPING_MISMATCH') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Unverifiable assignment retirement is invalid';
    END IF;
    PERFORM 1 FROM public.stage_worker_acquire_intents WHERE command_id = v_command_id FOR UPDATE;
    IF NOT EXISTS (SELECT 1 FROM public.stage_worker_acquire_results WHERE command_id = v_command_id
        AND result_kind = 'ASSIGNMENT' AND assignment_digest = v_digest) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Unverifiable assignment digest does not match';
    END IF;
    IF EXISTS (SELECT 1 FROM public.stage_assignment_unverifiable_tombstones WHERE command_id = v_command_id
        AND assignment_digest = v_digest) THEN RETURN false; END IF;
    IF NOT EXISTS (SELECT 1 FROM public.stage_assignment_history_pending WHERE command_id = v_command_id)
       OR EXISTS (SELECT 1 FROM public.stage_assignment_authority_receipts WHERE command_id = v_command_id) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Only pending legacy assignments may be retired as unverifiable';
    END IF;
    INSERT INTO public.stage_assignment_unverifiable_tombstones(command_id, assignment_digest, reason)
    VALUES (v_command_id, v_digest, v_reason);
    UPDATE public.stage_worker_acquire_results SET assignment_wire = NULL, assignment_retired_at = clock_timestamp()
    WHERE command_id = v_command_id AND assignment_retired_at IS NULL;
    DELETE FROM public.stage_assignment_history_pending WHERE command_id = v_command_id;
    RETURN true;
END
$$;

CREATE FUNCTION public.vela_read_stage_assignment_delivery(p_command_id uuid)
RETURNS TABLE (result_kind text, assignment_wire bytea, retry_after_ms bigint, detail text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_job_id uuid;
    v_deleted_at timestamptz;
    v_expires_at timestamptz;
    v_result public.stage_worker_acquire_results%ROWTYPE;
BEGIN
    SELECT receipt.job_id INTO v_job_id FROM public.stage_assignment_authority_receipts AS receipt
    WHERE receipt.command_id = p_command_id;
    IF v_job_id IS NOT NULL THEN
        SELECT job.request_content_deleted_at, job.request_content_expires_at INTO v_deleted_at, v_expires_at
        FROM public.jobs AS job WHERE job.id = v_job_id FOR SHARE;
        IF v_expires_at IS NULL OR v_deleted_at IS NOT NULL OR v_expires_at <= clock_timestamp() THEN
            UPDATE public.stage_worker_acquire_results AS result
            SET assignment_wire = NULL, assignment_retired_at = clock_timestamp()
            WHERE result.command_id = p_command_id AND result.assignment_retired_at IS NULL;
        END IF;
    END IF;
    SELECT * INTO STRICT v_result FROM public.stage_worker_acquire_results WHERE command_id = p_command_id;
    IF v_result.result_kind = 'ASSIGNMENT' THEN
        IF v_result.assignment_retired_at IS NOT NULL THEN
            RETURN QUERY SELECT 'REJECTED'::text, NULL::bytea, NULL::bigint, 'ASSIGNMENT_CONTENT_DELETED'::text;
            RETURN;
        ELSIF v_job_id IS NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'stage_assignment_history_backfill_required',
                MESSAGE = 'Assignment history must be verified before delivery replay';
        END IF;
    END IF;
    RETURN QUERY SELECT v_result.result_kind::text, v_result.assignment_wire, v_result.retry_after_ms, v_result.detail;
END
$$;

ALTER FUNCTION public.vela_complete_stage_worker_acquire(jsonb) RENAME TO vela_complete_stage_worker_acquire_v89;
REVOKE EXECUTE ON FUNCTION public.vela_complete_stage_worker_acquire_v89(jsonb) FROM vela_stage_worker_control;
CREATE FUNCTION public.vela_complete_stage_worker_acquire(p_result jsonb)
RETURNS TABLE (result_kind text, assignment_wire bytea, retry_after_ms bigint, detail text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_command_id uuid := (p_result ->> 'command_id')::uuid;
    v_wire bytea := decode(p_result ->> 'assignment_wire', 'hex');
    v_evidence jsonb := p_result -> 'authority_evidence';
    v_existing public.stage_worker_acquire_results%ROWTYPE;
BEGIN
    IF p_result ->> 'result_kind' IS DISTINCT FROM 'ASSIGNMENT' THEN
        RETURN QUERY SELECT * FROM public.vela_complete_stage_worker_acquire_v89(p_result);
        RETURN;
    END IF;
    IF (p_result ->> 'schema_version')::integer IS DISTINCT FROM 2
       OR v_command_id IS NULL OR COALESCE(octet_length(v_wire), 0) NOT BETWEEN 1 AND 4194304
       OR jsonb_typeof(v_evidence) IS DISTINCT FROM 'object'
       OR decode(v_evidence ->> 'assignment_digest', 'hex') IS DISTINCT FROM sha256(v_wire)
       OR p_result ->> 'retry_after_ms' IS NOT NULL OR p_result ->> 'detail' IS NOT NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', CONSTRAINT = 'stage_assignment_verified_completion_required',
            MESSAGE = 'Assignment completion requires verified content-free authority evidence';
    END IF;
    PERFORM 1 FROM public.non_content_job_roots WHERE id = (v_evidence ->> 'job_id')::uuid FOR KEY SHARE;
    PERFORM 1 FROM public.jobs WHERE id = (v_evidence ->> 'job_id')::uuid FOR SHARE;
    PERFORM 1 FROM public.stage_worker_acquire_intents WHERE command_id = v_command_id FOR UPDATE;
    INSERT INTO public.stage_worker_acquire_results(command_id, result_kind, assignment_wire, assignment_digest)
    VALUES (v_command_id, 'ASSIGNMENT', v_wire, sha256(v_wire)) ON CONFLICT (command_id) DO NOTHING;
    SELECT * INTO STRICT v_existing FROM public.stage_worker_acquire_results WHERE command_id = v_command_id;
    IF v_existing.result_kind IS DISTINCT FROM 'ASSIGNMENT'
       OR v_existing.assignment_digest IS DISTINCT FROM sha256(v_wire) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'stage_worker_acquire_result_replay_mismatch',
            MESSAGE = 'Stage Worker acquire result replay does not match';
    END IF;
    PERFORM public.vela_record_stage_assignment_authority(v_evidence || jsonb_build_object('command_id', v_command_id));
    RETURN QUERY SELECT * FROM public.vela_read_stage_assignment_delivery(v_command_id);
END
$$;

ALTER FUNCTION public.vela_begin_stage_worker_acquire(jsonb) RENAME TO vela_begin_stage_worker_acquire_v89;
REVOKE EXECUTE ON FUNCTION public.vela_begin_stage_worker_acquire_v89(jsonb) FROM vela_stage_worker_control;
CREATE FUNCTION public.vela_begin_stage_worker_acquire(p_command jsonb)
RETURNS TABLE (requested_at timestamptz, result_kind text, assignment_wire bytea, retry_after_ms bigint, detail text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_begin record;
BEGIN
    SELECT * INTO STRICT v_begin FROM public.vela_begin_stage_worker_acquire_v89(p_command);
    IF v_begin.result_kind = 'ASSIGNMENT' THEN
        RETURN QUERY SELECT v_begin.requested_at, delivery.*
        FROM public.vela_read_stage_assignment_delivery((p_command ->> 'command_id')::uuid) AS delivery;
    ELSE
        RETURN QUERY SELECT v_begin.requested_at, v_begin.result_kind, v_begin.assignment_wire,
            v_begin.retry_after_ms, v_begin.detail;
    END IF;
END
$$;

-- Preserve the exact old function for guarded rollback, and change only its
-- original-evidence lookup. All historical allocation checks remain in place.
ALTER FUNCTION public.vela_read_stage_terminal_history(jsonb) RENAME TO vela_read_stage_terminal_history_v89;
REVOKE EXECUTE ON FUNCTION public.vela_read_stage_terminal_history_v89(jsonb) FROM vela_stage_worker_control;
DO $$
DECLARE
    v_definition text := pg_get_functiondef('public.vela_read_stage_terminal_history_v89(jsonb)'::regprocedure);
BEGIN
    IF strpos(v_definition, 'SELECT result.assignment_wire INTO v_assignment_wire') = 0 THEN
        RAISE EXCEPTION 'Terminal history original lookup changed';
    END IF;
    v_definition := replace(v_definition, 'vela_read_stage_terminal_history_v89(', 'vela_read_stage_terminal_history(');
    v_definition := replace(v_definition, 'SELECT result.assignment_wire INTO v_assignment_wire',
        'SELECT receipt.authority_wire INTO v_assignment_wire');
    v_definition := replace(v_definition, 'WHERE intent.command_id = (p_request',
        E'JOIN public.stage_assignment_authority_receipts AS receipt ON receipt.command_id = result.command_id\n'
        || E'          AND receipt.stage_lease_id = v_lease.id AND receipt.job_id = v_job.id\n'
        || E'          AND receipt.authority_digest = v_authority_digest\n'
        || '        WHERE intent.command_id = (p_request');
    v_definition := replace(v_definition, '''assignment_wire'', encode(v_assignment_wire, ''hex'')',
        '''authority_wire'', encode(v_assignment_wire, ''hex'')');
    -- The request remains schema1; only the result representation changes.
    v_definition := replace(v_definition, 'jsonb_build_object(''schema_version'', 1, ''eligible''',
        'jsonb_build_object(''schema_version'', 2, ''eligible''');
    v_definition := replace(v_definition, '''schema_version'', 1, ''eligible'', true',
        '''schema_version'', 2, ''eligible'', true');
    EXECUTE v_definition;
END
$$;

DO $$
DECLARE v_signature text;
BEGIN
    FOREACH v_signature IN ARRAY ARRAY[
        'vela_guard_stage_assignment_content()', 'vela_stage_assignment_history_ready()',
        'vela_guard_assignment_content_deletion_completion()',
        'vela_retire_job_stage_assignment_content()', 'vela_read_stage_assignment_history_backfill(integer)',
        'vela_record_stage_assignment_authority(jsonb)', 'vela_retire_unverifiable_stage_assignment(jsonb)',
        'vela_read_stage_assignment_delivery(uuid)', 'vela_complete_stage_worker_acquire(jsonb)',
        'vela_begin_stage_worker_acquire(jsonb)', 'vela_read_stage_terminal_history(jsonb)'
    ] LOOP
        EXECUTE 'ALTER FUNCTION public.' || v_signature || ' OWNER TO vela_attempt_coordinator_owner';
        EXECUTE 'REVOKE ALL ON FUNCTION public.' || v_signature || ' FROM PUBLIC';
    END LOOP;
END
$$;
GRANT EXECUTE ON FUNCTION public.vela_stage_assignment_history_ready(),
    public.vela_complete_stage_worker_acquire(jsonb), public.vela_begin_stage_worker_acquire(jsonb),
    public.vela_read_stage_terminal_history(jsonb) TO vela_stage_worker_control;
GRANT EXECUTE ON FUNCTION public.vela_read_stage_assignment_history_backfill(integer),
    public.vela_record_stage_assignment_authority(jsonb), public.vela_retire_unverifiable_stage_assignment(jsonb)
    TO vela_assignment_history_migration;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE public.jobs, public.non_content_job_roots, public.stage_leases, public.content_deletion_requests,
    public.stage_worker_acquire_intents, public.stage_worker_acquire_results,
    public.stage_assignment_authority_receipts, public.stage_assignment_unverifiable_tombstones,
    public.stage_assignment_history_pending IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.stage_assignment_authority_receipts)
       OR EXISTS (SELECT 1 FROM public.stage_assignment_unverifiable_tombstones)
       OR EXISTS (SELECT 1 FROM public.stage_worker_acquire_results WHERE assignment_retired_at IS NOT NULL) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'stage_assignment_content_down_requires_empty_evidence',
            MESSAGE = 'Assignment authority evidence and retired delivery cannot be rolled back';
    END IF;
END
$$;
DROP TRIGGER jobs_retire_stage_assignment_content ON public.jobs;
DROP TRIGGER content_deletion_assignment_history_complete ON public.content_deletion_requests;
DROP FUNCTION public.vela_guard_assignment_content_deletion_completion();
DROP FUNCTION public.vela_retire_job_stage_assignment_content();
DROP FUNCTION public.vela_begin_stage_worker_acquire(jsonb);
ALTER FUNCTION public.vela_begin_stage_worker_acquire_v89(jsonb) RENAME TO vela_begin_stage_worker_acquire;
DROP FUNCTION public.vela_complete_stage_worker_acquire(jsonb);
ALTER FUNCTION public.vela_complete_stage_worker_acquire_v89(jsonb) RENAME TO vela_complete_stage_worker_acquire;
DROP FUNCTION public.vela_read_stage_terminal_history(jsonb);
ALTER FUNCTION public.vela_read_stage_terminal_history_v89(jsonb) RENAME TO vela_read_stage_terminal_history;
GRANT EXECUTE ON FUNCTION public.vela_begin_stage_worker_acquire(jsonb),
    public.vela_complete_stage_worker_acquire(jsonb), public.vela_read_stage_terminal_history(jsonb)
    TO vela_stage_worker_control;
DROP FUNCTION public.vela_read_stage_assignment_delivery(uuid);
DROP FUNCTION public.vela_record_stage_assignment_authority(jsonb);
DROP FUNCTION public.vela_retire_unverifiable_stage_assignment(jsonb);
DROP FUNCTION public.vela_read_stage_assignment_history_backfill(integer);
DROP FUNCTION public.vela_stage_assignment_history_ready();
DROP TABLE public.stage_assignment_history_pending, public.stage_assignment_unverifiable_tombstones,
    public.stage_assignment_authority_receipts;
REVOKE UPDATE (id) ON public.non_content_job_roots FROM vela_attempt_coordinator_owner;
DROP TRIGGER stage_worker_acquire_results_immutable ON public.stage_worker_acquire_results;
DROP FUNCTION public.vela_guard_stage_assignment_content();
ALTER TABLE public.stage_worker_acquire_results DROP CONSTRAINT stage_worker_acquire_result_content_shape,
    DROP COLUMN assignment_digest, DROP COLUMN assignment_retired_at;
ALTER TABLE public.stage_worker_acquire_results ADD CONSTRAINT stage_worker_acquire_results_check CHECK (
    (result_kind = 'ASSIGNMENT' AND octet_length(assignment_wire) BETWEEN 1 AND 4194304
        AND retry_after_ms IS NULL AND detail IS NULL)
    OR (result_kind = 'NO_WORK' AND assignment_wire IS NULL
        AND retry_after_ms BETWEEN 1 AND 3600000 AND detail IS NULL)
    OR (result_kind IN ('STALE', 'REJECTED') AND assignment_wire IS NULL
        AND retry_after_ms IS NULL AND detail IS NOT NULL)
);
CREATE TRIGGER stage_worker_acquire_results_immutable
BEFORE UPDATE OR DELETE ON public.stage_worker_acquire_results
FOR EACH ROW EXECUTE FUNCTION public.vela_reject_stage_worker_evidence_mutation();
-- +goose StatementEnd
