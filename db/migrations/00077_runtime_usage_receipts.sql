-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_record_allocation_usage() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_attempt public.attempts%ROWTYPE;
    v_quantity bigint;
    v_receipt jsonb;
BEGIN
    SELECT attempt.* INTO STRICT v_attempt
    FROM public.attempts AS attempt WHERE attempt.id = NEW.attempt_id;
    v_quantity := (extract(epoch FROM (NEW.released_at - NEW.allocated_at))
        * 1000000000)::bigint;
    IF v_quantity = 0 THEN RETURN NEW; END IF;
    v_receipt := jsonb_build_object(
        'revision', 'allocation-occupancy-v1', 'allocation_id', NEW.id,
        'stage_attempt_id', NEW.stage_attempt_id,
        'allocated_at', NEW.allocated_at, 'released_at', NEW.released_at);
    PERFORM public.vela_record_resource_usage(jsonb_build_object(
        'id', md5('allocation-occupancy-v1:' || NEW.id::text)::uuid,
        'schema_version', 1, 'source_kind', 'STAGE_ATTEMPT',
        'source_authority_id', NEW.stage_attempt_id,
        'receipt_digest', encode(sha256(convert_to(v_receipt::text, 'UTF8')), 'hex'),
        'attribution', 'DIRECT', 'usage_class', 'EXECUTION',
        'resource_kind', 'ALLOCATION_NANOSECOND', 'quantity', v_quantity,
        'organization_id', v_attempt.organization_id, 'project_id', v_attempt.project_id,
        'job_id', v_attempt.job_id, 'attempt_id', v_attempt.id,
        'stage_attempt_id', NEW.stage_attempt_id,
        'interval_start', NEW.allocated_at, 'interval_end', NEW.released_at,
        'recorded_at', greatest(clock_timestamp(), NEW.released_at)));
    RETURN NEW;
END
$$;

CREATE FUNCTION vela_record_cache_reuse_usage() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_attempt public.attempts%ROWTYPE;
    v_source public.stage_allocations%ROWTYPE;
    v_receipt jsonb;
    v_quantity bigint;
BEGIN
    -- References without a StageRun are pins, not evidence of avoided execution.
    IF NEW.owner_stage_run_id IS NULL THEN RETURN NEW; END IF;
    SELECT attempt.* INTO STRICT v_attempt
    FROM public.stage_runs AS run JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
    WHERE run.id = NEW.owner_stage_run_id AND attempt.job_id = NEW.owner_job_id;
    SELECT allocation.* INTO STRICT v_source
    FROM public.stage_artifacts AS artifact
    JOIN public.stage_allocations AS allocation
      ON allocation.stage_attempt_id = artifact.producer_stage_attempt_id
    WHERE artifact.id = NEW.stage_artifact_id AND allocation.state = 'RELEASED';
    v_quantity := (extract(epoch FROM (v_source.released_at - v_source.allocated_at))
        * 1000000000)::bigint;
    IF v_quantity = 0 THEN RETURN NEW; END IF;
    v_receipt := jsonb_build_object(
        'revision', 'cache-source-allocation-estimate-v1', 'reference_id', NEW.id,
        'source_allocation_id', v_source.id, 'quantity', v_quantity,
        'acquired_at', NEW.acquired_at);
    PERFORM public.vela_record_resource_usage(jsonb_build_object(
        'id', md5('cache-source-allocation-estimate-v1:' || NEW.id::text)::uuid,
        'schema_version', 1, 'source_kind', 'STAGE_CACHE', 'source_authority_id', NEW.id,
        'receipt_digest', encode(sha256(convert_to(v_receipt::text, 'UTF8')), 'hex'),
        'attribution', 'COUNTERFACTUAL', 'usage_class', 'CACHE_AVOIDED_COMPUTE',
        'resource_kind', 'ALLOCATION_NANOSECOND', 'quantity', v_quantity,
        'organization_id', v_attempt.organization_id, 'project_id', v_attempt.project_id,
        'job_id', v_attempt.job_id, 'attempt_id', v_attempt.id,
        'interval_start', NEW.acquired_at, 'interval_end', NEW.acquired_at,
        'recorded_at', greatest(clock_timestamp(), NEW.acquired_at)));
    RETURN NEW;
END
$$;

CREATE FUNCTION vela_record_transfer_payload_usage() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_attempt public.attempts%ROWTYPE;
    v_receipt jsonb;
BEGIN
    SELECT attempt.* INTO STRICT v_attempt
    FROM public.stage_artifact_pins AS pin
    JOIN public.stage_runs AS run ON run.id = pin.owner_stage_run_id
    JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
      AND attempt.job_id = pin.owner_job_id
    WHERE pin.id = NEW.stage_artifact_pin_id;
    v_receipt := jsonb_build_object(
        'revision', 'completed-transfer-payload-v1', 'ticket_id', NEW.id,
        'outcome_digest', encode(NEW.outcome_digest, 'hex'),
        'size_bytes', NEW.size_bytes, 'consumed_at', NEW.consumed_at);
    PERFORM public.vela_record_resource_usage(jsonb_build_object(
        'id', md5('completed-transfer-payload-v1:' || NEW.id::text)::uuid,
        'schema_version', 1, 'source_kind', 'TRANSFER_TICKET', 'source_authority_id', NEW.id,
        'receipt_digest', encode(sha256(convert_to(v_receipt::text, 'UTF8')), 'hex'),
        'attribution', 'DIRECT', 'usage_class', 'TRANSFER',
        'resource_kind', 'BYTE', 'quantity', NEW.size_bytes,
        'organization_id', v_attempt.organization_id, 'project_id', v_attempt.project_id,
        'job_id', v_attempt.job_id, 'attempt_id', v_attempt.id,
        'interval_start', NEW.issued_at, 'interval_end', NEW.consumed_at,
        'recorded_at', greatest(clock_timestamp(), NEW.consumed_at)));
    RETURN NEW;
END
$$;

GRANT SELECT ON attempts, stage_runs, stage_allocations, stage_artifacts, stage_artifact_pins
    TO vela_usage_cost_owner;
ALTER FUNCTION vela_record_allocation_usage() OWNER TO vela_usage_cost_owner;
ALTER FUNCTION vela_record_cache_reuse_usage() OWNER TO vela_usage_cost_owner;
ALTER FUNCTION vela_record_transfer_payload_usage() OWNER TO vela_usage_cost_owner;
REVOKE ALL ON FUNCTION vela_record_allocation_usage(), vela_record_cache_reuse_usage(),
    vela_record_transfer_payload_usage() FROM PUBLIC;

CREATE TRIGGER stage_allocations_record_usage
AFTER UPDATE OF state ON stage_allocations
FOR EACH ROW WHEN (OLD.state = 'ALLOCATED' AND NEW.state = 'RELEASED')
EXECUTE FUNCTION vela_record_allocation_usage();

CREATE TRIGGER stage_cache_references_record_usage
AFTER INSERT ON stage_cache_references
FOR EACH ROW EXECUTE FUNCTION vela_record_cache_reuse_usage();

CREATE TRIGGER transfer_tickets_record_usage
AFTER UPDATE OF state ON transfer_tickets
FOR EACH ROW WHEN (OLD.state = 'ACTIVE' AND NEW.state = 'CONSUMED')
EXECUTE FUNCTION vela_record_transfer_payload_usage();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER transfer_tickets_record_usage ON transfer_tickets;
DROP TRIGGER stage_cache_references_record_usage ON stage_cache_references;
DROP TRIGGER stage_allocations_record_usage ON stage_allocations;
DROP FUNCTION vela_record_cache_reuse_usage();
DROP FUNCTION vela_record_allocation_usage();
DROP FUNCTION vela_record_transfer_payload_usage();
REVOKE SELECT ON attempts, stage_runs, stage_allocations, stage_artifacts, stage_artifact_pins
    FROM vela_usage_cost_owner;
-- +goose StatementEnd
