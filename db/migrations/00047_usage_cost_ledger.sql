-- +goose Up
-- +goose StatementBegin
CREATE TYPE usage_attribution AS ENUM ('DIRECT', 'SHARED', 'COUNTERFACTUAL');
CREATE TYPE resource_usage_class AS ENUM (
    'EXECUTION', 'RESIDENCY', 'LOAD_WARMUP', 'RETRY', 'CANCELLATION',
    'TRANSFER', 'STORAGE', 'FINALIZATION', 'DRAIN', 'FAILED_RECONFIGURATION',
    'MINIMUM_WARM_CAPACITY', 'CACHE_AVOIDED_COMPUTE'
);
CREATE TYPE usage_resource_kind AS ENUM (
    'GPU_NANOSECOND', 'CPU_NANOSECOND', 'BYTE_NANOSECOND', 'BYTE', 'OBJECT_OPERATION'
);
CREATE TYPE usage_source_kind AS ENUM (
    'STAGE_ATTEMPT', 'STAGE_CACHE', 'TRANSFER_TICKET', 'STORAGE_RESERVATION',
    'FINALIZATION_CLAIM', 'CAPACITY_POOL', 'MODEL_RESIDENCY', 'RESIDENCY_OPERATION'
);

ALTER TABLE attempts
    ADD CONSTRAINT attempts_job_usage_identity UNIQUE (job_id, id);
ALTER TABLE stage_attempts
    ADD CONSTRAINT stage_attempts_attempt_usage_identity UNIQUE (attempt_id, id);

CREATE TABLE resource_usage_records (
    id uuid PRIMARY KEY,
    schema_version integer NOT NULL CHECK (schema_version = 1),
    source_kind usage_source_kind NOT NULL,
    source_authority_id uuid NOT NULL,
    receipt_digest bytea NOT NULL CHECK (octet_length(receipt_digest) = 32),
    attribution usage_attribution NOT NULL,
    usage_class resource_usage_class NOT NULL,
    resource_kind usage_resource_kind NOT NULL,
    quantity bigint NOT NULL CHECK (quantity > 0),
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    attempt_id uuid,
    stage_attempt_id uuid,
    capacity_pool_id uuid REFERENCES capacity_pools(id),
    interval_start timestamptz NOT NULL,
    interval_end timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_kind, source_authority_id, resource_kind, receipt_digest),
    UNIQUE (id, attribution, usage_class, resource_kind, quantity),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (job_id, attempt_id)
        REFERENCES attempts(job_id, id),
    FOREIGN KEY (attempt_id, stage_attempt_id)
        REFERENCES stage_attempts(attempt_id, id),
    CHECK (interval_end >= interval_start),
    CHECK (recorded_at >= interval_end),
    CHECK (
        (
            attribution = 'SHARED'
            AND organization_id IS NULL
            AND project_id IS NULL
            AND job_id IS NULL
            AND attempt_id IS NULL
            AND stage_attempt_id IS NULL
            AND capacity_pool_id IS NOT NULL
            AND usage_class IN (
                'RESIDENCY', 'LOAD_WARMUP', 'DRAIN',
                'FAILED_RECONFIGURATION', 'MINIMUM_WARM_CAPACITY'
            )
            AND source_kind IN ('CAPACITY_POOL', 'MODEL_RESIDENCY', 'RESIDENCY_OPERATION')
        )
        OR (
            attribution = 'DIRECT'
            AND organization_id IS NOT NULL
            AND project_id IS NOT NULL
            AND job_id IS NOT NULL
            AND attempt_id IS NOT NULL
            AND capacity_pool_id IS NULL
            AND usage_class <> 'CACHE_AVOIDED_COMPUTE'
            AND source_kind IN (
                'STAGE_ATTEMPT', 'TRANSFER_TICKET',
                'STORAGE_RESERVATION', 'FINALIZATION_CLAIM'
            )
        )
        OR (
            attribution = 'COUNTERFACTUAL'
            AND organization_id IS NOT NULL
            AND project_id IS NOT NULL
            AND job_id IS NOT NULL
            AND attempt_id IS NOT NULL
            AND stage_attempt_id IS NULL
            AND capacity_pool_id IS NULL
            AND usage_class = 'CACHE_AVOIDED_COMPUTE'
            AND source_kind = 'STAGE_CACHE'
        )
    ),
    CHECK (
        source_kind <> 'STAGE_ATTEMPT'
        OR source_authority_id = stage_attempt_id
    ),
    CHECK (
        source_kind <> 'CAPACITY_POOL'
        OR source_authority_id = capacity_pool_id
    )
);

CREATE TABLE cost_allocation_records (
    id uuid PRIMARY KEY,
    usage_record_id uuid NOT NULL,
    cost_model_revision_id uuid NOT NULL REFERENCES cost_model_revisions(id),
    supersedes_allocation_id uuid,
    attribution usage_attribution NOT NULL,
    usage_class resource_usage_class NOT NULL,
    resource_kind usage_resource_kind NOT NULL,
    quantity bigint NOT NULL CHECK (quantity > 0),
    rate_numerator_micro_units bigint NOT NULL CHECK (rate_numerator_micro_units > 0),
    rate_denominator_units bigint NOT NULL CHECK (rate_denominator_units > 0),
    cost_micro_units bigint NOT NULL CHECK (cost_micro_units >= 0),
    valued_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (usage_record_id, cost_model_revision_id),
    UNIQUE (supersedes_allocation_id),
    UNIQUE (id, usage_record_id),
    FOREIGN KEY (usage_record_id, attribution, usage_class, resource_kind, quantity)
        REFERENCES resource_usage_records(id, attribution, usage_class, resource_kind, quantity),
    FOREIGN KEY (supersedes_allocation_id, usage_record_id)
        REFERENCES cost_allocation_records(id, usage_record_id),
    CHECK (supersedes_allocation_id IS NULL OR supersedes_allocation_id <> id)
);

CREATE FUNCTION vela_reject_usage_cost_history_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'usage_cost_history_immutable',
        MESSAGE = TG_TABLE_NAME || ' is append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_usage_cost_history_mutation() FROM PUBLIC;

CREATE TRIGGER resource_usage_records_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON resource_usage_records
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_usage_cost_history_mutation();
CREATE TRIGGER cost_allocation_records_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON cost_allocation_records
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_usage_cost_history_mutation();

CREATE FUNCTION vela_record_resource_usage(p_command jsonb)
RETURNS TABLE (usage_record_id uuid, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_id uuid := (p_command ->> 'id')::uuid;
    v_existing public.resource_usage_records%ROWTYPE;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'resource_usage_receipt_invalid',
            MESSAGE = 'Resource Usage receipt must be an object';
    END IF;

    INSERT INTO public.resource_usage_records (
        id, schema_version, source_kind, source_authority_id, receipt_digest,
        attribution, usage_class, resource_kind, quantity,
        organization_id, project_id, job_id, attempt_id, stage_attempt_id,
        capacity_pool_id, interval_start, interval_end, recorded_at
    ) VALUES (
        v_id,
        (p_command ->> 'schema_version')::integer,
        (p_command ->> 'source_kind')::public.usage_source_kind,
        (p_command ->> 'source_authority_id')::uuid,
        decode(p_command ->> 'receipt_digest', 'hex'),
        (p_command ->> 'attribution')::public.usage_attribution,
        (p_command ->> 'usage_class')::public.resource_usage_class,
        (p_command ->> 'resource_kind')::public.usage_resource_kind,
        (p_command ->> 'quantity')::bigint,
        (p_command ->> 'organization_id')::uuid,
        (p_command ->> 'project_id')::uuid,
        (p_command ->> 'job_id')::uuid,
        (p_command ->> 'attempt_id')::uuid,
        (p_command ->> 'stage_attempt_id')::uuid,
        (p_command ->> 'capacity_pool_id')::uuid,
        (p_command ->> 'interval_start')::timestamptz,
        (p_command ->> 'interval_end')::timestamptz,
        (p_command ->> 'recorded_at')::timestamptz
    )
    ON CONFLICT (source_kind, source_authority_id, resource_kind, receipt_digest)
        DO NOTHING
    RETURNING id INTO usage_record_id;

    IF FOUND THEN
        replayed := false;
        RETURN NEXT;
        RETURN;
    END IF;

    SELECT usage.* INTO STRICT v_existing
    FROM public.resource_usage_records AS usage
    WHERE usage.source_kind = (p_command ->> 'source_kind')::public.usage_source_kind
      AND usage.source_authority_id = (p_command ->> 'source_authority_id')::uuid
      AND usage.resource_kind = (p_command ->> 'resource_kind')::public.usage_resource_kind
      AND usage.receipt_digest = decode(p_command ->> 'receipt_digest', 'hex');

    IF v_existing.id IS DISTINCT FROM v_id
       OR v_existing.schema_version IS DISTINCT FROM (p_command ->> 'schema_version')::integer
       OR v_existing.attribution IS DISTINCT FROM
            (p_command ->> 'attribution')::public.usage_attribution
       OR v_existing.usage_class IS DISTINCT FROM
            (p_command ->> 'usage_class')::public.resource_usage_class
       OR v_existing.quantity IS DISTINCT FROM (p_command ->> 'quantity')::bigint
       OR v_existing.organization_id IS DISTINCT FROM (p_command ->> 'organization_id')::uuid
       OR v_existing.project_id IS DISTINCT FROM (p_command ->> 'project_id')::uuid
       OR v_existing.job_id IS DISTINCT FROM (p_command ->> 'job_id')::uuid
       OR v_existing.attempt_id IS DISTINCT FROM (p_command ->> 'attempt_id')::uuid
       OR v_existing.stage_attempt_id IS DISTINCT FROM (p_command ->> 'stage_attempt_id')::uuid
       OR v_existing.capacity_pool_id IS DISTINCT FROM (p_command ->> 'capacity_pool_id')::uuid
       OR v_existing.interval_start IS DISTINCT FROM
            (p_command ->> 'interval_start')::timestamptz
       OR v_existing.interval_end IS DISTINCT FROM (p_command ->> 'interval_end')::timestamptz
       OR v_existing.recorded_at IS DISTINCT FROM (p_command ->> 'recorded_at')::timestamptz
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23505',
            CONSTRAINT = 'resource_usage_record_replay_mismatch',
            MESSAGE = 'Resource Usage replay does not match the immutable receipt';
    END IF;

    usage_record_id := v_existing.id;
    replayed := true;
    RETURN NEXT;
END
$$;

CREATE FUNCTION vela_value_resource_usage(p_command jsonb)
RETURNS TABLE (
    allocation_id uuid,
    usage_record_id uuid,
    cost_model_revision_id uuid,
    supersedes_allocation_id uuid,
    attribution usage_attribution,
    resource_kind usage_resource_kind,
    quantity bigint,
    rate_numerator_micro_units bigint,
    rate_denominator_units bigint,
    cost_micro_units bigint,
    valued_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_allocation_id uuid := (p_command ->> 'allocation_id')::uuid;
    v_usage_id uuid := (p_command ->> 'usage_record_id')::uuid;
    v_model_id uuid := (p_command ->> 'cost_model_revision_id')::uuid;
    v_valued_at timestamptz := (p_command ->> 'valued_at')::timestamptz;
    v_usage public.resource_usage_records%ROWTYPE;
    v_existing public.cost_allocation_records%ROWTYPE;
    v_model public.cost_model_revisions%ROWTYPE;
    v_rate jsonb;
    v_numerator numeric;
    v_denominator numeric;
    v_cost numeric;
    v_previous_id uuid;
    v_previous_valued_at timestamptz;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'cost_allocation_command_invalid',
            MESSAGE = 'Cost allocation command must be an object';
    END IF;

    SELECT usage.* INTO STRICT v_usage
    FROM public.resource_usage_records AS usage
    WHERE usage.id = v_usage_id
    FOR UPDATE;

    SELECT allocation.* INTO v_existing
    FROM public.cost_allocation_records AS allocation
    WHERE allocation.usage_record_id = v_usage.id
      AND allocation.cost_model_revision_id = v_model_id;
    IF FOUND THEN
        IF v_existing.id IS DISTINCT FROM v_allocation_id
           OR v_existing.valued_at IS DISTINCT FROM v_valued_at
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23505',
                CONSTRAINT = 'cost_allocation_record_replay_mismatch',
                MESSAGE = 'Cost allocation replay does not match the immutable valuation';
        END IF;

        allocation_id := v_existing.id;
        usage_record_id := v_existing.usage_record_id;
        cost_model_revision_id := v_existing.cost_model_revision_id;
        supersedes_allocation_id := v_existing.supersedes_allocation_id;
        attribution := v_existing.attribution;
        resource_kind := v_existing.resource_kind;
        quantity := v_existing.quantity;
        rate_numerator_micro_units := v_existing.rate_numerator_micro_units;
        rate_denominator_units := v_existing.rate_denominator_units;
        cost_micro_units := v_existing.cost_micro_units;
        valued_at := v_existing.valued_at;
        replayed := true;
        RETURN NEXT;
        RETURN;
    END IF;

    SELECT model.* INTO STRICT v_model
    FROM public.cost_model_revisions AS model
    WHERE model.id = v_model_id;
    IF v_model.state NOT IN ('ACTIVE', 'RETIRED')
       OR v_valued_at < v_usage.recorded_at
       OR v_valued_at < v_model.effective_at
       OR (v_model.expires_at IS NOT NULL AND v_valued_at >= v_model.expires_at)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'cost_model_revision_not_effective',
            MESSAGE = 'CostModelRevision is not effective for this valuation';
    END IF;

    v_rate := v_model.resource_valuations -> v_usage.resource_kind::text;
    IF jsonb_typeof(v_rate) <> 'object'
       OR jsonb_typeof(v_rate -> 'numerator_micro_units') <> 'number'
       OR jsonb_typeof(v_rate -> 'denominator_units') <> 'number'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'cost_model_resource_rate_missing',
            MESSAGE = 'CostModelRevision does not contain an exact rational rate';
    END IF;
    v_numerator := (v_rate ->> 'numerator_micro_units')::numeric;
    v_denominator := (v_rate ->> 'denominator_units')::numeric;
    IF v_numerator <> trunc(v_numerator) OR v_denominator <> trunc(v_denominator)
       OR v_numerator <= 0 OR v_denominator <= 0
       OR v_numerator > 9223372036854775807 OR v_denominator > 9223372036854775807
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'cost_model_resource_rate_invalid',
            MESSAGE = 'CostModelRevision rational rate must use positive int64 values';
    END IF;

    v_cost := trunc(v_usage.quantity::numeric * v_numerator / v_denominator);
    IF v_cost > 9223372036854775807 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22003',
            CONSTRAINT = 'cost_allocation_overflow',
            MESSAGE = 'Cost allocation exceeds int64 micro-units';
    END IF;

    SELECT allocation.id, allocation.valued_at
    INTO v_previous_id, v_previous_valued_at
    FROM public.cost_allocation_records AS allocation
    WHERE allocation.usage_record_id = v_usage.id
    ORDER BY allocation.valued_at DESC, allocation.id DESC
    LIMIT 1;
    IF v_previous_valued_at IS NOT NULL AND v_valued_at <= v_previous_valued_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'cost_allocation_successor_time_invalid',
            MESSAGE = 'Cost allocation successor must be later than its predecessor';
    END IF;

    INSERT INTO public.cost_allocation_records (
        id, usage_record_id, cost_model_revision_id, supersedes_allocation_id,
        attribution, usage_class, resource_kind, quantity,
        rate_numerator_micro_units, rate_denominator_units, cost_micro_units, valued_at
    ) VALUES (
        v_allocation_id, v_usage.id, v_model.id, v_previous_id,
        v_usage.attribution, v_usage.usage_class, v_usage.resource_kind, v_usage.quantity,
        v_numerator::bigint, v_denominator::bigint, v_cost::bigint, v_valued_at
    )
    RETURNING
        id, cost_allocation_records.usage_record_id,
        cost_allocation_records.cost_model_revision_id,
        cost_allocation_records.supersedes_allocation_id,
        cost_allocation_records.attribution,
        cost_allocation_records.resource_kind,
        cost_allocation_records.quantity,
        cost_allocation_records.rate_numerator_micro_units,
        cost_allocation_records.rate_denominator_units,
        cost_allocation_records.cost_micro_units,
        cost_allocation_records.valued_at
    INTO
        allocation_id, usage_record_id, cost_model_revision_id,
        supersedes_allocation_id, attribution, resource_kind, quantity,
        rate_numerator_micro_units, rate_denominator_units, cost_micro_units, valued_at;
    replayed := false;
    RETURN NEXT;
END
$$;

CREATE FUNCTION vela_summarize_usage_cost(
    p_cost_model_revision_id uuid,
    p_from timestamptz,
    p_to timestamptz
) RETURNS TABLE (
    attribution usage_attribution,
    usage_class resource_usage_class,
    resource_kind usage_resource_kind,
    quantity bigint,
    cost_micro_units bigint,
    usage_record_count bigint,
    unvalued_record_count bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_cost_model_revision_id IS NULL OR p_from IS NULL OR p_to IS NULL
       OR p_to <= p_from OR p_to - p_from > interval '366 days'
       OR NOT EXISTS (
            SELECT 1
            FROM public.cost_model_revisions AS model
            WHERE model.id = p_cost_model_revision_id
              AND model.state IN ('ACTIVE', 'RETIRED')
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'usage_cost_summary_query_invalid',
            MESSAGE = 'Usage Cost summary query is invalid or unbounded';
    END IF;

    RETURN QUERY
    SELECT
        usage.attribution,
        usage.usage_class,
        usage.resource_kind,
        sum(usage.quantity)::bigint,
        coalesce(sum(allocation.cost_micro_units), 0)::bigint,
        count(*)::bigint,
        count(*) FILTER (WHERE allocation.id IS NULL)::bigint
    FROM public.resource_usage_records AS usage
    LEFT JOIN public.cost_allocation_records AS allocation
      ON allocation.usage_record_id = usage.id
     AND allocation.cost_model_revision_id = p_cost_model_revision_id
    WHERE usage.recorded_at >= p_from
      AND usage.recorded_at < p_to
    GROUP BY usage.attribution, usage.usage_class, usage.resource_kind
    ORDER BY usage.attribution, usage.usage_class, usage.resource_kind;
END
$$;

ALTER TABLE resource_usage_records OWNER TO vela_usage_cost_owner;
ALTER TABLE cost_allocation_records OWNER TO vela_usage_cost_owner;
ALTER FUNCTION vela_reject_usage_cost_history_mutation() OWNER TO vela_usage_cost_owner;
ALTER FUNCTION vela_record_resource_usage(jsonb) OWNER TO vela_usage_cost_owner;
ALTER FUNCTION vela_value_resource_usage(jsonb) OWNER TO vela_usage_cost_owner;
ALTER FUNCTION vela_summarize_usage_cost(uuid, timestamptz, timestamptz)
    OWNER TO vela_usage_cost_owner;

ALTER TABLE resource_usage_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE resource_usage_records FORCE ROW LEVEL SECURITY;
ALTER TABLE cost_allocation_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE cost_allocation_records FORCE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE resource_usage_records, cost_allocation_records FROM PUBLIC;
REVOKE ALL ON TABLE resource_usage_records, cost_allocation_records FROM vela_usage_cost;
-- Migration 00043 changed the trigger owner after creation but never restored its
-- default-PUBLIC EXECUTE revocation. Close that inherited privilege before adding
-- another exact runtime role.
REVOKE ALL ON FUNCTION vela_validate_stage_graph_finalization_claim_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_reject_usage_cost_history_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_record_resource_usage(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_value_resource_usage(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_summarize_usage_cost(uuid, timestamptz, timestamptz) FROM PUBLIC;

GRANT SELECT ON cost_model_revisions TO vela_usage_cost_owner;
GRANT EXECUTE ON FUNCTION vela_record_resource_usage(jsonb) TO vela_usage_cost;
GRANT EXECUTE ON FUNCTION vela_value_resource_usage(jsonb) TO vela_usage_cost;
GRANT EXECUTE ON FUNCTION vela_summarize_usage_cost(uuid, timestamptz, timestamptz)
    TO vela_usage_cost;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE resource_usage_records, cost_allocation_records IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM resource_usage_records)
       OR EXISTS (SELECT 1 FROM cost_allocation_records)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'usage_cost_ledger_rollback_is_unsafe',
            MESSAGE = 'Migration 00047 cannot remove immutable Usage/Cost evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_summarize_usage_cost(uuid, timestamptz, timestamptz)
    FROM vela_usage_cost;
REVOKE EXECUTE ON FUNCTION vela_value_resource_usage(jsonb) FROM vela_usage_cost;
REVOKE EXECUTE ON FUNCTION vela_record_resource_usage(jsonb) FROM vela_usage_cost;
REVOKE SELECT ON cost_model_revisions FROM vela_usage_cost_owner;
GRANT EXECUTE ON FUNCTION vela_validate_stage_graph_finalization_claim_mutation() TO PUBLIC;

DROP FUNCTION vela_summarize_usage_cost(uuid, timestamptz, timestamptz);
DROP FUNCTION vela_value_resource_usage(jsonb);
DROP FUNCTION vela_record_resource_usage(jsonb);
DROP TRIGGER cost_allocation_records_immutable ON cost_allocation_records;
DROP TRIGGER resource_usage_records_immutable ON resource_usage_records;
DROP FUNCTION vela_reject_usage_cost_history_mutation();
DROP TABLE cost_allocation_records;
DROP TABLE resource_usage_records;
ALTER TABLE stage_attempts DROP CONSTRAINT stage_attempts_attempt_usage_identity;
ALTER TABLE attempts DROP CONSTRAINT attempts_job_usage_identity;
DROP TYPE usage_source_kind;
DROP TYPE usage_resource_kind;
DROP TYPE resource_usage_class;
DROP TYPE usage_attribution;
-- +goose StatementEnd
