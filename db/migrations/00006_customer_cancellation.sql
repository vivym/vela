-- +goose Up
-- +goose StatementBegin
CREATE TYPE charge_reason AS ENUM ('VISIBLE_COMPLETION', 'CUSTOMER_CANCELLATION');
CREATE TYPE cancellation_decision AS ENUM (
    'CANCELED', 'CANCELING', 'ALREADY_SUCCEEDED', 'ALREADY_FAILED'
);
CREATE TYPE cancellation_stop_source AS ENUM ('ACKNOWLEDGED', 'LEASE_EXPIRED_RECONCILIATION');

CREATE TABLE job_cancellation_decisions (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL UNIQUE,
    requested_by_principal_id uuid NOT NULL,
    previous_job_state job_state NOT NULL,
    decision cancellation_decision NOT NULL,
    billable boolean NOT NULL,
    attempt_id uuid,
    worker_id uuid,
    worker_epoch bigint,
    attempt_fence bigint,
    authority_lease_id uuid,
    authority_lease_phase lease_phase,
    authority_lease_owner_kind lease_owner_kind,
    authority_lease_owner_id text,
    authority_lease_expires_at timestamptz,
    cancellation_fence bigint NOT NULL CHECK (cancellation_fence >= 0),
    job_version bigint NOT NULL CHECK (job_version > 0),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, job_id, id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, requested_by_principal_id)
        REFERENCES service_principals(organization_id, project_id, principal_id),
    FOREIGN KEY (
        organization_id, project_id, attempt_id, job_id,
        worker_id, worker_epoch, attempt_fence
    ) REFERENCES attempts (
        organization_id, project_id, id, job_id,
        worker_id, worker_epoch, fence
    ),
    FOREIGN KEY (organization_id, project_id, authority_lease_id)
        REFERENCES attempt_leases(organization_id, project_id, id),
    CHECK (
        (
            previous_job_state IN ('QUEUED', 'RETRY_WAIT')
            AND decision = 'CANCELED'
            AND NOT billable
            AND attempt_id IS NULL
            AND worker_id IS NULL
            AND worker_epoch IS NULL
            AND attempt_fence IS NULL
            AND authority_lease_id IS NULL
            AND authority_lease_phase IS NULL
            AND authority_lease_owner_kind IS NULL
            AND authority_lease_owner_id IS NULL
            AND authority_lease_expires_at IS NULL
        )
        OR
        (
            previous_job_state = 'ASSIGNED'
            AND decision = 'CANCELED'
            AND NOT billable
            AND attempt_id IS NOT NULL
            AND worker_id IS NOT NULL
            AND worker_epoch > 0
            AND attempt_fence > 0
            AND authority_lease_id IS NOT NULL
            AND authority_lease_phase = 'EXECUTION'
            AND authority_lease_owner_kind = 'WORKER'
            AND length(authority_lease_owner_id) BETWEEN 1 AND 500
            AND authority_lease_expires_at IS NOT NULL
        )
        OR
        (
            previous_job_state IN ('RUNNING', 'FINALIZING')
            AND decision = 'CANCELING'
            AND billable
            AND attempt_id IS NOT NULL
            AND worker_id IS NOT NULL
            AND worker_epoch > 0
            AND attempt_fence > 0
            AND authority_lease_id IS NOT NULL
            AND length(authority_lease_owner_id) BETWEEN 1 AND 500
            AND authority_lease_expires_at IS NOT NULL
            AND (
                (
                    previous_job_state = 'RUNNING'
                    AND authority_lease_phase = 'EXECUTION'
                    AND authority_lease_owner_kind = 'WORKER'
                )
                OR (
                    previous_job_state = 'FINALIZING'
                    AND authority_lease_phase = 'FINALIZATION'
                    AND authority_lease_owner_kind IN ('WORKER', 'RECONCILER')
                )
            )
        )
    )
);

CREATE TABLE charges (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL UNIQUE,
    credit_reservation_id uuid NOT NULL UNIQUE,
    cancellation_id uuid UNIQUE,
    reason charge_reason NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    state text NOT NULL DEFAULT 'POSTED' CHECK (state = 'POSTED'),
    posted_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id, amount_minor, currency)
        REFERENCES jobs (
            organization_id, project_id, id,
            pricing_quoted_amount_minor, pricing_currency
        ),
    FOREIGN KEY (organization_id, project_id, credit_reservation_id)
        REFERENCES credit_reservations(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id, cancellation_id)
        REFERENCES job_cancellation_decisions(organization_id, project_id, job_id, id),
    CHECK (
        (reason = 'CUSTOMER_CANCELLATION' AND cancellation_id IS NOT NULL)
        OR (reason = 'VISIBLE_COMPLETION' AND cancellation_id IS NULL)
    )
);

CREATE TABLE cancellation_stop_receipts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    cancellation_id uuid NOT NULL UNIQUE,
    attempt_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    cancellation_fence bigint NOT NULL CHECK (cancellation_fence > attempt_fence),
    source cancellation_stop_source NOT NULL,
    terminal_job_version bigint NOT NULL CHECK (terminal_job_version > 0),
    stopped_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id, cancellation_id)
        REFERENCES job_cancellation_decisions(organization_id, project_id, job_id, id),
    FOREIGN KEY (
        organization_id, project_id, attempt_id, job_id,
        worker_id, worker_epoch, attempt_fence
    ) REFERENCES attempts (
        organization_id, project_id, id, job_id,
        worker_id, worker_epoch, fence
    )
);

DO $$
BEGIN
    IF to_regclass('vela_private.job_cancellation_decisions_rollback') IS NOT NULL THEN
        INSERT INTO public.job_cancellation_decisions
        SELECT (
            jsonb_populate_record(
                NULL::public.job_cancellation_decisions,
                snapshot.decision
            )
        ).*
        FROM vela_private.job_cancellation_decisions_rollback AS snapshot;

        DROP TABLE vela_private.job_cancellation_decisions_rollback;
    END IF;
END
$$;

DO $$
BEGIN
    IF to_regclass('vela_private.charges_rollback') IS NOT NULL THEN
        INSERT INTO public.charges
        SELECT (
            jsonb_populate_record(
                NULL::public.charges,
                snapshot.charge
            )
        ).*
        FROM vela_private.charges_rollback AS snapshot;

        DROP TABLE vela_private.charges_rollback;
    END IF;
END
$$;

DO $$
BEGIN
    IF to_regclass('vela_private.cancellation_stop_receipts_rollback') IS NOT NULL THEN
        INSERT INTO public.cancellation_stop_receipts
        SELECT (
            jsonb_populate_record(
                NULL::public.cancellation_stop_receipts,
                snapshot.receipt
            )
        ).*
        FROM vela_private.cancellation_stop_receipts_rollback AS snapshot;

        DROP TABLE vela_private.cancellation_stop_receipts_rollback;
    END IF;
END
$$;

CREATE FUNCTION vela_reject_cancellation_history_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'cancellation decisions and Charges are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_cancellation_history_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_cancellation_history_mutation() OWNER TO vela_internal;

CREATE TRIGGER job_cancellation_decisions_immutable
BEFORE UPDATE OR DELETE ON job_cancellation_decisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_cancellation_history_mutation();
CREATE TRIGGER charges_immutable
BEFORE UPDATE OR DELETE ON charges
FOR EACH ROW EXECUTE FUNCTION vela_reject_cancellation_history_mutation();
CREATE TRIGGER cancellation_stop_receipts_immutable
BEFORE UPDATE OR DELETE ON cancellation_stop_receipts
FOR EACH ROW EXECUTE FUNCTION vela_reject_cancellation_history_mutation();

ALTER TABLE job_cancellation_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_cancellation_decisions FORCE ROW LEVEL SECURITY;
ALTER TABLE charges ENABLE ROW LEVEL SECURITY;
ALTER TABLE charges FORCE ROW LEVEL SECURITY;
ALTER TABLE cancellation_stop_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE cancellation_stop_receipts FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_set_cancellation_request_context(
    p_credential_id uuid,
    p_credential_proof bytea
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_scopes text[];
    v_expires_at timestamptz;
    v_revoked_at timestamptz;
BEGIN
    IF octet_length(p_credential_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    SELECT credential.scopes, credential.expires_at, credential.revoked_at
    INTO v_scopes, v_expires_at, v_revoked_at
    FROM public.credentials AS credential
    WHERE credential.id = p_credential_id
      AND pg_catalog.sha256(credential.secret_digest) = pg_catalog.sha256(p_credential_proof)
    FOR SHARE;

    IF NOT FOUND OR v_revoked_at IS NOT NULL OR v_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;
    IF NOT coalesce('jobs:cancel' = ANY(v_scopes), false) THEN
        RAISE EXCEPTION 'request credential lacks jobs:cancel scope' USING ERRCODE = '42501';
    END IF;

    RETURN QUERY
    SELECT context.organization_id, context.project_id, context.principal_id, context.transaction_time
    FROM public.vela_set_request_context(
        p_credential_id,
        p_credential_proof,
        'jobs:cancel'
    ) AS context;
END
$$;
REVOKE ALL ON FUNCTION vela_set_cancellation_request_context(uuid, bytea) FROM PUBLIC;
ALTER FUNCTION vela_set_cancellation_request_context(uuid, bytea) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_proto_varint(p_value bigint) RETURNS bytea
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
DECLARE
    v_value bigint := p_value;
    v_byte integer;
    v_result bytea := decode('', 'hex');
BEGIN
    IF v_value < 0 THEN
        RAISE EXCEPTION 'protobuf unsigned integer cannot be negative';
    END IF;
    LOOP
        v_byte := (v_value & 127)::integer;
        v_value := v_value >> 7;
        IF v_value > 0 THEN
            v_byte := v_byte | 128;
        END IF;
        v_result := v_result || decode(lpad(to_hex(v_byte), 2, '0'), 'hex');
        EXIT WHEN v_value = 0;
    END LOOP;
    RETURN v_result;
END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_proto_varint(bigint) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_proto_varint(bigint) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_proto_bytes(p_field integer, p_value bytea) RETURNS bytea
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT vela_private.vela_proto_varint(((p_field::bigint << 3) | 2::bigint))
        || vela_private.vela_proto_varint(octet_length(p_value)::bigint)
        || p_value
$$;
REVOKE ALL ON FUNCTION vela_private.vela_proto_bytes(integer, bytea) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_proto_bytes(integer, bytea) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_proto_string(p_field integer, p_value text) RETURNS bytea
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE
        WHEN p_value = '' THEN decode('', 'hex')
        ELSE vela_private.vela_proto_bytes(p_field, convert_to(p_value, 'UTF8'))
    END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_proto_string(integer, text) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_proto_string(integer, text) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_proto_uint(p_field integer, p_value bigint) RETURNS bytea
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE
        WHEN p_value = 0 THEN decode('', 'hex')
        ELSE vela_private.vela_proto_varint((p_field::bigint << 3))
            || vela_private.vela_proto_varint(p_value)
    END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_proto_uint(integer, bigint) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_proto_uint(integer, bigint) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_proto_timestamp(p_value timestamptz) RETURNS bytea
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
DECLARE
    v_epoch numeric := extract(epoch FROM p_value);
    v_seconds bigint;
    v_nanos bigint;
BEGIN
    v_seconds := floor(v_epoch)::bigint;
    v_nanos := round((v_epoch - v_seconds) * 1000000000)::bigint;
    RETURN vela_private.vela_proto_uint(1, v_seconds)
        || vela_private.vela_proto_uint(2, v_nanos);
END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_proto_timestamp(timestamptz) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_proto_timestamp(timestamptz) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_cancellation_event_envelope(
    p_event_id uuid,
    p_job_id uuid,
    p_job_version bigint,
    p_event_type text,
    p_occurred_at timestamptz,
    p_payload_field integer,
    p_payload bytea
) RETURNS bytea
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT vela_private.vela_proto_string(1, p_event_id::text)
        || vela_private.vela_proto_string(2, 'Job')
        || vela_private.vela_proto_string(3, p_job_id::text)
        || vela_private.vela_proto_uint(4, p_job_version)
        || vela_private.vela_proto_string(5, p_event_type)
        || vela_private.vela_proto_uint(6, 1)
        || vela_private.vela_proto_bytes(
            7,
            vela_private.vela_proto_timestamp(p_occurred_at)
        )
        || vela_private.vela_proto_bytes(p_payload_field, p_payload)
$$;
REVOKE ALL ON FUNCTION vela_private.vela_cancellation_event_envelope(
    uuid, uuid, bigint, text, timestamptz, integer, bytea
) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_cancellation_event_envelope(
    uuid, uuid, bigint, text, timestamptz, integer, bytea
) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_insert_canonical_cancellation_event(
    p_event_id uuid,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_job_version bigint,
    p_event_type text,
    p_occurred_at timestamptz,
    p_payload_field integer,
    p_payload bytea
) RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, public
AS $$
    INSERT INTO public.outbox_events (
        event_id,
        organization_id,
        project_id,
        aggregate_type,
        aggregate_id,
        aggregate_version,
        event_type,
        schema_version,
        payload,
        occurred_at
    ) VALUES (
        p_event_id,
        p_organization_id,
        p_project_id,
        'Job',
        p_job_id,
        p_job_version,
        p_event_type,
        1,
        vela_private.vela_cancellation_event_envelope(
            p_event_id,
            p_job_id,
            p_job_version,
            p_event_type,
            p_occurred_at,
            p_payload_field,
            p_payload
        ),
        p_occurred_at
    )
$$;
REVOKE ALL ON FUNCTION vela_private.vela_insert_canonical_cancellation_event(
    uuid, uuid, uuid, uuid, bigint, text, timestamptz, integer, bytea
) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_insert_canonical_cancellation_event(
    uuid, uuid, uuid, uuid, bigint, text, timestamptz, integer, bytea
) OWNER TO vela_internal;

CREATE FUNCTION vela_private.vela_insert_cancellation_outbox_events(
    p_cancellation_id uuid,
    p_cancel_requested_event_id uuid,
    p_canceling_event_id uuid,
    p_canceled_event_id uuid,
    p_charge_posted_event_id uuid,
    p_invoice_export_event_id uuid
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_decision public.job_cancellation_decisions%ROWTYPE;
    v_charge public.charges%ROWTYPE;
    v_payload bytea;
BEGIN
    SELECT decision.* INTO STRICT v_decision
    FROM public.job_cancellation_decisions AS decision
    WHERE decision.id = p_cancellation_id;

    IF v_decision.attempt_id IS NOT NULL THEN
        v_payload :=
            vela_private.vela_proto_string(1, v_decision.organization_id::text)
            || vela_private.vela_proto_string(2, v_decision.project_id::text)
            || vela_private.vela_proto_string(3, v_decision.job_id::text)
            || vela_private.vela_proto_string(4, v_decision.id::text)
            || vela_private.vela_proto_string(5, v_decision.attempt_id::text)
            || vela_private.vela_proto_string(6, v_decision.worker_id::text)
            || vela_private.vela_proto_uint(7, v_decision.worker_epoch)
            || vela_private.vela_proto_uint(8, v_decision.attempt_fence)
            || vela_private.vela_proto_uint(9, v_decision.cancellation_fence)
            || vela_private.vela_proto_bytes(
                10,
                vela_private.vela_proto_timestamp(v_decision.decided_at)
            )
            || vela_private.vela_proto_string(11, v_decision.authority_lease_id::text)
            || vela_private.vela_proto_string(12, v_decision.authority_lease_phase::text)
            || vela_private.vela_proto_string(13, v_decision.authority_lease_owner_kind::text)
            || vela_private.vela_proto_string(14, v_decision.authority_lease_owner_id)
            || vela_private.vela_proto_bytes(
                15,
                vela_private.vela_proto_timestamp(v_decision.authority_lease_expires_at)
            );
        PERFORM vela_private.vela_insert_canonical_cancellation_event(
            p_cancel_requested_event_id,
            v_decision.organization_id,
            v_decision.project_id,
            v_decision.job_id,
            v_decision.job_version,
            'job.cancel_requested',
            v_decision.decided_at,
            26,
            v_payload
        );
    END IF;

    IF v_decision.billable THEN
        SELECT charge.* INTO STRICT v_charge
        FROM public.charges AS charge
        WHERE charge.cancellation_id = v_decision.id;

        v_payload :=
            vela_private.vela_proto_string(1, v_decision.organization_id::text)
            || vela_private.vela_proto_string(2, v_decision.project_id::text)
            || vela_private.vela_proto_string(3, v_decision.job_id::text)
            || vela_private.vela_proto_string(4, v_decision.id::text)
            || vela_private.vela_proto_uint(5, v_decision.cancellation_fence)
            || vela_private.vela_proto_string(6, v_charge.id::text)
            || vela_private.vela_proto_bytes(
                7,
                vela_private.vela_proto_timestamp(v_decision.decided_at)
            );
        PERFORM vela_private.vela_insert_canonical_cancellation_event(
            p_canceling_event_id,
            v_decision.organization_id,
            v_decision.project_id,
            v_decision.job_id,
            v_decision.job_version,
            'job.canceling',
            v_decision.decided_at,
            27,
            v_payload
        );

        v_payload :=
            vela_private.vela_proto_string(1, v_decision.organization_id::text)
            || vela_private.vela_proto_string(2, v_decision.project_id::text)
            || vela_private.vela_proto_string(3, v_decision.job_id::text)
            || vela_private.vela_proto_string(4, v_charge.id::text)
            || vela_private.vela_proto_string(5, v_decision.id::text)
            || vela_private.vela_proto_uint(6, v_charge.amount_minor)
            || vela_private.vela_proto_string(7, v_charge.currency)
            || vela_private.vela_proto_string(8, v_charge.reason::text)
            || vela_private.vela_proto_bytes(
                9,
                vela_private.vela_proto_timestamp(v_charge.posted_at)
            );
        PERFORM vela_private.vela_insert_canonical_cancellation_event(
            p_charge_posted_event_id,
            v_decision.organization_id,
            v_decision.project_id,
            v_decision.job_id,
            v_decision.job_version,
            'charge.posted',
            v_charge.posted_at,
            28,
            v_payload
        );

        v_payload :=
            vela_private.vela_proto_string(1, v_decision.organization_id::text)
            || vela_private.vela_proto_string(2, v_decision.project_id::text)
            || vela_private.vela_proto_string(3, v_decision.job_id::text)
            || vela_private.vela_proto_string(4, v_charge.id::text)
            || vela_private.vela_proto_bytes(
                5,
                vela_private.vela_proto_timestamp(v_decision.decided_at)
            );
        PERFORM vela_private.vela_insert_canonical_cancellation_event(
            p_invoice_export_event_id,
            v_decision.organization_id,
            v_decision.project_id,
            v_decision.job_id,
            v_decision.job_version,
            'invoice.export_requested',
            v_decision.decided_at,
            29,
            v_payload
        );
    ELSE
        v_payload :=
            vela_private.vela_proto_string(1, v_decision.organization_id::text)
            || vela_private.vela_proto_string(2, v_decision.project_id::text)
            || vela_private.vela_proto_string(3, v_decision.job_id::text)
            || vela_private.vela_proto_string(4, v_decision.id::text)
            || vela_private.vela_proto_uint(5, v_decision.cancellation_fence)
            || vela_private.vela_proto_bytes(
                8,
                vela_private.vela_proto_timestamp(v_decision.decided_at)
            );
        PERFORM vela_private.vela_insert_canonical_cancellation_event(
            p_canceled_event_id,
            v_decision.organization_id,
            v_decision.project_id,
            v_decision.job_id,
            v_decision.job_version,
            'job.canceled',
            v_decision.decided_at,
            25,
            v_payload
        );
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_insert_cancellation_outbox_events(
    uuid, uuid, uuid, uuid, uuid, uuid
) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_insert_cancellation_outbox_events(
    uuid, uuid, uuid, uuid, uuid, uuid
) OWNER TO vela_internal;

CREATE FUNCTION vela_cancel_job(
    p_job_id uuid,
    p_cancellation_id uuid,
    p_charge_id uuid,
    p_cancel_requested_event_id uuid,
    p_canceling_event_id uuid,
    p_canceled_event_id uuid,
    p_charge_posted_event_id uuid,
    p_invoice_export_event_id uuid
) RETURNS TABLE (
    cancellation_id uuid,
    job_id uuid,
    decision cancellation_decision,
    job_state job_state,
    previous_job_state job_state,
    job_version bigint,
    cancellation_fence bigint,
    attempt_id uuid,
    worker_id uuid,
    worker_epoch bigint,
    attempt_fence bigint,
    authority_lease_id uuid,
    authority_lease_phase lease_phase,
    authority_lease_owner_kind lease_owner_kind,
    authority_lease_owner_id text,
    authority_lease_expires_at timestamptz,
    billable boolean,
    charge_id uuid,
    charge_amount_minor bigint,
    charge_currency text,
    charge_reason charge_reason,
    charge_posted_at timestamptz,
    decided_at timestamptz,
    created boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_project_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_scope text := public.vela_current_request_scope();
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_lease public.attempt_leases%ROWTYPE;
    v_worker public.workers%ROWTYPE;
    v_decision public.job_cancellation_decisions%ROWTYPE;
    v_reservation public.credit_reservations%ROWTYPE;
    v_candidate_attempt_id uuid;
    v_candidate_worker_id uuid;
    v_billable boolean;
    v_next_decision public.cancellation_decision;
    v_next_job_state public.job_state;
    v_rows bigint;
    v_decided_at timestamptz := transaction_timestamp();
BEGIN
    IF v_scope IS DISTINCT FROM 'jobs:cancel'
        OR v_organization_id IS NULL
        OR v_project_id IS NULL
        OR v_principal_id IS NULL
    THEN
        RAISE EXCEPTION 'valid jobs:cancel request context is required'
            USING ERRCODE = '28000';
    END IF;
    IF p_job_id IS NULL OR p_cancellation_id IS NULL OR p_charge_id IS NULL
        OR p_cancel_requested_event_id IS NULL OR p_canceling_event_id IS NULL
        OR p_canceled_event_id IS NULL OR p_charge_posted_event_id IS NULL
        OR p_invoice_export_event_id IS NULL
    THEN
        RAISE EXCEPTION 'cancellation identity is required' USING ERRCODE = '22023';
    END IF;

    SELECT existing.* INTO v_decision
    FROM public.job_cancellation_decisions AS existing
    WHERE existing.job_id = p_job_id
      AND existing.organization_id = v_organization_id
      AND existing.project_id = v_project_id;
    IF FOUND THEN
        RETURN QUERY
        SELECT
            v_decision.id,
            v_decision.job_id,
            v_decision.decision,
            current_job.state,
            v_decision.previous_job_state,
            current_job.version,
            v_decision.cancellation_fence,
            v_decision.attempt_id,
            v_decision.worker_id,
            v_decision.worker_epoch,
            v_decision.attempt_fence,
            v_decision.authority_lease_id,
            v_decision.authority_lease_phase,
            v_decision.authority_lease_owner_kind,
            v_decision.authority_lease_owner_id,
            v_decision.authority_lease_expires_at,
            v_decision.billable,
            charge.id,
            charge.amount_minor,
            charge.currency,
            charge.reason,
            charge.posted_at,
            v_decision.decided_at,
            false
        FROM public.jobs AS current_job
        LEFT JOIN public.charges AS charge ON charge.cancellation_id = v_decision.id
        WHERE current_job.id = v_decision.job_id;
        RETURN;
    END IF;

    -- Active cancellation discovers authority without locks, then acquires the
    -- coordinator lock order before rechecking every identity and state.
    SELECT active_attempt.id, active_attempt.worker_id
    INTO v_candidate_attempt_id, v_candidate_worker_id
    FROM public.attempts AS active_attempt
    JOIN public.jobs AS active_job ON active_job.id = active_attempt.job_id
    WHERE active_job.id = p_job_id
      AND active_job.organization_id = v_organization_id
      AND active_job.project_id = v_project_id
      AND active_job.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
      AND active_attempt.state::text = active_job.state::text
    ORDER BY active_attempt.attempt_number DESC
    LIMIT 1;

    IF FOUND THEN
        SELECT candidate.* INTO v_worker
        FROM public.workers AS candidate
        WHERE candidate.id = v_candidate_worker_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'active Worker authority changed during cancellation'
                USING ERRCODE = '40001';
        END IF;

        LOCK TABLE public.attempt_leases IN ROW EXCLUSIVE MODE;

        SELECT candidate.* INTO v_lease
        FROM public.attempt_leases AS candidate
        WHERE candidate.attempt_id = v_candidate_attempt_id
          AND candidate.revoked_at IS NULL
          AND (
              (
                  candidate.phase = 'EXECUTION'
                  AND candidate.owner_kind = 'WORKER'
              )
              OR (
                  candidate.phase = 'FINALIZATION'
                  AND candidate.owner_kind IN ('WORKER', 'RECONCILER')
              )
          )
        ORDER BY candidate.issued_at DESC
        LIMIT 1
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'active Lease authority changed during cancellation'
                USING ERRCODE = '40001';
        END IF;

        SELECT candidate.* INTO v_attempt
        FROM public.attempts AS candidate
        WHERE candidate.id = v_candidate_attempt_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'active Attempt authority changed during cancellation'
                USING ERRCODE = '40001';
        END IF;

        SELECT candidate.* INTO v_job
        FROM public.jobs AS candidate
        WHERE candidate.id = p_job_id
          AND candidate.organization_id = v_organization_id
          AND candidate.project_id = v_project_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'active Job authority changed during cancellation'
                USING ERRCODE = '40001';
        END IF;

        -- A concurrent cancellation may have committed while this transaction
        -- waited for the Worker lock. Replay its immutable decision.
        SELECT existing.* INTO v_decision
        FROM public.job_cancellation_decisions AS existing
        WHERE existing.job_id = p_job_id
          AND existing.organization_id = v_organization_id
          AND existing.project_id = v_project_id;
        IF FOUND THEN
            RETURN QUERY
            SELECT
                v_decision.id,
                v_decision.job_id,
                v_decision.decision,
                v_job.state,
                v_decision.previous_job_state,
                v_job.version,
                v_decision.cancellation_fence,
                v_decision.attempt_id,
                v_decision.worker_id,
                v_decision.worker_epoch,
                v_decision.attempt_fence,
                v_decision.authority_lease_id,
                v_decision.authority_lease_phase,
                v_decision.authority_lease_owner_kind,
                v_decision.authority_lease_owner_id,
                v_decision.authority_lease_expires_at,
                v_decision.billable,
                charge.id,
                charge.amount_minor,
                charge.currency,
                charge.reason,
                charge.posted_at,
                v_decision.decided_at,
                false
            FROM public.charges AS charge
            WHERE charge.cancellation_id = v_decision.id
            UNION ALL
            SELECT
                v_decision.id,
                v_decision.job_id,
                v_decision.decision,
                v_job.state,
                v_decision.previous_job_state,
                v_job.version,
                v_decision.cancellation_fence,
                v_decision.attempt_id,
                v_decision.worker_id,
                v_decision.worker_epoch,
                v_decision.attempt_fence,
                v_decision.authority_lease_id,
                v_decision.authority_lease_phase,
                v_decision.authority_lease_owner_kind,
                v_decision.authority_lease_owner_id,
                v_decision.authority_lease_expires_at,
                v_decision.billable,
                NULL::uuid,
                NULL::bigint,
                NULL::text,
                NULL::public.charge_reason,
                NULL::timestamptz,
                v_decision.decided_at,
                false
            WHERE NOT EXISTS (
                SELECT 1 FROM public.charges AS existing_charge
                WHERE existing_charge.cancellation_id = v_decision.id
            );
            RETURN;
        END IF;

        IF v_job.state NOT IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
            OR v_attempt.job_id <> v_job.id
            OR v_attempt.organization_id <> v_job.organization_id
            OR v_attempt.project_id <> v_job.project_id
            OR v_attempt.state::text <> v_job.state::text
            OR v_attempt.worker_id <> v_worker.id
            OR v_attempt.worker_epoch <> v_worker.epoch
            OR v_attempt.fence <> v_job.current_fence
            OR v_lease.attempt_id <> v_attempt.id
            OR v_lease.worker_id <> v_attempt.worker_id
            OR v_lease.worker_epoch <> v_attempt.worker_epoch
            OR v_lease.fence <> v_attempt.fence
            OR v_lease.revoked_at IS NOT NULL
            OR (
                v_job.state IN ('ASSIGNED', 'RUNNING')
                AND (
                    v_lease.phase <> 'EXECUTION'
                    OR v_lease.owner_kind <> 'WORKER'
                )
            )
            OR (
                v_job.state = 'FINALIZING'
                AND (
                    v_lease.phase <> 'FINALIZATION'
                    OR v_lease.owner_kind NOT IN ('WORKER', 'RECONCILER')
                )
            )
        THEN
            RAISE EXCEPTION 'active Job authority changed during cancellation'
                USING ERRCODE = '40001';
        END IF;

        v_billable := v_job.state IN ('RUNNING', 'FINALIZING');
        v_next_decision := CASE
            WHEN v_billable THEN 'CANCELING'::public.cancellation_decision
            ELSE 'CANCELED'::public.cancellation_decision
        END;
        v_next_job_state := CASE
            WHEN v_billable THEN 'CANCELING'::public.job_state
            ELSE 'CANCELED'::public.job_state
        END;
        IF v_billable AND v_job.billable_started_at IS NULL THEN
            RAISE EXCEPTION 'billable cancellation requires immutable Billable Start'
                USING ERRCODE = '23514';
        END IF;

        PERFORM 1 FROM public.projects
        WHERE id = v_job.project_id AND organization_id = v_job.organization_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Project disappeared during cancellation'
                USING ERRCODE = '23503';
        END IF;

        SELECT reservation.* INTO STRICT v_reservation
        FROM public.credit_reservations AS reservation
        WHERE reservation.job_id = v_job.id
        FOR UPDATE;

        PERFORM 1 FROM public.organization_credit_accounts
        WHERE organization_id = v_job.organization_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Organization credit disappeared during cancellation'
                USING ERRCODE = '23503';
        END IF;
        IF v_reservation.state <> 'RESERVED' THEN
            RAISE EXCEPTION 'cancelable Job must retain RESERVED credit'
                USING ERRCODE = '23514';
        END IF;

        INSERT INTO public.job_cancellation_decisions (
            id,
            organization_id,
            project_id,
            job_id,
            requested_by_principal_id,
            previous_job_state,
            decision,
            billable,
            attempt_id,
            worker_id,
            worker_epoch,
            attempt_fence,
            authority_lease_id,
            authority_lease_phase,
            authority_lease_owner_kind,
            authority_lease_owner_id,
            authority_lease_expires_at,
            cancellation_fence,
            job_version,
            decided_at
        ) VALUES (
            p_cancellation_id,
            v_job.organization_id,
            v_job.project_id,
            v_job.id,
            v_principal_id,
            v_job.state,
            v_next_decision,
            v_billable,
            v_attempt.id,
            v_attempt.worker_id,
            v_attempt.worker_epoch,
            v_attempt.fence,
            v_lease.id,
            v_lease.phase,
            v_lease.owner_kind,
            v_lease.owner_id,
            v_lease.expires_at,
            v_job.current_fence + 1,
            v_job.version + 1,
            v_decided_at
        );

        UPDATE public.attempt_leases AS lease
        SET revoked_at = v_decided_at,
            updated_at = v_decided_at
        WHERE lease.id = v_lease.id
          AND lease.attempt_id = v_attempt.id
          AND lease.worker_id = v_attempt.worker_id
          AND lease.worker_epoch = v_attempt.worker_epoch
          AND lease.fence = v_attempt.fence
          AND lease.revoked_at IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Lease changed during cancellation' USING ERRCODE = '40001';
        END IF;

        UPDATE public.attempts AS attempt
        SET state = 'CANCELED',
            ended_at = v_decided_at,
            updated_at = v_decided_at
        WHERE attempt.id = v_attempt.id
          AND attempt.state::text = v_job.state::text
          AND attempt.ended_at IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Attempt changed during cancellation' USING ERRCODE = '40001';
        END IF;

        UPDATE public.jobs
        SET state = v_next_job_state,
            version = version + 1,
            current_fence = current_fence + 1,
            updated_at = v_decided_at
        WHERE id = v_job.id
          AND version = v_job.version
          AND current_fence = v_job.current_fence
          AND state = v_job.state;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Job changed during cancellation' USING ERRCODE = '40001';
        END IF;

        UPDATE public.workers
        SET lifecycle_state = CASE
                WHEN lifecycle_state IN ('READY', 'BUSY')
                THEN 'DRAINING'::public.worker_lifecycle_state
                ELSE lifecycle_state
            END,
            updated_at = v_decided_at
        WHERE id = v_worker.id AND epoch = v_worker.epoch;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Worker changed during cancellation' USING ERRCODE = '40001';
        END IF;

        UPDATE public.projects
        SET running_count = running_count - 1
        WHERE id = v_job.project_id
          AND organization_id = v_job.organization_id
          AND running_count > 0;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Project running counter changed during cancellation'
                USING ERRCODE = '23514';
        END IF;

        UPDATE public.credit_reservations
        SET state = CASE
                WHEN v_billable THEN 'CONSUMED'::public.credit_reservation_state
                ELSE 'RELEASED'::public.credit_reservation_state
            END,
            updated_at = v_decided_at
        WHERE id = v_reservation.id AND state = 'RESERVED';
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'CreditReservation changed during cancellation'
                USING ERRCODE = '23514';
        END IF;

        UPDATE public.organization_credit_accounts
        SET reserved_minor = reserved_minor - v_reservation.amount_minor,
            unsettled_posted_minor = unsettled_posted_minor
                + CASE WHEN v_billable THEN v_reservation.amount_minor ELSE 0 END,
            version = version + 1,
            updated_at = v_decided_at
        WHERE organization_id = v_job.organization_id
          AND currency = v_reservation.currency
          AND reserved_minor >= v_reservation.amount_minor;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Organization reserved credit changed during cancellation'
                USING ERRCODE = '23514';
        END IF;

        IF v_billable THEN
            INSERT INTO public.charges (
                id,
                organization_id,
                project_id,
                job_id,
                credit_reservation_id,
                cancellation_id,
                reason,
                amount_minor,
                currency,
                posted_at
            ) VALUES (
                p_charge_id,
                v_job.organization_id,
                v_job.project_id,
                v_job.id,
                v_reservation.id,
                p_cancellation_id,
                'CUSTOMER_CANCELLATION',
                v_reservation.amount_minor,
                v_reservation.currency,
                v_decided_at
            );
        END IF;

        PERFORM vela_private.vela_insert_cancellation_outbox_events(
            p_cancellation_id,
            p_cancel_requested_event_id,
            p_canceling_event_id,
            p_canceled_event_id,
            p_charge_posted_event_id,
            p_invoice_export_event_id
        );

        RETURN QUERY SELECT
            p_cancellation_id,
            v_job.id,
            v_next_decision,
            v_next_job_state,
            v_job.state,
            v_job.version + 1,
            v_job.current_fence + 1,
            v_attempt.id,
            v_attempt.worker_id,
            v_attempt.worker_epoch,
            v_attempt.fence,
            v_lease.id,
            v_lease.phase,
            v_lease.owner_kind,
            v_lease.owner_id,
            v_lease.expires_at,
            v_billable,
            CASE WHEN v_billable THEN p_charge_id ELSE NULL END::uuid,
            CASE WHEN v_billable THEN v_reservation.amount_minor ELSE NULL END::bigint,
            CASE WHEN v_billable THEN v_reservation.currency ELSE NULL END::text,
            CASE WHEN v_billable THEN 'CUSTOMER_CANCELLATION'::public.charge_reason ELSE NULL END,
            CASE WHEN v_billable THEN v_decided_at ELSE NULL END::timestamptz,
            v_decided_at,
            true;
        RETURN;
    END IF;

    SELECT candidate.* INTO v_job
    FROM public.jobs AS candidate
    WHERE candidate.id = p_job_id
      AND candidate.organization_id = v_organization_id
      AND candidate.project_id = v_project_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job is not visible in this Project' USING ERRCODE = 'P0002';
    END IF;

	SELECT existing.* INTO v_decision
	FROM public.job_cancellation_decisions AS existing
	WHERE existing.job_id = v_job.id
	  AND existing.organization_id = v_organization_id
	  AND existing.project_id = v_project_id;
	IF FOUND THEN
		RETURN QUERY
		SELECT
			v_decision.id,
			v_decision.job_id,
			v_decision.decision,
			v_job.state,
			v_decision.previous_job_state,
			v_job.version,
			v_decision.cancellation_fence,
			v_decision.attempt_id,
				v_decision.worker_id,
				v_decision.worker_epoch,
				v_decision.attempt_fence,
				v_decision.authority_lease_id,
				v_decision.authority_lease_phase,
				v_decision.authority_lease_owner_kind,
				v_decision.authority_lease_owner_id,
				v_decision.authority_lease_expires_at,
				v_decision.billable,
			charge.id,
			charge.amount_minor,
			charge.currency,
			charge.reason,
			charge.posted_at,
			v_decision.decided_at,
			false
		FROM public.charges AS charge
		WHERE charge.cancellation_id = v_decision.id
		UNION ALL
		SELECT
			v_decision.id,
			v_decision.job_id,
			v_decision.decision,
			v_job.state,
			v_decision.previous_job_state,
			v_job.version,
			v_decision.cancellation_fence,
			v_decision.attempt_id,
				v_decision.worker_id,
				v_decision.worker_epoch,
				v_decision.attempt_fence,
				v_decision.authority_lease_id,
				v_decision.authority_lease_phase,
				v_decision.authority_lease_owner_kind,
				v_decision.authority_lease_owner_id,
				v_decision.authority_lease_expires_at,
				v_decision.billable,
			NULL::uuid,
			NULL::bigint,
			NULL::text,
			NULL::public.charge_reason,
			NULL::timestamptz,
			v_decision.decided_at,
			false
		WHERE NOT EXISTS (
			SELECT 1 FROM public.charges AS existing_charge
			WHERE existing_charge.cancellation_id = v_decision.id
		);
		RETURN;
	END IF;

    IF v_job.state IN ('SUCCEEDED', 'FAILED') THEN
        RETURN QUERY SELECT
            NULL::uuid,
            v_job.id,
            CASE
                WHEN v_job.state = 'SUCCEEDED'
                THEN 'ALREADY_SUCCEEDED'::public.cancellation_decision
                ELSE 'ALREADY_FAILED'::public.cancellation_decision
            END,
            v_job.state,
            v_job.state,
            v_job.version,
            v_job.current_fence,
            NULL::uuid,
            NULL::uuid,
            NULL::bigint,
            NULL::bigint,
            NULL::uuid,
            NULL::public.lease_phase,
            NULL::public.lease_owner_kind,
            NULL::text,
            NULL::timestamptz,
            false,
            NULL::uuid,
            NULL::bigint,
            NULL::text,
            NULL::public.charge_reason,
            NULL::timestamptz,
            v_job.updated_at,
            false;
        RETURN;
    END IF;
    IF v_job.state IN ('CANCELED', 'CANCELING') THEN
        RETURN QUERY SELECT
            NULL::uuid,
            v_job.id,
            v_job.state::text::public.cancellation_decision,
            v_job.state,
            v_job.state,
            v_job.version,
            v_job.current_fence,
            NULL::uuid,
            NULL::uuid,
            NULL::bigint,
            NULL::bigint,
            NULL::uuid,
            NULL::public.lease_phase,
            NULL::public.lease_owner_kind,
            NULL::text,
            NULL::timestamptz,
            false,
            NULL::uuid,
            NULL::bigint,
            NULL::text,
            NULL::public.charge_reason,
            NULL::timestamptz,
            v_job.updated_at,
            false;
        RETURN;
    END IF;
    IF v_job.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING') THEN
        RAISE EXCEPTION 'active Job authority became visible after cancellation discovery'
            USING ERRCODE = '40001';
    END IF;
    IF v_job.state NOT IN ('QUEUED', 'RETRY_WAIT') THEN
        RAISE EXCEPTION 'active Job cancellation is not enabled in this schema revision'
            USING ERRCODE = '55000';
    END IF;

    SELECT reservation.* INTO STRICT v_reservation
    FROM public.credit_reservations AS reservation
    WHERE reservation.job_id = v_job.id
    FOR UPDATE;
    PERFORM 1 FROM public.projects
    WHERE id = v_job.project_id AND organization_id = v_job.organization_id
    FOR UPDATE;
    PERFORM 1 FROM public.worker_pools WHERE id = v_job.worker_pool_id FOR UPDATE;
    PERFORM 1 FROM public.organization_credit_accounts
    WHERE organization_id = v_job.organization_id
    FOR UPDATE;

    IF v_reservation.state <> 'RESERVED' THEN
        RAISE EXCEPTION 'cancelable Job must retain RESERVED credit'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO public.job_cancellation_decisions (
        id,
        organization_id,
        project_id,
        job_id,
        requested_by_principal_id,
        previous_job_state,
        decision,
        billable,
        cancellation_fence,
        job_version,
        decided_at
    ) VALUES (
        p_cancellation_id,
        v_job.organization_id,
        v_job.project_id,
        v_job.id,
        v_principal_id,
        v_job.state,
        'CANCELED',
        false,
        v_job.current_fence,
        v_job.version + 1,
        v_decided_at
    );

    UPDATE public.jobs
    SET state = 'CANCELED',
        version = version + 1,
        updated_at = v_decided_at
    WHERE id = v_job.id
      AND version = v_job.version
      AND state = v_job.state;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Job changed during cancellation' USING ERRCODE = '40001';
    END IF;

    UPDATE public.projects
    SET queued_count = queued_count - 1,
        retry_wait_count = retry_wait_count
            - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
    WHERE id = v_job.project_id
      AND organization_id = v_job.organization_id
      AND queued_count > 0
      AND (
          v_job.state <> 'RETRY_WAIT'
          OR retry_wait_count > 0
      );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Project queue counters changed during cancellation'
            USING ERRCODE = '23514';
    END IF;

    UPDATE public.worker_pools
    SET queued_count = queued_count - 1,
        retry_wait_count = retry_wait_count
            - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
    WHERE id = v_job.worker_pool_id
      AND queued_count > 0
      AND (
          v_job.state <> 'RETRY_WAIT'
          OR retry_wait_count > 0
      );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Worker pool queue counters changed during cancellation'
            USING ERRCODE = '23514';
    END IF;

    UPDATE public.credit_reservations
    SET state = 'RELEASED', updated_at = v_decided_at
    WHERE id = v_reservation.id AND state = 'RESERVED';
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'CreditReservation changed during cancellation'
            USING ERRCODE = '23514';
    END IF;

    UPDATE public.organization_credit_accounts
    SET reserved_minor = reserved_minor - v_reservation.amount_minor,
        version = version + 1,
        updated_at = v_decided_at
    WHERE organization_id = v_job.organization_id
      AND currency = v_reservation.currency
      AND reserved_minor >= v_reservation.amount_minor;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Organization reserved credit changed during cancellation'
            USING ERRCODE = '23514';
    END IF;

    PERFORM vela_private.vela_insert_cancellation_outbox_events(
        p_cancellation_id,
        p_cancel_requested_event_id,
        p_canceling_event_id,
        p_canceled_event_id,
        p_charge_posted_event_id,
        p_invoice_export_event_id
    );

    RETURN QUERY SELECT
        p_cancellation_id,
        v_job.id,
        'CANCELED'::public.cancellation_decision,
        'CANCELED'::public.job_state,
        v_job.state,
        v_job.version + 1,
        v_job.current_fence,
        NULL::uuid,
        NULL::uuid,
        NULL::bigint,
        NULL::bigint,
        NULL::uuid,
        NULL::public.lease_phase,
        NULL::public.lease_owner_kind,
        NULL::text,
        NULL::timestamptz,
        false,
        NULL::uuid,
        NULL::bigint,
        NULL::text,
        NULL::public.charge_reason,
        NULL::timestamptz,
        v_decided_at,
        true;
END
$$;
REVOKE ALL ON FUNCTION vela_cancel_job(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) FROM PUBLIC;
ALTER FUNCTION vela_cancel_job(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) OWNER TO vela_internal;

GRANT USAGE ON SCHEMA public TO vela_cancel;
GRANT EXECUTE ON FUNCTION vela_set_cancellation_request_context(uuid, bytea) TO vela_cancel;
GRANT EXECUTE ON FUNCTION vela_cancel_job(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) TO vela_cancel;

GRANT SELECT, INSERT ON job_cancellation_decisions, charges, cancellation_stop_receipts
    TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    workers,
    attempt_leases,
    attempts,
    jobs,
    credit_reservations,
    projects,
    worker_pools,
    organization_credit_accounts,
    job_cancellation_decisions,
    charges,
    cancellation_stop_receipts,
    outbox_events
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM jobs WHERE state = 'CANCELING') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'customer_cancellation_contract_requires_drained_canceling',
            MESSAGE = 'Migration 00006 cannot contract while CANCELING Jobs remain; acknowledge or reconcile their stop before schema rollback';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM job_cancellation_decisions) THEN
        CREATE TABLE vela_private.job_cancellation_decisions_rollback AS
        SELECT to_jsonb(cancellation_row) AS decision
        FROM job_cancellation_decisions AS cancellation_row;
        ALTER TABLE vela_private.job_cancellation_decisions_rollback OWNER TO vela_internal;
        REVOKE ALL ON vela_private.job_cancellation_decisions_rollback
            FROM PUBLIC, vela_request, vela_auth, vela_cancel;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM charges) THEN
        CREATE TABLE vela_private.charges_rollback AS
        SELECT to_jsonb(charge) AS charge
        FROM charges AS charge;
        ALTER TABLE vela_private.charges_rollback OWNER TO vela_internal;
        REVOKE ALL ON vela_private.charges_rollback
            FROM PUBLIC, vela_request, vela_auth, vela_cancel;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cancellation_stop_receipts) THEN
        CREATE TABLE vela_private.cancellation_stop_receipts_rollback AS
        SELECT to_jsonb(receipt) AS receipt
        FROM cancellation_stop_receipts AS receipt;
        ALTER TABLE vela_private.cancellation_stop_receipts_rollback OWNER TO vela_internal;
        REVOKE ALL ON vela_private.cancellation_stop_receipts_rollback
            FROM PUBLIC, vela_request, vela_auth, vela_cancel;
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_cancel_job(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) FROM vela_cancel;
REVOKE EXECUTE ON FUNCTION vela_set_cancellation_request_context(uuid, bytea) FROM vela_cancel;
REVOKE USAGE ON SCHEMA public FROM vela_cancel;
DROP FUNCTION IF EXISTS vela_cancel_job(uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid);
DROP FUNCTION IF EXISTS vela_set_cancellation_request_context(uuid, bytea);
DROP FUNCTION IF EXISTS vela_private.vela_insert_cancellation_outbox_events(
    uuid, uuid, uuid, uuid, uuid, uuid
);
DROP FUNCTION IF EXISTS vela_private.vela_insert_canonical_cancellation_event(
    uuid, uuid, uuid, uuid, bigint, text, timestamptz, integer, bytea
);
DROP FUNCTION IF EXISTS vela_private.vela_cancellation_event_envelope(
    uuid, uuid, bigint, text, timestamptz, integer, bytea
);
DROP FUNCTION IF EXISTS vela_private.vela_proto_timestamp(timestamptz);
DROP FUNCTION IF EXISTS vela_private.vela_proto_uint(integer, bigint);
DROP FUNCTION IF EXISTS vela_private.vela_proto_string(integer, text);
DROP FUNCTION IF EXISTS vela_private.vela_proto_bytes(integer, bytea);
DROP FUNCTION IF EXISTS vela_private.vela_proto_varint(bigint);
DROP TRIGGER IF EXISTS cancellation_stop_receipts_immutable ON cancellation_stop_receipts;
DROP TRIGGER IF EXISTS charges_immutable ON charges;
DROP TRIGGER IF EXISTS job_cancellation_decisions_immutable ON job_cancellation_decisions;
DROP FUNCTION IF EXISTS vela_reject_cancellation_history_mutation();
DROP TABLE IF EXISTS cancellation_stop_receipts;
DROP TABLE IF EXISTS charges;
DROP TABLE IF EXISTS job_cancellation_decisions;
DROP TYPE IF EXISTS cancellation_decision;
DROP TYPE IF EXISTS cancellation_stop_source;
DROP TYPE IF EXISTS charge_reason;
-- +goose StatementEnd
