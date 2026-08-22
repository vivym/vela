-- +goose Up
-- +goose StatementBegin
CREATE TYPE invoice_export_state AS ENUM ('PENDING', 'CLAIMED', 'EXPORTED');

CREATE TABLE invoice_exports (
    charge_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    requested_event_id uuid NOT NULL UNIQUE,
    state invoice_export_state NOT NULL DEFAULT 'PENDING',
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_by text,
    claim_token uuid,
    claim_expires_at timestamptz,
    last_error text CHECK (last_error IS NULL OR length(last_error) <= 2000),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id, charge_id),
    FOREIGN KEY (organization_id, project_id, job_id, charge_id)
        REFERENCES charges(organization_id, project_id, job_id, id),
    FOREIGN KEY (requested_event_id) REFERENCES outbox_events(event_id),
    CHECK (
        (
            state IN ('PENDING', 'EXPORTED')
            AND claimed_by IS NULL
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
        )
        OR (
            state = 'CLAIMED'
            AND claimed_by IS NOT NULL
            AND length(claimed_by) BETWEEN 1 AND 200
            AND claim_token IS NOT NULL
            AND claim_expires_at IS NOT NULL
        )
    )
);

CREATE TABLE invoice_export_receipts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    charge_id uuid NOT NULL UNIQUE,
    external_invoice_reference text NOT NULL
        CHECK (length(external_invoice_reference) BETWEEN 1 AND 500),
    external_line_reference text NOT NULL
        CHECK (length(external_line_reference) BETWEEN 1 AND 500),
    exported_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (external_invoice_reference, external_line_reference),
    FOREIGN KEY (organization_id, project_id, job_id, charge_id)
        REFERENCES invoice_exports(organization_id, project_id, job_id, charge_id)
);

CREATE INDEX invoice_exports_ready_idx
    ON invoice_exports (available_at, claim_expires_at, created_at, charge_id)
    WHERE state <> 'EXPORTED';

ALTER TABLE invoice_exports ENABLE ROW LEVEL SECURITY;
ALTER TABLE invoice_exports FORCE ROW LEVEL SECURITY;
ALTER TABLE invoice_export_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE invoice_export_receipts FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_invoice_export_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Invoice export receipts are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_invoice_export_receipt_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_invoice_export_receipt_mutation() OWNER TO vela_billing_owner;

CREATE TRIGGER invoice_export_receipts_immutable
BEFORE UPDATE OR DELETE ON invoice_export_receipts
FOR EACH ROW EXECUTE FUNCTION vela_reject_invoice_export_receipt_mutation();

CREATE FUNCTION vela_enforce_invoice_export_transition() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'PENDING'
           OR NEW.attempts <> 0
           OR NEW.claimed_by IS NOT NULL
           OR NEW.claim_token IS NOT NULL
           OR NEW.claim_expires_at IS NOT NULL
           OR NEW.last_error IS NOT NULL THEN
            RAISE EXCEPTION 'Invoice export authority must start as unclaimed PENDING';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Invoice export authority cannot be deleted';
    END IF;
    IF ROW(
        NEW.charge_id,
        NEW.organization_id,
        NEW.project_id,
        NEW.job_id,
        NEW.requested_event_id,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.charge_id,
        OLD.organization_id,
        OLD.project_id,
        OLD.job_id,
        OLD.requested_event_id,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'Invoice export identity is immutable';
    END IF;
    IF OLD.state = 'EXPORTED' THEN
        RAISE EXCEPTION 'exported Invoice authority is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'PENDING' AND NEW.state = 'CLAIMED')
        OR (OLD.state = 'CLAIMED' AND NEW.state IN ('PENDING', 'EXPORTED'))
        OR (
            OLD.state = 'CLAIMED'
            AND NEW.state = 'CLAIMED'
            AND OLD.claim_expires_at <= clock_timestamp()
        )
    ) THEN
        RAISE EXCEPTION 'invalid Invoice export transition from % to %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_invoice_export_transition() FROM PUBLIC;
ALTER FUNCTION vela_enforce_invoice_export_transition() OWNER TO vela_billing_owner;

CREATE TRIGGER invoice_exports_state_machine
BEFORE INSERT OR UPDATE OR DELETE ON invoice_exports
FOR EACH ROW EXECUTE FUNCTION vela_enforce_invoice_export_transition();

CREATE FUNCTION vela_check_invoice_export_receipt_consistency() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_charge_id uuid := COALESCE(NEW.charge_id, OLD.charge_id);
    v_state public.invoice_export_state;
    v_has_receipt boolean;
BEGIN
    SELECT export.state INTO v_state
    FROM public.invoice_exports AS export
    WHERE export.charge_id = v_charge_id;
    SELECT EXISTS (
        SELECT 1
        FROM public.invoice_export_receipts AS receipt
        WHERE receipt.charge_id = v_charge_id
    ) INTO v_has_receipt;
    IF v_state IS NULL OR ((v_state = 'EXPORTED') IS DISTINCT FROM v_has_receipt) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'invoice_export_receipt_consistency',
            MESSAGE = format(
                'Invoice export %s must be EXPORTED exactly when its receipt exists',
                v_charge_id
            );
    END IF;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_check_invoice_export_receipt_consistency() FROM PUBLIC;
ALTER FUNCTION vela_check_invoice_export_receipt_consistency() OWNER TO vela_billing_owner;

CREATE CONSTRAINT TRIGGER invoice_exports_receipt_consistency
AFTER INSERT OR UPDATE ON invoice_exports
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_check_invoice_export_receipt_consistency();
CREATE CONSTRAINT TRIGGER invoice_export_receipts_authority_consistency
AFTER INSERT ON invoice_export_receipts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_check_invoice_export_receipt_consistency();

CREATE FUNCTION vela_private.vela_find_invoice_export_intent(
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_charge_id uuid,
    p_posted_at timestamptz
) RETURNS uuid
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_event_ids uuid[];
BEGIN
    SELECT array_agg(candidate.event_id ORDER BY candidate.event_id)
    INTO v_event_ids
    FROM (
        SELECT event.event_id
        FROM public.outbox_events AS event
        WHERE event.organization_id = p_organization_id
          AND event.project_id = p_project_id
          AND event.aggregate_type = 'Job'
          AND event.aggregate_id = p_job_id
          AND event.event_type = 'invoice.export_requested'
          AND event.schema_version = 1
          AND event.occurred_at = p_posted_at
          AND event.payload = vela_private.vela_cancellation_event_envelope(
              event.event_id,
              p_job_id,
              event.aggregate_version,
              'invoice.export_requested',
              p_posted_at,
              29,
              vela_private.vela_proto_string(1, p_organization_id::text)
                  || vela_private.vela_proto_string(2, p_project_id::text)
                  || vela_private.vela_proto_string(3, p_job_id::text)
                  || vela_private.vela_proto_string(4, p_charge_id::text)
                  || vela_private.vela_proto_bytes(
                      5,
                      vela_private.vela_proto_timestamp(p_posted_at)
                  )
          )
        LIMIT 2
    ) AS candidate;

    IF cardinality(v_event_ids) IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'charge_requires_exact_invoice_export_intent',
            MESSAGE = format(
                'Charge %s requires exactly one matching invoice.export_requested event',
                p_charge_id
            );
    END IF;
    RETURN v_event_ids[1];
END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_find_invoice_export_intent(
    uuid, uuid, uuid, uuid, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_find_invoice_export_intent(
    uuid, uuid, uuid, uuid, timestamptz
) OWNER TO vela_billing_owner;

CREATE FUNCTION vela_create_invoice_export_authority() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_event_id uuid;
BEGIN
    v_event_id := vela_private.vela_find_invoice_export_intent(
        NEW.organization_id,
        NEW.project_id,
        NEW.job_id,
        NEW.id,
        NEW.posted_at
    );
    INSERT INTO public.invoice_exports (
        charge_id,
        organization_id,
        project_id,
        job_id,
        requested_event_id,
        available_at,
        created_at,
        updated_at
    ) VALUES (
        NEW.id,
        NEW.organization_id,
        NEW.project_id,
        NEW.job_id,
        v_event_id,
        NEW.posted_at,
        NEW.posted_at,
        NEW.posted_at
    );
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_create_invoice_export_authority() FROM PUBLIC;
ALTER FUNCTION vela_create_invoice_export_authority() OWNER TO vela_billing_owner;

CREATE CONSTRAINT TRIGGER charge_creates_invoice_export_authority
AFTER INSERT ON charges
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_create_invoice_export_authority();

DO $$
DECLARE
    v_charge public.charges%ROWTYPE;
    v_event_id uuid;
BEGIN
    FOR v_charge IN SELECT * FROM public.charges ORDER BY posted_at, id LOOP
        v_event_id := vela_private.vela_find_invoice_export_intent(
            v_charge.organization_id,
            v_charge.project_id,
            v_charge.job_id,
            v_charge.id,
            v_charge.posted_at
        );
        INSERT INTO public.invoice_exports (
            charge_id,
            organization_id,
            project_id,
            job_id,
            requested_event_id,
            available_at,
            created_at,
            updated_at
        ) VALUES (
            v_charge.id,
            v_charge.organization_id,
            v_charge.project_id,
            v_charge.job_id,
            v_event_id,
            v_charge.posted_at,
            v_charge.posted_at,
            v_charge.posted_at
        );
    END LOOP;
END
$$;

CREATE FUNCTION vela_claim_invoice_exports(
    p_claimed_by text,
    p_claim_token uuid,
    p_claim_seconds integer,
    p_batch_size integer
) RETURNS TABLE (
    charge_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    reason text,
    amount_minor bigint,
    currency text,
    posted_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_claimed_by IS NULL
       OR length(p_claimed_by) NOT BETWEEN 1 AND 200
       OR btrim(p_claimed_by) <> p_claimed_by
       OR p_claim_token IS NULL
       OR p_claim_seconds NOT BETWEEN 1 AND 300
       OR p_batch_size NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'Invoice export claim parameters are invalid or outside bounded limits';
    END IF;
    RETURN QUERY WITH candidates AS (
        SELECT export.charge_id
        FROM public.invoice_exports AS export
        WHERE export.state <> 'EXPORTED'
          AND export.available_at <= clock_timestamp()
          AND (
              export.state = 'PENDING'
              OR export.claim_expires_at <= clock_timestamp()
          )
        ORDER BY export.available_at, export.created_at, export.charge_id
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    ), claimed AS (
        UPDATE public.invoice_exports AS export
        SET state = 'CLAIMED',
            attempts = export.attempts + 1,
            claimed_by = p_claimed_by,
            claim_token = p_claim_token,
            claim_expires_at = clock_timestamp() + make_interval(secs => p_claim_seconds),
            last_error = NULL,
            updated_at = clock_timestamp()
        FROM candidates
        WHERE export.charge_id = candidates.charge_id
        RETURNING export.charge_id, export.organization_id, export.project_id, export.job_id
    )
    SELECT
        claimed.charge_id,
        claimed.organization_id,
        claimed.project_id,
        claimed.job_id,
        charge.reason::text,
        charge.amount_minor,
        charge.currency,
        charge.posted_at
    FROM claimed
    JOIN public.charges AS charge
      ON charge.organization_id = claimed.organization_id
     AND charge.project_id = claimed.project_id
     AND charge.job_id = claimed.job_id
     AND charge.id = claimed.charge_id
    ORDER BY charge.posted_at, charge.id
    ;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_invoice_exports(text, uuid, integer, integer) FROM PUBLIC;
ALTER FUNCTION vela_claim_invoice_exports(text, uuid, integer, integer) OWNER TO vela_billing_owner;

CREATE FUNCTION vela_mark_invoice_exported(
    p_receipt_id uuid,
    p_charge_id uuid,
    p_claim_token uuid,
    p_external_invoice_reference text,
    p_external_line_reference text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_export public.invoice_exports%ROWTYPE;
    v_exported_at timestamptz := clock_timestamp();
BEGIN
    IF p_receipt_id IS NULL
       OR p_charge_id IS NULL
       OR p_claim_token IS NULL
       OR p_external_invoice_reference IS NULL
       OR length(p_external_invoice_reference) NOT BETWEEN 1 AND 500
       OR btrim(p_external_invoice_reference) <> p_external_invoice_reference
       OR p_external_line_reference IS NULL
       OR length(p_external_line_reference) NOT BETWEEN 1 AND 500
       OR btrim(p_external_line_reference) <> p_external_line_reference THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'Invoice export receipt parameters are invalid';
    END IF;
    SELECT export.* INTO v_export
    FROM public.invoice_exports AS export
    WHERE export.charge_id = p_charge_id
      AND export.state = 'CLAIMED'
      AND export.claim_token = p_claim_token
      AND export.claim_expires_at > v_exported_at
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    INSERT INTO public.invoice_export_receipts (
        id,
        organization_id,
        project_id,
        job_id,
        charge_id,
        external_invoice_reference,
        external_line_reference,
        exported_at
    ) VALUES (
        p_receipt_id,
        v_export.organization_id,
        v_export.project_id,
        v_export.job_id,
        v_export.charge_id,
        p_external_invoice_reference,
        p_external_line_reference,
        v_exported_at
    );

    UPDATE public.invoice_exports
    SET state = 'EXPORTED',
        claimed_by = NULL,
        claim_token = NULL,
        claim_expires_at = NULL,
        last_error = NULL,
        updated_at = v_exported_at
    WHERE charge_id = v_export.charge_id;
    RETURN true;
END
$$;
REVOKE ALL ON FUNCTION vela_mark_invoice_exported(uuid, uuid, uuid, text, text) FROM PUBLIC;
ALTER FUNCTION vela_mark_invoice_exported(uuid, uuid, uuid, text, text) OWNER TO vela_billing_owner;

CREATE FUNCTION vela_mark_invoice_export_failed(
    p_charge_id uuid,
    p_claim_token uuid,
    p_retry_after_seconds integer,
    p_last_error text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_failed_at timestamptz := clock_timestamp();
BEGIN
    IF p_charge_id IS NULL
       OR p_claim_token IS NULL
       OR p_retry_after_seconds NOT BETWEEN 0 AND 3600
       OR p_last_error IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'Invoice export failure parameters are invalid or outside bounded limits';
    END IF;
    UPDATE public.invoice_exports
    SET state = 'PENDING',
        available_at = v_failed_at + make_interval(secs => p_retry_after_seconds),
        claimed_by = NULL,
        claim_token = NULL,
        claim_expires_at = NULL,
        last_error = left(p_last_error, 2000),
        updated_at = v_failed_at
    WHERE charge_id = p_charge_id
      AND state = 'CLAIMED'
      AND claim_token = p_claim_token
      AND claim_expires_at > v_failed_at;
    RETURN FOUND;
END
$$;
REVOKE ALL ON FUNCTION vela_mark_invoice_export_failed(uuid, uuid, integer, text) FROM PUBLIC;
ALTER FUNCTION vela_mark_invoice_export_failed(uuid, uuid, integer, text) OWNER TO vela_billing_owner;

GRANT USAGE ON SCHEMA vela_private TO vela_billing_owner;
GRANT EXECUTE ON FUNCTION vela_private.vela_proto_string(integer, text)
    TO vela_billing_owner;
GRANT EXECUTE ON FUNCTION vela_private.vela_proto_bytes(integer, bytea)
    TO vela_billing_owner;
GRANT EXECUTE ON FUNCTION vela_private.vela_proto_uint(integer, bigint)
    TO vela_billing_owner;
GRANT EXECUTE ON FUNCTION vela_private.vela_proto_varint(bigint)
    TO vela_billing_owner;
GRANT EXECUTE ON FUNCTION vela_private.vela_proto_timestamp(timestamptz)
    TO vela_billing_owner;
GRANT EXECUTE ON FUNCTION vela_private.vela_cancellation_event_envelope(
    uuid, uuid, bigint, text, timestamptz, integer, bytea
) TO vela_billing_owner;
GRANT SELECT ON charges, outbox_events TO vela_billing_owner;
GRANT SELECT, INSERT, UPDATE ON invoice_exports, invoice_export_receipts
    TO vela_billing_owner;

GRANT USAGE ON SCHEMA public TO vela_billing;
GRANT EXECUTE ON FUNCTION vela_claim_invoice_exports(text, uuid, integer, integer)
    TO vela_billing;
GRANT EXECUTE ON FUNCTION vela_mark_invoice_exported(uuid, uuid, uuid, text, text)
    TO vela_billing;
GRANT EXECUTE ON FUNCTION vela_mark_invoice_export_failed(uuid, uuid, integer, text)
    TO vela_billing;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    charges,
    outbox_events,
    invoice_exports,
    invoice_export_receipts
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM invoice_export_receipts)
       OR EXISTS (
           SELECT 1
           FROM invoice_exports
           WHERE attempts > 0 OR state <> 'PENDING' OR last_error IS NOT NULL
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'invoice_export_contract_has_durable_evidence',
            MESSAGE = 'Migration 00009 cannot contract after Invoice export attempts or receipts exist';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_mark_invoice_export_failed(uuid, uuid, integer, text)
    FROM vela_billing;
REVOKE EXECUTE ON FUNCTION vela_mark_invoice_exported(uuid, uuid, uuid, text, text)
    FROM vela_billing;
REVOKE EXECUTE ON FUNCTION vela_claim_invoice_exports(text, uuid, integer, integer)
    FROM vela_billing;
REVOKE USAGE ON SCHEMA public FROM vela_billing;

DROP FUNCTION IF EXISTS vela_mark_invoice_export_failed(uuid, uuid, integer, text);
DROP FUNCTION IF EXISTS vela_mark_invoice_exported(uuid, uuid, uuid, text, text);
DROP FUNCTION IF EXISTS vela_claim_invoice_exports(text, uuid, integer, integer);
DROP TRIGGER IF EXISTS charge_creates_invoice_export_authority ON charges;
DROP FUNCTION IF EXISTS vela_create_invoice_export_authority();
DROP FUNCTION IF EXISTS vela_private.vela_find_invoice_export_intent(
    uuid, uuid, uuid, uuid, timestamptz
);
DROP TRIGGER IF EXISTS invoice_exports_state_machine ON invoice_exports;
DROP FUNCTION IF EXISTS vela_enforce_invoice_export_transition();
DROP TRIGGER IF EXISTS invoice_export_receipts_authority_consistency ON invoice_export_receipts;
DROP TRIGGER IF EXISTS invoice_exports_receipt_consistency ON invoice_exports;
DROP FUNCTION IF EXISTS vela_check_invoice_export_receipt_consistency();
DROP TRIGGER IF EXISTS invoice_export_receipts_immutable ON invoice_export_receipts;
DROP FUNCTION IF EXISTS vela_reject_invoice_export_receipt_mutation();
DROP TABLE IF EXISTS invoice_export_receipts;
DROP TABLE IF EXISTS invoice_exports;
REVOKE SELECT ON charges, outbox_events FROM vela_billing_owner;
REVOKE EXECUTE ON FUNCTION vela_private.vela_cancellation_event_envelope(
    uuid, uuid, bigint, text, timestamptz, integer, bytea
) FROM vela_billing_owner;
REVOKE EXECUTE ON FUNCTION vela_private.vela_proto_timestamp(timestamptz)
    FROM vela_billing_owner;
REVOKE EXECUTE ON FUNCTION vela_private.vela_proto_varint(bigint)
    FROM vela_billing_owner;
REVOKE EXECUTE ON FUNCTION vela_private.vela_proto_uint(integer, bigint)
    FROM vela_billing_owner;
REVOKE EXECUTE ON FUNCTION vela_private.vela_proto_bytes(integer, bytea)
    FROM vela_billing_owner;
REVOKE EXECUTE ON FUNCTION vela_private.vela_proto_string(integer, text)
    FROM vela_billing_owner;
REVOKE USAGE ON SCHEMA vela_private FROM vela_billing_owner;
DROP TYPE IF EXISTS invoice_export_state;
-- +goose StatementEnd
