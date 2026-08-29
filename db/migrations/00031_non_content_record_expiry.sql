-- +goose Up
-- +goose StatementBegin
CREATE TYPE non_content_expiry_kind AS ENUM (
    'JOB_METADATA', 'JOB_FINANCIAL', 'ORGANIZATION_FINANCIAL'
);
CREATE TYPE non_content_expiry_state AS ENUM ('PENDING', 'CLAIMED', 'EXPIRED');

CREATE TABLE non_content_job_roots (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    cancellation_id uuid,
    artifact_set_id uuid,
    charge_id uuid,
    invoice_requested_event_id uuid,
    terminal_at timestamptz,
    metadata_expires_at timestamptz,
    financial_expires_at timestamptz,
    metadata_expired_at timestamptz,
    financial_expired_at timestamptz,
    created_at timestamptz NOT NULL,
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, id, cancellation_id),
    UNIQUE (organization_id, project_id, id, artifact_set_id),
    UNIQUE (organization_id, project_id, id, charge_id),
    UNIQUE (organization_id, project_id, id, invoice_requested_event_id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    CHECK (
        (terminal_at IS NULL
            AND metadata_expires_at IS NULL
            AND financial_expires_at IS NULL)
        OR (terminal_at IS NOT NULL
            AND metadata_expires_at > terminal_at
            AND financial_expires_at > metadata_expires_at)
    ),
    CHECK (metadata_expired_at IS NULL OR metadata_expired_at >= metadata_expires_at),
    CHECK (financial_expired_at IS NULL OR financial_expired_at >= financial_expires_at)
);

CREATE TABLE non_content_attempt_roots (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, id, job_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id)
);

CREATE TABLE non_content_expiry_candidates (
    kind non_content_expiry_kind NOT NULL,
    source_id uuid NOT NULL,
    record_class legal_hold_record_class NOT NULL,
    organization_id uuid NOT NULL,
    project_id uuid,
    job_id uuid,
    expires_at timestamptz NOT NULL,
    state non_content_expiry_state NOT NULL DEFAULT 'PENDING',
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL,
    claimed_by text,
    claim_id uuid,
    claim_expires_at timestamptz,
    expired_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (kind, source_id),
    CHECK (
        (kind IN ('JOB_METADATA', 'JOB_FINANCIAL')
            AND project_id IS NOT NULL AND job_id = source_id)
        OR (kind = 'ORGANIZATION_FINANCIAL'
            AND record_class = 'FINANCIAL'
            AND project_id IS NULL AND job_id IS NULL)
    ),
    CHECK (
        (kind = 'JOB_METADATA' AND record_class = 'METADATA')
        OR (kind <> 'JOB_METADATA' AND record_class = 'FINANCIAL')
    ),
    CHECK (next_attempt_at >= expires_at),
    CHECK (
        (state = 'PENDING'
            AND claimed_by IS NULL AND claim_id IS NULL
            AND claim_expires_at IS NULL AND expired_at IS NULL)
        OR (state = 'CLAIMED'
            AND length(claimed_by) BETWEEN 1 AND 200
            AND claim_id IS NOT NULL AND claim_expires_at IS NOT NULL
            AND expired_at IS NULL)
        OR (state = 'EXPIRED'
            AND claimed_by IS NULL AND claim_id IS NULL
            AND claim_expires_at IS NULL AND expired_at >= expires_at)
    )
);

CREATE INDEX non_content_expiry_candidates_ready_idx
    ON non_content_expiry_candidates (
        next_attempt_at, claim_expires_at, expires_at, kind, source_id
    )
    WHERE state <> 'EXPIRED';

CREATE TABLE non_content_expiry_receipts (
    id uuid PRIMARY KEY,
    kind non_content_expiry_kind NOT NULL,
    source_id uuid NOT NULL,
    record_class legal_hold_record_class NOT NULL,
    organization_id uuid NOT NULL,
    project_id uuid,
    job_id uuid,
    scheduled_at timestamptz NOT NULL,
    expired_at timestamptz NOT NULL CHECK (expired_at >= scheduled_at),
    deleted_job_count integer NOT NULL DEFAULT 0 CHECK (deleted_job_count BETWEEN 0 AND 1),
    deleted_attempt_count integer NOT NULL DEFAULT 0 CHECK (deleted_attempt_count >= 0),
    deleted_credit_reservation_count integer NOT NULL DEFAULT 0
        CHECK (deleted_credit_reservation_count BETWEEN 0 AND 1),
    deleted_charge_count integer NOT NULL DEFAULT 0 CHECK (deleted_charge_count BETWEEN 0 AND 1),
    deleted_invoice_export_count integer NOT NULL DEFAULT 0
        CHECK (deleted_invoice_export_count BETWEEN 0 AND 1),
    deleted_invoice_receipt_count integer NOT NULL DEFAULT 0
        CHECK (deleted_invoice_receipt_count BETWEEN 0 AND 1),
    deleted_reconciliation_count integer NOT NULL DEFAULT 0
        CHECK (deleted_reconciliation_count BETWEEN 0 AND 1),
    UNIQUE (kind, source_id),
    CHECK (
        (kind IN ('JOB_METADATA', 'JOB_FINANCIAL')
            AND project_id IS NOT NULL AND job_id = source_id)
        OR (kind = 'ORGANIZATION_FINANCIAL'
            AND project_id IS NULL AND job_id IS NULL)
    )
);

ALTER TABLE non_content_job_roots ENABLE ROW LEVEL SECURITY;
ALTER TABLE non_content_job_roots FORCE ROW LEVEL SECURITY;
ALTER TABLE non_content_attempt_roots ENABLE ROW LEVEL SECURITY;
ALTER TABLE non_content_attempt_roots FORCE ROW LEVEL SECURITY;
ALTER TABLE non_content_expiry_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE non_content_expiry_candidates FORCE ROW LEVEL SECURITY;
ALTER TABLE non_content_expiry_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE non_content_expiry_receipts FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_private.capture_non_content_job_root() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO public.non_content_job_roots (
        id, organization_id, project_id, created_at
    ) VALUES (
        NEW.id, NEW.organization_id, NEW.project_id, NEW.created_at
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_non_content_job_root() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_non_content_job_root()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER jobs_capture_non_content_root
AFTER INSERT ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_non_content_job_root();

CREATE FUNCTION vela_private.capture_non_content_attempt_root() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO public.non_content_attempt_roots (
        id, organization_id, project_id, job_id, created_at
    ) VALUES (
        NEW.id, NEW.organization_id, NEW.project_id, NEW.job_id, NEW.created_at
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_non_content_attempt_root() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_non_content_attempt_root()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER attempts_capture_non_content_root
AFTER INSERT ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_non_content_attempt_root();

INSERT INTO non_content_job_roots (
    id, organization_id, project_id, created_at
)
SELECT id, organization_id, project_id, created_at
FROM jobs;

INSERT INTO non_content_attempt_roots (
    id, organization_id, project_id, job_id, created_at
)
SELECT id, organization_id, project_id, job_id, created_at
FROM attempts;

CREATE FUNCTION vela_enforce_non_content_job_root_transition() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_changed boolean := false;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'non-content Job root is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
    END IF;
    IF ROW(NEW.id, NEW.organization_id, NEW.project_id, NEW.created_at)
        IS DISTINCT FROM
       ROW(OLD.id, OLD.organization_id, OLD.project_id, OLD.created_at)
    THEN
        RAISE EXCEPTION 'non-content Job root identity is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
    END IF;
    IF ROW(NEW.terminal_at, NEW.metadata_expires_at, NEW.financial_expires_at)
        IS DISTINCT FROM
       ROW(OLD.terminal_at, OLD.metadata_expires_at, OLD.financial_expires_at)
    THEN
        IF OLD.terminal_at IS NOT NULL
            OR NEW.terminal_at IS NULL
            OR NEW.metadata_expires_at IS NULL
            OR NEW.financial_expires_at IS NULL
        THEN
            RAISE EXCEPTION 'non-content Job root terminal clock is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
        END IF;
        v_changed := true;
    END IF;
    IF ROW(NEW.cancellation_id, NEW.artifact_set_id, NEW.charge_id)
        IS DISTINCT FROM
       ROW(OLD.cancellation_id, OLD.artifact_set_id, OLD.charge_id)
    THEN
        IF OLD.cancellation_id IS NOT NULL
            OR OLD.artifact_set_id IS NOT NULL
            OR OLD.charge_id IS NOT NULL
            OR NEW.charge_id IS NULL
            OR num_nonnulls(NEW.cancellation_id, NEW.artifact_set_id) <> 1
        THEN
            RAISE EXCEPTION 'non-content Job root Charge linkage is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
        END IF;
        v_changed := true;
    END IF;
    IF NEW.invoice_requested_event_id IS DISTINCT FROM OLD.invoice_requested_event_id THEN
        IF OLD.invoice_requested_event_id IS NOT NULL OR NEW.invoice_requested_event_id IS NULL THEN
            RAISE EXCEPTION 'non-content Job root Invoice linkage is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
        END IF;
        v_changed := true;
    END IF;
    IF NEW.metadata_expired_at IS DISTINCT FROM OLD.metadata_expired_at THEN
        IF OLD.metadata_expired_at IS NOT NULL OR NEW.metadata_expired_at IS NULL THEN
            RAISE EXCEPTION 'non-content Job root Metadata expiry is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
        END IF;
        v_changed := true;
    END IF;
    IF NEW.financial_expired_at IS DISTINCT FROM OLD.financial_expired_at THEN
        IF OLD.financial_expired_at IS NOT NULL OR NEW.financial_expired_at IS NULL THEN
            RAISE EXCEPTION 'non-content Job root Financial expiry is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
        END IF;
        v_changed := true;
    END IF;
    IF NOT v_changed THEN
        RAISE EXCEPTION 'non-content Job root is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'non_content_job_root_is_immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_non_content_job_root_transition() FROM PUBLIC;
ALTER FUNCTION vela_enforce_non_content_job_root_transition()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER non_content_job_roots_state_machine
BEFORE UPDATE OR DELETE ON non_content_job_roots
FOR EACH ROW EXECUTE FUNCTION vela_enforce_non_content_job_root_transition();

CREATE FUNCTION vela_reject_non_content_attempt_root_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'non-content Attempt root is immutable'
        USING ERRCODE = '55000', CONSTRAINT = 'non_content_attempt_root_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_non_content_attempt_root_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_non_content_attempt_root_mutation()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER non_content_attempt_roots_immutable
BEFORE UPDATE OR DELETE ON non_content_attempt_roots
FOR EACH ROW EXECUTE FUNCTION vela_reject_non_content_attempt_root_mutation();

CREATE FUNCTION vela_private.capture_non_content_terminal_candidates() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_terminal_times timestamptz[];
    v_terminal_at timestamptz;
BEGIN
    IF NEW.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED') THEN
        RETURN NULL;
    END IF;
    SELECT array_agg(event.occurred_at ORDER BY event.event_id)
    INTO v_terminal_times
    FROM public.outbox_events AS event
    WHERE event.organization_id = NEW.organization_id
      AND event.project_id = NEW.project_id
      AND event.aggregate_type = 'Job'
      AND event.aggregate_id = NEW.id
      AND event.aggregate_version = NEW.version
      AND event.event_type = CASE NEW.state
            WHEN 'SUCCEEDED' THEN 'job.succeeded'
            WHEN 'FAILED' THEN 'job.failed'
            WHEN 'CANCELED' THEN 'job.canceled'
          END;
    IF cardinality(v_terminal_times) IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'terminal Job requires exactly one canonical terminal event'
            USING ERRCODE = '55000',
                CONSTRAINT = 'terminal_job_requires_canonical_event';
    END IF;
    v_terminal_at := v_terminal_times[1];
    UPDATE public.non_content_job_roots
    SET terminal_at = v_terminal_at,
        metadata_expires_at = v_terminal_at + make_interval(days => NEW.retention_metadata_days),
        financial_expires_at = v_terminal_at + make_interval(days => NEW.retention_financial_days)
    WHERE id = NEW.id AND terminal_at IS NULL;

    INSERT INTO public.non_content_expiry_candidates (
        kind, source_id, record_class, organization_id, project_id, job_id,
        expires_at, next_attempt_at
    ) VALUES
        ('JOB_METADATA', NEW.id, 'METADATA', NEW.organization_id, NEW.project_id,
            NEW.id, v_terminal_at + make_interval(days => NEW.retention_metadata_days),
            v_terminal_at + make_interval(days => NEW.retention_metadata_days)),
        ('JOB_FINANCIAL', NEW.id, 'FINANCIAL', NEW.organization_id, NEW.project_id,
            NEW.id, v_terminal_at + make_interval(days => NEW.retention_financial_days),
            v_terminal_at + make_interval(days => NEW.retention_financial_days))
    ON CONFLICT (kind, source_id) DO NOTHING;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_non_content_terminal_candidates() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_non_content_terminal_candidates()
    OWNER TO vela_non_content_expiry_owner;
CREATE CONSTRAINT TRIGGER jobs_capture_non_content_terminal_candidates
AFTER INSERT OR UPDATE ON jobs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_non_content_terminal_candidates();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM jobs AS job
        LEFT JOIN LATERAL (
            SELECT count(*) AS event_count
            FROM outbox_events AS event
            WHERE event.organization_id = job.organization_id
              AND event.project_id = job.project_id
              AND event.aggregate_type = 'Job'
              AND event.aggregate_id = job.id
              AND event.aggregate_version = job.version
              AND event.event_type = CASE job.state
                    WHEN 'SUCCEEDED' THEN 'job.succeeded'
                    WHEN 'FAILED' THEN 'job.failed'
                    WHEN 'CANCELED' THEN 'job.canceled'
                  END
        ) AS terminal ON true
        WHERE job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
          AND terminal.event_count IS DISTINCT FROM 1
    ) THEN
        RAISE EXCEPTION 'terminal Job requires exactly one canonical terminal event'
            USING ERRCODE = '55000',
                CONSTRAINT = 'terminal_job_requires_canonical_event';
    END IF;
END
$$;

WITH terminal_jobs AS (
    SELECT job.*, terminal.occurred_at AS terminal_at
    FROM jobs AS job
    JOIN LATERAL (
        SELECT event.occurred_at
        FROM outbox_events AS event
        WHERE event.organization_id = job.organization_id
          AND event.project_id = job.project_id
          AND event.aggregate_type = 'Job'
          AND event.aggregate_id = job.id
          AND event.aggregate_version = job.version
          AND event.event_type = CASE job.state
                WHEN 'SUCCEEDED' THEN 'job.succeeded'
                WHEN 'FAILED' THEN 'job.failed'
                WHEN 'CANCELED' THEN 'job.canceled'
              END
        ORDER BY event.event_id
    ) AS terminal ON job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
)
UPDATE non_content_job_roots AS root
SET terminal_at = terminal.terminal_at,
    metadata_expires_at = terminal.terminal_at
        + make_interval(days => terminal.retention_metadata_days),
    financial_expires_at = terminal.terminal_at
        + make_interval(days => terminal.retention_financial_days)
FROM terminal_jobs AS terminal
WHERE root.id = terminal.id;

INSERT INTO non_content_expiry_candidates (
    kind, source_id, record_class, organization_id, project_id, job_id,
    expires_at, next_attempt_at
)
SELECT 'JOB_METADATA'::non_content_expiry_kind, root.id,
    'METADATA'::legal_hold_record_class, root.organization_id, root.project_id,
    root.id, root.metadata_expires_at, root.metadata_expires_at
FROM non_content_job_roots AS root
WHERE root.terminal_at IS NOT NULL
UNION ALL
SELECT 'JOB_FINANCIAL'::non_content_expiry_kind, root.id,
    'FINANCIAL'::legal_hold_record_class, root.organization_id, root.project_id,
    root.id, root.financial_expires_at, root.financial_expires_at
FROM non_content_job_roots AS root
WHERE root.terminal_at IS NOT NULL;

CREATE FUNCTION vela_private.capture_reconciliation_expiry_candidate() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO public.non_content_expiry_candidates (
        kind, source_id, record_class, organization_id, expires_at, next_attempt_at
    ) VALUES (
        'ORGANIZATION_FINANCIAL', NEW.id, 'FINANCIAL', NEW.organization_id,
        NEW.posted_at + make_interval(days => 2557),
        NEW.posted_at + make_interval(days => 2557)
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_reconciliation_expiry_candidate() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_reconciliation_expiry_candidate()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER finance_reconciliation_capture_expiry_candidate
AFTER INSERT ON finance_reconciliation_records
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_reconciliation_expiry_candidate();

INSERT INTO non_content_expiry_candidates (
    kind, source_id, record_class, organization_id, expires_at, next_attempt_at
)
SELECT 'ORGANIZATION_FINANCIAL'::non_content_expiry_kind, id,
    'FINANCIAL'::legal_hold_record_class, organization_id,
    posted_at + make_interval(days => 2557),
    posted_at + make_interval(days => 2557)
FROM finance_reconciliation_records;

UPDATE non_content_job_roots AS root
SET cancellation_id = charge.cancellation_id,
    artifact_set_id = charge.artifact_set_id,
    charge_id = charge.id
FROM charges AS charge
WHERE root.organization_id = charge.organization_id
  AND root.project_id = charge.project_id
  AND root.id = charge.job_id;

UPDATE non_content_job_roots AS root
SET invoice_requested_event_id = export.requested_event_id
FROM invoice_exports AS export
WHERE root.organization_id = export.organization_id
  AND root.project_id = export.project_id
  AND root.id = export.job_id;

CREATE FUNCTION vela_private.capture_non_content_charge_link() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.cancellation_id IS NOT NULL THEN
        PERFORM 1
        FROM public.job_cancellation_decisions AS decision
        WHERE decision.organization_id = NEW.organization_id
          AND decision.project_id = NEW.project_id
          AND decision.job_id = NEW.job_id
          AND decision.id = NEW.cancellation_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Charge requires the exact cancellation decision'
                USING ERRCODE = '23503',
                    CONSTRAINT = 'charges_organization_id_project_id_job_id_cancellation_id_fkey';
        END IF;
    END IF;
    IF NEW.artifact_set_id IS NOT NULL THEN
        PERFORM 1
        FROM public.artifact_sets AS artifact_set
        WHERE artifact_set.organization_id = NEW.organization_id
          AND artifact_set.project_id = NEW.project_id
          AND artifact_set.job_id = NEW.job_id
          AND artifact_set.id = NEW.artifact_set_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Charge requires the exact ArtifactSet'
                USING ERRCODE = '23503', CONSTRAINT = 'charges_artifact_set_identity';
        END IF;
    END IF;
    UPDATE public.non_content_job_roots
    SET cancellation_id = NEW.cancellation_id,
        artifact_set_id = NEW.artifact_set_id,
        charge_id = NEW.id
    WHERE organization_id = NEW.organization_id
      AND project_id = NEW.project_id AND id = NEW.job_id
      AND charge_id IS NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'non-content Job root is missing or already charged'
            USING ERRCODE = '23514',
                CONSTRAINT = 'charge_requires_non_content_job_root';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_non_content_charge_link() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_non_content_charge_link()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER charges_capture_non_content_link
BEFORE INSERT ON charges
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_non_content_charge_link();

CREATE FUNCTION vela_private.capture_non_content_invoice_link() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.outbox_events AS event
    WHERE event.organization_id = NEW.organization_id
      AND event.project_id = NEW.project_id
      AND event.aggregate_id = NEW.job_id
      AND event.event_id = NEW.requested_event_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invoice export requires the exact requested Outbox event'
            USING ERRCODE = '23503', CONSTRAINT = 'invoice_exports_requested_event_id_fkey';
    END IF;
    UPDATE public.non_content_job_roots
    SET invoice_requested_event_id = NEW.requested_event_id
    WHERE organization_id = NEW.organization_id
      AND project_id = NEW.project_id AND id = NEW.job_id
      AND charge_id = NEW.charge_id AND invoice_requested_event_id IS NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invoice export requires the exact non-content Job root'
            USING ERRCODE = '23514',
                CONSTRAINT = 'invoice_export_requires_non_content_job_root';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_non_content_invoice_link() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_non_content_invoice_link()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER invoice_exports_capture_non_content_link
BEFORE INSERT ON invoice_exports
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_non_content_invoice_link();

CREATE FUNCTION vela_private.validate_non_content_financial_amount() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_amount bigint;
    v_currency text;
BEGIN
    SELECT job.pricing_quoted_amount_minor, job.pricing_currency
    INTO v_amount, v_currency
    FROM public.jobs AS job
    WHERE job.organization_id = NEW.organization_id
      AND job.project_id = NEW.project_id AND job.id = NEW.job_id;
    IF NOT FOUND OR NEW.amount_minor IS DISTINCT FROM v_amount
        OR NEW.currency IS DISTINCT FROM v_currency
    THEN
        RAISE EXCEPTION 'financial record does not match the immutable Job quote'
            USING ERRCODE = '23514',
                CONSTRAINT = 'financial_record_requires_job_quote';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.validate_non_content_financial_amount() FROM PUBLIC;
ALTER FUNCTION vela_private.validate_non_content_financial_amount()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER credit_reservations_validate_non_content_amount
BEFORE INSERT ON credit_reservations
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_non_content_financial_amount();
CREATE TRIGGER charges_validate_non_content_amount
BEFORE INSERT ON charges
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_non_content_financial_amount();

ALTER TABLE artifact_sets
    DROP CONSTRAINT artifact_sets_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT artifact_sets_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE attempt_progress
    DROP CONSTRAINT attempt_progress_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT attempt_progress_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE break_glass_requests
    DROP CONSTRAINT break_glass_requests_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT break_glass_requests_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE charges
    DROP CONSTRAINT charges_organization_id_project_id_job_id_amount_minor_cur_fkey,
    DROP CONSTRAINT charges_organization_id_project_id_job_id_cancellation_id_fkey,
    DROP CONSTRAINT charges_artifact_set_identity,
    ADD CONSTRAINT charges_non_content_job_root
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id),
    ADD CONSTRAINT charges_non_content_cancellation_root
        FOREIGN KEY (organization_id, project_id, job_id, cancellation_id)
        REFERENCES non_content_job_roots(
            organization_id, project_id, id, cancellation_id
        ),
    ADD CONSTRAINT charges_non_content_artifact_root
        FOREIGN KEY (organization_id, project_id, job_id, artifact_set_id)
        REFERENCES non_content_job_roots(
            organization_id, project_id, id, artifact_set_id
        );
ALTER TABLE content_deletion_requests
    DROP CONSTRAINT content_deletion_requests_organization_id_project_id_job_i_fkey,
    ADD CONSTRAINT content_deletion_requests_organization_id_project_id_job_i_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE credit_reservations
    DROP CONSTRAINT credit_reservations_organization_id_project_id_job_id_amou_fkey,
    ADD CONSTRAINT credit_reservations_non_content_job_root
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE debug_dump_authorizations
    DROP CONSTRAINT debug_dump_authorizations_organization_id_project_id_job_i_fkey,
    ADD CONSTRAINT debug_dump_authorizations_organization_id_project_id_job_i_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE execution_failure_decisions
    DROP CONSTRAINT execution_failure_decisions_organization_id_project_id_job_fkey,
    ADD CONSTRAINT execution_failure_decisions_organization_id_project_id_job_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE execution_retry_evidence
    DROP CONSTRAINT execution_retry_evidence_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT execution_retry_evidence_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE idempotency_results
    DROP CONSTRAINT idempotency_results_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT idempotency_results_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE inbox_receipts
    DROP CONSTRAINT inbox_receipts_organization_id_project_id_aggregate_id_fkey,
    ADD CONSTRAINT inbox_receipts_organization_id_project_id_aggregate_id_fkey
        FOREIGN KEY (organization_id, project_id, aggregate_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE job_cancellation_decisions
    DROP CONSTRAINT job_cancellation_decisions_organization_id_project_id_job__fkey,
    ADD CONSTRAINT job_cancellation_decisions_organization_id_project_id_job__fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE job_runtime_predictions
    DROP CONSTRAINT job_runtime_predictions_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT job_runtime_predictions_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE legal_holds
    DROP CONSTRAINT legal_holds_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT legal_holds_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_organization_id_project_id_aggregate_id_fkey,
    ADD CONSTRAINT outbox_events_organization_id_project_id_aggregate_id_fkey
        FOREIGN KEY (organization_id, project_id, aggregate_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE profile_certification_circuit_openings
    DROP CONSTRAINT profile_certification_circuit_organization_id_project_id_t_fkey,
    ADD CONSTRAINT profile_certification_circuit_organization_id_project_id_t_fkey
        FOREIGN KEY (organization_id, project_id, triggering_job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE retry_runtime_states
    DROP CONSTRAINT retry_runtime_states_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT retry_runtime_states_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);
ALTER TABLE scheduler_dispatch_intents
    DROP CONSTRAINT scheduler_dispatch_intents_organization_id_project_id_job__fkey,
    ADD CONSTRAINT scheduler_dispatch_intents_organization_id_project_id_job__fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES non_content_job_roots(organization_id, project_id, id);

CREATE FUNCTION vela_private.require_live_artifact_attempt_identity(
    p_organization_id uuid,
    p_project_id uuid,
    p_attempt_id uuid,
    p_job_id uuid,
    p_fence bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.attempts AS attempt
    WHERE attempt.organization_id = p_organization_id
      AND attempt.project_id = p_project_id
      AND attempt.id = p_attempt_id
      AND attempt.job_id = p_job_id
      AND attempt.fence = p_fence
    FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Artifact row does not match the live Attempt identity'
            USING ERRCODE = '23503',
                CONSTRAINT = 'artifact_live_attempt_identity';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_live_artifact_attempt_identity(
    uuid, uuid, uuid, uuid, bigint
) FROM PUBLIC;
ALTER FUNCTION vela_private.require_live_artifact_attempt_identity(
    uuid, uuid, uuid, uuid, bigint
) OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_private.require_live_lease_attempt_identity(
    p_organization_id uuid,
    p_project_id uuid,
    p_attempt_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_fence bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.attempts AS attempt
    WHERE attempt.organization_id = p_organization_id
      AND attempt.project_id = p_project_id
      AND attempt.id = p_attempt_id
      AND attempt.worker_id = p_worker_id
      AND attempt.worker_epoch = p_worker_epoch
      AND attempt.fence = p_fence
    FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Lease row does not match the live Attempt identity'
            USING ERRCODE = '23503',
                CONSTRAINT = 'lease_live_attempt_identity';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_live_lease_attempt_identity(
    uuid, uuid, uuid, uuid, bigint, bigint
) FROM PUBLIC;
ALTER FUNCTION vela_private.require_live_lease_attempt_identity(
    uuid, uuid, uuid, uuid, bigint, bigint
) OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_private.require_live_full_attempt_identity(
    p_organization_id uuid,
    p_project_id uuid,
    p_attempt_id uuid,
    p_job_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_fence bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.attempts AS attempt
    WHERE attempt.organization_id = p_organization_id
      AND attempt.project_id = p_project_id
      AND attempt.id = p_attempt_id
      AND attempt.job_id = p_job_id
      AND attempt.worker_id = p_worker_id
      AND attempt.worker_epoch = p_worker_epoch
      AND attempt.fence = p_fence
    FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'dependent row does not match the live Attempt identity'
            USING ERRCODE = '23503',
                CONSTRAINT = 'full_live_attempt_identity';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_live_full_attempt_identity(
    uuid, uuid, uuid, uuid, uuid, bigint, bigint
) FROM PUBLIC;
ALTER FUNCTION vela_private.require_live_full_attempt_identity(
    uuid, uuid, uuid, uuid, uuid, bigint, bigint
) OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_private.validate_live_artifact_attempt_identity() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    PERFORM require_live_artifact_attempt_identity(
        NEW.organization_id, NEW.project_id, NEW.attempt_id, NEW.job_id,
        NEW.attempt_fence
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.validate_live_artifact_attempt_identity() FROM PUBLIC;
ALTER FUNCTION vela_private.validate_live_artifact_attempt_identity()
    OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_private.validate_live_lease_attempt_identity() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    PERFORM require_live_lease_attempt_identity(
        NEW.organization_id, NEW.project_id, NEW.attempt_id, NEW.worker_id,
        NEW.worker_epoch, NEW.fence
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.validate_live_lease_attempt_identity() FROM PUBLIC;
ALTER FUNCTION vela_private.validate_live_lease_attempt_identity()
    OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_private.validate_live_progress_attempt_identity() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    PERFORM require_live_full_attempt_identity(
        NEW.organization_id, NEW.project_id, NEW.attempt_id, NEW.job_id,
        NEW.worker_id, NEW.worker_epoch, NEW.fence
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.validate_live_progress_attempt_identity() FROM PUBLIC;
ALTER FUNCTION vela_private.validate_live_progress_attempt_identity()
    OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_private.validate_live_evidence_attempt_identity() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    IF NEW.attempt_id IS NOT NULL THEN
        PERFORM require_live_full_attempt_identity(
            NEW.organization_id, NEW.project_id, NEW.attempt_id, NEW.job_id,
            NEW.worker_id, NEW.worker_epoch, NEW.attempt_fence
        );
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.validate_live_evidence_attempt_identity() FROM PUBLIC;
ALTER FUNCTION vela_private.validate_live_evidence_attempt_identity()
    OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_private.validate_live_profile_circuit_attempt_identity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    PERFORM require_live_full_attempt_identity(
        NEW.organization_id, NEW.project_id, NEW.triggering_attempt_id,
        NEW.triggering_job_id, NEW.triggering_worker_id,
        NEW.triggering_worker_epoch, NEW.triggering_attempt_fence
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.validate_live_profile_circuit_attempt_identity()
    FROM PUBLIC;
ALTER FUNCTION vela_private.validate_live_profile_circuit_attempt_identity()
    OWNER TO vela_non_content_expiry_owner;

ALTER TABLE artifact_sets
    DROP CONSTRAINT artifact_sets_organization_id_project_id_attempt_id_job_id_fkey,
    ADD CONSTRAINT artifact_sets_organization_id_project_id_attempt_id_job_id_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id, job_id
        );
CREATE TRIGGER artifact_sets_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, attempt_id, job_id, attempt_fence
ON artifact_sets
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_artifact_attempt_identity();
ALTER TABLE artifacts
    DROP CONSTRAINT artifacts_organization_id_project_id_attempt_id_job_id_att_fkey,
    ADD CONSTRAINT artifacts_organization_id_project_id_attempt_id_job_id_att_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id, job_id
        );
CREATE TRIGGER artifacts_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, attempt_id, job_id, attempt_fence
ON artifacts
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_artifact_attempt_identity();
ALTER TABLE attempt_leases
    DROP CONSTRAINT attempt_leases_organization_id_project_id_attempt_id_worke_fkey,
    ADD CONSTRAINT attempt_leases_organization_id_project_id_attempt_id_worke_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id
        );
CREATE TRIGGER attempt_leases_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, attempt_id,
    worker_id, worker_epoch, fence
ON attempt_leases
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_lease_attempt_identity();
ALTER TABLE attempt_progress
    DROP CONSTRAINT attempt_progress_attempt_identity_fkey,
    ADD CONSTRAINT attempt_progress_attempt_identity_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id, job_id
        );
CREATE TRIGGER attempt_progress_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, attempt_id, job_id,
    worker_id, worker_epoch, fence
ON attempt_progress
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_progress_attempt_identity();
ALTER TABLE cancellation_stop_receipts
    DROP CONSTRAINT cancellation_stop_receipts_organization_id_project_id_atte_fkey,
    ADD CONSTRAINT cancellation_stop_receipts_organization_id_project_id_atte_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id, job_id
        );
CREATE TRIGGER cancellation_stop_receipts_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, attempt_id, job_id,
    worker_id, worker_epoch, attempt_fence
ON cancellation_stop_receipts
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_evidence_attempt_identity();
ALTER TABLE debug_dumps
    DROP CONSTRAINT debug_dumps_organization_id_project_id_attempt_id_fkey,
    ADD CONSTRAINT debug_dumps_organization_id_project_id_attempt_id_fkey
        FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES non_content_attempt_roots(organization_id, project_id, id);
ALTER TABLE execution_failure_decisions
    DROP CONSTRAINT execution_failure_decisions_organization_id_project_id_att_fkey,
    ADD CONSTRAINT execution_failure_decisions_organization_id_project_id_att_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id, job_id
        );
CREATE TRIGGER execution_failure_decisions_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, attempt_id, job_id,
    worker_id, worker_epoch, attempt_fence
ON execution_failure_decisions
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_evidence_attempt_identity();
ALTER TABLE job_cancellation_decisions
    DROP CONSTRAINT job_cancellation_decisions_organization_id_project_id_atte_fkey,
    ADD CONSTRAINT job_cancellation_decisions_organization_id_project_id_atte_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id, job_id
        );
CREATE TRIGGER job_cancellation_decisions_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, attempt_id, job_id,
    worker_id, worker_epoch, attempt_fence
ON job_cancellation_decisions
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_evidence_attempt_identity();
ALTER TABLE profile_certification_circuit_openings
    DROP CONSTRAINT profile_certification_circui_organization_id_project_id_t_fkey1,
    ADD CONSTRAINT profile_certification_circui_organization_id_project_id_t_fkey1
        FOREIGN KEY (
            organization_id, project_id, triggering_attempt_id, triggering_job_id
        ) REFERENCES non_content_attempt_roots(
            organization_id, project_id, id, job_id
        );
CREATE TRIGGER profile_circuit_openings_validate_live_attempt_identity
BEFORE INSERT OR UPDATE OF organization_id, project_id, triggering_attempt_id,
    triggering_job_id, triggering_worker_id, triggering_worker_epoch,
    triggering_attempt_fence
ON profile_certification_circuit_openings
FOR EACH ROW EXECUTE FUNCTION vela_private.validate_live_profile_circuit_attempt_identity();

ALTER TABLE visible_completions
    DROP CONSTRAINT visible_completions_organization_id_project_id_job_id_char_fkey,
    ADD CONSTRAINT visible_completions_non_content_charge_root
        FOREIGN KEY (organization_id, project_id, job_id, charge_id)
        REFERENCES non_content_job_roots(
            organization_id, project_id, id, charge_id
        );
ALTER TABLE invoice_exports
    DROP CONSTRAINT invoice_exports_requested_event_id_fkey,
    ADD CONSTRAINT invoice_exports_non_content_event_root
        FOREIGN KEY (organization_id, project_id, job_id, requested_event_id)
        REFERENCES non_content_job_roots(
            organization_id, project_id, id, invoice_requested_event_id
        );

CREATE FUNCTION vela_private.lock_non_content_hold_target() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1 FROM public.customer_organizations
    WHERE id = NEW.organization_id FOR UPDATE;
    IF NEW.project_id IS NOT NULL THEN
        PERFORM 1 FROM public.projects
        WHERE organization_id = NEW.organization_id AND id = NEW.project_id
        FOR UPDATE;
    END IF;
    IF NEW.job_id IS NOT NULL THEN
        PERFORM 1 FROM public.non_content_job_roots
        WHERE organization_id = NEW.organization_id
          AND project_id = NEW.project_id AND id = NEW.job_id
        FOR UPDATE;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.lock_non_content_hold_target() FROM PUBLIC;
ALTER FUNCTION vela_private.lock_non_content_hold_target()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER legal_holds_lock_non_content_target
BEFORE INSERT ON legal_holds
FOR EACH ROW EXECUTE FUNCTION vela_private.lock_non_content_hold_target();

CREATE OR REPLACE FUNCTION vela_private.lock_active_non_content_legal_holds(
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_record_class legal_hold_record_class
) RETURNS TABLE (hold_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_organization_id IS NULL OR p_record_class IS NULL
        OR (p_project_id IS NULL AND p_job_id IS NOT NULL)
    THEN
        RAISE EXCEPTION 'Legal Hold target is invalid' USING ERRCODE = '22023';
    END IF;
    PERFORM 1 FROM public.customer_organizations
    WHERE id = p_organization_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Legal Hold Organization is not found' USING ERRCODE = 'P0002';
    END IF;
    IF p_project_id IS NOT NULL THEN
        PERFORM 1 FROM public.projects
        WHERE organization_id = p_organization_id AND id = p_project_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold Project is not found' USING ERRCODE = 'P0002';
        END IF;
    END IF;
    IF p_job_id IS NOT NULL THEN
        PERFORM 1 FROM public.non_content_job_roots
        WHERE organization_id = p_organization_id
          AND project_id = p_project_id AND id = p_job_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold descendant Job is not found' USING ERRCODE = 'P0002';
        END IF;
    END IF;
    RETURN QUERY
    SELECT hold.id
    FROM public.legal_holds AS hold
    WHERE hold.organization_id = p_organization_id
      AND p_record_class = ANY(hold.record_classes)
      AND hold.state = 'ACTIVE'
      AND (
          hold.scope = 'ORGANIZATION'
          OR (p_project_id IS NOT NULL
              AND hold.scope = 'PROJECT' AND hold.project_id = p_project_id)
          OR (p_job_id IS NOT NULL
              AND hold.scope = 'JOB' AND hold.project_id = p_project_id
              AND hold.job_id = p_job_id)
      )
    ORDER BY hold.id
    FOR UPDATE;
END
$$;
ALTER FUNCTION vela_private.lock_active_non_content_legal_holds(
    uuid, uuid, uuid, legal_hold_record_class
) OWNER TO vela_non_content_expiry_owner;

CREATE OR REPLACE FUNCTION vela_apply_legal_hold_event(
    p_event_id uuid,
    p_idempotency_key text,
    p_source_sequence bigint,
    p_hold_id uuid,
    p_kind legal_hold_event_kind,
    p_scope legal_hold_scope,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_record_classes legal_hold_record_class[],
    p_reason_code text,
    p_external_reference text,
    p_effective_at timestamptz
) RETURNS TABLE (
    event_id uuid,
    replayed boolean,
    hold_id uuid,
    state legal_hold_state,
    scope legal_hold_scope,
    record_classes legal_hold_record_class[],
    recorded_at timestamptz,
    released_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_identity record;
    v_existing public.legal_hold_events%ROWTYPE;
    v_hold public.legal_holds%ROWTYPE;
    v_last_sequence bigint;
    v_recorded_at timestamptz;
BEGIN
    SELECT * INTO STRICT v_identity FROM vela_private.require_compliance_identity();
    IF p_event_id IS NULL OR p_hold_id IS NULL OR p_kind IS NULL
        OR p_source_sequence IS NULL OR p_source_sequence <= 0
        OR p_idempotency_key IS NULL OR length(p_idempotency_key) NOT BETWEEN 1 AND 500
        OR btrim(p_idempotency_key) <> p_idempotency_key
        OR p_idempotency_key ~ '[[:cntrl:]]'
        OR p_reason_code IS NULL OR p_reason_code !~ '^[A-Z][A-Z0-9_]{0,99}$'
        OR p_external_reference IS NULL
        OR length(p_external_reference) NOT BETWEEN 1 AND 500
        OR btrim(p_external_reference) <> p_external_reference
        OR p_external_reference ~ '[[:cntrl:]]'
        OR p_effective_at IS NULL
    THEN
        RAISE EXCEPTION 'invalid Legal Hold event input' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.compliance_event_cursors (principal_id)
    VALUES (v_identity.principal_id)
    ON CONFLICT (principal_id) DO NOTHING;
    SELECT cursor.last_sequence INTO STRICT v_last_sequence
    FROM public.compliance_event_cursors AS cursor
    WHERE cursor.principal_id = v_identity.principal_id
    FOR UPDATE;

    SELECT * INTO v_existing
    FROM public.legal_hold_events AS event
    WHERE event.principal_id = v_identity.principal_id
      AND event.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.source_sequence IS DISTINCT FROM p_source_sequence
            OR v_existing.hold_id IS DISTINCT FROM p_hold_id
            OR v_existing.kind IS DISTINCT FROM p_kind
            OR v_existing.scope IS DISTINCT FROM p_scope
            OR v_existing.organization_id IS DISTINCT FROM p_organization_id
            OR v_existing.project_id IS DISTINCT FROM p_project_id
            OR v_existing.job_id IS DISTINCT FROM p_job_id
            OR v_existing.record_classes IS DISTINCT FROM p_record_classes
            OR v_existing.reason_code IS DISTINCT FROM p_reason_code
            OR v_existing.external_reference IS DISTINCT FROM p_external_reference
            OR v_existing.effective_at IS DISTINCT FROM p_effective_at
        THEN
            RAISE EXCEPTION 'Legal Hold idempotency key conflicts with committed input'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_idempotency_conflict';
        END IF;
        SELECT * INTO STRICT v_hold FROM public.legal_holds WHERE id = p_hold_id;
        RETURN QUERY SELECT
            v_existing.id, true, v_hold.id, v_hold.state, v_hold.scope,
            v_hold.record_classes, v_existing.recorded_at, v_hold.released_at;
        RETURN;
    END IF;

    IF v_last_sequence = 9223372036854775807 OR p_source_sequence <> v_last_sequence + 1 THEN
        RAISE EXCEPTION 'Legal Hold source sequence is out of order'
            USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_sequence_out_of_order';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.legal_hold_events AS event
        WHERE event.principal_id = v_identity.principal_id
          AND event.external_reference = p_external_reference
    ) THEN
        RAISE EXCEPTION 'Legal Hold external reference conflicts with committed input'
            USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_external_reference_conflict';
    END IF;
    v_recorded_at := clock_timestamp();

    IF p_kind = 'HOLD_PLACED' THEN
        IF p_scope IS NULL OR p_organization_id IS NULL OR p_record_classes IS NULL
            OR NOT (
                (p_scope = 'ORGANIZATION' AND p_project_id IS NULL AND p_job_id IS NULL)
                OR (p_scope = 'PROJECT' AND p_project_id IS NOT NULL AND p_job_id IS NULL)
                OR (p_scope = 'JOB' AND p_project_id IS NOT NULL AND p_job_id IS NOT NULL)
            )
            OR NOT (
                p_record_classes = ARRAY['METADATA']::legal_hold_record_class[]
                OR p_record_classes = ARRAY['FINANCIAL']::legal_hold_record_class[]
                OR p_record_classes = ARRAY['METADATA', 'FINANCIAL']::legal_hold_record_class[]
            )
        THEN
            RAISE EXCEPTION 'invalid Legal Hold placement shape' USING ERRCODE = '22023';
        END IF;
        IF p_scope = 'ORGANIZATION' THEN
            PERFORM 1 FROM public.customer_organizations
            WHERE id = p_organization_id;
        ELSIF p_scope = 'PROJECT' THEN
            PERFORM 1 FROM public.projects
            WHERE organization_id = p_organization_id AND id = p_project_id;
        ELSE
            PERFORM 1 FROM public.non_content_job_roots
            WHERE organization_id = p_organization_id
              AND project_id = p_project_id AND id = p_job_id;
        END IF;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold target is not found' USING ERRCODE = 'P0002';
        END IF;
        INSERT INTO public.legal_holds (
            id, organization_id, project_id, job_id, scope, record_classes,
            placement_principal_id, placement_source_sequence,
            placement_reason_code, placement_external_reference,
            placement_effective_at, placed_at
        ) VALUES (
            p_hold_id, p_organization_id, p_project_id, p_job_id, p_scope,
            p_record_classes, v_identity.principal_id, p_source_sequence,
            p_reason_code, p_external_reference, p_effective_at, v_recorded_at
        );
    ELSIF p_kind = 'HOLD_RELEASED' THEN
        IF p_scope IS NOT NULL OR p_organization_id IS NOT NULL
            OR p_project_id IS NOT NULL OR p_job_id IS NOT NULL
            OR p_record_classes IS NOT NULL
        THEN
            RAISE EXCEPTION 'invalid Legal Hold release shape' USING ERRCODE = '22023';
        END IF;
        SELECT * INTO v_hold FROM public.legal_holds WHERE id = p_hold_id FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold is not found' USING ERRCODE = 'P0002';
        END IF;
        IF v_hold.state <> 'ACTIVE' THEN
            RAISE EXCEPTION 'Legal Hold is already released'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_already_released';
        END IF;
        IF p_effective_at < v_hold.placement_effective_at
            OR v_recorded_at < v_hold.placed_at
        THEN
            RAISE EXCEPTION 'Legal Hold release precedes placement'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_release_precedes_placement';
        END IF;
        UPDATE public.legal_holds
        SET state = 'RELEASED',
            release_principal_id = v_identity.principal_id,
            release_source_sequence = p_source_sequence,
            release_reason_code = p_reason_code,
            release_external_reference = p_external_reference,
            release_effective_at = p_effective_at,
            released_at = v_recorded_at,
            updated_at = v_recorded_at
        WHERE id = p_hold_id;
    ELSE
        RAISE EXCEPTION 'unsupported Legal Hold event kind' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.legal_hold_events (
        id, principal_id, idempotency_key, source_sequence, hold_id, kind,
        scope, organization_id, project_id, job_id, record_classes,
        reason_code, external_reference, effective_at, recorded_at
    ) VALUES (
        p_event_id, v_identity.principal_id, p_idempotency_key,
        p_source_sequence, p_hold_id, p_kind, p_scope, p_organization_id,
        p_project_id, p_job_id, p_record_classes, p_reason_code,
        p_external_reference, p_effective_at, v_recorded_at
    );
    UPDATE public.compliance_event_cursors
    SET last_sequence = p_source_sequence, updated_at = v_recorded_at
    WHERE principal_id = v_identity.principal_id;
    SELECT * INTO STRICT v_hold FROM public.legal_holds WHERE id = p_hold_id;
    RETURN QUERY SELECT
        p_event_id, false, v_hold.id, v_hold.state, v_hold.scope,
        v_hold.record_classes, v_recorded_at, v_hold.released_at;
END
$$;
GRANT SELECT ON non_content_job_roots TO vela_compliance_owner;
GRANT SELECT ON non_content_job_roots TO vela_internal;

CREATE FUNCTION vela_enforce_non_content_candidate_transition() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'non-content expiry candidates are durable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'non_content_expiry_candidate_is_durable';
    END IF;
    IF ROW(
        NEW.kind, NEW.source_id, NEW.record_class, NEW.organization_id,
        NEW.project_id, NEW.job_id, NEW.expires_at, NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.kind, OLD.source_id, OLD.record_class, OLD.organization_id,
        OLD.project_id, OLD.job_id, OLD.expires_at, OLD.created_at
    ) THEN
        RAISE EXCEPTION 'non-content expiry candidate identity is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'non_content_expiry_candidate_identity_is_immutable';
    END IF;
    IF OLD.state = 'EXPIRED' THEN
        RAISE EXCEPTION 'expired non-content candidate is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'non_content_expiry_candidate_is_terminal';
    END IF;
    IF NOT (
        (OLD.state = 'PENDING' AND NEW.state = 'CLAIMED')
        OR (OLD.state = 'CLAIMED' AND NEW.state IN ('PENDING', 'EXPIRED'))
        OR (OLD.state = 'CLAIMED' AND NEW.state = 'CLAIMED'
            AND OLD.claim_expires_at <= clock_timestamp())
    ) THEN
        RAISE EXCEPTION 'invalid non-content expiry candidate transition'
            USING ERRCODE = '55000',
                CONSTRAINT = 'non_content_expiry_candidate_transition';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_non_content_candidate_transition() FROM PUBLIC;
ALTER FUNCTION vela_enforce_non_content_candidate_transition()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER non_content_expiry_candidates_state_machine
BEFORE UPDATE OR DELETE ON non_content_expiry_candidates
FOR EACH ROW EXECUTE FUNCTION vela_enforce_non_content_candidate_transition();

CREATE FUNCTION vela_reject_non_content_expiry_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'non-content expiry receipts are immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'non_content_expiry_receipt_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_non_content_expiry_receipt_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_non_content_expiry_receipt_mutation()
    OWNER TO vela_non_content_expiry_owner;
CREATE TRIGGER non_content_expiry_receipts_immutable
BEFORE UPDATE OR DELETE ON non_content_expiry_receipts
FOR EACH ROW EXECUTE FUNCTION vela_reject_non_content_expiry_receipt_mutation();

CREATE OR REPLACE FUNCTION vela_reject_cancellation_history_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' AND TG_TABLE_NAME = 'charges'
        AND current_user = 'vela_non_content_expiry_owner'
        AND current_setting('vela.non_content_expiry_kind', true) = 'JOB_FINANCIAL'
        AND current_setting('vela.non_content_expiry_source_id', true) = OLD.job_id::text
    THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'cancellation decisions and Charges are immutable';
END
$$;

CREATE OR REPLACE FUNCTION vela_reject_invoice_export_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
        AND current_user = 'vela_non_content_expiry_owner'
        AND current_setting('vela.non_content_expiry_kind', true) = 'JOB_FINANCIAL'
        AND current_setting('vela.non_content_expiry_source_id', true) = OLD.job_id::text
    THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'Invoice export receipts are immutable';
END
$$;

CREATE OR REPLACE FUNCTION vela_enforce_invoice_export_transition() RETURNS trigger
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
        IF current_user = 'vela_non_content_expiry_owner'
            AND current_setting('vela.non_content_expiry_kind', true) = 'JOB_FINANCIAL'
            AND current_setting('vela.non_content_expiry_source_id', true) = OLD.job_id::text
        THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'Invoice export authority cannot be deleted';
    END IF;
    IF ROW(
        NEW.charge_id, NEW.organization_id, NEW.project_id, NEW.job_id,
        NEW.requested_event_id, NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.charge_id, OLD.organization_id, OLD.project_id, OLD.job_id,
        OLD.requested_event_id, OLD.created_at
    ) THEN
        RAISE EXCEPTION 'Invoice export identity is immutable';
    END IF;
    IF OLD.state = 'EXPORTED' THEN
        RAISE EXCEPTION 'exported Invoice authority is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'PENDING' AND NEW.state = 'CLAIMED')
        OR (OLD.state = 'CLAIMED' AND NEW.state IN ('PENDING', 'EXPORTED'))
        OR (OLD.state = 'CLAIMED' AND NEW.state = 'CLAIMED'
            AND OLD.claim_expires_at <= clock_timestamp())
    ) THEN
        RAISE EXCEPTION 'invalid Invoice export transition from % to %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION vela_reject_finance_reconciliation_record_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
        AND current_user = 'vela_non_content_expiry_owner'
        AND current_setting('vela.non_content_expiry_kind', true)
            = 'ORGANIZATION_FINANCIAL'
        AND current_setting('vela.non_content_expiry_source_id', true) = OLD.id::text
    THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'Finance Reconciliation records are immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'finance_reconciliation_record_is_immutable';
END
$$;

CREATE OR REPLACE FUNCTION vela_assert_job_required_children(p_job_id uuid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.jobs WHERE id = p_job_id)
        AND (
            (
                NOT EXISTS (
                    SELECT 1 FROM public.non_content_job_roots AS root
                    WHERE root.id = p_job_id AND root.financial_expired_at IS NOT NULL
                )
                AND (SELECT count(*) FROM public.credit_reservations
                     WHERE job_id = p_job_id) <> 1
            )
            OR (SELECT count(*) FROM public.retry_runtime_states
                WHERE job_id = p_job_id) <> 1
        )
    THEN
        RAISE EXCEPTION 'Job % must have required CreditReservation and RetryRuntimeState', p_job_id
            USING ERRCODE = '23514';
    END IF;
END
$$;

CREATE FUNCTION vela_claim_non_content_expiry(
    p_instance_id text,
    p_claim_id uuid,
    p_claim_ttl_seconds integer
) RETURNS TABLE (
    kind non_content_expiry_kind,
    source_id uuid,
    record_class legal_hold_record_class,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    expires_at timestamptz,
    claim_id uuid,
    claim_expires_at timestamptz,
    attempts integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_candidate public.non_content_expiry_candidates%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_instance_id IS NULL OR length(p_instance_id) NOT BETWEEN 1 AND 200
        OR btrim(p_instance_id) <> p_instance_id
        OR p_instance_id ~ '[[:cntrl:]]'
        OR p_claim_id IS NULL
        OR p_claim_ttl_seconds IS NULL OR p_claim_ttl_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid non-content expiry claim input' USING ERRCODE = '22023';
    END IF;
    SELECT candidate.* INTO v_candidate
    FROM public.non_content_expiry_candidates AS candidate
    WHERE candidate.expires_at <= v_now
      AND candidate.next_attempt_at <= v_now
      AND (
          candidate.state = 'PENDING'
          OR (candidate.state = 'CLAIMED' AND candidate.claim_expires_at <= v_now)
      )
    ORDER BY candidate.expires_at, candidate.kind, candidate.source_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    UPDATE public.non_content_expiry_candidates AS candidate
    SET state = 'CLAIMED', attempts = candidate.attempts + 1,
        claimed_by = p_instance_id, claim_id = p_claim_id,
        claim_expires_at = v_now + make_interval(secs => p_claim_ttl_seconds),
        updated_at = v_now
    WHERE candidate.kind = v_candidate.kind AND candidate.source_id = v_candidate.source_id
    RETURNING candidate.* INTO v_candidate;
    RETURN QUERY SELECT
        v_candidate.kind, v_candidate.source_id, v_candidate.record_class,
        v_candidate.organization_id, v_candidate.project_id, v_candidate.job_id,
        v_candidate.expires_at, v_candidate.claim_id, v_candidate.claim_expires_at,
        v_candidate.attempts;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_non_content_expiry(text, uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_claim_non_content_expiry(text, uuid, integer)
    OWNER TO vela_non_content_expiry_owner;

CREATE FUNCTION vela_complete_non_content_expiry(
    p_kind non_content_expiry_kind,
    p_source_id uuid,
    p_claim_id uuid,
    p_held_retry_seconds integer
) RETURNS TABLE (
    outcome text,
    receipt_id uuid,
    deleted_source_count integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_candidate public.non_content_expiry_candidates%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_receipt_id uuid := gen_random_uuid();
    v_holds integer;
    v_jobs integer := 0;
    v_attempts integer := 0;
    v_reservations integer := 0;
    v_charges integer := 0;
    v_exports integer := 0;
    v_invoice_receipts integer := 0;
    v_reconciliations integer := 0;
BEGIN
    IF p_kind IS NULL OR p_source_id IS NULL OR p_claim_id IS NULL
        OR p_held_retry_seconds IS NULL OR p_held_retry_seconds NOT BETWEEN 1 AND 86400
    THEN
        RAISE EXCEPTION 'invalid non-content expiry completion input'
            USING ERRCODE = '22023';
    END IF;
    SELECT candidate.* INTO v_candidate
    FROM public.non_content_expiry_candidates AS candidate
    WHERE candidate.kind = p_kind AND candidate.source_id = p_source_id
    FOR UPDATE;
    IF NOT FOUND OR v_candidate.state <> 'CLAIMED'
        OR v_candidate.claim_id IS DISTINCT FROM p_claim_id
        OR v_candidate.claim_expires_at <= v_now
    THEN
        RETURN QUERY SELECT 'STALE'::text, NULL::uuid, 0;
        RETURN;
    END IF;

    SELECT count(*)::integer INTO v_holds
    FROM vela_private.lock_active_non_content_legal_holds(
        v_candidate.organization_id, v_candidate.project_id,
        v_candidate.job_id, v_candidate.record_class
    );
    IF v_holds > 0 THEN
        UPDATE public.non_content_expiry_candidates
        SET state = 'PENDING', next_attempt_at = v_now
                + make_interval(secs => p_held_retry_seconds),
            claimed_by = NULL, claim_id = NULL, claim_expires_at = NULL,
            updated_at = v_now
        WHERE kind = p_kind AND source_id = p_source_id;
        RETURN QUERY SELECT 'HELD'::text, NULL::uuid, 0;
        RETURN;
    END IF;

    PERFORM set_config('vela.non_content_expiry_kind', p_kind::text, true);
    PERFORM set_config('vela.non_content_expiry_source_id', p_source_id::text, true);
    IF p_kind = 'JOB_METADATA' THEN
        UPDATE public.non_content_job_roots
        SET metadata_expired_at = v_now
        WHERE id = p_source_id AND metadata_expired_at IS NULL;
        DELETE FROM public.attempts WHERE job_id = p_source_id;
        GET DIAGNOSTICS v_attempts = ROW_COUNT;
        DELETE FROM public.jobs WHERE id = p_source_id;
        GET DIAGNOSTICS v_jobs = ROW_COUNT;
        IF v_jobs <> 1 THEN
            RAISE EXCEPTION 'metadata expiry source Job is missing'
                USING ERRCODE = '55000',
                    CONSTRAINT = 'metadata_expiry_source_missing';
        END IF;
    ELSIF p_kind = 'JOB_FINANCIAL' THEN
        UPDATE public.non_content_job_roots
        SET financial_expired_at = v_now
        WHERE id = p_source_id AND financial_expired_at IS NULL;
        DELETE FROM public.invoice_export_receipts AS receipt
        WHERE receipt.job_id = p_source_id;
        GET DIAGNOSTICS v_invoice_receipts = ROW_COUNT;
        DELETE FROM public.invoice_exports AS export
        WHERE export.job_id = p_source_id;
        GET DIAGNOSTICS v_exports = ROW_COUNT;
        DELETE FROM public.charges AS charge WHERE charge.job_id = p_source_id;
        GET DIAGNOSTICS v_charges = ROW_COUNT;
        DELETE FROM public.credit_reservations AS reservation
        WHERE reservation.job_id = p_source_id;
        GET DIAGNOSTICS v_reservations = ROW_COUNT;
        IF v_reservations <> 1 THEN
            RAISE EXCEPTION 'financial expiry CreditReservation is missing'
                USING ERRCODE = '55000',
                    CONSTRAINT = 'financial_expiry_source_missing';
        END IF;
    ELSE
        DELETE FROM public.finance_reconciliation_records AS reconciliation
        WHERE reconciliation.id = p_source_id
          AND reconciliation.organization_id = v_candidate.organization_id;
        GET DIAGNOSTICS v_reconciliations = ROW_COUNT;
        IF v_reconciliations <> 1 THEN
            RAISE EXCEPTION 'Finance Reconciliation expiry source is missing'
                USING ERRCODE = '55000',
                    CONSTRAINT = 'reconciliation_expiry_source_missing';
        END IF;
    END IF;

    INSERT INTO public.non_content_expiry_receipts (
        id, kind, source_id, record_class, organization_id, project_id, job_id,
        scheduled_at, expired_at, deleted_job_count, deleted_attempt_count,
        deleted_credit_reservation_count, deleted_charge_count,
        deleted_invoice_export_count, deleted_invoice_receipt_count,
        deleted_reconciliation_count
    ) VALUES (
        v_receipt_id, v_candidate.kind, v_candidate.source_id,
        v_candidate.record_class, v_candidate.organization_id,
        v_candidate.project_id, v_candidate.job_id, v_candidate.expires_at, v_now,
        v_jobs, v_attempts, v_reservations, v_charges, v_exports,
        v_invoice_receipts, v_reconciliations
    );
    UPDATE public.non_content_expiry_candidates
    SET state = 'EXPIRED', claimed_by = NULL, claim_id = NULL,
        claim_expires_at = NULL, expired_at = v_now, updated_at = v_now
    WHERE kind = p_kind AND source_id = p_source_id;
    RETURN QUERY SELECT
        'EXPIRED'::text, v_receipt_id,
        v_jobs + v_attempts + v_reservations + v_charges + v_exports
            + v_invoice_receipts + v_reconciliations;
END
$$;
REVOKE ALL ON FUNCTION vela_complete_non_content_expiry(
    non_content_expiry_kind, uuid, uuid, integer
) FROM PUBLIC;
ALTER FUNCTION vela_complete_non_content_expiry(
    non_content_expiry_kind, uuid, uuid, integer
) OWNER TO vela_non_content_expiry_owner;

GRANT USAGE ON SCHEMA public, vela_private TO vela_non_content_expiry_owner;
GRANT USAGE ON TYPE non_content_expiry_kind, non_content_expiry_state,
    legal_hold_record_class TO vela_non_content_expiry_owner;
GRANT SELECT, INSERT, UPDATE ON non_content_job_roots,
    non_content_expiry_candidates TO vela_non_content_expiry_owner;
GRANT SELECT, INSERT ON non_content_attempt_roots,
    non_content_expiry_receipts TO vela_non_content_expiry_owner;
GRANT SELECT ON customer_organizations, projects, legal_holds, jobs, attempts,
    charges, credit_reservations, invoice_exports, invoice_export_receipts,
    finance_reconciliation_records, outbox_events, artifact_sets,
    job_cancellation_decisions TO vela_non_content_expiry_owner;
GRANT UPDATE (id) ON customer_organizations TO vela_non_content_expiry_owner;
GRANT UPDATE (id) ON projects TO vela_non_content_expiry_owner;
GRANT UPDATE (id) ON legal_holds TO vela_non_content_expiry_owner;
GRANT UPDATE (id) ON attempts TO vela_non_content_expiry_owner;
GRANT DELETE ON jobs, attempts, charges, credit_reservations, invoice_exports,
    invoice_export_receipts, finance_reconciliation_records
    TO vela_non_content_expiry_owner;
GRANT EXECUTE ON FUNCTION vela_private.lock_active_non_content_legal_holds(
    uuid, uuid, uuid, legal_hold_record_class
) TO vela_non_content_expiry_owner;

GRANT USAGE ON SCHEMA public TO vela_non_content_expiry;
GRANT USAGE ON TYPE non_content_expiry_kind, legal_hold_record_class
    TO vela_non_content_expiry;
GRANT EXECUTE ON FUNCTION vela_claim_non_content_expiry(text, uuid, integer),
    vela_complete_non_content_expiry(
        non_content_expiry_kind, uuid, uuid, integer
    ) TO vela_non_content_expiry;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE non_content_expiry_candidates, non_content_expiry_receipts,
    non_content_job_roots, non_content_attempt_roots, jobs, attempts,
    finance_reconciliation_records IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM non_content_expiry_candidates)
        OR EXISTS (SELECT 1 FROM non_content_expiry_receipts)
        OR EXISTS (
            SELECT 1 FROM non_content_job_roots
            WHERE terminal_at IS NOT NULL
               OR metadata_expired_at IS NOT NULL
               OR financial_expired_at IS NOT NULL
        )
        OR EXISTS (
            SELECT 1
            FROM non_content_job_roots AS root
            LEFT JOIN jobs AS job ON job.id = root.id
            WHERE job.id IS NULL
        )
        OR EXISTS (
            SELECT 1
            FROM non_content_attempt_roots AS root
            LEFT JOIN attempts AS attempt ON attempt.id = root.id
            WHERE attempt.id IS NULL
        )
    THEN
        RAISE EXCEPTION 'non-content expiry authority or durable deletion evidence exists'
            USING ERRCODE = '55000',
                CONSTRAINT = 'non_content_expiry_rollback_is_unsafe';
    END IF;
END
$$;

DROP TRIGGER jobs_capture_non_content_root ON jobs;
DROP TRIGGER attempts_capture_non_content_root ON attempts;
DROP TRIGGER jobs_capture_non_content_terminal_candidates ON jobs;
DROP TRIGGER finance_reconciliation_capture_expiry_candidate
    ON finance_reconciliation_records;
DROP TRIGGER charges_capture_non_content_link ON charges;
DROP TRIGGER invoice_exports_capture_non_content_link ON invoice_exports;
DROP TRIGGER credit_reservations_validate_non_content_amount ON credit_reservations;
DROP TRIGGER charges_validate_non_content_amount ON charges;
DROP TRIGGER legal_holds_lock_non_content_target ON legal_holds;
DROP TRIGGER non_content_expiry_candidates_state_machine
    ON non_content_expiry_candidates;
DROP TRIGGER non_content_expiry_receipts_immutable ON non_content_expiry_receipts;
DROP TRIGGER non_content_job_roots_state_machine ON non_content_job_roots;
DROP TRIGGER non_content_attempt_roots_immutable ON non_content_attempt_roots;
DROP TRIGGER artifact_sets_validate_live_attempt_identity ON artifact_sets;
DROP TRIGGER artifacts_validate_live_attempt_identity ON artifacts;
DROP TRIGGER attempt_leases_validate_live_attempt_identity ON attempt_leases;
DROP TRIGGER attempt_progress_validate_live_attempt_identity ON attempt_progress;
DROP TRIGGER cancellation_stop_receipts_validate_live_attempt_identity
    ON cancellation_stop_receipts;
DROP TRIGGER execution_failure_decisions_validate_live_attempt_identity
    ON execution_failure_decisions;
DROP TRIGGER job_cancellation_decisions_validate_live_attempt_identity
    ON job_cancellation_decisions;
DROP TRIGGER profile_circuit_openings_validate_live_attempt_identity
    ON profile_certification_circuit_openings;

ALTER TABLE artifact_sets
    DROP CONSTRAINT artifact_sets_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT artifact_sets_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE attempt_progress
    DROP CONSTRAINT attempt_progress_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT attempt_progress_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE break_glass_requests
    DROP CONSTRAINT break_glass_requests_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT break_glass_requests_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE charges
    DROP CONSTRAINT charges_non_content_job_root,
    DROP CONSTRAINT charges_non_content_cancellation_root,
    DROP CONSTRAINT charges_non_content_artifact_root,
    ADD CONSTRAINT charges_organization_id_project_id_job_id_amount_minor_cur_fkey
        FOREIGN KEY (
            organization_id, project_id, job_id, amount_minor, currency
        ) REFERENCES jobs(
            organization_id, project_id, id, pricing_quoted_amount_minor,
            pricing_currency
        ),
    ADD CONSTRAINT charges_organization_id_project_id_job_id_cancellation_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id, cancellation_id)
        REFERENCES job_cancellation_decisions(
            organization_id, project_id, job_id, id
        ),
    ADD CONSTRAINT charges_artifact_set_identity
        FOREIGN KEY (organization_id, project_id, job_id, artifact_set_id)
        REFERENCES artifact_sets(organization_id, project_id, job_id, id);
ALTER TABLE content_deletion_requests
    DROP CONSTRAINT content_deletion_requests_organization_id_project_id_job_i_fkey,
    ADD CONSTRAINT content_deletion_requests_organization_id_project_id_job_i_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE credit_reservations
    DROP CONSTRAINT credit_reservations_non_content_job_root,
    ADD CONSTRAINT credit_reservations_organization_id_project_id_job_id_amou_fkey
        FOREIGN KEY (
            organization_id, project_id, job_id, amount_minor, currency
        ) REFERENCES jobs(
            organization_id, project_id, id, pricing_quoted_amount_minor,
            pricing_currency
        );
ALTER TABLE debug_dump_authorizations
    DROP CONSTRAINT debug_dump_authorizations_organization_id_project_id_job_i_fkey,
    ADD CONSTRAINT debug_dump_authorizations_organization_id_project_id_job_i_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE execution_failure_decisions
    DROP CONSTRAINT execution_failure_decisions_organization_id_project_id_job_fkey,
    ADD CONSTRAINT execution_failure_decisions_organization_id_project_id_job_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE execution_retry_evidence
    DROP CONSTRAINT execution_retry_evidence_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT execution_retry_evidence_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE idempotency_results
    DROP CONSTRAINT idempotency_results_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT idempotency_results_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE inbox_receipts
    DROP CONSTRAINT inbox_receipts_organization_id_project_id_aggregate_id_fkey,
    ADD CONSTRAINT inbox_receipts_organization_id_project_id_aggregate_id_fkey
        FOREIGN KEY (organization_id, project_id, aggregate_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE job_cancellation_decisions
    DROP CONSTRAINT job_cancellation_decisions_organization_id_project_id_job__fkey,
    ADD CONSTRAINT job_cancellation_decisions_organization_id_project_id_job__fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE job_runtime_predictions
    DROP CONSTRAINT job_runtime_predictions_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT job_runtime_predictions_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE legal_holds
    DROP CONSTRAINT legal_holds_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT legal_holds_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_organization_id_project_id_aggregate_id_fkey,
    ADD CONSTRAINT outbox_events_organization_id_project_id_aggregate_id_fkey
        FOREIGN KEY (organization_id, project_id, aggregate_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE profile_certification_circuit_openings
    DROP CONSTRAINT profile_certification_circuit_organization_id_project_id_t_fkey,
    ADD CONSTRAINT profile_certification_circuit_organization_id_project_id_t_fkey
        FOREIGN KEY (organization_id, project_id, triggering_job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE retry_runtime_states
    DROP CONSTRAINT retry_runtime_states_organization_id_project_id_job_id_fkey,
    ADD CONSTRAINT retry_runtime_states_organization_id_project_id_job_id_fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);
ALTER TABLE scheduler_dispatch_intents
    DROP CONSTRAINT scheduler_dispatch_intents_organization_id_project_id_job__fkey,
    ADD CONSTRAINT scheduler_dispatch_intents_organization_id_project_id_job__fkey
        FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id);

ALTER TABLE artifact_sets
    DROP CONSTRAINT artifact_sets_organization_id_project_id_attempt_id_job_id_fkey,
    ADD CONSTRAINT artifact_sets_organization_id_project_id_attempt_id_job_id_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id, attempt_fence
        ) REFERENCES attempts(organization_id, project_id, id, job_id, fence);
ALTER TABLE artifacts
    DROP CONSTRAINT artifacts_organization_id_project_id_attempt_id_job_id_att_fkey,
    ADD CONSTRAINT artifacts_organization_id_project_id_attempt_id_job_id_att_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id, attempt_fence
        ) REFERENCES attempts(organization_id, project_id, id, job_id, fence);
ALTER TABLE attempt_leases
    DROP CONSTRAINT attempt_leases_organization_id_project_id_attempt_id_worke_fkey,
    ADD CONSTRAINT attempt_leases_organization_id_project_id_attempt_id_worke_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, worker_id, worker_epoch, fence
        ) REFERENCES attempts(
            organization_id, project_id, id, worker_id, worker_epoch, fence
        );
ALTER TABLE attempt_progress
    DROP CONSTRAINT attempt_progress_attempt_identity_fkey,
    ADD CONSTRAINT attempt_progress_attempt_identity_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id,
            worker_id, worker_epoch, fence
        ) REFERENCES attempts(
            organization_id, project_id, id, job_id,
            worker_id, worker_epoch, fence
        );
ALTER TABLE cancellation_stop_receipts
    DROP CONSTRAINT cancellation_stop_receipts_organization_id_project_id_atte_fkey,
    ADD CONSTRAINT cancellation_stop_receipts_organization_id_project_id_atte_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id,
            worker_id, worker_epoch, attempt_fence
        ) REFERENCES attempts(
            organization_id, project_id, id, job_id,
            worker_id, worker_epoch, fence
        );
ALTER TABLE debug_dumps
    DROP CONSTRAINT debug_dumps_organization_id_project_id_attempt_id_fkey,
    ADD CONSTRAINT debug_dumps_organization_id_project_id_attempt_id_fkey
        FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id);
ALTER TABLE execution_failure_decisions
    DROP CONSTRAINT execution_failure_decisions_organization_id_project_id_att_fkey,
    ADD CONSTRAINT execution_failure_decisions_organization_id_project_id_att_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id,
            worker_id, worker_epoch, attempt_fence
        ) REFERENCES attempts(
            organization_id, project_id, id, job_id,
            worker_id, worker_epoch, fence
        );
ALTER TABLE job_cancellation_decisions
    DROP CONSTRAINT job_cancellation_decisions_organization_id_project_id_atte_fkey,
    ADD CONSTRAINT job_cancellation_decisions_organization_id_project_id_atte_fkey
        FOREIGN KEY (
            organization_id, project_id, attempt_id, job_id,
            worker_id, worker_epoch, attempt_fence
        ) REFERENCES attempts(
            organization_id, project_id, id, job_id,
            worker_id, worker_epoch, fence
        );
ALTER TABLE profile_certification_circuit_openings
    DROP CONSTRAINT profile_certification_circui_organization_id_project_id_t_fkey1,
    ADD CONSTRAINT profile_certification_circui_organization_id_project_id_t_fkey1
        FOREIGN KEY (
            organization_id, project_id, triggering_attempt_id, triggering_job_id,
            triggering_worker_id, triggering_worker_epoch, triggering_attempt_fence
        ) REFERENCES attempts(
            organization_id, project_id, id, job_id,
            worker_id, worker_epoch, fence
        );

ALTER TABLE visible_completions
    DROP CONSTRAINT visible_completions_non_content_charge_root,
    ADD CONSTRAINT visible_completions_organization_id_project_id_job_id_char_fkey
        FOREIGN KEY (organization_id, project_id, job_id, charge_id)
        REFERENCES charges(organization_id, project_id, job_id, id);
ALTER TABLE invoice_exports
    DROP CONSTRAINT invoice_exports_non_content_event_root,
    ADD CONSTRAINT invoice_exports_requested_event_id_fkey
        FOREIGN KEY (requested_event_id) REFERENCES outbox_events(event_id);

CREATE OR REPLACE FUNCTION vela_reject_cancellation_history_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'cancellation decisions and Charges are immutable';
END
$$;

CREATE OR REPLACE FUNCTION vela_reject_invoice_export_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Invoice export receipts are immutable';
END
$$;

CREATE OR REPLACE FUNCTION vela_enforce_invoice_export_transition() RETURNS trigger
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

CREATE OR REPLACE FUNCTION vela_reject_finance_reconciliation_record_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Finance Reconciliation records are immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'finance_reconciliation_record_is_immutable';
END
$$;

CREATE OR REPLACE FUNCTION vela_assert_job_required_children(p_job_id uuid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.jobs WHERE id = p_job_id)
        AND (
            (SELECT count(*) FROM public.credit_reservations
             WHERE job_id = p_job_id) <> 1
            OR (SELECT count(*) FROM public.retry_runtime_states
                WHERE job_id = p_job_id) <> 1
        )
    THEN
        RAISE EXCEPTION 'Job % must have exactly one CreditReservation and RetryRuntimeState',
            p_job_id USING ERRCODE = '23514';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION vela_private.lock_active_non_content_legal_holds(
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_record_class legal_hold_record_class
) RETURNS TABLE (hold_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_organization_id IS NULL OR p_project_id IS NULL OR p_job_id IS NULL
        OR p_record_class IS NULL
    THEN
        RAISE EXCEPTION 'Legal Hold target is invalid' USING ERRCODE = '22023';
    END IF;
    PERFORM 1
    FROM public.jobs AS job
    WHERE job.organization_id = p_organization_id
      AND job.project_id = p_project_id
      AND job.id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Legal Hold descendant Job is not found' USING ERRCODE = 'P0002';
    END IF;
    RETURN QUERY
    SELECT hold.id
    FROM public.legal_holds AS hold
    WHERE hold.organization_id = p_organization_id
      AND p_record_class = ANY(hold.record_classes)
      AND hold.state = 'ACTIVE'
      AND (
          hold.scope = 'ORGANIZATION'
          OR (hold.scope = 'PROJECT' AND hold.project_id = p_project_id)
          OR (
              hold.scope = 'JOB'
              AND hold.project_id = p_project_id
              AND hold.job_id = p_job_id
          )
      )
    ORDER BY hold.id
    FOR UPDATE;
END
$$;
ALTER FUNCTION vela_private.lock_active_non_content_legal_holds(
    uuid, uuid, uuid, legal_hold_record_class
) OWNER TO vela_compliance_owner;

CREATE OR REPLACE FUNCTION vela_apply_legal_hold_event(
    p_event_id uuid,
    p_idempotency_key text,
    p_source_sequence bigint,
    p_hold_id uuid,
    p_kind legal_hold_event_kind,
    p_scope legal_hold_scope,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_record_classes legal_hold_record_class[],
    p_reason_code text,
    p_external_reference text,
    p_effective_at timestamptz
) RETURNS TABLE (
    event_id uuid,
    replayed boolean,
    hold_id uuid,
    state legal_hold_state,
    scope legal_hold_scope,
    record_classes legal_hold_record_class[],
    recorded_at timestamptz,
    released_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_identity record;
    v_existing public.legal_hold_events%ROWTYPE;
    v_hold public.legal_holds%ROWTYPE;
    v_last_sequence bigint;
    v_recorded_at timestamptz;
BEGIN
    SELECT * INTO STRICT v_identity FROM vela_private.require_compliance_identity();
    IF p_event_id IS NULL OR p_hold_id IS NULL OR p_kind IS NULL
        OR p_source_sequence IS NULL OR p_source_sequence <= 0
        OR p_idempotency_key IS NULL OR length(p_idempotency_key) NOT BETWEEN 1 AND 500
        OR btrim(p_idempotency_key) <> p_idempotency_key
        OR p_idempotency_key ~ '[[:cntrl:]]'
        OR p_reason_code IS NULL OR p_reason_code !~ '^[A-Z][A-Z0-9_]{0,99}$'
        OR p_external_reference IS NULL
        OR length(p_external_reference) NOT BETWEEN 1 AND 500
        OR btrim(p_external_reference) <> p_external_reference
        OR p_external_reference ~ '[[:cntrl:]]'
        OR p_effective_at IS NULL
    THEN
        RAISE EXCEPTION 'invalid Legal Hold event input' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.compliance_event_cursors (principal_id)
    VALUES (v_identity.principal_id)
    ON CONFLICT (principal_id) DO NOTHING;
    SELECT cursor.last_sequence INTO STRICT v_last_sequence
    FROM public.compliance_event_cursors AS cursor
    WHERE cursor.principal_id = v_identity.principal_id
    FOR UPDATE;

    SELECT * INTO v_existing
    FROM public.legal_hold_events AS event
    WHERE event.principal_id = v_identity.principal_id
      AND event.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.source_sequence IS DISTINCT FROM p_source_sequence
            OR v_existing.hold_id IS DISTINCT FROM p_hold_id
            OR v_existing.kind IS DISTINCT FROM p_kind
            OR v_existing.scope IS DISTINCT FROM p_scope
            OR v_existing.organization_id IS DISTINCT FROM p_organization_id
            OR v_existing.project_id IS DISTINCT FROM p_project_id
            OR v_existing.job_id IS DISTINCT FROM p_job_id
            OR v_existing.record_classes IS DISTINCT FROM p_record_classes
            OR v_existing.reason_code IS DISTINCT FROM p_reason_code
            OR v_existing.external_reference IS DISTINCT FROM p_external_reference
            OR v_existing.effective_at IS DISTINCT FROM p_effective_at
        THEN
            RAISE EXCEPTION 'Legal Hold idempotency key conflicts with committed input'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_idempotency_conflict';
        END IF;
        SELECT * INTO STRICT v_hold FROM public.legal_holds WHERE id = p_hold_id;
        RETURN QUERY SELECT
            v_existing.id, true, v_hold.id, v_hold.state, v_hold.scope,
            v_hold.record_classes, v_existing.recorded_at, v_hold.released_at;
        RETURN;
    END IF;

    IF v_last_sequence = 9223372036854775807 OR p_source_sequence <> v_last_sequence + 1 THEN
        RAISE EXCEPTION 'Legal Hold source sequence is out of order'
            USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_sequence_out_of_order';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.legal_hold_events AS event
        WHERE event.principal_id = v_identity.principal_id
          AND event.external_reference = p_external_reference
    ) THEN
        RAISE EXCEPTION 'Legal Hold external reference conflicts with committed input'
            USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_external_reference_conflict';
    END IF;
    v_recorded_at := clock_timestamp();

    IF p_kind = 'HOLD_PLACED' THEN
        IF p_scope IS NULL OR p_organization_id IS NULL OR p_record_classes IS NULL
            OR NOT (
                (p_scope = 'ORGANIZATION' AND p_project_id IS NULL AND p_job_id IS NULL)
                OR (p_scope = 'PROJECT' AND p_project_id IS NOT NULL AND p_job_id IS NULL)
                OR (p_scope = 'JOB' AND p_project_id IS NOT NULL AND p_job_id IS NOT NULL)
            )
            OR NOT (
                p_record_classes = ARRAY['METADATA']::legal_hold_record_class[]
                OR p_record_classes = ARRAY['FINANCIAL']::legal_hold_record_class[]
                OR p_record_classes = ARRAY['METADATA', 'FINANCIAL']::legal_hold_record_class[]
            )
        THEN
            RAISE EXCEPTION 'invalid Legal Hold placement shape' USING ERRCODE = '22023';
        END IF;
        IF p_scope = 'ORGANIZATION' THEN
            PERFORM 1 FROM public.customer_organizations
            WHERE id = p_organization_id;
        ELSIF p_scope = 'PROJECT' THEN
            PERFORM 1 FROM public.projects
            WHERE organization_id = p_organization_id AND id = p_project_id;
        ELSE
            PERFORM 1 FROM public.jobs
            WHERE organization_id = p_organization_id
              AND project_id = p_project_id AND id = p_job_id;
        END IF;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold target is not found' USING ERRCODE = 'P0002';
        END IF;
        INSERT INTO public.legal_holds (
            id, organization_id, project_id, job_id, scope, record_classes,
            placement_principal_id, placement_source_sequence,
            placement_reason_code, placement_external_reference,
            placement_effective_at, placed_at
        ) VALUES (
            p_hold_id, p_organization_id, p_project_id, p_job_id, p_scope,
            p_record_classes, v_identity.principal_id, p_source_sequence,
            p_reason_code, p_external_reference, p_effective_at, v_recorded_at
        );
    ELSIF p_kind = 'HOLD_RELEASED' THEN
        IF p_scope IS NOT NULL OR p_organization_id IS NOT NULL
            OR p_project_id IS NOT NULL OR p_job_id IS NOT NULL
            OR p_record_classes IS NOT NULL
        THEN
            RAISE EXCEPTION 'invalid Legal Hold release shape' USING ERRCODE = '22023';
        END IF;
        SELECT * INTO v_hold FROM public.legal_holds WHERE id = p_hold_id FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold is not found' USING ERRCODE = 'P0002';
        END IF;
        IF v_hold.state <> 'ACTIVE' THEN
            RAISE EXCEPTION 'Legal Hold is already released'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_already_released';
        END IF;
        IF p_effective_at < v_hold.placement_effective_at
            OR v_recorded_at < v_hold.placed_at
        THEN
            RAISE EXCEPTION 'Legal Hold release precedes placement'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_release_precedes_placement';
        END IF;
        UPDATE public.legal_holds
        SET state = 'RELEASED',
            release_principal_id = v_identity.principal_id,
            release_source_sequence = p_source_sequence,
            release_reason_code = p_reason_code,
            release_external_reference = p_external_reference,
            release_effective_at = p_effective_at,
            released_at = v_recorded_at,
            updated_at = v_recorded_at
        WHERE id = p_hold_id;
    ELSE
        RAISE EXCEPTION 'unsupported Legal Hold event kind' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.legal_hold_events (
        id, principal_id, idempotency_key, source_sequence, hold_id, kind,
        scope, organization_id, project_id, job_id, record_classes,
        reason_code, external_reference, effective_at, recorded_at
    ) VALUES (
        p_event_id, v_identity.principal_id, p_idempotency_key,
        p_source_sequence, p_hold_id, p_kind, p_scope, p_organization_id,
        p_project_id, p_job_id, p_record_classes, p_reason_code,
        p_external_reference, p_effective_at, v_recorded_at
    );
    UPDATE public.compliance_event_cursors
    SET last_sequence = p_source_sequence, updated_at = v_recorded_at
    WHERE principal_id = v_identity.principal_id;
    SELECT * INTO STRICT v_hold FROM public.legal_holds WHERE id = p_hold_id;
    RETURN QUERY SELECT
        p_event_id, false, v_hold.id, v_hold.state, v_hold.scope,
        v_hold.record_classes, v_recorded_at, v_hold.released_at;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_private.lock_active_non_content_legal_holds(
    uuid, uuid, uuid, legal_hold_record_class
) FROM vela_non_content_expiry_owner;
REVOKE SELECT ON customer_organizations, projects, legal_holds, jobs, attempts,
    charges, credit_reservations, invoice_exports, invoice_export_receipts,
    finance_reconciliation_records, outbox_events, artifact_sets,
    job_cancellation_decisions FROM vela_non_content_expiry_owner;
REVOKE UPDATE (id) ON customer_organizations FROM vela_non_content_expiry_owner;
REVOKE UPDATE (id) ON projects FROM vela_non_content_expiry_owner;
REVOKE UPDATE (id) ON legal_holds FROM vela_non_content_expiry_owner;
REVOKE UPDATE (id) ON attempts FROM vela_non_content_expiry_owner;
REVOKE DELETE ON jobs, attempts, charges, credit_reservations, invoice_exports,
    invoice_export_receipts, finance_reconciliation_records
    FROM vela_non_content_expiry_owner;
REVOKE USAGE ON SCHEMA public FROM vela_non_content_expiry;
REVOKE USAGE ON SCHEMA public, vela_private FROM vela_non_content_expiry_owner;

DROP FUNCTION vela_complete_non_content_expiry(
    non_content_expiry_kind, uuid, uuid, integer
);
DROP FUNCTION vela_claim_non_content_expiry(text, uuid, integer);
DROP FUNCTION vela_reject_non_content_expiry_receipt_mutation();
DROP FUNCTION vela_enforce_non_content_candidate_transition();
DROP FUNCTION vela_reject_non_content_attempt_root_mutation();
DROP FUNCTION vela_enforce_non_content_job_root_transition();
DROP FUNCTION vela_private.validate_live_profile_circuit_attempt_identity();
DROP FUNCTION vela_private.validate_live_evidence_attempt_identity();
DROP FUNCTION vela_private.validate_live_progress_attempt_identity();
DROP FUNCTION vela_private.validate_live_lease_attempt_identity();
DROP FUNCTION vela_private.validate_live_artifact_attempt_identity();
DROP FUNCTION vela_private.require_live_full_attempt_identity(
    uuid, uuid, uuid, uuid, uuid, bigint, bigint
);
DROP FUNCTION vela_private.require_live_lease_attempt_identity(
    uuid, uuid, uuid, uuid, bigint, bigint
);
DROP FUNCTION vela_private.require_live_artifact_attempt_identity(
    uuid, uuid, uuid, uuid, bigint
);
DROP FUNCTION vela_private.lock_non_content_hold_target();
DROP FUNCTION vela_private.validate_non_content_financial_amount();
DROP FUNCTION vela_private.capture_non_content_invoice_link();
DROP FUNCTION vela_private.capture_non_content_charge_link();
DROP FUNCTION vela_private.capture_reconciliation_expiry_candidate();
DROP FUNCTION vela_private.capture_non_content_terminal_candidates();
DROP FUNCTION vela_private.capture_non_content_attempt_root();
DROP FUNCTION vela_private.capture_non_content_job_root();

DROP TABLE non_content_expiry_receipts;
DROP TABLE non_content_expiry_candidates;
DROP TABLE non_content_attempt_roots;
DROP TABLE non_content_job_roots;
DROP TYPE non_content_expiry_state;
DROP TYPE non_content_expiry_kind;
-- +goose StatementEnd
