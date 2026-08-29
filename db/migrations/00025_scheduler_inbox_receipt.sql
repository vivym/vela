-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_prepare_scheduler_inbox_receipt(
    p_event_id uuid,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_aggregate_version bigint
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_event_id IS NULL
       OR p_organization_id IS NULL
       OR p_project_id IS NULL
       OR p_job_id IS NULL
       OR p_aggregate_version < 1 THEN
        RAISE EXCEPTION 'invalid Scheduler Inbox receipt identity'
            USING ERRCODE = '22023';
    END IF;

    PERFORM 1
    FROM public.outbox_events AS event
    WHERE event.event_id = p_event_id
      AND event.organization_id = p_organization_id
      AND event.project_id = p_project_id
      AND event.aggregate_type = 'Job'
      AND event.aggregate_id = p_job_id
      AND event.aggregate_version = p_aggregate_version
      AND event.event_type = 'job.ready'
      AND event.schema_version = 1
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Scheduler Inbox receipt does not match the authoritative Outbox event'
            USING ERRCODE = '22023';
    END IF;

    RETURN NOT EXISTS (
        SELECT 1
        FROM public.inbox_receipts AS receipt
        WHERE receipt.consumer_name = 'scheduler'
          AND receipt.event_id = p_event_id
    );
END
$$;
REVOKE ALL ON FUNCTION vela_prepare_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) FROM PUBLIC;
ALTER FUNCTION vela_prepare_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_prepare_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) TO vela_scheduler_inbox;

CREATE FUNCTION vela_record_scheduler_inbox_receipt(
    p_event_id uuid,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_aggregate_version bigint
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_applied boolean;
BEGIN
    IF p_event_id IS NULL
       OR p_organization_id IS NULL
       OR p_project_id IS NULL
       OR p_job_id IS NULL
       OR p_aggregate_version < 1 THEN
        RAISE EXCEPTION 'invalid Scheduler Inbox receipt identity'
            USING ERRCODE = '22023';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.outbox_events AS event
        WHERE event.event_id = p_event_id
          AND event.organization_id = p_organization_id
          AND event.project_id = p_project_id
          AND event.aggregate_type = 'Job'
          AND event.aggregate_id = p_job_id
          AND event.aggregate_version = p_aggregate_version
          AND event.event_type = 'job.ready'
          AND event.schema_version = 1
    ) THEN
        RAISE EXCEPTION 'Scheduler Inbox receipt does not match the authoritative Outbox event'
            USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.inbox_receipts (
        consumer_name,
        event_id,
        organization_id,
        project_id,
        aggregate_type,
        aggregate_id,
        aggregate_version,
        event_type
    ) VALUES (
        'scheduler',
        p_event_id,
        p_organization_id,
        p_project_id,
        'Job',
        p_job_id,
        p_aggregate_version,
        'job.ready'
    )
    ON CONFLICT DO NOTHING
    RETURNING true INTO v_applied;

    RETURN COALESCE(v_applied, false);
END
$$;
REVOKE ALL ON FUNCTION vela_record_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) FROM PUBLIC;
ALTER FUNCTION vela_record_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_record_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) TO vela_scheduler_inbox;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE EXECUTE ON FUNCTION vela_record_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) FROM vela_scheduler_inbox;
DROP FUNCTION vela_record_scheduler_inbox_receipt(uuid, uuid, uuid, uuid, bigint);
REVOKE EXECUTE ON FUNCTION vela_prepare_scheduler_inbox_receipt(
    uuid, uuid, uuid, uuid, bigint
) FROM vela_scheduler_inbox;
DROP FUNCTION vela_prepare_scheduler_inbox_receipt(uuid, uuid, uuid, uuid, bigint);
-- +goose StatementEnd
