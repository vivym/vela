-- +goose Up
-- +goose StatementBegin
-- This migration is the irreversible Legacy H3 point of no return. A database
-- created from an empty schema has no production history to authorize. Every
-- non-empty upgrade must still bind the one immutable authorization to the
-- current cutover revision and must re-prove that live Legacy authority is zero.
LOCK TABLE jobs, attempts, attempt_leases,
    legacy_h3_contraction_authorizations, stage_cutover_control
IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    v_job_count bigint;
    v_authorization_count bigint;
    v_authorized_revision uuid;
    v_current_revision uuid;
    v_live_total bigint;
BEGIN
    SELECT count(*) INTO v_job_count FROM public.jobs;
    IF v_job_count = 0 THEN
        RETURN;
    END IF;

    SELECT count(*), min(cutover_revision_id)
    INTO v_authorization_count, v_authorized_revision
    FROM public.legacy_h3_contraction_authorizations;
    IF v_authorization_count <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_authorization_required',
            MESSAGE = 'Non-empty Legacy H3 schema contraction requires one immutable authorization';
    END IF;

    SELECT current_revision_id INTO STRICT v_current_revision
    FROM public.stage_cutover_control
    WHERE singleton;
    IF v_current_revision IS DISTINCT FROM v_authorized_revision THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_authorization_stale',
            MESSAGE = 'Legacy H3 contraction authorization no longer binds the current cutover revision';
    END IF;

    SELECT public.vela_current_legacy_authority_inventory_total()
    INTO v_live_total;
    IF v_live_total <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_live_inventory_nonzero',
            MESSAGE = 'Legacy H3 live authority inventory is no longer zero';
    END IF;
END
$$;

-- Rewrite the two Stage functions that physically insert transitional fields
-- before those fields are removed. The exact source fragments are assertions:
-- migration fails closed if an earlier migration changes either definition.
DO $$
DECLARE
    v_definition text;
    v_signature text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_instantiate_stage_graph(jsonb)'::regprocedure);
    v_old := E'        execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,\n'
        || E'        state, fence, assigned_at, execution_authority_kind, graph_state,\n'
        || E'        execution_graph_snapshot_id, profile_certification_id,\n'
        || E'        scheduler_dispatch_intent_id\n';
    v_new := E'        state, fence, graph_state, execution_graph_snapshot_id\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_dependency_drift',
            MESSAGE = 'vela_instantiate_stage_graph column dependency changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'        v_attempt_number, NULL, NULL, NULL, NULL, ''ASSIGNED'', v_next_fence, NULL,\n'
        || E'        ''STAGE_GRAPH'', ''QUEUED'', v_snapshot_id, NULL, NULL\n';
    v_new := E'        v_attempt_number, ''ASSIGNED'', v_next_fence, ''QUEUED'', v_snapshot_id\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_dependency_drift',
            MESSAGE = 'vela_instantiate_stage_graph value dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_cancel_stage_graph(jsonb)'::regprocedure);
    v_definition := replace(
        v_definition,
        'v_latest_authority public.execution_authority_kind;',
        'v_latest_authority text;'
    );
    v_definition := replace(
        v_definition,
        'AND existing.execution_authority_kind = ''STAGE_GRAPH'';',
        'AND existing.stage_graph_attempt_id IS NOT NULL;'
    );
    v_old := E'        execution_authority_kind, stage_graph_attempt_id,\n';
    v_new := E'        stage_graph_attempt_id,\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_dependency_drift',
            MESSAGE = 'vela_cancel_stage_graph column dependency changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'        ''STAGE_GRAPH'', v_attempt.id, v_attempt.fence, v_stage_lease_ids,\n';
    v_new := E'        v_attempt.id, v_attempt.fence, v_stage_lease_ids,\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_dependency_drift',
            MESSAGE = 'vela_cancel_stage_graph value dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);

    v_definition := pg_get_functiondef('public.vela_apply_stage_command(jsonb)'::regprocedure);
    v_old := E'v_attempt.execution_authority_kind <> ''STAGE_GRAPH''\n           OR ';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_dependency_drift',
            MESSAGE = 'vela_apply_stage_command authority dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, '');

    v_definition := pg_get_functiondef('public.vela_seal_stage_output(jsonb)'::regprocedure);
    v_old := E'v_attempt.execution_authority_kind <> ''STAGE_GRAPH''\n       OR ';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_schema_contraction_dependency_drift',
            MESSAGE = 'vela_seal_stage_output authority dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, '');

    -- All retained execution functions are Stage-only after this migration.
    -- Rewrite every reference to a column or relation that is removed below.
    -- Each exact fragment is an assertion against migration-history drift.
    FOR v_signature, v_old, v_new IN
        SELECT rewrite.signature, rewrite.old_source, rewrite.new_source
        FROM (VALUES
            (
                'public.vela_enforce_graph_snapshot_job_authority()',
                E'       OR v_job.execution_authority_kind <> ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_apply_stage_command(jsonb)',
                E'    IF v_attempt.execution_authority_kind <> ''STAGE_GRAPH''\n       OR NOT (\n',
                E'    IF NOT (\n'
            ),
            (
                'public.vela_apply_stage_command(jsonb)',
                E'            IF v_job.worker_pool_id IS NOT NULL THEN\n'
                    || E'                UPDATE public.worker_pools AS pool\n'
                    || E'                SET queued_count = pool.queued_count - 1,\n'
                    || E'                    retry_wait_count = pool.retry_wait_count\n'
                    || E'                        - CASE WHEN v_job.state = ''RETRY_WAIT'' THEN 1 ELSE 0 END\n'
                    || E'                WHERE pool.id = v_job.worker_pool_id\n'
                    || E'                  AND pool.queued_count > 0\n'
                    || E'                  AND (v_job.state <> ''RETRY_WAIT'' OR pool.retry_wait_count > 0);\n'
                    || E'                GET DIAGNOSTICS v_rows = ROW_COUNT;\n'
                    || E'                IF v_rows <> 1 THEN\n'
                    || E'                    RAISE EXCEPTION USING\n'
                    || E'                        ERRCODE = ''55000'',\n'
                    || E'                        CONSTRAINT = ''stage_first_progress_pool_capacity_changed'',\n'
                    || E'                        MESSAGE = ''Admission pool counters changed before first graph progress'';\n'
                    || E'                END IF;\n'
                    || E'            END IF;\n',
                ''
            ),
            (
                'public.vela_apply_stage_command(jsonb)',
                E'            IF v_job.worker_pool_id IS NOT NULL THEN\n'
                    || E'                UPDATE public.worker_pools AS pool\n'
                    || E'                SET queued_count = pool.queued_count - 1,\n'
                    || E'                    retry_wait_count = pool.retry_wait_count\n'
                    || E'                        - CASE WHEN v_job.state = ''RETRY_WAIT'' THEN 1 ELSE 0 END\n'
                    || E'                WHERE pool.id = v_job.worker_pool_id\n'
                    || E'                  AND pool.queued_count > 0\n'
                    || E'                  AND (\n'
                    || E'                      v_job.state <> ''RETRY_WAIT'' OR pool.retry_wait_count > 0\n'
                    || E'                  );\n'
                    || E'                GET DIAGNOSTICS v_rows = ROW_COUNT;\n'
                    || E'                IF v_rows <> 1 THEN\n'
                    || E'                    RAISE EXCEPTION USING\n'
                    || E'                        ERRCODE = ''55000'',\n'
                    || E'                        CONSTRAINT = ''stage_first_progress_pool_capacity_changed'',\n'
                    || E'                        MESSAGE = ''Admission pool counters changed before first graph progress'';\n'
                    || E'                END IF;\n'
                    || E'            END IF;\n',
                ''
            ),
            (
                'public.vela_cancel_stage_graph(jsonb)',
                E'    SELECT attempt.execution_authority_kind INTO v_latest_authority\n'
                    || E'    FROM public.attempts AS attempt\n'
                    || E'    WHERE attempt.job_id = v_job.id\n'
                    || E'    ORDER BY attempt.attempt_number DESC\n'
                    || E'    LIMIT 1;\n'
                    || E'    IF v_latest_authority IS DISTINCT FROM ''STAGE_GRAPH'' THEN\n'
                    || E'        RAISE EXCEPTION USING\n'
                    || E'            ERRCODE = ''P0003'',\n'
                    || E'            CONSTRAINT = ''stage_graph_cancellation_not_applicable'',\n'
                    || E'            MESSAGE = ''Job does not use Stage graph execution authority'';\n'
                    || E'    END IF;\n',
                ''
            ),
            (
                'public.vela_cancel_stage_graph(jsonb)',
                E'      AND candidate.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_cancel_stage_graph(jsonb)',
                E'        IF v_job.worker_pool_id IS NOT NULL THEN\n'
                    || E'            UPDATE public.worker_pools AS pool\n'
                    || E'            SET queued_count = pool.queued_count - 1,\n'
                    || E'                retry_wait_count = pool.retry_wait_count\n'
                    || E'                    - CASE WHEN v_job.state = ''RETRY_WAIT'' THEN 1 ELSE 0 END\n'
                    || E'            WHERE pool.id = v_job.worker_pool_id\n'
                    || E'              AND pool.queued_count > 0\n'
                    || E'              AND (v_job.state <> ''RETRY_WAIT'' OR pool.retry_wait_count > 0);\n'
                    || E'            GET DIAGNOSTICS v_rows = ROW_COUNT;\n'
                    || E'            IF v_rows <> 1 THEN\n'
                    || E'                RAISE EXCEPTION ''Worker pool queue counters changed during Stage graph cancellation''\n'
                    || E'                    USING ERRCODE = ''23514'';\n'
                    || E'            END IF;\n'
                    || E'        END IF;\n',
                ''
            ),
            (
                'public.vela_capture_stage_scheduler_snapshot(jsonb)',
                E'          AND attempt.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_claim_stage_scheduler_decision(jsonb)',
                E'             AND attempt.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_complete_stage_graph_instantiation(uuid,uuid,uuid,uuid,uuid)',
                E'           AND attempt.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_complete_stage_graph_visible_completion_attempt(uuid,bigint,timestamptz)',
                E'      AND attempt.worker_id IS NULL\n',
                ''
            ),
            (
                'public.vela_complete_stage_graph_visible_completion_attempt(uuid,bigint,timestamptz)',
                E'      AND attempt.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_guard_stage_graph_instantiate_command()',
                E'         AND job.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_guard_stage_graph_instantiate_command()',
                E'         AND attempt.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_heartbeat_stage_worker_command(jsonb)',
                E'    IF v_attempt.id IS NULL OR v_attempt.execution_authority_kind <> ''STAGE_GRAPH''\n       OR ',
                E'    IF v_attempt.id IS NULL\n       OR '
            ),
            (
                'public.vela_instantiate_admitted_stage_graph(uuid,uuid,uuid)',
                E'              AND job.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_read_stage_assignment_execution(uuid,uuid)',
                'v_attempt.execution_profile_revision_id',
                'v_job.stage_execution_profile_revision_id'
            ),
            (
                'public.vela_reattach_stage_worker_command(jsonb)',
                E'    IF v_attempt.id IS NULL OR v_attempt.execution_authority_kind <> ''STAGE_GRAPH''\n       OR ',
                E'    IF v_attempt.id IS NULL\n       OR '
            ),
            (
                'public.vela_reconcile_stage_graph_instantiations(integer)',
                E'               AND attempt.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_reconcile_stage_graphs(integer)',
                E'        WHERE attempt.execution_authority_kind = ''STAGE_GRAPH''\n          AND ',
                E'        WHERE '
            ),
            (
                'public.vela_refresh_stage_ready_queue(uuid)',
                E'      AND attempt.execution_authority_kind = ''STAGE_GRAPH''\n',
                ''
            ),
            (
                'public.vela_start_stage_worker_command(jsonb)',
                E'    IF v_attempt.id IS NULL OR v_attempt.execution_authority_kind <> ''STAGE_GRAPH''\n       OR ',
                E'    IF v_attempt.id IS NULL\n       OR '
            ),
            (
                'vela_private.vela_insert_stage_graph_canceled_event(uuid,timestamptz,bigint)',
                E'      AND decision.execution_authority_kind = ''STAGE_GRAPH'';\n',
                E';\n'
            ),
            (
                'vela_private.vela_insert_stage_graph_cancellation_outbox_events(uuid,uuid,uuid)',
                E'      AND decision.execution_authority_kind = ''STAGE_GRAPH'';\n',
                E';\n'
            )
        ) AS rewrite(signature, old_source, new_source)
    LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        IF strpos(v_definition, v_old) = 0 THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'legacy_h3_schema_contraction_dependency_drift',
                MESSAGE = v_signature || ' Legacy dependency changed';
        END IF;
        EXECUTE replace(v_definition, v_old, v_new);
    END LOOP;
END
$$;

-- Reviewed function/trigger dependency list. Every drop uses PostgreSQL's
-- default RESTRICT behavior so a new
-- dependency must be understood and added here instead of disappearing through
-- CASCADE.
DROP TRIGGER jobs_freeze_prepared_legacy_h3 ON jobs;
DROP TRIGGER attempts_freeze_prepared_legacy_h3 ON attempts;
DROP TRIGGER attempt_leases_freeze_prepared_legacy_h3 ON attempt_leases;
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
DROP TRIGGER profile_certification_circuit_opening_identity
    ON profile_certification_circuit_openings;
DROP TRIGGER jobs_zero_backlog_legacy_authority_guard ON jobs;
DROP TRIGGER jobs_decrement_retry_subset_on_assignment ON jobs;
DROP TRIGGER attempts_bind_profile_certification ON attempts;
DROP TRIGGER attempts_enforce_scheduler_dispatch ON attempts;
DROP TRIGGER attempts_fleet_assignment_protocol ON attempts;
DROP TRIGGER attempts_profile_certification_immutable ON attempts;
DROP TRIGGER attempts_reject_excluded_worker ON attempts;
DROP TRIGGER attempts_reject_scheduler_dispatch_mutation ON attempts;
DROP TRIGGER jobs_execution_authority_immutable ON jobs;
DROP TRIGGER attempts_match_job_execution_authority ON attempts;
DROP TRIGGER attempts_guard_stage_graph_writer ON attempts;
DROP TRIGGER attempts_identity_immutable ON attempts;
DROP TRIGGER jobs_snapshot_immutable ON jobs;
DROP TRIGGER jobs_enqueue_stage_graph_instantiation ON jobs;
DROP TRIGGER jobs_require_atomic_stage_graph_admission ON jobs;
DROP TRIGGER scheduler_dispatch_protocol_apply_transition
    ON scheduler_dispatch_protocol_transitions;
DROP TRIGGER scheduler_dispatch_protocol_history_immutable
    ON scheduler_dispatch_protocol_transitions;
DROP TRIGGER scheduler_dispatch_protocol_validate_transition
    ON scheduler_dispatch_protocol_transitions;
DROP TRIGGER scheduler_dispatch_protocol_enforce_transition
    ON scheduler_dispatch_protocol_state;
DROP TRIGGER scheduler_dispatch_protocol_reject_delete
    ON scheduler_dispatch_protocol_state;
DROP TRIGGER fleet_assignment_protocol_apply_transition
    ON fleet_assignment_protocol_transitions;
DROP TRIGGER fleet_assignment_protocol_history_immutable
    ON fleet_assignment_protocol_transitions;
DROP TRIGGER fleet_assignment_protocol_validate_transition
    ON fleet_assignment_protocol_transitions;
DROP TRIGGER fleet_assignment_protocol_enforce_transition
    ON fleet_assignment_protocol_state;
DROP TRIGGER fleet_assignment_protocol_state_required
    ON fleet_assignment_protocol_state;
DROP TRIGGER fleet_mutation_authorizations_immutable
    ON fleet_mutation_authorizations;
DROP TRIGGER fleet_retirement_completions_immutable
    ON fleet_retirement_completions;
DROP TRIGGER worker_pools_normal_queue_bounded ON worker_pools;
DROP TRIGGER workers_default_node_identity ON workers;
DROP TRIGGER workers_identity_and_quarantine_fence ON workers;
DROP TRIGGER workers_quarantine_fences_leases ON workers;
DROP TRIGGER workers_record_epoch ON workers;

DROP FUNCTION vela_authorize_legacy_h3_contraction(
    uuid, bytea, bytea, text, bytea, bytea, text
);
DROP FUNCTION vela_prepare_legacy_h3_contraction(uuid, text);
DROP FUNCTION vela_capture_legacy_authority_inventory(uuid, text);
DROP FUNCTION vela_current_legacy_authority_inventory_total();
DROP FUNCTION vela_guard_prepared_legacy_h3_lease();
DROP FUNCTION vela_guard_prepared_legacy_h3_row();
DROP FUNCTION vela_guard_sealed_legacy_job_authority();
DROP FUNCTION vela_decrement_retry_subset_on_assignment();
DROP FUNCTION vela_bind_attempt_profile_certification();
DROP FUNCTION vela_reject_attempt_profile_certification_mutation();
DROP FUNCTION vela_enforce_scheduler_dispatch_attempt();
DROP FUNCTION vela_reject_scheduler_dispatch_attempt_mutation();
DROP FUNCTION vela_guard_fleet_assignment_writer();
DROP FUNCTION vela_reject_excluded_worker_attempt();
DROP FUNCTION vela_guard_job_execution_authority();
DROP FUNCTION vela_enforce_attempt_job_authority();
DROP FUNCTION vela_guard_stage_graph_attempt_writer();
DROP FUNCTION vela_reject_attempt_identity_mutation();
DROP FUNCTION vela_reject_job_snapshot_mutation();
DROP FUNCTION vela_enqueue_stage_graph_instantiation();
DROP FUNCTION vela_validate_profile_certification_circuit_opening();
DROP FUNCTION vela_private.validate_live_profile_circuit_attempt_identity();
DROP FUNCTION vela_private.validate_live_evidence_attempt_identity();
DROP FUNCTION vela_private.validate_live_progress_attempt_identity();
DROP FUNCTION vela_private.validate_live_lease_attempt_identity();
DROP FUNCTION vela_private.require_live_full_attempt_identity(
    uuid, uuid, uuid, uuid, uuid, bigint, bigint
);
DROP FUNCTION vela_private.require_live_lease_attempt_identity(
    uuid, uuid, uuid, uuid, bigint, bigint
);

-- Make the public entry point statically Stage-only before removing the Legacy
-- implementation. Keeping this outside the dynamic rewrite block also makes
-- the dependency contraction visible to schema analyzers such as sqlc.
CREATE OR REPLACE FUNCTION vela_cancel_job(
    p_job_id uuid,
    p_cancellation_id uuid,
    p_charge_id uuid,
    p_cancel_requested_event_id uuid,
    p_canceling_event_id uuid,
    p_canceled_event_id uuid,
    p_charge_posted_event_id uuid,
    p_invoice_export_event_id uuid
) RETURNS TABLE (
    cancellation_id uuid, job_id uuid, decision cancellation_decision,
    job_state job_state, previous_job_state job_state, job_version bigint,
    cancellation_fence bigint, attempt_id uuid, worker_id uuid,
    worker_epoch bigint, attempt_fence bigint, authority_lease_id uuid,
    authority_lease_phase lease_phase, authority_lease_owner_kind lease_owner_kind,
    authority_lease_owner_id text, authority_lease_expires_at timestamptz,
    billable boolean, charge_id uuid, charge_amount_minor bigint,
    charge_currency text, charge_reason charge_reason,
    charge_posted_at timestamptz, decided_at timestamptz, created boolean
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT * FROM public.vela_cancel_stage_graph(jsonb_build_object(
        'schema_version', 1,
        'job_id', p_job_id,
        'cancellation_id', p_cancellation_id,
        'charge_id', p_charge_id,
        'cancel_requested_event_id', p_cancel_requested_event_id,
        'canceling_event_id', p_canceling_event_id,
        'canceled_event_id', p_canceled_event_id,
        'charge_posted_event_id', p_charge_posted_event_id,
        'invoice_export_event_id', p_invoice_export_event_id
    ))
$$;

-- sqlc cannot observe the PL/pgSQL body rewrite that originally renamed the
-- public function. Keep this one dependency-sensitive drop executable by
-- PostgreSQL while leaving it opaque to the schema parser.
DO $$
BEGIN
    EXECUTE 'DROP FUNCTION vela_cancel_legacy_job(uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid)';
END
$$;
DROP FUNCTION vela_private.vela_insert_cancellation_outbox_events(
    uuid, uuid, uuid, uuid, uuid, uuid
);

DROP FUNCTION vela_resolve_job_execution_route(uuid, uuid, uuid);
DROP FUNCTION vela_lock_compatible_pool(uuid, uuid, uuid);
DROP FUNCTION vela_predict_admission_capacity(uuid, uuid, uuid, uuid, uuid, integer);
DROP FUNCTION vela_predict_job_dynamic_eta(uuid);
DROP FUNCTION vela_list_schedulable_worker_pools();
DROP FUNCTION vela_claim_scheduler_dispatch(uuid, text, integer);
DROP FUNCTION vela_abandon_scheduler_dispatch(uuid, text, text);
DROP FUNCTION vela_reconcile_expired_scheduler_dispatches();
DROP FUNCTION vela_transition_scheduler_dispatch_protocol(boolean, text);
DROP FUNCTION vela_prepare_scheduler_inbox_receipt(uuid, uuid, uuid, uuid, bigint);
DROP FUNCTION vela_record_scheduler_inbox_receipt(uuid, uuid, uuid, uuid, bigint);
DROP FUNCTION vela_scheduler_candidates(uuid, timestamptz);
DROP FUNCTION vela_scheduler_capacity_timeline(uuid, timestamptz);
DROP FUNCTION vela_scheduler_job_worker_compatibility(uuid, timestamptz, uuid);
DROP FUNCTION vela_scheduler_projectable_jobs(uuid, timestamptz);
DROP FUNCTION vela_scheduler_queue_projection();
DROP FUNCTION vela_scheduler_queue_projection_for_pool(uuid, timestamptz);
DROP FUNCTION vela_apply_scheduler_dispatch_protocol_transition();
DROP FUNCTION vela_enforce_scheduler_dispatch_protocol_state_transition();
DROP FUNCTION vela_reject_scheduler_dispatch_protocol_delete();
DROP FUNCTION vela_reject_scheduler_dispatch_protocol_history_mutation();
DROP FUNCTION vela_validate_scheduler_dispatch_protocol_transition();

DROP FUNCTION vela_configure_worker_pool_capacity(
    uuid, text, bigint, bigint, bigint, bigint, bigint, bigint, text
);
DROP FUNCTION vela_observe_worker_capacity(
    uuid, uuid, bigint, bigint, timestamptz, fleet_scratch_watermark_state,
    bigint, bigint, bigint, bigint, bigint, boolean, text
);
DROP FUNCTION vela_get_worker_pool_capacity(uuid);
DROP FUNCTION vela_begin_worker_readiness(
    uuid, uuid, uuid, bigint, text, uuid, text, text, timestamptz
);
DROP FUNCTION vela_report_worker_readiness(
    uuid, fleet_readiness_check, boolean, bytea, text
);
DROP FUNCTION vela_get_worker_readiness(uuid);
DROP FUNCTION vela_get_worker_readiness_work(uuid, bigint);
DROP FUNCTION vela_request_worker_drain(uuid, uuid, bigint, text, timestamptz, text);
DROP FUNCTION vela_reconcile_worker_drain(uuid, text);
DROP FUNCTION vela_get_worker_drain(uuid);
DROP FUNCTION vela_resolve_worker_identity(text, uuid, text, text, text);
DROP FUNCTION vela_transition_fleet_assignment_protocol(boolean, text, integer);
DROP FUNCTION vela_current_fleet_assignment_protocol_version();
DROP FUNCTION vela_require_fleet_protocol_enforced();
DROP FUNCTION vela_apply_fleet_assignment_protocol_transition();
DROP FUNCTION vela_enforce_fleet_assignment_protocol_state_transition();
DROP FUNCTION vela_reject_fleet_assignment_protocol_history_mutation();
DROP FUNCTION vela_reject_fleet_assignment_protocol_state_delete();
DROP FUNCTION vela_validate_fleet_assignment_protocol_transition();
DROP FUNCTION vela_authorize_fleet_mutation(
    text, text, fleet_protected_resource_kind, fleet_mutation_operation,
    text, text, text, uuid, uuid, bigint, uuid[], bytea
);
DROP FUNCTION vela_has_fleet_retirement_authorization(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
);
DROP FUNCTION vela_record_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[], text
);
DROP FUNCTION vela_has_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
);
DROP FUNCTION vela_reject_fleet_retirement_completion_mutation();
DROP FUNCTION vela_reject_fleet_mutation_authorization_mutation();
DROP FUNCTION vela_default_worker_node_identity();
DROP FUNCTION vela_validate_worker_node_identity();
DROP FUNCTION vela_fence_worker_leases_on_quarantine();
DROP FUNCTION vela_worker_capacity_allows_assignment(uuid, bigint);
DROP FUNCTION vela_pool_capacity_allows_assignment(uuid);
DROP FUNCTION vela_pool_capacity_allows_readiness(uuid);
DROP FUNCTION vela_check_worker_pool_normal_queue_bound();
DROP FUNCTION vela_record_worker_epoch();
DROP FUNCTION vela_transition_execution_lease_renewal_protocol(boolean, text);

-- Remediation history is preserved. Historical terminal operations retain their
-- old identity in explicitly named archive columns; all new/active operations
-- must target a WorkerInstance epoch.
DROP FUNCTION vela_request_remediation(
    uuid, uuid, bigint, text, text, text, bytea, text,
    remediation_action_level, text, text
);
DROP FUNCTION vela_approve_remediation(uuid, text);
DROP FUNCTION vela_start_remediation(uuid, uuid, bigint, text);
DROP FUNCTION vela_claim_remediation_execution(uuid, uuid, bigint, uuid, text);
DROP FUNCTION vela_complete_remediation(
    uuid, uuid, bigint, boolean, text, text, bytea, text
);
DROP FUNCTION vela_recover_remediation(uuid, text);
DROP FUNCTION vela_get_remediation_operation(uuid);
DROP FUNCTION vela_list_executing_remediation(integer);
DROP TRIGGER remediation_operations_immutable ON remediation_operations;
DROP FUNCTION vela_reject_remediation_operation_mutation();

ALTER TABLE remediation_execution_claims
    RENAME COLUMN worker_id TO legacy_worker_id;
ALTER TABLE remediation_execution_claims
    RENAME COLUMN worker_epoch TO legacy_worker_epoch;
ALTER TABLE remediation_execution_claims
    ALTER COLUMN legacy_worker_id DROP NOT NULL,
    ALTER COLUMN legacy_worker_epoch DROP NOT NULL,
    ADD COLUMN worker_instance_id uuid,
    ADD COLUMN worker_instance_epoch bigint CHECK (worker_instance_epoch > 0),
    ADD CONSTRAINT remediation_execution_claims_worker_instance_epoch_fk
        FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    ADD CONSTRAINT remediation_execution_claims_target_shape CHECK (
        (worker_instance_id IS NOT NULL AND worker_instance_epoch IS NOT NULL
            AND legacy_worker_id IS NULL AND legacy_worker_epoch IS NULL)
        OR
        (worker_instance_id IS NULL AND worker_instance_epoch IS NULL
            AND legacy_worker_id IS NOT NULL AND legacy_worker_epoch IS NOT NULL)
    );

ALTER TABLE remediation_operations
    RENAME COLUMN worker_id TO legacy_worker_id;
ALTER TABLE remediation_operations
    RENAME COLUMN worker_epoch TO legacy_worker_epoch;
ALTER TABLE remediation_operations
    DROP CONSTRAINT remediation_operations_worker_id_worker_epoch_fkey,
    DROP CONSTRAINT remediation_operations_worker_id_worker_epoch_idempotency_k_key,
    ALTER COLUMN legacy_worker_id DROP NOT NULL,
    ALTER COLUMN legacy_worker_epoch DROP NOT NULL,
    ADD COLUMN worker_instance_id uuid,
    ADD COLUMN worker_instance_epoch bigint CHECK (worker_instance_epoch > 0),
    ADD CONSTRAINT remediation_operations_worker_instance_epoch_fk
        FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    ADD CONSTRAINT remediation_operations_target_shape CHECK (
        (worker_instance_id IS NOT NULL AND worker_instance_epoch IS NOT NULL
            AND legacy_worker_id IS NULL AND legacy_worker_epoch IS NULL)
        OR
        (worker_instance_id IS NULL AND worker_instance_epoch IS NULL
            AND legacy_worker_id IS NOT NULL AND legacy_worker_epoch IS NOT NULL
            AND state IN ('SUCCEEDED', 'QUARANTINED'))
    );

DROP INDEX remediation_operations_one_active_per_worker_epoch;
CREATE UNIQUE INDEX remediation_operations_one_active_per_worker_instance_epoch
    ON remediation_operations (worker_instance_id, worker_instance_epoch)
    WHERE worker_instance_id IS NOT NULL
      AND state IN ('REQUESTED', 'APPROVAL_REQUIRED', 'EXECUTING');
CREATE UNIQUE INDEX remediation_operations_worker_instance_idempotency
    ON remediation_operations (
        worker_instance_id, worker_instance_epoch, idempotency_key
    )
    WHERE worker_instance_id IS NOT NULL;

-- Retained business history no longer has a live machine Lease foreign key.
ALTER TABLE job_cancellation_decisions
    DROP CONSTRAINT job_cancellation_decisions_organization_id_project_id_auth_fkey,
    DROP CONSTRAINT job_cancellation_decisions_authority_shape,
    DROP COLUMN execution_authority_kind;
ALTER TABLE visible_completions
    DROP CONSTRAINT visible_completions_organization_id_project_id_authority_l_fkey;
ALTER TABLE debug_dumps
    DROP CONSTRAINT debug_dumps_worker_id_fkey;

-- Parent Job/Attempt authority is now unconditionally Stage graph authority.
ALTER TABLE attempts
    DROP CONSTRAINT attempts_authority_shape,
    DROP CONSTRAINT attempts_graph_state_projection,
    DROP COLUMN execution_profile_revision_id,
    DROP COLUMN worker_pool_id,
    DROP COLUMN worker_id,
    DROP COLUMN worker_epoch,
    DROP COLUMN assigned_at,
    DROP COLUMN profile_certification_id,
    DROP COLUMN scheduler_dispatch_intent_id,
    DROP COLUMN execution_authority_kind,
    ADD CONSTRAINT attempts_stage_graph_authority_required CHECK (
        graph_state IS NOT NULL AND execution_graph_snapshot_id IS NOT NULL
    );

ALTER TABLE jobs
    DROP CONSTRAINT jobs_execution_authority_shape,
    DROP COLUMN worker_pool_id,
    DROP COLUMN execution_authority_kind,
    ALTER COLUMN stage_cutover_revision_id SET NOT NULL,
    ALTER COLUMN execution_graph_revision_id SET NOT NULL,
    ALTER COLUMN stage_execution_profile_revision_id SET NOT NULL;

ALTER TABLE execution_profile_revisions
    DROP CONSTRAINT execution_profile_revisions_authority_shape,
    DROP COLUMN worker_pool_id,
    ALTER COLUMN execution_graph_revision_id SET NOT NULL;

-- Legacy relation dependency order. Business Job/Attempt rows, immutable
-- contraction evidence, ArtifactSet/Charge history, and Stage tables remain.
DROP VIEW vela_request_execution_lease_renewal_protocol;
DROP TABLE execution_lease_renewal_protocol;
DROP TABLE fleet_mutation_authorizations;
DROP TABLE fleet_retirement_completions;
DROP TABLE fleet_worker_pod_identity_bindings;
DROP TABLE worker_readiness_evidence;
DROP TABLE worker_readiness_cycles;
DROP TABLE worker_drain_operations;
DROP TABLE worker_capacity_conditions;
DROP TABLE worker_pool_capacity_conditions;
DROP TABLE worker_pool_capacity_policies;
DROP TABLE worker_profile_readiness;
DROP TABLE scheduler_dispatch_intents;
DROP TABLE scheduler_organization_deficits;
DROP TABLE scheduler_service_class_deficits;
DROP TABLE scheduler_project_deficits;
DROP TABLE scheduler_dispatch_protocol_transitions;
DROP TABLE scheduler_dispatch_protocol_state;
DROP TABLE fleet_assignment_protocol_transitions;
DROP TABLE fleet_assignment_protocol_state;
DROP TABLE project_capacity_shares;
DROP TABLE organization_capacity_shares;
DROP TABLE attempt_leases;
DROP TABLE worker_epochs;
DROP TABLE workers;
DROP TABLE worker_pools;

DROP TYPE execution_authority_kind;
DROP TYPE worker_profile_readiness_state;
DROP TYPE scheduler_candidate;
DROP TYPE scheduler_dispatch_state;
DROP TYPE worker_lifecycle_state;
DROP TYPE worker_reachability_condition;
DROP TYPE fleet_capacity_state;
DROP TYPE fleet_drain_state;
DROP TYPE fleet_readiness_check;
DROP TYPE fleet_readiness_state;
DROP TYPE fleet_scratch_watermark_state;
DROP TYPE fleet_protected_resource_kind;
-- Retained by WorkerInstance pod-mutation authorization.

CREATE FUNCTION vela_resolve_stage_job_execution_route(
    p_organization_id uuid,
    p_project_id uuid,
    p_model_revision_id uuid
) RETURNS TABLE (
    stage_cutover_revision_id uuid,
    execution_graph_revision_id uuid,
    execution_profile_revision_id uuid,
    reserved_storage_bytes bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_organization_id IS NULL OR p_project_id IS NULL
       OR p_model_revision_id IS NULL
       OR p_organization_id IS DISTINCT FROM public.vela_current_organization_id()
       OR p_project_id IS DISTINCT FROM public.vela_current_project_id() THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_route_request_scope_mismatch',
            MESSAGE = 'Stage route must match the authenticated request scope';
    END IF;
    RETURN QUERY
    SELECT revision.id, revision.execution_graph_revision_id,
        revision.execution_profile_revision_id, revision.reserved_storage_bytes
    FROM public.stage_cutover_control AS control
    JOIN public.stage_cutover_revisions AS revision
      ON revision.id = control.current_revision_id
    JOIN public.execution_graph_revisions AS graph
      ON graph.id = revision.execution_graph_revision_id
     AND graph.model_revision_id = revision.model_revision_id
     AND graph.state = 'ACTIVE'
    JOIN public.execution_profile_revisions AS profile
      ON profile.id = revision.execution_profile_revision_id
     AND profile.execution_graph_revision_id = graph.id
     AND profile.model_revision_id = graph.model_revision_id
     AND profile.state = 'ACTIVE'
    WHERE control.singleton
      AND revision.mode = 'STAGE_ONLY'
      AND revision.model_revision_id = p_model_revision_id
      AND revision.reserved_storage_bytes > 0
      AND (
          revision.scope = 'PRODUCTION'
          OR EXISTS (
              SELECT 1
              FROM public.stage_cutover_internal_projects AS binding
              WHERE binding.cutover_revision_id = revision.id
                AND binding.organization_id = p_organization_id
                AND binding.project_id = p_project_id
          )
      )
    FOR SHARE OF control, revision, graph, profile;
END
$$;
ALTER FUNCTION vela_resolve_stage_job_execution_route(uuid, uuid, uuid)
    OWNER TO vela_catalog_promotion_owner;
REVOKE ALL ON FUNCTION vela_resolve_stage_job_execution_route(uuid, uuid, uuid)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_resolve_stage_job_execution_route(uuid, uuid, uuid)
    TO vela_request;

CREATE FUNCTION vela_reject_remediation_operation_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Remediation operation identity is immutable'
            USING ERRCODE = '55000',
                  CONSTRAINT = 'remediation_operation_identity_is_immutable';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.legacy_worker_id IS DISTINCT FROM OLD.legacy_worker_id
        OR NEW.legacy_worker_epoch IS DISTINCT FROM OLD.legacy_worker_epoch
        OR NEW.worker_instance_id IS DISTINCT FROM OLD.worker_instance_id
        OR NEW.worker_instance_epoch IS DISTINCT FROM OLD.worker_instance_epoch
        OR NEW.node_identity IS DISTINCT FROM OLD.node_identity
        OR NEW.device_identity IS DISTINCT FROM OLD.device_identity
        OR NEW.failure_class IS DISTINCT FROM OLD.failure_class
        OR NEW.evidence_digest IS DISTINCT FROM OLD.evidence_digest
        OR NEW.certification_revision IS DISTINCT FROM OLD.certification_revision
        OR NEW.action_level IS DISTINCT FROM OLD.action_level
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.requested_by IS DISTINCT FROM OLD.requested_by
        OR NEW.requested_at IS DISTINCT FROM OLD.requested_at
        OR (NEW.deadline_at IS DISTINCT FROM OLD.deadline_at
            AND NOT (OLD.state = 'APPROVAL_REQUIRED' AND NEW.state = 'REQUESTED'))
    THEN
        RAISE EXCEPTION 'Remediation operation identity is immutable'
            USING ERRCODE = '55000',
                  CONSTRAINT = 'remediation_operation_identity_is_immutable';
    END IF;
    IF OLD.state IN ('SUCCEEDED', 'QUARANTINED')
       AND NEW.state IS DISTINCT FROM OLD.state THEN
        RAISE EXCEPTION 'terminal Remediation operation state is immutable'
            USING ERRCODE = '55000',
                  CONSTRAINT = 'remediation_operation_terminal_state_is_immutable';
    END IF;
    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'REQUESTED'
            AND NEW.state IN ('EXECUTING', 'APPROVAL_REQUIRED', 'QUARANTINED'))
        OR (OLD.state = 'APPROVAL_REQUIRED'
            AND NEW.state IN ('REQUESTED', 'QUARANTINED'))
        OR (OLD.state = 'EXECUTING'
            AND NEW.state IN ('SUCCEEDED', 'QUARANTINED'))
    ) THEN
        RAISE EXCEPTION 'invalid Remediation operation state transition from % to %',
            OLD.state, NEW.state
            USING ERRCODE = '23514',
                  CONSTRAINT = 'remediation_operation_state_transition_invalid';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_reject_remediation_operation_mutation()
    OWNER TO vela_remediation_owner;
REVOKE ALL ON FUNCTION vela_reject_remediation_operation_mutation() FROM PUBLIC;
CREATE TRIGGER remediation_operations_immutable
BEFORE UPDATE OR DELETE ON remediation_operations
FOR EACH ROW EXECUTE FUNCTION vela_reject_remediation_operation_mutation();

-- Remediation may revoke StageLease authority, but it cannot otherwise mutate
-- AttemptCoordinator-owned rows.
CREATE FUNCTION vela_revoke_stage_leases_for_remediation(
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_operation_id uuid
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_revoked integer;
BEGIN
    UPDATE public.stage_leases AS lease
    SET state = 'REVOKED',
        revoked_at = clock_timestamp(),
        revoke_reason = 'REMEDIATION:' || p_operation_id::text
    WHERE lease.worker_instance_id = p_worker_instance_id
      AND lease.worker_instance_epoch = p_worker_instance_epoch
      AND lease.state = 'ACTIVE';
    GET DIAGNOSTICS v_revoked = ROW_COUNT;
    RETURN v_revoked;
END
$$;
ALTER FUNCTION vela_revoke_stage_leases_for_remediation(uuid, bigint, uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_revoke_stage_leases_for_remediation(uuid, bigint, uuid)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_revoke_stage_leases_for_remediation(uuid, bigint, uuid)
    TO vela_remediation_owner;

CREATE FUNCTION vela_fence_remediation_target(
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_reason text,
    p_actor_identity text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM public.vela_fence_worker_instance(
        p_worker_instance_id, p_worker_instance_epoch, p_reason, p_actor_identity
    );
    UPDATE public.model_residencies AS residency
    SET state = 'FAILED',
        observed_at = clock_timestamp(),
        observed_by = p_actor_identity
    WHERE residency.worker_instance_id = p_worker_instance_id
      AND residency.worker_instance_epoch = p_worker_instance_epoch
      AND residency.state IN ('LOADING', 'WARMING', 'READY', 'DRAINING');
END
$$;
ALTER FUNCTION vela_fence_remediation_target(uuid, bigint, text, text)
    OWNER TO vela_remediation_owner;
REVOKE ALL ON FUNCTION vela_fence_remediation_target(uuid, bigint, text, text)
    FROM PUBLIC;

CREATE FUNCTION vela_request_remediation(
    p_operation_id uuid,
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_node_identity text,
    p_device_identity text,
    p_failure_class text,
    p_evidence_digest bytea,
    p_certification_revision text,
    p_action_level remediation_action_level,
    p_idempotency_key text,
    p_requested_by text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_instance_lifecycle_state,
    worker_reachability_condition worker_instance_reachability_state,
    requires_approval boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_existing public.remediation_operations%ROWTYPE;
    v_state remediation_operation_state;
    v_now timestamptz := clock_timestamp();
    v_deadline timestamptz;
BEGIN
    IF p_operation_id IS NULL OR p_worker_instance_id IS NULL
       OR p_worker_instance_epoch IS NULL OR p_worker_instance_epoch <= 0
       OR p_node_identity IS NULL OR length(p_node_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_node_identity) <> p_node_identity
       OR p_device_identity IS NULL OR length(p_device_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_device_identity) <> p_device_identity
       OR p_failure_class IS NULL OR length(p_failure_class) NOT BETWEEN 1 AND 200
       OR btrim(p_failure_class) <> p_failure_class
       OR p_evidence_digest IS NULL OR octet_length(p_evidence_digest) <> 32
       OR p_certification_revision IS NULL OR length(p_certification_revision) > 200
       OR btrim(p_certification_revision) <> p_certification_revision
       OR p_idempotency_key IS NULL OR length(p_idempotency_key) NOT BETWEEN 1 AND 200
       OR btrim(p_idempotency_key) <> p_idempotency_key
       OR p_requested_by IS NULL OR length(p_requested_by) NOT BETWEEN 1 AND 500
       OR btrim(p_requested_by) <> p_requested_by THEN
        RAISE EXCEPTION 'invalid Remediation operation request' USING ERRCODE = '22023';
    END IF;
    IF p_action_level <> 'L7_QUARANTINE' AND p_certification_revision = '' THEN
        RAISE EXCEPTION 'automatic Remediation requires a certification revision'
            USING ERRCODE = '22023';
    END IF;

    SELECT worker.* INTO STRICT v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    SELECT operation.* INTO v_existing
    FROM public.remediation_operations AS operation
    WHERE operation.worker_instance_id = p_worker_instance_id
      AND operation.worker_instance_epoch = p_worker_instance_epoch
      AND operation.idempotency_key = p_idempotency_key
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.id IS DISTINCT FROM p_operation_id
           OR v_existing.node_identity IS DISTINCT FROM p_node_identity
           OR v_existing.device_identity IS DISTINCT FROM p_device_identity
           OR v_existing.failure_class IS DISTINCT FROM p_failure_class
           OR v_existing.evidence_digest IS DISTINCT FROM p_evidence_digest
           OR v_existing.certification_revision IS DISTINCT FROM p_certification_revision
           OR v_existing.action_level IS DISTINCT FROM p_action_level
           OR v_existing.requested_by IS DISTINCT FROM p_requested_by THEN
            RAISE EXCEPTION 'Remediation idempotency key conflicts with committed input'
                USING ERRCODE = 'P0001',
                      CONSTRAINT = 'remediation_idempotency_conflict';
        END IF;
        RETURN QUERY SELECT
            v_existing.id, true, v_existing.state, v_existing.action_level,
            v_worker.lifecycle_state, v_worker.reachability_state,
            v_existing.state = 'APPROVAL_REQUIRED';
        RETURN;
    END IF;
    IF v_worker.instance_epoch <> p_worker_instance_epoch
       OR v_worker.lifecycle_state IN ('FENCED', 'RETIRED')
       OR NOT EXISTS (
            SELECT 1
            FROM public.worker_members AS member
            JOIN public.compute_nodes AS node ON node.id = member.compute_node_id
            WHERE member.worker_instance_id = p_worker_instance_id
              AND member.worker_instance_epoch = p_worker_instance_epoch
              AND node.node_identity = p_node_identity
       ) THEN
        RAISE EXCEPTION 'Remediation WorkerInstance identity or epoch does not match current authority'
            USING ERRCODE = 'P0001',
                  CONSTRAINT = 'remediation_worker_instance_identity_mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.remediation_operations AS operation
        WHERE operation.worker_instance_id = p_worker_instance_id
          AND operation.worker_instance_epoch = p_worker_instance_epoch
          AND operation.state IN ('REQUESTED', 'APPROVAL_REQUIRED', 'EXECUTING')
    ) THEN
        RAISE EXCEPTION 'WorkerInstance already has an active Remediation operation'
            USING ERRCODE = 'P0001',
                  CONSTRAINT = 'remediation_worker_instance_operation_active';
    END IF;

    PERFORM public.vela_revoke_stage_leases_for_remediation(
        p_worker_instance_id, p_worker_instance_epoch, p_operation_id
    );
    IF p_action_level = 'L7_QUARANTINE' THEN
        v_state := 'QUARANTINED';
        v_deadline := v_now + interval '15 minutes';
    ELSIF p_action_level = 'L6_BMC_POWER_CYCLE' THEN
        v_state := 'APPROVAL_REQUIRED';
        v_deadline := v_now + interval '5 minutes';
    ELSE
        v_state := 'REQUESTED';
        v_deadline := v_now + interval '15 minutes';
    END IF;
    INSERT INTO public.remediation_operations (
        id, worker_instance_id, worker_instance_epoch,
        node_identity, device_identity, failure_class,
        evidence_digest, certification_revision, action_level, idempotency_key,
        requested_by, state, requested_at, deadline_at,
        started_at, finished_at, result_code
    ) VALUES (
        p_operation_id, p_worker_instance_id, p_worker_instance_epoch,
        p_node_identity, p_device_identity, p_failure_class,
        p_evidence_digest, p_certification_revision, p_action_level,
        p_idempotency_key, p_requested_by, v_state, v_now, v_deadline,
        CASE WHEN v_state = 'QUARANTINED' THEN v_now END,
        CASE WHEN v_state = 'QUARANTINED' THEN v_now END,
        CASE WHEN v_state = 'QUARANTINED' THEN 'QUARANTINED_BY_POLICY' END
    );
    IF v_state = 'QUARANTINED' THEN
        PERFORM public.vela_fence_remediation_target(
            p_worker_instance_id, p_worker_instance_epoch,
            'L7_QUARANTINE', p_requested_by
        );
        v_worker.lifecycle_state := 'FENCED';
        v_worker.reachability_state := 'UNREACHABLE';
    ELSE
        UPDATE public.worker_instances AS worker
        SET lifecycle_state = 'DRAINING',
            reachability_state = 'DISCONNECTED',
            observed_at = v_now,
            observed_by = p_requested_by
        WHERE worker.id = p_worker_instance_id;
        v_worker.lifecycle_state := 'DRAINING';
        v_worker.reachability_state := 'DISCONNECTED';
    END IF;
    PERFORM public.vela_remediation_event(
        p_operation_id, 1, NULL, v_state, p_requested_by,
        CASE WHEN v_state = 'QUARANTINED' THEN 'QUARANTINED_BY_POLICY' END
    );
    RETURN QUERY SELECT
        p_operation_id, false, v_state, p_action_level,
        v_worker.lifecycle_state, v_worker.reachability_state,
        v_state = 'APPROVAL_REQUIRED';
END
$$;

CREATE FUNCTION vela_approve_remediation(
    p_operation_id uuid,
    p_approver_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    approval_count integer,
    requires_approval boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
    v_count integer;
    v_replayed boolean := false;
BEGIN
    IF p_operation_id IS NULL OR p_approver_identity IS NULL
       OR length(p_approver_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_approver_identity) <> p_approver_identity THEN
        RAISE EXCEPTION 'invalid Remediation approval' USING ERRCODE = '22023';
    END IF;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    IF v_operation.worker_instance_id IS NULL THEN
        RAISE EXCEPTION 'Archived Legacy Remediation operation is read-only'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_legacy_archive_read_only';
    END IF;
    SELECT worker.* INTO STRICT v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_operation.worker_instance_id
    FOR UPDATE;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    SELECT (CASE WHEN v_operation.first_approver IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN v_operation.second_approver IS NOT NULL THEN 1 ELSE 0 END)
    INTO v_count;
    IF v_operation.action_level <> 'L6_BMC_POWER_CYCLE' THEN
        RAISE EXCEPTION 'only L6 BMC power cycle operations require approval'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_not_required';
    END IF;
    IF v_operation.state IN ('EXECUTING', 'SUCCEEDED', 'QUARANTINED') THEN
        RETURN QUERY SELECT p_operation_id, true, v_operation.state, v_count, false;
        RETURN;
    END IF;
    IF v_worker.instance_epoch <> v_operation.worker_instance_epoch
       OR v_worker.lifecycle_state IN ('FENCED', 'RETIRED') THEN
        RAISE EXCEPTION 'Remediation approval WorkerInstance identity is invalid'
            USING ERRCODE = 'P0001',
                  CONSTRAINT = 'remediation_approval_identity_mismatch';
    END IF;
    IF v_operation.deadline_at <= v_now THEN
        UPDATE public.remediation_operations
        SET state = 'QUARANTINED',
            started_at = COALESCE(started_at, v_now),
            finished_at = v_now,
            result_code = 'REMEDIATION_DEADLINE_EXPIRED',
            result_detail = 'Remediation approval deadline expired'
        WHERE id = p_operation_id;
        PERFORM public.vela_fence_remediation_target(
            v_operation.worker_instance_id, v_operation.worker_instance_epoch,
            'REMEDIATION_DEADLINE_EXPIRED', p_approver_identity
        );
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, 'QUARANTINED',
            p_approver_identity, 'REMEDIATION_DEADLINE_EXPIRED'
        );
        RETURN QUERY SELECT p_operation_id, false,
            'QUARANTINED'::remediation_operation_state, v_count, false;
        RETURN;
    END IF;
    IF v_operation.first_approver = p_approver_identity
       OR v_operation.second_approver = p_approver_identity THEN
        v_replayed := true;
    ELSIF v_operation.first_approver IS NULL THEN
        UPDATE public.remediation_operations
        SET first_approver = p_approver_identity, approved_at = v_now
        WHERE id = p_operation_id;
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, v_operation.state,
            p_approver_identity, 'FIRST_APPROVAL_RECORDED'
        );
    ELSIF v_operation.second_approver IS NULL THEN
        UPDATE public.remediation_operations
        SET second_approver = p_approver_identity,
            approved_at = v_now,
            state = 'REQUESTED',
            deadline_at = v_now + interval '15 minutes'
        WHERE id = p_operation_id;
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, 'REQUESTED',
            p_approver_identity, 'SECOND_APPROVAL_RECORDED'
        );
    ELSE
        RAISE EXCEPTION 'Remediation operation already has two approvals'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_complete';
    END IF;
    SELECT operation.state,
        (CASE WHEN operation.first_approver IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN operation.second_approver IS NOT NULL THEN 1 ELSE 0 END)
    INTO v_operation.state, v_count
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    RETURN QUERY SELECT p_operation_id, v_replayed, v_operation.state, v_count,
        v_operation.state = 'APPROVAL_REQUIRED';
END
$$;

CREATE FUNCTION vela_start_remediation(
    p_operation_id uuid,
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_instance_lifecycle_state,
    worker_reachability_condition worker_instance_reachability_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
BEGIN
    IF p_operation_id IS NULL OR p_worker_instance_id IS NULL
       OR p_worker_instance_epoch IS NULL OR p_worker_instance_epoch <= 0
       OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_actor_identity) <> p_actor_identity THEN
        RAISE EXCEPTION 'invalid Remediation start' USING ERRCODE = '22023';
    END IF;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    SELECT worker.* INTO STRICT v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_operation.worker_instance_id
    FOR UPDATE;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    IF v_operation.worker_instance_id <> p_worker_instance_id
       OR v_operation.worker_instance_epoch <> p_worker_instance_epoch
       OR v_worker.instance_epoch <> p_worker_instance_epoch
       OR v_worker.lifecycle_state <> 'DRAINING' THEN
        RAISE EXCEPTION 'Remediation start identity or epoch does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_start_identity_mismatch';
    END IF;
    IF v_operation.state IN ('REQUESTED', 'APPROVAL_REQUIRED', 'EXECUTING')
       AND v_operation.deadline_at <= v_now THEN
        UPDATE public.remediation_operations
        SET state = 'QUARANTINED',
            started_at = COALESCE(started_at, v_now),
            finished_at = v_now,
            result_code = 'REMEDIATION_DEADLINE_EXPIRED',
            result_detail = 'Remediation execution deadline expired'
        WHERE id = p_operation_id;
        PERFORM public.vela_fence_remediation_target(
            p_worker_instance_id, p_worker_instance_epoch,
            'REMEDIATION_DEADLINE_EXPIRED', p_actor_identity
        );
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, 'QUARANTINED',
            p_actor_identity, 'REMEDIATION_DEADLINE_EXPIRED'
        );
        RETURN QUERY SELECT p_operation_id, false,
            'QUARANTINED'::remediation_operation_state, v_operation.action_level,
            'FENCED'::worker_instance_lifecycle_state,
            'UNREACHABLE'::worker_instance_reachability_state;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.stage_leases AS lease
        WHERE lease.worker_instance_id = p_worker_instance_id
          AND lease.worker_instance_epoch = p_worker_instance_epoch
          AND lease.state = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION 'Remediation cannot execute before current StageLease is fenced'
            USING ERRCODE = 'P0001',
                  CONSTRAINT = 'remediation_active_stage_lease_not_fenced';
    END IF;
    IF v_operation.state = 'EXECUTING' THEN
        RETURN QUERY SELECT v_operation.id, true, v_operation.state,
            v_operation.action_level, v_worker.lifecycle_state,
            v_worker.reachability_state;
        RETURN;
    END IF;
    IF v_operation.state <> 'REQUESTED' THEN
        RAISE EXCEPTION 'Remediation operation is not ready to execute'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_start_state_invalid';
    END IF;
    IF v_operation.action_level = 'L6_BMC_POWER_CYCLE'
       AND (v_operation.first_approver IS NULL
            OR v_operation.second_approver IS NULL
            OR v_operation.first_approver = v_operation.second_approver) THEN
        RAISE EXCEPTION 'L6 BMC power cycle requires two distinct approvals'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_incomplete';
    END IF;
    UPDATE public.remediation_operations
    SET state = 'EXECUTING', started_at = v_now
    WHERE id = p_operation_id;
    SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
    FROM public.remediation_operation_events AS event
    WHERE event.operation_id = p_operation_id;
    PERFORM public.vela_remediation_event(
        p_operation_id, v_sequence, v_operation.state, 'EXECUTING',
        p_actor_identity, 'EXECUTION_STARTED'
    );
    RETURN QUERY SELECT v_operation.id, false,
        'EXECUTING'::remediation_operation_state, v_operation.action_level,
        v_worker.lifecycle_state, v_worker.reachability_state;
END
$$;

CREATE FUNCTION vela_claim_remediation_execution(
    p_operation_id uuid,
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_claim_id uuid,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    claim_id uuid,
    replayed boolean,
    deadline_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_claim public.remediation_execution_claims%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_operation_id IS NULL OR p_worker_instance_id IS NULL
       OR p_worker_instance_epoch IS NULL OR p_worker_instance_epoch <= 0
       OR p_claim_id IS NULL
       OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_actor_identity) <> p_actor_identity THEN
        RAISE EXCEPTION 'invalid Remediation execution claim' USING ERRCODE = '22023';
    END IF;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    SELECT worker.* INTO STRICT v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_operation.worker_instance_id
    FOR UPDATE;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    IF v_operation.worker_instance_id <> p_worker_instance_id
       OR v_operation.worker_instance_epoch <> p_worker_instance_epoch
       OR v_worker.instance_epoch <> p_worker_instance_epoch
       OR v_worker.lifecycle_state <> 'DRAINING' THEN
        RAISE EXCEPTION 'Remediation execution claim identity does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_identity_mismatch';
    END IF;
    IF v_operation.state <> 'EXECUTING' THEN
        RAISE EXCEPTION 'Remediation operation is not executing'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_state_invalid';
    END IF;
    IF v_operation.deadline_at <= v_now THEN
        RAISE EXCEPTION 'Remediation execution deadline expired'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_deadline_expired';
    END IF;
    SELECT claim.* INTO v_claim
    FROM public.remediation_execution_claims AS claim
    WHERE claim.operation_id = p_operation_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_claim.claim_id <> p_claim_id
           OR v_claim.worker_instance_id <> p_worker_instance_id
           OR v_claim.worker_instance_epoch <> p_worker_instance_epoch
           OR v_claim.actor_identity <> p_actor_identity THEN
            RAISE EXCEPTION 'Remediation operation already has a conflicting execution claim'
                USING ERRCODE = 'P0001',
                      CONSTRAINT = 'remediation_execution_claim_exists';
        END IF;
        RETURN QUERY SELECT
            v_claim.operation_id, v_claim.claim_id, true, v_operation.deadline_at;
        RETURN;
    END IF;
    INSERT INTO public.remediation_execution_claims (
        operation_id, claim_id, worker_instance_id, worker_instance_epoch,
        actor_identity, claimed_at
    ) VALUES (
        p_operation_id, p_claim_id, p_worker_instance_id,
        p_worker_instance_epoch, p_actor_identity, v_now
    );
    RETURN QUERY SELECT p_operation_id, p_claim_id, false, v_operation.deadline_at;
END
$$;

CREATE FUNCTION vela_complete_remediation(
    p_operation_id uuid,
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_success boolean,
    p_result_code text,
    p_result_detail text,
    p_postcheck_digest bytea,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_instance_lifecycle_state,
    worker_reachability_condition worker_instance_reachability_state,
    result_code text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
    v_final_state remediation_operation_state;
    v_result_code text := p_result_code;
BEGIN
    IF p_operation_id IS NULL OR p_worker_instance_id IS NULL
       OR p_worker_instance_epoch IS NULL OR p_worker_instance_epoch <= 0
       OR p_success IS NULL
       OR p_result_code IS NULL OR length(p_result_code) NOT BETWEEN 1 AND 200
       OR btrim(p_result_code) <> p_result_code
       OR p_result_detail IS NULL OR length(p_result_detail) NOT BETWEEN 1 AND 2000
       OR btrim(p_result_detail) <> p_result_detail
       OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_actor_identity) <> p_actor_identity THEN
        RAISE EXCEPTION 'invalid Remediation completion' USING ERRCODE = '22023';
    END IF;
    IF p_success AND (p_postcheck_digest IS NULL
                      OR octet_length(p_postcheck_digest) <> 32) THEN
        RAISE EXCEPTION 'successful Remediation completion requires a post-check digest'
            USING ERRCODE = '22023';
    END IF;
    IF p_postcheck_digest IS NOT NULL
       AND octet_length(p_postcheck_digest) <> 32 THEN
        RAISE EXCEPTION 'invalid Remediation post-check digest' USING ERRCODE = '22023';
    END IF;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    SELECT worker.* INTO STRICT v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_operation.worker_instance_id
    FOR UPDATE;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    IF v_operation.state IN ('SUCCEEDED', 'QUARANTINED') THEN
        RETURN QUERY SELECT v_operation.id, true, v_operation.state,
            v_operation.action_level, v_worker.lifecycle_state,
            v_worker.reachability_state, v_operation.result_code;
        RETURN;
    END IF;
    IF v_operation.worker_instance_id <> p_worker_instance_id
       OR v_operation.worker_instance_epoch <> p_worker_instance_epoch
       OR v_worker.instance_epoch <> p_worker_instance_epoch
       OR v_worker.lifecycle_state <> 'DRAINING' THEN
        RAISE EXCEPTION 'Remediation completion identity does not match current authority'
            USING ERRCODE = 'P0001',
                  CONSTRAINT = 'remediation_completion_identity_mismatch';
    END IF;
    IF v_operation.state <> 'EXECUTING' THEN
        RAISE EXCEPTION 'Remediation operation is not executing'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_completion_state_invalid';
    END IF;
    IF v_operation.deadline_at <= v_now THEN
        p_success := false;
        v_result_code := 'REMEDIATION_DEADLINE_EXPIRED';
        p_result_detail := 'Remediation execution deadline expired';
    END IF;
    IF p_success AND EXISTS (
        SELECT 1 FROM public.stage_leases AS lease
        WHERE lease.worker_instance_id = p_worker_instance_id
          AND lease.worker_instance_epoch = p_worker_instance_epoch
          AND lease.state = 'ACTIVE'
    ) THEN
        p_success := false;
        v_result_code := 'ACTIVE_STAGE_LEASE_REMAINS';
        p_result_detail := 'WorkerInstance still owns an active StageLease after remediation';
    END IF;
    IF p_success THEN
        v_final_state := 'SUCCEEDED';
        UPDATE public.worker_instances AS worker
        SET lifecycle_state = 'READY',
            reachability_state = 'CONNECTED',
            observed_at = v_now,
            observed_by = p_actor_identity
        WHERE worker.id = p_worker_instance_id;
    ELSE
        v_final_state := 'QUARANTINED';
    END IF;
    UPDATE public.remediation_operations
    SET state = v_final_state,
        finished_at = v_now,
        result_code = v_result_code,
        result_detail = p_result_detail,
        postcheck_digest = p_postcheck_digest
    WHERE id = p_operation_id;
    IF NOT p_success THEN
        PERFORM public.vela_fence_remediation_target(
            p_worker_instance_id, p_worker_instance_epoch,
            v_result_code, p_actor_identity
        );
    END IF;
    SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
    FROM public.remediation_operation_events AS event
    WHERE event.operation_id = p_operation_id;
    PERFORM public.vela_remediation_event(
        p_operation_id, v_sequence, v_operation.state, v_final_state,
        p_actor_identity, v_result_code
    );
    RETURN QUERY SELECT p_operation_id, false, v_final_state,
        v_operation.action_level,
        CASE WHEN p_success THEN 'READY'::worker_instance_lifecycle_state
             ELSE 'FENCED'::worker_instance_lifecycle_state END,
        CASE WHEN p_success THEN 'CONNECTED'::worker_instance_reachability_state
             ELSE 'UNREACHABLE'::worker_instance_reachability_state END,
        v_result_code;
END
$$;

CREATE FUNCTION vela_recover_remediation(
    p_operation_id uuid,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_instance_lifecycle_state,
    worker_reachability_condition worker_instance_reachability_state,
    result_code text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
BEGIN
    IF p_operation_id IS NULL OR p_actor_identity IS NULL
       OR length(p_actor_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_actor_identity) <> p_actor_identity THEN
        RAISE EXCEPTION 'invalid Remediation recovery' USING ERRCODE = '22023';
    END IF;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    IF v_operation.worker_instance_id IS NULL THEN
        RAISE EXCEPTION 'Archived Legacy Remediation operation is read-only'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_legacy_archive_read_only';
    END IF;
    SELECT worker.* INTO STRICT v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_operation.worker_instance_id
    FOR UPDATE;
    SELECT operation.* INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    IF v_operation.state IN ('SUCCEEDED', 'QUARANTINED') THEN
        RETURN QUERY SELECT p_operation_id, true, v_operation.state,
            v_operation.action_level, v_worker.lifecycle_state,
            v_worker.reachability_state, v_operation.result_code;
        RETURN;
    END IF;
    IF v_worker.instance_epoch <> v_operation.worker_instance_epoch THEN
        RAISE EXCEPTION 'Remediation recovery identity does not match current authority'
            USING ERRCODE = 'P0001',
                  CONSTRAINT = 'remediation_recovery_identity_mismatch';
    END IF;
    IF v_operation.deadline_at > v_now THEN
        RAISE EXCEPTION 'Remediation operation deadline has not expired'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_deadline_not_reached';
    END IF;
    UPDATE public.remediation_operations
    SET state = 'QUARANTINED',
        started_at = COALESCE(started_at, v_now),
        finished_at = v_now,
        result_code = 'REMEDIATION_DEADLINE_EXPIRED',
        result_detail = 'Remediation operation recovered after deadline'
    WHERE id = p_operation_id;
    PERFORM public.vela_fence_remediation_target(
        v_operation.worker_instance_id, v_operation.worker_instance_epoch,
        'REMEDIATION_DEADLINE_EXPIRED', p_actor_identity
    );
    SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
    FROM public.remediation_operation_events AS event
    WHERE event.operation_id = p_operation_id;
    PERFORM public.vela_remediation_event(
        p_operation_id, v_sequence, v_operation.state, 'QUARANTINED',
        p_actor_identity, 'REMEDIATION_DEADLINE_EXPIRED'
    );
    RETURN QUERY SELECT p_operation_id, false,
        'QUARANTINED'::remediation_operation_state, v_operation.action_level,
        'FENCED'::worker_instance_lifecycle_state,
        'UNREACHABLE'::worker_instance_reachability_state,
        'REMEDIATION_DEADLINE_EXPIRED'::text;
END
$$;

CREATE FUNCTION vela_get_remediation_operation(p_operation_id uuid)
RETURNS TABLE (
    operation_id uuid,
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    node_identity text,
    device_identity text,
    failure_class text,
    evidence_digest bytea,
    certification_revision text,
    action_level remediation_action_level,
    idempotency_key text,
    requested_by text,
    state remediation_operation_state,
    requested_at timestamptz,
    deadline_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    result_code text,
    result_detail text,
    postcheck_digest bytea,
    first_approver text,
    second_approver text,
    approved_at timestamptz
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT operation.id, operation.worker_instance_id,
        operation.worker_instance_epoch, operation.node_identity,
        operation.device_identity, operation.failure_class,
        operation.evidence_digest, operation.certification_revision,
        operation.action_level, operation.idempotency_key,
        operation.requested_by, operation.state, operation.requested_at,
        operation.deadline_at, operation.started_at, operation.finished_at,
        operation.result_code, operation.result_detail,
        operation.postcheck_digest, operation.first_approver,
        operation.second_approver, operation.approved_at
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
      AND operation.worker_instance_id IS NOT NULL
$$;

CREATE FUNCTION vela_list_executing_remediation(p_limit integer)
RETURNS TABLE (
    operation_id uuid,
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    node_identity text,
    device_identity text,
    failure_class text,
    evidence_digest bytea,
    certification_revision text,
    action_level remediation_action_level,
    idempotency_key text,
    requested_by text,
    state remediation_operation_state,
    requested_at timestamptz,
    deadline_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    result_code text,
    result_detail text,
    postcheck_digest bytea,
    first_approver text,
    second_approver text,
    approved_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION 'invalid Remediation execution dispatch limit'
            USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
    SELECT operation.id, operation.worker_instance_id,
        operation.worker_instance_epoch, operation.node_identity,
        operation.device_identity, operation.failure_class,
        operation.evidence_digest, operation.certification_revision,
        operation.action_level, operation.idempotency_key,
        operation.requested_by, operation.state, operation.requested_at,
        operation.deadline_at, operation.started_at, operation.finished_at,
        operation.result_code, operation.result_detail,
        operation.postcheck_digest, operation.first_approver,
        operation.second_approver, operation.approved_at
    FROM public.remediation_operations AS operation
    WHERE operation.state = 'EXECUTING'
      AND operation.worker_instance_id IS NOT NULL
    ORDER BY operation.requested_at, operation.id
    LIMIT p_limit;
END
$$;

ALTER FUNCTION vela_request_remediation(
    uuid, uuid, bigint, text, text, text, bytea, text,
    remediation_action_level, text, text
) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_approve_remediation(uuid, text) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_start_remediation(uuid, uuid, bigint, text)
    OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_claim_remediation_execution(uuid, uuid, bigint, uuid, text)
    OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_complete_remediation(
    uuid, uuid, bigint, boolean, text, text, bytea, text
) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_recover_remediation(uuid, text) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_get_remediation_operation(uuid) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_list_executing_remediation(integer)
    OWNER TO vela_remediation_owner;

REVOKE ALL ON FUNCTION vela_request_remediation(
    uuid, uuid, bigint, text, text, text, bytea, text,
    remediation_action_level, text, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_approve_remediation(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_start_remediation(uuid, uuid, bigint, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_claim_remediation_execution(
    uuid, uuid, bigint, uuid, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_complete_remediation(
    uuid, uuid, bigint, boolean, text, text, bytea, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_recover_remediation(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_get_remediation_operation(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_list_executing_remediation(integer) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION vela_request_remediation(
    uuid, uuid, bigint, text, text, text, bytea, text,
    remediation_action_level, text, text
) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_approve_remediation(uuid, text) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_start_remediation(uuid, uuid, bigint, text)
    TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_claim_remediation_execution(
    uuid, uuid, bigint, uuid, text
) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_complete_remediation(
    uuid, uuid, bigint, boolean, text, text, bytea, text
) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_recover_remediation(uuid, text) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_get_remediation_operation(uuid) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_list_executing_remediation(integer)
    TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_fence_worker_instance(uuid, bigint, text, text)
    TO vela_remediation_owner;
GRANT SELECT, UPDATE ON worker_instances, model_residencies
    TO vela_remediation_owner;
GRANT SELECT ON worker_instance_epochs, worker_members, compute_nodes, stage_leases
    TO vela_remediation_owner;

CREATE FUNCTION vela_reject_job_snapshot_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        NEW.organization_id, NEW.project_id, NEW.created_by_principal_id,
        NEW.model_revision_id, NEW.generation_preset_revision_id,
        NEW.service_class_revision_id, NEW.output_spec_id,
        NEW.stage_cutover_revision_id, NEW.execution_graph_revision_id,
        NEW.stage_execution_profile_revision_id, NEW.request_hash,
        NEW.request_content, NEW.request_content_expires_at,
        NEW.pricing_rate_card_revision_id, NEW.pricing_rate_line_id,
        NEW.pricing_unit_amount_minor, NEW.pricing_quantity,
        NEW.pricing_quoted_amount_minor, NEW.pricing_currency,
        NEW.execution_max_attempts, NEW.execution_max_total_compute_seconds,
        NEW.execution_max_finalization_seconds_per_attempt,
        NEW.execution_retry_backoff_policy,
        NEW.execution_retryable_failure_classes,
        NEW.execution_circuit_breaker_policy, NEW.job_expires_at
    ) IS DISTINCT FROM ROW(
        OLD.organization_id, OLD.project_id, OLD.created_by_principal_id,
        OLD.model_revision_id, OLD.generation_preset_revision_id,
        OLD.service_class_revision_id, OLD.output_spec_id,
        OLD.stage_cutover_revision_id, OLD.execution_graph_revision_id,
        OLD.stage_execution_profile_revision_id, OLD.request_hash,
        OLD.request_content, OLD.request_content_expires_at,
        OLD.pricing_rate_card_revision_id, OLD.pricing_rate_line_id,
        OLD.pricing_unit_amount_minor, OLD.pricing_quantity,
        OLD.pricing_quoted_amount_minor, OLD.pricing_currency,
        OLD.execution_max_attempts, OLD.execution_max_total_compute_seconds,
        OLD.execution_max_finalization_seconds_per_attempt,
        OLD.execution_retry_backoff_policy,
        OLD.execution_retryable_failure_classes,
        OLD.execution_circuit_breaker_policy, OLD.job_expires_at
    ) THEN
        RAISE EXCEPTION 'immutable Job snapshot fields cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_reject_job_snapshot_mutation() OWNER TO vela_internal;
REVOKE ALL ON FUNCTION vela_reject_job_snapshot_mutation() FROM PUBLIC;
CREATE TRIGGER jobs_snapshot_immutable
BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_reject_job_snapshot_mutation();

CREATE FUNCTION vela_reject_attempt_identity_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        NEW.organization_id, NEW.project_id, NEW.job_id,
        NEW.attempt_number, NEW.fence, NEW.execution_graph_snapshot_id
    ) IS DISTINCT FROM ROW(
        OLD.organization_id, OLD.project_id, OLD.job_id,
        OLD.attempt_number, OLD.fence, OLD.execution_graph_snapshot_id
    ) THEN
        RAISE EXCEPTION 'immutable Attempt identity fields cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_reject_attempt_identity_mutation() OWNER TO vela_internal;
REVOKE ALL ON FUNCTION vela_reject_attempt_identity_mutation() FROM PUBLIC;
CREATE TRIGGER attempts_identity_immutable
BEFORE UPDATE ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_identity_mutation();

CREATE FUNCTION vela_guard_stage_graph_attempt_writer() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF current_user <> 'vela_attempt_coordinator_owner' THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'attempt_coordinator_writer_required',
            MESSAGE = 'Stage graph Attempt is writable only by AttemptCoordinator';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$$;
ALTER FUNCTION vela_guard_stage_graph_attempt_writer()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_guard_stage_graph_attempt_writer() FROM PUBLIC;
CREATE TRIGGER attempts_guard_stage_graph_writer
BEFORE INSERT OR UPDATE OR DELETE ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_graph_attempt_writer();

CREATE FUNCTION vela_enqueue_stage_graph_instantiation() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_reserved_storage_bytes bigint;
BEGIN
    SELECT revision.reserved_storage_bytes
    INTO STRICT v_reserved_storage_bytes
    FROM public.stage_cutover_revisions AS revision
    WHERE revision.id = NEW.stage_cutover_revision_id
      AND revision.execution_graph_revision_id = NEW.execution_graph_revision_id
      AND revision.execution_profile_revision_id = NEW.stage_execution_profile_revision_id;
    INSERT INTO public.stage_graph_instantiation_work (
        job_id, organization_id, project_id, stage_cutover_revision_id,
        command_id, expected_job_version, expected_job_fence,
        execution_graph_snapshot_id, execution_graph_revision_id,
        execution_profile_revision_id, attempt_id, storage_reservation_id,
        reserved_storage_bytes, source
    ) VALUES (
        NEW.id, NEW.organization_id, NEW.project_id, NEW.stage_cutover_revision_id,
        gen_random_uuid(), NEW.version, NEW.current_fence,
        gen_random_uuid(), NEW.execution_graph_revision_id,
        NEW.stage_execution_profile_revision_id, gen_random_uuid(), gen_random_uuid(),
        v_reserved_storage_bytes, 'ADMISSION_TRIGGER'
    );
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_enqueue_stage_graph_instantiation()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_enqueue_stage_graph_instantiation() FROM PUBLIC;
CREATE TRIGGER jobs_enqueue_stage_graph_instantiation
AFTER INSERT ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_enqueue_stage_graph_instantiation();

CREATE CONSTRAINT TRIGGER jobs_require_atomic_stage_graph_admission
AFTER INSERT ON jobs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_require_atomic_stage_graph_admission();

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'legacy_h3_schema_contraction_is_irreversible',
        MESSAGE = 'Legacy H3 schema contraction cannot be rolled back in place';
END
$$;
-- +goose StatementEnd
