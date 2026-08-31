package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Role string

const (
	RoleAuth                       Role = "vela_auth"
	RoleHumanAuth                  Role = "vela_human_auth"
	RoleHumanMembershipAuth        Role = "vela_human_membership_auth"
	RoleIdentityRequest            Role = "vela_identity_request"
	RoleHumanMembershipRequest     Role = "vela_human_membership_request"
	RoleOrganizationBillingRequest Role = "vela_organization_billing_request"
	RoleOrganizationAuditRequest   Role = "vela_organization_audit_request"
	RoleBreakGlassAuditRequest     Role = "vela_break_glass_audit_request"
	RoleRetentionRequest           Role = "vela_retention_request"
	RoleDebugDumpRequest           Role = "vela_debug_dump_request"
	RoleDebugDumpAuditRequest      Role = "vela_debug_dump_audit_request"
	RoleRetention                  Role = "vela_retention"
	RoleBackupRetention            Role = "vela_backup_retention"
	RoleArtifactReplication        Role = "vela_artifact_replication"
	RolePlatformOperatorAuth       Role = "vela_platform_operator_auth"
	RoleBreakGlassRequest          Role = "vela_break_glass_request"
	RoleRequest                    Role = "vela_request"
	RoleInternal                   Role = "vela_internal"
	RoleCancel                     Role = "vela_cancel"
	RoleArtifactRequest            Role = "vela_artifact_request"
	RoleScheduler                  Role = "vela_scheduler"
	RoleSchedulerInbox             Role = "vela_scheduler_inbox"
	RoleBilling                    Role = "vela_billing"
	RoleFinanceReconciliation      Role = "vela_finance_reconciliation"
	RoleCompliance                 Role = "vela_compliance"
	RoleNonContentExpiry           Role = "vela_non_content_expiry"
	RoleCatalogPromotion           Role = "vela_catalog_promotion"
	RoleStageCatalogActivation     Role = "vela_stage_catalog_activation"
	RoleSLOReporting               Role = "vela_slo_reporting"
	RoleWebhookRequest             Role = "vela_webhook_request"
	RoleWebhook                    Role = "vela_webhook"
	RoleRemediation                Role = "vela_remediation"
	RoleFleet                      Role = "vela_fleet"
	RoleAttemptCoordinator         Role = "vela_attempt_coordinator"
	RoleStageScheduler             Role = "vela_stage_scheduler"
	RoleStageArtifact              Role = "vela_stage_artifact"
	RoleStageWorkerControl         Role = "vela_stage_worker_control"
	RoleUsageCost                  Role = "vela_usage_cost"
	RoleH3CampaignEvidence         Role = "vela_h3_campaign_evidence"
)

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type roleDescriptor struct {
	requiresBypassRLS bool
	verifyPrivileges  func(context.Context, rowQuerier, string) error
}

var roleDescriptors = map[Role]roleDescriptor{
	RoleAuth:                       {verifyPrivileges: verifyAuthPrivileges},
	RoleHumanAuth:                  {verifyPrivileges: verifyHumanAuthPrivileges},
	RoleHumanMembershipAuth:        {verifyPrivileges: verifyHumanMembershipAuthPrivileges},
	RoleIdentityRequest:            {verifyPrivileges: verifyIdentityRequestPrivileges},
	RoleHumanMembershipRequest:     {verifyPrivileges: verifyHumanMembershipRequestPrivileges},
	RoleOrganizationBillingRequest: {verifyPrivileges: verifyOrganizationBillingRequestPrivileges},
	RoleOrganizationAuditRequest:   {verifyPrivileges: verifyOrganizationAuditRequestPrivileges},
	RoleBreakGlassAuditRequest:     {verifyPrivileges: verifyBreakGlassAuditRequestPrivileges},
	RoleRetentionRequest:           {verifyPrivileges: verifyRetentionRequestPrivileges},
	RoleDebugDumpRequest:           {verifyPrivileges: verifyDebugDumpRequestPrivileges},
	RoleDebugDumpAuditRequest:      {verifyPrivileges: verifyDebugDumpAuditRequestPrivileges},
	RoleRetention:                  {verifyPrivileges: verifyRetentionPrivileges},
	RoleBackupRetention:            {verifyPrivileges: verifyBackupRetentionPrivileges},
	RoleArtifactReplication:        {verifyPrivileges: verifyArtifactReplicationPrivileges},
	RolePlatformOperatorAuth:       {verifyPrivileges: verifyPlatformOperatorAuthPrivileges},
	RoleBreakGlassRequest:          {verifyPrivileges: verifyBreakGlassRequestPrivileges},
	RoleRequest:                    {verifyPrivileges: verifyRequestPrivileges},
	RoleInternal:                   {requiresBypassRLS: true},
	RoleCancel:                     {verifyPrivileges: verifyCancelPrivileges},
	RoleArtifactRequest:            {verifyPrivileges: verifyArtifactRequestPrivileges},
	RoleScheduler:                  {verifyPrivileges: verifySchedulerPrivileges},
	RoleSchedulerInbox:             {verifyPrivileges: verifySchedulerInboxPrivileges},
	RoleBilling:                    {verifyPrivileges: verifyBillingPrivileges},
	RoleFinanceReconciliation:      {verifyPrivileges: verifyFinanceReconciliationPrivileges},
	RoleCompliance:                 {verifyPrivileges: verifyCompliancePrivileges},
	RoleNonContentExpiry:           {verifyPrivileges: verifyNonContentExpiryPrivileges},
	RoleCatalogPromotion:           {verifyPrivileges: verifyCatalogPromotionPrivileges},
	RoleStageCatalogActivation:     {verifyPrivileges: verifyStageCatalogActivationPrivileges},
	RoleSLOReporting:               {verifyPrivileges: verifySLOReportingPrivileges},
	RoleWebhookRequest:             {verifyPrivileges: verifyWebhookRequestPrivileges},
	RoleWebhook:                    {verifyPrivileges: verifyWebhookPrivileges},
	RoleRemediation:                {verifyPrivileges: verifyRemediationPrivileges},
	RoleFleet:                      {verifyPrivileges: verifyFleetPrivileges},
	RoleAttemptCoordinator:         {verifyPrivileges: verifyAttemptCoordinatorPrivileges},
	RoleStageScheduler:             {verifyPrivileges: verifyStageSchedulerPrivileges},
	RoleStageArtifact:              {verifyPrivileges: verifyStageArtifactPrivileges},
	RoleStageWorkerControl:         {verifyPrivileges: verifyStageWorkerControlPrivileges},
	RoleUsageCost:                  {verifyPrivileges: verifyUsageCostPrivileges},
	RoleH3CampaignEvidence:         {verifyPrivileges: verifyH3CampaignEvidencePrivileges},
}

func VerifyRole(ctx context.Context, database rowQuerier, expected Role) error {
	if database == nil {
		return errors.New("database connection is required for role verification")
	}
	descriptor, supported := roleDescriptors[expected]
	if !supported {
		return fmt.Errorf("unsupported database role %q", expected)
	}

	var (
		currentUser          string
		superuser            bool
		createDatabase       bool
		createRole           bool
		replication          bool
		bypassRLS            bool
		membershipJSON       string
		unexpectedMembership bool
	)
	roleNames := make([]string, 0, len(roleDescriptors))
	for role := range roleDescriptors {
		roleNames = append(roleNames, string(role))
	}
	err := database.QueryRow(ctx, `
	        SELECT
            current_user,
            role.rolsuper,
            role.rolcreatedb,
            role.rolcreaterole,
            role.rolreplication,
            role.rolbypassrls,
			COALESCE((
				SELECT jsonb_agg(candidate.role_name ORDER BY candidate.role_name)::text
				FROM unnest($1::text[]) AS candidate(role_name)
				WHERE pg_has_role(current_user, candidate.role_name, 'MEMBER')
			), '[]'),
	            EXISTS (
	                SELECT 1
	                FROM pg_catalog.pg_roles AS inherited
	                WHERE inherited.rolname <> current_user
	                  AND inherited.rolname <> ALL($1::text[])
	                  AND pg_has_role(current_user, inherited.oid, 'SET')
            )
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = current_user
	    `, roleNames).Scan(
		&currentUser,
		&superuser,
		&createDatabase,
		&createRole,
		&replication,
		&bypassRLS,
		&membershipJSON,
		&unexpectedMembership,
	)
	if err != nil {
		return fmt.Errorf("inspect effective database role: %w", err)
	}
	if superuser || createDatabase || createRole || replication || unexpectedMembership {
		return fmt.Errorf("database login %q has forbidden cluster privileges or role inheritance", currentUser)
	}

	var inheritedRoles []Role
	if err := json.Unmarshal([]byte(membershipJSON), &inheritedRoles); err != nil {
		return fmt.Errorf("decode effective database role memberships: %w", err)
	}
	memberships := make(map[Role]bool, len(inheritedRoles))
	for _, role := range inheritedRoles {
		memberships[role] = true
	}
	if !memberships[expected] {
		return fmt.Errorf("database login %q is not a member of required role %q", currentUser, expected)
	}
	for role, member := range memberships {
		if role != expected && member {
			return fmt.Errorf("database login %q also belongs to forbidden role %q", currentUser, role)
		}
	}
	if descriptor.requiresBypassRLS != bypassRLS {
		if descriptor.requiresBypassRLS {
			return fmt.Errorf("database login %q must have BYPASSRLS for role %q", currentUser, expected)
		}
		return fmt.Errorf("database login %q must not have BYPASSRLS for role %q", currentUser, expected)
	}
	if descriptor.verifyPrivileges != nil {
		return descriptor.verifyPrivileges(ctx, database, currentUser)
	}
	return nil
}

type relationPrivilege struct {
	Relation  string
	Privilege string
}

type columnPrivilege struct {
	Relation  string
	Column    string
	Privilege string
}

type exactPrivilegeBoundary struct {
	inspectionLabel string
	failureLabel    string
	tables          []relationPrivilege
	columns         []columnPrivilege
	functions       []string
}

func databaseFunctionExists(
	ctx context.Context,
	database rowQuerier,
	signature string,
) (bool, error) {
	var exists bool
	if err := database.QueryRow(ctx, `
		SELECT to_regprocedure($1) IS NOT NULL
	`, signature).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func verifySchedulerPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Scheduler",
		failureLabel:    "Scheduler transaction",
		functions: []string{
			"vela_list_schedulable_worker_pools()",
			"vela_claim_scheduler_dispatch(uuid,text,integer)",
			"vela_abandon_scheduler_dispatch(uuid,text,text)",
			"vela_reconcile_expired_scheduler_dispatches()",
			"vela_predict_admission_capacity(uuid,uuid,uuid,uuid,uuid,integer)",
			"vela_predict_job_dynamic_eta(uuid)",
		},
	})
}

func verifyAttemptCoordinatorPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "AttemptCoordinator",
		failureLabel:    "AttemptCoordinator command",
		functions: []string{
			"vela_instantiate_stage_graph(jsonb)",
			"vela_apply_stage_command(jsonb)",
			"vela_reconcile_stage_graphs(integer)",
			"vela_claim_stage_graph_instantiations(text,uuid,integer,integer)",
			"vela_complete_stage_graph_instantiation(uuid,uuid,uuid,uuid,uuid)",
			"vela_release_stage_graph_instantiation(uuid,uuid,integer,text)",
			"vela_reconcile_stage_graph_instantiations(integer)",
			"vela_set_project_stage_cache_control(jsonb)",
			"vela_set_organization_stage_cache_authorization(jsonb)",
			"vela_admit_stage_cache_entry(jsonb)",
			"vela_hit_stage_cache(jsonb)",
			"vela_release_stage_cache_execution_pin(jsonb)",
			"vela_request_stage_cache_deletion(jsonb)",
			"vela_reconcile_stage_cache_deletions(timestamp with time zone,integer)",
		},
	})
}

func verifyStageSchedulerPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "StageScheduler",
		failureLabel:    "StageScheduler transaction",
		functions: []string{
			"vela_capture_stage_scheduler_snapshot(jsonb)",
			"vela_read_stage_scheduler_claim(uuid)",
			"vela_claim_stage_scheduler_decision(jsonb)",
			"vela_commit_stage_scheduler_claim(uuid,uuid)",
			"vela_abandon_stage_scheduler_claim(uuid,text)",
			"vela_reconcile_expired_stage_scheduler_claims(integer)",
			"vela_list_stage_scheduler_shadow_snapshots(integer)",
			"vela_record_stage_scheduler_shadow_replay(jsonb)",
		},
	})
}

func verifyStageArtifactPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "StageArtifact",
		failureLabel:    "StageArtifact transaction",
		functions: []string{
			"vela_seal_stage_output(jsonb)",
			"vela_is_stage_materialization_authority_active(jsonb)",
			"vela_commit_stage_artifact(jsonb)",
			"vela_fail_stage_materialization_source(jsonb)",
			"vela_issue_stage_transfer_ticket(jsonb)",
			"vela_resolve_stage_transfer_ticket(jsonb)",
			"vela_consume_stage_transfer_ticket(jsonb)",
		},
	})
}

func verifyStageWorkerControlPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "StageWorkerControl",
		failureLabel:    "StageWorkerControl authority snapshot",
		functions: []string{
			"vela_begin_stage_worker_acquire(jsonb)",
			"vela_read_stage_worker_acquire_authority(uuid)",
			"vela_read_stage_assignment_execution(uuid,uuid)",
			"vela_complete_stage_worker_acquire(jsonb)",
			"vela_read_stage_authority_snapshot(uuid,bigint)",
			"vela_start_stage_worker_command(jsonb)",
			"vela_heartbeat_stage_worker_command(jsonb)",
			"vela_reattach_stage_worker_command(jsonb)",
			"vela_verify_stage_worker_registration(jsonb)",
			"vela_verify_stage_capacity_observation(jsonb)",
		},
	})
}

func verifyUsageCostPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Usage/Cost Ledger",
		failureLabel:    "Usage/Cost Ledger command",
		functions: []string{
			"vela_record_resource_usage(jsonb)",
			"vela_value_resource_usage(jsonb)",
			"vela_summarize_usage_cost(uuid,timestamp with time zone,timestamp with time zone)",
		},
	})
}

func verifyH3CampaignEvidencePrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	relations := []string{
		"artifact_sets",
		"attempts",
		"charges",
		"compute_nodes",
		"devices",
		"jobs",
		"stage_allocations",
		"stage_artifact_inputs",
		"stage_artifact_pins",
		"stage_artifacts",
		"stage_attempts",
		"stage_cache_entries",
		"stage_cache_references",
		"stage_dependencies",
		"stage_run_output_bindings",
		"stage_runs",
		"transfer_tickets",
		"visible_completions",
		"worker_instances",
		"worker_member_devices",
		"worker_members",
	}
	tables := make([]relationPrivilege, 0, len(relations))
	for _, relation := range relations {
		tables = append(tables, relationPrivilege{Relation: relation, Privilege: "SELECT"})
	}
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "H3 campaign evidence",
		failureLabel:    "H3 campaign evidence read-only capture",
		tables:          tables,
	})
}

func verifySchedulerInboxPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Scheduler Inbox",
		failureLabel:    "Scheduler Inbox receipt",
		functions: []string{
			"vela_prepare_scheduler_inbox_receipt(uuid,uuid,uuid,uuid,bigint)",
			"vela_record_scheduler_inbox_receipt(uuid,uuid,uuid,uuid,bigint)",
		},
	})
}

func verifyBillingPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "billing",
		failureLabel:    "Invoice export transaction",
		functions: []string{
			"vela_claim_invoice_exports(text,uuid,integer,integer)",
			"vela_mark_invoice_exported(uuid,uuid,uuid,text,text)",
			"vela_mark_invoice_export_failed(uuid,uuid,integer,text)",
		},
	})
}

func verifyFinanceReconciliationPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Finance Reconciliation",
		failureLabel:    "Finance Reconciliation transaction",
		functions: []string{
			"vela_get_finance_reconciliation_identity()",
			"vela_apply_finance_reconciliation(uuid,text,bigint,uuid,finance_reconciliation_kind,text,bigint,bigint,bigint,text,timestamp with time zone)",
		},
	})
}

func verifyCompliancePrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Compliance",
		failureLabel:    "Legal Hold transaction",
		functions: []string{
			"vela_get_compliance_identity()",
			"vela_apply_legal_hold_event(uuid,text,bigint,uuid,legal_hold_event_kind,legal_hold_scope,uuid,uuid,uuid,legal_hold_record_class[],text,text,timestamp with time zone)",
		},
	})
}

func verifyNonContentExpiryPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "non-content expiry",
		failureLabel:    "non-content expiry transaction",
		functions: []string{
			"vela_claim_non_content_expiry(text,uuid,integer)",
			"vela_complete_non_content_expiry(non_content_expiry_kind,uuid,uuid,integer)",
		},
	})
}

func verifyCatalogPromotionPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	boundary := exactPrivilegeBoundary{
		inspectionLabel: "Catalog Promotion",
		failureLabel:    "Catalog Promotion transaction",
		functions: []string{
			"vela_record_production_gate_receipt(uuid,integer,production_gate,bytea,text,text,production_gate_result,text,text,text,text,bytea,bytea,timestamp with time zone,timestamp with time zone,timestamp with time zone)",
			"vela_seal_production_gate_manifest(bytea)",
			"vela_promote_profile_certification(uuid,uuid,uuid,text,text,integer,integer,integer,integer,bigint,bigint,bigint,bigint,bigint,text,integer,integer,uuid)",
			"vela_promote_rate_card(uuid,uuid,uuid)",
			"vela_enable_evidenced_catalog(uuid)",
		},
	}
	stageCutoverPrivilegesPresent, err := databaseFunctionExists(
		ctx,
		database,
		"vela_execution_profile_connector_set_digest(uuid,uuid)",
	)
	if err != nil {
		return fmt.Errorf("inspect Catalog Promotion Stage cutover surface: %w", err)
	}
	if stageCutoverPrivilegesPresent {
		boundary.functions = append(boundary.functions,
			"vela_execution_profile_connector_set_digest(uuid,uuid)",
			"vela_activate_stage_cutover(uuid,bigint,uuid,stage_cutover_scope,stage_cutover_mode,integer,uuid,uuid,bigint,integer,bytea,text,bytea,bytea,bytea,text,text)",
			"vela_authorize_stage_cutover_internal_project(uuid,uuid,uuid,text)",
			"vela_capture_legacy_authority_inventory(uuid,text)",
		)
	}
	zeroBacklogPrivilegesPresent, err := databaseFunctionExists(
		ctx,
		database,
		"vela_record_stage_cutover_external_drain_evidence(uuid,bigint,bigint,bigint,bigint,bigint,bytea,text)",
	)
	if err != nil {
		return fmt.Errorf("inspect Catalog Promotion zero-backlog surface: %w", err)
	}
	if zeroBacklogPrivilegesPresent {
		boundary.functions = append(boundary.functions,
			"vela_record_stage_cutover_external_drain_evidence(uuid,bigint,bigint,bigint,bigint,bigint,bytea,text)",
			"vela_seal_stage_cutover_zero_backlog(uuid,uuid,uuid,uuid,uuid,text)",
		)
	}
	contractionPreparationPresent, err := databaseFunctionExists(
		ctx,
		database,
		"vela_prepare_legacy_h3_contraction(uuid,text)",
	)
	if err != nil {
		return fmt.Errorf("inspect Catalog Promotion Legacy H3 contraction surface: %w", err)
	}
	if contractionPreparationPresent {
		boundary.functions = append(
			boundary.functions,
			"vela_prepare_legacy_h3_contraction(uuid,text)",
		)
	}
	return verifyExactPrivileges(ctx, database, currentUser, boundary)
}

func verifyStageCatalogActivationPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Stage Catalog activation",
		failureLabel:    "Stage Catalog activation transaction",
		functions: []string{
			"vela_activate_execution_graph(uuid,bytea)",
		},
	})
}

func verifySLOReportingPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "statistical SLO reporting",
		failureLabel:    "statistical SLO reporting transaction",
		functions: []string{
			"vela_register_slo_contract(uuid,text,uuid,uuid,uuid,uuid,integer,bigint,integer,integer,uuid)",
			"vela_enable_slo_measurement(uuid)",
			"vela_seal_slo_measurement(uuid,uuid,timestamp with time zone,timestamp with time zone)",
			"vela_get_slo_measurement(uuid)",
		},
	})
}

func verifyWebhookPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "webhook",
		failureLabel:    "Webhook Delivery transaction",
		functions: []string{
			"vela_claim_webhook_deliveries(text,integer,integer)",
			"vela_mark_webhook_delivered(uuid,uuid,integer)",
			"vela_mark_webhook_failed(uuid,uuid,integer,text)",
		},
	})
}

func verifyRemediationPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Remediation",
		failureLabel:    "Remediation transaction",
		functions: []string{
			"vela_request_remediation(uuid,uuid,bigint,text,text,text,bytea,text,remediation_action_level,text,text)",
			"vela_approve_remediation(uuid,text)",
			"vela_start_remediation(uuid,uuid,bigint,text)",
			"vela_claim_remediation_execution(uuid,uuid,bigint,uuid,text)",
			"vela_complete_remediation(uuid,uuid,bigint,boolean,text,text,bytea,text)",
			"vela_recover_remediation(uuid,text)",
			"vela_get_remediation_operation(uuid)",
			"vela_list_executing_remediation(integer)",
		},
	})
}

func verifyFleetPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Fleet",
		failureLabel:    "Fleet transaction",
		functions: []string{
			"vela_resolve_worker_identity(text,uuid,text,text,text)",
			"vela_configure_worker_pool_capacity(uuid,text,bigint,bigint,bigint,bigint,bigint,bigint,text)",
			"vela_observe_worker_capacity(uuid,uuid,bigint,bigint,timestamp with time zone,fleet_scratch_watermark_state,bigint,bigint,bigint,bigint,bigint,boolean,text)",
			"vela_get_worker_pool_capacity(uuid)",
			"vela_begin_worker_readiness(uuid,uuid,uuid,bigint,text,uuid,text,text,timestamptz)",
			"vela_report_worker_readiness(uuid,fleet_readiness_check,boolean,bytea,text)",
			"vela_get_worker_readiness(uuid)",
			"vela_get_worker_readiness_work(uuid,bigint)",
			"vela_request_worker_drain(uuid,uuid,bigint,text,timestamptz,text)",
			"vela_reconcile_worker_drain(uuid,text)",
			"vela_get_worker_drain(uuid)",
			"vela_authorize_fleet_mutation(text,text,fleet_protected_resource_kind,fleet_mutation_operation,text,text,text,uuid,uuid,bigint,uuid[],bytea)",
			"vela_has_fleet_retirement_authorization(fleet_protected_resource_kind,text,text,text,uuid,uuid,bigint,uuid[])",
			"vela_record_fleet_retirement_completion(fleet_protected_resource_kind,text,text,text,uuid,uuid,bigint,uuid[],text)",
			"vela_has_fleet_retirement_completion(fleet_protected_resource_kind,text,text,text,uuid,uuid,bigint,uuid[])",
		},
	})
}

func verifyWebhookRequestPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "webhook request",
		failureLabel:    "Webhook management transaction",
		functions: []string{
			"vela_set_request_context(uuid,bytea,text)",
			"vela_create_webhook_subscription(uuid,uuid,text,webhook_event_type[],uuid,text,bytea,bytea)",
			"vela_lock_webhook_secret_rotation(uuid,uuid)",
			"vela_rotate_webhook_secret(uuid,uuid,uuid,integer,text,bytea,bytea)",
			"vela_disable_webhook_subscription(uuid,uuid)",
			"vela_replay_webhook_delivery(uuid,uuid,uuid,uuid)",
			"vela_list_webhook_subscriptions(uuid,integer)",
			"vela_list_webhook_deliveries(uuid,uuid,integer)",
		},
	})
}

func verifyRetentionRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "retention request",
		failureLabel:    "Retention Policy and Content Deletion request transaction",
		functions: []string{
			"vela_set_request_context(uuid,bytea,text)",
			"vela_get_project_retention_policy(uuid)",
			"vela_set_project_retention_policy(uuid,integer,uuid)",
			"vela_get_content_deletion_request(uuid,uuid)",
			"vela_accept_content_deletion_request(uuid,uuid,uuid,text,bytea,uuid,uuid,uuid,uuid,uuid,uuid,uuid)",
		},
	})
}

func verifyDebugDumpRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "debug dump request",
		failureLabel:    "debug dump request transaction",
		functions: []string{
			"vela_set_request_context(uuid,bytea,text)",
			"vela_authorize_debug_dump(uuid,uuid,uuid,text,bytea,debug_dump_purpose)",
			"vela_get_debug_dump_authorization(uuid,uuid,uuid)",
			"vela_revoke_debug_dump_authorization(uuid,uuid,uuid,text,bytea)",
			"vela_list_debug_dumps(uuid,uuid,uuid)",
			"vela_authorize_debug_dump_read(uuid,uuid,uuid,uuid)",
			"vela_record_debug_dump_delivery(uuid,uuid,uuid,uuid,text,boolean)",
		},
	})
}

func verifyDebugDumpAuditRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "debug dump audit request",
		failureLabel:    "debug dump audit projection transaction",
		functions: []string{
			"vela_set_organization_identity_admin_context(uuid,bytea,text)",
			"vela_list_organization_audit_events_v3(uuid,integer)",
		},
	})
}

func verifyRetentionPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "retention",
		failureLabel:    "Content Deletion reconciliation transaction",
		functions: []string{
			"vela_claim_content_deletion_target(text,uuid,integer)",
			"vela_complete_content_deletion_target(uuid,uuid,uuid,text,text)",
			"vela_retry_content_deletion_target(uuid,uuid,integer,text)",
			"vela_enqueue_expired_content_deletions(integer)",
		},
	})
}

func verifyBackupRetentionPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "off-cluster backup retention",
		failureLabel:    "off-cluster backup Content Deletion transaction",
		functions: []string{
			"vela_claim_off_cluster_content_deletion_target(text,uuid,integer)",
			"vela_complete_off_cluster_content_deletion_target(uuid,uuid,uuid,integer)",
			"vela_retry_off_cluster_content_deletion_target(uuid,uuid,integer,integer)",
		},
	})
}

func verifyArtifactReplicationPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Artifact backup replication",
		failureLabel:    "Artifact backup replication transaction",
		functions: []string{
			"vela_claim_artifact_backup_replication(text,uuid,integer)",
			"vela_complete_artifact_backup_replication(uuid,uuid,text,bigint,bytea,text)",
			"vela_retry_artifact_backup_replication(uuid,uuid,integer,text)",
		},
	})
}

func verifyArtifactRequestPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Artifact request",
		failureLabel:    "Artifact read transaction",
		tables: []relationPrivilege{
			{Relation: "jobs", Privilege: "SELECT"},
			{Relation: "artifact_sets", Privilege: "SELECT"},
			{Relation: "artifact_set_items", Privilege: "SELECT"},
			{Relation: "artifact_access_grants", Privilege: "SELECT"},
		},
		functions: []string{
			"vela_current_organization_id()",
			"vela_current_project_id()",
			"vela_current_principal_id()",
			"vela_current_request_scope()",
			"vela_set_artifact_request_context(uuid,bytea)",
		},
	})
}

func verifyCancelPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "cancel",
		failureLabel:    "cancellation command",
		functions: []string{
			"vela_set_cancellation_request_context(uuid,bytea)",
			"vela_cancel_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)",
		},
	})
}

func verifyAuthPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "auth",
		failureLabel:    "credential lookup",
		functions:       []string{"vela_authenticate_service_credential(uuid)"},
	})
}

func verifyHumanAuthPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Human auth",
		failureLabel:    "Human OIDC authorization",
		functions:       []string{"vela_authenticate_human_oidc(text,text,bytea,timestamptz)"},
	})
}

func verifyHumanMembershipAuthPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Human membership auth",
		failureLabel:    "Human Organization OIDC authorization",
		functions: []string{
			"vela_authenticate_human_organization_oidc(text,text,bytea,timestamptz)",
		},
	})
}

func verifyIdentityRequestPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "identity request",
		failureLabel:    "Service Principal administration transaction",
		functions: []string{
			"vela_set_identity_admin_context(uuid,bytea,text)",
			"vela_create_service_principal(uuid,uuid,text)",
			"vela_list_service_principals(uuid,integer)",
			"vela_issue_service_credential(uuid,uuid,uuid,bytea,text[],timestamp with time zone)",
			"vela_list_service_credentials(uuid,uuid,integer)",
			"vela_revoke_service_credential(uuid,uuid,uuid)",
			"vela_disable_service_principal(uuid,uuid)",
		},
	})
}

func verifyHumanMembershipRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Human membership request",
		failureLabel:    "Human membership administration transaction",
		functions: []string{
			"vela_set_organization_identity_admin_context(uuid,bytea,text)",
			"vela_set_project_membership_admin_context(uuid,bytea,uuid,text)",
			"vela_create_human_member(uuid,uuid,text,text,text)",
			"vela_disable_human_member(uuid,uuid)",
			"vela_list_human_members(uuid,integer)",
			"vela_list_project_members(uuid,integer)",
			"vela_assign_organization_role(uuid,uuid,organization_role)",
			"vela_revoke_organization_role(uuid,uuid,organization_role)",
			"vela_assign_project_role(uuid,uuid,project_role)",
			"vela_revoke_project_role(uuid,uuid,project_role)",
		},
	})
}

func verifyOrganizationBillingRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Organization billing request",
		failureLabel:    "Organization billing reporting transaction",
		functions: []string{
			"vela_set_organization_identity_admin_context(uuid,bytea,text)",
			"vela_get_organization_credit_summary(uuid)",
			"vela_list_organization_charges(uuid,integer)",
			"vela_create_settlement_contact(uuid,uuid,text,text)",
			"vela_list_settlement_contacts(uuid,integer)",
			"vela_disable_settlement_contact(uuid,uuid)",
			"vela_get_organization_usage(uuid,timestamp with time zone,timestamp with time zone)",
		},
	})
}

func verifyOrganizationAuditRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Organization audit request",
		failureLabel:    "Organization audit reporting transaction",
		functions: []string{
			"vela_set_organization_identity_admin_context(uuid,bytea,text)",
			"vela_get_organization_usage(uuid,timestamp with time zone,timestamp with time zone)",
			"vela_list_organization_audit_events(uuid,integer)",
		},
	})
}

func verifyBreakGlassAuditRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Break-glass audit request",
		failureLabel:    "Break-glass audit projection transaction",
		functions: []string{
			"vela_set_organization_identity_admin_context(uuid,bytea,text)",
			"vela_list_organization_audit_events_v2(uuid,integer)",
		},
	})
}

func verifyPlatformOperatorAuthPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Platform Operator auth",
		failureLabel:    "Platform Operator authentication transaction",
		functions: []string{
			"vela_authenticate_platform_operator_oidc(text,text,bytea,timestamp with time zone)",
		},
	})
}

func verifyBreakGlassRequestPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
) error {
	return verifyExactPrivileges(ctx, database, currentUser, exactPrivilegeBoundary{
		inspectionLabel: "Break-glass request",
		failureLabel:    "Break-glass request transaction",
		functions: []string{
			"vela_set_break_glass_request_context(uuid,bytea)",
			"vela_create_break_glass_request(uuid,text,bytea,uuid,uuid,uuid,break_glass_scope[],break_glass_reason_code,text,integer)",
			"vela_approve_break_glass_request(uuid,uuid)",
			"vela_revoke_break_glass_grant(uuid)",
			"vela_get_break_glass_request(uuid)",
			"vela_get_break_glass_grant(uuid)",
			"vela_authorize_break_glass_request_content(uuid)",
			"vela_authorize_break_glass_artifacts(uuid)",
			"vela_record_break_glass_artifact_delivery(uuid,boolean)",
		},
	})
}

func verifyExactPrivileges(
	ctx context.Context,
	database rowQuerier,
	currentUser string,
	boundary exactPrivilegeBoundary,
) error {
	tableRelations := make([]string, len(boundary.tables))
	tablePrivileges := make([]string, len(boundary.tables))
	for index, privilege := range boundary.tables {
		tableRelations[index] = privilege.Relation
		tablePrivileges[index] = privilege.Privilege
	}
	columnRelations := make([]string, len(boundary.columns))
	columnNames := make([]string, len(boundary.columns))
	columnPrivileges := make([]string, len(boundary.columns))
	for index, privilege := range boundary.columns {
		columnRelations[index] = privilege.Relation
		columnNames[index] = privilege.Column
		columnPrivileges[index] = privilege.Privilege
	}

	var boundaryViolation bool
	err := database.QueryRow(ctx, `
		WITH expected_table_privileges (relation_name, privilege) AS (
			SELECT * FROM unnest($1::text[], $2::text[])
		),
		expected_column_privileges (relation_name, column_name, privilege) AS (
			SELECT * FROM unnest($3::text[], $4::text[], $5::text[])
		),
		expected_functions (function_oid) AS (
			SELECT to_regprocedure(signature)::oid
			FROM unnest($6::text[]) AS signature
		),
		public_relations AS (
			SELECT relation.oid, relation.relname
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = 'public'
			  AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
		),
		table_privilege_names (privilege) AS (
			VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'), ('REFERENCES'), ('TRIGGER')
		),
		column_privilege_names (privilege) AS (
			VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('REFERENCES')
		),
		sequence_privilege_names (privilege) AS (
			VALUES ('SELECT'), ('UPDATE'), ('USAGE')
		)
		SELECT
			NOT has_schema_privilege(current_user, 'public', 'USAGE')
			OR has_schema_privilege(current_user, 'public', 'CREATE')
			OR has_schema_privilege(current_user, 'vela_private', 'USAGE')
			OR has_schema_privilege(current_user, 'vela_private', 'CREATE')
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				CROSS JOIN table_privilege_names AS candidate
				WHERE namespace.nspname = 'vela_private'
				  AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
				  AND has_table_privilege(current_user, relation.oid, candidate.privilege)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				CROSS JOIN sequence_privilege_names AS candidate
				WHERE namespace.nspname = 'vela_private'
				  AND relation.relkind = 'S'
				  AND has_sequence_privilege(current_user, relation.oid, candidate.privilege)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_proc AS function
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
				WHERE namespace.nspname = 'vela_private'
				  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
			)
			OR EXISTS (
				SELECT 1
				FROM public_relations AS relation
				CROSS JOIN table_privilege_names AS candidate
				LEFT JOIN expected_table_privileges AS expected
				  ON expected.relation_name = relation.relname
				 AND expected.privilege = candidate.privilege
				WHERE has_table_privilege(current_user, relation.oid, candidate.privilege)
				  AND expected.relation_name IS NULL
			)
			OR EXISTS (
				SELECT 1
				FROM expected_table_privileges AS expected
				WHERE NOT has_table_privilege(
					current_user,
					format('public.%I', expected.relation_name),
					expected.privilege
				)
			)
			OR EXISTS (
				SELECT 1
				FROM public_relations AS relation
				JOIN pg_catalog.pg_attribute AS attribute
				  ON attribute.attrelid = relation.oid
				 AND attribute.attnum > 0
				 AND NOT attribute.attisdropped
				CROSS JOIN column_privilege_names AS candidate
				LEFT JOIN expected_column_privileges AS expected
				  ON expected.relation_name = relation.relname
				 AND expected.column_name = attribute.attname
				 AND expected.privilege = candidate.privilege
				WHERE has_column_privilege(
					current_user,
					relation.oid,
					attribute.attnum,
					candidate.privilege
				)
				  AND NOT has_table_privilege(current_user, relation.oid, candidate.privilege)
				  AND expected.relation_name IS NULL
			)
			OR EXISTS (
				SELECT 1
				FROM expected_column_privileges AS expected
				WHERE NOT has_column_privilege(
					current_user,
					format('public.%I', expected.relation_name),
					expected.column_name,
					expected.privilege
				)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_class AS sequence
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = sequence.relnamespace
				CROSS JOIN sequence_privilege_names AS candidate
				WHERE namespace.nspname = 'public'
				  AND sequence.relkind = 'S'
				  AND has_sequence_privilege(current_user, sequence.oid, candidate.privilege)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_proc AS function
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
				LEFT JOIN expected_functions AS expected ON expected.function_oid = function.oid
				WHERE namespace.nspname = 'public'
				  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
				  AND expected.function_oid IS NULL
			)
			OR EXISTS (
				SELECT 1
				FROM expected_functions AS expected
				WHERE expected.function_oid IS NULL
				   OR NOT has_function_privilege(current_user, expected.function_oid, 'EXECUTE')
			)
	`, tableRelations, tablePrivileges, columnRelations, columnNames, columnPrivileges, boundary.functions).
		Scan(&boundaryViolation)
	if err != nil {
		return fmt.Errorf("inspect %s database privileges: %w", boundary.inspectionLabel, err)
	}
	if boundaryViolation {
		return fmt.Errorf(
			"database login %q exceeds the %s privilege boundary",
			currentUser,
			boundary.failureLabel,
		)
	}
	return nil
}
func verifyRequestPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	boundary := exactPrivilegeBoundary{
		inspectionLabel: "request",
		failureLabel:    "request transaction",
		tables: []relationPrivilege{
			{Relation: "worker_pools", Privilege: "SELECT"},
			{Relation: "customer_organizations", Privilege: "SELECT"},
			{Relation: "projects", Privilege: "SELECT"},
			{Relation: "principals", Privilege: "SELECT"},
			{Relation: "service_principals", Privilege: "SELECT"},
			{Relation: "organization_credit_accounts", Privilege: "SELECT"},
			{Relation: "jobs", Privilege: "SELECT"},
			{Relation: "credit_reservations", Privilege: "SELECT"},
			{Relation: "idempotency_results", Privilege: "SELECT"},
			{Relation: "outbox_events", Privilege: "SELECT"},
			{Relation: "vela_request_execution_lease_renewal_protocol", Privilege: "SELECT"},
			{Relation: "vela_request_job_runtime", Privilege: "SELECT"},
			{Relation: "vela_request_job_progress", Privilege: "SELECT"},
			{Relation: "jobs", Privilege: "INSERT"},
			{Relation: "credit_reservations", Privilege: "INSERT"},
			{Relation: "retry_runtime_states", Privilege: "INSERT"},
			{Relation: "idempotency_results", Privilege: "INSERT"},
			{Relation: "outbox_events", Privilege: "INSERT"},
		},
		columns: []columnPrivilege{
			{Relation: "worker_pools", Column: "queued_count", Privilege: "UPDATE"},
			{Relation: "projects", Column: "queued_count", Privilege: "UPDATE"},
			{Relation: "projects", Column: "running_count", Privilege: "UPDATE"},
			{Relation: "organization_credit_accounts", Column: "reserved_minor", Privilege: "UPDATE"},
			{Relation: "organization_credit_accounts", Column: "version", Privilege: "UPDATE"},
			{Relation: "organization_credit_accounts", Column: "updated_at", Privilege: "UPDATE"},
		},
		functions: []string{
			"vela_current_organization_id()",
			"vela_current_project_id()",
			"vela_current_principal_id()",
			"vela_current_request_scope()",
			"vela_set_request_context(uuid,bytea,text)",
			"vela_resolve_active_sku(text,text,text,text)",
			"vela_lock_compatible_pool(uuid,uuid,uuid)",
		},
	}
	stageRoutePrivilegesPresent, err := databaseFunctionExists(
		ctx,
		database,
		"vela_resolve_job_execution_route(uuid,uuid,uuid)",
	)
	if err != nil {
		return fmt.Errorf("inspect request Stage route surface: %w", err)
	}
	if stageRoutePrivilegesPresent {
		boundary.functions = append(boundary.functions,
			"vela_resolve_job_execution_route(uuid,uuid,uuid)",
			"vela_lock_stage_graph_ready_capacity_path(uuid,uuid)",
			"vela_instantiate_admitted_stage_graph(uuid,uuid,uuid)",
		)
	}
	var leaseRenewalProtocolEnabled bool
	if err := database.QueryRow(ctx, `
		SELECT enabled FROM vela_request_execution_lease_renewal_protocol
	`).Scan(&leaseRenewalProtocolEnabled); err != nil {
		return fmt.Errorf("inspect request execution Lease renewal protocol: %w", err)
	}
	if !leaseRenewalProtocolEnabled {
		boundary.tables = append(boundary.tables, relationPrivilege{
			Relation:  "retry_runtime_states",
			Privilege: "SELECT",
		})
	}
	return verifyExactPrivileges(ctx, database, currentUser, boundary)
}
