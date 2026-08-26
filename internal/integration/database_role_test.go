//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	veladb "github.com/vivym/vela/internal/database"
)

func TestDatabasePoolsFailClosedOnRoleConfusion(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	humanMembershipAuthPool := newRolePool(
		t,
		database.DSN,
		"vela_human_membership_auth_login",
		"vela-human-membership-auth-password",
	)
	identityRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_identity_request_login",
		"vela-identity-request-password",
	)
	humanMembershipRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_human_membership_request_login",
		"vela-human-membership-request-password",
	)
	organizationBillingRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_organization_billing_request_login",
		"vela-organization-billing-request-password",
	)
	organizationAuditRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_organization_audit_request_login",
		"vela-organization-audit-request-password",
	)
	breakGlassAuditRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_break_glass_audit_request_login",
		"vela-break-glass-audit-request-password",
	)
	retentionRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	)
	retentionPool := newRolePool(
		t,
		database.DSN,
		"vela_retention_login",
		"vela-retention-password",
	)
	platformOperatorAuthPool := newRolePool(
		t,
		database.DSN,
		"vela_platform_operator_auth_login",
		"vela-platform-operator-auth-password",
	)
	breakGlassRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_break_glass_request_login",
		"vela-break-glass-request-password",
	)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	schedulerInboxPool := newRolePool(
		t,
		database.DSN,
		"vela_scheduler_inbox_login",
		"vela-scheduler-inbox-password",
	)
	billingPool := newRolePool(t, database.DSN, "vela_billing_login", "vela-billing-password")
	financeReconciliationPool := newRolePool(
		t,
		database.DSN,
		"vela_finance_reconciliation_login",
		"vela-finance-reconciliation-password",
	)
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	webhookPool := newRolePool(t, database.DSN, "vela_webhook_login", "vela-webhook-password")
	remediationPool := newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	)
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	for _, login := range []string{
		"vela_internal_login",
		"vela_scheduler_inbox_login",
		"vela_billing_login",
		"vela_finance_reconciliation_login",
		"vela_webhook_request_login",
		"vela_webhook_login",
		"vela_remediation_login",
		"vela_fleet_login",
		"vela_identity_request_login",
		"vela_human_membership_request_login",
		"vela_organization_billing_request_login",
		"vela_organization_audit_request_login",
		"vela_break_glass_audit_request_login",
		"vela_retention_request_login",
		"vela_retention_login",
		"vela_platform_operator_auth_login",
		"vela_break_glass_request_login",
	} {
		for _, ownerRole := range []string{
			"vela_billing_owner",
			"vela_finance_reconciliation_owner",
			"vela_organization_reporting_owner",
			"vela_retention_owner",
			"vela_break_glass_owner",
			"vela_fleet_owner",
		} {
			var inheritsOwner bool
			if err := database.Admin.QueryRow(
				"SELECT pg_has_role($1, $2, 'MEMBER')", login, ownerRole,
			).Scan(&inheritsOwner); err != nil {
				t.Fatalf("inspect %s %s membership: %v", login, ownerRole, err)
			}
			if inheritsOwner {
				t.Fatalf("runtime login %s inherits %s", login, ownerRole)
			}
		}
	}

	for _, test := range []struct {
		name string
		pool *pgxpool.Pool
		role veladb.Role
	}{
		{name: "auth", pool: authPool, role: veladb.RoleAuth},
		{name: "Human auth", pool: humanAuthPool, role: veladb.RoleHumanAuth},
		{
			name: "Human membership auth", pool: humanMembershipAuthPool,
			role: veladb.RoleHumanMembershipAuth,
		},
		{name: "identity request", pool: identityRequestPool, role: veladb.RoleIdentityRequest},
		{
			name: "Human membership request", pool: humanMembershipRequestPool,
			role: veladb.RoleHumanMembershipRequest,
		},
		{
			name: "Organization billing request", pool: organizationBillingRequestPool,
			role: veladb.RoleOrganizationBillingRequest,
		},
		{
			name: "Organization audit request", pool: organizationAuditRequestPool,
			role: veladb.RoleOrganizationAuditRequest,
		},
		{
			name: "Break-glass audit request", pool: breakGlassAuditRequestPool,
			role: veladb.RoleBreakGlassAuditRequest,
		},
		{name: "retention request", pool: retentionRequestPool, role: veladb.RoleRetentionRequest},
		{name: "retention", pool: retentionPool, role: veladb.RoleRetention},
		{
			name: "Platform Operator auth", pool: platformOperatorAuthPool,
			role: veladb.RolePlatformOperatorAuth,
		},
		{
			name: "Break-glass request", pool: breakGlassRequestPool,
			role: veladb.RoleBreakGlassRequest,
		},
		{name: "request", pool: requestPool, role: veladb.RoleRequest},
		{name: "cancel", pool: cancelPool, role: veladb.RoleCancel},
		{name: "Artifact request", pool: artifactPool, role: veladb.RoleArtifactRequest},
		{name: "Scheduler", pool: schedulerPool, role: veladb.RoleScheduler},
		{
			name: "Scheduler Inbox", pool: schedulerInboxPool,
			role: veladb.RoleSchedulerInbox,
		},
		{name: "billing", pool: billingPool, role: veladb.RoleBilling},
		{
			name: "Finance Reconciliation", pool: financeReconciliationPool,
			role: veladb.RoleFinanceReconciliation,
		},
		{name: "webhook request", pool: webhookRequestPool, role: veladb.RoleWebhookRequest},
		{name: "webhook", pool: webhookPool, role: veladb.RoleWebhook},
		{name: "remediation", pool: remediationPool, role: veladb.RoleRemediation},
		{name: "Fleet", pool: fleetPool, role: veladb.RoleFleet},
		{name: "internal", pool: internalPool, role: veladb.RoleInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := veladb.VerifyRole(context.Background(), test.pool, test.role); err != nil {
				t.Fatalf("verify correct %s role: %v", test.name, err)
			}
		})
	}
	for _, role := range []struct {
		name      string
		bypassRLS bool
	}{
		{name: "vela_platform_operator_auth"},
		{name: "vela_break_glass_request"},
		{name: "vela_break_glass_audit_request"},
		{name: "vela_break_glass_owner", bypassRLS: true},
		{name: "vela_finance_reconciliation"},
		{name: "vela_scheduler_inbox"},
		{name: "vela_finance_reconciliation_owner", bypassRLS: true},
		{name: "vela_remediation", bypassRLS: false},
		{name: "vela_remediation_owner", bypassRLS: true},
		{name: "vela_fleet", bypassRLS: false},
		{name: "vela_fleet_owner", bypassRLS: true},
	} {
		var canLogin, bypassRLS, superuser bool
		if err := database.Admin.QueryRow(`
			SELECT rolcanlogin, rolbypassrls, rolsuper
			FROM pg_catalog.pg_roles
			WHERE rolname = $1
		`, role.name).Scan(&canLogin, &bypassRLS, &superuser); err != nil {
			t.Fatalf("inspect %s attributes: %v", role.name, err)
		}
		if canLogin || superuser || bypassRLS != role.bypassRLS {
			t.Fatalf(
				"%s attributes = canLogin %t bypassRLS %t superuser %t",
				role.name,
				canLogin,
				bypassRLS,
				superuser,
			)
		}
	}

	for _, function := range []struct {
		signature string
		owner     string
		proconfig string
	}{
		{
			signature: "vela_private.require_finance_reconciliation_identity()",
			owner:     "vela_finance_reconciliation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_get_finance_reconciliation_identity()",
			owner:     "vela_finance_reconciliation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_apply_finance_reconciliation(uuid,text,bigint,uuid,finance_reconciliation_kind,text,bigint,bigint,bigint,text,timestamp with time zone)",
			owner:     "vela_finance_reconciliation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_authenticate_platform_operator_oidc(text,text,bytea,timestamp with time zone)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_set_break_glass_request_context(uuid,bytea)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_private.current_break_glass_request_context()",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_create_break_glass_request(uuid,text,bytea,uuid,uuid,uuid,break_glass_scope[],break_glass_reason_code,text,integer)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_approve_break_glass_request(uuid,uuid)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_revoke_break_glass_grant(uuid)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_get_break_glass_request(uuid)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_get_break_glass_grant(uuid)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_private.authorize_break_glass_scope(uuid,break_glass_scope)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_private.available_break_glass_artifact_set(uuid,uuid,uuid,timestamp with time zone)",
			owner:     "vela_internal",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_authorize_break_glass_request_content(uuid)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_authorize_break_glass_artifacts(uuid)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_record_break_glass_artifact_delivery(uuid,boolean)",
			owner:     "vela_break_glass_owner",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_list_organization_audit_events_v2(uuid,integer)",
			owner:     "vela_internal",
			proconfig: "search_path=pg_catalog, public, vela_private",
		},
		{
			signature: "vela_remediation_event(uuid,integer,remediation_operation_state,remediation_operation_state,text,text)",
			owner:     "vela_remediation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_request_remediation(uuid,uuid,bigint,text,text,text,bytea,text,remediation_action_level,text,text)",
			owner:     "vela_remediation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_approve_remediation(uuid,text)",
			owner:     "vela_remediation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_start_remediation(uuid,uuid,bigint,text)",
			owner:     "vela_remediation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_complete_remediation(uuid,uuid,bigint,boolean,text,text,bytea,text)",
			owner:     "vela_remediation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_recover_remediation(uuid,text)",
			owner:     "vela_remediation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_get_remediation_operation(uuid)",
			owner:     "vela_remediation_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_enqueue_expired_content_deletions(integer)",
			owner:     "vela_retention_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_enqueue_expired_content_deletions_v23(integer)",
			owner:     "vela_retention_owner",
			proconfig: "search_path=pg_catalog, public",
		},
		{
			signature: "vela_enqueue_incomplete_artifact_deletions(integer)",
			owner:     "vela_retention_owner",
			proconfig: "search_path=pg_catalog, public",
		},
	} {
		var owner string
		var securityDefiner, configurationMatches, publicExecute bool
		if err := database.Admin.QueryRow(`
			SELECT owner.rolname,
				procedure.prosecdef,
				procedure.proconfig = ARRAY[$2]::text[],
				EXISTS (
					SELECT 1
					FROM pg_catalog.aclexplode(
						COALESCE(
							procedure.proacl,
							pg_catalog.acldefault('f', procedure.proowner)
						)
					) AS privilege
					WHERE privilege.grantee = 0
					  AND privilege.privilege_type = 'EXECUTE'
				)
			FROM pg_catalog.pg_proc AS procedure
			JOIN pg_catalog.pg_roles AS owner ON owner.oid = procedure.proowner
			WHERE procedure.oid = $1::regprocedure
		`, function.signature, function.proconfig).Scan(
			&owner,
			&securityDefiner,
			&configurationMatches,
			&publicExecute,
		); err != nil {
			t.Fatalf("inspect %s security boundary: %v", function.signature, err)
		}
		if owner != function.owner || !securityDefiner || !configurationMatches || publicExecute {
			t.Fatalf(
				"%s boundary = owner %s security definer %t public execute %t config match %t",
				function.signature,
				owner,
				securityDefiner,
				publicExecute,
				configurationMatches,
			)
		}
	}
	var breakGlassOwnerCanLock, breakGlassRuntimeCanLock bool
	if err := database.Admin.QueryRow(`
		SELECT
			pg_catalog.has_function_privilege(
				'vela_break_glass_owner',
				'vela_private.available_break_glass_artifact_set(uuid,uuid,uuid,timestamp with time zone)',
				'EXECUTE'
			),
			pg_catalog.has_function_privilege(
				'vela_break_glass_request',
				'vela_private.available_break_glass_artifact_set(uuid,uuid,uuid,timestamp with time zone)',
				'EXECUTE'
			)
	`).Scan(&breakGlassOwnerCanLock, &breakGlassRuntimeCanLock); err != nil {
		t.Fatalf("inspect Break-glass lifecycle lock execution boundary: %v", err)
	}
	if !breakGlassOwnerCanLock || breakGlassRuntimeCanLock {
		t.Fatalf(
			"Break-glass lifecycle lock execution = owner %t runtime %t",
			breakGlassOwnerCanLock,
			breakGlassRuntimeCanLock,
		)
	}

	var privateContextCount int
	err := requestPool.QueryRow(
		context.Background(),
		"SELECT count(*) FROM vela_private.request_contexts",
	).Scan(&privateContextCount)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("request login private context read error = %v, want SQLSTATE 42501", err)
	}
	for _, runtime := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Organization billing request", pool: organizationBillingRequestPool},
		{name: "Organization audit request", pool: organizationAuditRequestPool},
		{name: "Break-glass audit request", pool: breakGlassAuditRequestPool},
	} {
		for _, relation := range []string{
			"jobs",
			"credit_reservations",
			"charges",
			"invoice_exports",
			"invoice_export_receipts",
			"organization_credit_accounts",
			"organization_settlement_contacts",
			"organization_settlement_contact_events",
			"human_oidc_bindings",
			"human_organization_auth_sessions",
			"human_auth_sessions",
			"organization_role_bindings",
			"project_role_bindings",
			"credentials",
			"human_identity_events",
			"project_identity_events",
			"vela_private.human_administration_contexts",
		} {
			var count int64
			err := runtime.pool.QueryRow(
				context.Background(), "SELECT count(*) FROM "+relation,
			).Scan(&count)
			if !isPermissionDenied(err) {
				t.Fatalf(
					"%s direct %s read error = %v, want permission denied",
					runtime.name,
					relation,
					err,
				)
			}
		}
		for mutation, statement := range map[string]string{
			"credit":                 "UPDATE organization_credit_accounts SET reserved_minor = reserved_minor",
			"Job":                    "UPDATE jobs SET updated_at = updated_at",
			"Charge":                 "UPDATE charges SET amount_minor = amount_minor",
			"Invoice export":         "UPDATE invoice_exports SET state = state",
			"Invoice receipt":        "UPDATE invoice_export_receipts SET exported_at = exported_at",
			"settlement contact":     "UPDATE organization_settlement_contacts SET display_name = display_name",
			"contact evidence":       "UPDATE organization_settlement_contact_events SET created_at = created_at",
			"Human audit evidence":   "UPDATE human_identity_events SET created_at = created_at",
			"Project audit evidence": "UPDATE project_identity_events SET created_at = created_at",
		} {
			if _, err := runtime.pool.Exec(
				context.Background(), statement,
			); !isPermissionDenied(err) {
				t.Fatalf(
					"%s direct %s mutation error = %v, want permission denied",
					runtime.name,
					mutation,
					err,
				)
			}
		}
	}
	if _, err := organizationBillingRequestPool.Exec(
		context.Background(),
		"SELECT * FROM vela_list_organization_audit_events($1, 100)",
		uuid.MustParse(testOrganizationID),
	); !isPermissionDenied(err) {
		t.Fatalf("Organization billing request audit call error = %v, want permission denied", err)
	}
	if _, err := organizationAuditRequestPool.Exec(
		context.Background(),
		"SELECT * FROM vela_list_organization_audit_events_v2($1, 100)",
		uuid.MustParse(testOrganizationID),
	); !isPermissionDenied(err) {
		t.Fatalf("legacy Organization audit request v2 call error = %v, want permission denied", err)
	}
	if _, err := breakGlassAuditRequestPool.Exec(
		context.Background(),
		"SELECT * FROM vela_list_organization_audit_events($1, 100)",
		uuid.MustParse(testOrganizationID),
	); !isPermissionDenied(err) {
		t.Fatalf("Break-glass audit request legacy call error = %v, want permission denied", err)
	}
	if _, err := organizationAuditRequestPool.Exec(
		context.Background(),
		"SELECT * FROM vela_list_organization_charges($1, 100)",
		uuid.MustParse(testOrganizationID),
	); !isPermissionDenied(err) {
		t.Fatalf("Organization audit request Charge call error = %v, want permission denied", err)
	}
	for _, relation := range []string{
		"projects",
		"jobs",
		"retention_policy_revisions",
		"project_retention_policy_events",
	} {
		var count int64
		err := retentionRequestPool.QueryRow(
			context.Background(), "SELECT count(*) FROM "+relation,
		).Scan(&count)
		if !isPermissionDenied(err) {
			t.Fatalf("retention request direct %s read error = %v, want permission denied", relation, err)
		}
	}
	for _, runtime := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Platform Operator auth", pool: platformOperatorAuthPool},
		{name: "Break-glass request", pool: breakGlassRequestPool},
	} {
		for _, relation := range []string{
			"platform_operator_oidc_bindings",
			"platform_operator_auth_sessions",
			"break_glass_requests",
			"break_glass_grants",
			"break_glass_events",
			"break_glass_denial_events",
			"jobs",
			"artifacts",
			"credentials",
			"human_oidc_bindings",
			"vela_private.break_glass_request_contexts",
		} {
			var count int64
			err := runtime.pool.QueryRow(
				context.Background(), "SELECT count(*) FROM "+relation,
			).Scan(&count)
			if !isPermissionDenied(err) {
				t.Fatalf(
					"%s direct %s read error = %v, want permission denied",
					runtime.name,
					relation,
					err,
				)
			}
		}
	}
	if _, err := authPool.Exec(
		context.Background(),
		"SELECT * FROM vela_authenticate_platform_operator_oidc('', '', decode(repeat('00', 32), 'hex'), clock_timestamp())",
	); !isPermissionDenied(err) {
		t.Fatalf("customer auth role Platform Operator authentication error = %v, want permission denied", err)
	}
	if _, err := requestPool.Exec(
		context.Background(),
		"SELECT vela_set_break_glass_request_context($1, decode(repeat('00', 32), 'hex'))",
		uuid.New(),
	); !isPermissionDenied(err) {
		t.Fatalf("customer request role Break-glass context error = %v, want permission denied", err)
	}
	for _, relation := range []string{
		"jobs",
		"artifacts",
		"artifact_uploads",
		"content_deletion_requests",
		"content_deletion_targets",
		"content_deletion_receipts",
		"content_deletion_receipt_targets",
	} {
		var count int64
		err := retentionPool.QueryRow(
			context.Background(), "SELECT count(*) FROM "+relation,
		).Scan(&count)
		if !isPermissionDenied(err) {
			t.Fatalf("retention runtime direct %s read error = %v, want permission denied", relation, err)
		}
	}
	for _, runtime := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "request", pool: requestPool},
		{name: "authentication", pool: authPool},
		{name: "internal Worker/control", pool: internalPool},
		{name: "retention request", pool: retentionRequestPool},
		{name: "retention runtime", pool: retentionPool},
		{name: "cancellation", pool: cancelPool},
		{name: "Artifact request", pool: artifactPool},
	} {
		if _, err := runtime.pool.Exec(
			context.Background(),
			"SELECT vela_enqueue_incomplete_artifact_deletions(1)",
		); !isPermissionDenied(err) {
			t.Fatalf(
				"%s incomplete Artifact enqueue error = %v, want permission denied",
				runtime.name,
				err,
			)
		}
	}

	for _, test := range []struct {
		name string
		pool *pgxpool.Pool
		role veladb.Role
	}{
		{name: "request as internal", pool: requestPool, role: veladb.RoleInternal},
		{name: "internal as request", pool: internalPool, role: veladb.RoleRequest},
		{name: "auth as request", pool: authPool, role: veladb.RoleRequest},
		{name: "Human auth as request", pool: humanAuthPool, role: veladb.RoleRequest},
		{
			name: "Human membership auth as Human auth", pool: humanMembershipAuthPool,
			role: veladb.RoleHumanAuth,
		},
		{name: "request as Human auth", pool: requestPool, role: veladb.RoleHumanAuth},
		{name: "auth as Human auth", pool: authPool, role: veladb.RoleHumanAuth},
		{name: "Human auth as auth", pool: humanAuthPool, role: veladb.RoleAuth},
		{name: "identity request as internal", pool: identityRequestPool, role: veladb.RoleInternal},
		{
			name: "Human membership request as identity request", pool: humanMembershipRequestPool,
			role: veladb.RoleIdentityRequest,
		},
		{name: "internal as identity request", pool: internalPool, role: veladb.RoleIdentityRequest},
		{name: "request as identity request", pool: requestPool, role: veladb.RoleIdentityRequest},
		{name: "identity request as request", pool: identityRequestPool, role: veladb.RoleRequest},
		{
			name: "Organization billing request as audit request",
			pool: organizationBillingRequestPool, role: veladb.RoleOrganizationAuditRequest,
		},
		{
			name: "Organization audit request as billing request",
			pool: organizationAuditRequestPool, role: veladb.RoleOrganizationBillingRequest,
		},
		{
			name: "Organization audit request as Break-glass audit request",
			pool: organizationAuditRequestPool, role: veladb.RoleBreakGlassAuditRequest,
		},
		{
			name: "Break-glass audit request as Organization audit request",
			pool: breakGlassAuditRequestPool, role: veladb.RoleOrganizationAuditRequest,
		},
		{
			name: "Human membership request as Organization billing request",
			pool: humanMembershipRequestPool, role: veladb.RoleOrganizationBillingRequest,
		},
		{
			name: "Organization billing request as Human membership request",
			pool: organizationBillingRequestPool, role: veladb.RoleHumanMembershipRequest,
		},
		{
			name: "request as Organization audit request",
			pool: requestPool, role: veladb.RoleOrganizationAuditRequest,
		},
		{name: "request as retention request", pool: requestPool, role: veladb.RoleRetentionRequest},
		{name: "retention request as request", pool: retentionRequestPool, role: veladb.RoleRequest},
		{name: "internal as retention request", pool: internalPool, role: veladb.RoleRetentionRequest},
		{name: "request as retention", pool: requestPool, role: veladb.RoleRetention},
		{name: "retention as request", pool: retentionPool, role: veladb.RoleRequest},
		{name: "internal as retention", pool: internalPool, role: veladb.RoleRetention},
		{
			name: "request as Platform Operator auth", pool: requestPool,
			role: veladb.RolePlatformOperatorAuth,
		},
		{
			name: "Platform Operator auth as request", pool: platformOperatorAuthPool,
			role: veladb.RoleRequest,
		},
		{
			name: "Break-glass request as Platform Operator auth", pool: breakGlassRequestPool,
			role: veladb.RolePlatformOperatorAuth,
		},
		{
			name: "Platform Operator auth as Break-glass request", pool: platformOperatorAuthPool,
			role: veladb.RoleBreakGlassRequest,
		},
		{
			name: "request as Break-glass request", pool: requestPool,
			role: veladb.RoleBreakGlassRequest,
		},
		{
			name: "Break-glass request as request", pool: breakGlassRequestPool,
			role: veladb.RoleRequest,
		},
		{
			name: "internal as Break-glass request", pool: internalPool,
			role: veladb.RoleBreakGlassRequest,
		},
		{
			name: "retention request as retention",
			pool: retentionRequestPool, role: veladb.RoleRetention,
		},
		{
			name: "retention as retention request",
			pool: retentionPool, role: veladb.RoleRetentionRequest,
		},
		{name: "cancel as request", pool: cancelPool, role: veladb.RoleRequest},
		{name: "request as cancel", pool: requestPool, role: veladb.RoleCancel},
		{name: "internal as cancel", pool: internalPool, role: veladb.RoleCancel},
		{name: "request as Artifact request", pool: requestPool, role: veladb.RoleArtifactRequest},
		{name: "Artifact request as request", pool: artifactPool, role: veladb.RoleRequest},
		{name: "internal as Artifact request", pool: internalPool, role: veladb.RoleArtifactRequest},
		{name: "Scheduler as request", pool: schedulerPool, role: veladb.RoleRequest},
		{name: "Scheduler as internal", pool: schedulerPool, role: veladb.RoleInternal},
		{
			name: "Scheduler as Scheduler Inbox", pool: schedulerPool,
			role: veladb.RoleSchedulerInbox,
		},
		{
			name: "Scheduler Inbox as Scheduler", pool: schedulerInboxPool,
			role: veladb.RoleScheduler,
		},
		{
			name: "Scheduler Inbox as internal", pool: schedulerInboxPool,
			role: veladb.RoleInternal,
		},
		{name: "internal as Scheduler", pool: internalPool, role: veladb.RoleScheduler},
		{
			name: "internal as Scheduler Inbox", pool: internalPool,
			role: veladb.RoleSchedulerInbox,
		},
		{name: "request as Scheduler", pool: requestPool, role: veladb.RoleScheduler},
		{
			name: "request as Scheduler Inbox", pool: requestPool,
			role: veladb.RoleSchedulerInbox,
		},
		{name: "billing as internal", pool: billingPool, role: veladb.RoleInternal},
		{name: "internal as billing", pool: internalPool, role: veladb.RoleBilling},
		{name: "request as billing", pool: requestPool, role: veladb.RoleBilling},
		{name: "webhook request as internal", pool: webhookRequestPool, role: veladb.RoleInternal},
		{name: "internal as webhook request", pool: internalPool, role: veladb.RoleWebhookRequest},
		{name: "request as webhook request", pool: requestPool, role: veladb.RoleWebhookRequest},
		{name: "webhook request as request", pool: webhookRequestPool, role: veladb.RoleRequest},
		{name: "webhook as webhook request", pool: webhookPool, role: veladb.RoleWebhookRequest},
		{name: "webhook request as webhook", pool: webhookRequestPool, role: veladb.RoleWebhook},
		{name: "webhook as internal", pool: webhookPool, role: veladb.RoleInternal},
		{name: "internal as webhook", pool: internalPool, role: veladb.RoleWebhook},
		{name: "request as webhook", pool: requestPool, role: veladb.RoleWebhook},
		{name: "webhook as request", pool: webhookPool, role: veladb.RoleRequest},
		{name: "billing as webhook", pool: billingPool, role: veladb.RoleWebhook},
		{name: "webhook as billing", pool: webhookPool, role: veladb.RoleBilling},
		{name: "remediation as request", pool: remediationPool, role: veladb.RoleRequest},
		{name: "request as remediation", pool: requestPool, role: veladb.RoleRemediation},
		{name: "remediation as internal", pool: remediationPool, role: veladb.RoleInternal},
		{name: "internal as remediation", pool: internalPool, role: veladb.RoleRemediation},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := veladb.VerifyRole(context.Background(), test.pool, test.role); err == nil {
				t.Fatalf("role confusion %s was accepted", test.name)
			}
		})
	}

	if _, err := database.Admin.Exec("GRANT SELECT ON jobs TO vela_auth_login"); err != nil {
		t.Fatalf("grant unexpected auth table privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), authPool, veladb.RoleAuth); err == nil {
		t.Fatal("auth login with direct table access was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE SELECT ON jobs FROM vela_auth_login"); err != nil {
		t.Fatalf("revoke unexpected auth table privilege: %v", err)
	}
	if _, err := database.Admin.Exec(
		"GRANT SELECT ON project_identity_events TO vela_identity_request_login",
	); err != nil {
		t.Fatalf("grant unexpected identity request table privilege: %v", err)
	}
	if err := veladb.VerifyRole(
		context.Background(), identityRequestPool, veladb.RoleIdentityRequest,
	); err == nil {
		t.Fatal("identity request login with direct audit access was accepted")
	}
	if _, err := database.Admin.Exec(
		"REVOKE SELECT ON project_identity_events FROM vela_identity_request_login",
	); err != nil {
		t.Fatalf("revoke unexpected identity request table privilege: %v", err)
	}
	if _, err := database.Admin.Exec(
		"GRANT SELECT ON charges TO vela_organization_billing_request_login",
	); err != nil {
		t.Fatalf("grant unexpected Organization billing Charge privilege: %v", err)
	}
	if err := veladb.VerifyRole(
		context.Background(),
		organizationBillingRequestPool,
		veladb.RoleOrganizationBillingRequest,
	); err == nil {
		t.Fatal("Organization billing request login with direct Charge access was accepted")
	}
	if _, err := database.Admin.Exec(
		"REVOKE SELECT ON charges FROM vela_organization_billing_request_login",
	); err != nil {
		t.Fatalf("revoke unexpected Organization billing Charge privilege: %v", err)
	}
	if _, err := database.Admin.Exec(
		"GRANT SELECT ON human_identity_events TO vela_organization_audit_request_login",
	); err != nil {
		t.Fatalf("grant unexpected Organization audit event privilege: %v", err)
	}
	if err := veladb.VerifyRole(
		context.Background(),
		organizationAuditRequestPool,
		veladb.RoleOrganizationAuditRequest,
	); err == nil {
		t.Fatal("Organization audit request login with direct identity-event access was accepted")
	}
	if _, err := database.Admin.Exec(
		"REVOKE SELECT ON human_identity_events FROM vela_organization_audit_request_login",
	); err != nil {
		t.Fatalf("revoke unexpected Organization audit event privilege: %v", err)
	}

	if _, err := database.Admin.Exec(`
        CREATE ROLE vela_rogue_privileged NOLOGIN BYPASSRLS;
        GRANT vela_rogue_privileged TO vela_auth_login;
    `); err != nil {
		t.Fatalf("grant rogue privileged role: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), authPool, veladb.RoleAuth); err == nil {
		t.Fatal("auth login that can SET ROLE to BYPASSRLS was accepted")
	}

	if _, err := database.Admin.Exec(`
		CREATE ROLE vela_rogue_reader NOLOGIN;
		GRANT SELECT ON credentials TO vela_rogue_reader;
		GRANT vela_rogue_reader TO vela_request_login;
	`); err != nil {
		t.Fatalf("grant rogue data-reading role: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("request login that can SET ROLE to an unexpected data-reading role was accepted")
	}
	if _, err := database.Admin.Exec(`
		REVOKE vela_rogue_reader FROM vela_request_login;
		REVOKE SELECT ON credentials FROM vela_rogue_reader;
		DROP ROLE vela_rogue_reader;
	`); err != nil {
		t.Fatalf("remove rogue data-reading role: %v", err)
	}

	if _, err := database.Admin.Exec("GRANT DELETE ON jobs TO vela_request_login"); err != nil {
		t.Fatalf("grant unexpected request table privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("request login with direct Job deletion privilege was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE DELETE ON jobs FROM vela_request_login"); err != nil {
		t.Fatalf("revoke unexpected request Job deletion privilege: %v", err)
	}

	if _, err := database.Admin.Exec("GRANT SELECT ON artifacts TO vela_artifact_request_login"); err != nil {
		t.Fatalf("grant unexpected Artifact staging privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), artifactPool, veladb.RoleArtifactRequest); err == nil {
		t.Fatal("Artifact request login with staging Artifact access was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE SELECT ON artifacts FROM vela_artifact_request_login"); err != nil {
		t.Fatalf("revoke unexpected Artifact staging privilege: %v", err)
	}

	if _, err := database.Admin.Exec("GRANT SELECT ON jobs TO vela_scheduler_login"); err != nil {
		t.Fatalf("grant unexpected Scheduler table privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), schedulerPool, veladb.RoleScheduler); err == nil {
		t.Fatal("Scheduler login with direct Job access was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE SELECT ON jobs FROM vela_scheduler_login"); err != nil {
		t.Fatalf("revoke unexpected Scheduler table privilege: %v", err)
	}
	if _, err := database.Admin.Exec("GRANT SELECT ON inbox_receipts TO vela_scheduler_inbox_login"); err != nil {
		t.Fatalf("grant unexpected Scheduler Inbox table privilege: %v", err)
	}
	if err := veladb.VerifyRole(
		context.Background(), schedulerInboxPool, veladb.RoleSchedulerInbox,
	); err == nil {
		t.Fatal("Scheduler Inbox login with direct receipt access was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE SELECT ON inbox_receipts FROM vela_scheduler_inbox_login"); err != nil {
		t.Fatalf("revoke unexpected Scheduler Inbox table privilege: %v", err)
	}
	if _, err := database.Admin.Exec("GRANT SELECT ON charges TO vela_billing_login"); err != nil {
		t.Fatalf("grant unexpected billing Charge privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), billingPool, veladb.RoleBilling); err == nil {
		t.Fatal("billing login with direct Charge access was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE SELECT ON charges FROM vela_billing_login"); err != nil {
		t.Fatalf("revoke unexpected billing Charge privilege: %v", err)
	}
	if _, err := database.Admin.Exec("GRANT SELECT ON webhook_deliveries TO vela_webhook_login"); err != nil {
		t.Fatalf("grant unexpected webhook Delivery privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), webhookPool, veladb.RoleWebhook); err == nil {
		t.Fatal("webhook login with direct Delivery access was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE SELECT ON webhook_deliveries FROM vela_webhook_login"); err != nil {
		t.Fatalf("revoke unexpected webhook Delivery privilege: %v", err)
	}
	if _, err := database.Admin.Exec(
		"GRANT SELECT ON webhook_subscriptions TO vela_webhook_request_login",
	); err != nil {
		t.Fatalf("grant unexpected webhook request Subscription privilege: %v", err)
	}
	if err := veladb.VerifyRole(
		context.Background(), webhookRequestPool, veladb.RoleWebhookRequest,
	); err == nil {
		t.Fatal("webhook request login with direct Subscription access was accepted")
	}
	if _, err := database.Admin.Exec(
		"REVOKE SELECT ON webhook_subscriptions FROM vela_webhook_request_login",
	); err != nil {
		t.Fatalf("revoke unexpected webhook request Subscription privilege: %v", err)
	}
	if _, err := database.Admin.Exec(
		"GRANT SELECT ON remediation_operations TO vela_remediation_login",
	); err != nil {
		t.Fatalf("grant unexpected remediation operation privilege: %v", err)
	}
	if err := veladb.VerifyRole(
		context.Background(), remediationPool, veladb.RoleRemediation,
	); err == nil {
		t.Fatal("remediation login with direct operation access was accepted")
	}
	if _, err := database.Admin.Exec(
		"REVOKE SELECT ON remediation_operations FROM vela_remediation_login",
	); err != nil {
		t.Fatalf("revoke unexpected remediation operation privilege: %v", err)
	}
	if _, err := database.Admin.Exec(`
		GRANT EXECUTE ON FUNCTION vela_transition_scheduler_dispatch_protocol(boolean, text)
		TO vela_scheduler_login
	`); err != nil {
		t.Fatalf("grant unexpected Scheduler protocol-transition privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), schedulerPool, veladb.RoleScheduler); err == nil {
		t.Fatal("Scheduler login with protocol-transition privilege was accepted")
	}
	if _, err := internalPool.Exec(context.Background(), `
		SELECT require_dispatch_intent
		FROM scheduler_dispatch_protocol_state
		WHERE singleton
	`); !isPermissionDenied(err) {
		t.Fatalf("internal runtime Scheduler protocol read error = %v, want permission denied", err)
	}
	if _, err := internalPool.Exec(context.Background(), `
		SELECT protocol_version
		FROM scheduler_dispatch_protocol_transitions
		LIMIT 1
	`); !isPermissionDenied(err) {
		t.Fatalf("internal runtime Scheduler protocol history read error = %v, want permission denied", err)
	}
	if _, err := internalPool.Exec(context.Background(), `
		UPDATE scheduler_dispatch_protocol_state
		SET require_dispatch_intent = false
		WHERE singleton
	`); !isPermissionDenied(err) {
		t.Fatalf("internal runtime direct Scheduler protocol update error = %v, want permission denied", err)
	}
	if _, err := internalPool.Exec(context.Background(), `
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'internal runtime must not own the operator switch'
		)
	`); !isPermissionDenied(err) {
		t.Fatalf("internal runtime Scheduler protocol transition error = %v, want permission denied", err)
	}

	if _, err := database.Admin.Exec(`
		GRANT USAGE ON SCHEMA vela_private TO vela_request_login;
		GRANT SELECT ON vela_private.request_contexts TO vela_request_login;
	`); err != nil {
		t.Fatalf("grant unexpected private request-context access: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("request login with private request-context access was accepted")
	}

	if _, err := database.Admin.Exec(`
		GRANT USAGE ON SCHEMA vela_private TO vela_auth_login;
		GRANT SELECT ON vela_private.request_contexts TO vela_auth_login;
	`); err != nil {
		t.Fatalf("grant unexpected private auth request-context access: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), authPool, veladb.RoleAuth); err == nil {
		t.Fatal("auth login with private request-context access was accepted")
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "billing", pool: billingPool},
		{name: "webhook request", pool: webhookRequestPool},
		{name: "webhook", pool: webhookPool},
	} {
		for _, relation := range []string{
			"job_cancellation_decisions",
			"charges",
			"cancellation_stop_receipts",
		} {
			var count int64
			err := pool.pool.QueryRow(
				context.Background(), "SELECT count(*) FROM "+relation,
			).Scan(&count)
			var permissionError *pgconn.PgError
			if !errors.As(err, &permissionError) || permissionError.Code != "42501" {
				t.Fatalf("%s direct %s read error = %v, want SQLSTATE 42501", pool.name, relation, err)
			}
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Scheduler", pool: schedulerPool},
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "billing", pool: billingPool},
		{name: "webhook request", pool: webhookRequestPool},
		{name: "webhook", pool: webhookPool},
	} {
		for _, relation := range []string{
			"webhook_subscriptions",
			"webhook_subscription_secrets",
			"webhook_deliveries",
			"webhook_delivery_attempts",
			"webhook_delivery_replays",
		} {
			var count int64
			err := pool.pool.QueryRow(
				context.Background(), "SELECT count(*) FROM "+relation,
			).Scan(&count)
			if !isPermissionDenied(err) {
				t.Fatalf(
					"%s direct %s read error = %v, want permission denied",
					pool.name,
					relation,
					err,
				)
			}
		}
	}
	var internalCredentialContext *uuid.UUID
	if err := internalPool.QueryRow(
		context.Background(), "SELECT vela_current_credential_id()",
	).Scan(&internalCredentialContext); err != nil {
		t.Fatalf("read empty internal webhook credential context: %v", err)
	}
	if internalCredentialContext != nil {
		t.Fatalf("internal backend exposed webhook credential context %s", *internalCredentialContext)
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Scheduler", pool: schedulerPool},
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "billing", pool: billingPool},
		{name: "webhook", pool: webhookPool},
		{name: "internal", pool: internalPool},
	} {
		_, err := pool.pool.Exec(
			context.Background(),
			"SELECT * FROM vela_list_webhook_subscriptions($1, 1)",
			"00000000-0000-0000-0000-000000000002",
		)
		if !isPermissionDenied(err) {
			t.Fatalf("%s webhook list error = %v, want permission denied", pool.name, err)
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Scheduler", pool: schedulerPool},
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "billing", pool: billingPool},
		{name: "webhook request", pool: webhookRequestPool},
		{name: "internal", pool: internalPool},
	} {
		_, err := pool.pool.Exec(
			context.Background(),
			"SELECT * FROM vela_claim_webhook_deliveries('unauthorized-runtime', 30, 1)",
		)
		if !isPermissionDenied(err) {
			t.Fatalf("%s webhook claim error = %v, want permission denied", pool.name, err)
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Scheduler", pool: schedulerPool},
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "billing", pool: billingPool},
		{name: "webhook request", pool: webhookRequestPool},
		{name: "webhook", pool: webhookPool},
	} {
		_, err := pool.pool.Exec(context.Background(), "SELECT vela_current_credential_id()")
		if !isPermissionDenied(err) {
			t.Fatalf(
				"%s webhook credential-context error = %v, want permission denied",
				pool.name,
				err,
			)
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Scheduler", pool: schedulerPool},
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "billing", pool: billingPool},
	} {
		for _, relation := range []string{
			"organization_capacity_shares",
			"project_capacity_shares",
			"worker_profile_readiness",
			"job_runtime_predictions",
			"scheduler_organization_deficits",
			"scheduler_service_class_deficits",
			"scheduler_project_deficits",
			"scheduler_dispatch_intents",
			"scheduler_dispatch_protocol_state",
			"scheduler_dispatch_protocol_transitions",
		} {
			var count int64
			err := pool.pool.QueryRow(
				context.Background(), "SELECT count(*) FROM "+relation,
			).Scan(&count)
			var permissionError *pgconn.PgError
			if !errors.As(err, &permissionError) || permissionError.Code != "42501" {
				t.Fatalf(
					"%s direct %s read error = %v, want SQLSTATE 42501",
					pool.name,
					relation,
					err,
				)
			}
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Scheduler", pool: schedulerPool},
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "billing", pool: billingPool},
		{name: "internal", pool: internalPool},
	} {
		for _, relation := range []string{"invoice_exports", "invoice_export_receipts"} {
			var count int64
			err := pool.pool.QueryRow(
				context.Background(), "SELECT count(*) FROM "+relation,
			).Scan(&count)
			if !isPermissionDenied(err) {
				t.Fatalf(
					"%s direct %s read error = %v, want permission denied",
					pool.name,
					relation,
					err,
				)
			}
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "Scheduler", pool: schedulerPool},
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
		{name: "internal", pool: internalPool},
	} {
		_, err := pool.pool.Exec(
			context.Background(),
			"SELECT * FROM vela_claim_invoice_exports($1, $2, $3, $4)",
			"unauthorized-runtime",
			"00000000-0000-0000-0000-000000000001",
			30,
			1,
		)
		if !isPermissionDenied(err) {
			t.Fatalf("%s Invoice export claim error = %v, want permission denied", pool.name, err)
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "cancel", pool: cancelPool},
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
	} {
		_, err := pool.pool.Exec(context.Background(), "SELECT * FROM vela_list_schedulable_worker_pools()")
		var permissionError *pgconn.PgError
		if !errors.As(err, &permissionError) || permissionError.Code != "42501" {
			t.Fatalf("%s Scheduler discovery error = %v, want SQLSTATE 42501", pool.name, err)
		}
	}

	for _, pool := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{name: "request", pool: requestPool},
		{name: "auth", pool: authPool},
		{name: "Artifact request", pool: artifactPool},
	} {
		_, err := pool.pool.Exec(
			context.Background(),
			"SELECT * FROM vela_cancel_job($1, $2, $3, $4, $5, $6, $7, $8)",
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-000000000002",
			"00000000-0000-0000-0000-000000000003",
			"00000000-0000-0000-0000-000000000004",
			"00000000-0000-0000-0000-000000000005",
			"00000000-0000-0000-0000-000000000006",
			"00000000-0000-0000-0000-000000000007",
			"00000000-0000-0000-0000-000000000008",
		)
		var permissionError *pgconn.PgError
		if !errors.As(err, &permissionError) || permissionError.Code != "42501" {
			t.Fatalf("%s cancellation function error = %v, want SQLSTATE 42501", pool.name, err)
		}
	}
}

func TestHumanIdentityEvidenceIsNotDirectlyReadableByRuntimeRoles(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	for _, runtime := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{
			name: "auth",
			pool: newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		},
		{
			name: "Human auth",
			pool: newRolePool(
				t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
			),
		},
		{
			name: "Human membership auth",
			pool: newRolePool(
				t,
				database.DSN,
				"vela_human_membership_auth_login",
				"vela-human-membership-auth-password",
			),
		},
		{
			name: "identity request",
			pool: newRolePool(
				t,
				database.DSN,
				"vela_identity_request_login",
				"vela-identity-request-password",
			),
		},
		{
			name: "Human membership request",
			pool: newRolePool(
				t,
				database.DSN,
				"vela_human_membership_request_login",
				"vela-human-membership-request-password",
			),
		},
		{
			name: "request",
			pool: newRolePool(t, database.DSN, "vela_request_login", "vela-request-password"),
		},
		{
			name: "Artifact request",
			pool: newRolePool(
				t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
			),
		},
		{
			name: "cancel",
			pool: newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password"),
		},
		{
			name: "webhook request",
			pool: newRolePool(
				t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
			),
		},
	} {
		for _, relation := range []string{
			"public.credentials",
			"public.human_oidc_bindings",
			"public.organization_role_bindings",
			"public.project_role_bindings",
			"public.human_auth_sessions",
			"public.human_organization_auth_sessions",
			"public.human_administration_actor_attributions",
			"public.human_identity_events",
			"public.project_principal_attributions",
			"public.project_actor_session_attributions",
			"public.project_identity_events",
			"vela_private.request_contexts",
			"vela_private.human_administration_contexts",
		} {
			var count int64
			err := runtime.pool.QueryRow(
				context.Background(), "SELECT count(*) FROM "+relation,
			).Scan(&count)
			if !isPermissionDenied(err) {
				t.Fatalf(
					"%s direct %s read error = %v, want permission denied",
					runtime.name,
					relation,
					err,
				)
			}
		}
	}
}

func TestHumanIdentityTablesForceRowLevelSecurity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	for _, relation := range []string{
		"human_oidc_bindings",
		"organization_role_bindings",
		"project_role_bindings",
		"human_auth_sessions",
		"human_organization_auth_sessions",
		"human_administration_actor_attributions",
		"human_identity_events",
		"project_principal_attributions",
		"project_actor_session_attributions",
		"project_identity_events",
	} {
		var enabled, forced bool
		if err := database.Admin.QueryRow(`
			SELECT relrowsecurity, relforcerowsecurity
			FROM pg_catalog.pg_class
			WHERE oid = $1::regclass
		`, relation).Scan(&enabled, &forced); err != nil {
			t.Fatalf("read %s RLS flags: %v", relation, err)
		}
		if !enabled || !forced {
			t.Fatalf("%s RLS = enabled %t forced %t, want both true", relation, enabled, forced)
		}
	}
}

func isPermissionDenied(err error) bool {
	var permissionError *pgconn.PgError
	return errors.As(err, &permissionError) && permissionError.Code == "42501"
}
