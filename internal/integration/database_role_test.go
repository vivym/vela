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
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	billingPool := newRolePool(t, database.DSN, "vela_billing_login", "vela-billing-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	webhookPool := newRolePool(t, database.DSN, "vela_webhook_login", "vela-webhook-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	for _, login := range []string{
		"vela_internal_login",
		"vela_billing_login",
		"vela_webhook_request_login",
		"vela_webhook_login",
	} {
		var inheritsBillingOwner bool
		if err := database.Admin.QueryRow(
			"SELECT pg_has_role($1, 'vela_billing_owner', 'MEMBER')",
			login,
		).Scan(&inheritsBillingOwner); err != nil {
			t.Fatalf("inspect %s billing-owner membership: %v", login, err)
		}
		if inheritsBillingOwner {
			t.Fatalf("runtime login %s inherits vela_billing_owner", login)
		}
	}

	for _, test := range []struct {
		name string
		pool *pgxpool.Pool
		role veladb.Role
	}{
		{name: "auth", pool: authPool, role: veladb.RoleAuth},
		{name: "Human auth", pool: humanAuthPool, role: veladb.RoleHumanAuth},
		{name: "request", pool: requestPool, role: veladb.RoleRequest},
		{name: "cancel", pool: cancelPool, role: veladb.RoleCancel},
		{name: "Artifact request", pool: artifactPool, role: veladb.RoleArtifactRequest},
		{name: "Scheduler", pool: schedulerPool, role: veladb.RoleScheduler},
		{name: "billing", pool: billingPool, role: veladb.RoleBilling},
		{name: "webhook request", pool: webhookRequestPool, role: veladb.RoleWebhookRequest},
		{name: "webhook", pool: webhookPool, role: veladb.RoleWebhook},
		{name: "internal", pool: internalPool, role: veladb.RoleInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := veladb.VerifyRole(context.Background(), test.pool, test.role); err != nil {
				t.Fatalf("verify correct %s role: %v", test.name, err)
			}
		})
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

	for _, test := range []struct {
		name string
		pool *pgxpool.Pool
		role veladb.Role
	}{
		{name: "request as internal", pool: requestPool, role: veladb.RoleInternal},
		{name: "internal as request", pool: internalPool, role: veladb.RoleRequest},
		{name: "auth as request", pool: authPool, role: veladb.RoleRequest},
		{name: "Human auth as request", pool: humanAuthPool, role: veladb.RoleRequest},
		{name: "request as Human auth", pool: requestPool, role: veladb.RoleHumanAuth},
		{name: "auth as Human auth", pool: authPool, role: veladb.RoleHumanAuth},
		{name: "Human auth as auth", pool: humanAuthPool, role: veladb.RoleAuth},
		{name: "cancel as request", pool: cancelPool, role: veladb.RoleRequest},
		{name: "request as cancel", pool: requestPool, role: veladb.RoleCancel},
		{name: "internal as cancel", pool: internalPool, role: veladb.RoleCancel},
		{name: "request as Artifact request", pool: requestPool, role: veladb.RoleArtifactRequest},
		{name: "Artifact request as request", pool: artifactPool, role: veladb.RoleRequest},
		{name: "internal as Artifact request", pool: internalPool, role: veladb.RoleArtifactRequest},
		{name: "Scheduler as request", pool: schedulerPool, role: veladb.RoleRequest},
		{name: "Scheduler as internal", pool: schedulerPool, role: veladb.RoleInternal},
		{name: "internal as Scheduler", pool: internalPool, role: veladb.RoleScheduler},
		{name: "request as Scheduler", pool: requestPool, role: veladb.RoleScheduler},
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
			"public.project_principal_attributions",
			"public.project_actor_session_attributions",
			"vela_private.request_contexts",
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
		"project_principal_attributions",
		"project_actor_session_attributions",
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
