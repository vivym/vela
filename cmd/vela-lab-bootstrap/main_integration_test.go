//go:build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestBootstrapDatabaseAppliesSchema65AndReplaysStageFixture(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	container, err := postgrescontainer.Run(
		ctx,
		"postgres:17-alpine",
		postgrescontainer.WithDatabase("vela"),
		postgrescontainer.WithUsername("postgres"),
		postgrescontainer.WithPassword("vela-lab-bootstrap-integration"),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2*time.Minute),
			wait.ForMappedPort("5432/tcp").WithStartupTimeout(2*time.Minute),
		)),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("PostgreSQL connection string: %v", err)
	}
	smokeSecret := filepath.Join(t.TempDir(), "smoke-secret")
	if err := os.WriteFile(smokeSecret, []byte(strings.Repeat("s", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration := databaseIntegrationConfiguration(t, dsn, smokeSecret)

	for pass := 1; pass <= 2; pass++ {
		if err := bootstrapDatabase(ctx, configuration); err != nil {
			t.Fatalf("bootstrap database pass %d: %v", pass, err)
		}
	}

	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	version, err := goose.GetDBVersion(database)
	if err != nil || version != 65 {
		t.Fatalf("migration version = %d error=%v, want 65", version, err)
	}
	var graphState string
	var topologicalOrderJSON string
	if err := database.QueryRowContext(ctx, `
		SELECT state::text, to_json(topological_order)::text
		FROM execution_graph_revisions
		WHERE id = '84000000-0000-0000-0000-000000000501'
	`).Scan(&graphState, &topologicalOrderJSON); err != nil {
		t.Fatalf("read activated graph: %v", err)
	}
	var topologicalOrder []string
	if err := json.Unmarshal([]byte(topologicalOrderJSON), &topologicalOrder); err != nil {
		t.Fatalf("decode activated graph order: %v", err)
	}
	if graphState != "ACTIVE" || strings.Join(topologicalOrder, ",") != "encoder,dit,vae,thumbnail" {
		t.Fatalf("graph state/order = %s/%v", graphState, topologicalOrder)
	}
	for _, workerID := range []string{worker1ID, worker2ID, thumbnailWorkerID} {
		var count int
		var minimum, maximum int64
		if err := database.QueryRowContext(ctx, `
			SELECT count(*), min(observation_sequence), max(observation_sequence)
			FROM capacity_observations
			WHERE worker_instance_id = $1
		`, workerID).Scan(&count, &minimum, &maximum); err != nil {
			t.Fatalf("read capacity observations for %s: %v", workerID, err)
		}
		if count != 2 || minimum != 1 || maximum != 2 {
			t.Fatalf("capacity observations for %s = count:%d sequence:%d..%d", workerID, count, minimum, maximum)
		}
	}
	var productionGateReceipts int
	if err := database.QueryRowContext(ctx, `SELECT count(*) FROM production_gate_receipts`).Scan(&productionGateReceipts); err != nil {
		t.Fatal(err)
	}
	if productionGateReceipts != 0 {
		t.Fatalf("Production Gate receipts = %d, want zero", productionGateReceipts)
	}
}

func databaseIntegrationConfiguration(t *testing.T, dsn, smokeSecret string) configuration {
	t.Helper()
	roles := map[string]string{
		"VELA_ARTIFACT_REPLICATION_DATABASE_URL":         "vela_artifact_replication",
		"VELA_ARTIFACT_REQUEST_DATABASE_URL":             "vela_artifact_request",
		"VELA_ATTEMPT_COORDINATOR_DATABASE_URL":          "vela_attempt_coordinator",
		"VELA_AUTH_DATABASE_URL":                         "vela_auth",
		"VELA_BACKUP_RETENTION_DATABASE_URL":             "vela_backup_retention",
		"VELA_BILLING_DATABASE_URL":                      "vela_billing",
		"VELA_BREAK_GLASS_AUDIT_DATABASE_URL":            "vela_break_glass_audit_request",
		"VELA_BREAK_GLASS_REQUEST_DATABASE_URL":          "vela_break_glass_request",
		"VELA_CANCEL_DATABASE_URL":                       "vela_cancel",
		"VELA_COMPLIANCE_DATABASE_URL":                   "vela_compliance",
		"VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL":     "vela_debug_dump_audit_request",
		"VELA_DEBUG_DUMP_REQUEST_DATABASE_URL":           "vela_debug_dump_request",
		"VELA_FINANCE_RECONCILIATION_DATABASE_URL":       "vela_finance_reconciliation",
		"VELA_FLEET_DATABASE_URL":                        "vela_fleet",
		"VELA_HUMAN_AUTH_DATABASE_URL":                   "vela_human_auth",
		"VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL":        "vela_human_membership_auth",
		"VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL":     "vela_human_membership_request",
		"VELA_IDENTITY_REQUEST_DATABASE_URL":             "vela_identity_request",
		"VELA_INTERNAL_DATABASE_URL":                     "vela_internal",
		"VELA_NON_CONTENT_EXPIRY_DATABASE_URL":           "vela_non_content_expiry",
		"VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL":   "vela_organization_audit_request",
		"VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL": "vela_organization_billing_request",
		"VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL":       "vela_platform_operator_auth",
		"VELA_REMEDIATION_DATABASE_URL":                  "vela_remediation",
		"VELA_REQUEST_DATABASE_URL":                      "vela_request",
		"VELA_RETENTION_DATABASE_URL":                    "vela_retention",
		"VELA_RETENTION_REQUEST_DATABASE_URL":            "vela_retention_request",
		"VELA_STAGE_ARTIFACT_DATABASE_URL":               "vela_stage_artifact",
		"VELA_STAGE_SCHEDULER_DATABASE_URL":              "vela_stage_scheduler",
		"VELA_STAGE_WORKER_CONTROL_DATABASE_URL":         "vela_stage_worker_control",
		"VELA_WEBHOOK_DATABASE_URL":                      "vela_webhook",
		"VELA_WEBHOOK_REQUEST_DATABASE_URL":              "vela_webhook_request",
	}
	if len(roles) != len(requiredDatabaseEnvironments) {
		t.Fatalf("integration role map = %d, want %d", len(roles), len(requiredDatabaseEnvironments))
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	databaseURLs := make(map[string]string, len(roles))
	passwords := make(map[string]string, len(roles))
	for environment, group := range roles {
		login := group + "_login"
		password := strings.Repeat("a", 48)
		roleURL := *parsed
		roleURL.User = url.UserPassword(login, password)
		databaseURLs[environment] = roleURL.String()
		passwords[login] = password
	}
	return configuration{
		options: options{
			databaseRoot: repositoryRoot(t, "db"),
			smokeSecret:  smokeSecret,
		},
		adminDatabaseURL:            dsn,
		databaseURLs:                databaseURLs,
		loginPasswords:              passwords,
		credentialPepper:            []byte(strings.Repeat("p", 32)),
		runtimeImageDigest:          testRuntimeDigest,
		thumbnailRuntimeImageDigest: testThumbnailRuntimeDigest,
	}
}

func repositoryRoot(t *testing.T, path ...string) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	parts := append([]string{filepath.Dir(filename), "..", ".."}, path...)
	return filepath.Clean(filepath.Join(parts...))
}
