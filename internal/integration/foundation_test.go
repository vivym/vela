//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestServicePrincipalRejectsHumanPrincipal(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	_, err := database.Admin.Exec(`
        INSERT INTO customer_organizations (id, display_name)
        VALUES ('10000000-0000-0000-0000-000000000001', 'Identity Test Organization');
        INSERT INTO projects (
            id, organization_id, display_name, queued_limit, running_limit
        ) VALUES (
            '10000000-0000-0000-0000-000000000002',
            '10000000-0000-0000-0000-000000000001',
            'Identity Test Project', 1, 1
        );
        INSERT INTO principals (id, organization_id, kind, display_name)
        VALUES (
            '10000000-0000-0000-0000-000000000003',
            '10000000-0000-0000-0000-000000000001',
            'HUMAN', 'Human Principal'
        );
        INSERT INTO service_principals (principal_id, organization_id, project_id)
        VALUES (
            '10000000-0000-0000-0000-000000000003',
            '10000000-0000-0000-0000-000000000001',
            '10000000-0000-0000-0000-000000000002'
        );
    `)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23503" {
		t.Fatalf("bind HUMAN Principal as Service Principal error = %v, want SQLSTATE 23503", err)
	}
}

func TestCredentialRequiresCreatorAttribution(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	_, err := database.Admin.Exec(`
        INSERT INTO credentials (
            id, organization_id, project_id, principal_id, secret_digest, scopes, expires_at
        ) VALUES (
            '10000000-0000-0000-0000-000000000004',
            $1, $2, $3, decode(repeat('ab', 32), 'hex'),
            ARRAY['jobs:read'], clock_timestamp() + interval '1 day'
        )
    `, testOrganizationID, testProjectID, testPrincipalID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23502" {
		t.Fatalf("credential without creator error = %v, want SQLSTATE 23502", err)
	}
}

func TestServiceClassRejectsRetryBudgetAboveTwiceCertifiedP95(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	_, err := database.Admin.Exec(`
        INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts,
            max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
            retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy
        ) VALUES (
            '10000000-0000-0000-0000-000000000012', 'invalid-budget', 1, 'REGISTERED',
            7200, 3, 2001, 600, '{}', ARRAY['WORKER_LOST'], '{}'
        )
    `)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
		t.Fatalf("retry multiplier above 2x error = %v, want SQLSTATE 23514", err)
	}
}

func TestCatalogAndRateRevisionDefinitionsAreImmutable(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	for _, test := range []struct {
		name      string
		statement string
	}{
		{name: "ModelRevision", statement: "UPDATE model_revisions SET content_hash = repeat('f', 32)"},
		{name: "GenerationPresetRevision", statement: "UPDATE generation_preset_revisions SET certified_p95_compute_seconds = 1"},
		{name: "ServiceClassRevision", statement: "UPDATE service_class_revisions SET queue_retry_allowance_seconds = 1"},
		{name: "OutputSpec", statement: "UPDATE output_specs SET codec = 'av1'"},
		{name: "ExecutionProfileRevision", statement: "UPDATE execution_profile_revisions SET stable_id = 'rewritten-profile'"},
		{name: "ProfileCertification", statement: "UPDATE profile_certifications SET evidence_digest = repeat('0', 32)"},
		{name: "RateCardRevision", statement: "UPDATE rate_card_revisions SET effective_at = effective_at - interval '1 day'"},
		{name: "RateCardLine", statement: "UPDATE rate_card_lines SET unit_amount_minor = unit_amount_minor + 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := database.Admin.Exec(test.statement)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
				t.Fatalf("revision mutation error = %v, want SQLSTATE P0001", err)
			}
		})
	}

	if _, err := database.Admin.Exec("UPDATE model_revisions SET state = 'DRAINING'"); err != nil {
		t.Fatalf("update ModelRevision lifecycle state: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE profile_certifications
		SET state = 'INVALID', invalidated_at = clock_timestamp()
	`); err != nil {
		t.Fatalf("invalidate ProfileCertification: %v", err)
	}
}

func TestAcceptedJobRequestSnapshotIsImmutable(t *testing.T) {
	server, admin := newAdmissionServer(t)
	accepted := submitJob(t, server.URL, "immutable-request", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"do not rewrite this request"
    }`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}

	_, err := admin.Exec(`
        UPDATE jobs
        SET request_content = jsonb_set(request_content, '{prompt}', '"rewritten"')
    `)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
		t.Fatalf("request snapshot mutation error = %v, want SQLSTATE P0001", err)
	}
}

func TestJobCannotCommitWithoutRequiredChildRows(t *testing.T) {
	server, admin := newAdmissionServer(t)
	accepted := submitJob(t, server.URL, "required-job-children", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"create a complete Job template"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}

	transaction, err := admin.Begin()
	if err != nil {
		t.Fatalf("begin bare Job transaction: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at
		)
		SELECT
			'10000000-0000-0000-0000-000000000090', organization_id, project_id,
			created_by_principal_id, model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, worker_pool_id, request_hash,
			request_content, request_content_expires_at, pricing_rate_card_revision_id,
			pricing_rate_line_id, pricing_unit_amount_minor, pricing_quantity,
			pricing_quoted_amount_minor, pricing_currency, execution_max_attempts,
			execution_max_total_compute_seconds, execution_max_finalization_seconds_per_attempt,
			execution_retry_backoff_policy, execution_retryable_failure_classes,
			execution_circuit_breaker_policy, job_expires_at
		FROM jobs
		LIMIT 1
	`); err != nil {
		t.Fatalf("insert bare Job clone: %v", err)
	}
	err = transaction.Commit()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
		t.Fatalf("bare Job commit error = %v, want SQLSTATE 23514", err)
	}
}

func TestJobRequiredChildRowsCannotBeDeleted(t *testing.T) {
	server, admin := newAdmissionServer(t)
	accepted := submitJob(t, server.URL, "required-job-child-deletion", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"keep both required Job child records"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var jobID string
	if err := admin.QueryRow("SELECT id FROM jobs").Scan(&jobID); err != nil {
		t.Fatalf("read Accepted Job id: %v", err)
	}

	for _, test := range []struct {
		name      string
		statement string
	}{
		{name: "CreditReservation", statement: "DELETE FROM credit_reservations WHERE job_id = $1"},
		{name: "RetryRuntimeState", statement: "DELETE FROM retry_runtime_states WHERE job_id = $1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			transaction, err := admin.Begin()
			if err != nil {
				t.Fatalf("begin child deletion transaction: %v", err)
			}
			defer func() { _ = transaction.Rollback() }()
			if _, err := transaction.Exec(test.statement, jobID); err != nil {
				t.Fatalf("delete required child row: %v", err)
			}
			err = transaction.Commit()
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
				t.Fatalf("required child deletion commit error = %v, want SQLSTATE 23514", err)
			}
		})
	}
}

func TestFoundationMigrationUpDownUp(t *testing.T) {
	database := newPostgres(t)
	db := database.Admin
	repositoryRoot := repositoryRoot(t)

	bootstrapSQL, err := os.ReadFile(filepath.Join(repositoryRoot, "db", "bootstrap", "roles.sql"))
	if err != nil {
		t.Fatalf("read role bootstrap: %v", err)
	}
	if _, err := db.Exec(string(bootstrapSQL)); err != nil {
		t.Fatalf("apply role bootstrap: %v", err)
	}

	migrations := filepath.Join(repositoryRoot, "db", "migrations")
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set goose dialect: %v", err)
	}
	if err := goose.Up(db, migrations); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	assertTableExists(t, db, "retry_runtime_states")

	if err := goose.Down(db, migrations); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	assertRoleExists(t, db, "vela_request")
	assertRoleExists(t, db, "vela_auth")
	assertRoleExists(t, db, "vela_internal")

	if err := goose.Up(db, migrations); err != nil {
		t.Fatalf("second migrate up: %v", err)
	}
	assertTableExists(t, db, "retry_runtime_states")
}

type testDatabase struct {
	Admin *sql.DB
	DSN   string
}

func newPostgres(t *testing.T) testDatabase {
	t.Helper()

	ctx := context.Background()
	container, err := postgrescontainer.Run(
		ctx,
		"postgres:17-alpine",
		postgrescontainer.WithDatabase("vela"),
		postgrescontainer.WithUsername("postgres"),
		postgrescontainer.WithPassword("vela-integration"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
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
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	return testDatabase{Admin: db, DSN: dsn}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func assertTableExists(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	var exists bool
	if err := db.QueryRow(
		"SELECT to_regclass('public.' || $1) IS NOT NULL",
		table,
	).Scan(&exists); err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	if !exists {
		t.Errorf("expected table %s to exist", table)
	}
}

func assertRoleExists(t *testing.T, db *sql.DB, role string) {
	t.Helper()
	var exists bool
	if err := db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
		role,
	).Scan(&exists); err != nil {
		t.Fatalf("check role %s: %v", role, err)
	}
	if !exists {
		t.Errorf("expected role %s to survive schema rollback", role)
	}
}
