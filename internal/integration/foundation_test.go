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
	"strings"
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
	seedStageExecutionCatalog(t, database.Admin)
	seedH3ProfileCertification(t, database.Admin)

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
			output_spec_id, stage_cutover_revision_id, execution_graph_revision_id,
			stage_execution_profile_revision_id, request_hash, request_content,
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
			service_class_revision_id, output_spec_id, stage_cutover_revision_id,
			execution_graph_revision_id, stage_execution_profile_revision_id, request_hash,
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
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "stage_graph_admission_requires_atomic_instantiation" {
		t.Fatalf("bare Job commit error = %v, want atomic Stage graph rejection", err)
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
	if err := goose.UpTo(db, migrations, 57); err != nil {
		t.Fatalf("migrate reversible history up: %v", err)
	}
	assertTableExists(t, db, "retry_runtime_states")
	assertTableExists(t, db, "attempts")
	assertTableExists(t, db, "execution_failure_decisions")
	assertTableExists(t, db, "execution_retry_evidence")

	if err := goose.DownTo(db, migrations, 0); err != nil {
		t.Fatalf("migrate down to zero: %v", err)
	}
	assertRoleExists(t, db, "vela_request")
	assertRoleExists(t, db, "vela_auth")
	assertRoleExists(t, db, "vela_internal")
	assertRoleExists(t, db, "vela_billing")
	assertRoleExists(t, db, "vela_billing_owner")
	assertRoleExists(t, db, "vela_finance_reconciliation")
	assertRoleExists(t, db, "vela_finance_reconciliation_owner")
	assertRoleExists(t, db, "vela_compliance")
	assertRoleExists(t, db, "vela_compliance_owner")
	assertRoleExists(t, db, "vela_organization_reporting_owner")
	assertTableDoesNotExist(t, db, "attempts")
	assertTableDoesNotExist(t, db, "execution_failure_decisions")
	assertTableDoesNotExist(t, db, "execution_retry_evidence")

	if err := goose.Up(db, migrations); err != nil {
		t.Fatalf("second migrate up: %v", err)
	}
	assertTableExists(t, db, "retry_runtime_states")
	assertTableExists(t, db, "attempts")
	assertTableExists(t, db, "execution_failure_decisions")
	assertTableExists(t, db, "execution_retry_evidence")
}

func TestHierarchicalSchedulerMigrationEmptyDownUpRestoresSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 8)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 7); err != nil {
		t.Fatalf("contract empty Hierarchical Scheduler migration: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "organization_capacity_shares")
	assertTableDoesNotExist(t, database.Admin, "worker_profile_readiness")
	assertTableDoesNotExist(t, database.Admin, "scheduler_dispatch_intents")
	assertTableDoesNotExist(t, database.Admin, "scheduler_dispatch_protocol_state")
	var contractedProgressView string
	if err := database.Admin.QueryRow(`
		SELECT pg_get_viewdef('vela_request_job_progress'::regclass, true)
	`).Scan(&contractedProgressView); err != nil {
		t.Fatalf("read contracted customer progress view: %v", err)
	}
	if strings.Contains(contractedProgressView, "vela_scheduler_queue_projection") {
		t.Fatalf("contracted customer progress view retained Scheduler dependency: %s", contractedProgressView)
	}
	var contractedQueueProjection sql.NullString
	if err := database.Admin.QueryRow(`
		SELECT to_regprocedure('vela_scheduler_queue_projection()')::text
	`).Scan(&contractedQueueProjection); err != nil {
		t.Fatalf("inspect contracted request queue projection: %v", err)
	}
	if contractedQueueProjection.Valid {
		t.Fatalf("contracted request queue projection still exists: %s", contractedQueueProjection.String)
	}
	if err := goose.UpTo(database.Admin, migrations, 8); err != nil {
		t.Fatalf("re-expand empty Hierarchical Scheduler migration: %v", err)
	}
	assertTableExists(t, database.Admin, "organization_capacity_shares")
	assertTableExists(t, database.Admin, "worker_profile_readiness")
	assertTableExists(t, database.Admin, "scheduler_dispatch_intents")
	assertTableExists(t, database.Admin, "scheduler_dispatch_protocol_state")
	var expandedProgressView string
	if err := database.Admin.QueryRow(`
		SELECT pg_get_viewdef('vela_request_job_progress'::regclass, true)
	`).Scan(&expandedProgressView); err != nil {
		t.Fatalf("read re-expanded customer progress view: %v", err)
	}
	if expandedProgressView != contractedProgressView {
		t.Fatalf(
			"re-expanded customer progress view changed the released request-role surface:\n%s",
			expandedProgressView,
		)
	}
	var expandedQueueProjection string
	if err := database.Admin.QueryRow(`
		SELECT to_regprocedure('vela_scheduler_queue_projection()')::text
	`).Scan(&expandedQueueProjection); err != nil {
		t.Fatalf("inspect re-expanded request queue projection: %v", err)
	}
	if expandedQueueProjection != "vela_scheduler_queue_projection()" {
		t.Fatalf("re-expanded request queue projection = %q", expandedQueueProjection)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 8 {
		t.Fatalf("migration version after empty Scheduler Down/Up = %d error=%v", version, err)
	}
}

func TestHierarchicalSchedulerMigrationActiveAttemptIndexesRoundTrip(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 8)

	assertIndex := func(name string, wantColumns []string, predicateParts ...string) {
		t.Helper()
		var (
			columns   string
			predicate string
		)
		if err := database.Admin.QueryRow(`
			SELECT
				array_to_string(ARRAY(
					SELECT attribute.attname
					FROM unnest(index.indkey) WITH ORDINALITY AS key(attribute_number, position)
					JOIN pg_attribute AS attribute
					  ON attribute.attrelid = index.indrelid
					 AND attribute.attnum = key.attribute_number
					ORDER BY key.position
				), ','),
				pg_get_expr(index.indpred, index.indrelid)
			FROM pg_index AS index
			JOIN pg_class AS relation ON relation.oid = index.indrelid
			JOIN pg_class AS index_relation ON index_relation.oid = index.indexrelid
			JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = 'public'
			  AND relation.relname = 'attempts'
			  AND index_relation.relname = $1
		`, name).Scan(&columns, &predicate); err != nil {
			t.Fatalf("read active Attempt index %s: %v", name, err)
		}
		if columns != strings.Join(wantColumns, ",") {
			t.Fatalf("active Attempt index %s columns = %v, want %v", name, columns, wantColumns)
		}
		for _, part := range predicateParts {
			if !strings.Contains(predicate, part) {
				t.Fatalf("active Attempt index %s predicate = %q, want %q", name, predicate, part)
			}
		}
	}
	assertIndexes := func() {
		t.Helper()
		assertIndex(
			"attempts_active_pool_organization_idx",
			[]string{"worker_pool_id", "organization_id"},
			"ASSIGNED",
			"RUNNING",
			"FINALIZING",
		)
		assertIndex(
			"attempts_active_retry_pool_idx",
			[]string{"worker_pool_id"},
			"attempt_number > 1",
			"ASSIGNED",
			"RUNNING",
			"FINALIZING",
		)
	}
	assertIndexes()

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 7); err != nil {
		t.Fatalf("contract Scheduler migration with active Attempt indexes: %v", err)
	}
	for _, name := range []string{
		"attempts_active_pool_organization_idx",
		"attempts_active_retry_pool_idx",
	} {
		var exists bool
		if err := database.Admin.QueryRow(
			"SELECT to_regclass('public.' || $1) IS NOT NULL",
			name,
		).Scan(&exists); err != nil {
			t.Fatalf("inspect contracted index %s: %v", name, err)
		}
		if exists {
			t.Fatalf("active Attempt index %s survived Scheduler migration Down", name)
		}
	}
	if err := goose.UpTo(database.Admin, migrations, 8); err != nil {
		t.Fatalf("re-expand Scheduler active Attempt indexes: %v", err)
	}
	assertIndexes()
}

func TestHierarchicalSchedulerMigrationDefaultPolicyCatalogDownUpRestoresSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 8)
	seedAdmissionFixture(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 7); err != nil {
		t.Fatalf("contract default-policy Hierarchical Scheduler migration: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "scheduler_dispatch_intents")
	if err := goose.UpTo(database.Admin, migrations, 8); err != nil {
		t.Fatalf("re-expand default-policy Hierarchical Scheduler migration: %v", err)
	}
	assertTableExists(t, database.Admin, "scheduler_dispatch_intents")
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 8 {
		t.Fatalf("migration version after default-policy Down/Up = %d error=%v", version, err)
	}
}

func TestHierarchicalSchedulerProtocolRollbackRetainsReceiptAndBlocksDown(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 8)

	assertProtocolState := func(
		wantRequired bool,
		wantVersion int64,
		wantReceipt sql.NullString,
		wantTransitioned bool,
	) {
		t.Helper()
		var (
			required     bool
			version      int64
			receipt      sql.NullString
			transitioned sql.NullTime
		)
		if err := database.Admin.QueryRow(`
			SELECT require_dispatch_intent, protocol_version,
				transition_receipt, transitioned_at
			FROM scheduler_dispatch_protocol_state
			WHERE singleton
		`).Scan(&required, &version, &receipt, &transitioned); err != nil {
			t.Fatalf("read Scheduler protocol state: %v", err)
		}
		if required != wantRequired || version != wantVersion ||
			receipt != wantReceipt || transitioned.Valid != wantTransitioned {
			t.Fatalf(
				"Scheduler protocol state = required %t version %d receipt %#v transitioned %t",
				required,
				version,
				receipt,
				transitioned.Valid,
			)
		}
	}

	assertProtocolState(false, 1, sql.NullString{}, false)
	_, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(false, '   ')
	`)
	var receiptError *pgconn.PgError
	if !errors.As(err, &receiptError) || receiptError.Code != "22023" {
		t.Fatalf("blank Scheduler rollback receipt error = %v", err)
	}
	assertProtocolState(false, 1, sql.NullString{}, false)

	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'N-1 writers drained before Scheduler protocol enable'
		)
	`); err != nil {
		t.Fatalf("enable Scheduler protocol: %v", err)
	}
	_, err = database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(false, NULL)
	`)
	if !errors.As(err, &receiptError) || receiptError.Code != "22023" {
		t.Fatalf("missing Scheduler rollback receipt error = %v", err)
	}
	assertProtocolState(
		true,
		2,
		sql.NullString{String: "N-1 writers drained before Scheduler protocol enable", Valid: true},
		true,
	)

	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			false,
			'operator verified binary rollback before protocol disable'
		)
	`); err != nil {
		t.Fatalf("disable Scheduler protocol with rollback receipt: %v", err)
	}
	assertProtocolState(
		false,
		3,
		sql.NullString{String: "operator verified binary rollback before protocol disable", Valid: true},
		true,
	)
	var (
		enableRequired   bool
		enableReceipt    string
		enableTime       time.Time
		rollbackRequired bool
		rollbackReceipt  string
		rollbackTime     time.Time
	)
	if err := database.Admin.QueryRow(`
		SELECT require_dispatch_intent, transition_receipt, transitioned_at
		FROM scheduler_dispatch_protocol_transitions
		WHERE protocol_version = 2
	`).Scan(&enableRequired, &enableReceipt, &enableTime); err != nil {
		t.Fatalf("read Scheduler protocol enable history: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT require_dispatch_intent, transition_receipt, transitioned_at
		FROM scheduler_dispatch_protocol_transitions
		WHERE protocol_version = 3
	`).Scan(&rollbackRequired, &rollbackReceipt, &rollbackTime); err != nil {
		t.Fatalf("read Scheduler protocol rollback history: %v", err)
	}
	if !enableRequired ||
		enableReceipt != "N-1 writers drained before Scheduler protocol enable" ||
		rollbackRequired ||
		rollbackReceipt != "operator verified binary rollback before protocol disable" ||
		rollbackTime.Before(enableTime) {
		t.Fatalf(
			"Scheduler protocol history = enable %t %q %s rollback %t %q %s",
			enableRequired,
			enableReceipt,
			enableTime,
			rollbackRequired,
			rollbackReceipt,
			rollbackTime,
		)
	}
	for label, statement := range map[string]string{
		"update":   "UPDATE scheduler_dispatch_protocol_transitions SET transition_receipt = 'rewritten'",
		"delete":   "DELETE FROM scheduler_dispatch_protocol_transitions",
		"truncate": "TRUNCATE scheduler_dispatch_protocol_transitions",
	} {
		_, mutationErr := database.Admin.Exec(statement)
		var postgresError *pgconn.PgError
		if !errors.As(mutationErr, &postgresError) ||
			postgresError.Code != "55000" ||
			postgresError.ConstraintName != "scheduler_dispatch_protocol_history_immutable" {
			t.Fatalf("Scheduler protocol history %s error = %v", label, mutationErr)
		}
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 7)
	assertHierarchicalSchedulerMigrationRefused(t, database.Admin, err)
}

func TestHierarchicalSchedulerMigrationDownRefusesDurableEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 8)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO organization_capacity_shares (
			worker_pool_id, organization_id, weight, running_limit
		) VALUES (
			'00000000-0000-0000-0000-000000000005', $1, 1, 1
		)
	`, testOrganizationID); err != nil {
		t.Fatalf("seed durable Scheduler evidence: %v", err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err := goose.DownTo(database.Admin, migrations, 7)
	assertHierarchicalSchedulerMigrationRefused(t, database.Admin, err)

	var shares int
	if err := database.Admin.QueryRow(
		"SELECT count(*) FROM organization_capacity_shares",
	).Scan(&shares); err != nil {
		t.Fatalf("read preserved Scheduler evidence: %v", err)
	}
	if shares != 1 {
		t.Fatalf("preserved Scheduler Capacity Shares = %d, want 1", shares)
	}
}

func TestHierarchicalSchedulerMigrationDownRefusesCustomizedPolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		update string
	}{
		{
			name: "Worker pool policy",
			update: `UPDATE worker_pools
				SET scheduler_quantum_seconds = scheduler_quantum_seconds + 1`,
		},
		{
			name: "Service Class policy",
			update: `
				INSERT INTO service_class_revisions (
					id, stable_id, revision, state, queue_retry_allowance_seconds,
					max_attempts, max_total_compute_multiplier_milli,
					max_finalization_seconds_per_attempt, retry_backoff_policy,
					retryable_failure_classes, circuit_breaker_policy, queue_weight,
					max_queue_wait_before_protection_seconds, max_aging_credit_seconds,
					max_expiry_urgency_credit_seconds, max_retry_risk_penalty_seconds
				)
				SELECT
					'00000000-0000-0000-0000-000000000498', 'migration-policy-test', 1,
					state, queue_retry_allowance_seconds, max_attempts,
					max_total_compute_multiplier_milli,
					max_finalization_seconds_per_attempt, retry_backoff_policy,
					retryable_failure_classes, circuit_breaker_policy, queue_weight,
					max_queue_wait_before_protection_seconds, max_aging_credit_seconds + 1,
					max_expiry_urgency_credit_seconds, max_retry_risk_penalty_seconds
				FROM service_class_revisions
				WHERE id = '00000000-0000-0000-0000-000000000012'`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundationTo(t, database.Admin, 8)
			seedAdmissionFixture(t, database.Admin)
			result, err := database.Admin.Exec(test.update)
			if err != nil {
				t.Fatalf("customize %s: %v", test.name, err)
			}
			if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows == 0 {
				t.Fatalf("customize %s rows = %d error=%v", test.name, rows, rowsErr)
			}
			migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
			err = goose.DownTo(database.Admin, migrations, 7)
			assertHierarchicalSchedulerMigrationRefused(t, database.Admin, err)
		})
	}
}

func assertHierarchicalSchedulerMigrationRefused(t *testing.T, database *sql.DB, err error) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "hierarchical_scheduler_contract_requires_empty_evidence" {
		t.Fatalf("Hierarchical Scheduler migration Down error = %v, want named fail-closed SQLSTATE 55000", err)
	}
	version, versionErr := goose.GetDBVersion(database)
	if versionErr != nil || version != 8 {
		t.Fatalf("migration version after refused Scheduler Down = %d error=%v", version, versionErr)
	}
	assertTableExists(t, database, "scheduler_dispatch_intents")
}

func TestArtifactFinalizationMigrationEmptyDownUpRestoresSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 7)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 6); err != nil {
		t.Fatalf("contract empty Artifact finalization migration: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "artifacts")
	assertTableDoesNotExist(t, database.Admin, "artifact_uploads")
	assertTableDoesNotExist(t, database.Admin, "artifact_sets")
	assertTableDoesNotExist(t, database.Admin, "visible_completions")
	if err := goose.UpTo(database.Admin, migrations, 7); err != nil {
		t.Fatalf("re-expand empty Artifact finalization migration: %v", err)
	}
	assertTableExists(t, database.Admin, "artifacts")
	assertTableExists(t, database.Admin, "artifact_uploads")
	assertTableExists(t, database.Admin, "artifact_sets")
	assertTableExists(t, database.Admin, "visible_completions")
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 7 {
		t.Fatalf("migration version after empty Down/Up = %d error=%v", version, err)
	}
}

func assertArtifactFinalizationMigrationRefused(t *testing.T, database *sql.DB, err error) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "artifact_finalization_contract_requires_empty_evidence" {
		t.Fatalf("Artifact finalization migration Down error = %v, want fail-closed SQLSTATE 55000", err)
	}
	version, versionErr := goose.GetDBVersion(database)
	if versionErr != nil || version != 7 {
		t.Fatalf("migration version after refused Artifact Down = %d error=%v", version, versionErr)
	}
	assertTableExists(t, database, "artifacts")
	assertTableExists(t, database, "artifact_sets")
}

type testDatabase struct {
	Admin     *sql.DB
	DSN       string
	Container testcontainers.Container
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
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(2*time.Minute),
				wait.ForMappedPort("5432/tcp").WithStartupTimeout(2*time.Minute),
			),
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
	return testDatabase{Admin: db, DSN: dsn, Container: container}
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

func assertTableDoesNotExist(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	var exists bool
	if err := db.QueryRow(
		"SELECT to_regclass('public.' || $1) IS NOT NULL",
		table,
	).Scan(&exists); err != nil {
		t.Fatalf("check table %s absence: %v", table, err)
	}
	if exists {
		t.Errorf("expected table %s not to exist", table)
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
