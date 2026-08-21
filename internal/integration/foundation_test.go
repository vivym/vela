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

	"github.com/google/uuid"
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
	assertTableExists(t, db, "attempts")
	assertTableExists(t, db, "execution_failure_decisions")
	assertTableExists(t, db, "execution_retry_evidence")

	if err := goose.DownTo(db, migrations, 0); err != nil {
		t.Fatalf("migrate down to zero: %v", err)
	}
	assertRoleExists(t, db, "vela_request")
	assertRoleExists(t, db, "vela_auth")
	assertRoleExists(t, db, "vela_internal")
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

func TestExecutionFailureMigrationDownUpPreservesProtectedEvidence(t *testing.T) {
	fixture := newAssignmentFixture(t, "migration-protected-evidence", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create terminal migration Assignment: %v", err)
	}
	observation := validFailureObservation()
	observation.FailureClass = "FATAL_BACKEND"
	observation.FailureFingerprint = "migration.worker.lost"
	observation.RetryRecommended = false
	if _, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	); err != nil {
		t.Fatalf("create terminal migration decision: %v", err)
	}
	jobID := assignment.JobID.String()
	var decisionSnapshot string
	if err := fixture.database.Admin.QueryRow(`
		SELECT to_jsonb(decision)::text
		FROM execution_failure_decisions AS decision
		WHERE job_id = $1
	`, jobID).Scan(&decisionSnapshot); err != nil {
		t.Fatalf("read terminal decision snapshot: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE execution_retry_evidence
		SET excluded_workers = '[{
			"worker_id":"00000000-0000-0000-0000-000000000090",
			"worker_epoch":7,
			"reason":"WORKER_LOST",
			"expires_at":"2099-01-01T00:00:00Z"
		}]'::jsonb
		WHERE job_id = $1
	`, jobID); err != nil {
		t.Fatalf("seed protected migration evidence: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 4); err != nil {
		t.Fatalf("migrate execution failure down with durable evidence: %v", err)
	}
	assertTableDoesNotExist(t, fixture.database.Admin, "execution_retry_evidence")
	var downPublicEvidenceCleared, privateEvidencePreserved bool
	var evidenceRequestRoleDenied, decisionPreserved, decisionRequestRoleDenied bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(
				SELECT excluded_workers = '[]'::jsonb AND failure_fingerprints = '[]'::jsonb
				FROM retry_runtime_states
				WHERE job_id = $1
			),
			(
				SELECT
					excluded_workers @> '[{
						"worker_id":"00000000-0000-0000-0000-000000000090"
					}]'::jsonb
					AND failure_fingerprints @> '[{
						"fingerprint":"migration.worker.lost"
					}]'::jsonb
				FROM vela_private.execution_retry_evidence_rollback
				WHERE job_id = $1
			),
			NOT has_table_privilege(
				'vela_request',
				'vela_private.execution_retry_evidence_rollback',
				'SELECT'
			),
			(
				SELECT decision = $2::jsonb
				FROM vela_private.execution_failure_decisions_rollback
				WHERE decision ->> 'job_id' = $1::text
			),
			NOT has_table_privilege(
				'vela_request',
				'vela_private.execution_failure_decisions_rollback',
				'SELECT'
			)
	`, jobID, decisionSnapshot).Scan(
		&downPublicEvidenceCleared,
		&privateEvidencePreserved,
		&evidenceRequestRoleDenied,
		&decisionPreserved,
		&decisionRequestRoleDenied,
	); err != nil {
		t.Fatalf("read evidence after migration down: %v", err)
	}
	if !downPublicEvidenceCleared || !privateEvidencePreserved || !evidenceRequestRoleDenied ||
		!decisionPreserved || !decisionRequestRoleDenied {
		t.Fatalf(
			"migration down = public evidence cleared %t private evidence preserved %t evidence denied %t decision preserved %t decision denied %t",
			downPublicEvidenceCleared,
			privateEvidencePreserved,
			evidenceRequestRoleDenied,
			decisionPreserved,
			decisionRequestRoleDenied,
		)
	}

	if err := goose.Up(fixture.database.Admin, migrations); err != nil {
		t.Fatalf("migrate execution failure up after data-preserving down: %v", err)
	}
	var publicEvidenceCleared, protectedEvidenceRestored, decisionRestored bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			runtime.excluded_workers = '[]'::jsonb
				AND runtime.failure_fingerprints = '[]'::jsonb,
			evidence.excluded_workers @> '[{
				"worker_id":"00000000-0000-0000-0000-000000000090"
			}]'::jsonb
				AND evidence.failure_fingerprints @> '[{
					"fingerprint":"migration.worker.lost"
				}]'::jsonb,
			(
				SELECT to_jsonb(decision) = $2::jsonb
				FROM execution_failure_decisions AS decision
				WHERE decision.job_id = $1
			)
		FROM retry_runtime_states AS runtime
		JOIN execution_retry_evidence AS evidence USING (job_id)
		WHERE runtime.job_id = $1
	`, jobID, decisionSnapshot).Scan(
		&publicEvidenceCleared,
		&protectedEvidenceRestored,
		&decisionRestored,
	); err != nil {
		t.Fatalf("read evidence after migration re-up: %v", err)
	}
	if !publicEvidenceCleared || !protectedEvidenceRestored || !decisionRestored {
		t.Fatalf(
			"migration re-up = public evidence cleared %t protected evidence restored %t decision restored %t",
			publicEvidenceCleared,
			protectedEvidenceRestored,
			decisionRestored,
		)
	}
}

func TestExecutionFailureMigrationDownRefusesActiveRetryWait(t *testing.T) {
	fixture := newAssignmentFixture(t, "migration-active-retry", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create migration Retry Assignment: %v", err)
	}
	if _, err := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); err != nil {
		t.Fatalf("start migration Retry Assignment: %v", err)
	}
	if _, err := fixture.service.Fail(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		validFailureObservation(),
	); err != nil {
		t.Fatalf("create active RETRY_WAIT migration state: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	for index := range 10 {
		accepted := submitJob(t, server.URL, "migration-active-retry-"+string(rune('a'+index)), []byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"fill the normal queue beside an active Retry"
		}`))
		if accepted.StatusCode != http.StatusAccepted {
			t.Fatalf("submit queued Job %d status = %d; body=%s", index, accepted.StatusCode, accepted.Body)
		}
	}
	var projectQueued, projectRetry, projectLimit int
	if err := fixture.database.Admin.QueryRow(`
		SELECT queued_count, retry_wait_count, queued_limit
		FROM projects
		WHERE id = $1
	`, testProjectID).Scan(&projectQueued, &projectRetry, &projectLimit); err != nil {
		t.Fatalf("read full normal queue plus Retry counters: %v", err)
	}
	if projectQueued != projectLimit+1 || projectRetry != 1 {
		t.Fatalf(
			"full normal queue plus Retry counters = total %d retry %d limit %d",
			projectQueued,
			projectRetry,
			projectLimit,
		)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(fixture.database.Admin, migrations, 4)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "execution_failure_contract_requires_drained_retry_wait" {
		t.Fatalf("migration down with active Retry error = %v, want fail-closed SQLSTATE 55000", err)
	}
	version, versionErr := goose.GetDBVersion(fixture.database.Admin)
	if versionErr != nil {
		t.Fatalf("read migration version after refused down: %v", versionErr)
	}
	if version != 5 {
		t.Fatalf("migration version after refused down = %d, want 5", version)
	}
	assertTableExists(t, fixture.database.Admin, "execution_retry_evidence")
}

func TestExecutionFailureMigrationDownSerializesWithConcurrentFail(t *testing.T) {
	fixture := newAssignmentFixture(t, "migration-concurrent-fail", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create concurrent migration Assignment: %v", err)
	}
	const advisoryLockKey int64 = 580005
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_retry_evidence_update() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(580005);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_retry_evidence_update
		BEFORE UPDATE ON execution_retry_evidence
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_retry_evidence_update();
	`); err != nil {
		t.Fatalf("install concurrent migration pause trigger: %v", err)
	}
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin advisory-lock blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		t.Fatalf("acquire advisory-lock blocker: %v", err)
	}

	failErrors := make(chan error, 1)
	go func() {
		_, failErr := fixture.service.Fail(
			context.Background(),
			fixture.worker,
			leaseCredentials(assignment),
			validFailureObservation(),
		)
		failErrors <- failErr
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	downErrors := make(chan error, 1)
	go func() {
		downErrors <- goose.DownTo(fixture.database.Admin, migrations, 4)
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "postgres")
	var unlocked bool
	if err := blocker.QueryRow("SELECT pg_advisory_unlock($1)", advisoryLockKey).Scan(&unlocked); err != nil {
		t.Fatalf("release advisory-lock blocker: %v", err)
	}
	if !unlocked {
		t.Fatal("advisory-lock blocker was not held")
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit advisory-lock blocker: %v", err)
	}
	select {
	case err := <-failErrors:
		if err != nil {
			t.Fatalf("concurrent Fail: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Fail did not finish")
	}
	select {
	case err := <-downErrors:
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
			postgresError.ConstraintName != "execution_failure_contract_requires_drained_retry_wait" {
			t.Fatalf("concurrent migration Down error = %v, want drained-Retry refusal", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent migration Down did not finish")
	}
	var jobState string
	var decisions, protectedFingerprints int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			(SELECT count(*) FROM execution_failure_decisions WHERE job_id = job.id),
			jsonb_array_length(evidence.failure_fingerprints)
		FROM jobs AS job
		JOIN execution_retry_evidence AS evidence ON evidence.job_id = job.id
		WHERE job.id = $1
	`, assignment.JobID).Scan(&jobState, &decisions, &protectedFingerprints); err != nil {
		t.Fatalf("read concurrent Fail/migration result: %v", err)
	}
	if jobState != "RETRY_WAIT" || decisions != 1 || protectedFingerprints != 1 {
		t.Fatalf(
			"concurrent Fail/migration result = state %s decisions %d fingerprints %d",
			jobState,
			decisions,
			protectedFingerprints,
		)
	}
}

func TestExecutionFailureMigrationDoesNotTrustLegacyRequestEvidence(t *testing.T) {
	database := newPostgres(t)
	repositoryRoot := repositoryRoot(t)
	bootstrapSQL, err := os.ReadFile(filepath.Join(repositoryRoot, "db", "bootstrap", "roles.sql"))
	if err != nil {
		t.Fatalf("read role bootstrap: %v", err)
	}
	if _, err := database.Admin.Exec(string(bootstrapSQL)); err != nil {
		t.Fatalf("apply role bootstrap: %v", err)
	}
	migrations := filepath.Join(repositoryRoot, "db", "migrations")
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set goose dialect: %v", err)
	}
	if err := goose.UpTo(database.Admin, migrations, 4); err != nil {
		t.Fatalf("migrate to N-1 schema: %v", err)
	}
	if _, err := database.Admin.Exec(`
		CREATE ROLE vela_auth_login LOGIN PASSWORD 'vela-auth-password' IN ROLE vela_auth;
		CREATE ROLE vela_request_login LOGIN PASSWORD 'vela-request-password' IN ROLE vela_request;
		CREATE ROLE vela_internal_login LOGIN PASSWORD 'vela-internal-password' BYPASSRLS IN ROLE vela_internal;
	`); err != nil {
		t.Fatalf("create N-1 application login roles: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneFailureControlCommit)
	templateJobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)

	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	requestTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin legacy poison transaction: %v", err)
	}
	defer func() { _ = requestTx.Rollback(context.Background()) }()
	if _, err := requestTx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		"jobs:submit",
	); err != nil {
		t.Fatalf("establish legacy jobs:submit context: %v", err)
	}
	poisonedJobID := uuid.New()
	if err := cloneRequestRoleJob(
		context.Background(), requestTx, poisonedJobID, templateJobID,
	); err != nil {
		t.Fatalf("clone legacy request-role Job: %v", err)
	}
	if err := cloneRequestRoleCreditReservation(
		context.Background(), requestTx, uuid.New(), poisonedJobID, templateJobID,
	); err != nil {
		t.Fatalf("clone legacy request-role CreditReservation: %v", err)
	}
	if _, err := requestTx.Exec(context.Background(), `
		INSERT INTO retry_runtime_states (
			job_id, organization_id, project_id, excluded_workers, failure_fingerprints
		) VALUES (
			$1, $2, $3,
			'[{
				"worker_id":"00000000-0000-0000-0000-000000000998",
				"worker_epoch":98,
				"reason":"legacy-forged",
				"expires_at":"2099-01-01T00:00:00Z"
			}]'::jsonb,
			'["legacy.forged.fingerprint"]'::jsonb
		)
	`, poisonedJobID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("insert legacy request-controlled retry evidence: %v", err)
	}
	if err := requestTx.Commit(context.Background()); err != nil {
		t.Fatalf("commit legacy request-controlled retry evidence: %v", err)
	}

	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("migrate poisoned N-1 row to execution failure schema: %v", err)
	}
	var publicEvidenceCleared, protectedEvidenceEmpty bool
	if err := database.Admin.QueryRow(`
		SELECT
			runtime.excluded_workers = '[]'::jsonb
				AND runtime.failure_fingerprints = '[]'::jsonb,
			evidence.excluded_workers = '[]'::jsonb
				AND evidence.failure_fingerprints = '[]'::jsonb
		FROM retry_runtime_states AS runtime
		JOIN execution_retry_evidence AS evidence USING (job_id)
		WHERE runtime.job_id = $1
	`, poisonedJobID).Scan(&publicEvidenceCleared, &protectedEvidenceEmpty); err != nil {
		t.Fatalf("read migrated legacy poison state: %v", err)
	}
	if !publicEvidenceCleared || !protectedEvidenceEmpty {
		t.Fatalf(
			"migrated legacy poison = public cleared %t protected empty %t",
			publicEvidenceCleared,
			protectedEvidenceEmpty,
		)
	}
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
