//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/productiongates"
	"github.com/vivym/vela/internal/slo"
	"github.com/vivym/vela/internal/telemetry"
)

func TestStatisticalSLOMeasurementEnforcesCoverageAndSealsMonthlyReport(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedThreePresetCatalog(t, database)
	activateH3StageGraph(t, database)
	createSLOReportingLogin(t, database)

	promotion := newRolePool(
		t, database.DSN, "vela_catalog_promotion_login", "vela-catalog-promotion-password",
	)
	receipts := recordAndSealLaunchManifest(t, promotion)
	receiptID := receipts[productiongates.GatePresetCertification]
	reporting := newRolePool(
		t, database.DSN, "vela_slo_reporting_login", "vela-slo-reporting-password",
	)
	if err := veladb.VerifyRole(context.Background(), reporting, veladb.RoleSLOReporting); err != nil {
		t.Fatalf("verify statistical SLO reporting role: %v", err)
	}

	contracts := map[string]uuid.UUID{
		"balanced": uuid.MustParse("33000000-0000-0000-0000-000000000011"),
		"quality":  uuid.MustParse("33000000-0000-0000-0000-000000000021"),
		"fast":     uuid.MustParse("33000000-0000-0000-0000-000000000031"),
	}
	presets := map[string]uuid.UUID{
		"balanced": uuid.MustParse("00000000-0000-0000-0000-000000000011"),
		"quality":  uuid.MustParse("35000000-0000-0000-0000-000000000011"),
		"fast":     uuid.MustParse("35000000-0000-0000-0000-000000000021"),
	}
	for _, preset := range []string{"quality", "balanced", "fast"} {
		if _, err := reporting.Exec(context.Background(), `
			SELECT * FROM vela_register_slo_contract(
				$1, 'h3-standard-slo-v1', $2, $3, $4, $5,
				1, 60000, 700000, 20, $6
			)
		`, contracts[preset],
			uuid.MustParse("00000000-0000-0000-0000-000000000010"),
			presets[preset],
			uuid.MustParse("00000000-0000-0000-0000-000000000012"),
			uuid.MustParse("00000000-0000-0000-0000-000000000013"),
			receiptID,
		); err != nil {
			t.Fatalf("register %s statistical SLO contract: %v", preset, err)
		}
	}

	var protocolVersion int
	var mode string
	var replayed bool
	if err := reporting.QueryRow(context.Background(), `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_slo_measurement($1)
	`, receiptID).Scan(&protocolVersion, &mode, &replayed); !postgresConstraint(
		err,
		"statistical_slo_saleable_coverage_is_incomplete",
	) {
		t.Fatalf("enable statistical SLO measurement with only generation_count=1 error = %v", err)
	}
	windowStart := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := windowStart.AddDate(0, 1, 0)
	if _, err := reporting.Exec(context.Background(), `
		SELECT * FROM vela_seal_slo_measurement($1, $2, $3, $4)
	`, uuid.New(), contracts["quality"], windowStart, windowEnd); !postgresConstraint(
		err,
		"statistical_slo_protocol_is_not_enforced",
	) {
		t.Fatalf("seal statistical SLO report before enforcement error = %v", err)
	}
	for _, preset := range []string{"quality", "balanced", "fast"} {
		for generationCount := 2; generationCount <= 16; generationCount++ {
			if _, err := reporting.Exec(context.Background(), `
				SELECT * FROM vela_register_slo_contract(
					$1, 'h3-standard-slo-v1', $2, $3, $4, $5,
					$6, 60000, 700000, 20, $7
				)
			`, uuid.New(),
				uuid.MustParse("00000000-0000-0000-0000-000000000010"),
				presets[preset],
				uuid.MustParse("00000000-0000-0000-0000-000000000012"),
				uuid.MustParse("00000000-0000-0000-0000-000000000013"),
				generationCount,
				receiptID,
			); err != nil {
				t.Fatalf(
					"register %s generation_count=%d statistical SLO contract: %v",
					preset,
					generationCount,
					err,
				)
			}
		}
	}
	if err := reporting.QueryRow(context.Background(), `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_slo_measurement($1)
	`, receiptID).Scan(&protocolVersion, &mode, &replayed); err != nil ||
		protocolVersion != 2 || mode != "ENFORCED" || replayed {
		t.Fatalf("enable statistical SLO measurement = %d/%s/%t error=%v", protocolVersion, mode, replayed, err)
	}
	if err := reporting.QueryRow(context.Background(), `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_slo_measurement($1)
	`, receiptID).Scan(&protocolVersion, &mode, &replayed); err != nil || !replayed {
		t.Fatalf("replay statistical SLO transition = %d/%s/%t error=%v", protocolVersion, mode, replayed, err)
	}

	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "slo-covered-admission", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"freeze exact SLO revision at Admission"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("covered Admission = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var admitted jobResponse
	if err := json.Unmarshal(accepted.Body, &admitted); err != nil {
		t.Fatalf("decode covered Admission: %v", err)
	}
	coveredQuantity := submitJob(t, server.URL, "slo-covered-quantity", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":2,
		"prompt":"all accepted generation counts have exact SLO targets"
	}`))
	if coveredQuantity.StatusCode != 202 {
		t.Fatalf("generation_count=2 covered Admission = %d body=%s", coveredQuantity.StatusCode, coveredQuantity.Body)
	}
	var captured int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM job_slo_admissions`).Scan(&captured); err != nil || captured != 2 {
		t.Fatalf("captured Admission snapshots = %d error=%v", captured, err)
	}
	terminalTransaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin statistical SLO terminal fixture: %v", err)
	}
	terminalizeJobWithCanonicalEvent(
		t,
		terminalTransaction,
		uuid.MustParse(admitted.JobID),
		"FAILED",
		nil,
	)
	if err := terminalTransaction.Commit(); err != nil {
		t.Fatalf("commit statistical SLO terminal fixture: %v", err)
	}
	var terminalOutcome string
	if err := database.Admin.QueryRow(`
		SELECT outcome::text FROM job_slo_outcomes WHERE job_id = $1
	`, uuid.MustParse(admitted.JobID)).Scan(&terminalOutcome); err != nil || terminalOutcome != "FAILED" {
		t.Fatalf("captured terminal statistical SLO outcome = %q error=%v", terminalOutcome, err)
	}

	sourceObservations := seedSLOMeasurementObservations(t, database, contracts["quality"])
	reportID := uuid.MustParse("33000000-0000-0000-0000-000000000041")
	var (
		result                                                    string
		observations, eligible, succeeded, failed, canceled, open int
		p95                                                       int64
		successPPM, lowerPPM                                      int
		sourceDigest                                              string
	)
	if err := reporting.QueryRow(context.Background(), `
		SELECT result::text, observation_count, eligible_count,
			succeeded_count, failed_count, customer_canceled_count,
			open_count, p95_milliseconds, success_observed_ppm,
			success_lower_bound_ppm,
			'sha256:' || encode(source_set_digest, 'hex')
		FROM vela_seal_slo_measurement($1, $2, $3, $4)
	`, reportID, contracts["quality"], windowStart, windowEnd).Scan(
		&result, &observations, &eligible, &succeeded, &failed, &canceled,
		&open, &p95, &successPPM, &lowerPPM, &sourceDigest,
	); err != nil {
		t.Fatalf("seal statistical SLO report: %v", err)
	}
	if result != "PASS" || observations != 24 || eligible != 23 || succeeded != 20 ||
		failed != 3 || canceled != 1 || open != 0 || p95 != 19_000 ||
		successPPM != 869565 || lowerPPM < 700000 {
		t.Fatalf(
			"sealed report = result %s counts %d/%d/%d/%d/%d/%d p95 %d rate %d lower %d",
			result, observations, eligible, succeeded, failed, canceled, open, p95,
			successPPM, lowerPPM,
		)
	}
	canonicalReport, err := slo.Evaluate(slo.Contract{
		RevisionID:                 contracts["quality"].String(),
		ModelRevisionID:            "00000000-0000-0000-0000-000000000010",
		GenerationPreset:           "quality",
		GenerationPresetRevisionID: "35000000-0000-0000-0000-000000000011",
		ServiceClassRevisionID:     "00000000-0000-0000-0000-000000000012",
		OutputSpecID:               "00000000-0000-0000-0000-000000000013",
		GenerationCount:            1, P95TargetMilliseconds: 60_000,
		SuccessTargetPPM: 700_000, MinimumSample: 20,
		ConfidenceMethod:      slo.ConfidenceMethod,
		OneSidedConfidencePPM: slo.OneSidedConfidencePPM,
		CancellationPolicy:    slo.CancellationPolicy,
	}, slo.Window{Start: windowStart, End: windowEnd}, windowEnd, sourceObservations)
	if err != nil || canonicalReport.SourceSetDigest != sourceDigest {
		t.Fatalf(
			"canonical source digest = Go %q SQL %q error=%v",
			canonicalReport.SourceSetDigest,
			sourceDigest,
			err,
		)
	}
	internalPool := newRolePool(
		t,
		database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	metrics, err := telemetry.NewPostgresSLOReportReader(internalPool).LatestSLOReports(
		context.Background(),
	)
	var reported *telemetry.SLOMetric
	missingReports := 0
	for index := range metrics {
		if metrics[index].HasReport {
			reported = &metrics[index]
		} else {
			missingReports++
		}
	}
	if err != nil || len(metrics) != 48 || missingReports != 47 || reported == nil ||
		reported.GenerationPreset != "quality" || reported.GenerationCount != 1 ||
		reported.EligibleCount != 23 || reported.SucceededCount != 20 ||
		reported.FailedCount != 3 || reported.CustomerCanceledCount != 1 ||
		reported.OpenCount != 0 {
		t.Fatalf("internal statistical SLO collector reports = %#v error=%v", metrics, err)
	}

	if _, err := reporting.Exec(context.Background(), `
		INSERT INTO job_slo_outcomes (job_id, outcome, terminal_at)
		VALUES ($1, 'FAILED', clock_timestamp())
	`, uuid.New()); !isPostgresCode(err, "42501") {
		t.Fatalf("direct statistical SLO outcome insert error = %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE statistical_slo_contract_revisions SET minimum_sample = 1 WHERE id = $1
	`, contracts["quality"]); !postgresConstraint(err, "statistical_slo_evidence_is_immutable") {
		t.Fatalf("statistical SLO contract mutation error = %v", err)
	}
	if _, err := reporting.Exec(context.Background(), `
		SELECT * FROM vela_seal_slo_measurement($1, $2, $3, $4)
	`, uuid.New(), contracts["quality"], windowStart, windowEnd); !postgresConstraint(err, "statistical_slo_measurement_replay_mismatch") {
		t.Fatalf("statistical SLO report replay mismatch error = %v", err)
	}
}

func TestStatisticalSLOMigrationEmptyDownUpAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 33)
		if err := goose.DownTo(database.Admin, migrations, 32); err != nil {
			t.Fatalf("contract empty statistical SLO migration: %v", err)
		}
		if err := goose.UpTo(database.Admin, migrations, 33); err != nil {
			t.Fatalf("re-expand statistical SLO migration: %v", err)
		}
		createSLOReportingLogin(t, database)
		reporting := newRolePool(
			t, database.DSN, "vela_slo_reporting_login", "vela-slo-reporting-password",
		)
		if err := veladb.VerifyRole(context.Background(), reporting, veladb.RoleSLOReporting); err != nil {
			t.Fatalf("verify statistical SLO role after Down Up: %v", err)
		}
	})

	t.Run("durable contract refuses Down", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 33)
		seedAdmissionFixture(t, database.Admin)
		createSLOReportingLogin(t, database)
		promotion := newRolePool(
			t, database.DSN, "vela_catalog_promotion_login", "vela-catalog-promotion-password",
		)
		receiptID := recordAndSealLaunchManifest(t, promotion)[productiongates.GatePresetCertification]
		reporting := newRolePool(
			t, database.DSN, "vela_slo_reporting_login", "vela-slo-reporting-password",
		)
		if _, err := reporting.Exec(context.Background(), `
			SELECT * FROM vela_register_slo_contract(
				$1, 'h3-standard-slo-v1', $2, $3, $4, $5,
				1, 60000, 800000, 20, $6
			)
		`, uuid.New(),
			uuid.MustParse("00000000-0000-0000-0000-000000000010"),
			uuid.MustParse("00000000-0000-0000-0000-000000000011"),
			uuid.MustParse("00000000-0000-0000-0000-000000000012"),
			uuid.MustParse("00000000-0000-0000-0000-000000000013"),
			receiptID,
		); err != nil {
			t.Fatalf("register rollback-refusal contract: %v", err)
		}
		err := goose.DownTo(database.Admin, migrations, 32)
		if !postgresConstraint(err, "statistical_slo_rollback_is_unsafe") {
			t.Fatalf("statistical SLO Down refusal error = %v", err)
		}
	})
}

func TestStatisticalSLOActivationDownSerializesWithConcurrentJobWriter(t *testing.T) {
	t.Run("writer commits before activation backfill", func(t *testing.T) {
		database, reporting, receiptID := prepareSLOActivationFixture(t)
		const pauseLock int64 = 337001
		if _, err := database.Admin.Exec(`
			CREATE FUNCTION vela_test_pause_slo_admission_insert() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				PERFORM pg_advisory_xact_lock(337001);
				RETURN NEW;
			END
			$$;
			CREATE TRIGGER vela_test_pause_slo_admission_insert
			BEFORE INSERT ON job_slo_admissions
			FOR EACH ROW EXECUTE FUNCTION vela_test_pause_slo_admission_insert();
		`); err != nil {
			t.Fatalf("install SLO Admission pause trigger: %v", err)
		}
		blocker, err := database.Admin.Begin()
		if err != nil {
			t.Fatalf("begin SLO Admission pause blocker: %v", err)
		}
		defer cleanupSLOTestPause(blocker, pauseLock)
		if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", pauseLock); err != nil {
			t.Fatalf("acquire SLO Admission pause blocker: %v", err)
		}

		writerResults := make(chan sloWriterResult, 1)
		go func() {
			writerResults <- insertLegacySLOJobWriter(
				database.DSN, "serialize writer before activation",
			)
		}()
		waitForRoleDatabaseLock(t, database.Admin, "vela_request_login")

		activationResults := enableSLOMeasurementAsync(reporting, receiptID)
		waitForRoleDatabaseLock(t, database.Admin, "vela_slo_reporting_login")
		downResults := migrateSLODownAsync(t, database)
		waitForRoleDatabaseLock(t, database.Admin, "postgres")
		releaseSLOTestPause(t, blocker, pauseLock)

		assertConcurrentSLOWriter(t, writerResults)
		assertSLOActivation(t, activationResults)
		assertSLODownRefused(t, database, downResults)
	})

	t.Run("activation commits before writer capture", func(t *testing.T) {
		database, reporting, receiptID := prepareSLOActivationFixture(t)
		const pauseLock int64 = 337002
		if _, err := database.Admin.Exec(`
			CREATE FUNCTION vela_test_pause_slo_protocol_update() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				PERFORM pg_advisory_xact_lock(337002);
				RETURN NEW;
			END
			$$;
			CREATE TRIGGER vela_test_pause_slo_protocol_update
			BEFORE UPDATE ON slo_measurement_protocol_state
			FOR EACH ROW EXECUTE FUNCTION vela_test_pause_slo_protocol_update();
		`); err != nil {
			t.Fatalf("install SLO protocol pause trigger: %v", err)
		}
		blocker, err := database.Admin.Begin()
		if err != nil {
			t.Fatalf("begin SLO protocol pause blocker: %v", err)
		}
		defer cleanupSLOTestPause(blocker, pauseLock)
		if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", pauseLock); err != nil {
			t.Fatalf("acquire SLO protocol pause blocker: %v", err)
		}

		activationResults := enableSLOMeasurementAsync(reporting, receiptID)
		waitForRoleDatabaseLock(t, database.Admin, "vela_slo_reporting_login")
		writerResults := make(chan sloWriterResult, 1)
		go func() {
			writerResults <- insertLegacySLOJobWriter(
				database.DSN, "serialize activation before writer",
			)
		}()
		waitForRoleDatabaseLock(t, database.Admin, "vela_request_login")
		downResults := migrateSLODownAsync(t, database)
		waitForRoleDatabaseLock(t, database.Admin, "postgres")
		releaseSLOTestPause(t, blocker, pauseLock)

		assertSLOActivation(t, activationResults)
		assertConcurrentSLOWriter(t, writerResults)
		assertSLODownRefused(t, database, downResults)
	})
}

type sloActivationResult struct {
	protocolVersion int
	mode            string
	replayed        bool
	err             error
}

type sloWriterResult struct {
	result httpResult
	err    error
}

func prepareSLOActivationFixture(t *testing.T) (testDatabase, *pgxpool.Pool, uuid.UUID) {
	t.Helper()
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 33)
	seedAdmissionFixture(t, database.Admin)
	seedThreePresetCatalog(t, database)
	createSLOReportingLogin(t, database)
	promotion := newRolePool(
		t, database.DSN, "vela_catalog_promotion_login", "vela-catalog-promotion-password",
	)
	receiptID := recordAndSealLaunchManifest(t, promotion)[productiongates.GatePresetCertification]
	reporting := newRolePool(
		t, database.DSN, "vela_slo_reporting_login", "vela-slo-reporting-password",
	)
	registerCompleteSLOContractMatrix(t, reporting, receiptID)
	return database, reporting, receiptID
}

func insertLegacySLOJobWriter(adminDSN, prompt string) sloWriterResult {
	dsn, err := url.Parse(adminDSN)
	if err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	dsn.User = url.UserPassword("vela_request_login", "vela-request-password")
	pool, err := pgxpool.New(context.Background(), dsn.String())
	if err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	defer pool.Close()

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if _, err := tx.Exec(ctx, `
		SELECT * FROM vela_set_request_context($1, $2, $3)
	`, uuid.MustParse(testCredentialID), credentialDigest([]byte(testCredentialSecret)),
		"jobs:submit"); err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	jobID := uuid.New()
	reservationID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id,
			model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, worker_pool_id,
			request_hash, request_content, request_content_expires_at,
			retention_policy_revision_id, retention_artifact_days,
			retention_request_content_days, retention_incomplete_content_hours,
			retention_scratch_hours, retention_debug_hours,
			retention_metadata_days, retention_financial_days,
			pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity,
			pricing_quoted_amount_minor, pricing_currency,
			execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt,
			execution_retry_backoff_policy,
			execution_retryable_failure_classes,
			execution_circuit_breaker_policy, job_expires_at
		)
		SELECT
			$1, project.organization_id, project.id, $2,
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000012',
			'00000000-0000-0000-0000-000000000013',
			'00000000-0000-0000-0000-000000000005',
			sha256(convert_to($1::uuid::text, 'UTF8')),
			jsonb_build_object('model', 'minimax-h3', 'prompt', $5::text),
			transaction_timestamp() + interval '2 days',
			project.retention_policy_revision_id,
			project.retention_artifact_days,
			project.retention_request_content_days,
			project.retention_incomplete_content_hours,
			project.retention_scratch_hours,
			project.retention_debug_hours,
			project.retention_metadata_days,
			project.retention_financial_days,
			'00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000017',
			1250, 1, 1250, 'CNY', 3, 2400, 600,
			'{"kind":"exponential","initial_seconds":30,"max_seconds":300}'::jsonb,
			ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'],
			'{"policy_revision":"h3-standard-v1"}'::jsonb,
			transaction_timestamp() + interval '1 day'
		FROM projects AS project
		WHERE project.organization_id = $3 AND project.id = $4
	`, jobID, uuid.MustParse(testPrincipalID), uuid.MustParse(testOrganizationID),
		uuid.MustParse(testProjectID), prompt); err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO credit_reservations (
			id, organization_id, project_id, job_id, amount_minor, currency
		) VALUES ($1, $2, $3, $4, 1250, 'CNY')
	`, reservationID, uuid.MustParse(testOrganizationID), uuid.MustParse(testProjectID), jobID); err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
		VALUES ($1, $2, $3)
	`, jobID, uuid.MustParse(testOrganizationID), uuid.MustParse(testProjectID)); err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	if err := tx.Commit(ctx); err != nil {
		return sloWriterResult{result: httpResult{StatusCode: 500}, err: err}
	}
	committed = true
	return sloWriterResult{result: httpResult{StatusCode: 202}}
}

func cleanupSLOTestPause(blocker *sql.Tx, pauseLock int64) {
	var unlocked bool
	_ = blocker.QueryRow("SELECT pg_advisory_unlock($1)", pauseLock).Scan(&unlocked)
	_ = blocker.Rollback()
}

func registerCompleteSLOContractMatrix(
	t *testing.T,
	reporting *pgxpool.Pool,
	receiptID uuid.UUID,
) {
	t.Helper()
	presets := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000011"),
		uuid.MustParse("35000000-0000-0000-0000-000000000011"),
		uuid.MustParse("35000000-0000-0000-0000-000000000021"),
	}
	for _, presetID := range presets {
		for generationCount := 1; generationCount <= 16; generationCount++ {
			if _, err := reporting.Exec(context.Background(), `
				SELECT * FROM vela_register_slo_contract(
					$1, 'h3-standard-slo-v1', $2, $3, $4, $5,
					$6, 60000, 800000, 20, $7
				)
			`, uuid.New(),
				uuid.MustParse("00000000-0000-0000-0000-000000000010"),
				presetID,
				uuid.MustParse("00000000-0000-0000-0000-000000000012"),
				uuid.MustParse("00000000-0000-0000-0000-000000000013"),
				generationCount,
				receiptID,
			); err != nil {
				t.Fatalf(
					"register complete SLO contract preset %s generation_count=%d: %v",
					presetID,
					generationCount,
					err,
				)
			}
		}
	}
}

func enableSLOMeasurementAsync(
	reporting *pgxpool.Pool,
	receiptID uuid.UUID,
) <-chan sloActivationResult {
	results := make(chan sloActivationResult, 1)
	go func() {
		var result sloActivationResult
		result.err = reporting.QueryRow(context.Background(), `
			SELECT protocol_version, mode::text, replayed
			FROM vela_enable_slo_measurement($1)
		`, receiptID).Scan(&result.protocolVersion, &result.mode, &result.replayed)
		results <- result
	}()
	return results
}

func migrateSLODownAsync(t *testing.T, database testDatabase) <-chan error {
	t.Helper()
	results := make(chan error, 1)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	go func() {
		results <- goose.DownTo(database.Admin, migrations, 32)
	}()
	return results
}

func releaseSLOTestPause(t *testing.T, blocker *sql.Tx, pauseLock int64) {
	t.Helper()
	var unlocked bool
	if err := blocker.QueryRow("SELECT pg_advisory_unlock($1)", pauseLock).Scan(&unlocked); err != nil {
		t.Fatalf("release SLO test pause: %v", err)
	}
	if !unlocked {
		t.Fatal("SLO test pause was not held")
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit SLO test pause blocker: %v", err)
	}
}

func assertSLOActivation(t *testing.T, results <-chan sloActivationResult) {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil || result.protocolVersion != 2 || result.mode != "ENFORCED" || result.replayed {
			t.Fatalf(
				"concurrent SLO activation = %d/%s/%t error=%v",
				result.protocolVersion,
				result.mode,
				result.replayed,
				result.err,
			)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent SLO activation did not finish")
	}
}

func assertConcurrentSLOWriter(t *testing.T, results <-chan sloWriterResult) {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil || result.result.StatusCode != 202 {
			t.Fatalf(
				"concurrent SLO Job writer = status %d body=%s error=%v",
				result.result.StatusCode,
				result.result.Body,
				result.err,
			)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent SLO Job writer did not finish")
	}
}

func assertSLODownRefused(t *testing.T, database testDatabase, results <-chan error) {
	t.Helper()
	select {
	case err := <-results:
		if !postgresConstraint(err, "statistical_slo_rollback_is_unsafe") {
			t.Fatalf("concurrent statistical SLO Down error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent statistical SLO Down did not finish")
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 33 {
		t.Fatalf("statistical SLO version after concurrent Down = %d error=%v", version, err)
	}
}

func createSLOReportingLogin(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		CREATE ROLE vela_slo_reporting_login LOGIN
		PASSWORD 'vela-slo-reporting-password' IN ROLE vela_slo_reporting
	`); err != nil {
		t.Fatalf("create statistical SLO reporting login: %v", err)
	}
}

func seedSLOMeasurementObservations(
	t *testing.T,
	database testDatabase,
	contractID uuid.UUID,
) []slo.Observation {
	t.Helper()
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin statistical SLO observations: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	start := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	observations := make([]slo.Observation, 0, 24)
	insertAdmission := func(jobID uuid.UUID, queuedAt, expiresAt time.Time) {
		if _, err := transaction.Exec(`
			INSERT INTO job_slo_admissions (
				job_id, organization_id, project_id, contract_revision_id,
				model_revision_id, generation_preset_revision_id,
				service_class_revision_id, output_spec_id, generation_count,
				queued_at, job_expires_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $10)
		`, jobID,
			uuid.MustParse(testOrganizationID), uuid.MustParse(testProjectID), contractID,
			uuid.MustParse("00000000-0000-0000-0000-000000000010"),
			uuid.MustParse("35000000-0000-0000-0000-000000000011"),
			uuid.MustParse("00000000-0000-0000-0000-000000000012"),
			uuid.MustParse("00000000-0000-0000-0000-000000000013"),
			queuedAt, expiresAt,
		); err != nil {
			t.Fatalf("insert statistical SLO admission %s: %v", jobID, err)
		}
	}
	for index := 0; index < 20; index++ {
		jobID := uuid.New()
		queuedAt := start.Add(time.Duration(index+1) * time.Minute)
		expiresAt := queuedAt.Add(time.Hour)
		completedAt := queuedAt.Add(time.Duration(index+1) * time.Second)
		insertAdmission(jobID, queuedAt, expiresAt)
		if _, err := transaction.Exec(`
			INSERT INTO job_slo_outcomes (
				job_id, outcome, terminal_at, visible_completed_at
			) VALUES ($1, 'SUCCEEDED', $2, $2)
		`, jobID, completedAt); err != nil {
			t.Fatalf("insert successful statistical SLO outcome %d: %v", index, err)
		}
		observations = append(observations, slo.Observation{
			JobID: jobID.String(), ContractRevisionID: contractID.String(),
			AcceptedAt: queuedAt, ExpiresAt: expiresAt, Outcome: slo.OutcomeSucceeded,
			TerminalAt: &completedAt, VisibleCompletedAt: &completedAt,
		})
	}
	for index, outcome := range []string{"FAILED", "CUSTOMER_CANCELED"} {
		jobID := uuid.New()
		queuedAt := start.Add(time.Duration(21+index) * time.Minute)
		expiresAt := queuedAt.Add(time.Hour)
		terminalAt := queuedAt.Add(time.Minute)
		insertAdmission(jobID, queuedAt, expiresAt)
		if _, err := transaction.Exec(`
			INSERT INTO job_slo_outcomes (job_id, outcome, terminal_at)
			VALUES ($1, $2, $3)
		`, jobID, outcome, terminalAt); err != nil {
			t.Fatalf("insert %s statistical SLO outcome: %v", outcome, err)
		}
		observations = append(observations, slo.Observation{
			JobID: jobID.String(), ContractRevisionID: contractID.String(),
			AcceptedAt: queuedAt, ExpiresAt: expiresAt, Outcome: slo.Outcome(outcome),
			TerminalAt: &terminalAt,
		})
	}
	expiredOpenID := uuid.New()
	expiredOpenQueuedAt := start.Add(23 * time.Minute)
	expiredOpenExpiresAt := expiredOpenQueuedAt.Add(time.Minute)
	insertAdmission(expiredOpenID, expiredOpenQueuedAt, expiredOpenExpiresAt)
	observations = append(observations, slo.Observation{
		JobID: expiredOpenID.String(), ContractRevisionID: contractID.String(),
		AcceptedAt: expiredOpenQueuedAt, ExpiresAt: expiredOpenExpiresAt,
		Outcome: slo.OutcomeOpen,
	})

	lateSuccessID := uuid.New()
	lateSuccessQueuedAt := start.Add(24 * time.Minute)
	lateSuccessExpiresAt := lateSuccessQueuedAt.Add(time.Minute)
	lateSuccessCompletedAt := lateSuccessExpiresAt.Add(time.Second)
	insertAdmission(lateSuccessID, lateSuccessQueuedAt, lateSuccessExpiresAt)
	if _, err := transaction.Exec(`
		INSERT INTO job_slo_outcomes (
			job_id, outcome, terminal_at, visible_completed_at
		) VALUES ($1, 'SUCCEEDED', $2, $2)
	`, lateSuccessID, lateSuccessCompletedAt); err != nil {
		t.Fatalf("insert late-success statistical SLO outcome: %v", err)
	}
	observations = append(observations, slo.Observation{
		JobID: lateSuccessID.String(), ContractRevisionID: contractID.String(),
		AcceptedAt: lateSuccessQueuedAt, ExpiresAt: lateSuccessExpiresAt,
		Outcome:    slo.OutcomeSucceeded,
		TerminalAt: &lateSuccessCompletedAt, VisibleCompletedAt: &lateSuccessCompletedAt,
	})
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit statistical SLO observations: %v", err)
	}
	return observations
}

func isPostgresCode(err error, code string) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == code
}

func postgresConstraint(err error, constraint string) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		(postgresError.ConstraintName == constraint || strings.Contains(err.Error(), constraint))
}
