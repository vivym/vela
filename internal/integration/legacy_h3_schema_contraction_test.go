//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
)

func TestLegacyH3SchemaContractionRejectsNonEmptyUpgradeWithoutAuthorization(t *testing.T) {
	var jobID uuid.UUID
	fixture := newLegacyH3ReleaseGateFixtureWithPreCutoverSetup(
		t,
		func(database testDatabase) {
			jobID = insertAdmittedStageJobBeforeLegacyH3Contraction(
				t, database, "schema-contraction-without-authorization",
			)
		},
	)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	err := goose.UpTo(fixture.database.Admin, migrations, 58)
	assertPostgresConstraint(t, err, "legacy_h3_schema_contraction_authorization_required")

	version, versionErr := goose.GetDBVersion(fixture.database.Admin)
	if versionErr != nil || version != 57 {
		t.Fatalf("schema version after refused non-empty contraction = %d error=%v, want 57", version, versionErr)
	}
	assertStageJobAndAttemptSurviveLegacyH3Contraction(t, fixture.database, jobID)
}

func TestLegacyH3SchemaContractionRejectsRetainedHistoryWithoutJobs(t *testing.T) {
	fixture := newLegacyH3ReleaseGateFixture(t)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	err := goose.UpTo(fixture.database.Admin, migrations, 58)
	assertPostgresConstraint(t, err, "legacy_h3_schema_contraction_authorization_required")

	version, versionErr := goose.GetDBVersion(fixture.database.Admin)
	if versionErr != nil || version != 57 {
		t.Fatalf(
			"schema version after refused retained-history contraction = %d error=%v, want 57",
			version, versionErr,
		)
	}
}

func TestLegacyH3SchemaContractionPreservesStageJobAfterAuthorizedUpgrade(t *testing.T) {
	var jobID uuid.UUID
	fixture := newLegacyH3ReleaseGateFixtureWithPreCutoverSetup(
		t,
		func(database testDatabase) {
			jobID = insertAdmittedStageJobBeforeLegacyH3Contraction(
				t, database, "schema-contraction-with-authorization",
			)
		},
	)
	if _, err := authorizeLegacyH3Contraction(
		fixture,
		fixture.launchManifestDigest,
		fixture.releaseDigest,
		fixture.configurationRevision,
		fixture.configurationManifest,
		fixture.reachabilityEvidence,
		"integration-schema-contraction",
	); err != nil {
		t.Fatalf("authorize non-empty Legacy H3 schema contraction: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.UpTo(fixture.database.Admin, migrations, 58); err != nil {
		t.Fatalf("contract authorized non-empty Legacy H3 schema: %v", err)
	}
	version, err := goose.GetDBVersion(fixture.database.Admin)
	if err != nil || version != 58 {
		t.Fatalf("authorized contraction schema version = %d error=%v, want 58", version, err)
	}
	assertStageJobAndAttemptSurviveLegacyH3Contraction(t, fixture.database, jobID)
	var legacyProfileState, legacyCertificationState string
	var legacyProfileGraphBound bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT profile.state::text, certification.state::text,
		       profile.execution_graph_revision_id IS NOT NULL
		FROM execution_profile_revisions AS profile
		JOIN profile_certifications AS certification
		  ON certification.execution_profile_revision_id = profile.id
		WHERE profile.id = '00000000-0000-0000-0000-000000000014'
	`).Scan(
		&legacyProfileState,
		&legacyCertificationState,
		&legacyProfileGraphBound,
	); err != nil {
		t.Fatalf("inspect contracted Legacy H3 Catalog history: %v", err)
	}
	if legacyProfileState != "RETIRED" || legacyCertificationState != "RETIRED" ||
		legacyProfileGraphBound {
		t.Fatalf(
			"contracted Legacy H3 Catalog history = profile %s certification %s graph_bound %t",
			legacyProfileState, legacyCertificationState, legacyProfileGraphBound,
		)
	}
	_, err = fixture.database.Admin.Exec(`
		UPDATE profile_certifications
		SET state = 'ACTIVE'
		WHERE id = '00000000-0000-0000-0000-000000000015'
	`)
	assertPostgresConstraint(t, err, "profile_certifications_stage_or_inert_fk")

	if err := goose.UpTo(fixture.database.Admin, migrations, 65); err != nil {
		t.Fatalf("upgrade contracted Stage-only schema to current version: %v", err)
	}
	version, err = goose.GetDBVersion(fixture.database.Admin)
	if err != nil || version != 65 {
		t.Fatalf("current Stage-only schema version = %d error=%v, want 65", version, err)
	}
	assertStageJobAndAttemptSurviveLegacyH3Contraction(t, fixture.database, jobID)
}

func TestLegacyH3SchemaContractionPreservesInvalidatedLegacyCertification(t *testing.T) {
	fixture := newLegacyH3ReleaseGateFixtureWithPreCutoverSetup(
		t,
		func(database testDatabase) {
			if _, err := database.Admin.Exec(`
				UPDATE profile_certifications
				SET state = 'INVALID', invalidated_at = clock_timestamp()
				WHERE id = '00000000-0000-0000-0000-000000000015'
			`); err != nil {
				t.Fatalf("invalidate pre-contraction Legacy certification: %v", err)
			}
		},
	)
	if _, err := authorizeLegacyH3Contraction(
		fixture,
		fixture.launchManifestDigest,
		fixture.releaseDigest,
		fixture.configurationRevision,
		fixture.configurationManifest,
		fixture.reachabilityEvidence,
		"integration-schema-contraction-invalidated-certification",
	); err != nil {
		t.Fatalf("authorize contraction with invalidated Legacy certification: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.UpTo(fixture.database.Admin, migrations, 58); err != nil {
		t.Fatalf("contract schema with invalidated Legacy certification: %v", err)
	}
	var profileState, certificationState string
	var invalidated bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT profile.state::text, certification.state::text,
		       certification.invalidated_at IS NOT NULL
		FROM execution_profile_revisions AS profile
		JOIN profile_certifications AS certification
		  ON certification.execution_profile_revision_id = profile.id
		WHERE profile.id = '00000000-0000-0000-0000-000000000014'
	`).Scan(&profileState, &certificationState, &invalidated); err != nil {
		t.Fatalf("inspect invalidated Legacy Catalog history: %v", err)
	}
	if profileState != "RETIRED" || certificationState != "INVALID" || !invalidated {
		t.Fatalf(
			"invalidated Legacy Catalog history = profile %s certification %s invalidated %t",
			profileState, certificationState, invalidated,
		)
	}
}

func TestLegacyH3SchemaContractionFreshInstallNeedsNoProductionAuthorization(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 65 {
		t.Fatalf("fresh Stage-only schema version = %d error=%v, want 65", version, err)
	}

	for _, table := range []string{
		"attempt_leases",
		"scheduler_dispatch_intents",
		"scheduler_pool_counters",
		"worker_profile_readiness",
		"worker_epochs",
		"workers",
	} {
		assertTableDoesNotExist(t, database.Admin, table)
	}

	var contracted bool
	if err := database.Admin.QueryRow(`
		SELECT to_regtype('public.execution_authority_kind') IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'attempts'
				  AND column_name IN (
					'execution_authority_kind', 'execution_profile_revision_id',
					'worker_pool_id', 'worker_id', 'worker_epoch', 'assigned_at',
					'profile_certification_id', 'scheduler_dispatch_intent_id'
				  )
			)
			AND NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'jobs'
				  AND column_name IN ('execution_authority_kind', 'worker_pool_id')
			)
	`).Scan(&contracted); err != nil {
		t.Fatalf("inspect fresh Stage-only schema contraction: %v", err)
	}
	if !contracted {
		t.Fatal("fresh install retained Legacy H3 authority columns or discriminator")
	}

	var legacyFunctionCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM (
			VALUES
				('public.vela_cancel_legacy_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'),
				('public.execution_authority_kind(public.jobs)'),
				('public.worker_pool_id(public.jobs)'),
				('public.execution_authority_kind(public.attempts)'),
				('public.execution_profile_revision_id(public.attempts)'),
				('public.worker_pool_id(public.attempts)'),
				('public.worker_id(public.attempts)'),
				('public.worker_epoch(public.attempts)')
		) AS legacy(signature)
		WHERE to_regprocedure(legacy.signature) IS NOT NULL
	`).Scan(&legacyFunctionCount); err != nil {
		t.Fatalf("inspect Legacy H3 compatibility functions: %v", err)
	}
	if legacyFunctionCount != 0 {
		t.Fatalf("fresh install retained %d Legacy H3 compatibility functions", legacyFunctionCount)
	}

	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM pg_proc AS function
		JOIN pg_namespace AS namespace ON namespace.oid = function.pronamespace
		WHERE namespace.nspname IN ('public', 'vela_private')
		  AND function.prokind = 'f'
		  AND pg_get_functiondef(function.oid) ~* (
			'execution_authority_kind|worker_pool_id|worker_pools|attempt_leases|'
			|| 'scheduler_dispatch_intent|'
			|| '(^|[^a-z_])(attempt|v_attempt)\\.'
			|| '(execution_profile_revision_id|worker_id|worker_epoch|'
			|| 'profile_certification_id|scheduler_dispatch_intent_id)|'
			|| '(^|[^a-z_])(job|v_job)\\.(execution_authority_kind|worker_pool_id)|'
			|| '(^|[^a-z_])(decision|v_decision)\\.execution_authority_kind'
		  )
	`).Scan(&legacyFunctionCount); err != nil {
		t.Fatalf("scan retained function definitions for Legacy H3 authority: %v", err)
	}
	if legacyFunctionCount != 0 {
		t.Fatalf("fresh install retained %d functions with Legacy H3 authority dependencies", legacyFunctionCount)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 57)
	assertPostgresConstraint(t, err, "legacy_h3_schema_contraction_is_irreversible")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 58 {
		t.Fatalf("schema version after refused contraction Down = %d error=%v", version, versionErr)
	}
}

func insertAdmittedStageJobBeforeLegacyH3Contraction(
	t *testing.T,
	database testDatabase,
	fixtureKey string,
) uuid.UUID {
	t.Helper()
	jobID := uuid.New()
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin pre-contraction Stage Admission fixture: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec(`
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:submit')
	`, testCredentialID, credentialDigest([]byte(testCredentialSecret))); err != nil {
		t.Fatalf("set pre-contraction Stage Admission context: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id,
			model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id,
			execution_authority_kind, stage_cutover_revision_id,
			execution_graph_revision_id, stage_execution_profile_revision_id,
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
			execution_circuit_breaker_policy,
			execution_circuit_fingerprint_window_seconds,
			execution_circuit_min_distinct_healthy_workers,
			job_expires_at
		)
		SELECT
			$1, project.organization_id, project.id, $2,
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011', service.id,
			'00000000-0000-0000-0000-000000000013',
			'STAGE_GRAPH', $3, $4, $5,
			sha256(convert_to($1::uuid::text, 'UTF8')),
			jsonb_build_object('model', 'minimax-h3', 'prompt', $6::text),
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
			1250, 1, 1250, 'CNY', service.max_attempts, 2400,
			service.max_finalization_seconds_per_attempt,
			service.retry_backoff_policy, service.retryable_failure_classes,
			service.circuit_breaker_policy,
			service.circuit_fingerprint_window_seconds,
			service.circuit_min_distinct_healthy_workers,
			transaction_timestamp() + interval '1 day'
		FROM projects AS project
		JOIN service_class_revisions AS service
		  ON service.id = '00000000-0000-0000-0000-000000000012'
		WHERE project.organization_id = $7 AND project.id = $8
	`, jobID, testPrincipalID, stageCutoverRevisionID, stageGraphID,
		graphExecutionProfileID, fixtureKey, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("insert pre-contraction Stage Job: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
		VALUES ($1, $2, $3)
	`, jobID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("insert pre-contraction RetryRuntimeState: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO credit_reservations (
			id, organization_id, project_id, job_id, amount_minor, currency
		) VALUES ($1, $2, $3, $4, 1250, 'CNY')
	`, uuid.New(), testOrganizationID, testProjectID, jobID); err != nil {
		t.Fatalf("insert pre-contraction CreditReservation: %v", err)
	}
	if _, err := transaction.Exec(`
		UPDATE projects SET queued_count = queued_count + 1 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("increment pre-contraction Project queue: %v", err)
	}
	var attemptID uuid.UUID
	if err := transaction.QueryRow(`
		SELECT attempt_id
		FROM vela_instantiate_admitted_stage_graph($1, $2, $3)
	`, testOrganizationID, testProjectID, jobID).Scan(&attemptID); err != nil {
		t.Fatalf("instantiate pre-contraction Stage Job: %v", err)
	}
	if attemptID == uuid.Nil {
		t.Fatal("pre-contraction Stage Admission returned an empty Attempt identity")
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit pre-contraction Stage Admission fixture: %v", err)
	}
	return jobID
}

func assertStageJobAndAttemptSurviveLegacyH3Contraction(
	t *testing.T,
	database testDatabase,
	jobID uuid.UUID,
) {
	t.Helper()
	var jobCount, attemptCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM jobs WHERE id = $1),
			(SELECT count(*) FROM attempts
			 WHERE job_id = $1 AND execution_graph_snapshot_id IS NOT NULL)
	`, jobID).Scan(&jobCount, &attemptCount); err != nil {
		t.Fatalf("inspect Stage Job after Legacy H3 schema contraction: %v", err)
	}
	if jobCount != 1 || attemptCount != 1 {
		t.Fatalf(
			"Stage authority after Legacy H3 schema contraction = jobs %d attempts %d, want 1/1",
			jobCount, attemptCount,
		)
	}
}
