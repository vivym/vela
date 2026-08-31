//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/identity"
)

func TestStageCutoverRollbackChangesOnlySubsequentJobs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)

	stageJob := submitCutoverJob(t, server.URL, "stage-cutover-before-rollback")
	var stageAuthority string
	var stagePoolID, stageRevisionID, graphID, profileID uuid.NullUUID
	if err := database.Admin.QueryRow(`
		SELECT execution_authority_kind::text, worker_pool_id,
		       stage_cutover_revision_id, execution_graph_revision_id,
		       stage_execution_profile_revision_id
		FROM jobs WHERE id = $1
	`, stageJob.JobID).Scan(
		&stageAuthority, &stagePoolID, &stageRevisionID, &graphID, &profileID,
	); err != nil {
		t.Fatalf("read Stage-routed Job authority: %v", err)
	}
	if stageAuthority != "STAGE_GRAPH" || stagePoolID.Valid ||
		!stageRevisionID.Valid || stageRevisionID.UUID.String() != stageCutoverRevisionID ||
		!graphID.Valid || graphID.UUID.String() != stageGraphID ||
		!profileID.Valid || profileID.UUID.String() != graphExecutionProfileID {
		t.Fatalf(
			"Stage Job authority=%s pool=%v cutover=%v graph=%v profile=%v",
			stageAuthority, stagePoolID, stageRevisionID, graphID, profileID,
		)
	}
	if _, err := database.Admin.Exec(`
		ALTER TABLE jobs DISABLE TRIGGER jobs_execution_authority_immutable
	`); err != nil {
		t.Fatalf("disable Job authority trigger for relational constraint test: %v", err)
	}
	_, mismatchErr := database.Admin.Exec(`
		UPDATE jobs
		SET stage_cutover_revision_id = '00000000-0000-0000-0000-000000000049'
		WHERE id = $1
	`, stageJob.JobID)
	assertPostgresConstraint(t, mismatchErr, "jobs_stage_cutover_authority")
	if _, err := database.Admin.Exec(`
		ALTER TABLE jobs ENABLE TRIGGER jobs_execution_authority_immutable
	`); err != nil {
		t.Fatalf("restore Job authority trigger after relational constraint test: %v", err)
	}
	var stagePoolQueued int
	if err := database.Admin.QueryRow(`
		SELECT queued_count FROM worker_pools WHERE stable_id = 'h3-primary'
	`).Scan(&stagePoolQueued); err != nil {
		t.Fatalf("read legacy WorkerPool counter after Stage Admission: %v", err)
	}
	if stagePoolQueued != 0 {
		t.Fatalf("legacy WorkerPool queued count after Stage Admission = %d, want 0", stagePoolQueued)
	}

	instantiation := readStageGraphInstantiation(t, database.Admin, stageJob.JobID)
	attemptID := instantiation.AttemptID

	activateLegacyRollback(t, database, 3, uuid.MustParse(stageCutoverRevisionID))
	legacyJob := submitCutoverJob(t, server.URL, "stage-cutover-after-rollback")
	var legacyAuthority string
	var legacyPoolID, legacyRevisionID, legacyGraphID, legacyProfileID uuid.NullUUID
	if err := database.Admin.QueryRow(`
		SELECT execution_authority_kind::text, worker_pool_id,
		       stage_cutover_revision_id, execution_graph_revision_id,
		       stage_execution_profile_revision_id
		FROM jobs WHERE id = $1
	`, legacyJob.JobID).Scan(
		&legacyAuthority, &legacyPoolID, &legacyRevisionID, &legacyGraphID, &legacyProfileID,
	); err != nil {
		t.Fatalf("read legacy-routed Job authority: %v", err)
	}
	if legacyAuthority != "LEGACY_WORKER" || !legacyPoolID.Valid ||
		legacyRevisionID.Valid || legacyGraphID.Valid || legacyProfileID.Valid {
		t.Fatalf(
			"legacy Job authority=%s pool=%v cutover=%v graph=%v profile=%v",
			legacyAuthority, legacyPoolID, legacyRevisionID, legacyGraphID, legacyProfileID,
		)
	}

	var frozenJobAuthority, frozenAttemptAuthority string
	if err := database.Admin.QueryRow(`
		SELECT job.execution_authority_kind::text, attempt.execution_authority_kind::text
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		WHERE job.id = $1 AND attempt.id = $2
	`, stageJob.JobID, attemptID).Scan(&frozenJobAuthority, &frozenAttemptAuthority); err != nil {
		t.Fatalf("read in-flight authority after rollback: %v", err)
	}
	if frozenJobAuthority != "STAGE_GRAPH" || frozenAttemptAuthority != "STAGE_GRAPH" {
		t.Fatalf("in-flight authority after rollback = %s/%s", frozenJobAuthority, frozenAttemptAuthority)
	}

	_, err := database.Admin.Exec(`
		UPDATE jobs
		SET execution_authority_kind = 'LEGACY_WORKER',
		    worker_pool_id = $2,
		    stage_cutover_revision_id = NULL,
		    execution_graph_revision_id = NULL,
		    stage_execution_profile_revision_id = NULL
		WHERE id = $1
	`, stageJob.JobID, legacyPoolID.UUID)
	assertPostgresConstraint(t, err, "job_execution_authority_immutable")
	_, err = database.Admin.Exec(`
		UPDATE attempts SET execution_authority_kind = 'LEGACY_WORKER' WHERE id = $1
	`, attemptID)
	assertPostgresConstraint(t, err, "attempt_coordinator_writer_required")
	_, err = database.Admin.Exec(`
		INSERT INTO execution_graph_snapshots (
			id, organization_id, project_id, job_id,
			execution_graph_revision_id, execution_profile_revision_id,
			graph_content_digest, topological_order, snapshot_contract
		)
		SELECT gen_random_uuid(), job.organization_id, job.project_id, job.id,
		       graph.id, $2, graph.content_digest, graph.topological_order,
		       '{}'::jsonb
		FROM jobs AS job
		JOIN execution_graph_revisions AS graph ON graph.id = $3
		WHERE job.id = $1
	`, legacyJob.JobID, profileID.UUID, graphID.UUID)
	assertPostgresConstraint(t, err, "graph_snapshot_job_execution_authority_mismatch")
}

func TestStageCutoverInventoryIsFunctionOwnedAndDurable(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)

	snapshotID := uuid.New()
	var total, retainedPublishedOutbox int64
	if err := promotion.QueryRow(context.Background(), `
		SELECT total_count, retained_published_outbox_events
		FROM vela_capture_legacy_authority_inventory($1, $2)
	`, snapshotID, "integration-cutover-observer").Scan(
		&total, &retainedPublishedOutbox,
	); err != nil {
		t.Fatalf("capture legacy database inventory: %v", err)
	}
	if total != 0 || retainedPublishedOutbox != 0 {
		t.Fatalf(
			"legacy database inventory total/retained-outbox = %d/%d, want zero",
			total, retainedPublishedOutbox,
		)
	}

	_, err := promotion.Exec(context.Background(), `
		INSERT INTO legacy_authority_inventory_snapshots (
			id, cutover_revision_id, observed_by,
			nonterminal_jobs, nonterminal_attempts,
			active_execution_leases, active_finalization_leases,
			active_artifact_uploads, unpublished_outbox_events,
			retained_published_outbox_events, scheduler_inbox_backlog,
			retry_recovery_backlog, total_count, content_digest
		) VALUES (
			$1, $2, 'forged-observer', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			decode(repeat('b1', 32), 'hex')
		)
	`, uuid.New(), stageCutoverRevisionID)
	if err == nil {
		t.Fatal("catalog promotion role directly forged a legacy inventory snapshot")
	}
	_, err = promotion.Exec(context.Background(), `
		UPDATE legacy_authority_inventory_snapshots
		SET observed_by = 'mutated-observer'
		WHERE id = $1
	`, snapshotID)
	if err == nil {
		t.Fatal("catalog promotion role mutated immutable legacy inventory")
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 48)
	assertPostgresConstraint(t, err, "stage_cutover_control_rollback_is_unsafe")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 49 {
		t.Fatalf("Stage cutover version after refused Down = %d error=%v", version, versionErr)
	}
}

func TestStageCutoverActivationAndModelScopeFailClosed(t *testing.T) {
	const h3ModelRevisionID = "00000000-0000-0000-0000-000000000010"

	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)

	var connectorSetDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT vela_execution_profile_connector_set_digest($1, $2)
	`, graphExecutionProfileID, stageGraphID).Scan(&connectorSetDigest); err != nil {
		t.Fatalf("read cutover connector-set digest: %v", err)
	}
	activate := func(
		revision int64,
		previousID string,
		scope string,
		mode string,
		basisPoints int,
		connectorDigest []byte,
	) error {
		_, err := promotion.Exec(context.Background(), `
			SELECT vela_activate_stage_cutover(
				$1, $2, $3, $4::stage_cutover_scope, $5::stage_cutover_mode, $6,
				$7, $8, 2147483648, 1, decode(repeat('a1', 32), 'hex'),
				'integration-stage-cutover-validation',
				sha256(convert_to('integration-stage-cutover-validation', 'UTF8')),
				$9, NULL, 'integration-catalog-promotion',
				'validate cutover activation controls'
			)
		`, uuid.New(), revision, previousID, scope, mode, basisPoints,
			stageGraphID, graphExecutionProfileID, connectorDigest)
		return err
	}

	err := activate(4, stageCutoverRevisionID, "INTERNAL", "STAGE_ONLY", 10000, connectorSetDigest)
	assertPostgresConstraint(t, err, "stage_cutover_revision_stale")
	err = activate(3, stageCutoverRevisionID, "INTERNAL", "STAGE_ONLY", 10000, make([]byte, 32))
	assertPostgresConstraint(t, err, "stage_cutover_connector_set_mismatch")
	err = activate(3, stageCutoverRevisionID, "PRODUCTION", "STAGE_ONLY", 10000, connectorSetDigest)
	assertPostgresConstraint(t, err, "stage_cutover_launch_manifest_required")
	err = activate(3, stageCutoverRevisionID, "PRODUCTION", "COHORT", 5000, connectorSetDigest)
	assertPostgresConstraint(t, err, "stage_cutover_launch_manifest_required")

	cohortID := uuid.New()
	if _, err := promotion.Exec(context.Background(), `
		SELECT vela_activate_stage_cutover(
			$1, 3, $2, 'INTERNAL', 'COHORT', 5000, $3, $4,
			2147483648, 1, decode(repeat('a1', 32), 'hex'),
			'integration-stage-cohort',
			sha256(convert_to('integration-stage-cohort', 'UTF8')),
			$5, NULL, 'integration-catalog-promotion',
			'validate deterministic cohort routing'
		)
	`, cohortID, stageCutoverRevisionID, stageGraphID,
		graphExecutionProfileID, connectorSetDigest); err != nil {
		t.Fatalf("activate Stage cohort: %v", err)
	}

	otherModelID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO model_revisions (id, stable_id, revision, state, content_hash)
		VALUES ($1, 'non-h3-cutover-control', 1, 'ACTIVE', decode(repeat('c1', 32), 'hex'))
	`, otherModelID); err != nil {
		t.Fatalf("seed non-H3 ModelRevision: %v", err)
	}
	requestPool := newRolePool(
		t, database.DSN, "vela_request_login", "vela-request-password",
	)
	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin cutover route transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(
		context.Background(), "SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID, credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish cutover request context: %v", err)
	}

	readRoute := func(modelID string) (string, uuid.NullUUID, uuid.NullUUID) {
		t.Helper()
		var authority string
		var graphID, profileID uuid.NullUUID
		if err := tx.QueryRow(context.Background(), `
			SELECT execution_authority_kind::text,
			       execution_graph_revision_id, execution_profile_revision_id
			FROM vela_resolve_job_execution_route($1, $2, $3)
		`, testOrganizationID, testProjectID, modelID).Scan(
			&authority, &graphID, &profileID,
		); err != nil {
			t.Fatalf("resolve cutover route for model %s: %v", modelID, err)
		}
		return authority, graphID, profileID
	}
	otherAuthority, otherGraphID, otherProfileID := readRoute(otherModelID.String())
	if otherAuthority != "LEGACY_WORKER" || otherGraphID.Valid || otherProfileID.Valid {
		t.Fatalf(
			"non-H3 route = %s graph=%v profile=%v, want legacy without Stage authority",
			otherAuthority, otherGraphID, otherProfileID,
		)
	}
	unboundAuthority, unboundGraphID, unboundProfileID := readRoute(h3ModelRevisionID)
	if unboundAuthority != "LEGACY_WORKER" || unboundGraphID.Valid || unboundProfileID.Valid {
		t.Fatalf(
			"unbound INTERNAL route = %s graph=%v profile=%v, want legacy",
			unboundAuthority, unboundGraphID, unboundProfileID,
		)
	}
	authorizeInternalCutoverProject(t, promotion, cohortID)
	firstAuthority, firstGraphID, firstProfileID := readRoute(h3ModelRevisionID)
	secondAuthority, secondGraphID, secondProfileID := readRoute(h3ModelRevisionID)
	if firstAuthority != secondAuthority || firstGraphID != secondGraphID ||
		firstProfileID != secondProfileID {
		t.Fatalf(
			"cohort route changed across reads: %s/%v/%v then %s/%v/%v",
			firstAuthority, firstGraphID, firstProfileID,
			secondAuthority, secondGraphID, secondProfileID,
		)
	}
	if firstAuthority == "STAGE_GRAPH" &&
		(!firstGraphID.Valid || firstGraphID.UUID.String() != stageGraphID ||
			!firstProfileID.Valid || firstProfileID.UUID.String() != graphExecutionProfileID) {
		t.Fatalf("Stage cohort route has wrong graph/profile: %v/%v", firstGraphID, firstProfileID)
	}
	if firstAuthority == "LEGACY_WORKER" && (firstGraphID.Valid || firstProfileID.Valid) {
		t.Fatalf("legacy cohort route leaked Stage authority: %v/%v", firstGraphID, firstProfileID)
	}
}

func TestStageCutoverMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 48); err != nil {
		t.Fatalf("migrate empty Stage cutover control down: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "stage_cutover_revisions")
	if err := goose.UpTo(database.Admin, migrations, 49); err != nil {
		t.Fatalf("migrate Stage cutover control up again: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 49 {
		t.Fatalf("Stage cutover version after Down Up = %d error=%v", version, err)
	}
}

func TestStageCutoverMigrationRejectsPreCutoverStageAttempts(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 48); err != nil {
		t.Fatalf("migrate to pre-cutover schema: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3ExecutionGraph(t, database)

	jobID := uuid.New()
	reservationID := uuid.New()
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin pre-cutover Stage Attempt fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id,
			model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, worker_pool_id,
			request_hash, request_content, request_content_expires_at,
			pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity,
			pricing_quoted_amount_minor, pricing_currency,
			execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt,
			execution_retry_backoff_policy,
			execution_retryable_failure_classes,
			execution_circuit_breaker_policy, job_expires_at
		) VALUES (
			$1::uuid, $2, $3, $4,
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000012',
			'00000000-0000-0000-0000-000000000013',
			'00000000-0000-0000-0000-000000000005',
			sha256(convert_to(($1::uuid)::text, 'UTF8')),
			'{"model":"minimax-h3","prompt":"pre-cutover-stage"}'::jsonb,
			clock_timestamp() + interval '2 days',
			'00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000017',
			1250, 1, 1250, 'CNY', 3, 2400, 600,
			'{"kind":"exponential","initial_seconds":30,"max_seconds":300}'::jsonb,
			ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'],
			'{"policy_revision":"h3-standard-v1"}'::jsonb,
			clock_timestamp() + interval '1 day'
		)
	`, jobID, testOrganizationID, testProjectID, testPrincipalID); err != nil {
		t.Fatalf("insert pre-cutover Job: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO credit_reservations (
			id, organization_id, project_id, job_id, amount_minor, currency
		) VALUES ($1, $2, $3, $4, 1250, 'CNY')
	`, reservationID, testOrganizationID, testProjectID, jobID); err != nil {
		t.Fatalf("insert pre-cutover CreditReservation: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
		VALUES ($1, $2, $3)
	`, jobID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("insert pre-cutover RetryRuntimeState: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE projects SET queued_count = queued_count + 1 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("seed pre-cutover Project queue counter: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE worker_pools SET queued_count = queued_count + 1
		WHERE id = '00000000-0000-0000-0000-000000000005'
	`); err != nil {
		t.Fatalf("seed pre-cutover WorkerPool queue counter: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit pre-cutover Job: %v", err)
	}

	coordinator, err := attemptcoordinator.NewService(newRolePool(
		t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct pre-cutover AttemptCoordinator: %v", err)
	}
	if _, err := coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID: uuid.New(), JobID: jobID,
		ExpectedJobVersion: 1, ExpectedJobFence: 0,
		ExecutionGraphSnapshotID:   uuid.New(),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  uuid.New(), StorageReservationID: uuid.New(),
		ReservedStorageBytes: 2 << 30,
	}); err != nil {
		t.Fatalf("instantiate pre-cutover Stage Attempt: %v", err)
	}

	err = goose.UpTo(database.Admin, migrations, 49)
	assertPostgresConstraint(t, err, "stage_cutover_upgrade_requires_no_stage_attempts")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 48 {
		t.Fatalf("migration version after refused upgrade = %d error=%v", version, versionErr)
	}
}

func submitCutoverJob(t *testing.T, serverURL, idempotencyKey string) jobResponse {
	t.Helper()
	accepted := submitJob(t, serverURL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"stage cutover authority"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit cutover Job status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode cutover Job: %v", err)
	}
	return job
}

func activateLegacyRollback(
	t *testing.T,
	database testDatabase,
	revision int64,
	previousRevisionID uuid.UUID,
) {
	t.Helper()
	promotion := stageCutoverPromotionPool(t, database)
	if _, err := promotion.Exec(context.Background(), `
		SELECT vela_activate_stage_cutover(
			$1, $2, $3, 'INTERNAL', 'LEGACY_ONLY', 0,
			NULL, NULL, 0, 1, decode(repeat('a2', 32), 'hex'),
			'integration-stage-rollback',
			sha256(convert_to('integration-stage-rollback', 'UTF8')),
			sha256(convert_to('', 'UTF8')), NULL,
			'integration-catalog-promotion', 'route only subsequent Jobs to legacy'
		)
	`, uuid.New(), revision, previousRevisionID); err != nil {
		t.Fatalf("activate legacy routing rollback: %v", err)
	}
}

func stageCutoverPromotionPool(t *testing.T, database testDatabase) *pgxpool.Pool {
	t.Helper()
	return newRolePool(
		t, database.DSN,
		"vela_catalog_promotion_login", "vela-catalog-promotion-password",
	)
}

func authorizeInternalCutoverProject(
	t *testing.T,
	promotion *pgxpool.Pool,
	cutoverID uuid.UUID,
) {
	t.Helper()
	if _, err := promotion.Exec(context.Background(), `
		SELECT vela_authorize_stage_cutover_internal_project($1, $2, $3, $4)
	`, cutoverID, testOrganizationID, testProjectID,
		"integration-catalog-promotion"); err != nil {
		t.Fatalf("authorize integration project for INTERNAL Stage cutover: %v", err)
	}
}
