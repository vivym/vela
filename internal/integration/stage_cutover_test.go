//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	veladb "github.com/vivym/vela/internal/database"
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

func TestStageCutoverZeroBacklogSealRejectsNonzeroExternalDrainEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)
	cutoverID := activateProductionStageOnlyCutover(t, database, promotion)

	startInventoryID := captureLegacyAuthorityInventory(t, promotion, "zero-window-start")
	startEvidenceID := recordStageCutoverExternalDrainEvidence(
		t,
		promotion,
		[5]int64{1, 0, 0, 0, 0},
		"external-zero-window-start",
	)
	time.Sleep(1100 * time.Millisecond)
	endInventoryID := captureLegacyAuthorityInventory(t, promotion, "zero-window-end")
	endEvidenceID := recordStageCutoverExternalDrainEvidence(
		t,
		promotion,
		[5]int64{},
		"external-zero-window-end",
	)

	_, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_seal_stage_cutover_zero_backlog(
			$1, $2, $3, $4, $5, $6
		)
	`, uuid.New(), startInventoryID, endInventoryID, startEvidenceID,
		endEvidenceID, "integration-cutover-sealer")
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_external_nonzero")

	var receipts int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM stage_cutover_zero_backlog_receipts
		WHERE cutover_revision_id = $1
	`, cutoverID).Scan(&receipts); err != nil {
		t.Fatalf("count rejected zero-backlog receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("nonzero external drain evidence created %d zero-backlog receipts", receipts)
	}
}

func TestStageCutoverExternalDrainEvidenceRejectsNullBacklogs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)
	activateProductionStageOnlyCutover(t, database, promotion)

	manifestDigest := make([]byte, 32)
	manifestDigest[0] = 1
	for backlogIndex := range 5 {
		_, err := promotion.Exec(context.Background(), `
			SELECT * FROM vela_record_stage_cutover_external_drain_evidence(
				$1,
				CASE WHEN $2 = 0 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 1 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 2 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 3 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 4 THEN NULL ELSE 0 END,
				$3,
				'null-backlog-new-evidence'
			)
		`, uuid.New(), backlogIndex, manifestDigest)
		assertPostgresConstraint(
			t, err, "stage_cutover_external_drain_evidence_invalid",
		)
	}

	evidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "null-backlog-replay",
	)
	for backlogIndex := range 5 {
		_, err := promotion.Exec(context.Background(), `
			SELECT * FROM vela_record_stage_cutover_external_drain_evidence(
				$1,
				CASE WHEN $2 = 0 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 1 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 2 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 3 THEN NULL ELSE 0 END,
				CASE WHEN $2 = 4 THEN NULL ELSE 0 END,
				$3,
				'null-backlog-replay'
			)
		`, evidenceID, backlogIndex, manifestDigest)
		assertPostgresConstraint(
			t, err, "stage_cutover_external_drain_evidence_invalid",
		)
	}
}

func TestStageCutoverZeroBacklogSealBindsEvidenceAndFencesCutover(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)
	cutoverID := activateProductionStageOnlyCutover(t, database, promotion)

	startInventoryID := captureLegacyAuthorityInventory(t, promotion, "seal-window-start")
	startEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "seal-external-start",
	)
	manifestDigest := make([]byte, 32)
	manifestDigest[0] = 1
	var evidenceReplayed bool
	if err := promotion.QueryRow(context.Background(), `
		SELECT replayed
		FROM vela_record_stage_cutover_external_drain_evidence(
			$1, 0, 0, 0, 0, 0, $2, 'seal-external-start'
		)
	`, startEvidenceID, manifestDigest).Scan(&evidenceReplayed); err != nil {
		t.Fatalf("replay external drain evidence: %v", err)
	}
	if !evidenceReplayed {
		t.Fatal("exact external drain evidence replay was not identified")
	}
	_, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_record_stage_cutover_external_drain_evidence(
			$1, 1, 0, 0, 0, 0, $2, 'seal-external-start'
		)
	`, startEvidenceID, manifestDigest)
	assertPostgresConstraint(
		t, err, "stage_cutover_external_drain_evidence_replay_mismatch",
	)

	time.Sleep(1100 * time.Millisecond)
	endInventoryID := captureLegacyAuthorityInventory(t, promotion, "seal-window-end")
	endEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "seal-external-end",
	)
	receiptID := uuid.New()
	type sealResult struct {
		receiptID                      uuid.UUID
		windowStartedAt, windowEndedAt time.Time
		digest                         []byte
		replayed                       bool
		err                            error
	}
	sealResults := make(chan sealResult, 2)
	for range 2 {
		go func() {
			var result sealResult
			result.err = promotion.QueryRow(context.Background(), `
				SELECT receipt_id, window_started_at, window_ended_at,
				       content_digest, replayed
				FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
			`, receiptID, startInventoryID, endInventoryID, startEvidenceID,
				endEvidenceID, "integration-zero-backlog-sealer").Scan(
				&result.receiptID,
				&result.windowStartedAt,
				&result.windowEndedAt,
				&result.digest,
				&result.replayed,
			)
			sealResults <- result
		}()
	}
	var created sealResult
	replayCount := 0
	for range 2 {
		result := <-sealResults
		if result.err != nil {
			t.Fatalf("concurrent seal Stage cutover zero backlog: %v", result.err)
		}
		if result.replayed {
			replayCount++
		} else {
			created = result
		}
	}
	if replayCount != 1 || created.receiptID != receiptID || len(created.digest) != 32 ||
		created.windowEndedAt.Sub(created.windowStartedAt) < time.Second {
		t.Fatalf(
			"concurrent zero-backlog seal replay=%d id=%s digest=%d window=%s",
			replayCount, created.receiptID, len(created.digest),
			created.windowEndedAt.Sub(created.windowStartedAt),
		)
	}

	var boundCutoverID, boundStartInventoryID, boundEndInventoryID uuid.UUID
	var boundStartEvidenceID, boundEndEvidenceID uuid.UUID
	var revisionBindingMatches bool
	if err := database.Admin.QueryRow(`
		SELECT receipt.cutover_revision_id,
		       receipt.start_inventory_snapshot_id,
		       receipt.end_inventory_snapshot_id,
		       receipt.start_external_evidence_id,
		       receipt.end_external_evidence_id,
		       receipt.release_digest = revision.release_digest
		       AND receipt.configuration_revision = revision.configuration_revision
		       AND receipt.configuration_digest = revision.configuration_digest
		       AND receipt.execution_graph_revision_id = revision.execution_graph_revision_id
		       AND receipt.execution_profile_revision_id = revision.execution_profile_revision_id
		       AND receipt.connector_set_digest = revision.connector_set_digest
		       AND receipt.launch_manifest_digest = revision.launch_manifest_digest
		FROM stage_cutover_zero_backlog_receipts AS receipt
		JOIN stage_cutover_revisions AS revision
		  ON revision.id = receipt.cutover_revision_id
		WHERE receipt.id = $1
	`, receiptID).Scan(
		&boundCutoverID,
		&boundStartInventoryID,
		&boundEndInventoryID,
		&boundStartEvidenceID,
		&boundEndEvidenceID,
		&revisionBindingMatches,
	); err != nil {
		t.Fatalf("read zero-backlog receipt bindings: %v", err)
	}
	if boundCutoverID != cutoverID || boundStartInventoryID != startInventoryID ||
		boundEndInventoryID != endInventoryID || boundStartEvidenceID != startEvidenceID ||
		boundEndEvidenceID != endEvidenceID || !revisionBindingMatches {
		t.Fatalf(
			"zero-backlog receipt bindings cutover=%s inventories=%s/%s evidence=%s/%s revision=%t",
			boundCutoverID, boundStartInventoryID, boundEndInventoryID,
			boundStartEvidenceID, boundEndEvidenceID, revisionBindingMatches,
		)
	}

	var replayed bool
	if err := promotion.QueryRow(context.Background(), `
		SELECT replayed
		FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
	`, receiptID, startInventoryID, endInventoryID, startEvidenceID,
		endEvidenceID, "integration-zero-backlog-sealer").Scan(&replayed); err != nil {
		t.Fatalf("replay zero-backlog seal: %v", err)
	}
	if !replayed {
		t.Fatal("exact zero-backlog seal replay was not identified")
	}
	_, err = promotion.Exec(context.Background(), `
		SELECT * FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
	`, receiptID, startInventoryID, endInventoryID, startEvidenceID,
		endEvidenceID, "different-zero-backlog-sealer")
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_replay_mismatch")

	if _, err := promotion.Exec(context.Background(), `
		UPDATE stage_cutover_external_drain_evidence SET observed_by = 'forged'
		WHERE id = $1
	`, startEvidenceID); err == nil {
		t.Fatal("Catalog Promotion directly mutated external drain evidence")
	}
	if _, err := promotion.Exec(context.Background(), `
		UPDATE stage_cutover_zero_backlog_receipts SET sealed_by = 'forged'
		WHERE id = $1
	`, receiptID); err == nil {
		t.Fatal("Catalog Promotion directly mutated zero-backlog receipt")
	}

	server := admissionServerForDatabase(t, database)
	stageJob := submitCutoverJob(t, server.URL, "stage-job-after-zero-backlog-seal")
	if _, err := database.Admin.Exec(`
		CREATE FUNCTION pg_temp.insert_legacy_job_clone(
			p_job_id uuid,
			p_source_job_id uuid
		) RETURNS void
		LANGUAGE plpgsql
		AS $$
		DECLARE
			v_columns text;
			v_job jsonb;
		BEGIN
			SELECT to_jsonb(source) || jsonb_build_object(
				'id', p_job_id,
				'execution_authority_kind', 'LEGACY_WORKER',
				'worker_pool_id', '00000000-0000-0000-0000-000000000005',
				'stage_cutover_revision_id', NULL,
				'execution_graph_revision_id', NULL,
				'stage_execution_profile_revision_id', NULL
			) INTO v_job
			FROM public.jobs AS source
			WHERE source.id = p_source_job_id;
			SELECT string_agg(format('%I', attribute.attname), ', '
			                  ORDER BY attribute.attnum)
			INTO v_columns
			FROM pg_catalog.pg_attribute AS attribute
			WHERE attribute.attrelid = 'public.jobs'::regclass
			  AND attribute.attnum > 0
			  AND NOT attribute.attisdropped
			  AND attribute.attgenerated = '';
			EXECUTE format(
				'INSERT INTO public.jobs (%1$s) '
				|| 'SELECT %1$s FROM jsonb_populate_record(NULL::public.jobs, $1) AS cloned',
				v_columns
			) USING v_job;
		END
		$$
	`); err != nil {
		t.Fatalf("create sealed legacy Job fixture function: %v", err)
	}
	_, err = database.Admin.Exec(`
		SELECT pg_temp.insert_legacy_job_clone($1, $2)
	`, uuid.New(), stageJob.JobID)
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_legacy_authority_sealed")

	_, err = promotion.Exec(context.Background(), `
		SELECT vela_activate_stage_cutover(
			$1, 4, $2, 'INTERNAL', 'LEGACY_ONLY', 0,
			NULL, NULL, 0, 1, decode(repeat('a2', 32), 'hex'),
			'integration-sealed-rollback',
			sha256(convert_to('integration-sealed-rollback', 'UTF8')),
			sha256(convert_to('', 'UTF8')), NULL,
			'integration-catalog-promotion', 'forbidden rollback after seal'
		)
	`, uuid.New(), cutoverID)
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_sealed")

	var revisionCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM stage_cutover_revisions WHERE revision = 4
	`).Scan(&revisionCount); err != nil {
		t.Fatalf("count rejected post-seal cutover revisions: %v", err)
	}
	if revisionCount != 0 {
		t.Fatalf("post-seal mutation retained %d cutover revisions", revisionCount)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 51)
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_rollback_is_unsafe")
}

func TestStageCutoverZeroBacklogSealRejectsDatabaseAndLiveBacklog(t *testing.T) {
	t.Run("database evidence is nonzero", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		seedAdmissionFixture(t, database.Admin)
		seedStageExecutionCatalog(t, database.Admin)
		activateH3StageGraph(t, database)
		activateLegacyRollback(t, database, 3, uuid.MustParse(stageCutoverRevisionID))
		server := admissionServerForDatabase(t, database)
		submitCutoverJob(t, server.URL, "legacy-zero-backlog-evidence")
		promotion := stageCutoverPromotionPool(t, database)
		activateProductionStageOnlyCutover(t, database, promotion)

		startInventoryID, startTotal := captureLegacyAuthorityInventoryTotal(
			t, promotion, "nonzero-database-start",
		)
		endInventoryID, endTotal := captureLegacyAuthorityInventoryTotal(
			t, promotion, "nonzero-database-end",
		)
		if startTotal == 0 || endTotal == 0 {
			t.Fatalf("legacy database evidence totals = %d/%d, want nonzero", startTotal, endTotal)
		}
		startEvidenceID := recordStageCutoverExternalDrainEvidence(
			t, promotion, [5]int64{}, "nonzero-database-external-start",
		)
		endEvidenceID := recordStageCutoverExternalDrainEvidence(
			t, promotion, [5]int64{}, "nonzero-database-external-end",
		)
		_, err := promotion.Exec(context.Background(), `
			SELECT * FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
		`, uuid.New(), startInventoryID, endInventoryID, startEvidenceID,
			endEvidenceID, "integration-database-backlog-sealer")
		assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_inventory_nonzero")
	})

	t.Run("live database inventory changed after evidence", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		seedAdmissionFixture(t, database.Admin)
		seedStageExecutionCatalog(t, database.Admin)
		activateH3StageGraph(t, database)
		promotion := stageCutoverPromotionPool(t, database)
		activateProductionStageOnlyCutover(t, database, promotion)

		startInventoryID := captureLegacyAuthorityInventory(t, promotion, "live-window-start")
		startEvidenceID := recordStageCutoverExternalDrainEvidence(
			t, promotion, [5]int64{}, "live-external-start",
		)
		time.Sleep(1100 * time.Millisecond)
		endInventoryID := captureLegacyAuthorityInventory(t, promotion, "live-window-end")
		endEvidenceID := recordStageCutoverExternalDrainEvidence(
			t, promotion, [5]int64{}, "live-external-end",
		)

		server := admissionServerForDatabase(t, database)
		stageJob := submitCutoverJob(t, server.URL, "live-backlog-after-evidence")
		if _, err := database.Admin.Exec(`
			ALTER TABLE jobs DISABLE TRIGGER jobs_execution_authority_immutable;
			ALTER TABLE jobs DISABLE TRIGGER jobs_snapshot_immutable;
			ALTER TABLE jobs DISABLE TRIGGER jobs_zero_backlog_legacy_authority_guard
		`); err != nil {
			t.Fatalf("disable Job immutable triggers for live backlog fixture: %v", err)
		}
		legacyWriter, err := database.Admin.Begin()
		if err != nil {
			t.Fatalf("begin concurrent legacy backlog write: %v", err)
		}
		if _, err := legacyWriter.Exec(`
			UPDATE jobs
			SET execution_authority_kind = 'LEGACY_WORKER',
			    worker_pool_id = '00000000-0000-0000-0000-000000000005',
			    stage_cutover_revision_id = NULL,
			    execution_graph_revision_id = NULL,
			    stage_execution_profile_revision_id = NULL
			WHERE id = $1
		`, stageJob.JobID); err != nil {
			_ = legacyWriter.Rollback()
			t.Fatalf("seed live legacy backlog after evidence: %v", err)
		}

		sealResult := make(chan error, 1)
		go func() {
			_, sealErr := promotion.Exec(context.Background(), `
				SELECT * FROM vela_seal_stage_cutover_zero_backlog(
					$1, $2, $3, $4, $5, $6
				)
			`, uuid.New(), startInventoryID, endInventoryID, startEvidenceID,
				endEvidenceID, "integration-live-backlog-sealer")
			sealResult <- sealErr
		}()
		select {
		case sealErr := <-sealResult:
			_ = legacyWriter.Rollback()
			t.Fatalf("zero-backlog seal did not wait for concurrent legacy write: %v", sealErr)
		case <-time.After(200 * time.Millisecond):
		}
		if err := legacyWriter.Commit(); err != nil {
			t.Fatalf("commit concurrent legacy backlog write: %v", err)
		}
		if _, err := database.Admin.Exec(`
			ALTER TABLE jobs ENABLE TRIGGER jobs_zero_backlog_legacy_authority_guard;
			ALTER TABLE jobs ENABLE TRIGGER jobs_snapshot_immutable;
			ALTER TABLE jobs ENABLE TRIGGER jobs_execution_authority_immutable
		`); err != nil {
			t.Fatalf("restore Job immutable triggers after live backlog fixture: %v", err)
		}

		assertPostgresConstraint(
			t,
			<-sealResult,
			"stage_cutover_zero_backlog_live_inventory_nonzero",
		)
	})
}

func TestStageCutoverZeroBacklogSealRejectsInvalidObservationWindow(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)
	activateProductionStageOnlyCutover(t, database, promotion)

	startInventoryID := captureLegacyAuthorityInventory(t, promotion, "invalid-window-start")
	startEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "invalid-window-external-start",
	)
	endInventoryID := captureLegacyAuthorityInventory(t, promotion, "invalid-window-end")
	endEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "invalid-window-external-end",
	)
	_, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
	`, uuid.New(), startInventoryID, endInventoryID, startEvidenceID,
		uuid.New(), "integration-missing-evidence-sealer")
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_external_missing")

	_, err = promotion.Exec(context.Background(), `
		SELECT * FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
	`, uuid.New(), startInventoryID, endInventoryID, startEvidenceID,
		endEvidenceID, "integration-short-window-sealer")
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_window_too_short")

	activateNextProductionStageOnlyCutover(t, database, promotion)
	_, err = promotion.Exec(context.Background(), `
		SELECT * FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
	`, uuid.New(), startInventoryID, endInventoryID, startEvidenceID,
		endEvidenceID, "integration-revision-mismatch-sealer")
	assertPostgresConstraint(t, err, "stage_cutover_zero_backlog_revision_mismatch")
}

func TestStageCutoverZeroBacklogMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 51); err != nil {
		t.Fatalf("migrate empty zero-backlog schema down: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "stage_cutover_external_drain_evidence")
	assertTableDoesNotExist(t, database.Admin, "stage_cutover_zero_backlog_receipts")
	if err := goose.UpTo(database.Admin, migrations, 52); err != nil {
		t.Fatalf("migrate zero-backlog schema up again: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 52 {
		t.Fatalf("zero-backlog migration version after Down Up = %d error=%v", version, err)
	}
	promotion := stageCutoverPromotionPool(t, database)
	if err := veladb.VerifyRole(
		context.Background(), promotion, veladb.RoleCatalogPromotion,
	); err != nil {
		t.Fatalf("verify Catalog Promotion role after zero-backlog Down Up: %v", err)
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

func activateProductionStageOnlyCutover(
	t *testing.T,
	database testDatabase,
	promotion *pgxpool.Pool,
) uuid.UUID {
	t.Helper()
	recordAndSealLaunchManifest(t, promotion)
	var manifestDigest, releaseDigest, connectorSetDigest []byte
	var currentRevision int64
	var previousID uuid.UUID
	var configurationRevision string
	if err := database.Admin.QueryRow(`
		SELECT revision.revision, revision.id,
		       manifest.manifest_digest, manifest.release_digest,
		       manifest.configuration_revision,
		       vela_execution_profile_connector_set_digest($1, $2)
		FROM production_gate_manifests AS manifest
		CROSS JOIN stage_cutover_control AS control
		JOIN stage_cutover_revisions AS revision
		  ON revision.id = control.current_revision_id
		WHERE manifest.sealed_at IS NOT NULL
	`, graphExecutionProfileID, stageGraphID).Scan(
		&currentRevision,
		&previousID,
		&manifestDigest,
		&releaseDigest,
		&configurationRevision,
		&connectorSetDigest,
	); err != nil {
		t.Fatalf("read production Stage cutover evidence binding: %v", err)
	}
	cutoverID := uuid.New()
	var activatedID uuid.UUID
	if err := promotion.QueryRow(context.Background(), `
		SELECT (vela_activate_stage_cutover(
			$1, $2, $3, 'PRODUCTION', 'STAGE_ONLY', 10000,
			$4, $5, 2147483648, 1, $6, $7,
			sha256(convert_to($7, 'UTF8')), $8, $9,
			'integration-catalog-promotion',
			'activate production Stage-only drain observation'
		)).id
	`, cutoverID, currentRevision+1, previousID, stageGraphID,
		graphExecutionProfileID, releaseDigest, configurationRevision,
		connectorSetDigest, manifestDigest).Scan(&activatedID); err != nil {
		t.Fatalf("activate production Stage-only cutover: %v", err)
	}
	if activatedID != cutoverID {
		t.Fatalf("production Stage-only cutover id = %s, want %s", activatedID, cutoverID)
	}
	return cutoverID
}

func activateNextProductionStageOnlyCutover(
	t *testing.T,
	database testDatabase,
	promotion *pgxpool.Pool,
) uuid.UUID {
	t.Helper()
	var revision int64
	var previousID uuid.UUID
	var releaseDigest, configurationDigest, connectorSetDigest, manifestDigest []byte
	var configurationRevision string
	if err := database.Admin.QueryRow(`
		SELECT revision.revision, revision.id, revision.release_digest,
		       revision.configuration_revision, revision.configuration_digest,
		       revision.connector_set_digest, revision.launch_manifest_digest
		FROM stage_cutover_control AS control
		JOIN stage_cutover_revisions AS revision
		  ON revision.id = control.current_revision_id
		WHERE control.singleton
	`).Scan(
		&revision,
		&previousID,
		&releaseDigest,
		&configurationRevision,
		&configurationDigest,
		&connectorSetDigest,
		&manifestDigest,
	); err != nil {
		t.Fatalf("read current production Stage-only cutover: %v", err)
	}
	cutoverID := uuid.New()
	var activatedID uuid.UUID
	if err := promotion.QueryRow(context.Background(), `
		SELECT (vela_activate_stage_cutover(
			$1, $2, $3, 'PRODUCTION', 'STAGE_ONLY', 10000,
			$4, $5, 2147483648, 1, $6, $7, $8, $9, $10,
			'integration-catalog-promotion',
			'advance production Stage-only drain observation revision'
		)).id
	`, cutoverID, revision+1, previousID, stageGraphID,
		graphExecutionProfileID, releaseDigest, configurationRevision,
		configurationDigest, connectorSetDigest, manifestDigest).Scan(&activatedID); err != nil {
		t.Fatalf("advance production Stage-only cutover: %v", err)
	}
	if activatedID != cutoverID {
		t.Fatalf("next production Stage-only cutover id = %s, want %s", activatedID, cutoverID)
	}
	return cutoverID
}

func captureLegacyAuthorityInventory(
	t *testing.T,
	promotion *pgxpool.Pool,
	observedBy string,
) uuid.UUID {
	t.Helper()
	snapshotID, total := captureLegacyAuthorityInventoryTotal(t, promotion, observedBy)
	if total != 0 {
		t.Fatalf("%s legacy authority inventory total = %d, want zero", observedBy, total)
	}
	return snapshotID
}

func captureLegacyAuthorityInventoryTotal(
	t *testing.T,
	promotion *pgxpool.Pool,
	observedBy string,
) (uuid.UUID, int64) {
	t.Helper()
	snapshotID := uuid.New()
	var total int64
	if err := promotion.QueryRow(context.Background(), `
		SELECT total_count
		FROM vela_capture_legacy_authority_inventory($1, $2)
	`, snapshotID, observedBy).Scan(&total); err != nil {
		t.Fatalf("capture %s legacy authority inventory: %v", observedBy, err)
	}
	return snapshotID, total
}

func recordStageCutoverExternalDrainEvidence(
	t *testing.T,
	promotion *pgxpool.Pool,
	backlog [5]int64,
	observedBy string,
) uuid.UUID {
	t.Helper()
	evidenceID := uuid.New()
	manifestDigest := make([]byte, 32)
	manifestDigest[0] = byte(backlog[0] + backlog[1] + backlog[2] + backlog[3] + backlog[4] + 1)
	var returnedID uuid.UUID
	if err := promotion.QueryRow(context.Background(), `
		SELECT evidence_id
		FROM vela_record_stage_cutover_external_drain_evidence(
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`, evidenceID, backlog[0], backlog[1], backlog[2], backlog[3], backlog[4],
		manifestDigest, observedBy).Scan(&returnedID); err != nil {
		t.Fatalf("record %s external Stage cutover drain evidence: %v", observedBy, err)
	}
	if returnedID != evidenceID {
		t.Fatalf("external Stage cutover drain evidence id = %s, want %s", returnedID, evidenceID)
	}
	return evidenceID
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
