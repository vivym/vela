//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/recovery"
	"github.com/vivym/vela/internal/stagefinalization"
)

func TestStageSuccessfulStorageReservationPermitsQuiescenceAndPreservesEvidence(t *testing.T) {
	outcome := runCPUMediaH3Graph(t)
	before := readStageStorageCompletionEvidence(t, outcome)
	if before.state != "RESERVED" || before.consumedBytes == 0 {
		t.Fatalf("physical graph did not consume an active storage budget: %+v", before)
	}
	completed, replay := completeStorageReservationGraph(t, outcome)
	if completed.CompletionID != replay.CompletionID || completed.ChargeID != replay.ChargeID {
		t.Fatalf("successful storage finalization changed its replay: %+v / %+v", completed, replay)
	}
	assertConsumedStageStorageEvidence(t, outcome, before)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	retentionPool := newRolePool(t, outcome.database.DSN, "vela_retention_login", "vela-retention-password")
	if _, err := retentionPool.Exec(ctx, `SELECT vela_prepare_stage_artifact_lifecycle(100)`); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.Quiesce(ctx, recoveryConnection(t, outcome.database), uuid.New(), time.Millisecond); err != nil {
		t.Fatalf("successful graph did not become quiescent: %v", err)
	}
	assertConsumedStageStorageEvidence(t, outcome, before)
	for _, artifact := range completed.Artifacts {
		reader, err := outcome.objectStore.ReadExactVersion(ctx, artifact.ObjectKey, artifact.ObjectVersionID)
		if err != nil {
			t.Fatalf("quiescence removed retained public output: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStageSuccessfulStorageReservationMigrationBackfillsWithoutReactivation(t *testing.T) {
	var queuedAttempt uuid.UUID
	outcome := runCPUMediaH3GraphAtSchema(t, "storage-reservation-backfill", 81, func(database testDatabase, serverURL string) {
		_, queuedAttempt = instantiateH3IntegrationGraph(t, database, serverURL, "storage-backfill-live-job")
	})
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	var oldDefinition, functionIdentity string
	if err := outcome.database.Admin.QueryRow(`SELECT pg_get_functiondef(oid),
		oid::text || ':' || proowner::text || ':' || proacl::text FROM pg_proc
		WHERE oid = 'vela_complete_stage_graph_visible_completion_attempt(uuid,bigint,timestamptz)'::regprocedure`).
		Scan(&oldDefinition, &functionIdentity); err != nil {
		t.Fatal(err)
	}
	// The current Go finalizer reads the additive schema-99 media contract column.
	// Supply its legacy default without changing the schema-81 storage completion
	// function under test or applying the schema-82 reservation repair early.
	if _, err := outcome.database.Admin.Exec(`ALTER TABLE output_specs
		ADD COLUMN media_contract text NOT NULL DEFAULT 'exact-video-v1'`); err != nil {
		t.Fatal(err)
	}
	before := readStageStorageCompletionEvidence(t, outcome)
	completeStorageReservationGraph(t, outcome)
	if got := readStageStorageCompletionEvidence(t, outcome); got.state != "RESERVED" {
		t.Fatalf("schema 81 did not reproduce the successful reservation leak: %+v", got)
	}
	for cycle := 0; cycle < 2; cycle++ {
		if err := goose.UpTo(outcome.database.Admin, migrations, 82); err != nil {
			t.Fatal(err)
		}
		assertConsumedStageStorageEvidence(t, outcome, before)
		var queuedState, currentIdentity string
		if err := outcome.database.Admin.QueryRow(`SELECT state::text FROM stage_storage_reservations WHERE attempt_id = $1`,
			queuedAttempt).Scan(&queuedState); err != nil || queuedState != "RESERVED" {
			t.Fatalf("history backfill consumed an unfinished graph budget: %s %v", queuedState, err)
		}
		if err := outcome.database.Admin.QueryRow(`SELECT oid::text || ':' || proowner::text || ':' || proacl::text
			FROM pg_proc WHERE oid = 'vela_complete_stage_graph_visible_completion_attempt(uuid,bigint,timestamptz)'::regprocedure`).
			Scan(&currentIdentity); err != nil || currentIdentity != functionIdentity {
			t.Fatalf("storage migration changed function identity/owner/ACL: %s %v", currentIdentity, err)
		}
		if err := goose.DownTo(outcome.database.Admin, migrations, 81); err != nil {
			t.Fatal(err)
		}
		assertConsumedStageStorageEvidence(t, outcome, before)
		var restoredDefinition string
		if err := outcome.database.Admin.QueryRow(`SELECT pg_get_functiondef(
			'vela_complete_stage_graph_visible_completion_attempt(uuid,bigint,timestamptz)'::regprocedure)`).
			Scan(&restoredDefinition); err != nil || restoredDefinition != oldDefinition {
			t.Fatalf("storage migration did not restore exact schema 81 function: %v", err)
		}
	}
}

func TestStageSuccessfulStorageMigrationWaitsBeforeBlockingFinalizerJobWrite(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 81)
	finalizer, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// Visible Completion locks the Job before writing the Attempt, and updates
	// the Job only after its other completion records have been inserted.
	if _, err := finalizer.Exec(`LOCK TABLE jobs IN ROW SHARE MODE`); err != nil {
		t.Fatal(err)
	}
	if _, err := finalizer.Exec(`LOCK TABLE attempts IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	migrated := make(chan error, 1)
	go func() {
		migrated <- goose.UpTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 82)
	}()
	defer func() {
		_ = finalizer.Rollback()
		if err := <-migrated; err != nil {
			t.Errorf("finish storage migration after finalizer release: %v", err)
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err := database.Admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_locks
			WHERE NOT granted AND relation IN ('jobs'::regclass, 'attempts'::regclass))`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("storage migration did not wait for in-flight finalization")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := finalizer.Exec(`LOCK TABLE jobs IN ROW EXCLUSIVE MODE NOWAIT`); err != nil {
		t.Fatalf("waiting storage migration blocked finalizer Job write: %v", err)
	}
}

type stageStorageCompletionEvidence struct {
	state                        string
	reservedBytes, consumedBytes int64
	artifacts, usage             string
}

func readStageStorageCompletionEvidence(t *testing.T, outcome cpuMediaGraphOutcome) stageStorageCompletionEvidence {
	t.Helper()
	var evidence stageStorageCompletionEvidence
	if err := outcome.database.Admin.QueryRow(`
		SELECT reservation.state::text, reservation.reserved_bytes, reservation.consumed_bytes,
		       (SELECT jsonb_agg(to_jsonb(artifact) ORDER BY artifact.id)::text
		        FROM stage_artifacts AS artifact WHERE artifact.job_id = reservation.job_id),
		       (SELECT jsonb_agg(to_jsonb(usage) ORDER BY usage.id)::text
		        FROM resource_usage_records AS usage WHERE usage.job_id = reservation.job_id)
		FROM stage_storage_reservations AS reservation WHERE reservation.attempt_id = $1
	`, outcome.attemptID).Scan(&evidence.state, &evidence.reservedBytes, &evidence.consumedBytes,
		&evidence.artifacts, &evidence.usage); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func assertConsumedStageStorageEvidence(t *testing.T, outcome cpuMediaGraphOutcome, before stageStorageCompletionEvidence) {
	t.Helper()
	after := readStageStorageCompletionEvidence(t, outcome)
	if after.state != "CONSUMED" || after.reservedBytes != before.reservedBytes ||
		after.consumedBytes != before.consumedBytes || after.artifacts != before.artifacts || after.usage != before.usage {
		t.Fatalf("storage settlement: state=%s reserved=%d/%d consumed=%d/%d artifacts unchanged=%t usage unchanged=%t",
			after.state, before.reservedBytes, after.reservedBytes, before.consumedBytes, after.consumedBytes,
			after.artifacts == before.artifacts, after.usage == before.usage)
	}
	var charges, completions int
	if err := outcome.database.Admin.QueryRow(`SELECT
		(SELECT count(*) FROM charges WHERE job_id = $1),
		(SELECT count(*) FROM visible_completions WHERE job_id = $1)`, outcome.jobID).Scan(&charges, &completions); err != nil {
		t.Fatal(err)
	}
	if charges != 1 || completions != 1 {
		t.Fatalf("storage settlement changed fixed-price completion: charges=%d completions=%d", charges, completions)
	}
}

func completeStorageReservationGraph(t *testing.T, outcome cpuMediaGraphOutcome) (stagefinalization.VisibleCompletionResult, stagefinalization.VisibleCompletionResult) {
	t.Helper()
	service := visibleCompletionService(t, outcome.database.DSN, outcome.objectStore)
	finalizer := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela.internal/finalizer/storage-completion"}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted {
		t.Fatalf("claim storage completion: %+v %v", claim, err)
	}
	candidate := stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion}
	completed, err := service.CompleteStageGraphVisibleCompletion(context.Background(), finalizer, claim.Credentials, candidate)
	if err != nil || completed.Decision != stagefinalization.VisibleCompletionCommitted {
		t.Fatalf("complete storage graph: %+v %v", completed, err)
	}
	replay, err := service.CompleteStageGraphVisibleCompletion(context.Background(), finalizer, claim.Credentials, candidate)
	if err != nil || replay.Decision != stagefinalization.VisibleCompletionCommitted {
		t.Fatalf("replay storage graph: %+v %v", replay, err)
	}
	return completed, replay
}
