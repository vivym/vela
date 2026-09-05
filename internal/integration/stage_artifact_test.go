//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/stageartifact"
)

func TestStageArtifactSealRefusesStorageReservationOverflowBeforeGPURelease(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-artifact-storage-bound")
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct storage-bound StageArtifact repository: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin storage reservation bound: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("assume AttemptCoordinator owner role: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE stage_storage_reservations SET reserved_bytes = 1 WHERE attempt_id = $1
	`, attemptID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("shrink storage reservation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit storage reservation bound: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	digest := sha256.Sum256([]byte("two-byte sealed output"))
	lineage := sha256.Sum256([]byte("storage-bound lineage"))
	tokenDigest := sha256.Sum256([]byte("storage-bound materialization authority"))
	leaseID := uuid.New()
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "storage-bound-local-receipt", LocalReceiptDigest: digest,
		ManifestSHA256: digest, SHA256: digest, LineageDigest: lineage,
		TokenDigest: tokenDigest, SizeBytes: 2, ArtifactID: uuid.New(),
		MaterializationLeaseID: leaseID,
		ObjectKey: "artifacts/stage/org/project/" + attemptID.String() +
			"/encoder/storage-bound-output.bin",
		ContentType: "application/octet-stream", SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	}); err == nil {
		t.Fatal("seal issued MaterializationAuthority beyond reserved storage capacity")
	}

	var runState, attemptState, allocationState, leaseState string
	var materializationCount int
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, physical.state::text, allocation.state::text,
		       lease.state::text,
		       (SELECT count(*) FROM stage_materialization_leases WHERE id = $5)
		FROM stage_runs AS run
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.id = $3
		JOIN stage_leases AS lease ON lease.id = $4
		WHERE run.id = $1
	`, encoderRunID, assignment.StageAttemptID, assignment.StageAllocationID,
		assignment.StageLeaseID, leaseID).Scan(
		&runState, &attemptState, &allocationState, &leaseState, &materializationCount,
	); err != nil {
		t.Fatalf("inspect storage-bound seal rejection: %v", err)
	}
	if runState != "RUNNING" || attemptState != "RUNNING" || allocationState != "ALLOCATED" ||
		leaseState != "ACTIVE" || materializationCount != 0 {
		t.Fatalf(
			"storage-bound seal mutated authority run=%s attempt=%s allocation=%s lease=%s materializations=%d",
			runState, attemptState, allocationState, leaseState, materializationCount,
		)
	}
}

func TestStageArtifactLocalSourceLossRetriesComputeWithoutHoldingGPU(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-artifact-source-lost")
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct source-loss StageArtifact repository: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	digest := sha256.Sum256([]byte("sealed output lost with node"))
	lineage := sha256.Sum256([]byte("source-loss root lineage"))
	materializationTokenDigest := sha256.Sum256([]byte("source-loss materialization authority"))
	leaseID := uuid.New()
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID,
		StageAttemptID:    assignment.StageAttemptID,
		StageAllocationID: assignment.StageAllocationID,
		StageLeaseID:      assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "lost-local-receipt", LocalReceiptDigest: digest,
		ManifestSHA256: digest, SHA256: digest, LineageDigest: lineage,
		TokenDigest: materializationTokenDigest,
		SizeBytes:   64, ArtifactID: uuid.New(), MaterializationLeaseID: leaseID,
		ObjectKey: "artifacts/stage/org/project/" + attemptID.String() +
			"/encoder/lost-output.bin",
		ContentType: "application/octet-stream", SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	}); err != nil {
		t.Fatalf("seal source-loss StageArtifact: %v", err)
	}
	fingerprint := sha256.Sum256([]byte("node epoch disappeared before durable commit"))
	forgedTokenDigest := sha256.Sum256([]byte("forged materialization authority"))
	if _, err := repository.FailSourceLost(
		context.Background(),
		stageartifact.SourceLostCommand{
			CommandID: uuid.New(), MaterializationLeaseID: leaseID,
			TokenDigest:        forgedTokenDigest,
			FailureFingerprint: fingerprint, ConsumedResourceUnits: 100,
			LostAt: now.Add(time.Second), RetryAt: now.Add(2 * time.Second),
		},
	); err == nil {
		t.Fatal("forged materialization token authorized source loss")
	}
	var preflightRunState, preflightMaterializationState string
	var preflightAttemptsConsumed int
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, materialization.state::text, budget.attempts_consumed
		FROM stage_runs AS run
		JOIN stage_materialization_leases AS materialization ON materialization.id = $2
		JOIN stage_retry_budgets AS budget ON budget.stage_run_id = run.id
		WHERE run.id = $1
	`, encoderRunID, leaseID).Scan(
		&preflightRunState, &preflightMaterializationState, &preflightAttemptsConsumed,
	); err != nil {
		t.Fatalf("inspect forged source-loss rejection: %v", err)
	}
	if preflightRunState != "MATERIALIZING" || preflightMaterializationState != "ACTIVE" ||
		preflightAttemptsConsumed != 1 {
		t.Fatalf(
			"forged source-loss mutated authority run=%s materialization=%s attempts=%d",
			preflightRunState, preflightMaterializationState, preflightAttemptsConsumed,
		)
	}
	decision, err := repository.FailSourceLost(
		context.Background(),
		stageartifact.SourceLostCommand{
			CommandID: uuid.New(), MaterializationLeaseID: leaseID,
			TokenDigest:        materializationTokenDigest,
			FailureFingerprint: fingerprint, ConsumedResourceUnits: 100,
			LostAt: now.Add(time.Second), RetryAt: now.Add(2 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("fail lost local StageArtifact source: %v", err)
	}
	if decision.State != "RETRY_WAIT" || decision.StageFence != 2 ||
		decision.StageVersion != 5 {
		t.Fatalf("source-loss decision = %#v", decision)
	}
	var runState, attemptState, allocationState, materializationState string
	var retryCount, artifactCount int
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, physical.state::text, allocation.state::text,
		       materialization.state::text, run.retry_count,
		       (SELECT count(*) FROM stage_artifacts WHERE producer_stage_run_id = run.id)
		FROM stage_runs AS run
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.id = $3
		JOIN stage_materialization_leases AS materialization ON materialization.id = $4
		WHERE run.id = $1
	`, encoderRunID, assignment.StageAttemptID, assignment.StageAllocationID, leaseID).Scan(
		&runState, &attemptState, &allocationState, &materializationState,
		&retryCount, &artifactCount,
	); err != nil {
		t.Fatalf("inspect source-loss retry authority: %v", err)
	}
	if runState != "RETRY_WAIT" || attemptState != "LOST" ||
		allocationState != "RELEASED" || materializationState != "REVOKED" ||
		retryCount != 1 || artifactCount != 0 {
		t.Fatalf(
			"source-loss states run=%s attempt=%s allocation=%s materialization=%s retry=%d artifacts=%d",
			runState, attemptState, allocationState, materializationState,
			retryCount, artifactCount,
		)
	}
}

func TestStageArtifactLocalSourceLossExhaustedBudgetFailsParentGraph(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-artifact-source-lost-terminal")
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct terminal source-loss repository: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	digest := sha256.Sum256([]byte("last-attempt output lost before L2"))
	lineage := sha256.Sum256([]byte("terminal source-loss lineage"))
	tokenDigest := sha256.Sum256([]byte("terminal source-loss authority"))
	leaseID := uuid.New()
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "terminal-lost-local-receipt", LocalReceiptDigest: digest,
		ManifestSHA256: digest, SHA256: digest, LineageDigest: lineage,
		TokenDigest: tokenDigest, SizeBytes: 64, ArtifactID: uuid.New(),
		MaterializationLeaseID: leaseID,
		ObjectKey: "artifacts/stage/org/project/" + attemptID.String() +
			"/encoder/terminal-lost-output.bin",
		ContentType: "application/octet-stream", SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	}); err != nil {
		t.Fatalf("seal terminal source-loss StageArtifact: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin stage retry budget exhaustion: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("assume AttemptCoordinator owner role: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE stage_retry_budgets
		SET attempts_consumed = max_attempts, state = 'EXHAUSTED'
		WHERE stage_run_id = $1
	`, encoderRunID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("exhaust stage retry budget: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit stage retry budget exhaustion: %v", err)
	}
	fingerprint := sha256.Sum256([]byte("last local source vanished"))
	decision, err := repository.FailSourceLost(
		context.Background(),
		stageartifact.SourceLostCommand{
			CommandID: uuid.New(), MaterializationLeaseID: leaseID, TokenDigest: tokenDigest,
			FailureFingerprint: fingerprint, ConsumedResourceUnits: 100,
			LostAt: now.Add(time.Second), RetryAt: now.Add(2 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("terminal source loss: %v", err)
	}
	if decision.State != "FAILED" || decision.StageFence != 2 || decision.StageVersion != 5 {
		t.Fatalf("terminal source-loss decision = %#v", decision)
	}
	var jobState, attemptState, graphState, runState, materializationState string
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.state::text, attempt.graph_state::text,
		       run.state::text, materialization.state::text
		FROM attempts AS attempt
		JOIN jobs AS job ON job.id = attempt.job_id
		JOIN stage_runs AS run ON run.attempt_id = attempt.id AND run.id = $2
		JOIN stage_materialization_leases AS materialization
		  ON materialization.stage_run_id = run.id AND materialization.id = $3
		WHERE attempt.id = $1
	`, attemptID, encoderRunID, leaseID).Scan(
		&jobState, &attemptState, &graphState, &runState, &materializationState,
	); err != nil {
		t.Fatalf("inspect terminal source-loss graph: %v", err)
	}
	if jobState != "FAILED" || attemptState != "FAILED" || graphState != "FAILED" ||
		runState != "FAILED" || materializationState != "REVOKED" {
		t.Fatalf(
			"terminal source-loss states job=%s attempt=%s graph=%s run=%s materialization=%s",
			jobState, attemptState, graphState, runState, materializationState,
		)
	}
}

func TestStageArtifactMigrationEmptyDownUpRestoresExactRoleSurface(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 38)
	if err := goose.DownTo(database.Admin, migrations, 37); err != nil {
		t.Fatalf("migrate empty StageArtifact down: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 37 {
		t.Fatalf("StageArtifact version after Down = %d error=%v", version, err)
	}
	var runsRemain, artifactsRemoved, functionRemoved, winnerColumnRemoved bool
	if err := database.Admin.QueryRow(`
			SELECT to_regclass('stage_runs') IS NOT NULL,
			       to_regclass('stage_artifacts') IS NULL,
			       to_regprocedure('vela_commit_stage_artifact(jsonb)') IS NULL,
			       NOT EXISTS (
			           SELECT 1 FROM information_schema.columns
			           WHERE table_schema = 'public' AND table_name = 'stage_runs'
			             AND column_name = 'winner_stage_artifact_id'
			       )
	`).Scan(
		&runsRemain, &artifactsRemoved, &functionRemoved, &winnerColumnRemoved,
	); err != nil {
		t.Fatalf("inspect schema 37 StageArtifact rollback surface: %v", err)
	}
	if !runsRemain || !artifactsRemoved || !functionRemoved || !winnerColumnRemoved {
		t.Fatalf(
			"schema 37 StageArtifact surface runs/artifacts/function/column = %t/%t/%t/%t",
			runsRemain, artifactsRemoved, functionRemoved, winnerColumnRemoved,
		)
	}
	if err := goose.UpTo(database.Admin, migrations, 38); err != nil {
		t.Fatalf("migrate StageArtifact up again: %v", err)
	}
	stageArtifactPool := newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	)
	if err := veladb.VerifyRole(
		context.Background(), stageArtifactPool, veladb.RoleStageArtifact,
	); err != nil {
		t.Fatalf("verify StageArtifact role after Down Up: %v", err)
	}
}

func TestStageTransferServerClockMigrationEmptyDownUp(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 68)
	assertServerClock := func(want bool) {
		t.Helper()
		for _, signature := range []string{
			"public.vela_resolve_stage_transfer_ticket(jsonb)",
			"public.vela_consume_stage_transfer_ticket(jsonb)",
		} {
			var definition string
			if err := database.Admin.QueryRow(`
				SELECT pg_get_functiondef($1::regprocedure)
			`, signature).Scan(&definition); err != nil {
				t.Fatalf("read %s definition: %v", signature, err)
			}
			for _, fragment := range []string{
				"v_server_now < v_ticket.issued_at",
				"v_server_now >= v_ticket.expires_at",
				"v_server_now := clock_timestamp()",
				"observation.expires_at > v_server_now",
			} {
				if got := strings.Contains(definition, fragment); got != want {
					t.Fatalf("%s contains %q=%t want=%t", signature, fragment, got, want)
				}
			}
		}
	}

	assertServerClock(false)
	if err := goose.UpTo(database.Admin, migrations, 69); err != nil {
		t.Fatalf("add Stage TransferTicket server clock: %v", err)
	}
	assertServerClock(true)
	if err := goose.DownTo(database.Admin, migrations, 68); err != nil {
		t.Fatalf("remove Stage TransferTicket server clock: %v", err)
	}
	assertServerClock(false)
	if err := goose.UpTo(database.Admin, migrations, 69); err != nil {
		t.Fatalf("restore Stage TransferTicket server clock: %v", err)
	}
	assertServerClock(true)
}
