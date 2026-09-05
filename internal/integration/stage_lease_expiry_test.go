//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageartifact"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageLeaseExpiryRecoversLostWorkersWithoutWorkerReports(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "assigned"
		if started {
			name = "running"
		}
		t.Run(name, func(t *testing.T) {
			database, _, coordinator, job, attemptID, runID, _ :=
				newStageGraphCancellationFixture(t, "lost-worker-"+name)
			expiresAt := time.Now().UTC().Add(2 * time.Second)
			var assignment attemptcoordinator.AssignStageCommand
			if started {
				assignment = assignAndStartEncoder(t, database, coordinator, attemptID, runID, expiresAt)
			} else {
				assignment = assignEncoder(t, database, coordinator, attemptID, runID, expiresAt)
			}
			assertStageExpiryAllocation(t, database, assignment, "ALLOCATED")
			if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
				t.Fatalf("reconcile before expiry: %v", err)
			}
			assertStageExpiryAllocation(t, database, assignment, "ALLOCATED")
			waitStageExpiry(t, expiresAt)
			decisions, err := coordinator.Reconcile(context.Background(), 100)
			if err != nil {
				t.Fatalf("reconcile expired authority: %v", err)
			}
			if len(decisions) != 1 || decisions[0].StageRunID != runID ||
				decisions[0].State != "RETRY_WAIT" || decisions[0].Reason != "STAGE_AUTHORITY_EXPIRED" {
				t.Fatalf("expired authority decisions = %#v", decisions)
			}
			assertStageExpiryAllocation(t, database, assignment, "RELEASED")
			var physicalState, leaseState, jobState string
			var consumed, charges int64
			var billable bool
			if err := database.Admin.QueryRow(`
				SELECT physical.state::text, lease.state::text, job.state::text,
				       budget.consumed_resource_units, job.billable_started_at IS NOT NULL,
				       (SELECT count(*) FROM charges WHERE job_id = job.id)
				FROM stage_attempts AS physical
				JOIN stage_leases AS lease ON lease.stage_attempt_id = physical.id
				JOIN attempts AS attempt ON attempt.id = physical.attempt_id
				JOIN jobs AS job ON job.id = attempt.job_id
				JOIN attempt_retry_budgets AS budget ON budget.attempt_id = attempt.id
				WHERE physical.id = $1 AND job.id = $2
			`, assignment.StageAttemptID, job.JobID).Scan(
				&physicalState, &leaseState, &jobState, &consumed, &billable, &charges,
			); err != nil {
				t.Fatalf("read recovered lost Worker: %v", err)
			}
			if physicalState != "LOST" || leaseState != "EXPIRED" || billable != started ||
				charges != 0 || (started && (consumed <= 0 || jobState != "RUNNING")) ||
				(!started && (consumed != 0 || jobState != "QUEUED")) {
				t.Fatalf("lost Worker physical/lease/job=%s/%s/%s units=%d billable=%t charges=%d",
					physicalState, leaseState, jobState, consumed, billable, charges)
			}
			if replay, err := coordinator.Reconcile(context.Background(), 100); err != nil || len(replay) != 0 {
				t.Fatalf("replayed expiry = %#v, error=%v", replay, err)
			}
			waitStageExpiry(t, time.Now().Add(time.Second))
			if ready, err := coordinator.Reconcile(context.Background(), 100); err != nil ||
				len(ready) != 1 || ready[0].State != "READY" || ready[0].StageFence != 2 {
				t.Fatalf("lost Worker retry readiness = %#v, error=%v", ready, err)
			}
		})
	}
}

func TestStageLeaseExpiryHonorsLatestRenewalDuringCancellation(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, runID, _ :=
		newStageGraphCancellationFixture(t, "renewed-cancellation-expiry")
	assignment := assignEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(2*time.Second))
	expiresAt := renewStageExpiryFixture(t, database, job, assignment)
	waitStageExpiry(t, assignment.ExpiresAt)
	if decisions, err := coordinator.Reconcile(context.Background(), 100); err != nil || len(decisions) != 0 {
		t.Fatalf("live renewed execution reconciled = %#v, error=%v", decisions, err)
	}
	assertStageExpiryAllocation(t, database, assignment, "ALLOCATED")
	canceled := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel renewed Job status=%d body=%s", canceled.StatusCode, canceled.Body)
	}
	if decisions, err := coordinator.Reconcile(context.Background(), 100); err != nil || len(decisions) != 0 {
		t.Fatalf("renewed cancellation released early = %#v, error=%v", decisions, err)
	}
	assertStageExpiryAllocation(t, database, assignment, "ALLOCATED")
	var stops int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM stage_cancellation_stop_receipts WHERE stage_lease_id = $1`,
		assignment.StageLeaseID).Scan(&stops); err != nil || stops != 0 {
		t.Fatalf("premature cancellation stop proofs=%d error=%v", stops, err)
	}
	waitStageExpiry(t, expiresAt)
	if decisions, err := coordinator.Reconcile(context.Background(), 100); err != nil ||
		len(decisions) != 1 || decisions[0].Reason != "CANCELLATION_LEASE_EXPIRED" {
		t.Fatalf("renewed cancellation expiry = %#v, error=%v", decisions, err)
	}
	assertStageExpiryAllocation(t, database, assignment, "RELEASED")
}

func TestStageLeaseExpiryTerminatesExhaustedAttemptAndReleasesCredit(t *testing.T) {
	database, _, coordinator, job, attemptID, runID, _ :=
		newStageGraphCancellationFixture(t, "exhausted-expiry")
	expiresAt := time.Now().Add(2 * time.Second)
	assignment := assignEncoder(t, database, coordinator, attemptID, runID, expiresAt)
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE stage_retry_budgets SET max_attempts = attempts_consumed WHERE stage_run_id = $1`, runID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	waitStageExpiry(t, expiresAt)
	if decisions, err := coordinator.Reconcile(context.Background(), 100); err != nil ||
		len(decisions) != 1 || decisions[0].State != "FAILED" {
		t.Fatalf("exhausted expiry = %#v, error=%v", decisions, err)
	}
	assertStageExpiryAllocation(t, database, assignment, "RELEASED")
	var state, reservation string
	var queued, running, reserved, charges, failedEvents int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, reservation.state::text,
		       project.queued_count, project.running_count, account.reserved_minor,
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       (SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'job.failed')
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN organization_credit_accounts AS account ON account.organization_id = job.organization_id
		WHERE job.id = $1
	`, job.JobID).Scan(&state, &reservation, &queued, &running, &reserved, &charges, &failedEvents); err != nil {
		t.Fatalf("read exhausted Job counters: %v", err)
	}
	if state != "FAILED" || reservation != "RELEASED" || queued != 0 || running != 0 ||
		reserved != 0 || charges != 0 || failedEvents != 1 {
		t.Fatalf("terminal expiry state=%s credit=%s queue/run/reserved/charges/events=%d/%d/%d/%d/%d",
			state, reservation, queued, running, reserved, charges, failedEvents)
	}
	if replay, err := coordinator.Reconcile(context.Background(), 100); err != nil || len(replay) != 0 {
		t.Fatalf("terminal expiry replay=%#v error=%v", replay, err)
	}
}

func TestStageMaterializationExpiryRecoversWithoutSourceLossReport(t *testing.T) {
	database, _, coordinator, _, attemptID, runID, _ :=
		newStageGraphCancellationFixture(t, "materialization-expiry")
	assignment := assignAndStartEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(time.Hour))
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	digest := sha256.Sum256([]byte("expired output"))
	leaseID := uuid.New()
	seal := stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "expiry-local-receipt", LocalReceiptDigest: digest, ManifestSHA256: digest,
		SHA256: digest, LineageDigest: digest, TokenDigest: digest, SizeBytes: 64,
		ArtifactID: uuid.New(), MaterializationLeaseID: leaseID,
		ObjectKey:   "artifacts/stage/expiry/" + attemptID.String() + "/output.bin",
		ContentType: "application/octet-stream", SealedAt: now, LeaseExpiresAt: now.Add(time.Second),
	}
	if _, err := repository.Seal(context.Background(), seal); err != nil {
		t.Fatalf("seal expiring output: %v", err)
	}
	waitStageExpiry(t, seal.LeaseExpiresAt)
	if _, err := repository.Commit(context.Background(), stageartifact.CommitCommand{
		CommandID: uuid.New(), ProgressReceiptID: uuid.New(), MaterializationLeaseID: leaseID,
		ArtifactID: seal.ArtifactID, ObjectKey: seal.ObjectKey, ObjectVersion: "backdated-object",
		SHA256: digest, SizeBytes: seal.SizeBytes, TokenDigest: digest,
		CommittedAt: seal.SealedAt.Add(10 * time.Millisecond),
	}); err == nil {
		t.Fatal("expired materialization accepted a backdated commit before reconciliation")
	}
	if decisions, err := coordinator.Reconcile(context.Background(), 100); err != nil ||
		len(decisions) != 1 || decisions[0].State != "RETRY_WAIT" ||
		decisions[0].Reason != "MATERIALIZATION_AUTHORITY_EXPIRED" {
		t.Fatalf("materialization expiry = %#v, error=%v", decisions, err)
	}
	assertStageExpiryAllocation(t, database, assignment, "RELEASED")
	if _, err := repository.Commit(context.Background(), stageartifact.CommitCommand{
		CommandID: uuid.New(), ProgressReceiptID: uuid.New(), MaterializationLeaseID: leaseID,
		ArtifactID: seal.ArtifactID, ObjectKey: seal.ObjectKey, ObjectVersion: "late-object",
		SHA256: digest, SizeBytes: seal.SizeBytes, TokenDigest: digest, CommittedAt: time.Now(),
	}); err == nil {
		t.Fatal("expired materialization committed after retry fencing")
	}
}

func TestStageLeaseExpiryMigrationRoundTrip(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 69); err != nil {
		t.Fatalf("rollback expiry recovery: %v", err)
	}
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("reapply expiry recovery: %v", err)
	}
	var publicExecute, coordinatorExecute bool
	if err := database.Admin.QueryRow(`
		SELECT has_function_privilege('vela_request', 'vela_recover_expired_stage_authority(uuid)', 'EXECUTE'),
		       has_function_privilege('vela_attempt_coordinator', 'vela_reconcile_stage_graphs(integer)', 'EXECUTE')
	`).Scan(&publicExecute, &coordinatorExecute); err != nil {
		t.Fatalf("inspect recovery entrypoint authority: %v", err)
	}
	if publicExecute || !coordinatorExecute {
		t.Fatalf("recovery grants api=%t coordinator=%t", publicExecute, coordinatorExecute)
	}
}

func TestStageLeaseExpiryConcurrentReconciliationAppliesOnce(t *testing.T) {
	database, _, coordinator, _, attemptID, runID, _ :=
		newStageGraphCancellationFixture(t, "concurrent-stage-expiry")
	expiresAt := time.Now().Add(time.Second)
	assignment := assignEncoder(t, database, coordinator, attemptID, runID, expiresAt)
	waitStageExpiry(t, expiresAt)
	var group sync.WaitGroup
	var mu sync.Mutex
	var results []attemptcoordinator.ReconcileDecision
	for range 4 {
		group.Go(func() {
			decisions, err := coordinator.Reconcile(context.Background(), 1)
			if err != nil {
				t.Errorf("concurrent expiry reconciliation: %v", err)
				return
			}
			mu.Lock()
			results = append(results, decisions...)
			mu.Unlock()
		})
	}
	group.Wait()
	if len(results) != 1 || results[0].StageRunID != runID || results[0].State != "RETRY_WAIT" {
		t.Fatalf("concurrent recovery results=%#v", results)
	}
	assertStageExpiryAllocation(t, database, assignment, "RELEASED")
}

func TestStageMaterializationExpiryIsCheckedAfterCommitLockWait(t *testing.T) {
	database, _, coordinator, _, attemptID, runID, _ :=
		newStageGraphCancellationFixture(t, "materialization-expiry-lock-wait")
	assignment := assignAndStartEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(time.Hour))
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	digest := sha256.Sum256([]byte("lock delayed output"))
	seal := stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "lock-delay-receipt", LocalReceiptDigest: digest, ManifestSHA256: digest,
		SHA256: digest, LineageDigest: digest, TokenDigest: digest, SizeBytes: 64,
		ArtifactID: uuid.New(), MaterializationLeaseID: uuid.New(),
		ObjectKey:   "artifacts/stage/expiry/" + attemptID.String() + "/delayed-output.bin",
		ContentType: "application/octet-stream", SealedAt: now, LeaseExpiresAt: now.Add(2 * time.Second),
	}
	if _, err := repository.Seal(context.Background(), seal); err != nil {
		t.Fatalf("seal delayed output: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM stage_materialization_leases WHERE id = $1 FOR UPDATE`,
		seal.MaterializationLeaseID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := repository.Commit(ctx, stageartifact.CommitCommand{
			CommandID: uuid.New(), ProgressReceiptID: uuid.New(), MaterializationLeaseID: seal.MaterializationLeaseID,
			ArtifactID: seal.ArtifactID, ObjectKey: seal.ObjectKey, ObjectVersion: "lock-delayed-object",
			SHA256: digest, SizeBytes: seal.SizeBytes, TokenDigest: digest,
			CommittedAt: now.Add(10 * time.Millisecond),
		})
		result <- err
	}()
	for {
		var waiting bool
		if err := database.Admin.QueryRow(`
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE usename = 'vela_stage_artifact_login' AND wait_event_type = 'Lock')
		`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(seal.LeaseExpiresAt) {
			t.Fatal("commit did not enter its lock wait before authority expiration")
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitStageExpiry(t, seal.LeaseExpiresAt)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil || !strings.Contains(err.Error(), "StageArtifact commit authority") {
		t.Fatalf("commit after lock wait error=%v", err)
	}
	var artifacts int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM stage_artifacts WHERE id = $1`, seal.ArtifactID).
		Scan(&artifacts); err != nil || artifacts != 0 {
		t.Fatalf("expired delayed commit artifacts=%d error=%v", artifacts, err)
	}
}

func renewStageExpiryFixture(t *testing.T, database testDatabase, job jobResponse,
	assignment attemptcoordinator.AssignStageCommand,
) time.Time {
	t.Helper()
	assigned := signedAssignedStageAuthority(t, database, job, assignment, 2)
	startedEnvelope := proto.Clone(assigned.Authority).(*velav1.StageAuthority)
	startedEnvelope.StageVersion = 3
	startedEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(10 * time.Millisecond))
	startedEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(time.Second))
	startedEnvelope.MonotonicValidFor = durationpb.New(time.Second)
	startedEnvelope.Signature = nil
	started := signAndVerifyStageAuthority(t, startedEnvelope, assignment.IssuedAt.Add(11*time.Millisecond))
	startedWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(started.Authority)
	if err != nil {
		t.Fatal(err)
	}
	workerPool := newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	if _, err := workerPool.Exec(context.Background(), `SELECT * FROM vela_start_stage_worker_command($1::jsonb)`,
		stageWorkerStartPayload(t, uuid.New(), assignment, assigned, started, startedWire,
			assignment.IssuedAt.Add(5*time.Millisecond))); err != nil {
		t.Fatalf("start renewed expiry fixture: %v", err)
	}
	heartbeatEnvelope := proto.Clone(started.Authority).(*velav1.StageAuthority)
	heartbeatEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(30 * time.Millisecond))
	heartbeatEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(2 * time.Second))
	heartbeatEnvelope.MonotonicValidFor = durationpb.New(3 * time.Second)
	heartbeatEnvelope.Signature = nil
	heartbeat := signAndVerifyStageAuthority(t, heartbeatEnvelope, assignment.IssuedAt.Add(31*time.Millisecond))
	heartbeatWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(heartbeat.Authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workerPool.Exec(context.Background(), `SELECT * FROM vela_heartbeat_stage_worker_command($1::jsonb)`,
		stageWorkerHeartbeatPayload(t, uuid.New(), assignment, started, heartbeat, heartbeatWire,
			1, assignment.IssuedAt.Add(20*time.Millisecond))); err != nil {
		t.Fatalf("renew heartbeat expiry fixture: %v", err)
	}
	return heartbeatEnvelope.GetExpiresAt().AsTime()
}

func assertStageExpiryAllocation(t *testing.T, database testDatabase,
	assignment attemptcoordinator.AssignStageCommand, want string,
) {
	t.Helper()
	var state string
	if err := database.Admin.QueryRow(`SELECT state::text FROM stage_allocations WHERE id = $1`,
		assignment.StageAllocationID).Scan(&state); err != nil || state != want {
		t.Fatalf("allocation=%s want=%s error=%v", state, want, err)
	}
}

func waitStageExpiry(t *testing.T, deadline time.Time) {
	t.Helper()
	if duration := time.Until(deadline.Add(30 * time.Millisecond)); duration > 0 {
		time.Sleep(duration)
	}
}
