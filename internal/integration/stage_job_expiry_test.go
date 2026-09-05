//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stagefinalization"
)

func TestStageJobExpiryRejectsQueuedAssignmentAndReleasesCredit(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironmentWithCatalogSetup(t, seedShortStageJobLifetime)
	job, attemptID := instantiateH3IntegrationGraph(t, database, serverURL, "queued-job-expiry")
	var runID uuid.UUID
	if err := database.Admin.QueryRow(`SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'`,
		attemptID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	stage := h3IntegrationStages([]string{"expiry-encoder", "unused-dit", "unused-vae"}, nil)[0]
	worker := seedH3IntegrationWorker(t, database, registry, stage, 0xd1)
	waitStageExpiry(t, job.JobExpiresAt)
	command := stageJobExpiryAssignment(attemptID, runID, worker)
	if decision, err := coordinator.Apply(context.Background(), command); err == nil {
		t.Fatalf("expired queued Job received new Stage authority: %#v", decision)
	}
	if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	assertExpiredStageJob(t, database, job.JobID)
	if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	assertExpiredStageJob(t, database, job.JobID)
}

func TestStageJobExpiryConvergesActiveAuthorityWithoutEarlyCapacityReuse(t *testing.T) {
	for _, state := range []string{"ASSIGNED", "RUNNING", "MATERIALIZING"} {
		t.Run(state, func(t *testing.T) {
			database, coordinator, serverURL := newH3IntegrationEnvironmentWithCatalogSetup(t, seedShortStageJobLifetime)
			job, attemptID := instantiateH3IntegrationGraph(t, database, serverURL, "active-expiry-"+state)
			var runID uuid.UUID
			if err := database.Admin.QueryRow(`SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'`,
				attemptID).Scan(&runID); err != nil {
				t.Fatal(err)
			}
			assignment := assignEncoder(t, database, coordinator, attemptID, runID, job.JobExpiresAt.Add(time.Second))
			latestExpiry := assignment.ExpiresAt
			if state != "ASSIGNED" {
				latestExpiry = renewStageExpiryFixture(t, database, job, assignment)
			}
			var repository *stageartifact.PostgresRepository
			var seal stageartifact.SealCommand
			if state == "MATERIALIZING" {
				repository, seal = sealFailureGraphOutput(t, database, assignment, job.JobExpiresAt)
			}
			waitStageExpiry(t, job.JobExpiresAt)
			if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
				t.Fatal(err)
			}
			assertExpiredStageJob(t, database, job.JobID)
			if state == "MATERIALIZING" {
				assertStageExpiryAllocation(t, database, assignment, "RELEASED")
				if _, err := repository.Commit(context.Background(), stageartifact.CommitCommand{
					CommandID: uuid.New(), ProgressReceiptID: uuid.New(), MaterializationLeaseID: seal.MaterializationLeaseID,
					ArtifactID: seal.ArtifactID, ObjectKey: seal.ObjectKey, ObjectVersion: "expired-job-output",
					SHA256: seal.SHA256, SizeBytes: seal.SizeBytes, TokenDigest: seal.TokenDigest,
					CommittedAt: seal.SealedAt.Add(time.Millisecond),
				}); err == nil {
					t.Fatal("expired Job accepted materialization completion")
				}
			} else {
				assertStageExpiryAllocation(t, database, assignment, "ALLOCATED")
				waitStageExpiry(t, latestExpiry)
				if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
					t.Fatal(err)
				}
				assertStageExpiryAllocation(t, database, assignment, "RELEASED")
			}
			assertExpiredStageJob(t, database, job.JobID)
		})
	}
}

func TestStageJobExpiryConvergesFinalizationAndExpiresClaim(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironmentWithCatalogSetup(t, seedShortStageJobLifetime)
	seedWorkerRegistryPlan(t, database.Admin)
	outcome := runSplitH3StageGraphInEnvironment(t, database, coordinator, serverURL,
		[]string{"expiry-enc", "expiry-dit", "expiry-vae"}, "finalizing-expiry")
	service := visibleCompletionService(t, database.DSN, outcome.objectStore)
	finalizer := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela/finalizer/expiry"}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted {
		t.Fatalf("finalization expiry claim=%#v error=%v", claim, err)
	}
	waitStageExpiry(t, claim.FinalizationDeadlineAt)
	if decisions, err := coordinator.Reconcile(context.Background(), 100); err != nil ||
		len(decisions) != 1 || decisions[0].Reason != "FINALIZATION_DEADLINE_EXPIRED" {
		t.Fatalf("finalization deadline decisions=%#v error=%v", decisions, err)
	}
	assertExpiredStageJob(t, database, outcome.jobID.String())
	var state string
	if err := database.Admin.QueryRow(`SELECT state::text FROM stage_graph_finalization_claims WHERE id = $1`, claim.ClaimID).
		Scan(&state); err != nil || state != "EXPIRED" {
		t.Fatalf("terminal graph claim=%s error=%v", state, err)
	}
	result, err := service.CompleteStageGraphVisibleCompletion(context.Background(), finalizer, claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion})
	if err != nil || (result.Decision != stagefinalization.VisibleCompletionAlreadyFailed &&
		result.Decision != stagefinalization.VisibleCompletionRejectedStaleLease) {
		t.Fatalf("late finalization result=%#v error=%v", result, err)
	}
}

func TestStageJobExpiryIsCheckedAfterAssignmentLockWait(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironmentWithCatalogSetup(t, seedShortStageJobLifetime)
	job, attemptID := instantiateH3IntegrationGraph(t, database, serverURL, "assignment-lock-expiry")
	var runID uuid.UUID
	if err := database.Admin.QueryRow(`SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'`,
		attemptID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	worker := seedH3IntegrationWorker(t, database, registry,
		h3IntegrationStages([]string{"late-assignment", "unused-dit", "unused-vae"}, nil)[0], 0xd2)
	command := stageJobExpiryAssignment(attemptID, runID, worker)
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM jobs WHERE id = $1 FOR UPDATE`, job.JobID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() { _, err := coordinator.Apply(ctx, command); result <- err }()
	waitForRoleDatabaseLock(t, database.Admin, "vela_attempt_coordinator_login")
	waitStageExpiry(t, job.JobExpiresAt)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("assignment acquired before Job expiry committed after the lock wait")
	}
	var physical int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM stage_attempts WHERE attempt_id = $1`, attemptID).
		Scan(&physical); err != nil || physical != 0 {
		t.Fatalf("expired assignment left physical attempts=%d error=%v", physical, err)
	}
}

func TestStageGraphFinalizationLocksParentAndRechecksClockAfterWait(t *testing.T) {
	outcome := runCPUMediaH3Graph(t)
	service, err := stagefinalization.NewService(context.Background(),
		newRolePool(t, outcome.database.DSN, "vela_internal_login", "vela-internal-password"),
		stagefinalization.Config{
			LeaseTTL: 2 * time.Second, ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{"lease-key-v1": []byte("0123456789abcdef0123456789abcdef")},
			ArtifactInspector: artifactInspectorFunc(func(_ context.Context,
				request stagefinalization.ArtifactInspectionRequest) (stagefinalization.ArtifactInspection, error) {
				return validInspectionForRequest(request), nil
			}),
			ArtifactStore: outcome.objectStore,
		})
	if err != nil {
		t.Fatal(err)
	}
	finalizer := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela/finalizer/blocked-completion"}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted {
		t.Fatalf("claim=%#v error=%v", claim, err)
	}
	tx, err := outcome.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM jobs WHERE id = $1 FOR UPDATE`, claim.JobID); err != nil {
		t.Fatal(err)
	}
	type completionOutcome struct {
		result stagefinalization.VisibleCompletionResult
		err    error
	}
	completed := make(chan completionOutcome, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		result, err := service.CompleteStageGraphVisibleCompletion(ctx, finalizer, claim.Credentials,
			stagefinalization.StageGraphVisibleCompletionCandidate{
				CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion,
			})
		completed <- completionOutcome{result, err}
	}()
	waitForRoleDatabaseLock(t, outcome.database.Admin, "vela_internal_login")
	if _, err := tx.Exec(`SELECT id FROM stage_graph_finalization_claims WHERE id = $1 FOR UPDATE NOWAIT`,
		claim.ClaimID); err != nil {
		t.Fatalf("completion locked child claim before its parent Job: %v", err)
	}
	waitStageExpiry(t, claim.ClaimExpiresAt)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	result := <-completed
	if result.err != nil || result.result.Decision != stagefinalization.VisibleCompletionRejectedStaleLease {
		t.Fatalf("completion after claim expired during parent lock wait=%#v error=%v", result.result, result.err)
	}
	assertNoStageGraphVisibleCompletionWrites(t, outcome)
}

func TestStageJobExpiryMigrationRoundTripRestoresFunctionDefinitions(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 78)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	readDefinitions := func() string {
		t.Helper()
		var definitions string
		if err := database.Admin.QueryRow(`SELECT
			pg_get_functiondef('vela_terminalize_stage_graph_failure(uuid,uuid,uuid,text)'::regprocedure)
			|| pg_get_functiondef('vela_reconcile_stage_graphs(integer)'::regprocedure)`).Scan(&definitions); err != nil {
			t.Fatal(err)
		}
		return definitions
	}
	before := readDefinitions()
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 79); err != nil {
			t.Fatal(err)
		}
		if err := goose.DownTo(database.Admin, migrations, 78); err != nil {
			t.Fatal(err)
		}
		if after := readDefinitions(); before != after {
			t.Fatal("Job Expiry Down did not restore actual schema-78 function definitions")
		}
	}
}

func seedShortStageJobLifetime(t *testing.T, database testDatabase) {
	t.Helper()
	// Publish independent short-lived fixture revisions; never rewrite the Job's
	// immutable deadline or disable the production snapshot guards.
	if _, err := database.Admin.Exec(`
		UPDATE generation_preset_revisions SET state = 'RETIRED'
		WHERE id = '00000000-0000-0000-0000-000000000011';
		INSERT INTO generation_preset_revisions (id, model_revision_id, stable_id, revision, state, certified_p95_compute_seconds)
		VALUES ('79000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000010', 'balanced', 2, 'ACTIVE', 1);
		UPDATE service_class_revisions SET state = 'RETIRED'
		WHERE id = '00000000-0000-0000-0000-000000000012';
		INSERT INTO service_class_revisions (id, stable_id, revision, state, queue_retry_allowance_seconds,
			max_attempts, max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
			retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy)
		SELECT '79000000-0000-0000-0000-000000000012', stable_id, 2, 'ACTIVE', 6,
			1, 1000, 1, retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy
		FROM service_class_revisions WHERE id = '00000000-0000-0000-0000-000000000012';
		INSERT INTO profile_certifications (id, model_revision_id, generation_preset_revision_id,
			output_spec_id, execution_profile_revision_id, state, evidence_digest, certified_at)
		VALUES ('79000000-0000-0000-0000-000000000015', '00000000-0000-0000-0000-000000000010',
			'79000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000013',
			'49000000-0000-0000-0000-000000000070', 'ACTIVE', 'short-lifetime-fixture', clock_timestamp());
		INSERT INTO rate_card_lines (id, rate_card_revision_id, model_revision_id,
			generation_preset_revision_id, service_class_revision_id, output_spec_id, unit_amount_minor, currency)
		SELECT '79000000-0000-0000-0000-000000000017', rate_card_revision_id, model_revision_id,
			'79000000-0000-0000-0000-000000000011', '79000000-0000-0000-0000-000000000012',
			output_spec_id, unit_amount_minor, currency
		FROM rate_card_lines WHERE id = '00000000-0000-0000-0000-000000000017';
	`); err != nil {
		t.Fatal(err)
	}
}

func stageJobExpiryAssignment(attemptID, runID uuid.UUID, worker h3IntegrationWorker) attemptcoordinator.AssignStageCommand {
	now := time.Now().UTC().Truncate(time.Millisecond)
	tokenDigest := sha256.Sum256(bytesOf(0xb3, 32))
	return attemptcoordinator.AssignStageCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		StageAttemptID: uuid.New(), StageAllocationID: uuid.New(), StageLeaseID: uuid.New(),
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID), CapacityPoolID: worker.poolID,
		WorkerInstanceID: worker.workerID, WorkerInstanceEpoch: 1,
		ObservationSequence: worker.evidence.Capacity.Sequence,
		DeviceSetDigest:     worker.authority.DeviceSetDigest, MembershipDigest: worker.authority.MembershipDigest,
		ModelResidencyID: worker.authority.ModelResidencyID, ModelRuntimeEpoch: 1,
		CapacityVector: worker.capacity, TokenDigest: tokenDigest[:],
		SigningKeyID: "stage-authority-key-v1", ExecutionNonce: bytesOf(0xa5, 32),
		IssuedAt: now, ExpiresAt: now.Add(time.Minute), LocalDeadlineAt: now.Add(50 * time.Second),
	}
}

func assertExpiredStageJob(t *testing.T, database testDatabase, jobID string) {
	t.Helper()
	var job, attempt, graph, credit string
	var fence, queued, running, reserved, charges, events, activeStages int64
	if err := database.Admin.QueryRow(`SELECT job.state::text, attempt.state::text, attempt.graph_state::text,
		credit.state::text, job.current_fence, project.queued_count, project.running_count, account.reserved_minor,
		(SELECT count(*) FROM charges WHERE job_id = job.id),
		(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'job.failed'),
		(SELECT count(*) FROM stage_runs WHERE attempt_id = attempt.id AND state NOT IN ('SUCCEEDED','FAILED','CANCELED'))
		FROM jobs AS job JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN credit_reservations AS credit ON credit.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN organization_credit_accounts AS account ON account.organization_id = job.organization_id
		WHERE job.id = $1`, jobID).Scan(&job, &attempt, &graph, &credit, &fence,
		&queued, &running, &reserved, &charges, &events, &activeStages); err != nil {
		t.Fatal(err)
	}
	if job != "FAILED" || attempt != "FAILED" || graph != "FAILED" || credit != "RELEASED" || fence != 2 ||
		queued != 0 || running != 0 || reserved != 0 || charges != 0 || events != 1 || activeStages != 0 {
		t.Fatalf("expired Job states=%s/%s/%s/%s fence=%d queue/run/credit/charge/event/active=%d/%d/%d/%d/%d/%d",
			job, attempt, graph, credit, fence, queued, running, reserved, charges, events, activeStages)
	}
}
