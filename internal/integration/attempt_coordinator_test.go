//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/stagecache"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	graphExecutionProfileID = "49000000-0000-0000-0000-000000000070"
	encoderWorkerProfileID  = "49300000-0000-0000-0000-000000000030"
	encoderStageProfileID   = "49300000-0000-0000-0000-000000000031"
	ditWorkerProfileID      = "49300000-0000-0000-0000-000000000032"
	ditStageProfileID       = "49300000-0000-0000-0000-000000000033"
	stageCutoverRevisionID  = "49300000-0000-0000-0000-000000000049"
	admissionCapacityBundle = "49300000-0000-0000-0000-000000000090"
)

func TestAdmissionAtomicallyInstantiatesAcceptedStageJob(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "admission-atomic-instantiation", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"atomic stage graph authority"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}
	if job.AttemptsStarted != 1 {
		t.Fatalf("Accepted Stage Job attempts_started = %d, want 1", job.AttemptsStarted)
	}

	var (
		workState        string
		completionReason string
		jobVersion       int64
		jobFence         int64
		snapshotCount    int
		attemptCount     int
		stageRunCount    int
		reservationCount int
		commandCount     int
		aggregateVersion int64
		payload          []byte
	)
	if err := database.Admin.QueryRow(`
		SELECT
			work.state::text,
			work.completion_reason,
			accepted.version,
			accepted.current_fence,
			(SELECT count(*) FROM execution_graph_snapshots
			 WHERE job_id = work.job_id),
			(SELECT count(*) FROM attempts
			 WHERE id = work.attempt_id AND job_id = work.job_id),
			(SELECT count(*) FROM stage_runs
			 WHERE attempt_id = work.attempt_id),
			(SELECT count(*) FROM stage_storage_reservations
			 WHERE id = work.storage_reservation_id
			   AND job_id = work.job_id
			   AND attempt_id = work.attempt_id),
			(SELECT count(*) FROM attempt_coordinator_commands
			 WHERE command_id = work.command_id
			   AND job_id = work.job_id
			   AND attempt_id = work.attempt_id
			   AND command_kind = 'INSTANTIATE'),
			ready.aggregate_version,
			ready.payload
		FROM stage_graph_instantiation_work AS work
		JOIN jobs AS accepted ON accepted.id = work.job_id
		JOIN outbox_events AS ready
		  ON ready.aggregate_id = work.job_id
		 AND ready.event_type = 'job.ready'
		WHERE work.job_id = $1
	`, job.JobID).Scan(
		&workState,
		&completionReason,
		&jobVersion,
		&jobFence,
		&snapshotCount,
		&attemptCount,
		&stageRunCount,
		&reservationCount,
		&commandCount,
		&aggregateVersion,
		&payload,
	); err != nil {
		t.Fatalf("read atomic Stage graph Admission authority: %v", err)
	}
	var ready velav1.EventEnvelope
	if err := proto.Unmarshal(payload, &ready); err != nil {
		t.Fatalf("decode atomic job.ready envelope: %v", err)
	}
	if workState != "COMPLETED" ||
		completionReason != "ADMISSION_TRANSACTION_INSTANTIATED" ||
		jobVersion != 2 || jobFence != 1 || snapshotCount != 1 ||
		attemptCount != 1 || stageRunCount != 3 || reservationCount != 1 ||
		commandCount != 1 || aggregateVersion != jobVersion ||
		ready.GetAggregateVersion() != uint64(jobVersion) {
		t.Fatalf(
			"atomic authority = state/reason %s/%s job %d/%d snapshot/attempt/stages/reservation/command %d/%d/%d/%d/%d outbox %d/%d",
			workState,
			completionReason,
			jobVersion,
			jobFence,
			snapshotCount,
			attemptCount,
			stageRunCount,
			reservationCount,
			commandCount,
			aggregateVersion,
			ready.GetAggregateVersion(),
		)
	}

	coordinatorPool := newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	)
	manual, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct manual AttemptCoordinator: %v", err)
	}
	_, err = manual.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.New(),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.New(),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  uuid.New(),
		StorageReservationID:       uuid.New(),
		ReservedStorageBytes:       2 << 30,
	})
	assertPostgresConstraint(t, err, "stage_graph_job_fence_stale")
	coordinator, err := attemptcoordinator.NewAutomatedService(
		coordinatorPool,
		attemptcoordinator.AutomationConfig{
			InstanceID: "attempt-coordinator-automatic-1",
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Second,
			BatchSize:  10,
		},
	)
	if err != nil {
		t.Fatalf("construct automatic AttemptCoordinator: %v", err)
	}
	result, err := coordinator.RunCycle(context.Background())
	if err != nil {
		t.Fatalf("run automatic AttemptCoordinator: %v", err)
	}
	if result.Reconciled != 0 || result.Discarded != 0 || result.Claims != 0 ||
		result.Reclaimed != 0 || result.Instantiated != 0 || result.Replayed != 0 {
		t.Fatalf("automatic cycle observed newly Accepted work = %#v", result)
	}
}

func TestAdmissionRejectsIncompleteStageCapacityPathWithoutEffects(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	if _, err := database.Admin.Exec(`
		UPDATE worker_instances
		SET reachability_state = 'UNREACHABLE'
		WHERE id = '49300000-0000-0000-0000-000000000096'
	`); err != nil {
		t.Fatalf("remove VAE READY capacity from Admission path: %v", err)
	}
	assertStageAdmissionCapacityRejectedWithoutEffects(
		t,
		database,
		"admission-incomplete-ready-path",
		"reject an incomplete Stage capacity path",
	)
}

func TestAdmissionRejectsSupersededStageCapacityObservationWithoutEffects(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	result, err := database.Admin.Exec(`
		INSERT INTO capacity_observations (
			worker_instance_id, worker_instance_epoch, observation_sequence,
			capacity_vector, observed_at, expires_at, observed_by
		)
		SELECT worker_instance_id, worker_instance_epoch,
			observation_sequence + 1,
			jsonb_set(capacity_vector, '{concurrency}', '0'::jsonb),
			clock_timestamp(), clock_timestamp() + interval '1 minute',
			'integration/admission-superseded-capacity'
		FROM capacity_observations
		WHERE worker_instance_id = '49300000-0000-0000-0000-000000000096'
		ORDER BY observation_sequence DESC
		LIMIT 1
	`)
	if err != nil {
		t.Fatalf("supersede VAE CapacityObservation: %v", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("superseded VAE CapacityObservation rows = %d error=%v", rows, rowsErr)
	}
	assertStageAdmissionCapacityRejectedWithoutEffects(
		t,
		database,
		"admission-superseded-capacity",
		"reject superseded positive Stage capacity",
	)
}

func assertStageAdmissionCapacityRejectedWithoutEffects(
	t *testing.T,
	database testDatabase,
	idempotencyKey string,
	prompt string,
) {
	t.Helper()
	server := admissionServerForDatabase(t, database)
	requestBody := fmt.Sprintf(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":%q
	}`, prompt)
	rejected := submitJob(t, server.URL, idempotencyKey, []byte(requestBody))
	if rejected.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf(
			"Stage capacity rejection status = %d, want 503; body=%s",
			rejected.StatusCode,
			rejected.Body,
		)
	}
	var responseError struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rejected.Body, &responseError); err != nil {
		t.Fatalf("decode Stage capacity rejection: %v", err)
	}
	if responseError.Code != "capacity_unavailable" ||
		rejected.Header.Get("Retry-After") == "" {
		t.Fatalf(
			"Stage capacity rejection code=%q Retry-After=%q",
			responseError.Code,
			rejected.Header.Get("Retry-After"),
		)
	}
	assertNoAdmissionEffects(t, database.Admin)
	var work, snapshots, attempts, runs, reservations, commands int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_graph_instantiation_work),
			(SELECT count(*) FROM execution_graph_snapshots),
			(SELECT count(*) FROM attempts),
			(SELECT count(*) FROM stage_runs),
			(SELECT count(*) FROM stage_storage_reservations),
			(SELECT count(*) FROM attempt_coordinator_commands)
	`).Scan(&work, &snapshots, &attempts, &runs, &reservations, &commands); err != nil {
		t.Fatalf("read Stage capacity rejection side effects: %v", err)
	}
	if work != 0 || snapshots != 0 || attempts != 0 || runs != 0 ||
		reservations != 0 || commands != 0 {
		t.Fatalf(
			"Stage capacity rejection left work/snapshot/attempt/run/reservation/command = %d/%d/%d/%d/%d/%d",
			work, snapshots, attempts, runs, reservations, commands,
		)
	}
}

func TestAdmissionStageInstantiationFailureRollsBackEveryEffect(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	if _, err := database.Admin.Exec(`
		CREATE FUNCTION vela_test_reject_admission_stage_run() RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION USING
				ERRCODE = '55000',
				CONSTRAINT = 'test_admission_stage_instantiation_failure',
				MESSAGE = 'injected StageRun insertion failure';
		END
		$$;
		CREATE TRIGGER test_reject_admission_stage_run
		BEFORE INSERT ON stage_runs
		FOR EACH ROW EXECUTE FUNCTION vela_test_reject_admission_stage_run();
	`); err != nil {
		t.Fatalf("install Admission instantiation failure injection: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	result := submitJob(t, server.URL, "admission-atomic-rollback", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"rollback every Admission effect"
	}`))
	if result.StatusCode != http.StatusInternalServerError {
		t.Fatalf("failed atomic Admission status = %d, want 500; body=%s", result.StatusCode, result.Body)
	}
	assertNoAdmissionEffects(t, database.Admin)
	var work, snapshots, attempts, runs, reservations, commands int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_graph_instantiation_work),
			(SELECT count(*) FROM execution_graph_snapshots),
			(SELECT count(*) FROM attempts),
			(SELECT count(*) FROM stage_runs),
			(SELECT count(*) FROM stage_storage_reservations),
			(SELECT count(*) FROM attempt_coordinator_commands
			 WHERE command_kind = 'INSTANTIATE')
	`).Scan(&work, &snapshots, &attempts, &runs, &reservations, &commands); err != nil {
		t.Fatalf("read rolled-back Stage graph Admission effects: %v", err)
	}
	if work != 0 || snapshots != 0 || attempts != 0 || runs != 0 ||
		reservations != 0 || commands != 0 {
		t.Fatalf(
			"failed Admission left work/snapshot/attempt/run/reservation/command = %d/%d/%d/%d/%d/%d",
			work, snapshots, attempts, runs, reservations, commands,
		)
	}
}

func TestAttemptCoordinatorAutomaticClaimIsExclusiveAcrossReplicas(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-exclusive", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"exclusive automatic claim"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}
	rewindAtomicAdmissionToPendingDispatch(t, database.Admin, job.JobID)

	newCoordinator := func(instanceID string) *attemptcoordinator.Service {
		pool := newRolePool(
			t,
			database.DSN,
			"vela_attempt_coordinator_login",
			"vela-attempt-coordinator-password",
		)
		coordinator, err := attemptcoordinator.NewAutomatedService(
			pool,
			attemptcoordinator.AutomationConfig{
				InstanceID: instanceID,
				ClaimTTL:   30 * time.Second,
				RetryDelay: time.Second,
				BatchSize:  10,
			},
		)
		if err != nil {
			t.Fatalf("construct automatic AttemptCoordinator %s: %v", instanceID, err)
		}
		return coordinator
	}
	coordinators := []*attemptcoordinator.Service{
		newCoordinator("attempt-coordinator-replica-a"),
		newCoordinator("attempt-coordinator-replica-b"),
	}
	results := make([]attemptcoordinator.AutomationResult, len(coordinators))
	errorsByReplica := make([]error, len(coordinators))
	ready := make(chan struct{})
	var workers sync.WaitGroup
	for index, coordinator := range coordinators {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-ready
			results[index], errorsByReplica[index] = coordinator.RunCycle(context.Background())
		}()
	}
	close(ready)
	workers.Wait()

	claimed := 0
	instantiated := 0
	for index, err := range errorsByReplica {
		if err != nil {
			t.Fatalf("replica %d automatic cycle: %v", index, err)
		}
		claimed += results[index].Claims
		instantiated += results[index].Instantiated + results[index].Replayed
	}
	if claimed != 1 || instantiated != 1 {
		t.Fatalf("replica results = %#v", results)
	}
	var workCount, attemptCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_graph_instantiation_work WHERE job_id = $1),
			(SELECT count(*) FROM attempts WHERE job_id = $1)
	`, job.JobID).Scan(&workCount, &attemptCount); err != nil {
		t.Fatalf("read exclusive automatic authority: %v", err)
	}
	if workCount != 1 || attemptCount != 1 {
		t.Fatalf("exclusive automatic counts work=%d attempts=%d", workCount, attemptCount)
	}
}

func TestAttemptCoordinatorExpiredClaimKeepsStableInstantiationIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-expired-claim", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"expired automatic claim"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}
	rewindAtomicAdmissionToPendingDispatch(t, database.Admin, job.JobID)

	claimToken := uuid.New()
	claimed := claimStageGraphInstantiation(
		t,
		database.Admin,
		"attempt-coordinator-crashed",
		claimToken,
	)
	if claimed.JobID.String() != job.JobID {
		t.Fatalf("claimed Job = %s, want %s", claimed.JobID, job.JobID)
	}
	if _, err := database.Admin.Exec(`
		UPDATE stage_graph_instantiation_work
		SET claimed_at = clock_timestamp() - interval '2 seconds',
			claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE job_id = $1 AND claim_token = $2
	`, claimed.JobID, claimToken); err != nil {
		t.Fatalf("expire crashed automatic claim: %v", err)
	}

	coordinatorPool := newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewAutomatedService(
		coordinatorPool,
		attemptcoordinator.AutomationConfig{
			InstanceID: "attempt-coordinator-takeover",
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Second,
			BatchSize:  10,
		},
	)
	if err != nil {
		t.Fatalf("construct takeover AttemptCoordinator: %v", err)
	}
	result, err := coordinator.RunCycle(context.Background())
	if err != nil {
		t.Fatalf("run takeover AttemptCoordinator: %v", err)
	}
	if result.Claims != 1 || result.Reclaimed != 1 || result.Instantiated != 1 {
		t.Fatalf("takeover result = %#v", result)
	}
	var commandID, snapshotID, attemptID, reservationID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT command_id, execution_graph_snapshot_id, attempt_id, storage_reservation_id
		FROM stage_graph_instantiation_work
		WHERE job_id = $1 AND state = 'COMPLETED'
	`, claimed.JobID).Scan(&commandID, &snapshotID, &attemptID, &reservationID); err != nil {
		t.Fatalf("read takeover authority: %v", err)
	}
	if commandID != claimed.CommandID || snapshotID != claimed.ExecutionGraphSnapshotID ||
		attemptID != claimed.AttemptID || reservationID != claimed.StorageReservationID {
		t.Fatalf(
			"takeover identities command=%s snapshot=%s attempt=%s reservation=%s, claimed=%#v",
			commandID,
			snapshotID,
			attemptID,
			reservationID,
			claimed,
		)
	}
}

func TestAttemptCoordinatorReconcilesCommittedInstantiationAfterCrash(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-commit-crash", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"committed automatic claim"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}
	rewindAtomicAdmissionToPendingDispatch(t, database.Admin, job.JobID)

	claim := claimStageGraphInstantiation(
		t,
		database.Admin,
		"attempt-coordinator-before-commit-crash",
		uuid.New(),
	)
	coordinatorPool := newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	)
	manual, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct manual AttemptCoordinator: %v", err)
	}
	if _, err := manual.Instantiate(context.Background(), claim.InstantiateCommand); err != nil {
		t.Fatalf("commit instantiation before simulated crash: %v", err)
	}
	automatic, err := attemptcoordinator.NewAutomatedService(
		coordinatorPool,
		attemptcoordinator.AutomationConfig{
			InstanceID: "attempt-coordinator-after-commit-crash",
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Second,
			BatchSize:  10,
		},
	)
	if err != nil {
		t.Fatalf("construct recovering AttemptCoordinator: %v", err)
	}
	result, err := automatic.RunCycle(context.Background())
	if err != nil {
		t.Fatalf("reconcile committed instantiation: %v", err)
	}
	if result.Reconciled != 1 || result.Claims != 0 || result.Instantiated != 0 {
		t.Fatalf("committed crash reconciliation = %#v", result)
	}
	var state string
	if err := database.Admin.QueryRow(`
		SELECT state::text FROM stage_graph_instantiation_work WHERE job_id = $1
	`, job.JobID).Scan(&state); err != nil {
		t.Fatalf("read reconciled work: %v", err)
	}
	if state != "COMPLETED" {
		t.Fatalf("reconciled work state = %s", state)
	}
}

type claimedStageGraphInstantiation struct {
	attemptcoordinator.InstantiateCommand
}

func claimStageGraphInstantiation(
	t *testing.T,
	database *sql.DB,
	instanceID string,
	claimToken uuid.UUID,
) claimedStageGraphInstantiation {
	t.Helper()
	var claim claimedStageGraphInstantiation
	var reclaimed bool
	err := database.QueryRow(`
		SELECT
			job_id,
			command_id,
			expected_job_version,
			expected_job_fence,
			execution_graph_snapshot_id,
			execution_graph_revision_id,
			execution_profile_revision_id,
			attempt_id,
			storage_reservation_id,
			reserved_storage_bytes,
			reclaimed
		FROM vela_claim_stage_graph_instantiations($1, $2, 30, 1)
	`, instanceID, claimToken).Scan(
		&claim.JobID,
		&claim.CommandID,
		&claim.ExpectedJobVersion,
		&claim.ExpectedJobFence,
		&claim.ExecutionGraphSnapshotID,
		&claim.ExecutionGraphRevisionID,
		&claim.ExecutionProfileRevisionID,
		&claim.AttemptID,
		&claim.StorageReservationID,
		&claim.ReservedStorageBytes,
		&reclaimed,
	)
	if err != nil {
		t.Fatalf("claim automatic Stage graph instantiation: %v", err)
	}
	if reclaimed {
		t.Fatal("initial automatic claim was reported as reclaimed")
	}
	return claim
}

func readStageGraphInstantiation(
	t *testing.T,
	database *sql.DB,
	jobID string,
) claimedStageGraphInstantiation {
	t.Helper()
	var command claimedStageGraphInstantiation
	if err := database.QueryRow(`
		SELECT
			job_id,
			command_id,
			expected_job_version,
			expected_job_fence,
			execution_graph_snapshot_id,
			execution_graph_revision_id,
			execution_profile_revision_id,
			attempt_id,
			storage_reservation_id,
			reserved_storage_bytes
		FROM stage_graph_instantiation_work
		WHERE job_id = $1 AND state = 'COMPLETED'
	`, jobID).Scan(
		&command.JobID,
		&command.CommandID,
		&command.ExpectedJobVersion,
		&command.ExpectedJobFence,
		&command.ExecutionGraphSnapshotID,
		&command.ExecutionGraphRevisionID,
		&command.ExecutionProfileRevisionID,
		&command.AttemptID,
		&command.StorageReservationID,
		&command.ReservedStorageBytes,
	); err != nil {
		t.Fatalf("read completed Admission Stage graph instantiation: %v", err)
	}
	return command
}

func rewindAtomicAdmissionToPendingDispatch(t *testing.T, database *sql.DB, jobID string) {
	t.Helper()
	tx, err := database.Begin()
	if err != nil {
		t.Fatalf("begin historical pending-instantiation fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatalf("disable constraint triggers for historical pending-instantiation fixture: %v", err)
	}
	statements := []string{
		`DELETE FROM stage_dependencies
		WHERE attempt_id = (
			SELECT attempt_id FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`DELETE FROM stage_retry_budgets
		WHERE stage_run_id IN (
			SELECT id FROM stage_runs
			WHERE attempt_id = (
				SELECT attempt_id FROM stage_graph_instantiation_work WHERE job_id = $1
			)
		)`,
		`DELETE FROM stage_runs
		WHERE attempt_id = (
			SELECT attempt_id FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`DELETE FROM stage_storage_reservations
		WHERE attempt_id = (
			SELECT attempt_id FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`DELETE FROM attempt_retry_budgets
		WHERE attempt_id = (
			SELECT attempt_id FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`DELETE FROM attempt_coordinator_commands
		WHERE command_id = (
			SELECT command_id FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`DELETE FROM non_content_attempt_roots
		WHERE id = (
			SELECT attempt_id FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`DELETE FROM attempts
		WHERE id = (
			SELECT attempt_id FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`DELETE FROM execution_graph_snapshots
		WHERE id = (
			SELECT execution_graph_snapshot_id
			FROM stage_graph_instantiation_work WHERE job_id = $1
		)`,
		`UPDATE retry_runtime_states
		SET attempts_started = 0, version = 1, updated_at = transaction_timestamp()
		WHERE job_id = $1`,
		`UPDATE jobs
		SET version = 1, current_fence = 0, updated_at = transaction_timestamp()
		WHERE id = $1`,
		`UPDATE stage_graph_instantiation_work
		SET state = 'PENDING', available_at = clock_timestamp(),
			claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
			claim_expires_at = NULL, claim_count = 0, last_error = NULL,
			completed_at = NULL, completion_reason = NULL,
			updated_at = clock_timestamp()
		WHERE job_id = $1`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement, jobID); err != nil {
			t.Fatalf("rewind atomic Admission to migration-50 pending work: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit historical pending-instantiation fixture: %v", err)
	}
}

func insertPreDispatchStageJob(t *testing.T, database *sql.DB, fixtureName string) jobResponse {
	t.Helper()
	tx, job := beginPreDispatchStageJob(t, database, fixtureName)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit pre-dispatch Stage Job fixture: %v", err)
	}
	return job
}

func beginPreDispatchStageJob(
	t *testing.T,
	database *sql.DB,
	fixtureName string,
) (*sql.Tx, jobResponse) {
	t.Helper()
	jobID := uuid.New()
	reservationID := uuid.New()
	tx, err := database.Begin()
	if err != nil {
		t.Fatalf("begin pre-dispatch Stage Job fixture: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id,
			model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id,
			execution_authority_kind, stage_cutover_revision_id,
			execution_graph_revision_id, stage_execution_profile_revision_id,
			worker_pool_id, request_hash, request_content,
			request_content_expires_at, retention_policy_revision_id,
			retention_artifact_days, retention_request_content_days,
			retention_incomplete_content_hours, retention_scratch_hours,
			retention_debug_hours, retention_metadata_days,
			retention_financial_days, pricing_rate_card_revision_id,
			pricing_rate_line_id, pricing_unit_amount_minor, pricing_quantity,
			pricing_quoted_amount_minor, pricing_currency,
			execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt,
			execution_retry_backoff_policy, execution_retryable_failure_classes,
			execution_circuit_breaker_policy,
			execution_circuit_fingerprint_window_seconds,
			execution_circuit_min_distinct_healthy_workers, job_expires_at
		)
		SELECT
				$1::uuid, project.organization_id, project.id, $2,
			'00000000-0000-0000-0000-000000000010'::uuid,
			'00000000-0000-0000-0000-000000000011'::uuid,
			service.id,
			'00000000-0000-0000-0000-000000000013'::uuid,
			'STAGE_GRAPH', $3,
			$4, $5, NULL,
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
			'00000000-0000-0000-0000-000000000016'::uuid,
			'00000000-0000-0000-0000-000000000017'::uuid,
			1250, 1, 1250, 'CNY',
			service.max_attempts,
			2400,
			service.max_finalization_seconds_per_attempt,
			service.retry_backoff_policy,
			service.retryable_failure_classes,
			service.circuit_breaker_policy,
			service.circuit_fingerprint_window_seconds,
			service.circuit_min_distinct_healthy_workers,
			transaction_timestamp() + interval '1 day'
		FROM projects AS project
		JOIN service_class_revisions AS service
		  ON service.id = '00000000-0000-0000-0000-000000000012'
		WHERE project.organization_id = $7
		  AND project.id = $8
	`,
		jobID,
		testPrincipalID,
		stageCutoverRevisionID,
		stageGraphID,
		graphExecutionProfileID,
		fixtureName,
		testOrganizationID,
		testProjectID,
	); err != nil {
		t.Fatalf("insert pre-dispatch Stage Job fixture: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO credit_reservations (
			id, organization_id, project_id, job_id, amount_minor, currency
		) VALUES ($1, $2, $3, $4, 1250, 'CNY')
	`, reservationID, testOrganizationID, testProjectID, jobID); err != nil {
		t.Fatalf("insert pre-dispatch CreditReservation fixture: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
		VALUES ($1, $2, $3)
	`, jobID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("insert pre-dispatch retry runtime state fixture: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE organization_credit_accounts
		SET reserved_minor = reserved_minor + 1250,
			version = version + 1,
			updated_at = clock_timestamp()
		WHERE organization_id = $1
	`, testOrganizationID); err != nil {
		t.Fatalf("reserve pre-dispatch Job credit fixture: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE projects
		SET queued_count = queued_count + 1
		WHERE organization_id = $1 AND id = $2
	`, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("increment pre-dispatch Project queue fixture: %v", err)
	}
	return tx, jobResponse{JobID: jobID.String(), ProjectID: testProjectID, State: "QUEUED"}
}

func TestAttemptCoordinatorInstantiateIsDurableAndIdempotent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-instantiate", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"stage graph authority"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}
	rewindAtomicAdmissionToPendingDispatch(t, database.Admin, job.JobID)

	coordinatorPool := newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct AttemptCoordinator: %v", err)
	}
	command := claimStageGraphInstantiation(
		t, database.Admin, "attempt-coordinator-idempotent", uuid.New(),
	).InstantiateCommand
	first, err := coordinator.Instantiate(context.Background(), command)
	if err != nil {
		t.Fatalf("instantiate Stage graph: %v", err)
	}
	replay, err := coordinator.Instantiate(context.Background(), command)
	if err != nil {
		t.Fatalf("replay Stage graph instantiation: %v", err)
	}
	if first.AttemptID != command.AttemptID || first.SnapshotID != command.ExecutionGraphSnapshotID ||
		first.AttemptFence != 1 || first.StageRunCount != 3 || first.Replayed {
		t.Fatalf("first AttemptHandle = %#v", first)
	}
	if replay.AttemptID != first.AttemptID || replay.SnapshotID != first.SnapshotID ||
		replay.AttemptFence != first.AttemptFence || replay.StageRunCount != 3 || !replay.Replayed {
		t.Fatalf("replayed AttemptHandle = %#v, first=%#v", replay, first)
	}

	rows, err := database.Admin.Query(`
		SELECT stage_key, state::text
		FROM stage_runs
		WHERE attempt_id = $1
		ORDER BY stage_key
	`, command.AttemptID)
	if err != nil {
		t.Fatalf("read instantiated StageRuns: %v", err)
	}
	defer rows.Close()
	states := map[string]string{}
	for rows.Next() {
		var key, state string
		if err := rows.Scan(&key, &state); err != nil {
			t.Fatalf("scan StageRun: %v", err)
		}
		states[key] = state
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate StageRuns: %v", err)
	}
	if states["encoder"] != "READY" || states["dit"] != "BLOCKED" || states["vae"] != "BLOCKED" {
		t.Fatalf("instantiated StageRun states = %#v", states)
	}

	var snapshotCount, attemptCount, dependencyCount, reservationCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM execution_graph_snapshots WHERE id = $1),
			(SELECT count(*) FROM attempts WHERE id = $2),
			(SELECT count(*) FROM stage_dependencies WHERE attempt_id = $2),
			(SELECT count(*) FROM stage_storage_reservations WHERE attempt_id = $2)
	`, command.ExecutionGraphSnapshotID, command.AttemptID).Scan(
		&snapshotCount, &attemptCount, &dependencyCount, &reservationCount,
	); err != nil {
		t.Fatalf("read graph authority counts: %v", err)
	}
	if snapshotCount != 1 || attemptCount != 1 || dependencyCount != 2 || reservationCount != 1 {
		t.Fatalf(
			"graph authority counts snapshot=%d attempt=%d dependencies=%d reservation=%d",
			snapshotCount, attemptCount, dependencyCount, reservationCount,
		)
	}
}

func TestAttemptCoordinatorPhysicalStageLifecycleIsIdempotent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	activateH3StageGraph(t, database)
	seedWorkerRegistryPlan(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-assign", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"assign encoder stage"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct AttemptCoordinator: %v", err)
	}
	claim := readStageGraphInstantiation(t, database.Admin, job.JobID)
	var encoderRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, claim.AttemptID).Scan(&encoderRunID); err != nil {
		t.Fatalf("read Encoder StageRun: %v", err)
	}

	const encoderPoolID = "49300000-0000-0000-0000-000000000020"
	workerID := uuid.MustParse("49300000-0000-0000-0000-000000000021")
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, 'h3-encoder-shanghai', $2, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
	`, encoderPoolID, encoderStageProfileID); err != nil {
		t.Fatalf("seed Encoder CapacityPool: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, workerID, encoderWorkerProfileID, encoderPoolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed Encoder WorkerInstance: %v", err)
	}
	evidence := workerRegistryEvidenceValue(t, workerID, 0x31)
	evidence.Residencies[0].ModelComponentRevision = "h3-encoder-v1"
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	registry, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct Worker Registry: %v", err)
	}
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe Encoder WorkerInstance: %v", err)
	}
	authority := workerAuthority(t, evidence)
	now := time.Now().UTC().Truncate(time.Millisecond)
	assign := attemptcoordinator.AssignStageCommand{
		CommandID:              uuid.MustParse("49300000-0000-0000-0000-000000000022"),
		AttemptID:              claim.AttemptID,
		StageRunID:             encoderRunID,
		ExpectedAttemptFence:   1,
		ExpectedStageFence:     1,
		ExpectedStageVersion:   1,
		StageAttemptID:         uuid.MustParse("49300000-0000-0000-0000-000000000023"),
		StageAllocationID:      uuid.MustParse("49300000-0000-0000-0000-000000000024"),
		StageLeaseID:           uuid.MustParse("49300000-0000-0000-0000-000000000025"),
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		CapacityPoolID:         uuid.MustParse(encoderPoolID),
		WorkerInstanceID:       workerID,
		WorkerInstanceEpoch:    1,
		ObservationSequence:    evidence.Capacity.Sequence,
		DeviceSetDigest:        authority.DeviceSetDigest,
		MembershipDigest:       authority.MembershipDigest,
		ModelResidencyID:       authority.ModelResidencyID,
		ModelRuntimeEpoch:      1,
		CapacityVector:         map[string]int64{"concurrency": 1},
		TokenDigest:            bytesOf(0x71, 32),
		SigningKeyID:           "stage-authority-key-v1",
		ExecutionNonce:         bytesOf(0x72, 32),
		IssuedAt:               now,
		ExpiresAt:              now.Add(time.Minute),
		LocalDeadlineAt:        now.Add(50 * time.Second),
	}
	first, err := coordinator.Apply(context.Background(), assign)
	if err != nil {
		t.Fatalf("assign Encoder StageRun: %v", err)
	}
	replay, err := coordinator.Apply(context.Background(), assign)
	if err != nil {
		t.Fatalf("replay Encoder assignment: %v", err)
	}
	if first.StageAttemptID != assign.StageAttemptID || first.State != "ASSIGNED" ||
		first.StageFence != 1 || first.StageVersion != 2 || first.Replayed {
		t.Fatalf("first StageDecision = %#v", first)
	}
	if replay.StageAttemptID != first.StageAttemptID || replay.State != first.State ||
		replay.StageVersion != first.StageVersion || !replay.Replayed {
		t.Fatalf("replayed StageDecision = %#v, first=%#v", replay, first)
	}
	var activeAttempts int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM stage_attempts
		WHERE stage_run_id = $1 AND state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED')
	`, encoderRunID).Scan(&activeAttempts); err != nil {
		t.Fatalf("count active StageAttempts: %v", err)
	}
	if activeAttempts != 1 {
		t.Fatalf("active StageAttempt count = %d, want 1", activeAttempts)
	}

	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin active StageAttempt invariant transaction: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		t.Fatalf("assume AttemptCoordinator owner role: %v", err)
	}
	_, err = tx.Exec(`
		INSERT INTO stage_attempts (
			id, organization_id, project_id, attempt_id, stage_run_id,
			physical_attempt_number, state, selected_stage_profile_revision_id, assigned_at
		) VALUES (
			$1, $2, $3, $4, $5, 2, 'ASSIGNED', $6, clock_timestamp()
		)
	`, uuid.New(), testOrganizationID, testProjectID, assign.AttemptID,
		encoderRunID, assign.StageProfileRevisionID)
	assertPostgresConstraint(t, err, "stage_attempts_one_active_per_stage_run_idx")
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback active StageAttempt invariant transaction: %v", err)
	}

	for _, test := range []struct {
		name       string
		statement  string
		constraint string
		target     uuid.UUID
		code       string
		message    string
	}{
		{
			name: "Graph Attempt fence",
			statement: `
				UPDATE attempts
				SET fence = fence + 1
				WHERE id = $1
			`,
			target:  assign.AttemptID,
			code:    "P0001",
			message: "immutable Attempt identity fields cannot be changed",
		},
		{
			name: "Graph Attempt state transition",
			statement: `
				UPDATE attempts
				SET state = 'FINALIZING', graph_state = 'FINALIZING'
				WHERE id = $1
			`,
			target:  assign.AttemptID,
			code:    "23514",
			message: "invalid Attempt state transition",
		},
		{
			name: "StageRun state transition",
			statement: `
				UPDATE stage_runs
				SET state = 'BLOCKED', version = version + 1
				WHERE id = $1
			`,
			constraint: "stage_run_transition_invalid",
			target:     encoderRunID,
		},
		{
			name: "StageAttempt identity",
			statement: `
				UPDATE stage_attempts
				SET physical_attempt_number = physical_attempt_number + 1
				WHERE id = $1
			`,
			constraint: "stage_attempt_identity_immutable",
			target:     assign.StageAttemptID,
		},
		{
			name: "StageAllocation identity",
			statement: `
				UPDATE stage_allocations
				SET model_runtime_epoch = model_runtime_epoch + 1
				WHERE stage_attempt_id = $1
			`,
			constraint: "stage_allocation_identity_immutable",
			target:     assign.StageAttemptID,
		},
		{
			name: "StageLease identity",
			statement: `
				UPDATE stage_leases
				SET token_digest = decode(repeat('ab', 32), 'hex')
				WHERE stage_attempt_id = $1
			`,
			constraint: "stage_lease_identity_immutable",
			target:     assign.StageAttemptID,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := database.Admin.Begin()
			if err != nil {
				t.Fatalf("begin rejected authority mutation: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
				t.Fatalf("assume AttemptCoordinator owner role: %v", err)
			}
			_, err = tx.Exec(test.statement, test.target)
			if test.constraint != "" {
				assertPostgresConstraint(t, err, test.constraint)
				return
			}
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != test.code ||
				!strings.Contains(postgresError.Message, test.message) {
				t.Fatalf(
					"PostgreSQL error = %v, want SQLSTATE %s containing %q",
					err, test.code, test.message,
				)
			}
		})
	}

	start := attemptcoordinator.StartStageCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000026"),
		AttemptID:            assign.AttemptID,
		StageRunID:           encoderRunID,
		StageAttemptID:       assign.StageAttemptID,
		StageLeaseID:         assign.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 2,
		StartedAt:            now.Add(time.Second),
	}
	started, err := coordinator.Apply(context.Background(), start)
	if err != nil {
		t.Fatalf("start Encoder StageAttempt: %v", err)
	}
	startReplay, err := coordinator.Apply(context.Background(), start)
	if err != nil {
		t.Fatalf("replay Encoder start: %v", err)
	}
	if started.StageAttemptID != assign.StageAttemptID || started.State != "RUNNING" ||
		started.StageFence != 1 || started.StageVersion != 3 || started.Replayed {
		t.Fatalf("first Start StageDecision = %#v", started)
	}
	if startReplay.StageAttemptID != started.StageAttemptID ||
		startReplay.StageVersion != started.StageVersion || !startReplay.Replayed {
		t.Fatalf("replayed Start StageDecision = %#v, first=%#v", startReplay, started)
	}
	var jobState, graphState, runState string
	var billableStartedAt, attemptStartedAt time.Time
	var startCommandCount int
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.billable_started_at,
		       attempt.graph_state::text, attempt.started_at,
		       run.state::text,
		       (SELECT count(*) FROM attempt_coordinator_commands
		        WHERE attempt_id = attempt.id AND command_kind = 'START')
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN stage_runs AS run ON run.attempt_id = attempt.id AND run.id = $2
		WHERE job.id = $1
	`, job.JobID, encoderRunID).Scan(
		&jobState, &billableStartedAt, &graphState, &attemptStartedAt,
		&runState, &startCommandCount,
	); err != nil {
		t.Fatalf("read first-progress authority: %v", err)
	}
	if jobState != "RUNNING" || graphState != "RUNNING" || runState != "RUNNING" ||
		!billableStartedAt.Equal(start.StartedAt) || !attemptStartedAt.Equal(start.StartedAt) ||
		startCommandCount != 1 {
		t.Fatalf(
			"first progress job=%s billable=%s graph=%s attemptStart=%s run=%s commands=%d",
			jobState, billableStartedAt, graphState, attemptStartedAt, runState, startCommandCount,
		)
	}

	complete := attemptcoordinator.CompleteStageCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000027"),
		AttemptID:            assign.AttemptID,
		StageRunID:           encoderRunID,
		StageAttemptID:       assign.StageAttemptID,
		StageLeaseID:         assign.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 3,
		ProgressReceiptID:    uuid.MustParse("49300000-0000-0000-0000-000000000028"),
		OutputIdentity:       "stage-output/encoder/exact-version-1",
		OutputDigest:         bytesOf(0x79, 32),
		CompletedAt:          now.Add(2 * time.Second),
	}
	completed, err := coordinator.Apply(context.Background(), complete)
	if err != nil {
		t.Fatalf("complete Encoder StageAttempt: %v", err)
	}
	completeReplay, err := coordinator.Apply(context.Background(), complete)
	if err != nil {
		t.Fatalf("replay Encoder completion: %v", err)
	}
	if completed.StageAttemptID != assign.StageAttemptID || completed.State != "SUCCEEDED" ||
		completed.StageFence != 1 || completed.StageVersion != 4 || completed.Replayed {
		t.Fatalf("first Complete StageDecision = %#v", completed)
	}
	if completeReplay.StageAttemptID != completed.StageAttemptID ||
		completeReplay.StageVersion != completed.StageVersion || !completeReplay.Replayed {
		t.Fatalf("replayed Complete StageDecision = %#v, first=%#v", completeReplay, completed)
	}
	var encoderState, ditState, physicalState, allocationState, leaseState, progressKind string
	var winnerAttemptID, winnerOutputID, dependencyReceipt uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT encoder.state::text, dit.state::text, physical.state::text,
		       allocation.state::text, lease.state::text, receipt.progress_kind::text,
		       encoder.winner_stage_attempt_id, encoder.winner_output_identity,
		       dependency.satisfied_progress_receipt_id
		FROM stage_runs AS encoder
		JOIN stage_runs AS dit ON dit.attempt_id = encoder.attempt_id AND dit.stage_key = 'dit'
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.stage_attempt_id = physical.id
		JOIN stage_leases AS lease ON lease.stage_attempt_id = physical.id
		JOIN stage_progress_receipts AS receipt ON receipt.stage_run_id = encoder.id
		JOIN stage_dependencies AS dependency
		  ON dependency.source_stage_run_id = encoder.id
		 AND dependency.destination_stage_run_id = dit.id
		WHERE encoder.id = $1
	`, encoderRunID, assign.StageAttemptID).Scan(
		&encoderState, &ditState, &physicalState, &allocationState, &leaseState,
		&progressKind, &winnerAttemptID, &winnerOutputID, &dependencyReceipt,
	); err != nil {
		t.Fatalf("read physical Stage completion: %v", err)
	}
	if encoderState != "SUCCEEDED" || ditState != "READY" || physicalState != "SUCCEEDED" ||
		allocationState != "RELEASED" || leaseState != "REVOKED" ||
		progressKind != "PHYSICAL_OUTPUT" || winnerAttemptID != assign.StageAttemptID ||
		winnerOutputID != complete.ProgressReceiptID || dependencyReceipt != complete.ProgressReceiptID {
		t.Fatalf(
			"physical completion encoder=%s dit=%s attempt=%s allocation=%s lease=%s kind=%s winners=%s/%s dependency=%s",
			encoderState, ditState, physicalState, allocationState, leaseState,
			progressKind, winnerAttemptID, winnerOutputID, dependencyReceipt,
		)
	}
}

func TestAttemptCoordinatorCacheProgressAndStageRetryPreserveUpstreamIdentityWhenCrossJobCacheDisabled(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedDiTAssignmentProfile(t, database)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-cache-advance", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"exact cached conditioning"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit cache graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode cache graph Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct AttemptCoordinator: %v", err)
	}
	cache, err := stagecache.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct disabled cross-Job Stage Cache repository: %v", err)
	}
	if _, err := cache.SetProjectControl(
		context.Background(),
		stagecache.ProjectControlCommand{
			OrganizationID:        uuid.MustParse(testOrganizationID),
			ProjectID:             uuid.MustParse(testProjectID),
			CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID),
			Enabled:               false, MaxEntries: 100, MaxBytes: 1 << 30,
			UpdatedAt: time.Now().UTC(),
		},
	); err != nil {
		t.Fatalf("disable cross-Job Stage Cache for retry fixture: %v", err)
	}
	claim := readStageGraphInstantiation(t, database.Admin, job.JobID)
	attemptID := claim.AttemptID
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read cache fixture StageRuns: %v", err)
	}
	advancedAt := time.Now().UTC().Truncate(time.Millisecond)
	advance := attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000045"),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 1,
		ProgressReceiptID:    uuid.MustParse("49300000-0000-0000-0000-000000000046"),
		CacheSourceIdentity:  "shadow-cache-entry/encoder/exact-v1",
		OutputDigest:         bytesOf(0x81, 32),
		AdvancedAt:           advancedAt,
	}
	first, err := coordinator.Apply(context.Background(), advance)
	if err != nil {
		t.Fatalf("advance exact cache hit: %v", err)
	}
	replay, err := coordinator.Apply(context.Background(), advance)
	if err != nil {
		t.Fatalf("replay exact cache advance: %v", err)
	}
	if first.StageRunID != encoderRunID || first.StageAttemptID != uuid.Nil ||
		first.State != "SUCCEEDED" || first.StageFence != 1 || first.StageVersion != 2 ||
		first.Replayed {
		t.Fatalf("first exact-cache StageDecision = %#v", first)
	}
	if replay.StageRunID != first.StageRunID || replay.StageAttemptID != uuid.Nil ||
		replay.StageVersion != first.StageVersion || !replay.Replayed {
		t.Fatalf("replayed exact-cache StageDecision = %#v, first=%#v", replay, first)
	}
	var jobState, graphState, encoderState, ditState, progressKind string
	var billableStartedAt time.Time
	var dependencyReceipt uuid.UUID
	var progressCount int
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.billable_started_at, attempt.graph_state::text,
		       encoder.state::text, dit.state::text, receipt.progress_kind::text,
		       dependency.satisfied_progress_receipt_id,
		       (SELECT count(*) FROM stage_progress_receipts WHERE stage_run_id = encoder.id)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN stage_runs AS encoder ON encoder.attempt_id = attempt.id AND encoder.id = $2
		JOIN stage_runs AS dit ON dit.attempt_id = attempt.id AND dit.id = $3
		JOIN stage_progress_receipts AS receipt ON receipt.stage_run_id = encoder.id
		JOIN stage_dependencies AS dependency
		  ON dependency.source_stage_run_id = encoder.id
		 AND dependency.destination_stage_run_id = dit.id
		WHERE job.id = $1
	`, job.JobID, encoderRunID, ditRunID).Scan(
		&jobState, &billableStartedAt, &graphState, &encoderState, &ditState,
		&progressKind, &dependencyReceipt, &progressCount,
	); err != nil {
		t.Fatalf("read exact cache graph advancement: %v", err)
	}
	if jobState != "RUNNING" || graphState != "RUNNING" || encoderState != "SUCCEEDED" ||
		ditState != "READY" || progressKind != "EXACT_CACHE" ||
		dependencyReceipt != advance.ProgressReceiptID || progressCount != 1 ||
		!billableStartedAt.Equal(advancedAt) {
		t.Fatalf(
			"cache advance job=%s billable=%s graph=%s encoder=%s dit=%s kind=%s receipt=%s count=%d",
			jobState, billableStartedAt, graphState, encoderState, ditState,
			progressKind, dependencyReceipt, progressCount,
		)
	}

	seedWorkerRegistryPlan(t, database.Admin)
	const ditPoolID = "49300000-0000-0000-0000-000000000050"
	ditWorkerID := uuid.MustParse("49300000-0000-0000-0000-000000000051")
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, 'h3-dit-retry-shanghai', $2, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
	`, ditPoolID, ditStageProfileID); err != nil {
		t.Fatalf("seed DiT retry CapacityPool: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, ditWorkerID, ditWorkerProfileID, ditPoolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed DiT retry WorkerInstance: %v", err)
	}
	ditEvidence := workerRegistryEvidenceValue(t, ditWorkerID, 0x41)
	ditEvidence.Residencies[0].ModelComponentRevision = "h3-dit-v1"
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	registry, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct Worker Registry for DiT retry: %v", err)
	}
	if _, err := registry.Observe(context.Background(), ditEvidence); err != nil {
		t.Fatalf("observe DiT retry WorkerInstance: %v", err)
	}
	ditAuthority := workerAuthority(t, ditEvidence)
	assign := attemptcoordinator.AssignStageCommand{
		CommandID:              uuid.MustParse("49300000-0000-0000-0000-000000000052"),
		AttemptID:              attemptID,
		StageRunID:             ditRunID,
		ExpectedAttemptFence:   1,
		ExpectedStageFence:     1,
		ExpectedStageVersion:   2,
		StageAttemptID:         uuid.MustParse("49300000-0000-0000-0000-000000000053"),
		StageAllocationID:      uuid.MustParse("49300000-0000-0000-0000-000000000054"),
		StageLeaseID:           uuid.MustParse("49300000-0000-0000-0000-000000000055"),
		StageProfileRevisionID: uuid.MustParse(ditStageProfileID),
		CapacityPoolID:         uuid.MustParse(ditPoolID),
		WorkerInstanceID:       ditWorkerID,
		WorkerInstanceEpoch:    1,
		ObservationSequence:    ditEvidence.Capacity.Sequence,
		DeviceSetDigest:        ditAuthority.DeviceSetDigest,
		MembershipDigest:       ditAuthority.MembershipDigest,
		ModelResidencyID:       ditAuthority.ModelResidencyID,
		ModelRuntimeEpoch:      1,
		CapacityVector:         map[string]int64{"concurrency": 1},
		TokenDigest:            bytesOf(0x82, 32),
		SigningKeyID:           "stage-authority-key-v1",
		ExecutionNonce:         bytesOf(0x83, 32),
		IssuedAt:               advancedAt.Add(time.Millisecond),
		ExpiresAt:              advancedAt.Add(time.Minute),
		LocalDeadlineAt:        advancedAt.Add(50 * time.Second),
	}
	assigned, err := coordinator.Apply(context.Background(), assign)
	if err != nil {
		t.Fatalf("assign DiT retry StageRun: %v", err)
	}
	if assigned.StageVersion != 3 {
		t.Fatalf("assigned DiT StageDecision = %#v", assigned)
	}
	start := attemptcoordinator.StartStageCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000056"),
		AttemptID:            attemptID,
		StageRunID:           ditRunID,
		StageAttemptID:       assign.StageAttemptID,
		StageLeaseID:         assign.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 3,
		StartedAt:            advancedAt.Add(2 * time.Millisecond),
	}
	started, err := coordinator.Apply(context.Background(), start)
	if err != nil {
		t.Fatalf("start DiT retry StageAttempt: %v", err)
	}
	if started.StageVersion != 4 {
		t.Fatalf("started DiT StageDecision = %#v", started)
	}
	fail := attemptcoordinator.FailStageCommand{
		CommandID:             uuid.MustParse("49300000-0000-0000-0000-000000000057"),
		AttemptID:             attemptID,
		StageRunID:            ditRunID,
		StageAttemptID:        assign.StageAttemptID,
		StageLeaseID:          assign.StageLeaseID,
		ExpectedAttemptFence:  1,
		ExpectedStageFence:    1,
		ExpectedStageVersion:  4,
		FailureClass:          "TRANSIENT_BACKEND",
		FailureFingerprint:    bytesOf(0x84, 32),
		ConsumedResourceUnits: 15,
		FailedAt:              advancedAt.Add(3 * time.Millisecond),
		RetryAt:               advancedAt.Add(4 * time.Millisecond),
	}
	failed, err := coordinator.Apply(context.Background(), fail)
	if err != nil {
		t.Fatalf("fail DiT StageAttempt for retry: %v", err)
	}
	if failed.StageAttemptID != assign.StageAttemptID || failed.State != "RETRY_WAIT" ||
		failed.StageFence != 2 || failed.StageVersion != 5 || failed.Replayed {
		t.Fatalf("failed DiT StageDecision = %#v", failed)
	}
	var ditFence int64
	var ditRetryCount int
	var consumedUnits int64
	var retryPhysicalState, retryAllocationState, retryLeaseState string
	if err := database.Admin.QueryRow(`
		SELECT dit.state::text, dit.fence, dit.retry_count,
		       physical.state::text, allocation.state::text, lease.state::text,
		       dependency.satisfied_progress_receipt_id,
		       budget.consumed_resource_units
		FROM stage_runs AS dit
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.stage_attempt_id = physical.id
		JOIN stage_leases AS lease ON lease.stage_attempt_id = physical.id
		JOIN stage_dependencies AS dependency ON dependency.destination_stage_run_id = dit.id
		JOIN attempt_retry_budgets AS budget ON budget.attempt_id = dit.attempt_id
		WHERE dit.id = $1
	`, ditRunID, assign.StageAttemptID).Scan(
		&ditState, &ditFence, &ditRetryCount, &retryPhysicalState, &retryAllocationState,
		&retryLeaseState, &dependencyReceipt, &consumedUnits,
	); err != nil {
		t.Fatalf("read DiT retry authority: %v", err)
	}
	if ditState != "RETRY_WAIT" || ditFence != 2 || ditRetryCount != 1 ||
		retryPhysicalState != "FAILED" || retryAllocationState != "RELEASED" ||
		retryLeaseState != "REVOKED" || dependencyReceipt != advance.ProgressReceiptID ||
		consumedUnits != fail.ConsumedResourceUnits {
		t.Fatalf(
			"DiT retry state=%s fence=%d retries=%d physical=%s allocation=%s lease=%s dependency=%s consumed=%d",
			ditState, ditFence, ditRetryCount, retryPhysicalState, retryAllocationState,
			retryLeaseState, dependencyReceipt, consumedUnits,
		)
	}

	reconciled, err := coordinator.Reconcile(context.Background(), 10)
	if err != nil {
		t.Fatalf("reconcile due DiT retry: %v", err)
	}
	if len(reconciled) != 1 || reconciled[0].AttemptID != attemptID ||
		reconciled[0].StageRunID != ditRunID || reconciled[0].State != "READY" ||
		reconciled[0].StageFence != 2 || reconciled[0].StageVersion != 6 ||
		reconciled[0].Reason != "RETRY_DUE" {
		t.Fatalf("reconciled DiT retry = %#v", reconciled)
	}
	replayed, err := coordinator.Reconcile(context.Background(), 10)
	if err != nil {
		t.Fatalf("replay due DiT retry reconciliation: %v", err)
	}
	if len(replayed) != 1 || replayed[0].AttemptID != attemptID ||
		replayed[0].StageRunID != ditRunID || replayed[0].State != "READY" ||
		replayed[0].StageFence != 2 || replayed[0].StageVersion != 6 ||
		replayed[0].Reason != "READY_REPLAY" {
		t.Fatalf("replayed DiT retry reconciliation = %#v", replayed)
	}
}

func TestStageGraphRunningCancellationPostsChargeAndFencesLateProgress(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Stage graph cancellation scope: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "cancel-stage-graph-running", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"cancel a running durable stage graph"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit Stage graph cancellation Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Stage graph cancellation Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct cancellation AttemptCoordinator: %v", err)
	}
	claim := readStageGraphInstantiation(t, database.Admin, job.JobID)
	attemptID := claim.AttemptID
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read cancellable StageRuns: %v", err)
	}
	advancedAt := time.Now().UTC().Truncate(time.Millisecond)
	_, err = coordinator.Apply(context.Background(), attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.MustParse("49400000-0000-0000-0000-000000000005"),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 1,
		ProgressReceiptID:    uuid.MustParse("49400000-0000-0000-0000-000000000006"),
		CacheSourceIdentity:  "stage-cancel/cache/encoder/exact-v1",
		OutputDigest:         bytesOf(0x91, 32),
		AdvancedAt:           advancedAt,
	})
	if err != nil {
		t.Fatalf("start cancellable graph with exact cache progress: %v", err)
	}

	first := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if first.StatusCode != http.StatusOK {
		t.Fatalf("cancel Stage graph status = %d, body=%s", first.StatusCode, first.Body)
	}
	replay := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if replay.StatusCode != http.StatusOK {
		t.Fatalf("replay Stage graph cancellation status = %d, body=%s", replay.StatusCode, replay.Body)
	}
	var firstResult, replayResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode Stage graph cancellation: %v", err)
	}
	if err := json.Unmarshal(replay.Body, &replayResult); err != nil {
		t.Fatalf("decode replayed Stage graph cancellation: %v", err)
	}
	if firstResult.CancellationID == "" ||
		firstResult.CancellationID != replayResult.CancellationID ||
		firstResult.Decision != "CANCELED" || replayResult.Decision != "CANCELED" ||
		firstResult.State != "CANCELED" || replayResult.State != "CANCELED" ||
		firstResult.JobVersion != 5 || replayResult.JobVersion != 5 ||
		!firstResult.Billable || !replayResult.Billable ||
		firstResult.Charge == nil || replayResult.Charge == nil ||
		firstResult.Charge.Amount != 1250 || replayResult.Charge.Amount != 1250 ||
		firstResult.Charge.Currency != "CNY" || replayResult.Charge.Currency != "CNY" ||
		firstResult.Charge.Reason != "CUSTOMER_CANCELLATION" ||
		replayResult.Charge.ChargeID != firstResult.Charge.ChargeID {
		t.Fatalf("Stage graph first/replayed cancellation = %#v / %#v", firstResult, replayResult)
	}

	_, err = coordinator.Apply(context.Background(), attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.MustParse("49400000-0000-0000-0000-000000000007"),
		AttemptID:            attemptID,
		StageRunID:           ditRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 2,
		ProgressReceiptID:    uuid.MustParse("49400000-0000-0000-0000-000000000008"),
		CacheSourceIdentity:  "stage-cancel/cache/dit/late-v1",
		OutputDigest:         bytesOf(0x92, 32),
		AdvancedAt:           advancedAt.Add(time.Second),
	})
	if err == nil {
		t.Fatal("late Stage progress succeeded after Customer Cancellation fenced the graph")
	}
}

func TestUnboundExactCacheLeafCannotBeginFinalization(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, ditRunID :=
		newStageGraphCancellationFixture(t, "stage-graph-cache-only-finalization")
	var vaeRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id
		FROM stage_runs
		WHERE attempt_id = $1 AND stage_key = 'vae'
	`, attemptID).Scan(&vaeRunID); err != nil {
		t.Fatalf("read cache-only VAE StageRun: %v", err)
	}

	advancedAt := time.Now().UTC().Truncate(time.Millisecond)
	for index, stage := range []struct {
		runID   uuid.UUID
		version int64
		name    string
	}{
		{runID: encoderRunID, version: 1, name: "encoder"},
		{runID: ditRunID, version: 2, name: "dit"},
		{runID: vaeRunID, version: 2, name: "vae"},
	} {
		decision, err := coordinator.Apply(context.Background(), attemptcoordinator.ExactCacheAdvanceCommand{
			CommandID:            uuid.New(),
			AttemptID:            attemptID,
			StageRunID:           stage.runID,
			ExpectedAttemptFence: 1,
			ExpectedStageFence:   1,
			ExpectedStageVersion: stage.version,
			ProgressReceiptID:    uuid.New(),
			CacheSourceIdentity:  "stage-cache-only/" + stage.name + "/exact-v1",
			OutputDigest:         bytesOf(byte(0xa1+index), 32),
			AdvancedAt:           advancedAt.Add(time.Duration(index) * time.Millisecond),
		})
		if err != nil {
			t.Fatalf("advance cache-only %s StageRun: %v", stage.name, err)
		}
		if decision.State != "SUCCEEDED" {
			t.Fatalf("cache-only %s decision = %#v", stage.name, decision)
		}
	}

	var jobState, attemptState, graphState string
	var finalizationStartedAt, finalizationDeadlineAt sql.NullTime
	var outputBindings int
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.state::text, attempt.graph_state::text,
		       attempt.finalization_started_at, attempt.finalization_deadline_at,
		       (SELECT count(*) FROM stage_run_output_bindings
		        WHERE stage_run_id = $2)
		FROM attempts AS attempt
		JOIN jobs AS job ON job.id = attempt.job_id
		WHERE attempt.id = $1
	`, attemptID, vaeRunID).Scan(
		&jobState, &attemptState, &graphState,
		&finalizationStartedAt, &finalizationDeadlineAt, &outputBindings,
	); err != nil {
		t.Fatalf("read unbound cache finalization authority: %v", err)
	}
	if jobState != "RUNNING" || attemptState != "RUNNING" ||
		graphState != "RUNNING" || finalizationStartedAt.Valid ||
		finalizationDeadlineAt.Valid || outputBindings != 0 {
		t.Fatalf(
			"unbound cache finalization = job/attempt/graph %s/%s/%s started/deadline %v/%v bindings=%d",
			jobState, attemptState, graphState,
			finalizationStartedAt, finalizationDeadlineAt, outputBindings,
		)
	}
}

func TestStageGraphPhysicalCancellationRetainsCapacityUntilLeaseExpiry(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-graph-physical-cancellation")
	leaseExpiresAt := time.Now().UTC().Add(2 * time.Second).Truncate(time.Millisecond)
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, leaseExpiresAt,
	)

	first := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	replay := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	if first.StatusCode != http.StatusOK || replay.StatusCode != http.StatusOK {
		t.Fatalf(
			"physical Stage cancellation statuses = %d/%d bodies=%s/%s",
			first.StatusCode, replay.StatusCode, first.Body, replay.Body,
		)
	}
	var firstResult, replayResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode physical Stage cancellation: %v", err)
	}
	if err := json.Unmarshal(replay.Body, &replayResult); err != nil {
		t.Fatalf("decode replayed physical Stage cancellation: %v", err)
	}
	if firstResult.CancellationID == "" ||
		firstResult.CancellationID != replayResult.CancellationID ||
		firstResult.Decision != "CANCELING" || replayResult.Decision != "CANCELING" ||
		firstResult.State != "CANCELING" || replayResult.State != "CANCELING" ||
		!firstResult.Billable || firstResult.Charge == nil ||
		replayResult.Charge == nil ||
		replayResult.Charge.ChargeID != firstResult.Charge.ChargeID {
		t.Fatalf("physical Stage cancellation = %#v / %#v", firstResult, replayResult)
	}

	var jobState, attemptState, runState, physicalState, leaseState, allocationState string
	var requestedEvents, cancelingEvents, canceledEvents, chargeEvents, invoiceEvents int64
	var requestedPayload []byte
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.graph_state::text, run.state::text,
		       physical.state::text, lease.state::text, allocation.state::text,
		       (SELECT count(*) FROM outbox_events
		        WHERE aggregate_id = job.id AND event_type = 'job.cancel_requested'),
		       (SELECT count(*) FROM outbox_events
		        WHERE aggregate_id = job.id AND event_type = 'job.canceling'),
		       (SELECT count(*) FROM outbox_events
		        WHERE aggregate_id = job.id AND event_type = 'job.canceled'),
		       (SELECT count(*) FROM outbox_events
		        WHERE aggregate_id = job.id AND event_type = 'charge.posted'),
		       (SELECT count(*) FROM outbox_events
		        WHERE aggregate_id = job.id AND event_type = 'invoice.export_requested'),
		       (SELECT payload FROM outbox_events
		        WHERE aggregate_id = job.id AND event_type = 'job.cancel_requested'
		        LIMIT 1)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $1 AND attempt.job_id = job.id
		JOIN stage_runs AS run ON run.id = $2 AND run.attempt_id = attempt.id
		JOIN stage_attempts AS physical ON physical.id = $3
		JOIN stage_leases AS lease ON lease.id = $4
		JOIN stage_allocations AS allocation ON allocation.id = $5
		WHERE job.id = $6
	`, attemptID, encoderRunID, assignment.StageAttemptID, assignment.StageLeaseID,
		assignment.StageAllocationID, job.JobID).Scan(
		&jobState, &attemptState, &runState, &physicalState, &leaseState, &allocationState,
		&requestedEvents, &cancelingEvents, &canceledEvents, &chargeEvents, &invoiceEvents,
		&requestedPayload,
	); err != nil {
		t.Fatalf("read physical Stage cancellation authority: %v", err)
	}
	if jobState != "CANCELING" || attemptState != "CANCELED" || runState != "CANCELED" ||
		physicalState != "CANCELED" || leaseState != "REVOKED" ||
		allocationState != "ALLOCATED" || requestedEvents != 1 || cancelingEvents != 1 ||
		canceledEvents != 0 || chargeEvents != 1 || invoiceEvents != 1 {
		t.Fatalf(
			"physical cancellation states=%s/%s/%s/%s/%s/%s events=%d/%d/%d/%d/%d",
			jobState, attemptState, runState, physicalState, leaseState, allocationState,
			requestedEvents, cancelingEvents, canceledEvents, chargeEvents, invoiceEvents,
		)
	}
	var envelope velav1.EventEnvelope
	if err := proto.Unmarshal(requestedPayload, &envelope); err != nil {
		t.Fatalf("decode Stage cancellation request event: %v", err)
	}
	requested := envelope.GetJobCancelRequested()
	if requested == nil || requested.GetJobId() != job.JobID ||
		requested.GetCancellationId() != firstResult.CancellationID ||
		requested.GetAttemptId() != attemptID.String() ||
		requested.GetWorkerId() != assignment.WorkerInstanceID.String() ||
		requested.GetWorkerEpoch() != uint64(assignment.WorkerInstanceEpoch) ||
		requested.GetAttemptFence() != uint64(assignment.ExpectedAttemptFence) ||
		requested.GetAuthorityLeaseId() != assignment.StageLeaseID.String() {
		t.Fatalf("Stage cancellation request payload = %#v", requested)
	}

	secondJob, secondAttemptID, secondEncoderRunID := instantiateAdditionalStageGraphJob(
		t, database, serverURL, coordinator, "stage-graph-capacity-reuse",
	)
	secondAssignment := assignment
	secondAssignment.CommandID = uuid.New()
	secondAssignment.AttemptID = secondAttemptID
	secondAssignment.StageRunID = secondEncoderRunID
	secondAssignment.StageAttemptID = uuid.New()
	secondAssignment.StageAllocationID = uuid.New()
	secondAssignment.StageLeaseID = uuid.New()
	secondAssignment.TokenDigest = bytesOf(0xb1, 32)
	secondAssignment.ExecutionNonce = bytesOf(0xb2, 32)
	secondAssignment.IssuedAt = time.Now().UTC().Truncate(time.Millisecond)
	secondAssignment.ExpiresAt = secondAssignment.IssuedAt.Add(time.Minute)
	secondAssignment.LocalDeadlineAt = secondAssignment.IssuedAt.Add(50 * time.Second)
	if _, err := coordinator.Apply(context.Background(), secondAssignment); err == nil {
		t.Fatalf("reused WorkerInstance before physical stop proof for Job %s", secondJob.JobID)
	}

	if wait := time.Until(leaseExpiresAt.Add(50 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	reconciled, err := coordinator.Reconcile(context.Background(), 10)
	if err != nil {
		t.Fatalf("reconcile expired physical Stage cancellation: %v", err)
	}
	var foundStop bool
	for _, decision := range reconciled {
		if decision.StageRunID == encoderRunID &&
			decision.Reason == "CANCELLATION_LEASE_EXPIRED" {
			foundStop = true
		}
	}
	if !foundStop {
		t.Fatalf("expired physical Stage cancellation reconcile = %#v", reconciled)
	}
	if _, err := coordinator.Apply(context.Background(), secondAssignment); err != nil {
		t.Fatalf("reuse WorkerInstance after physical stop proof: %v", err)
	}

	var terminalState, releasedState string
	var stopReceipts, terminalEvents int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, allocation.state::text,
		       (SELECT count(*) FROM stage_cancellation_stop_receipts
		        WHERE cancellation_id = $1),
		       (SELECT count(*) FROM outbox_events
		        WHERE aggregate_id = job.id AND event_type = 'job.canceled')
		FROM jobs AS job
		JOIN stage_allocations AS allocation ON allocation.id = $2
		WHERE job.id = $3
	`, firstResult.CancellationID, assignment.StageAllocationID, job.JobID).Scan(
		&terminalState, &releasedState, &stopReceipts, &terminalEvents,
	); err != nil {
		t.Fatalf("read reconciled physical Stage cancellation: %v", err)
	}
	if terminalState != "CANCELED" || releasedState != "RELEASED" ||
		stopReceipts != 1 || terminalEvents != 1 {
		t.Fatalf(
			"reconciled physical cancellation = job/allocation %s/%s receipts/events %d/%d",
			terminalState, releasedState, stopReceipts, terminalEvents,
		)
	}
}

func TestStageGraphCancellationSerializesWithPhysicalStart(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-graph-cancel-start-race")
	leaseExpiresAt := time.Now().UTC().Add(2 * time.Second).Truncate(time.Millisecond)
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, leaseExpiresAt,
	)
	startCommand := attemptcoordinator.StartStageCommand{
		CommandID:            uuid.New(),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		StageAttemptID:       assignment.StageAttemptID,
		StageLeaseID:         assignment.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 2,
		StartedAt:            assignment.IssuedAt.Add(time.Millisecond),
	}
	type startCall struct {
		decision attemptcoordinator.StageDecision
		err      error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	startGate := make(chan struct{})
	started := make(chan startCall, 1)
	canceled := make(chan cancelCall, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		<-startGate
		decision, err := coordinator.Apply(ctx, startCommand)
		started <- startCall{decision: decision, err: err}
	}()
	go func() {
		<-startGate
		result, err := doCancelJob(
			serverURL, testProjectID, job.JobID, testBearerCredential(),
		)
		canceled <- cancelCall{result: result, err: err}
	}()
	close(startGate)
	startResult := <-started
	cancelResult := <-canceled
	if cancelResult.err != nil || cancelResult.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"cancel/start race cancellation = status %d body=%s error=%v",
			cancelResult.result.StatusCode, cancelResult.result.Body, cancelResult.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(cancelResult.result.Body, &response); err != nil {
		t.Fatalf("decode cancel/start race cancellation: %v", err)
	}
	if startResult.err == nil {
		if startResult.decision.State != "RUNNING" || response.Decision != "CANCELING" ||
			response.State != "CANCELING" || !response.Billable || response.Charge == nil {
			t.Fatalf("physical Start winner = %#v cancellation=%#v", startResult, response)
		}
	} else if response.Decision != "CANCELED" || response.State != "CANCELED" ||
		response.Billable || response.Charge != nil {
		t.Fatalf("cancellation winner before physical Start = %#v start error=%v", response, startResult.err)
	}

	if wait := time.Until(leaseExpiresAt.Add(50 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	if _, err := coordinator.Reconcile(context.Background(), 10); err != nil {
		t.Fatalf("reconcile cancel/start race stop: %v", err)
	}
	var jobState, allocationState string
	var receipts, decisions, charges int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, allocation.state::text,
		       (SELECT count(*) FROM stage_cancellation_stop_receipts
		        WHERE stage_lease_id = $1),
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job
		JOIN stage_allocations AS allocation ON allocation.id = $2
		WHERE job.id = $3
	`, assignment.StageLeaseID, assignment.StageAllocationID, job.JobID).Scan(
		&jobState, &allocationState, &receipts, &decisions, &charges,
	); err != nil {
		t.Fatalf("read cancel/start race outcome: %v", err)
	}
	wantCharges := int64(0)
	if startResult.err == nil {
		wantCharges = 1
	}
	if jobState != "CANCELED" || allocationState != "RELEASED" || receipts != 1 ||
		decisions != 1 || charges != wantCharges {
		t.Fatalf(
			"cancel/start race outcome job/allocation=%s/%s receipts/decisions/charges=%d/%d/%d",
			jobState, allocationState, receipts, decisions, charges,
		)
	}
}

func TestStageGraphCancellationSerializesWithPhysicalCompletion(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-graph-cancel-complete-race")
	leaseExpiresAt := time.Now().UTC().Add(2 * time.Second).Truncate(time.Millisecond)
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, leaseExpiresAt,
	)
	completeCommand := attemptcoordinator.CompleteStageCommand{
		CommandID:            uuid.New(),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		StageAttemptID:       assignment.StageAttemptID,
		StageLeaseID:         assignment.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 3,
		ProgressReceiptID:    uuid.New(),
		OutputIdentity:       "stage-output/cancel-complete-race/encoder-v1",
		OutputDigest:         bytesOf(0xc1, 32),
		CompletedAt:          assignment.IssuedAt.Add(2 * time.Millisecond),
	}
	type completeCall struct {
		decision attemptcoordinator.StageDecision
		err      error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	startGate := make(chan struct{})
	completed := make(chan completeCall, 1)
	canceled := make(chan cancelCall, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		<-startGate
		decision, err := coordinator.Apply(ctx, completeCommand)
		completed <- completeCall{decision: decision, err: err}
	}()
	go func() {
		<-startGate
		result, err := doCancelJob(
			serverURL, testProjectID, job.JobID, testBearerCredential(),
		)
		canceled <- cancelCall{result: result, err: err}
	}()
	close(startGate)
	completeResult := <-completed
	cancelResult := <-canceled
	if cancelResult.err != nil || cancelResult.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"cancel/complete race cancellation = status %d body=%s error=%v",
			cancelResult.result.StatusCode, cancelResult.result.Body, cancelResult.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(cancelResult.result.Body, &response); err != nil {
		t.Fatalf("decode cancel/complete race cancellation: %v", err)
	}
	if !response.Billable || response.Charge == nil {
		t.Fatalf("cancel/complete race did not preserve Billable Start: %#v", response)
	}
	if completeResult.err == nil {
		if completeResult.decision.State != "SUCCEEDED" ||
			response.Decision != "CANCELED" || response.State != "CANCELED" {
			t.Fatalf("physical Complete winner = %#v cancellation=%#v", completeResult, response)
		}
	} else if response.Decision != "CANCELING" || response.State != "CANCELING" {
		t.Fatalf("cancellation winner before physical Complete = %#v complete error=%v", response, completeResult.err)
	}

	if response.State == "CANCELING" {
		if wait := time.Until(leaseExpiresAt.Add(50 * time.Millisecond)); wait > 0 {
			time.Sleep(wait)
		}
		if _, err := coordinator.Reconcile(context.Background(), 10); err != nil {
			t.Fatalf("reconcile cancel/complete race stop: %v", err)
		}
	}
	var jobState, allocationState string
	var decisions, charges int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, allocation.state::text,
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job
		JOIN stage_allocations AS allocation ON allocation.id = $1
		WHERE job.id = $2
	`, assignment.StageAllocationID, job.JobID).Scan(
		&jobState, &allocationState, &decisions, &charges,
	); err != nil {
		t.Fatalf("read cancel/complete race outcome: %v", err)
	}
	if jobState != "CANCELED" || allocationState != "RELEASED" ||
		decisions != 1 || charges != 1 {
		t.Fatalf(
			"cancel/complete race outcome job/allocation=%s/%s decisions/charges=%d/%d",
			jobState, allocationState, decisions, charges,
		)
	}
}

func TestStageGraphQueuedCancellationReleasesGraphAuthorityWithoutCharge(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, _, _ :=
		newStageGraphCancellationFixture(t, "cancel-stage-graph-queued")

	first := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	replay := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	if first.StatusCode != http.StatusOK || replay.StatusCode != http.StatusOK {
		t.Fatalf(
			"queued Stage graph cancellation statuses = %d/%d bodies=%s/%s",
			first.StatusCode, replay.StatusCode, first.Body, replay.Body,
		)
	}
	var firstResult, replayResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode queued Stage graph cancellation: %v", err)
	}
	if err := json.Unmarshal(replay.Body, &replayResult); err != nil {
		t.Fatalf("decode replayed queued Stage graph cancellation: %v", err)
	}
	if firstResult.CancellationID == "" ||
		firstResult.CancellationID != replayResult.CancellationID ||
		firstResult.Decision != "CANCELED" || replayResult.Decision != "CANCELED" ||
		firstResult.State != "CANCELED" || replayResult.State != "CANCELED" ||
		firstResult.JobVersion != 3 || replayResult.JobVersion != 3 ||
		firstResult.Billable || replayResult.Billable ||
		firstResult.Charge != nil || replayResult.Charge != nil {
		t.Fatalf("queued Stage graph first/replayed cancellation = %#v / %#v", firstResult, replayResult)
	}

	var jobState, attemptState, storageState, reservationState string
	var activeRuns, activeLeases, activeAllocations, decisions, charges int64
	var projectQueued, readyQueue, capacityReady, reservedMinor int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.graph_state::text, storage.state::text,
		       credit_reservation.state::text,
		       (SELECT count(*) FROM stage_runs
		        WHERE attempt_id = attempt.id AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')),
		       (SELECT count(*) FROM stage_leases
		        WHERE attempt_id = attempt.id AND state = 'ACTIVE'),
		       (SELECT count(*) FROM stage_allocations
		        WHERE attempt_id = attempt.id AND state = 'ALLOCATED'),
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       project.queued_count,
		       (SELECT count(*) FROM stage_ready_queue_entries
		        WHERE project_id = job.project_id),
		       (SELECT COALESCE(sum(ready_count), 0)
		        FROM stage_capacity_pool_counters),
		       account.reserved_minor
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $1
		JOIN stage_storage_reservations AS storage ON storage.attempt_id = attempt.id
		JOIN credit_reservations AS credit_reservation ON credit_reservation.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN organization_credit_accounts AS account
		  ON account.organization_id = job.organization_id
		WHERE job.id = $2
		`, attemptID, job.JobID).Scan(
		&jobState, &attemptState, &storageState, &reservationState,
		&activeRuns, &activeLeases, &activeAllocations, &decisions, &charges,
		&projectQueued, &readyQueue, &capacityReady, &reservedMinor,
	); err != nil {
		t.Fatalf("read queued Stage graph cancellation authority: %v", err)
	}
	if jobState != "CANCELED" || attemptState != "CANCELED" ||
		storageState != "RELEASED" || reservationState != "RELEASED" ||
		activeRuns != 0 || activeLeases != 0 || activeAllocations != 0 ||
		decisions != 1 || charges != 0 || projectQueued != 0 || readyQueue != 0 ||
		capacityReady != 0 ||
		reservedMinor != 0 {
		t.Fatalf(
			"queued Stage graph cancellation = job/attempt/storage/credit %s/%s/%s/%s active %d/%d/%d decisions/charges %d/%d counters project/ready/capacity/reserved %d/%d/%d/%d",
			jobState, attemptState, storageState, reservationState,
			activeRuns, activeLeases, activeAllocations, decisions, charges,
			projectQueued, readyQueue, capacityReady, reservedMinor,
		)
	}
	if reconciled, err := coordinator.Reconcile(context.Background(), 10); err != nil || len(reconciled) != 0 {
		t.Fatalf("reconcile canceled queued Stage graph = %#v error=%v", reconciled, err)
	}
}

func TestStageGraphCancellationFailsClosedOnQueueCounterDrift(t *testing.T) {
	database, serverURL, _, job, attemptID, _, _ :=
		newStageGraphCancellationFixture(t, "cancel-stage-graph-counter-drift")
	if _, err := database.Admin.Exec(`
		UPDATE projects SET queued_count = 0 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("inject Stage graph Project counter drift: %v", err)
	}

	result := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	if result.StatusCode != http.StatusInternalServerError {
		t.Fatalf(
			"Stage graph cancellation with counter drift status = %d body=%s, want 500",
			result.StatusCode, result.Body,
		)
	}
	var jobState, attemptState, reservationState string
	var decisions, charges, canceledRuns int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.graph_state::text, reservation.state::text,
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       (SELECT count(*) FROM stage_runs
		        WHERE attempt_id = attempt.id AND state = 'CANCELED')
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $1
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $2
	`, attemptID, job.JobID).Scan(
		&jobState, &attemptState, &reservationState, &decisions, &charges, &canceledRuns,
	); err != nil {
		t.Fatalf("read rejected Stage graph cancellation effects: %v", err)
	}
	if jobState != "QUEUED" || attemptState != "QUEUED" ||
		reservationState != "RESERVED" || decisions != 0 || charges != 0 ||
		canceledRuns != 0 {
		t.Fatalf(
			"rejected Stage graph cancellation = job/attempt/reservation %s/%s/%s decisions/charges/runs %d/%d/%d",
			jobState, attemptState, reservationState, decisions, charges, canceledRuns,
		)
	}
}

func TestStageGraphCancellationAndFirstProgressProduceOneTerminalBillingOutcome(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "cancel-stage-graph-first-progress-race")
	advancedAt := time.Now().UTC().Truncate(time.Millisecond)
	advance := attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.New(),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 1,
		ProgressReceiptID:    uuid.New(),
		CacheSourceIdentity:  "stage-cancel/cache/encoder/race-v1",
		OutputDigest:         bytesOf(0x93, 32),
		AdvancedAt:           advancedAt,
	}
	type advanceCall struct {
		decision attemptcoordinator.StageDecision
		err      error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	start := make(chan struct{})
	advanceResult := make(chan advanceCall, 1)
	cancelResult := make(chan cancelCall, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		<-start
		decision, err := coordinator.Apply(ctx, advance)
		advanceResult <- advanceCall{decision: decision, err: err}
	}()
	go func() {
		<-start
		result, err := doCancelJob(
			serverURL, testProjectID, job.JobID, testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: err}
	}()
	close(start)
	advanced := <-advanceResult
	canceled := <-cancelResult
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"concurrent Stage graph cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode, canceled.result.Body, canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode Stage graph progress race cancellation: %v", err)
	}

	var jobState, attemptState, reservationState string
	var jobVersion, decisionCount, chargeCount, receiptCount int64
	var reservedMinor, unsettledPostedMinor int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.version, attempt.graph_state::text,
		       reservation.state::text, account.reserved_minor,
		       account.unsettled_posted_minor,
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       (SELECT count(*) FROM stage_progress_receipts WHERE attempt_id = attempt.id)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $1
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS account
		  ON account.organization_id = job.organization_id
		WHERE job.id = $2
	`, attemptID, job.JobID).Scan(
		&jobState, &jobVersion, &attemptState, &reservationState,
		&reservedMinor, &unsettledPostedMinor,
		&decisionCount, &chargeCount, &receiptCount,
	); err != nil {
		t.Fatalf("read Stage graph progress race result: %v", err)
	}
	if jobState != "CANCELED" || attemptState != "CANCELED" || decisionCount != 1 {
		t.Fatalf(
			"Stage graph progress race authority = job %s attempt %s decisions %d",
			jobState, attemptState, decisionCount,
		)
	}
	if advanced.err == nil {
		if advanced.decision.State != "SUCCEEDED" || response.JobVersion != 5 ||
			jobVersion != 5 || !response.Billable || response.Charge == nil ||
			response.Charge.Amount != 1250 || reservationState != "CONSUMED" ||
			reservedMinor != 0 || unsettledPostedMinor != 1250 ||
			chargeCount != 1 || receiptCount != 1 {
			t.Fatalf(
				"progress-winning cancellation = advance %#v response %#v jobVersion %d reservation %s credit %d/%d charges/receipts %d/%d",
				advanced.decision, response, jobVersion, reservationState,
				reservedMinor, unsettledPostedMinor, chargeCount, receiptCount,
			)
		}
	} else if response.JobVersion != 3 || jobVersion != 3 || response.Billable ||
		response.Charge != nil || reservationState != "RELEASED" ||
		reservedMinor != 0 || unsettledPostedMinor != 0 ||
		chargeCount != 0 || receiptCount != 0 {
		t.Fatalf(
			"cancellation-winning progress race = error %v response %#v jobVersion %d reservation %s credit %d/%d charges/receipts %d/%d",
			advanced.err, response, jobVersion, reservationState,
			reservedMinor, unsettledPostedMinor, chargeCount, receiptCount,
		)
	}
}

func TestStageGraphInstantiationDispatchMigrationBackfillsAndResumes(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 50)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 49); err != nil {
		t.Fatalf("migrate empty instantiation dispatch down: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	completedJob := insertPreDispatchStageJob(
		t, database.Admin, "completed before migration",
	)
	pendingJob := insertPreDispatchStageJob(
		t, database.Admin, "pending before migration",
	)

	coordinatorPool := newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	)
	manual, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct pre-migration AttemptCoordinator: %v", err)
	}
	completedCommand := attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.MustParse("49300000-0000-0000-0000-00000000f004"),
		JobID:                      uuid.MustParse(completedJob.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.MustParse("49300000-0000-0000-0000-00000000f001"),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  uuid.MustParse("49300000-0000-0000-0000-00000000f002"),
		StorageReservationID:       uuid.MustParse("49300000-0000-0000-0000-00000000f003"),
		ReservedStorageBytes:       2 << 30,
	}
	if _, err := manual.Instantiate(context.Background(), completedCommand); err != nil {
		t.Fatalf("instantiate pre-migration Stage graph: %v", err)
	}

	if err := goose.UpTo(database.Admin, migrations, 50); err != nil {
		t.Fatalf("upgrade Stage graph instantiation dispatch: %v", err)
	}
	rows, err := database.Admin.Query(`
		SELECT job_id::text, state::text, source
		FROM stage_graph_instantiation_work
		ORDER BY job_id
	`)
	if err != nil {
		t.Fatalf("read backfilled instantiation work: %v", err)
	}
	defer rows.Close()
	states := map[string]string{}
	for rows.Next() {
		var jobID, state, source string
		if err := rows.Scan(&jobID, &state, &source); err != nil {
			t.Fatalf("scan backfilled instantiation work: %v", err)
		}
		if source != "MIGRATION_BACKFILL" {
			t.Fatalf("backfilled work %s source = %s", jobID, source)
		}
		states[jobID] = state
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate backfilled instantiation work: %v", err)
	}
	if states[completedJob.JobID] != "COMPLETED" || states[pendingJob.JobID] != "PENDING" {
		t.Fatalf("backfilled instantiation states = %#v", states)
	}

	automatic, err := attemptcoordinator.NewAutomatedService(
		coordinatorPool,
		attemptcoordinator.AutomationConfig{
			InstanceID: "attempt-coordinator-migration-resume",
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Second,
			BatchSize:  10,
		},
	)
	if err != nil {
		t.Fatalf("construct resumed AttemptCoordinator: %v", err)
	}
	result, err := automatic.RunCycle(context.Background())
	if err != nil {
		t.Fatalf("resume backfilled instantiation work: %v", err)
	}
	if result.Claims != 1 || result.Instantiated != 1 {
		t.Fatalf("resumed instantiation result = %#v", result)
	}
	var pendingState string
	var pendingAttempts int
	if err := database.Admin.QueryRow(`
		SELECT
			work.state::text,
			(SELECT count(*) FROM attempts WHERE job_id = work.job_id)
		FROM stage_graph_instantiation_work AS work
		WHERE work.job_id = $1
	`, pendingJob.JobID).Scan(&pendingState, &pendingAttempts); err != nil {
		t.Fatalf("read resumed instantiation work: %v", err)
	}
	if pendingState != "COMPLETED" || pendingAttempts != 1 {
		t.Fatalf("resumed work state=%s attempts=%d", pendingState, pendingAttempts)
	}
}

func TestStageGraphInstantiationDispatchMigrationRejectsStorageMismatch(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 50)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 49); err != nil {
		t.Fatalf("migrate empty instantiation dispatch down: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	job := insertPreDispatchStageJob(
		t, database.Admin, "mismatched pre-migration storage authority",
	)
	manual, err := attemptcoordinator.NewService(newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct pre-migration AttemptCoordinator: %v", err)
	}
	_, err = manual.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.New(),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.New(),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  uuid.New(),
		StorageReservationID:       uuid.New(),
		ReservedStorageBytes:       1 << 30,
	})
	if err != nil {
		t.Fatalf("instantiate mismatched pre-migration Stage graph: %v", err)
	}

	err = goose.UpTo(database.Admin, migrations, 50)
	assertPostgresConstraint(t, err, "stage_graph_instantiation_backfill_storage_mismatch")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 49 {
		t.Fatalf("migration version after storage mismatch = %d error=%v", version, versionErr)
	}
}

func TestAtomicStageGraphAdmissionMigrationBoundary(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	t.Run("empty Down Up restores exact request authority", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 51)
		assertAtomicStageAdmissionRequestSurface(t, database.Admin, true)

		if err := goose.DownTo(database.Admin, migrations, 50); err != nil {
			t.Fatalf("migrate empty atomic Stage graph Admission down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 50 {
			t.Fatalf("atomic Stage graph Admission version after Down = %d error=%v", version, err)
		}
		assertAtomicStageAdmissionRequestSurface(t, database.Admin, false)
		requestPool := newRolePool(
			t, database.DSN, "vela_request_login", "vela-request-password",
		)
		if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
			t.Fatal("current request role verification accepted schema 50 without atomic Admission")
		}

		if err := goose.UpTo(database.Admin, migrations, 51); err != nil {
			t.Fatalf("migrate atomic Stage graph Admission up again: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 51 {
			t.Fatalf("atomic Stage graph Admission version after Down Up = %d error=%v", version, err)
		}
		assertAtomicStageAdmissionRequestSurface(t, database.Admin, true)
		if err := veladb.VerifyRole(
			context.Background(), requestPool, veladb.RoleRequest,
		); err != nil {
			t.Fatalf("verify request role after atomic Admission Down Up: %v", err)
		}
	})

	t.Run("current Admission fails closed on schema 50", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 51)
		if err := goose.DownTo(database.Admin, migrations, 50); err != nil {
			t.Fatalf("contract atomic Admission schema for current-service probe: %v", err)
		}
		seedAdmissionFixture(t, database.Admin)
		seedStageExecutionCatalog(t, database.Admin)
		activateH3StageGraph(t, database)
		server := admissionServerForDatabase(t, database)
		result := submitJob(t, server.URL, "atomic-admission-schema-50", []byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"current Admission must fail closed on schema 50"
		}`))
		if result.StatusCode != http.StatusInternalServerError {
			t.Fatalf(
				"current Admission against schema 50 status = %d, want 500; body=%s",
				result.StatusCode,
				result.Body,
			)
		}
		assertNoAdmissionEffects(t, database.Admin)
	})

	t.Run("deferred trigger rejects non-atomic Stage Job", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 51)
		seedAdmissionFixture(t, database.Admin)
		seedStageExecutionCatalog(t, database.Admin)
		activateH3StageGraph(t, database)
		tx, job := beginPreDispatchStageJob(
			t, database.Admin, "schema 51 non-atomic Stage Job",
		)
		_, err := tx.Exec("SET CONSTRAINTS jobs_require_atomic_stage_graph_admission IMMEDIATE")
		assertPostgresConstraint(t, err, "stage_graph_admission_requires_atomic_instantiation")
		_ = tx.Rollback()

		var jobs, work int
		if err := database.Admin.QueryRow(`
			SELECT
				(SELECT count(*) FROM jobs WHERE id = $1),
				(SELECT count(*) FROM stage_graph_instantiation_work WHERE job_id = $1)
		`, job.JobID).Scan(&jobs, &work); err != nil {
			t.Fatalf("inspect rejected non-atomic Stage Job: %v", err)
		}
		if jobs != 0 || work != 0 {
			t.Fatalf("rejected non-atomic Stage Job left job/work = %d/%d", jobs, work)
		}
	})

	t.Run("request context is exact", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 51)
		seedAdmissionFixture(t, database.Admin)
		requestPool := newRolePool(
			t, database.DSN, "vela_request_login", "vela-request-password",
		)
		for _, test := range []struct {
			name           string
			organizationID uuid.UUID
			projectID      uuid.UUID
		}{
			{
				name:           "cross organization",
				organizationID: uuid.New(),
				projectID:      uuid.MustParse(testProjectID),
			},
			{
				name:           "cross Project",
				organizationID: uuid.MustParse(testOrganizationID),
				projectID:      uuid.New(),
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				tx, err := requestPool.Begin(context.Background())
				if err != nil {
					t.Fatalf("begin mismatched atomic Admission transaction: %v", err)
				}
				defer func() { _ = tx.Rollback(context.Background()) }()
				if _, err := tx.Exec(
					context.Background(),
					"SELECT * FROM vela_set_request_context($1, $2, $3)",
					testCredentialID,
					credentialDigest([]byte(testCredentialSecret)),
					"jobs:submit",
				); err != nil {
					t.Fatalf("establish atomic Admission request context: %v", err)
				}
				_, err = tx.Exec(
					context.Background(),
					"SELECT * FROM vela_instantiate_admitted_stage_graph($1, $2, $3)",
					test.organizationID,
					test.projectID,
					uuid.New(),
				)
				assertPostgresConstraint(t, err, "stage_graph_admission_context_mismatch")
			})
		}
	})

	t.Run("durable Admission evidence refuses Down", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 51)
		seedAdmissionFixture(t, database.Admin)
		seedStageExecutionCatalog(t, database.Admin)
		activateH3StageGraph(t, database)
		tx, job := beginPreDispatchStageJob(
			t, database.Admin, "atomic Admission rollback evidence",
		)
		if _, err := tx.Exec(`
			UPDATE stage_graph_instantiation_work
			SET state = 'COMPLETED',
				completed_at = clock_timestamp(),
				completion_reason = 'ADMISSION_TRANSACTION_INSTANTIATED',
				updated_at = clock_timestamp()
			WHERE job_id = $1
		`, job.JobID); err != nil {
			t.Fatalf("record atomic Admission rollback evidence: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit atomic Admission rollback evidence: %v", err)
		}

		err := goose.DownTo(database.Admin, migrations, 50)
		assertPostgresConstraint(t, err, "atomic_stage_graph_admission_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 51 {
			t.Fatalf("atomic Admission version after refused Down = %d error=%v", version, versionErr)
		}
	})
}

func assertAtomicStageAdmissionRequestSurface(t *testing.T, database *sql.DB, want bool) {
	t.Helper()
	const signature = "vela_instantiate_admitted_stage_graph(uuid,uuid,uuid)"
	const capacitySignature = "vela_lock_stage_graph_ready_capacity_path(uuid,uuid)"
	const guardSignature = "vela_require_atomic_stage_graph_admission()"
	var functionExists, requestCanExecute, capacityExists, requestCanCheckCapacity bool
	var authCanExecute, requestCanExecuteGuard bool
	var directAuthorityRelations int
	if err := database.QueryRow(`
		SELECT
			to_regprocedure($1) IS NOT NULL,
			COALESCE(
				has_function_privilege(
					'vela_request_login', to_regprocedure($1), 'EXECUTE'
				),
				false
			),
			COALESCE(
				has_function_privilege(
					'vela_auth_login', to_regprocedure($1), 'EXECUTE'
				),
				false
			),
			COALESCE(
				to_regprocedure($2) IS NOT NULL,
				false
			),
			COALESCE(
				has_function_privilege(
					'vela_request_login', to_regprocedure($2), 'EXECUTE'
				),
				false
			),
			COALESCE(
				has_function_privilege(
					'vela_request_login', to_regprocedure($3), 'EXECUTE'
				),
				false
			),
			(
				SELECT count(*)
				FROM unnest(ARRAY[
					'stage_graph_instantiation_work',
					'execution_graph_snapshots',
					'attempts',
					'stage_runs',
					'stage_storage_reservations'
				]) AS relation(name)
				WHERE has_table_privilege(
					'vela_request_login', relation.name,
					'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
				) OR has_any_column_privilege(
					'vela_request_login', relation.name,
					'SELECT,INSERT,UPDATE,REFERENCES'
				)
			)
	`, signature, capacitySignature, guardSignature).Scan(
		&functionExists,
		&requestCanExecute,
		&authCanExecute,
		&capacityExists,
		&requestCanCheckCapacity,
		&requestCanExecuteGuard,
		&directAuthorityRelations,
	); err != nil {
		t.Fatalf("inspect atomic Stage graph Admission request surface: %v", err)
	}
	if functionExists != want || requestCanExecute != want || capacityExists != want ||
		requestCanCheckCapacity != want || authCanExecute ||
		requestCanExecuteGuard || directAuthorityRelations != 0 {
		t.Fatalf(
			"atomic Stage graph Admission request surface = function/request/capacity/request-capacity/auth/guard/direct %t/%t/%t/%t/%t/%t/%d, want %t/%t/%t/%t/false/false/0",
			functionExists,
			requestCanExecute,
			capacityExists,
			requestCanCheckCapacity,
			authCanExecute,
			requestCanExecuteGuard,
			directAuthorityRelations,
			want,
			want,
			want,
			want,
		)
	}
}

func TestStageAttemptAuthorityMigrationRoundTripAndDurableAuthorityRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up restores legacy cancellation surface", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 36)
		if err := goose.DownTo(database.Admin, migrations, 35); err != nil {
			t.Fatalf("migrate empty Stage Attempt authority down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 35 {
			t.Fatalf("Stage Attempt authority version after Down = %d error=%v", version, err)
		}
		var currentCancellation, legacyCancellation, stageRuns bool
		var stageAwareLegacyTriggers int
		if err := database.Admin.QueryRow(`
			SELECT
				to_regprocedure(
				  'vela_cancel_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'
				) IS NOT NULL,
				to_regprocedure(
				  'vela_cancel_legacy_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'
				) IS NOT NULL,
				to_regclass('stage_runs') IS NOT NULL,
				(SELECT count(*)
				 FROM unnest(ARRAY[
				   'vela_bind_attempt_profile_certification()'::regprocedure,
				   'vela_enforce_scheduler_dispatch_attempt()'::regprocedure,
				   'vela_guard_fleet_assignment_writer()'::regprocedure,
				   'vela_validate_job_state_transition()'::regprocedure,
				   'vela_reject_billable_start_mutation()'::regprocedure
				 ]) AS function_id
				 WHERE pg_get_functiondef(function_id) LIKE '%execution_authority_kind%')
		`).Scan(
			&currentCancellation, &legacyCancellation, &stageRuns, &stageAwareLegacyTriggers,
		); err != nil {
			t.Fatalf("inspect schema 35 Stage Attempt rollback surface: %v", err)
		}
		if !currentCancellation || legacyCancellation || stageRuns || stageAwareLegacyTriggers != 0 {
			t.Fatalf(
				"schema 35 rollback surface = current/legacy/stageRuns %t/%t/%t stage-aware-triggers=%d",
				currentCancellation, legacyCancellation, stageRuns, stageAwareLegacyTriggers,
			)
		}
		if err := goose.UpTo(database.Admin, migrations, 36); err != nil {
			t.Fatalf("migrate Stage Attempt authority up again: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 36 {
			t.Fatalf("Stage Attempt authority version after Down Up = %d error=%v", version, err)
		}
	})

	t.Run("durable Stage graph authority refuses Down at cutover boundary", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 36)
		seedAdmissionFixture(t, database.Admin)
		seedStageExecutionCatalog(t, database.Admin)
		jobID := uuid.New()
		snapshotID := uuid.New()
		tx, err := database.Admin.Begin()
		if err != nil {
			t.Fatalf("begin historical Stage authority fixture: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		if _, err := tx.Exec(`
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
				execution_circuit_breaker_policy,
				execution_circuit_fingerprint_window_seconds,
				execution_circuit_min_distinct_healthy_workers,
				job_expires_at
			)
			SELECT
				$1::uuid, project.organization_id, project.id, $2,
				'00000000-0000-0000-0000-000000000010',
				'00000000-0000-0000-0000-000000000011', service.id,
				'00000000-0000-0000-0000-000000000013',
				'00000000-0000-0000-0000-000000000005',
				sha256(convert_to($1::uuid::text, 'UTF8')),
				'{"model":"minimax-h3","prompt":"historical Stage authority"}'::jsonb,
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
			WHERE project.organization_id = $3 AND project.id = $4
		`, jobID, testPrincipalID, testOrganizationID, testProjectID); err != nil {
			t.Fatalf("insert historical Stage authority Job: %v", err)
		}
		if _, err := tx.Exec(`
			INSERT INTO credit_reservations (
				id, organization_id, project_id, job_id, amount_minor, currency
			) VALUES ($1, $2, $3, $4, 1250, 'CNY')
		`, uuid.New(), testOrganizationID, testProjectID, jobID); err != nil {
			t.Fatalf("insert historical Stage authority CreditReservation: %v", err)
		}
		if _, err := tx.Exec(`
			INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
			VALUES ($1, $2, $3)
		`, jobID, testOrganizationID, testProjectID); err != nil {
			t.Fatalf("insert historical Stage authority RetryRuntimeState: %v", err)
		}
		if _, err := tx.Exec(`
			INSERT INTO execution_graph_snapshots (
				id, organization_id, project_id, job_id,
				execution_graph_revision_id, execution_profile_revision_id,
				graph_content_digest, topological_order, snapshot_contract
			)
			SELECT
				$1, $2, $3, $4, graph.id, profile.id,
				graph.content_digest, ARRAY['encoder', 'dit', 'vae'],
				jsonb_build_object('source', 'schema-36-rollback-fixture')
			FROM execution_graph_revisions AS graph
			JOIN execution_profile_revisions AS profile
			  ON profile.id = $5
			 AND profile.execution_graph_revision_id = graph.id
			WHERE graph.id = $6
		`, snapshotID, testOrganizationID, testProjectID, jobID,
			graphExecutionProfileID, stageGraphID); err != nil {
			t.Fatalf("insert historical Stage authority snapshot: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit historical Stage authority fixture: %v", err)
		}

		err = goose.DownTo(database.Admin, migrations, 35)
		assertPostgresConstraint(
			t,
			err,
			"stage_attempt_authority_rollback_is_unsafe",
		)
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 36 {
			t.Fatalf(
				"Stage Attempt authority version after refusal = %d error=%v",
				version, versionErr,
			)
		}
		var snapshots int64
		if err := database.Admin.QueryRow(`
			SELECT count(*) FROM execution_graph_snapshots WHERE id = $1
		`, snapshotID).Scan(&snapshots); err != nil {
			t.Fatalf("read durable Stage graph authority after refusal: %v", err)
		}
		if snapshots != 1 {
			t.Fatalf("durable Stage graph authority snapshots after refusal = %d, want 1", snapshots)
		}
	})
}

func newStageGraphCancellationFixture(
	t *testing.T,
	idempotencyKey string,
) (testDatabase, string, *attemptcoordinator.Service, jobResponse, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	activateH3StageGraph(t, database)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Stage graph cancellation scope: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"race durable stage graph authority"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit cancellable Stage graph status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode cancellable Stage graph Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct cancellable AttemptCoordinator: %v", err)
	}
	claim := readStageGraphInstantiation(t, database.Admin, job.JobID)
	attemptID := claim.AttemptID
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read cancellable StageRuns: %v", err)
	}
	return database, server.URL, coordinator, job, attemptID, encoderRunID, ditRunID
}

func assignAndStartEncoder(
	t *testing.T,
	database testDatabase,
	coordinator *attemptcoordinator.Service,
	attemptID, encoderRunID uuid.UUID,
	leaseExpiresAt time.Time,
) attemptcoordinator.AssignStageCommand {
	t.Helper()
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, leaseExpiresAt,
	)
	started, err := coordinator.Apply(context.Background(), attemptcoordinator.StartStageCommand{
		CommandID:            uuid.New(),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		StageAttemptID:       assignment.StageAttemptID,
		StageLeaseID:         assignment.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 2,
		StartedAt:            assignment.IssuedAt.Add(time.Millisecond),
	})
	if err != nil {
		t.Fatalf("start physical cancellation Encoder: %v", err)
	}
	if started.State != "RUNNING" || started.StageVersion != 3 {
		t.Fatalf("physical cancellation Encoder start = %#v", started)
	}
	return assignment
}

func assignEncoder(
	t *testing.T,
	database testDatabase,
	coordinator *attemptcoordinator.Service,
	attemptID, encoderRunID uuid.UUID,
	leaseExpiresAt time.Time,
) attemptcoordinator.AssignStageCommand {
	t.Helper()
	leaseExpiresAt = leaseExpiresAt.UTC().Truncate(time.Microsecond)
	seedWorkerRegistryPlan(t, database.Admin)
	poolID := uuid.New()
	workerID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, $2, $3, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
	`, poolID, "h3-encoder-physical-cancel", encoderStageProfileID); err != nil {
		t.Fatalf("seed physical cancellation Encoder CapacityPool: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, workerID, encoderWorkerProfileID, poolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed physical cancellation Encoder WorkerInstance: %v", err)
	}
	evidence := workerRegistryEvidenceValue(t, workerID, 0xb0)
	evidence.Residencies[0].ModelComponentRevision = "h3-encoder-v1"
	workerSPIFFEID := "spiffe://vela/worker/" + workerID.String()
	workerSPIFFEDigest := sha256.Sum256([]byte(workerSPIFFEID))
	evidence.Members[0].IdentityDigest = hex.EncodeToString(workerSPIFFEDigest[:])
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	registry, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct physical cancellation Worker Registry: %v", err)
	}
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe physical cancellation Encoder WorkerInstance: %v", err)
	}
	seedModelRuntimeCapacityRoute(
		t, database.Admin, workerID, evidence.Residencies[0].ID,
		poolID, uuid.MustParse(encoderStageProfileID),
	)
	authority := workerAuthority(t, evidence)
	issuedAt := time.Now().UTC().Truncate(time.Millisecond)
	if !leaseExpiresAt.After(issuedAt.Add(100 * time.Millisecond)) {
		t.Fatalf("physical cancellation Lease deadline %s is too close to issue time %s", leaseExpiresAt, issuedAt)
	}
	leaseToken := bytesOf(0xb3, 32)
	leaseTokenDigest := sha256.Sum256(leaseToken)
	assignment := attemptcoordinator.AssignStageCommand{
		CommandID:              uuid.New(),
		AttemptID:              attemptID,
		StageRunID:             encoderRunID,
		ExpectedAttemptFence:   1,
		ExpectedStageFence:     1,
		ExpectedStageVersion:   1,
		StageAttemptID:         uuid.New(),
		StageAllocationID:      uuid.New(),
		StageLeaseID:           uuid.New(),
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		CapacityPoolID:         poolID,
		WorkerInstanceID:       workerID,
		WorkerInstanceEpoch:    authority.InstanceEpoch,
		ObservationSequence:    evidence.Capacity.Sequence,
		DeviceSetDigest:        authority.DeviceSetDigest,
		MembershipDigest:       authority.MembershipDigest,
		ModelResidencyID:       authority.ModelResidencyID,
		ModelRuntimeEpoch:      authority.ModelRuntimeEpoch,
		CapacityVector:         map[string]int64{"concurrency": 1},
		TokenDigest:            leaseTokenDigest[:],
		SigningKeyID:           "stage-authority-key-v1",
		ExecutionNonce:         bytesOf(0xb4, 32),
		IssuedAt:               issuedAt,
		ExpiresAt:              leaseExpiresAt,
		LocalDeadlineAt:        leaseExpiresAt.Add(-50 * time.Millisecond),
	}
	assigned, err := coordinator.Apply(context.Background(), assignment)
	if err != nil {
		t.Fatalf("assign physical cancellation Encoder: %v", err)
	}
	if assigned.State != "ASSIGNED" || assigned.StageVersion != 2 {
		t.Fatalf("physical cancellation Encoder assignment = %#v", assigned)
	}
	return assignment
}

func instantiateAdditionalStageGraphJob(
	t *testing.T,
	database testDatabase,
	serverURL string,
	coordinator *attemptcoordinator.Service,
	idempotencyKey string,
) (jobResponse, uuid.UUID, uuid.UUID) {
	t.Helper()
	accepted := submitJob(t, serverURL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"prove physical Stage capacity fencing"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit capacity reuse Stage graph status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode capacity reuse Stage graph Job: %v", err)
	}
	claim := readStageGraphInstantiation(t, database.Admin, job.JobID)
	attemptID := claim.AttemptID
	var encoderRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id
		FROM stage_runs
		WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, attemptID).Scan(&encoderRunID); err != nil {
		t.Fatalf("read capacity reuse Encoder StageRun: %v", err)
	}
	return job, attemptID, encoderRunID
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for index := range result {
		result[index] = value
	}
	return result
}

func seedEncoderAssignmentProfile(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			$1, 'h3-encoder-dedicated', 1, 'CERTIFIED', 1, 1,
			'{"kind":"single-gpu"}', '["h3-encoder-v1"]',
			'{"concurrency":1}', '{"warmup":true}', decode(repeat('73', 32), 'hex')
		)
	`, encoderWorkerProfileID); err != nil {
		t.Fatalf("seed Encoder WorkerProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest, worker_profile_revision_id,
			result_equivalence_revision_id, certified_capacity_vector, content_digest
		) VALUES (
			$1, 'h3-encoder-dedicated', 1, 'CERTIFIED',
			'49000000-0000-0000-0000-000000000030', 'h3-encoder-v1',
			'sha256:3333333333333333333333333333333333333333333333333333333333333333',
			$2, '49000000-0000-0000-0000-000000000023',
			'{"concurrency":1}', decode(repeat('74', 32), 'hex')
		)
	`, encoderStageProfileID, encoderWorkerProfileID); err != nil {
		t.Fatalf("seed Encoder StageProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES (
			$1, $2, 'encoder', '49000000-0000-0000-0000-000000000030',
			$3, -1, '{"dedicated_model":true}'
		)
	`, graphExecutionProfileID, stageGraphID, encoderStageProfileID); err != nil {
		t.Fatalf("seed Encoder StageProfile option: %v", err)
	}
}

func seedDiTAssignmentProfile(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			$1, 'h3-dit-dedicated', 1, 'CERTIFIED', 1, 1,
			'{"kind":"single-gpu"}', '["h3-dit-v1"]',
			'{"concurrency":1}', '{"warmup":true}', decode(repeat('75', 32), 'hex')
		)
	`, ditWorkerProfileID); err != nil {
		t.Fatalf("seed DiT WorkerProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest, worker_profile_revision_id,
			result_equivalence_revision_id, certified_capacity_vector, content_digest
		) VALUES (
			$1, 'h3-dit-dedicated', 1, 'CERTIFIED',
			'49000000-0000-0000-0000-000000000031', 'h3-dit-v1',
			'sha256:4444444444444444444444444444444444444444444444444444444444444444',
			$2, '49000000-0000-0000-0000-000000000024',
			'{"concurrency":1}', decode(repeat('76', 32), 'hex')
		)
	`, ditStageProfileID, ditWorkerProfileID); err != nil {
		t.Fatalf("seed DiT StageProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES (
			$1, $2, 'dit', '49000000-0000-0000-0000-000000000031',
			$3, -1, '{"dedicated_model":true}'
		)
	`, graphExecutionProfileID, stageGraphID, ditStageProfileID); err != nil {
		t.Fatalf("seed DiT StageProfile option: %v", err)
	}
}

func activateH3StageGraph(t *testing.T, database testDatabase) {
	t.Helper()
	seedH3ProfileCertification(t, database.Admin)
	seedH3AdmissionCapacityPath(t, database)
	activateH3ExecutionGraph(t, database)
	activateStageCutoverRevision(
		t, database, uuid.MustParse(stageCutoverRevisionID), 2,
		uuid.MustParse("00000000-0000-0000-0000-000000000049"),
		uuid.MustParse(stageGraphID), uuid.MustParse(graphExecutionProfileID),
		2<<30, "integration-stage-cutover",
	)
}

func seedH3AdmissionCapacityPath(t *testing.T, database testDatabase) {
	t.Helper()
	var hasModelRuntimeCapacityRoutes bool
	if err := database.Admin.QueryRow(`
		SELECT to_regclass('public.model_runtime_capacity_routes') IS NOT NULL
	`).Scan(&hasModelRuntimeCapacityRoutes); err != nil {
		t.Fatalf("inspect ModelRuntime CapacityPool route schema: %v", err)
	}
	stages := []struct {
		key          string
		profileID    string
		component    string
		poolID       uuid.UUID
		workerID     uuid.UUID
		identityByte byte
	}{
		{
			key: "encoder", profileID: "49000000-0000-0000-0000-000000000040",
			component:    "h3-encoder-v1",
			poolID:       uuid.MustParse("49300000-0000-0000-0000-000000000091"),
			workerID:     uuid.MustParse("49300000-0000-0000-0000-000000000094"),
			identityByte: 0x91,
		},
		{
			key: "dit", profileID: "49000000-0000-0000-0000-000000000041",
			component:    "h3-dit-v1",
			poolID:       uuid.MustParse("49300000-0000-0000-0000-000000000092"),
			workerID:     uuid.MustParse("49300000-0000-0000-0000-000000000095"),
			identityByte: 0x92,
		},
		{
			key: "vae", profileID: "49000000-0000-0000-0000-000000000042",
			component:    "h3-vae-v1",
			poolID:       uuid.MustParse("49300000-0000-0000-0000-000000000093"),
			workerID:     uuid.MustParse("49300000-0000-0000-0000-000000000096"),
			identityByte: 0x93,
		},
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_bundles (
			id, stable_id, plan_revision, desired_generation, observed_generation,
			lifecycle_state, layout_digest, approved_by
		) VALUES (
			$1, 'h3-admission-capacity-path', 'integration-ready-path-v1',
			1, 0, 'APPLYING', decode(repeat('90', 32), 'hex'), 'integration-fixture'
		)
	`, admissionCapacityBundle); err != nil {
		t.Fatalf("seed Admission capacity WorkerBundle: %v", err)
	}
	for _, stage := range stages {
		if _, err := database.Admin.Exec(`
			INSERT INTO capacity_pools (
				id, stable_id, stage_profile_revision_id, resource_class,
				security_class, region, max_ready_queue_depth, state
			) VALUES ($1, $2, $3, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
		`, stage.poolID, "h3-admission-ready-"+stage.key, stage.profileID); err != nil {
			t.Fatalf("seed %s Admission CapacityPool: %v", stage.key, err)
		}
		if _, err := database.Admin.Exec(`
			INSERT INTO worker_instances (
				id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
				lifecycle_state, reachability_state, instance_epoch,
				control_session_epoch, desired_member_count, desired_device_count
			) VALUES (
				$1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1
			)
		`,
			stage.workerID,
			workerRegistryProfileID,
			stage.poolID,
			admissionCapacityBundle,
		); err != nil {
			t.Fatalf("seed %s Admission WorkerInstance: %v", stage.key, err)
		}
	}

	registry, err := fleet.NewService(workerRegistryPoolForSchema(t, database))
	if err != nil {
		t.Fatalf("construct Admission capacity Worker Registry: %v", err)
	}
	for _, stage := range stages {
		evidence := workerRegistryEvidenceValue(t, stage.workerID, stage.identityByte)
		nodeID := uuid.NewSHA1(stage.workerID, []byte("admission-node"))
		deviceID := uuid.NewSHA1(stage.workerID, []byte("admission-device"))
		evidence.DeviceSet.Devices[0].ID = deviceID
		evidence.DeviceSet.Devices[0].ComputeNodeID = nodeID
		evidence.DeviceSet.Devices[0].NodeIdentity = "h3-admission-" + stage.key
		evidence.DeviceSet.Devices[0].GPUUUID = "GPU-" + stage.workerID.String()
		evidence.DeviceSet.Devices[0].PCIBDF = fmt.Sprintf(
			"0000:%02x:00.0", stage.identityByte,
		)
		evidence.Members[0].ComputeNodeID = nodeID
		evidence.Members[0].DeviceIDs = []uuid.UUID{deviceID}
		evidence.Residencies[0].ModelComponentRevision = stage.component
		evidence.Residencies[0].RuntimeIdentity =
			stage.key + "@sha256:admission-capacity"
		evidence.ObservedBy = "node-agent/h3-admission-" + stage.key
		if _, err := registry.Observe(context.Background(), evidence); err != nil {
			t.Fatalf("observe %s Admission READY capacity: %v", stage.key, err)
		}
		if hasModelRuntimeCapacityRoutes {
			seedModelRuntimeCapacityRoute(
				t, database.Admin, stage.workerID, evidence.Residencies[0].ID,
				stage.poolID, uuid.MustParse(stage.profileID),
			)
		}
	}
}

func activateH3ExecutionGraph(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions
		SET state = 'CERTIFIED'
		WHERE stable_id IN ('h3-conditioning-l2', 'h3-latent-l2')
	`); err != nil {
		t.Fatalf("certify H3 graph connectors: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE execution_profile_revisions
		SET state = 'ACTIVE'
		WHERE id = $1
	`, graphExecutionProfileID); err != nil {
		t.Fatalf("certify H3 graph profile: %v", err)
	}
	activation := newRolePool(
		t,
		database.DSN,
		"vela_stage_catalog_activation_login",
		"vela-stage-catalog-activation-password",
	)
	var graphDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT content_digest FROM execution_graph_revisions WHERE id = $1
	`, stageGraphID).Scan(&graphDigest); err != nil {
		t.Fatalf("read H3 graph digest: %v", err)
	}
	var state string
	var order []string
	if err := activation.QueryRow(context.Background(), `
		SELECT state::text, topological_order
		FROM vela_activate_execution_graph($1, $2)
	`, stageGraphID, graphDigest).Scan(&state, &order); err != nil {
		t.Fatalf("activate H3 Stage graph: %v", err)
	}
	if state != "ACTIVE" || len(order) != 3 {
		t.Fatalf("activated H3 Stage graph state/order = %s/%v", state, order)
	}
}

func activateStageCutoverRevision(
	t *testing.T,
	database testDatabase,
	cutoverID uuid.UUID,
	revision int64,
	previousRevisionID uuid.UUID,
	graphID uuid.UUID,
	profileID uuid.UUID,
	reservedStorageBytes int64,
	configurationRevision string,
) {
	t.Helper()
	var connectorSetDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT vela_execution_profile_connector_set_digest($1, $2)
	`, profileID, graphID).Scan(&connectorSetDigest); err != nil {
		t.Fatalf("read Stage connector-set digest: %v", err)
	}
	promotion := newRolePool(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	var activatedID uuid.UUID
	if err := promotion.QueryRow(context.Background(), `
		SELECT (vela_activate_stage_cutover(
			$1, $2, $3,
			'INTERNAL', 'STAGE_ONLY', 10000, $4, $5, $6, 1,
			decode(repeat('a1', 32), 'hex'), $7,
			sha256(convert_to($7, 'UTF8')),
			$8, NULL, 'integration-catalog-promotion',
			'activate the integration Stage graph'
		)).id
	`, cutoverID, revision, previousRevisionID, graphID, profileID,
		reservedStorageBytes, configurationRevision, connectorSetDigest).Scan(&activatedID); err != nil {
		t.Fatalf("activate Stage cutover: %v", err)
	}
	if activatedID != cutoverID {
		t.Fatalf("activated Stage cutover id = %s, want %s", activatedID, cutoverID)
	}
	authorizeInternalCutoverProject(t, promotion, cutoverID)
}
