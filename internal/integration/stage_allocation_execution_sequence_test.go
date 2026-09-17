//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagescheduler"
)

func TestStageAllocationExecutionSequenceAssignedByDatabase(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "allocation-execution-sequence")
	// Isolate the version-88 downgrade guard before newer migrations add their own guards.
	if err := goose.DownTo(fixture.database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 88); err != nil {
		t.Fatal(err)
	}
	service, err := stagescheduler.NewService(fixture.repository, fixture.coordinator, stagescheduler.Config{
		SchedulerID: "allocation-execution-sequence", ClaimTTL: 30 * time.Second,
		LeaseTTL: time.Minute, LocalDeadlineTTL: 50 * time.Second, SigningKeyID: "stage-authority-key-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Acquire(context.Background(), fixture.authority, fixture.observation); err != nil {
		t.Fatal(err)
	}
	var sequence sql.NullInt64
	if err := fixture.database.Admin.QueryRow(`SELECT (to_jsonb(allocation)->>'execution_sequence')::bigint
		FROM stage_allocations AS allocation WHERE stage_run_id = $1`, fixture.stageRunID).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if !sequence.Valid || sequence.Int64 <= 0 {
		t.Fatalf("durable allocation execution_sequence = %v, want database-assigned positive sequence", sequence)
	}
	var allocationID, leaseID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`SELECT allocation.id, lease.id FROM stage_allocations allocation
		JOIN stage_leases lease ON lease.stage_allocation_id = allocation.id
		WHERE allocation.stage_run_id = $1`, fixture.stageRunID).Scan(&allocationID, &leaseID); err != nil {
		t.Fatal(err)
	}
	for _, update := range []string{
		`UPDATE stage_allocations SET execution_sequence = execution_sequence + 1 WHERE id = $1`,
		`UPDATE stage_allocations SET execution_sequence = NULL WHERE id = $1`,
	} {
		err := executionSequenceOwnerMutation(t, fixture.database.Admin, update, allocationID)
		requireExecutionSequenceConstraint(t, err, "stage_allocation_identity_immutable")
	}
	err = executionSequenceOwnerMutation(t, fixture.database.Admin, `INSERT INTO stage_allocations
		SELECT allocation.* FROM stage_allocations allocation WHERE id = $1`, allocationID)
	requireExecutionSequenceConstraint(t, err, "stage_allocation_execution_sequence_database_assigned")
	pool := newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	var actual sql.NullInt64
	if err := pool.QueryRow(context.Background(), `SELECT vela_read_stage_allocation_execution_sequence($1,$2)`,
		leaseID, allocationID).Scan(&actual); err != nil || !actual.Valid || actual.Int64 != sequence.Int64 {
		t.Fatalf("role-scoped allocation sequence = %v error=%v", actual, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT vela_read_stage_allocation_execution_sequence($1,$2)`,
		leaseID, uuid.New()).Scan(&actual); err != nil || actual.Valid {
		t.Fatalf("mismatched allocation sequence = %v error=%v", actual, err)
	}
	for _, statement := range []string{
		`SELECT nextval('public.stage_allocation_execution_sequence')`,
		`SELECT setval('public.stage_allocation_execution_sequence', 1, false)`,
		`UPDATE stage_allocations SET execution_sequence = NULL`,
		`ALTER TABLE stage_allocations DISABLE TRIGGER stage_allocations_assign_execution_sequence`,
	} {
		_, err := pool.Exec(context.Background(), statement)
		var pgError *pgconn.PgError
		if !errors.As(err, &pgError) || pgError.Code != "42501" {
			t.Fatalf("ordinary runtime role mutation %q error=%v, want insufficient_privilege", statement, err)
		}
	}
	err = goose.DownTo(fixture.database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 87)
	requireExecutionSequenceConstraint(t, err, "stage_allocation_execution_sequence_downgrade_unsafe")
	if version, err := goose.GetDBVersion(fixture.database.Admin); err != nil || version != 88 {
		t.Fatalf("blocked downgrade version=%d error=%v", version, err)
	}
}

func TestStageAllocationExecutionSequenceWaitsForWorkerAndSurvivesRollback(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "allocation-sequence-lock-rollback")
	service, err := stagescheduler.NewService(fixture.repository, panickingStageCoordinator{}, stagescheduler.Config{
		SchedulerID: "allocation-sequence-lock-rollback", ClaimTTL: 30 * time.Second,
		LeaseTTL: time.Minute, LocalDeadlineTTL: 50 * time.Second, SigningKeyID: "stage-authority-key-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != "simulated StageScheduler process crash" {
				t.Fatalf("expected interruption after durable claim, got %v", recovered)
			}
		}()
		_, _, _ = service.Acquire(context.Background(), fixture.authority, fixture.observation)
	}()
	var command []byte
	var allocationID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`SELECT command_payload, stage_allocation_id
		FROM stage_scheduler_claims WHERE stage_run_id = $1`, fixture.stageRunID).Scan(&command, &allocationID); err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM worker_instances WHERE id = $1 FOR UPDATE`, fixture.authority.WorkerInstanceID); err != nil {
		t.Fatal(err)
	}
	var beforeValue int64
	var beforeCalled bool
	if err := fixture.database.Admin.QueryRow(`SELECT last_value, is_called FROM stage_allocation_execution_sequence`).Scan(&beforeValue, &beforeCalled); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := newRolePool(t, fixture.database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password")
	result := make(chan error, 1)
	go func() {
		var state string
		err := pool.QueryRow(ctx, `SELECT stage_state FROM vela_apply_stage_command($1::jsonb)`, command).Scan(&state)
		if err == nil && state != "ASSIGNED" {
			err = errors.New("waiting assignment did not become ASSIGNED")
		}
		result <- err
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_attempt_coordinator_login")
	var waitingValue int64
	var waitingCalled bool
	if err := fixture.database.Admin.QueryRow(`SELECT last_value, is_called FROM stage_allocation_execution_sequence`).Scan(&waitingValue, &waitingCalled); err != nil {
		t.Fatal(err)
	}
	if waitingValue != beforeValue || waitingCalled != beforeCalled {
		t.Fatalf("waiting ASSIGN consumed sequence before Worker lock: (%d,%t) -> (%d,%t)",
			beforeValue, beforeCalled, waitingValue, waitingCalled)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_login`); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := tx.QueryRow(`SELECT stage_state FROM vela_apply_stage_command($1::jsonb)`, command).Scan(&state); err != nil || state != "ASSIGNED" {
		t.Fatalf("first real ASSIGN state=%s error=%v", state, err)
	}
	if _, err := tx.Exec(`RESET ROLE`); err != nil {
		t.Fatal(err)
	}
	var rolledBackSequence int64
	if err := tx.QueryRow(`SELECT execution_sequence FROM stage_allocations WHERE id = $1`, allocationID).Scan(&rolledBackSequence); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	var committedSequence int64
	if err := fixture.database.Admin.QueryRow(`SELECT execution_sequence FROM stage_allocations WHERE id = $1`, allocationID).Scan(&committedSequence); err != nil {
		t.Fatal(err)
	}
	if committedSequence <= rolledBackSequence || rolledBackSequence <= 0 {
		t.Fatalf("rollback sequence=%d committed=%d, want strictly increasing", rolledBackSequence, committedSequence)
	}
	var replayed bool
	if err := pool.QueryRow(ctx, `SELECT replayed FROM vela_apply_stage_command($1::jsonb)`, command).Scan(&replayed); err != nil || !replayed {
		t.Fatalf("exact ASSIGN replay=%t error=%v", replayed, err)
	}
	if err := fixture.database.Admin.QueryRow(`SELECT last_value FROM stage_allocation_execution_sequence`).Scan(&waitingValue); err != nil {
		t.Fatal(err)
	}
	if waitingValue != committedSequence {
		t.Fatalf("exact ASSIGN replay allocated another sequence: %d -> %d", committedSequence, waitingValue)
	}
}

func TestStageAllocationExecutionSequenceDowngradeRetainsRolledBackIssuance(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "allocation-sequence-downgrade-gap")
	service, err := stagescheduler.NewService(fixture.repository, panickingStageCoordinator{}, stagescheduler.Config{
		SchedulerID: "allocation-sequence-downgrade-gap", ClaimTTL: 30 * time.Second,
		LeaseTTL: time.Minute, LocalDeadlineTTL: 50 * time.Second, SigningKeyID: "stage-authority-key-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != "simulated StageScheduler process crash" {
				t.Fatalf("expected interruption after durable claim, got %v", recovered)
			}
		}()
		_, _, _ = service.Acquire(context.Background(), fixture.authority, fixture.observation)
	}()
	var command []byte
	if err := fixture.database.Admin.QueryRow(`SELECT command_payload
		FROM stage_scheduler_claims WHERE stage_run_id = $1`, fixture.stageRunID).Scan(&command); err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_login`); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := tx.QueryRow(`SELECT stage_state FROM vela_apply_stage_command($1::jsonb)`, command).Scan(&state); err != nil || state != "ASSIGNED" {
		t.Fatalf("real ASSIGN before rollback: state=%s error=%v", state, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var allocations int
	if err := fixture.database.Admin.QueryRow(`SELECT count(*) FROM stage_allocations`).Scan(&allocations); err != nil || allocations != 0 {
		t.Fatalf("allocation rows after rollback=%d error=%v", allocations, err)
	}
	err = goose.DownTo(fixture.database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 87)
	requireExecutionSequenceConstraint(t, err, "stage_allocation_execution_sequence_downgrade_unsafe")
}

func TestStageAllocationExecutionSequenceAssignmentRenewalAndRetry(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "allocation-sequence-renewal-retry")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	ctx := context.Background()
	command := stageWorkerAcquireCommand(fixture)
	request := stageWorkerAcquireRequest(fixture)
	result, err := backend.AcquireStage(ctx, command, request)
	if err != nil || result.Assignment == nil {
		t.Fatalf("AcquireStage = %#v error=%v", result, err)
	}
	authority := result.Assignment.GetAuthority()
	if authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 || authority.GetExecutionSequence() <= 0 {
		t.Fatalf("signed StageAuthority schema=%d sequence=%d", authority.GetSchemaVersion(), authority.GetExecutionSequence())
	}
	var sequence int64
	if err := fixture.database.Admin.QueryRow(`SELECT execution_sequence FROM stage_allocations WHERE id = $1`,
		authority.GetStageAllocationId()).Scan(&sequence); err != nil || sequence != authority.GetExecutionSequence() {
		t.Fatalf("durable sequence=%d signed=%d error=%v", sequence, authority.GetExecutionSequence(), err)
	}
	validator, err := stageauthority.NewValidator(map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	}, func() time.Time { return authority.GetIssuedAt().AsTime().Add(time.Millisecond) })
	if err != nil {
		t.Fatal(err)
	}
	verified, err := validator.ValidateEnvelope(authority)
	if err != nil {
		t.Fatal(err)
	}
	assignment := attemptcoordinator.AssignStageCommand{
		WorkerInstanceID: uuid.MustParse(authority.GetWorkerInstanceId()),
		IssuedAt:         authority.GetIssuedAt().AsTime(),
	}
	started := startH3IntegrationStage(t, fixture.database, assignment, verified)
	if started.Authority.GetExecutionSequence() != sequence {
		t.Fatalf("START renewal changed execution sequence to %d", started.Authority.GetExecutionSequence())
	}
	failedAt := time.Now().Add(20 * time.Millisecond)
	failure := attemptcoordinator.FailStageCommand{
		CommandID: uuid.New(), AttemptID: uuid.MustParse(authority.GetAttemptId()),
		StageRunID: uuid.MustParse(authority.GetStageRunId()), StageAttemptID: uuid.MustParse(authority.GetStageAttemptId()),
		StageLeaseID:         uuid.MustParse(authority.GetStageLeaseId()),
		ExpectedAttemptFence: authority.GetAttemptFence(), ExpectedStageFence: authority.GetStageFence(),
		ExpectedStageVersion: started.Authority.GetStageVersion(), FailureClass: "TRANSIENT_BACKEND",
		FailureFingerprint: bytes.Repeat([]byte{0x77}, 32), ConsumedResourceUnits: 1,
		FailedAt: failedAt, RetryAt: failedAt.Add(time.Millisecond),
	}
	decision, err := fixture.coordinator.Apply(ctx, failure)
	if err != nil || decision.State != "RETRY_WAIT" {
		t.Fatalf("FailStage = %#v error=%v", decision, err)
	}
	waitStageExpiry(t, failure.RetryAt)
	if _, err := fixture.coordinator.Reconcile(ctx, 10); err != nil {
		t.Fatal(err)
	}
	next, err := backend.AcquireStage(ctx, stageWorkerAcquireCommand(fixture), request)
	if err != nil || next.Assignment == nil {
		t.Fatalf("retry AcquireStage = %#v error=%v", next, err)
	}
	nextAuthority := next.Assignment.GetAuthority()
	if nextAuthority.GetExecutionSequence() <= sequence || nextAuthority.GetStageAttemptId() == authority.GetStageAttemptId() ||
		nextAuthority.GetWorkerInstanceId() != authority.GetWorkerInstanceId() {
		t.Fatalf("retry authority sequence=%d previous=%d physical=%s previous=%s worker=%s",
			nextAuthority.GetExecutionSequence(), sequence, nextAuthority.GetStageAttemptId(), authority.GetStageAttemptId(),
			nextAuthority.GetWorkerInstanceId())
	}
}

func TestStageAllocationExecutionSequenceAllowsDifferentWorkerAllocation(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	seedWorkerRegistryPlan(t, database.Admin)
	var payloads [2][]byte
	var allocations [2]uuid.UUID
	for index := range 2 {
		_, attemptID := instantiateH3IntegrationGraph(t, database, serverURL, "parallel-execution-sequence-"+uuid.NewString())
		var runID uuid.UUID
		if err := database.Admin.QueryRow(`SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'`, attemptID).Scan(&runID); err != nil {
			t.Fatal(err)
		}
		stage := h3IntegrationStages([]string{uuid.NewString(), "unused-dit", "unused-vae"}, nil)[0]
		fixture := newH3AssignmentWorkerFixture(t, database, coordinator, runID, stage, byte(0xe0+index))
		service, err := stagescheduler.NewService(fixture.repository, panickingStageCoordinator{}, stagescheduler.Config{
			SchedulerID: "parallel-execution-sequence", ClaimTTL: 30 * time.Second,
			LeaseTTL: time.Minute, LocalDeadlineTTL: 50 * time.Second, SigningKeyID: "stage-authority-key-v1",
		})
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != "simulated StageScheduler process crash" {
					t.Fatalf("expected interruption after durable claim, got %v", recovered)
				}
			}()
			_, _, _ = service.Acquire(context.Background(), fixture.authority, fixture.observation)
		}()
		if err := database.Admin.QueryRow(`SELECT command_payload, stage_allocation_id FROM stage_scheduler_claims
			WHERE stage_run_id = $1`, runID).Scan(&payloads[index], &allocations[index]); err != nil {
			t.Fatal(err)
		}
	}
	first, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback()
	if _, err := first.Exec(`SET LOCAL ROLE vela_attempt_coordinator_login`); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := first.QueryRow(`SELECT stage_state FROM vela_apply_stage_command($1::jsonb)`, payloads[0]).Scan(&state); err != nil || state != "ASSIGNED" {
		t.Fatalf("first Worker ASSIGN state=%s error=%v", state, err)
	}
	var beforeSecond int64
	if err := database.Admin.QueryRow(`SELECT last_value FROM stage_allocation_execution_sequence`).Scan(&beforeSecond); err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		var secondState string
		err := pool.QueryRow(ctx, `SELECT stage_state FROM vela_apply_stage_command($1::jsonb)`, payloads[1]).Scan(&secondState)
		if err == nil && secondState != "ASSIGNED" {
			err = errors.New("second Worker did not become ASSIGNED")
		}
		result <- err
	}()
	// Both StageRuns are eligible for both pools, so the existing READY queue
	// projection shares counters. The new sequence must allocate before that wait.
	waitForRoleDatabaseLock(t, database.Admin, "vela_attempt_coordinator_login")
	var afterSecond int64
	var tupleRelations string
	if err := database.Admin.QueryRow(`SELECT last_value FROM stage_allocation_execution_sequence`).Scan(&afterSecond); err != nil {
		t.Fatal(err)
	}
	if err := database.Admin.QueryRow(`SELECT COALESCE(string_agg(lock.relation::regclass::text, ','), '')
		FROM pg_locks lock JOIN pg_stat_activity activity ON activity.pid = lock.pid
		WHERE activity.usename = 'vela_attempt_coordinator_login' AND activity.wait_event_type = 'Lock'
		AND lock.locktype = 'tuple'`).Scan(&tupleRelations); err != nil {
		t.Fatal(err)
	}
	t.Logf("different Worker sequence %d -> %d before first commit; waiting tuple relations=%s", beforeSecond, afterSecond, tupleRelations)
	if afterSecond <= beforeSecond {
		t.Fatalf("global sequence blocked a different Worker while the first transaction remained open: %d -> %d; tuple relations=%s",
			beforeSecond, afterSecond, tupleRelations)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	var firstSequence, secondSequence int64
	var firstWorker, secondWorker uuid.UUID
	if err := database.Admin.QueryRow(`SELECT first.execution_sequence, second.execution_sequence,
		first.worker_instance_id, second.worker_instance_id FROM stage_allocations first, stage_allocations second
		WHERE first.id = $1 AND second.id = $2`, allocations[0], allocations[1]).
		Scan(&firstSequence, &secondSequence, &firstWorker, &secondWorker); err != nil {
		t.Fatal(err)
	}
	if firstSequence <= 0 || secondSequence <= firstSequence || firstWorker == secondWorker {
		t.Fatalf("independent allocations worker=%s/%s sequence=%d/%d", firstWorker, secondWorker, firstSequence, secondSequence)
	}
}

func requireExecutionSequenceConstraint(t *testing.T, err error, constraint string) {
	t.Helper()
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.ConstraintName != constraint {
		t.Fatalf("error=%v, want PostgreSQL constraint %s", err, constraint)
	}
}

func executionSequenceOwnerMutation(t *testing.T, database *sql.DB, statement string, arguments ...any) error {
	t.Helper()
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(statement, arguments...)
	return err
}
