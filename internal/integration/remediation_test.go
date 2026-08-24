//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestRemediationOperationIsBoundedIdempotentAndAudited(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}

	evidence := sha256.Sum256([]byte("worker fault evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001920"),
		WorkerID:              workerID,
		WorkerEpoch:           1,
		NodeIdentity:          "node-remediation-1",
		DeviceIdentity:        "GPU-REM-0",
		FailureClass:          "CUDA_CONTEXT_STALE",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "matrix-v1",
		ActionLevel:           remediation.ActionL0ProcessRestart,
		IdempotencyKey:        "remediation-idempotency-1",
		RequestedBy:           "node-agent-1",
	}
	created, err := service.Request(context.Background(), request)
	if err != nil || created.Replayed || created.State != remediation.StateRequested ||
		string(created.WorkerLifecycleState) != "DRAINING" {
		t.Fatalf("request Remediation = %#v error=%v", created, err)
	}
	replayed, err := service.Request(context.Background(), request)
	if err != nil || !replayed.Replayed || replayed.OperationID != request.OperationID {
		t.Fatalf("replay Remediation = %#v error=%v", replayed, err)
	}

	_, err = service.Start(context.Background(), request.OperationID, workerID, 2, "node-agent-1")
	assertRemediationFailure(t, err, remediation.FailureConflict)
	started, err := service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1")
	if err != nil || started.State != remediation.StateExecuting {
		t.Fatalf("start Remediation = %#v error=%v", started, err)
	}
	postcheck := sha256.Sum256([]byte("post-check"))
	completed, err := service.Complete(context.Background(), remediation.Completion{
		OperationID: request.OperationID, WorkerID: workerID, WorkerEpoch: 1,
		Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "process restarted and checks passed",
		PostcheckHash: postcheck[:], ActorIdentity: "node-agent-1",
	})
	if err != nil || completed.State != remediation.StateSucceeded ||
		string(completed.WorkerLifecycleState) != "READY" || string(completed.WorkerReachability) != "HEALTHY" {
		t.Fatalf("complete Remediation = %#v error=%v", completed, err)
	}
	operation, err := service.Get(context.Background(), request.OperationID)
	if err != nil || operation.State != remediation.StateSucceeded || operation.ResultCode != "POSTCHECK_OK" {
		t.Fatalf("get Remediation operation = %#v error=%v", operation, err)
	}
	var events int
	if err := database.Admin.QueryRow(
		"SELECT count(*) FROM remediation_operation_events WHERE operation_id = $1",
		request.OperationID,
	).Scan(&events); err != nil {
		t.Fatalf("count Remediation audit events: %v", err)
	}
	if events != 3 {
		t.Fatalf("Remediation audit event count = %d, want 3", events)
	}
	assertRemediationSQLState(t, database.Admin, `
		UPDATE remediation_operations
		SET device_identity = 'GPU-REM-MUTATED'
		WHERE id = $1
	`, request.OperationID, "55000")
	assertRemediationSQLState(t, database.Admin, `
		DELETE FROM remediation_operations WHERE id = $1
	`, request.OperationID, "55000")
	assertRemediationSQLState(t, database.Admin, `
		UPDATE remediation_operation_events
		SET result_code = 'MUTATED'
		WHERE operation_id = $1 AND sequence = 1
	`, request.OperationID, "55000")
	assertRemediationSQLState(t, database.Admin, `
		DELETE FROM remediation_operation_events
		WHERE operation_id = $1 AND sequence = 1
	`, request.OperationID, "55000")

	conflicting := request
	conflicting.OperationID = uuid.MustParse("10000000-0000-0000-0000-000000001921")
	conflicting.DeviceIdentity = "GPU-REM-1"
	_, err = service.Request(context.Background(), conflicting)
	assertRemediationFailure(t, err, remediation.FailureConflict)
}

func TestRemediationL6RequiresTwoApproversAndQuarantinesOnFailure(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("BMC evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001922"),
		WorkerID:              workerID,
		WorkerEpoch:           1,
		NodeIdentity:          "node-remediation-1",
		DeviceIdentity:        "PCI-0000:01:00.0",
		FailureClass:          "NODE_UNRESPONSIVE",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "matrix-v2",
		ActionLevel:           remediation.ActionL6BMCPowerCycle,
		IdempotencyKey:        "remediation-l6-1",
		RequestedBy:           "node-agent-1",
	}
	created, err := service.Request(context.Background(), request)
	if err != nil || created.State != remediation.StateApprovalRequired || !created.RequiresApproval {
		t.Fatalf("request L6 Remediation = %#v error=%v", created, err)
	}
	_, err = service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1")
	assertRemediationFailure(t, err, remediation.FailureConflict)
	first, err := service.Approve(context.Background(), request.OperationID, "operator-a")
	if err != nil || first.ApprovalCount != 1 || !first.RequiresApproval {
		t.Fatalf("first L6 approval = %#v error=%v", first, err)
	}
	_, err = service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1")
	assertRemediationFailure(t, err, remediation.FailureConflict)
	replayed, err := service.Approve(context.Background(), request.OperationID, "operator-a")
	if err != nil || !replayed.Replayed || replayed.ApprovalCount != 1 {
		t.Fatalf("replayed first L6 approval = %#v error=%v", replayed, err)
	}
	second, err := service.Approve(context.Background(), request.OperationID, "operator-b")
	if err != nil || second.ApprovalCount != 2 || second.State != remediation.StateRequested {
		t.Fatalf("second L6 approval = %#v error=%v", second, err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1"); err != nil {
		t.Fatalf("start approved L6 Remediation: %v", err)
	}
	completed, err := service.Complete(context.Background(), remediation.Completion{
		OperationID: request.OperationID, WorkerID: workerID, WorkerEpoch: 1,
		Success: false, ResultCode: "BMC_POSTCHECK_FAILED", ResultDetail: "node identity did not return",
		ActorIdentity: "node-agent-1",
	})
	if err != nil || completed.State != remediation.StateQuarantined ||
		string(completed.WorkerLifecycleState) != "QUARANTINED" || string(completed.WorkerReachability) != "OFFLINE" {
		t.Fatalf("failed L6 Remediation = %#v error=%v", completed, err)
	}
	var lifecycle, reachability string
	if err := database.Admin.QueryRow(
		"SELECT lifecycle_state, reachability_condition FROM workers WHERE id = $1", workerID,
	).Scan(&lifecycle, &reachability); err != nil {
		t.Fatalf("read quarantined Worker: %v", err)
	}
	if lifecycle != "QUARANTINED" || reachability != "OFFLINE" {
		t.Fatalf("quarantined Worker = %s/%s", lifecycle, reachability)
	}
}

func TestRemediationL7QuarantineIsImmediateAndEpochBound(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("quarantine evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001923"),
		WorkerID:              workerID,
		WorkerEpoch:           9,
		NodeIdentity:          "node-remediation-1",
		DeviceIdentity:        "GPU-REM-0",
		FailureClass:          "IDENTITY_AMBIGUOUS",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "fail-closed-v1",
		ActionLevel:           remediation.ActionL7Quarantine,
		IdempotencyKey:        "remediation-l7-wrong-epoch",
		RequestedBy:           "node-agent-1",
	}
	_, err = service.Request(context.Background(), request)
	assertRemediationFailure(t, err, remediation.FailureConflict)
	request.WorkerEpoch = 1
	request.IdempotencyKey = "remediation-l7-1"
	request.CertificationRevision = ""
	result, err := service.Request(context.Background(), request)
	if err != nil || result.State != remediation.StateQuarantined || result.Replayed {
		t.Fatalf("immediate L7 Remediation = %#v error=%v", result, err)
	}
	var startedAt, finishedAt sql.NullTime
	var resultCode string
	if err := database.Admin.QueryRow(`
		SELECT started_at, finished_at, result_code
		FROM remediation_operations WHERE id = $1
	`, request.OperationID).Scan(&startedAt, &finishedAt, &resultCode); err != nil {
		t.Fatalf("read immediate L7 operation: %v", err)
	}
	if !startedAt.Valid || !finishedAt.Valid || resultCode != "QUARANTINED_BY_POLICY" {
		t.Fatalf(
			"L7 terminal fields = started %t finished %t result %q",
			startedAt.Valid, finishedAt.Valid, resultCode,
		)
	}
	if _, err := service.Request(context.Background(), remediation.Request{
		OperationID: uuid.MustParse("10000000-0000-0000-0000-000000001929"),
		WorkerID:    workerID, WorkerEpoch: 1, NodeIdentity: request.NodeIdentity,
		DeviceIdentity: request.DeviceIdentity, FailureClass: "NEW_FAULT",
		EvidenceDigest: evidence[:], CertificationRevision: "matrix-v1",
		ActionLevel:    remediation.ActionL0ProcessRestart,
		IdempotencyKey: "remediation-after-quarantine", RequestedBy: "node-agent-1",
	}); err == nil {
		t.Fatal("new remediation was accepted for a quarantined Worker")
	} else {
		assertRemediationFailure(t, err, remediation.FailureConflict)
	}
	assertRemediationSQLState(t, database.Admin, `
		UPDATE workers SET lifecycle_state = 'READY' WHERE id = $1
	`, workerID, "55000")
}

func TestRemediationRequestFencesActiveLease(t *testing.T) {
	fixture := newAssignmentFixture(t, "remediation-fence-active-lease", 1)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 1, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create active Assignment: %v", err)
	}
	var nodeIdentity string
	if err := fixture.database.Admin.QueryRow(
		"SELECT node_identity FROM workers WHERE id = $1", assignment.WorkerID,
	).Scan(&nodeIdentity); err != nil {
		t.Fatalf("read Worker node identity: %v", err)
	}
	service, err := remediation.NewService(newRolePool(
		t, fixture.database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("active lease evidence"))
	request := remediation.Request{
		OperationID: uuid.MustParse("10000000-0000-0000-0000-000000001930"),
		WorkerID:    assignment.WorkerID, WorkerEpoch: 1, NodeIdentity: nodeIdentity,
		DeviceIdentity: "GPU-LEASE-0", FailureClass: "WORKER_LOST",
		EvidenceDigest: evidence[:], CertificationRevision: "matrix-lease-v1",
		ActionLevel:    remediation.ActionL0ProcessRestart,
		IdempotencyKey: "remediation-fence-active-lease-1", RequestedBy: "node-agent-lease",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request Remediation with active Lease: %v", err)
	}
	var revokedAt sql.NullTime
	if err := fixture.database.Admin.QueryRow(
		"SELECT revoked_at FROM attempt_leases WHERE attempt_id = $1", assignment.AttemptID,
	).Scan(&revokedAt); err != nil {
		t.Fatalf("read fenced Lease: %v", err)
	}
	if !revokedAt.Valid {
		t.Fatal("Remediation request left the active Worker Lease unfenced")
	}
	heartbeat, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(2),
	)
	if err != nil || heartbeat.Decision != workercontrol.HeartbeatStop ||
		heartbeat.StopReason != workercontrol.StopInvalidAuthority {
		t.Fatalf("heartbeat after Remediation fencing = %#v error=%v", heartbeat, err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, assignment.WorkerID, 1, request.RequestedBy); err != nil {
		t.Fatalf("start fenced Remediation: %v", err)
	}
	postcheck := sha256.Sum256([]byte("post-check-active-lease"))
	completed, err := service.Complete(context.Background(), remediation.Completion{
		OperationID: request.OperationID, WorkerID: assignment.WorkerID, WorkerEpoch: 1,
		Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "active Attempt remains",
		PostcheckHash: postcheck[:], ActorIdentity: request.RequestedBy,
	})
	if err != nil || completed.State != remediation.StateQuarantined {
		t.Fatalf("active Lease completion = %#v error=%v", completed, err)
	}
}

func TestRemediationRecoveryQuarantinesExpiredExecutingOperation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("orphaned execution evidence"))
	request := remediation.Request{
		OperationID: uuid.MustParse("10000000-0000-0000-0000-000000001931"),
		WorkerID:    workerID, WorkerEpoch: 1, NodeIdentity: "node-remediation-1",
		DeviceIdentity: "GPU-ORPHAN-0", FailureClass: "NODE_AGENT_LOST",
		EvidenceDigest: evidence[:], CertificationRevision: "matrix-orphan-v1",
		ActionLevel:    remediation.ActionL0ProcessRestart,
		IdempotencyKey: "remediation-orphaned-execution-1", RequestedBy: "node-agent-orphan",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request orphaned Remediation: %v", err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, workerID, 1, request.RequestedBy); err != nil {
		t.Fatalf("start orphaned Remediation: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin deadline fixture transaction: %v", err)
	}
	if _, err := tx.Exec("SET LOCAL session_replication_role = 'replica'"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("disable triggers for deadline fixture: %v", err)
	}
	if _, err := tx.Exec(
		"UPDATE remediation_operations SET deadline_at = requested_at + interval '1 millisecond' WHERE id = $1",
		request.OperationID,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("expire Remediation deadline fixture: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit deadline fixture: %v", err)
	}
	recovered, err := service.Recover(context.Background(), remediation.Recovery{
		OperationID: request.OperationID, ActorIdentity: "remediation-reconciler",
	})
	if err != nil || recovered.State != remediation.StateQuarantined ||
		recovered.ResultCode != "REMEDIATION_DEADLINE_EXPIRED" {
		t.Fatalf("recovered orphaned Remediation = %#v error=%v", recovered, err)
	}
	replayed, err := service.Recover(context.Background(), remediation.Recovery{
		OperationID: request.OperationID, ActorIdentity: "remediation-reconciler",
	})
	if err != nil || !replayed.Replayed || replayed.State != remediation.StateQuarantined {
		t.Fatalf("replayed recovery = %#v error=%v", replayed, err)
	}
}

func TestRemediationMutationsShareWorkerLockOrder(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("lock order evidence"))
	request := remediation.Request{
		OperationID: uuid.MustParse("10000000-0000-0000-0000-000000001932"),
		WorkerID:    workerID, WorkerEpoch: 1, NodeIdentity: "node-remediation-1",
		DeviceIdentity: "GPU-LOCK-0", FailureClass: "LOCK_ORDER_TEST",
		EvidenceDigest: evidence[:], CertificationRevision: "matrix-lock-v1",
		ActionLevel:    remediation.ActionL0ProcessRestart,
		IdempotencyKey: "remediation-lock-order-1", RequestedBy: "node-agent-lock",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request lock-order Remediation: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		_, callErr := service.Start(ctx, request.OperationID, workerID, 1, request.RequestedBy)
		errs <- callErr
	}()
	go func() {
		defer wait.Done()
		<-start
		_, callErr := service.Request(ctx, request)
		errs <- callErr
	}()
	close(start)
	wait.Wait()
	close(errs)
	for callErr := range errs {
		if callErr != nil {
			t.Fatalf("concurrent request/start lock-order call: %v", callErr)
		}
	}
	postcheck := sha256.Sum256([]byte("lock order post-check"))
	completed, err := service.Complete(ctx, remediation.Completion{
		OperationID: request.OperationID, WorkerID: workerID, WorkerEpoch: 1,
		Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "lock order complete",
		PostcheckHash: postcheck[:], ActorIdentity: request.RequestedBy,
	})
	if err != nil || completed.State != remediation.StateSucceeded {
		t.Fatalf("lock-order completion = %#v error=%v", completed, err)
	}
}

func TestRemediationCompletionQuarantinesWorkerWithActiveAttempt(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "remediation-active-attempt", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"keep this Attempt active during remediation"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit active Attempt Job status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var jobID uuid.UUID
	if err := database.Admin.QueryRow("SELECT id FROM jobs LIMIT 1").Scan(&jobID); err != nil {
		t.Fatalf("read active Attempt Job: %v", err)
	}
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000001926")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition, node_identity
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.example/worker/remediation-active-1', 1, 'READY', 'HEALTHY',
			'node-remediation-active-1'
		)
	`, workerID); err != nil {
		t.Fatalf("seed active Attempt Worker: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO attempts (
			id, organization_id, project_id, job_id, attempt_number,
			execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
			state, fence, assigned_at
		)
		SELECT '10000000-0000-0000-0000-000000001927', organization_id, project_id, id, 1,
			'00000000-0000-0000-0000-000000000014', worker_pool_id, $1, 1,
			'ASSIGNED', 1, clock_timestamp()
		FROM jobs WHERE id = $2
	`, workerID, jobID); err != nil {
		t.Fatalf("seed active Attempt: %v", err)
	}
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create active Attempt Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("active Attempt evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001928"),
		WorkerID:              workerID,
		WorkerEpoch:           1,
		NodeIdentity:          "node-remediation-active-1",
		DeviceIdentity:        "GPU-REM-ACTIVE-0",
		FailureClass:          "WORKER_LOST",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "matrix-active-attempt-v1",
		ActionLevel:           remediation.ActionL0ProcessRestart,
		IdempotencyKey:        "remediation-active-attempt-1",
		RequestedBy:           "node-agent-active-1",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request active Attempt Remediation: %v", err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, workerID, 1, request.RequestedBy); err != nil {
		t.Fatalf("start active Attempt Remediation: %v", err)
	}
	postcheck := sha256.Sum256([]byte("post-check-with-active-attempt"))
	completed, err := service.Complete(context.Background(), remediation.Completion{
		OperationID: request.OperationID, WorkerID: workerID, WorkerEpoch: 1,
		Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "post-check passed but Attempt remains active",
		PostcheckHash: postcheck[:], ActorIdentity: request.RequestedBy,
	})
	if err != nil || completed.State != remediation.StateQuarantined || completed.ResultCode != "ACTIVE_ATTEMPT_REMAINS" {
		t.Fatalf("active Attempt completion = %#v error=%v", completed, err)
	}
	var lifecycle, reachability string
	if err := database.Admin.QueryRow(
		"SELECT lifecycle_state, reachability_condition FROM workers WHERE id = $1", workerID,
	).Scan(&lifecycle, &reachability); err != nil {
		t.Fatalf("read active Attempt quarantined Worker: %v", err)
	}
	if lifecycle != "QUARANTINED" || reachability != "OFFLINE" {
		t.Fatalf("active Attempt Worker = %s/%s", lifecycle, reachability)
	}
}

func TestRemediationMigrationDownAndUpPreservesRoles(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 18); err != nil {
		t.Fatalf("Remediation migration down: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "remediation_operations")
	assertTableDoesNotExist(t, database.Admin, "remediation_operation_events")
	var nodeIdentityExists bool
	if err := database.Admin.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'workers' AND column_name = 'node_identity'
		)
	`).Scan(&nodeIdentityExists); err != nil {
		t.Fatalf("inspect node_identity after down: %v", err)
	}
	if nodeIdentityExists {
		t.Fatal("node_identity survived Remediation migration down")
	}
	assertRoleExists(t, database.Admin, "vela_remediation")
	assertRoleExists(t, database.Admin, "vela_remediation_owner")
	if err := goose.UpTo(database.Admin, migrations, 19); err != nil {
		t.Fatalf("Remediation migration up: %v", err)
	}
	assertTableExists(t, database.Admin, "remediation_operations")
	assertTableExists(t, database.Admin, "remediation_operation_events")
	if err := database.Admin.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'workers' AND column_name = 'node_identity'
		)
	`).Scan(&nodeIdentityExists); err != nil {
		t.Fatalf("inspect node_identity after up: %v", err)
	}
	if !nodeIdentityExists {
		t.Fatal("node_identity missing after Remediation migration up")
	}
}

func seedRemediationWorker(t *testing.T, database testDatabase) uuid.UUID {
	t.Helper()
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000001925")
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_pools (id, stable_id, queued_limit)
		VALUES ('10000000-0000-0000-0000-000000001924', 'remediation-pool', 10)
	`); err != nil {
		t.Fatalf("seed Remediation Worker pool: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition, node_identity
		) VALUES (
			$1, '10000000-0000-0000-0000-000000001924',
			'spiffe://vela.example/worker/remediation-1', 1, 'READY', 'HEALTHY', 'node-remediation-1'
		)
	`, workerID); err != nil {
		t.Fatalf("seed Remediation Worker: %v", err)
	}
	return workerID
}

func assertRemediationFailure(t *testing.T, err error, code remediation.FailureCode) {
	t.Helper()
	var failure *remediation.Failure
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("Remediation error = %v, want %s", err, code)
	}
}

func assertRemediationSQLState(t *testing.T, database *sql.DB, statement string, argument uuid.UUID, code string) {
	t.Helper()
	_, err := database.Exec(statement, argument)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != code {
		t.Fatalf("Remediation mutation error = %v, want SQLSTATE %s", err, code)
	}
}
