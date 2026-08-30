//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/debugdump"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const testWorkerID = "00000000-0000-0000-0000-000000000020"

func TestAcquireReplaysOneAssignmentWithoutRenewingLease(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "integration fixture switch")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "assignment-replay", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"return one durable execution authority"
    }`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	if _, err := database.Admin.Exec(`
        INSERT INTO workers (
            id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
        ) VALUES (
            $1, '00000000-0000-0000-0000-000000000005',
            'spiffe://vela.internal/worker/h3-primary-01', 7, 'READY', 'HEALTHY'
        )
    `, testWorkerID); err != nil {
		t.Fatalf("seed READY Worker: %v", err)
	}

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
		LeaseTTL:         2 * time.Minute,
		ActiveLeaseKeyID: "lease-key-v1",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
		},
	})
	if err != nil {
		t.Fatalf("create Worker coordinator: %v", err)
	}
	candidate := &workercontrol.AssignmentCandidate{
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
	}
	identity := workercontrol.AuthenticatedWorker{ID: uuid.MustParse(testWorkerID)}

	start := make(chan struct{})
	results := make(chan workercontrol.Assignment, 2)
	acquireErrors := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			assignment, acquireErr := service.Acquire(context.Background(), identity, 7, candidate)
			results <- assignment
			acquireErrors <- acquireErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(acquireErrors)
	for acquireErr := range acquireErrors {
		if acquireErr != nil {
			t.Fatalf("concurrent Acquire: %v", acquireErr)
		}
	}
	assignments := make([]workercontrol.Assignment, 0, 2)
	for assignment := range results {
		assignments = append(assignments, assignment)
	}
	if len(assignments) != 2 {
		t.Fatalf("Assignment results = %d, want 2", len(assignments))
	}
	if !sameAssignmentAuthority(assignments[0], assignments[1]) {
		t.Fatalf("concurrent Assignment replay differs: first=%#v second=%#v", assignments[0], assignments[1])
	}
	first := assignments[0]
	if first.DebugDumpAuthorization != nil {
		t.Fatalf("default-off Assignment debug authorization = %#v", first.DebugDumpAuthorization)
	}
	if first.AttemptID == uuid.Nil || first.JobID != candidate.JobID || first.WorkerID != identity.ID {
		t.Fatalf("Assignment identity = %#v", first)
	}
	if first.WorkerEpoch != 7 || first.AttemptNumber != 1 || first.LeaseFence != 1 || first.LeaseToken == "" {
		t.Fatalf("Assignment authority = %#v", first)
	}
	if first.ModelRevisionID != uuid.MustParse("00000000-0000-0000-0000-000000000010") ||
		first.GenerationPresetRevisionID != uuid.MustParse("00000000-0000-0000-0000-000000000011") ||
		first.ExecutionProfileRevisionID != uuid.MustParse("00000000-0000-0000-0000-000000000014") ||
		first.OutputSpecID != uuid.MustParse("00000000-0000-0000-0000-000000000013") {
		t.Fatalf("Assignment execution snapshot = %#v", first)
	}
	var requestContent map[string]any
	if err := json.Unmarshal([]byte(first.RequestContent), &requestContent); err != nil {
		t.Fatalf("decode Assignment request content: %v", err)
	}
	if requestContent["model"] != "minimax-h3" || requestContent["generation_preset"] != "balanced" ||
		requestContent["output_spec"] != "video-1080p-5s-24fps" ||
		requestContent["prompt"] != "return one durable execution authority" {
		t.Fatalf("Assignment request content = %#v", requestContent)
	}
	if first.LeaseValidFor <= 0 || first.LeaseValidFor > 2*time.Minute {
		t.Fatalf("Assignment lease_valid_for = %s, want (0, 2m]", first.LeaseValidFor)
	}
	malformedCandidate := &workercontrol.AssignmentCandidate{}
	replayedBeforeCandidateValidation, err := service.Acquire(
		context.Background(),
		identity,
		7,
		malformedCandidate,
	)
	if err != nil {
		t.Fatalf("Acquire replay before malformed candidate validation: %v", err)
	}
	if !sameAssignmentAuthority(replayedBeforeCandidateValidation, first) {
		t.Fatalf("malformed-candidate replay = %#v, want %#v", replayedBeforeCandidateValidation, first)
	}
	_, err = service.Acquire(context.Background(), identity, 8, nil)
	var staleEpochFailure *workercontrol.Failure
	if !errors.As(err, &staleEpochFailure) ||
		staleEpochFailure.Code != workercontrol.FailureStaleWorkerEpoch {
		t.Fatalf("post-Assignment stale epoch error = %v, want stale_worker_epoch", err)
	}

	replayed, err := service.Acquire(context.Background(), identity, 7, nil)
	if err != nil {
		t.Fatalf("Acquire replay without candidate: %v", err)
	}
	if !sameAssignmentAuthority(replayed, first) {
		t.Fatalf("lost-response replay = %#v, want %#v", replayed, first)
	}
	if replayed.LeaseValidFor <= 0 || replayed.LeaseValidFor > first.LeaseValidFor {
		t.Fatalf("replayed lease_valid_for = %s, want (0, %s]", replayed.LeaseValidFor, first.LeaseValidFor)
	}

	var (
		attempts, leases, assignmentEvents        int
		jobState, workerState                     string
		jobVersion, currentFence, attemptsStarted int64
		projectQueued, projectRunning, poolQueued int
	)
	if err := database.Admin.QueryRow(`
        SELECT
            (SELECT count(*) FROM attempts WHERE job_id = $1),
            (SELECT count(*) FROM attempt_leases WHERE attempt_id = $2),
            (SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'job.assigned'),
            (SELECT state::text FROM jobs WHERE id = $1),
            (SELECT lifecycle_state::text FROM workers WHERE id = $3),
            (SELECT version FROM jobs WHERE id = $1),
            (SELECT current_fence FROM jobs WHERE id = $1),
            (SELECT attempts_started FROM retry_runtime_states WHERE job_id = $1),
            (SELECT queued_count FROM projects WHERE id = $4),
            (SELECT running_count FROM projects WHERE id = $4),
            (SELECT queued_count FROM worker_pools WHERE id = '00000000-0000-0000-0000-000000000005')
    `, first.JobID, first.AttemptID, first.WorkerID, testProjectID).Scan(
		&attempts,
		&leases,
		&assignmentEvents,
		&jobState,
		&workerState,
		&jobVersion,
		&currentFence,
		&attemptsStarted,
		&projectQueued,
		&projectRunning,
		&poolQueued,
	); err != nil {
		t.Fatalf("read Assignment transaction effects: %v", err)
	}
	if attempts != 1 || leases != 1 || assignmentEvents != 1 {
		t.Fatalf("Assignment rows = attempts %d, leases %d, events %d", attempts, leases, assignmentEvents)
	}
	if jobState != "ASSIGNED" || workerState != "BUSY" || jobVersion != 2 || currentFence != 1 || attemptsStarted != 1 {
		t.Fatalf(
			"Assignment state = Job %s v%d fence%d, Worker %s, attempts %d",
			jobState,
			jobVersion,
			currentFence,
			workerState,
			attemptsStarted,
		)
	}
	if projectQueued != 0 || projectRunning != 1 || poolQueued != 0 {
		t.Fatalf(
			"Assignment counters = Project queued %d running %d, pool queued %d",
			projectQueued,
			projectRunning,
			poolQueued,
		)
	}

	revocation, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent Lease revocation: %v", err)
	}
	if _, err := revocation.Exec(`
        UPDATE attempt_leases
        SET revoked_at = clock_timestamp()
        WHERE attempt_id = $1
    `, first.AttemptID); err != nil {
		_ = revocation.Rollback()
		t.Fatalf("stage concurrent Lease revocation: %v", err)
	}
	replayContext, cancelReplay := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelReplay()
	_, err = service.Acquire(replayContext, identity, 7, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		_ = revocation.Rollback()
		t.Fatalf("Acquire during uncommitted Lease revocation error = %v, want context deadline", err)
	}
	if err := revocation.Rollback(); err != nil {
		t.Fatalf("roll back concurrent Lease revocation: %v", err)
	}
	replayedAfterRollback, err := service.Acquire(context.Background(), identity, 7, nil)
	if err != nil {
		t.Fatalf("Acquire after Lease revocation rollback: %v", err)
	}
	if !sameAssignmentAuthority(replayedAfterRollback, first) {
		t.Fatalf("post-rollback replay = %#v, want %#v", replayedAfterRollback, first)
	}

	restartedService, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("restart Worker coordinator: %v", err)
	}
	replayedAfterRestart, err := restartedService.Acquire(context.Background(), identity, 7, nil)
	if err != nil {
		t.Fatalf("Acquire after coordinator restart: %v", err)
	}
	if !sameAssignmentAuthority(replayedAfterRestart, first) {
		t.Fatalf("post-restart replay = %#v, want %#v", replayedAfterRestart, first)
	}

	var eventPayload []byte
	if err := database.Admin.QueryRow(`
        SELECT payload
        FROM outbox_events
        WHERE aggregate_id = $1 AND event_type = 'job.assigned'
    `, first.JobID).Scan(&eventPayload); err != nil {
		t.Fatalf("read job.assigned payload: %v", err)
	}
	var envelope velav1.EventEnvelope
	if err := proto.Unmarshal(eventPayload, &envelope); err != nil {
		t.Fatalf("decode job.assigned payload: %v", err)
	}
	assignedEvent := envelope.GetJobAssigned()
	if envelope.GetAggregateVersion() != 2 || assignedEvent == nil ||
		assignedEvent.GetAttemptId() != first.AttemptID.String() ||
		assignedEvent.GetWorkerId() != first.WorkerID.String() ||
		assignedEvent.GetLeaseFence() != uint64(first.LeaseFence) {
		t.Fatalf("job.assigned envelope = %#v", &envelope)
	}
}

func TestAcquireSnapshotsActiveDebugDumpAuthorizationAcrossReplay(t *testing.T) {
	fixture := newAssignmentFixture(t, "assignment-debug-dump-authorization", 7)
	principalID := uuid.New()
	seedHumanRoleFixture(
		t,
		fixture.database.Admin,
		principalID,
		"assignment-debug-dump-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		fixture.database,
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "assignment-debug-dump-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(context.Background(), "assignment-debug-dump-token")
	if err != nil {
		t.Fatalf("authenticate debug dump ProjectAdmin: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	actor, ok := actor.ForProject(projectID)
	if !ok {
		t.Fatal("select debug dump Project authority")
	}
	debugService, err := debugdump.NewService(newRolePool(
		t,
		fixture.database.DSN,
		"vela_debug_dump_request_login",
		"vela-debug-dump-request-password",
	))
	if err != nil {
		t.Fatalf("create debug dump service: %v", err)
	}
	authorization, err := debugService.Authorize(
		context.Background(),
		actor,
		projectID,
		fixture.candidate.JobID,
		"assignment-debug-dump-authorization",
		debugdump.PurposeIncidentInvestigation,
	)
	if err != nil {
		t.Fatalf("authorize Assignment debug dump: %v", err)
	}

	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("Acquire authorized Assignment: %v", err)
	}
	if assignment.DebugDumpAuthorization == nil ||
		assignment.DebugDumpAuthorization.AuthorizationID != authorization.ID ||
		!assignment.DebugDumpAuthorization.ExpiresAt.Equal(authorization.ExpiresAt) {
		t.Fatalf(
			"Assignment debug dump authorization = %#v, want %s until %s",
			assignment.DebugDumpAuthorization,
			authorization.ID,
			authorization.ExpiresAt,
		)
	}
	replayed, err := fixture.service.Acquire(context.Background(), fixture.worker, 7, nil)
	if err != nil {
		t.Fatalf("replay authorized Assignment: %v", err)
	}
	if !sameAssignmentAuthority(assignment, replayed) {
		t.Fatalf("authorized Assignment replay = %#v, want %#v", replayed, assignment)
	}
}

func TestAttemptCannotCrossJobWorkerOrProfilePool(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedSecondProjectAndPool(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-pool-binding", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"bind execution authority to one Worker pool"
    }`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	const secondaryWorkerID = "00000000-0000-0000-0000-000000000120"
	if _, err := database.Admin.Exec(`
        INSERT INTO workers (
            id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
        ) VALUES (
            $1, '00000000-0000-0000-0000-000000000105',
            'spiffe://vela.internal/worker/h3-secondary-01', 1, 'READY', 'HEALTHY'
        )
    `, secondaryWorkerID); err != nil {
		t.Fatalf("seed secondary-pool Worker: %v", err)
	}

	_, err := database.Admin.Exec(`
        INSERT INTO attempts (
            id, organization_id, project_id, job_id, attempt_number,
            execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
            state, fence, assigned_at
        ) VALUES (
            '00000000-0000-0000-0000-000000000121', $1, $2, $3, 1,
            '00000000-0000-0000-0000-000000000014',
            '00000000-0000-0000-0000-000000000005', $4, 1,
            'ASSIGNED', 1, clock_timestamp()
        )
    `, testOrganizationID, testProjectID, job.JobID, secondaryWorkerID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23503" {
		t.Fatalf("cross-pool Attempt error = %v, want SQLSTATE 23503", err)
	}
}

func TestAcquireNeverReplaysExpiredLease(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "expired-assignment-replay", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"never replay expired execution authority"
    }`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	if _, err := database.Admin.Exec(`
        INSERT INTO workers (
            id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
        ) VALUES (
            $1, '00000000-0000-0000-0000-000000000005',
            'spiffe://vela.internal/worker/h3-primary-expiry', 9, 'READY', 'HEALTHY'
        )
    `, testWorkerID); err != nil {
		t.Fatalf("seed READY Worker: %v", err)
	}
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
		LeaseTTL:         2 * time.Minute,
		ActiveLeaseKeyID: "lease-key-v1",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
		},
	})
	if err != nil {
		t.Fatalf("create Worker coordinator: %v", err)
	}
	identity := workercontrol.AuthenticatedWorker{ID: uuid.MustParse(testWorkerID)}
	assignment, err := service.Acquire(context.Background(), identity, 9, &workercontrol.AssignmentCandidate{
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
	})
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if _, err := database.Admin.Exec(`
        UPDATE attempt_leases
        SET expires_at = issued_at + interval '1 microsecond'
        WHERE attempt_id = $1
    `, assignment.AttemptID); err != nil {
		t.Fatalf("expire Lease: %v", err)
	}

	_, err = service.Acquire(context.Background(), identity, 9, nil)
	var failure *workercontrol.Failure
	if !errors.As(err, &failure) || failure.Code != workercontrol.FailureLeaseExpired {
		t.Fatalf("expired Lease replay error = %v, want lease_expired", err)
	}
	var attempts int
	var leaseExpired bool
	if err := database.Admin.QueryRow(`
        SELECT
            (SELECT count(*) FROM attempts WHERE job_id = $1),
			(SELECT expires_at < clock_timestamp()
			 FROM attempt_leases
			 WHERE attempt_id = $2)
	`, assignment.JobID, assignment.AttemptID).Scan(&attempts, &leaseExpired); err != nil {
		t.Fatalf("read expired Assignment: %v", err)
	}
	if attempts != 1 || !leaseExpired {
		t.Fatalf("expired Assignment mutated: attempts=%d lease_expired=%t", attempts, leaseExpired)
	}
}

func TestAcquireRechecksLeaseExpiryAfterRowLockWait(t *testing.T) {
	fixture := newAssignmentFixture(t, "lease-expiry-after-lock-wait", 7)
	internalPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	shortLeaseService, err := workercontrol.NewService(
		context.Background(),
		internalPool,
		workercontrol.Config{
			LeaseTTL:         time.Second,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
		},
	)
	if err != nil {
		t.Fatalf("create short-Lease Worker coordinator: %v", err)
	}
	assignment, err := shortLeaseService.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	holder, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Lease lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec(`
		SELECT id
		FROM attempt_leases
		WHERE attempt_id = $1
		FOR UPDATE
	`, assignment.AttemptID); err != nil {
		t.Fatalf("lock active Lease: %v", err)
	}

	type acquireResult struct {
		assignment workercontrol.Assignment
		err        error
	}
	result := make(chan acquireResult, 1)
	go func() {
		replayed, acquireErr := shortLeaseService.Acquire(
			context.Background(),
			fixture.worker,
			7,
			nil,
		)
		result <- acquireResult{assignment: replayed, err: acquireErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
	waitForDatabaseTimeAfter(t, fixture.database.Admin, assignment.LeaseExpiresAt)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release expired Lease row: %v", err)
	}

	replay := <-result
	var failure *workercontrol.Failure
	if replay.assignment.AttemptID != uuid.Nil ||
		!errors.As(replay.err, &failure) ||
		failure.Code != workercontrol.FailureLeaseExpired {
		t.Fatalf("post-lock expiry replay = %#v, error %v, want lease_expired", replay.assignment, replay.err)
	}
}

func TestAcquireNeverReplaysReconcilerFinalizationLease(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconciler-finalization-not-worker-authority", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE attempt_leases
		SET revoked_at = clock_timestamp()
		WHERE attempt_id = $1
	`, assignment.AttemptID); err != nil {
		t.Fatalf("revoke Worker EXECUTION Lease: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(
		"UPDATE attempts SET state = 'RUNNING', started_at = clock_timestamp() WHERE id = $1",
		assignment.AttemptID,
	); err != nil {
		t.Fatalf("start Attempt fixture: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(
		"UPDATE attempts SET state = 'FINALIZING' WHERE id = $1",
		assignment.AttemptID,
	); err != nil {
		t.Fatalf("finalize Attempt fixture: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
			INSERT INTO attempt_leases (
				id, organization_id, project_id, attempt_id, worker_id, worker_epoch,
				phase, owner_kind, owner_id, fence, token_digest, signing_key_id,
				issued_at, expires_at
		)
		SELECT
			'00000000-0000-0000-0000-000000000122', organization_id, project_id,
			id, worker_id, worker_epoch, 'FINALIZATION', 'RECONCILER',
			'spiffe://vela.internal/reconciler/artifact-01', fence,
			decode(repeat('ab', 32), 'hex'), 'lease-key-v1',
			clock_timestamp(), clock_timestamp() + interval '2 minutes'
		FROM attempts
		WHERE id = $1
	`, assignment.AttemptID); err != nil {
		t.Fatalf("insert Reconciler FINALIZATION Lease: %v", err)
	}

	_, err = fixture.service.Acquire(context.Background(), fixture.worker, 7, nil)
	var failure *workercontrol.Failure
	if !errors.As(err, &failure) || failure.Code != workercontrol.FailureNoAssignment {
		t.Fatalf("Acquire with Reconciler FINALIZATION Lease error = %v, want no_assignment", err)
	}
}

func TestAcquireLeaseNeverOutlivesJobExpiry(t *testing.T) {
	fixture := newAssignmentFixture(t, "lease-bounded-by-job-expiry", 7)
	internalPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	service, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
		LeaseTTL:         4 * time.Hour,
		ActiveLeaseKeyID: "lease-key-v1",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
		},
	})
	if err != nil {
		t.Fatalf("create long-TTL Worker coordinator: %v", err)
	}
	assignment, err := service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("Acquire with long Lease TTL: %v", err)
	}
	var jobExpiresAt time.Time
	if err := fixture.database.Admin.QueryRow(
		"SELECT job_expires_at FROM jobs WHERE id = $1",
		fixture.candidate.JobID,
	).Scan(&jobExpiresAt); err != nil {
		t.Fatalf("read Job Expiry: %v", err)
	}
	if !assignment.LeaseExpiresAt.Equal(jobExpiresAt) {
		t.Fatalf(
			"Lease expiry = %s, want Job Expiry ceiling %s",
			assignment.LeaseExpiresAt,
			jobExpiresAt,
		)
	}
}

func TestAcquireRechecksJobExpiryAfterRowLockWait(t *testing.T) {
	fixture := newAssignmentFixture(t, "job-expiry-after-lock-wait", 7)
	before := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	holder, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Job lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable snapshot trigger for Job expiry fixture: %v", err)
	}
	var expiresAt time.Time
	if err := holder.QueryRow(`
		UPDATE jobs
		SET job_expires_at = clock_timestamp() + interval '1 second'
		WHERE id = $1
		RETURNING job_expires_at
	`, fixture.candidate.JobID).Scan(&expiresAt); err != nil {
		t.Fatalf("lock Job with near expiry: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, acquireErr := fixture.service.Acquire(
			context.Background(),
			fixture.worker,
			7,
			&fixture.candidate,
		)
		result <- acquireErr
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
	waitForDatabaseTimeAfter(t, fixture.database.Admin, expiresAt)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release expired Job row: %v", err)
	}

	err = <-result
	var failure *workercontrol.Failure
	if !errors.As(err, &failure) || failure.Code != workercontrol.FailureCandidateUnavailable {
		t.Fatalf("post-lock Job expiry error = %v, want candidate_unavailable", err)
	}
	after := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	if after != before {
		t.Fatalf("post-lock expired Assignment mutated state: before=%#v after=%#v", before, after)
	}
}

func TestAcquireRollsBackWhenJobExpiresDuringOutboxWrite(t *testing.T) {
	fixture := newAssignmentFixture(t, "job-expiry-during-outbox-write", 7)
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION delay_assignment_outbox_past_job_expiry() RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(8675309);
			RETURN NEW;
		END
		$$
	`); err != nil {
		t.Fatalf("create delayed Outbox trigger function: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		CREATE TRIGGER delay_assignment_outbox
		BEFORE INSERT ON outbox_events
		FOR EACH ROW
		WHEN (NEW.event_type = 'job.assigned')
		EXECUTE FUNCTION delay_assignment_outbox_past_job_expiry()
	`); err != nil {
		t.Fatalf("create delayed Outbox trigger: %v", err)
	}
	outboxLatch, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin delayed Outbox latch: %v", err)
	}
	defer func() { _ = outboxLatch.Rollback() }()
	if _, err := outboxLatch.Exec("SELECT pg_advisory_xact_lock(8675309)"); err != nil {
		t.Fatalf("acquire delayed Outbox latch: %v", err)
	}
	expiryFixture, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Job expiry fixture transaction: %v", err)
	}
	defer func() { _ = expiryFixture.Rollback() }()
	if _, err := expiryFixture.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable snapshot trigger for Job expiry fixture: %v", err)
	}
	var expiresAt time.Time
	if err := expiryFixture.QueryRow(`
		UPDATE jobs
		SET job_expires_at = clock_timestamp() + interval '3 seconds'
		WHERE id = $1
		RETURNING job_expires_at
	`, fixture.candidate.JobID).Scan(&expiresAt); err != nil {
		t.Fatalf("set near Job Expiry: %v", err)
	}
	if err := expiryFixture.Commit(); err != nil {
		t.Fatalf("commit near Job Expiry fixture: %v", err)
	}
	before := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	var beforeExpiry bool
	if err := fixture.database.Admin.QueryRow(
		"SELECT clock_timestamp() < $1",
		expiresAt,
	).Scan(&beforeExpiry); err != nil {
		t.Fatalf("compare Job Expiry before Acquire: %v", err)
	}
	if !beforeExpiry {
		t.Fatal("Job expired before Acquire reached the delayed Outbox path")
	}

	result := make(chan error, 1)
	go func() {
		_, acquireErr := fixture.service.Acquire(
			context.Background(),
			fixture.worker,
			7,
			&fixture.candidate,
		)
		result <- acquireErr
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
	if err := fixture.database.Admin.QueryRow(
		"SELECT clock_timestamp() < $1",
		expiresAt,
	).Scan(&beforeExpiry); err != nil {
		t.Fatalf("compare Job Expiry at delayed Outbox latch: %v", err)
	}
	if !beforeExpiry {
		t.Fatal("Job expired before Acquire reached the delayed Outbox latch")
	}
	waitForDatabaseTimeAfter(t, fixture.database.Admin, expiresAt)
	if err := outboxLatch.Commit(); err != nil {
		t.Fatalf("release delayed Outbox latch: %v", err)
	}
	err = <-result
	var failure *workercontrol.Failure
	if !errors.As(err, &failure) || failure.Code != workercontrol.FailureCandidateUnavailable {
		t.Fatalf("expiry during Outbox write error = %v, want candidate_unavailable", err)
	}
	after := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	if after != before {
		t.Fatalf("expired Outbox transaction was partial: before=%#v after=%#v", before, after)
	}
}

func TestWorkerCoordinatorRequiresKeysForEveryActiveLease(t *testing.T) {
	fixture := newAssignmentFixture(t, "active-lease-keyring-coverage", 7)
	original, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create v1 Assignment: %v", err)
	}
	internalPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	_, err = workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
		LeaseTTL:         2 * time.Minute,
		ActiveLeaseKeyID: "lease-key-v2",
		LeaseKeys: map[string][]byte{
			"lease-key-v2": []byte("abcdef0123456789abcdef0123456789"),
		},
	})
	if err == nil {
		t.Fatal("Worker coordinator without active v1 Lease key was accepted")
	}

	rotated, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
		LeaseTTL:         2 * time.Minute,
		ActiveLeaseKeyID: "lease-key-v2",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			"lease-key-v2": []byte("abcdef0123456789abcdef0123456789"),
		},
	})
	if err != nil {
		t.Fatalf("create overlapping-key Worker coordinator: %v", err)
	}
	replayed, err := rotated.Acquire(context.Background(), fixture.worker, 7, nil)
	if err != nil {
		t.Fatalf("replay v1 Assignment after rotating active key: %v", err)
	}
	if !sameAssignmentAuthority(replayed, original) {
		t.Fatalf("rotated-key replay = %#v, want %#v", replayed, original)
	}
}

func TestAcquireRejectsIneligibleAuthorityWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*testing.T, *sql.DB, uuid.UUID)
		epoch    int64
		version  int64
		wantCode workercontrol.FailureCode
	}{
		{
			name:     "stale Worker epoch",
			epoch:    6,
			version:  1,
			wantCode: workercontrol.FailureStaleWorkerEpoch,
		},
		{
			name: "SUSPECT Worker",
			mutate: func(t *testing.T, db *sql.DB, _ uuid.UUID) {
				if _, err := db.Exec("UPDATE workers SET reachability_condition = 'SUSPECT'"); err != nil {
					t.Fatalf("mark Worker SUSPECT: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureWorkerUnavailable,
		},
		{
			name: "BUSY Worker without active Assignment",
			mutate: func(t *testing.T, db *sql.DB, _ uuid.UUID) {
				if _, err := db.Exec("UPDATE workers SET lifecycle_state = 'BUSY'"); err != nil {
					t.Fatalf("mark Worker BUSY: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureWorkerUnavailable,
		},
		{
			name: "mismatched Worker pool",
			mutate: func(t *testing.T, db *sql.DB, _ uuid.UUID) {
				if _, err := db.Exec(`
                    INSERT INTO worker_pools (id, stable_id, queued_limit)
                    VALUES ('00000000-0000-0000-0000-000000000025', 'h3-ineligible', 10);
                    UPDATE workers
                    SET worker_pool_id = '00000000-0000-0000-0000-000000000025';
                `); err != nil {
					t.Fatalf("move Worker to mismatched pool: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name:     "stale Job version",
			epoch:    7,
			version:  99,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "expired Job",
			mutate: func(t *testing.T, db *sql.DB, jobID uuid.UUID) {
				transaction, err := db.Begin()
				if err != nil {
					t.Fatalf("begin expired Job fixture: %v", err)
				}
				defer func() { _ = transaction.Rollback() }()
				if _, err := transaction.Exec("SET LOCAL session_replication_role = replica"); err != nil {
					t.Fatalf("disable triggers for expired Job fixture: %v", err)
				}
				if _, err := transaction.Exec(`
                    UPDATE jobs
                    SET created_at = clock_timestamp() - interval '10 seconds',
                        job_expires_at = clock_timestamp() - interval '1 second'
                    WHERE id = $1
                `, jobID); err != nil {
					t.Fatalf("make Job expired: %v", err)
				}
				if err := transaction.Commit(); err != nil {
					t.Fatalf("commit expired Job fixture: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "released Credit Reservation",
			mutate: func(t *testing.T, db *sql.DB, jobID uuid.UUID) {
				if _, err := db.Exec(
					"UPDATE credit_reservations SET state = 'RELEASED' WHERE job_id = $1",
					jobID,
				); err != nil {
					t.Fatalf("release Credit Reservation: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "exhausted compute budget",
			mutate: func(t *testing.T, db *sql.DB, jobID uuid.UUID) {
				if _, err := db.Exec(`
                    UPDATE retry_runtime_states AS runtime
                    SET compute_seconds_consumed = job.execution_max_total_compute_seconds
                    FROM jobs AS job
                    WHERE runtime.job_id = job.id AND job.id = $1
                `, jobID); err != nil {
					t.Fatalf("exhaust compute budget: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "exhausted Attempt budget",
			mutate: func(t *testing.T, db *sql.DB, jobID uuid.UUID) {
				if _, err := db.Exec(
					"UPDATE retry_runtime_states SET attempts_started = 3 WHERE job_id = $1",
					jobID,
				); err != nil {
					t.Fatalf("exhaust Attempt budget: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "invalidated Profile Certification",
			mutate: func(t *testing.T, db *sql.DB, _ uuid.UUID) {
				if _, err := db.Exec(`
                    UPDATE profile_certifications
                    SET state = 'INVALID', invalidated_at = clock_timestamp()
                `); err != nil {
					t.Fatalf("invalidate Profile Certification: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "inactive ServiceClassRevision",
			mutate: func(t *testing.T, db *sql.DB, _ uuid.UUID) {
				if _, err := db.Exec("UPDATE service_class_revisions SET state = 'DRAINING'"); err != nil {
					t.Fatalf("drain ServiceClassRevision: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "Project running limit",
			mutate: func(t *testing.T, db *sql.DB, _ uuid.UUID) {
				if _, err := db.Exec("UPDATE projects SET running_count = running_limit"); err != nil {
					t.Fatalf("exhaust Project running limit: %v", err)
				}
			},
			epoch:    7,
			version:  1,
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, fmt.Sprintf("ineligible-%d", index), 7)
			if test.mutate != nil {
				test.mutate(t, fixture.database.Admin, fixture.candidate.JobID)
			}
			before := readAssignmentState(t, fixture.database.Admin, fixture.candidate.JobID, fixture.worker.ID)
			candidate := fixture.candidate
			candidate.ExpectedJobVersion = test.version
			_, err := fixture.service.Acquire(context.Background(), fixture.worker, test.epoch, &candidate)
			var failure *workercontrol.Failure
			if !errors.As(err, &failure) || failure.Code != test.wantCode {
				t.Fatalf("Acquire error = %v, want %s", err, test.wantCode)
			}
			after := readAssignmentState(t, fixture.database.Admin, fixture.candidate.JobID, fixture.worker.ID)
			if after != before {
				t.Fatalf("rejected Assignment mutated state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestConcurrentWorkersCannotClaimTheSameJob(t *testing.T) {
	fixture := newAssignmentFixture(t, "competing-workers", 7)
	const secondWorkerID = "00000000-0000-0000-0000-000000000021"
	if _, err := fixture.database.Admin.Exec(`
        INSERT INTO workers (
            id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
        ) VALUES (
            $1, '00000000-0000-0000-0000-000000000005',
            'spiffe://vela.internal/worker/h3-primary-02', 11, 'READY', 'HEALTHY'
        )
    `, secondWorkerID); err != nil {
		t.Fatalf("seed competing Worker: %v", err)
	}
	workers := []struct {
		identity workercontrol.AuthenticatedWorker
		epoch    int64
	}{
		{identity: fixture.worker, epoch: 7},
		{identity: workercontrol.AuthenticatedWorker{ID: uuid.MustParse(secondWorkerID)}, epoch: 11},
	}
	start := make(chan struct{})
	results := make(chan workercontrol.Assignment, 2)
	acquireErrors := make(chan error, 2)
	var wait sync.WaitGroup
	for _, worker := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			assignment, err := fixture.service.Acquire(
				context.Background(),
				worker.identity,
				worker.epoch,
				&fixture.candidate,
			)
			results <- assignment
			acquireErrors <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(acquireErrors)
	var successes, unavailable int
	for assignment := range results {
		if assignment.AttemptID != uuid.Nil {
			successes++
		}
	}
	for err := range acquireErrors {
		if err == nil {
			continue
		}
		var failure *workercontrol.Failure
		if errors.As(err, &failure) && failure.Code == workercontrol.FailureCandidateUnavailable {
			unavailable++
			continue
		}
		t.Fatalf("competing Acquire error = %v", err)
	}
	if successes != 1 || unavailable != 1 {
		t.Fatalf("competing Acquire results = %d success, %d unavailable", successes, unavailable)
	}
	var attempts, leases, busyWorkers int
	if err := fixture.database.Admin.QueryRow(`
        SELECT
            (SELECT count(*) FROM attempts WHERE job_id = $1),
            (SELECT count(*) FROM attempt_leases),
            (SELECT count(*) FROM workers WHERE lifecycle_state = 'BUSY')
    `, fixture.candidate.JobID).Scan(&attempts, &leases, &busyWorkers); err != nil {
		t.Fatalf("read competing Assignment result: %v", err)
	}
	if attempts != 1 || leases != 1 || busyWorkers != 1 {
		t.Fatalf("competing Assignment rows = attempts %d leases %d BUSY Workers %d", attempts, leases, busyWorkers)
	}
}

func TestOneWorkerCannotClaimTwoConcurrentJobs(t *testing.T) {
	fixture := newAssignmentFixture(t, "competing-jobs-first", 7)
	server := admissionServerForDatabase(t, fixture.database)
	accepted := submitJob(t, server.URL, "competing-jobs-second", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"compete for one Worker"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit second Job status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var secondJob jobResponse
	if err := json.Unmarshal(accepted.Body, &secondJob); err != nil {
		t.Fatalf("decode second Accepted Job: %v", err)
	}
	candidates := []workercontrol.AssignmentCandidate{
		fixture.candidate,
		{
			JobID:                      uuid.MustParse(secondJob.JobID),
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: fixture.candidate.ExecutionProfileRevisionID,
		},
	}
	start := make(chan struct{})
	results := make(chan workercontrol.Assignment, 2)
	acquireErrors := make(chan error, 2)
	var wait sync.WaitGroup
	for _, candidate := range candidates {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			assignment, err := fixture.service.Acquire(
				context.Background(),
				fixture.worker,
				7,
				&candidate,
			)
			results <- assignment
			acquireErrors <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(acquireErrors)
	for err := range acquireErrors {
		if err != nil {
			t.Fatalf("competing Job Acquire: %v", err)
		}
	}
	assignments := make([]workercontrol.Assignment, 0, 2)
	for assignment := range results {
		assignments = append(assignments, assignment)
	}
	if len(assignments) != 2 || !sameAssignmentAuthority(assignments[0], assignments[1]) {
		t.Fatalf("competing Job Assignments = %#v, want one exact replay", assignments)
	}
	var attempts, leases, assignedJobs, queuedJobs, busyWorkers int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM attempts WHERE state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')),
			(SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
			(SELECT count(*) FROM jobs WHERE state = 'ASSIGNED'),
			(SELECT count(*) FROM jobs WHERE state = 'QUEUED'),
			(SELECT count(*) FROM workers WHERE lifecycle_state = 'BUSY')
	`).Scan(&attempts, &leases, &assignedJobs, &queuedJobs, &busyWorkers); err != nil {
		t.Fatalf("read competing Job result: %v", err)
	}
	if attempts != 1 || leases != 1 || assignedJobs != 1 || queuedJobs != 1 || busyWorkers != 1 {
		t.Fatalf(
			"competing Job rows = attempts %d leases %d assigned %d queued %d BUSY Workers %d",
			attempts,
			leases,
			assignedJobs,
			queuedJobs,
			busyWorkers,
		)
	}
}

func TestAttemptAndLeaseActiveAuthorityIsUnique(t *testing.T) {
	fixture := newAssignmentFixture(t, "active-authority-uniqueness-first", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create first Assignment: %v", err)
	}
	const secondWorkerID = "00000000-0000-0000-0000-000000000123"
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/h3-uniqueness-02', 1, 'READY', 'HEALTHY'
		)
	`, secondWorkerID); err != nil {
		t.Fatalf("seed second Worker: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	accepted := submitJob(t, server.URL, "active-authority-uniqueness-second", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"exercise active authority uniqueness"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit second Job status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var secondJob jobResponse
	if err := json.Unmarshal(accepted.Body, &secondJob); err != nil {
		t.Fatalf("decode second Job: %v", err)
	}

	_, err = fixture.database.Admin.Exec(`
		INSERT INTO attempts (
			id, organization_id, project_id, job_id, attempt_number,
			execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
			state, fence, assigned_at
		) VALUES (
			'00000000-0000-0000-0000-000000000124', $1, $2, $3, 2,
			'00000000-0000-0000-0000-000000000014',
			'00000000-0000-0000-0000-000000000005', $4, 1,
			'ASSIGNED', 2, clock_timestamp()
		)
	`, testOrganizationID, testProjectID, assignment.JobID, secondWorkerID)
	requirePostgresConstraint(t, err, "attempts_one_active_per_job")

	_, err = fixture.database.Admin.Exec(`
		INSERT INTO attempts (
			id, organization_id, project_id, job_id, attempt_number,
			execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
			state, fence, assigned_at
		) VALUES (
			'00000000-0000-0000-0000-000000000125', $1, $2, $3, 1,
			'00000000-0000-0000-0000-000000000014',
			'00000000-0000-0000-0000-000000000005', $4, 7,
			'ASSIGNED', 1, clock_timestamp()
		)
	`, testOrganizationID, testProjectID, secondJob.JobID, fixture.worker.ID)
	requirePostgresConstraint(t, err, "attempts_one_active_per_worker")

	perAttempt, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin per-Attempt Lease uniqueness transaction: %v", err)
	}
	defer func() { _ = perAttempt.Rollback() }()
	if _, err := perAttempt.Exec(
		"UPDATE attempt_leases SET revoked_at = clock_timestamp() WHERE attempt_id = $1",
		assignment.AttemptID,
	); err != nil {
		t.Fatalf("revoke original Lease for per-Attempt test: %v", err)
	}
	for index, leaseID := range []string{
		"00000000-0000-0000-0000-000000000126",
		"00000000-0000-0000-0000-000000000127",
	} {
		_, insertErr := perAttempt.Exec(`
			INSERT INTO attempt_leases (
				id, organization_id, project_id, attempt_id, worker_id, worker_epoch,
				phase, owner_kind, owner_id, fence, token_digest, signing_key_id,
				issued_at, expires_at
			) VALUES (
				$1, $2, $3, $4, $5, 7, 'FINALIZATION', 'RECONCILER',
				'spiffe://vela.internal/reconciler/uniqueness', 1,
				decode(repeat('ab', 32), 'hex'), 'lease-key-v1',
				clock_timestamp(), clock_timestamp() + interval '2 minutes'
			)
		`, leaseID, testOrganizationID, testProjectID, assignment.AttemptID, fixture.worker.ID)
		if index == 0 && insertErr != nil {
			t.Fatalf("insert first Reconciler Lease: %v", insertErr)
		}
		if index == 1 {
			requirePostgresConstraint(t, insertErr, "attempt_leases_one_active_per_attempt")
		}
	}
	if err := perAttempt.Rollback(); err != nil {
		t.Fatalf("roll back per-Attempt Lease uniqueness transaction: %v", err)
	}

	perWorker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin per-Worker Lease uniqueness transaction: %v", err)
	}
	defer func() { _ = perWorker.Rollback() }()
	if _, err := perWorker.Exec(
		"UPDATE attempts SET state = 'FAILED', ended_at = clock_timestamp() WHERE id = $1",
		assignment.AttemptID,
	); err != nil {
		t.Fatalf("finish original Attempt for per-Worker Lease test: %v", err)
	}
	const secondAttemptID = "00000000-0000-0000-0000-000000000128"
	if _, err := perWorker.Exec(`
		INSERT INTO attempts (
			id, organization_id, project_id, job_id, attempt_number,
			execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
			state, fence, assigned_at
		) VALUES (
			$1, $2, $3, $4, 1, '00000000-0000-0000-0000-000000000014',
			'00000000-0000-0000-0000-000000000005', $5, 7,
			'ASSIGNED', 1, clock_timestamp()
		)
	`, secondAttemptID, testOrganizationID, testProjectID, secondJob.JobID, fixture.worker.ID); err != nil {
		t.Fatalf("insert replacement Attempt for per-Worker Lease test: %v", err)
	}
	_, err = perWorker.Exec(`
		INSERT INTO attempt_leases (
			id, organization_id, project_id, attempt_id, worker_id, worker_epoch,
			phase, owner_kind, owner_id, fence, token_digest, signing_key_id,
			renewal_protocol_version, issued_at, expires_at
		) VALUES (
			'00000000-0000-0000-0000-000000000129', $1, $2, $3, $4, 7,
			'EXECUTION', 'WORKER', 'spiffe://vela.internal/worker/h3-primary-fixture', 1,
			decode(repeat('cd', 32), 'hex'), 'lease-key-v1',
			2,
			clock_timestamp(), clock_timestamp() + interval '2 minutes'
		)
	`, testOrganizationID, testProjectID, secondAttemptID, fixture.worker.ID)
	requirePostgresConstraint(t, err, "attempt_leases_one_active_per_worker")
}

func TestAttemptIsolationRejectsCrossOrganizationReferencesAndReads(t *testing.T) {
	fixture := newAssignmentFixture(t, "attempt-organization-a", 7)
	if _, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	); err != nil {
		t.Fatalf("create Organization A Assignment: %v", err)
	}
	seedOtherOrganization(t, fixture.database.Admin)
	server := admissionServerForDatabase(t, fixture.database)
	otherResult, err := doSubmitJob(
		server.URL,
		testOtherProjectID,
		bearerCredential(testOtherCredentialID, testOtherSecret),
		"attempt-organization-b",
		[]byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"Organization B execution record"
		}`),
	)
	if err != nil {
		t.Fatalf("submit Organization B Job: %v", err)
	}
	if otherResult.StatusCode != 202 {
		t.Fatalf("Organization B submit status = %d, want 202; body=%s", otherResult.StatusCode, otherResult.Body)
	}
	var otherJob jobResponse
	if err := json.Unmarshal(otherResult.Body, &otherJob); err != nil {
		t.Fatalf("decode Organization B Job: %v", err)
	}
	const otherWorkerID = "00000000-0000-0000-0000-000000000130"
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/h3-organization-b', 1, 'READY', 'HEALTHY'
		)
	`, otherWorkerID); err != nil {
		t.Fatalf("seed Organization B Worker fixture: %v", err)
	}

	_, err = fixture.database.Admin.Exec(`
		INSERT INTO attempts (
			id, organization_id, project_id, job_id, attempt_number,
			execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
			state, fence, assigned_at
		) VALUES (
			'00000000-0000-0000-0000-000000000131', $1, $2, $3, 1,
			'00000000-0000-0000-0000-000000000014',
			'00000000-0000-0000-0000-000000000005', $4, 1,
			'ASSIGNED', 1, clock_timestamp()
		)
	`, testOrganizationID, testProjectID, otherJob.JobID, otherWorkerID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23503" {
		t.Fatalf("cross-Organization Attempt error = %v, want SQLSTATE 23503", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO attempts (
			id, organization_id, project_id, job_id, attempt_number,
			execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
			state, fence, assigned_at
		) VALUES (
			'00000000-0000-0000-0000-000000000132', $1, $2, $3, 1,
			'00000000-0000-0000-0000-000000000014',
			'00000000-0000-0000-0000-000000000005', $4, 1,
			'ASSIGNED', 1, clock_timestamp()
		)
	`, testOtherOrganizationID, testOtherProjectID, otherJob.JobID, otherWorkerID); err != nil {
		t.Fatalf("insert Organization B Attempt: %v", err)
	}

	requestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_request_login",
		"vela-request-password",
	)
	assertRequestReadDenied := func(label, query string) {
		t.Helper()
		tx, beginErr := requestPool.Begin(context.Background())
		if beginErr != nil {
			t.Fatalf("begin %s request transaction: %v", label, beginErr)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, contextErr := tx.Exec(
			context.Background(),
			"SELECT * FROM vela_set_request_context($1, $2, $3)",
			testCredentialID,
			credentialDigest([]byte(testCredentialSecret)),
			"jobs:read",
		); contextErr != nil {
			t.Fatalf("establish Organization A context for %s: %v", label, contextErr)
		}
		_, readErr := tx.Exec(context.Background(), query)
		var readPostgresError *pgconn.PgError
		if !errors.As(readErr, &readPostgresError) || readPostgresError.Code != "42501" {
			t.Fatalf("customer %s read error = %v, want SQLSTATE 42501", label, readErr)
		}
	}
	assertRequestReadDenied("Attempt", "SELECT count(*) FROM attempts")
	assertRequestReadDenied("Lease", "SELECT count(*) FROM attempt_leases")
}

type assignmentFixture struct {
	database  testDatabase
	service   *workercontrol.Service
	worker    workercontrol.AuthenticatedWorker
	candidate workercontrol.AssignmentCandidate
}

func newAssignmentFixture(t *testing.T, key string, epoch int64) assignmentFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "integration fixture switch")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, key, []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"exercise Assignment rejection invariants"
    }`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	if _, err := database.Admin.Exec(`
        INSERT INTO workers (
            id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
        ) VALUES (
            $1, '00000000-0000-0000-0000-000000000005',
            'spiffe://vela.internal/worker/h3-primary-fixture', $2, 'READY', 'HEALTHY'
        )
    `, testWorkerID, epoch); err != nil {
		t.Fatalf("seed READY Worker: %v", err)
	}
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Worker coordinator: %v", err)
	}
	return assignmentFixture{
		database: database,
		service:  service,
		worker:   workercontrol.AuthenticatedWorker{ID: uuid.MustParse(testWorkerID)},
		candidate: workercontrol.AssignmentCandidate{
			JobID:                      uuid.MustParse(job.JobID),
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
		},
	}
}

func newWorkerControlService(pool *pgxpool.Pool) (*workercontrol.Service, error) {
	return workercontrol.NewService(context.Background(), pool, workercontrol.Config{
		LeaseTTL:         2 * time.Minute,
		ActiveLeaseKeyID: "lease-key-v1",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
		},
	})
}

func setLeaseRenewalProtocolGate(t *testing.T, db *sql.DB, enabled bool, receipt string) {
	t.Helper()
	result, err := db.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol($1, $2)",
		enabled,
		receipt,
	)
	if err != nil {
		t.Fatalf("set execution Lease renewal protocol gate to %t: %v", enabled, err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("protocol gate update rows = %d error=%v, want 1", rows, err)
	}
}

type assignmentState struct {
	JobState         string
	JobVersion       int64
	WorkerState      string
	ProjectQueued    int
	ProjectRunning   int
	PoolQueued       int
	AttemptsStarted  int
	Attempts         int
	Leases           int
	AssignmentEvents int
}

func readAssignmentState(t *testing.T, db *sql.DB, jobID, workerID uuid.UUID) assignmentState {
	t.Helper()
	var state assignmentState
	if err := db.QueryRow(`
        SELECT
            (SELECT state::text FROM jobs WHERE id = $1),
            (SELECT version FROM jobs WHERE id = $1),
            (SELECT lifecycle_state::text FROM workers WHERE id = $2),
            (SELECT queued_count FROM projects WHERE id = $3),
            (SELECT running_count FROM projects WHERE id = $3),
            (SELECT queued_count FROM worker_pools WHERE id = '00000000-0000-0000-0000-000000000005'),
            (SELECT attempts_started FROM retry_runtime_states WHERE job_id = $1),
            (SELECT count(*) FROM attempts WHERE job_id = $1),
            (SELECT count(*) FROM attempt_leases),
            (SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'job.assigned')
    `, jobID, workerID, testProjectID).Scan(
		&state.JobState,
		&state.JobVersion,
		&state.WorkerState,
		&state.ProjectQueued,
		&state.ProjectRunning,
		&state.PoolQueued,
		&state.AttemptsStarted,
		&state.Attempts,
		&state.Leases,
		&state.AssignmentEvents,
	); err != nil {
		t.Fatalf("read Assignment state: %v", err)
	}
	return state
}

func waitForRoleDatabaseLock(t *testing.T, db *sql.DB, role string) {
	t.Helper()
	waitForRoleDatabaseLockCount(t, db, role, 1)
}

func waitForRoleDatabaseLockCount(t *testing.T, db *sql.DB, role string, want int) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := db.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE usename = $1 AND wait_event_type = 'Lock'
		`, role).Scan(&waiting); err != nil {
			t.Fatalf("inspect %s database lock wait: %v", role, err)
		}
		if waiting >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s database lock waiters did not reach %d", role, want)
}

func waitForDatabaseTimeAfter(t *testing.T, db *sql.DB, instant time.Time) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		var passed bool
		if err := db.QueryRow("SELECT clock_timestamp() > $1", instant).Scan(&passed); err != nil {
			t.Fatalf("read PostgreSQL wall clock: %v", err)
		}
		if passed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PostgreSQL clock did not pass %s", instant)
}

func sameAssignmentAuthority(left, right workercontrol.Assignment) bool {
	if (left.DebugDumpAuthorization == nil) != (right.DebugDumpAuthorization == nil) {
		return false
	}
	if left.DebugDumpAuthorization != nil &&
		*left.DebugDumpAuthorization != *right.DebugDumpAuthorization {
		return false
	}
	left.DebugDumpAuthorization = nil
	right.DebugDumpAuthorization = nil
	left.LeaseValidFor = 0
	right.LeaseValidFor = 0
	return left == right
}

func requirePostgresConstraint(t *testing.T, err error, constraint string) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.Code != "23505" ||
		postgresError.ConstraintName != constraint {
		t.Fatalf("constraint error = %v, want SQLSTATE 23505 from %s", err, constraint)
	}
}
