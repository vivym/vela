//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestFailSchedulesRetryAndReplaysDurableDecision(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-retry-replay", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || result.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", result, startErr)
	}

	observation := validFailureObservation()
	decision, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		decision.FailureClass != observation.FailureClass ||
		decision.AttemptID != assignment.AttemptID ||
		decision.JobID != assignment.JobID ||
		decision.AttemptState != workercontrol.FailedAttempt ||
		decision.AttemptComputeSeconds != 1 || decision.TotalComputeSeconds != 1 ||
		decision.NextRetryAt == nil ||
		!decision.NextRetryAt.Equal(decision.DecidedAt.Add(30*time.Second)) ||
		decision.JobFence != assignment.LeaseFence+1 || decision.JobVersion != 4 {
		t.Fatalf("RetryDecision = %#v", decision)
	}

	replayed, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil {
		t.Fatalf("replay Fail: %v", err)
	}
	if !reflect.DeepEqual(replayed, decision) {
		t.Fatalf("replayed RetryDecision = %#v, want %#v", replayed, decision)
	}

	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.AttemptState != "FAILED" || !state.AttemptEndedAt.Valid ||
		!state.LeaseRevokedAt.Valid || state.JobState != "RETRY_WAIT" ||
		state.JobFence != decision.JobFence || state.JobVersion != decision.JobVersion ||
		state.AttemptsStarted != 1 || state.ComputeSecondsConsumed != 1 ||
		!state.NextRetryAt.Valid || !state.NextRetryAt.Time.Equal(*decision.NextRetryAt) ||
		state.ProjectQueued != 1 || state.ProjectRetryWait != 1 || state.ProjectRunning != 0 ||
		state.PoolQueued != 1 || state.PoolRetryWait != 1 ||
		state.CreditReservationState != "RESERVED" || state.OrganizationReservedMinor != 1250 ||
		state.WorkerLifecycle != "READY" || state.WorkerReachability != "HEALTHY" ||
		state.DecisionCount != 1 || state.OutboxCount != 1 {
		t.Fatalf("state after replayed Fail = %#v", state)
	}
}

func TestFailTerminatesNonRetryableJobAndReleasesCredit(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-terminal-release", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || result.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", result, startErr)
	}

	observation := validFailureObservation()
	observation.FailureClass = "FATAL_BACKEND"
	observation.FailureFingerprint = "sglang.invalid.model.revision"
	decision, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if decision.Disposition != workercontrol.RetryDispositionFailed ||
		decision.FailureClass != observation.FailureClass ||
		decision.AttemptID != assignment.AttemptID || decision.JobID != assignment.JobID ||
		decision.AttemptState != workercontrol.FailedAttempt ||
		decision.AttemptComputeSeconds != 1 || decision.TotalComputeSeconds != 1 ||
		decision.NextRetryAt != nil ||
		decision.JobFence != assignment.LeaseFence+1 || decision.JobVersion != 4 {
		t.Fatalf("terminal RetryDecision = %#v", decision)
	}

	replayed, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil {
		t.Fatalf("replay terminal Fail: %v", err)
	}
	if !reflect.DeepEqual(replayed, decision) {
		t.Fatalf("replayed terminal RetryDecision = %#v, want %#v", replayed, decision)
	}

	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.AttemptState != "FAILED" || !state.AttemptEndedAt.Valid ||
		!state.LeaseRevokedAt.Valid || state.JobState != "FAILED" ||
		state.JobFence != decision.JobFence || state.JobVersion != decision.JobVersion ||
		state.AttemptsStarted != 1 || state.ComputeSecondsConsumed != 1 || state.NextRetryAt.Valid ||
		state.ProjectQueued != 0 || state.ProjectRetryWait != 0 || state.ProjectRunning != 0 ||
		state.PoolQueued != 0 || state.PoolRetryWait != 0 ||
		state.CreditReservationState != "RELEASED" || state.OrganizationReservedMinor != 0 ||
		state.WorkerLifecycle != "READY" || state.WorkerReachability != "HEALTHY" ||
		state.DecisionCount != 1 || state.OutboxCount != 0 || state.FailedOutboxCount != 1 {
		t.Fatalf("state after terminal Fail replay = %#v", state)
	}
}

func TestFailureOutboxPayloadsAreTypedAndExcludeSensitiveEvidence(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		eventType   string
		disposition workercontrol.RetryDisposition
		mutate      func(*workercontrol.FailureObservation)
	}{
		{
			name:        "Retry wait",
			key:         "failure-outbox-retry-wait",
			eventType:   "job.retry_wait",
			disposition: workercontrol.RetryDispositionRetryWait,
		},
		{
			name:        "terminal failure",
			key:         "failure-outbox-terminal",
			eventType:   "job.failed",
			disposition: workercontrol.RetryDispositionFailed,
			mutate: func(observation *workercontrol.FailureObservation) {
				observation.FailureClass = "FATAL_BACKEND"
				observation.FailureFingerprint = "sensitive.failure.fingerprint"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, test.key, 7)
			assignment, err := fixture.service.Acquire(
				context.Background(), fixture.worker, 7, &fixture.candidate,
			)
			if err != nil {
				t.Fatalf("create Assignment: %v", err)
			}
			observation := validFailureObservation()
			observation.ErrorSummary = "sensitive diagnostic summary must remain receipt-only"
			observation.GPUUUIDs = []string{"GPU-sensitive-00000000-0000-0000-0000-000000000001"}
			if test.mutate != nil {
				test.mutate(&observation)
			}
			decision, err := fixture.service.Fail(
				context.Background(), fixture.worker, leaseCredentials(assignment), observation,
			)
			if err != nil || decision.Disposition != test.disposition {
				t.Fatalf("Fail = %#v error=%v", decision, err)
			}

			var payload []byte
			if err := fixture.database.Admin.QueryRow(`
				SELECT payload
				FROM outbox_events
				WHERE aggregate_id = $1 AND event_type = $2
			`, assignment.JobID, test.eventType).Scan(&payload); err != nil {
				t.Fatalf("read %s payload: %v", test.eventType, err)
			}
			for label, sensitive := range map[string]string{
				"error summary":       observation.ErrorSummary,
				"failure fingerprint": observation.FailureFingerprint,
				"GPU UUID":            observation.GPUUUIDs[0],
				"Worker identity":     assignment.WorkerID.String(),
			} {
				if bytes.Contains(payload, []byte(sensitive)) {
					t.Fatalf("%s payload contains sensitive %s", test.eventType, label)
				}
			}
			var envelope velav1.EventEnvelope
			if err := proto.Unmarshal(payload, &envelope); err != nil {
				t.Fatalf("decode %s payload: %v", test.eventType, err)
			}
			if envelope.GetEventType() != test.eventType ||
				envelope.GetAggregateId() != assignment.JobID.String() ||
				envelope.GetAggregateVersion() != uint64(decision.JobVersion) ||
				envelope.GetOccurredAt() == nil || !envelope.GetOccurredAt().IsValid() {
				t.Fatalf("%s envelope = %#v", test.eventType, &envelope)
			}
			if test.eventType == "job.retry_wait" {
				retryWait := envelope.GetJobRetryWait()
				if retryWait == nil || envelope.GetJobFailed() != nil ||
					retryWait.GetAttemptId() != assignment.AttemptID.String() ||
					retryWait.GetFailureClass() != observation.FailureClass ||
					retryWait.GetJobFence() != uint64(decision.JobFence) ||
					retryWait.GetNextRetryAt() == nil || !retryWait.GetNextRetryAt().IsValid() {
					t.Fatalf("job.retry_wait typed payload = %#v", retryWait)
				}
				fields := retryWait.ProtoReflect().Descriptor().Fields()
				if fields.ByJSONName("workerId") != nil || fields.ByJSONName("workerEpoch") != nil {
					t.Fatal("job.retry_wait schema exposes Worker identity")
				}
			} else {
				failed := envelope.GetJobFailed()
				if failed == nil || envelope.GetJobRetryWait() != nil ||
					failed.GetAttemptId() != assignment.AttemptID.String() ||
					failed.GetFailureClass() != observation.FailureClass ||
					failed.GetAttemptState() != string(workercontrol.FailedAttempt) ||
					failed.GetJobFence() != uint64(decision.JobFence) {
					t.Fatalf("job.failed typed payload = %#v", failed)
				}
				fields := failed.ProtoReflect().Descriptor().Fields()
				if fields.ByJSONName("workerId") != nil || fields.ByJSONName("workerEpoch") != nil {
					t.Fatal("job.failed schema exposes Worker identity")
				}
			}
		})
	}
}

func TestFailRejectsDifferentReplayWithoutMutation(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-different-replay", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	original := validFailureObservation()
	decision, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), original,
	)
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	changed := original
	changed.ErrorSummary = "a different observation must not replace the committed receipt"
	rejected, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), changed,
	)
	if err != nil {
		t.Fatalf("different replay Fail: %v", err)
	}
	if rejected.Disposition != workercontrol.RetryDispositionRejectedStaleLease {
		t.Fatalf("different replay decision = %#v", rejected)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.JobFence != decision.JobFence || state.JobVersion != decision.JobVersion ||
		state.ComputeSecondsConsumed != decision.TotalComputeSeconds ||
		state.DecisionCount != 1 || state.OutboxCount != 1 || state.FailedOutboxCount != 0 {
		t.Fatalf("state after different replay = %#v", state)
	}
}

func TestFailChargesAssignedAttemptZeroCompute(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-assigned-zero-compute", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	decision, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), validFailureObservation(),
	)
	if err != nil {
		t.Fatalf("Fail ASSIGNED Attempt: %v", err)
	}
	if decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		decision.AttemptComputeSeconds != 0 || decision.TotalComputeSeconds != 0 {
		t.Fatalf("ASSIGNED RetryDecision = %#v", decision)
	}
	var billableStartedAt sql.NullTime
	if err := fixture.database.Admin.QueryRow(
		"SELECT billable_started_at FROM jobs WHERE id = $1", assignment.JobID,
	).Scan(&billableStartedAt); err != nil {
		t.Fatalf("read Billable Start: %v", err)
	}
	if billableStartedAt.Valid {
		t.Fatalf("ASSIGNED failure created Billable Start at %s", billableStartedAt.Time)
	}
}

func TestFailReplaysCommittedDecisionAfterWorkerEpochAdvance(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-replay-after-epoch", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	observation := validFailureObservation()
	decision, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(
		"UPDATE workers SET epoch = 8, updated_at = clock_timestamp() WHERE id = $1",
		fixture.worker.ID,
	); err != nil {
		t.Fatalf("advance Worker epoch: %v", err)
	}
	replayed, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil {
		t.Fatalf("replay Fail after Worker epoch advance: %v", err)
	}
	if !reflect.DeepEqual(replayed, decision) {
		t.Fatalf("post-epoch replay = %#v, want %#v", replayed, decision)
	}
}

func TestFailEnforcesEveryRetryDecisionBudget(t *testing.T) {
	tests := []struct {
		name              string
		key               string
		policyUpdate      string
		observationChange func(*workercontrol.FailureObservation)
	}{
		{
			name: "Worker recommendation is false",
			key:  "failure-budget-recommendation",
			observationChange: func(observation *workercontrol.FailureObservation) {
				observation.RetryRecommended = false
			},
		},
		{
			name:         "Attempt budget is exhausted",
			key:          "failure-budget-attempts",
			policyUpdate: "execution_max_attempts = 1",
		},
		{
			name:         "compute budget is exhausted",
			key:          "failure-budget-compute",
			policyUpdate: "execution_max_total_compute_seconds = 1",
		},
		{
			name:         "backoff policy is invalid",
			key:          "failure-budget-policy",
			policyUpdate: "execution_retry_backoff_policy = '{}'::jsonb",
		},
		{
			name:         "backoff reaches Job Expiry",
			key:          "failure-budget-expiry",
			policyUpdate: "job_expires_at = clock_timestamp() + interval '5 seconds'",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, test.key, 7)
			if test.policyUpdate != "" {
				forceJobPolicySnapshot(t, fixture.database.Admin, fixture.candidate.JobID, test.policyUpdate)
			}
			assignment, err := fixture.service.Acquire(
				context.Background(), fixture.worker, 7, &fixture.candidate,
			)
			if err != nil {
				t.Fatalf("create Assignment: %v", err)
			}
			if result, startErr := fixture.service.Start(
				context.Background(), fixture.worker, leaseCredentials(assignment),
			); startErr != nil || result.Decision != workercontrol.StartGranted {
				t.Fatalf("Start = %#v error=%v", result, startErr)
			}
			observation := validFailureObservation()
			if test.observationChange != nil {
				test.observationChange(&observation)
			}
			decision, err := fixture.service.Fail(
				context.Background(), fixture.worker, leaseCredentials(assignment), observation,
			)
			if err != nil {
				t.Fatalf("Fail: %v", err)
			}
			if decision.Disposition != workercontrol.RetryDispositionFailed ||
				decision.NextRetryAt != nil {
				t.Fatalf("RetryDecision = %#v", decision)
			}
			state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
			if state.JobState != "FAILED" || state.ProjectRunning != 0 ||
				state.ProjectRetryWait != 0 || state.PoolRetryWait != 0 ||
				state.CreditReservationState != "RELEASED" || state.OrganizationReservedMinor != 0 ||
				state.DecisionCount != 1 || state.OutboxCount != 0 || state.FailedOutboxCount != 1 {
				t.Fatalf("terminal budget state = %#v", state)
			}
		})
	}
}

func TestReconcileExpiresQueuedJobWithoutAttempt(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-queued-job-expiry", 7)
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		"job_expires_at = clock_timestamp()",
	)

	result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile queued Job Expiry: %v", err)
	}
	if !result.Processed || result.Source != workercontrol.FailureSourceJobExpired ||
		result.Decision.Disposition != workercontrol.RetryDispositionFailed ||
		result.Decision.JobID != fixture.candidate.JobID ||
		result.Decision.AttemptID != uuid.Nil || result.Decision.AttemptState != "" ||
		result.Decision.AttemptComputeSeconds != 0 || result.Decision.TotalComputeSeconds != 0 ||
		result.Decision.NextRetryAt != nil || result.Decision.JobFence != 1 ||
		result.Decision.JobVersion != 2 {
		t.Fatalf("queued Job Expiry result = %#v", result)
	}
	state := readJobExpiryState(t, fixture.database.Admin, fixture.candidate.JobID)
	if state.JobState != "FAILED" || state.JobFence != 1 || state.JobVersion != 2 ||
		state.ProjectQueued != 0 || state.ProjectRetryWait != 0 || state.ProjectRunning != 0 ||
		state.PoolQueued != 0 || state.PoolRetryWait != 0 ||
		state.CreditReservationState != "RELEASED" || state.OrganizationReservedMinor != 0 ||
		state.DecisionCount != 1 || state.FailedOutboxCount != 1 {
		t.Fatalf("queued Job Expiry state = %#v", state)
	}

	replayed, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("replay queued Job Expiry scan: %v", err)
	}
	if replayed.Processed {
		t.Fatalf("second queued Job Expiry scan = %#v, want no work", replayed)
	}
}

func TestReconcileExecutionLeaseOnlyAfterWorkerLostGrace(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-worker-lost-grace", 7)
	service := newFailureReconciler(t, fixture.database, 2*time.Second)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	forceLeaseExpiry(t, fixture.database.Admin, assignment.AttemptID, time.Second)

	beforeGrace, err := service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile before Worker Lost grace: %v", err)
	}
	if beforeGrace.Processed {
		t.Fatalf("reconciliation before Worker Lost grace = %#v", beforeGrace)
	}
	forceLeaseExpiry(t, fixture.database.Admin, assignment.AttemptID, 3*time.Second)

	result, err := service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile Worker Lost: %v", err)
	}
	if !result.Processed || result.Source != workercontrol.FailureSourceExecutionLeaseExpired ||
		result.Decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		result.Decision.AttemptID != assignment.AttemptID ||
		result.Decision.AttemptState != workercontrol.LostAttempt ||
		result.Decision.AttemptComputeSeconds != 0 || result.Decision.TotalComputeSeconds != 0 ||
		result.Decision.NextRetryAt == nil || result.Decision.JobFence != assignment.LeaseFence+1 ||
		result.Decision.JobVersion != 3 {
		t.Fatalf("Worker Lost result = %#v", result)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.AttemptState != "LOST" || !state.AttemptEndedAt.Valid ||
		!state.LeaseRevokedAt.Valid || state.JobState != "RETRY_WAIT" ||
		state.ProjectRetryWait != 1 || state.ProjectRunning != 0 || state.PoolRetryWait != 1 ||
		state.CreditReservationState != "RESERVED" || state.OrganizationReservedMinor != 1250 ||
		state.WorkerLifecycle != "DRAINING" || state.WorkerReachability != "OFFLINE" ||
		state.DecisionCount != 1 || state.OutboxCount != 1 || state.FailedOutboxCount != 0 {
		t.Fatalf("Worker Lost state = %#v", state)
	}
}

func TestReconcileUsesThirtySecondDefaultWorkerLostGrace(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-default-worker-lost-grace", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	forceLeaseExpiry(t, fixture.database.Admin, assignment.AttemptID, 29*time.Second)

	beforeDefaultGrace, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile before default Worker Lost grace: %v", err)
	}
	if beforeDefaultGrace.Processed {
		t.Fatalf("reconciliation before default Worker Lost grace = %#v", beforeDefaultGrace)
	}
	forceLeaseExpiry(t, fixture.database.Admin, assignment.AttemptID, 31*time.Second)

	result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile after default Worker Lost grace: %v", err)
	}
	if !result.Processed || result.Source != workercontrol.FailureSourceExecutionLeaseExpired ||
		result.Decision.AttemptState != workercontrol.LostAttempt {
		t.Fatalf("default Worker Lost grace result = %#v", result)
	}
}

func TestRetryReplacementUsesAnotherWorkerHigherFenceAndResetProgress(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-real-replacement", 7)
	first, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create first Assignment: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(first),
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start first Attempt = %#v error=%v", started, err)
	}
	progress := 0.8
	heartbeat := validHeartbeatObservation(1)
	heartbeat.BackendStageProgress = &progress
	if result, heartbeatErr := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(first), heartbeat,
	); heartbeatErr != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("Heartbeat first Attempt = %#v error=%v", result, heartbeatErr)
	}
	observation := validFailureObservation()
	decision, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(first), observation,
	)
	if err != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("Fail first Attempt = %#v error=%v", decision, err)
	}

	secondWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000021")
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/h3-replacement', 7, 'READY', 'HEALTHY'
		)
	`, secondWorkerID); err != nil {
		t.Fatalf("seed replacement Worker: %v", err)
	}
	secondWorker := workercontrol.AuthenticatedWorker{ID: secondWorkerID}
	candidate := &workercontrol.AssignmentCandidate{
		JobID:                      first.JobID,
		ExpectedJobVersion:         decision.JobVersion,
		ExecutionProfileRevisionID: first.ExecutionProfileRevisionID,
	}
	if _, err := fixture.service.Acquire(context.Background(), secondWorker, 7, candidate); err == nil {
		t.Fatal("replacement Acquire before next_retry_at succeeded")
	} else {
		var candidateFailure *workercontrol.Failure
		if !errors.As(err, &candidateFailure) || candidateFailure.Code != workercontrol.FailureCandidateUnavailable {
			t.Fatalf("replacement Acquire before next_retry_at error = %v", err)
		}
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() - interval '1 second'
		WHERE job_id = $1
	`, first.JobID); err != nil {
		t.Fatalf("make Retry eligible: %v", err)
	}
	if _, err := fixture.service.Acquire(context.Background(), fixture.worker, 7, candidate); err == nil {
		t.Fatal("excluded failed Worker received replacement Assignment")
	} else {
		var candidateFailure *workercontrol.Failure
		if !errors.As(err, &candidateFailure) || candidateFailure.Code != workercontrol.FailureCandidateUnavailable {
			t.Fatalf("excluded Worker Acquire error = %v", err)
		}
	}
	replacement, err := fixture.service.Acquire(context.Background(), secondWorker, 7, candidate)
	if err != nil {
		t.Fatalf("Acquire replacement: %v", err)
	}
	if replacement.AttemptID == first.AttemptID || replacement.AttemptNumber != 2 ||
		replacement.WorkerID != secondWorkerID ||
		replacement.ExecutionProfileRevisionID != first.ExecutionProfileRevisionID ||
		replacement.LeaseFence <= decision.JobFence || replacement.LeaseFence <= first.LeaseFence {
		t.Fatalf("replacement Assignment = %#v after decision %#v", replacement, decision)
	}

	server := admissionServerForDatabase(t, fixture.database)
	view, _ := fetchJobView(t, server.URL, first.JobID)
	if view.State != "ASSIGNED" || view.Phase == nil || *view.Phase != "PREPARING" ||
		view.AttemptsStarted != 2 || view.NextRetryAt != nil ||
		view.PhaseProgress != nil || view.EstimatedFinishAt != nil || view.ProgressUpdatedAt != nil {
		t.Fatalf("replacement Job view reused old Attempt progress: %#v", view)
	}
	var billableStartedAt time.Time
	if err := fixture.database.Admin.QueryRow(
		"SELECT billable_started_at FROM jobs WHERE id = $1", first.JobID,
	).Scan(&billableStartedAt); err != nil {
		t.Fatalf("read preserved Billable Start: %v", err)
	}
	if !billableStartedAt.Equal(started.StartedAt) {
		t.Fatalf("replacement Billable Start = %s, want %s", billableStartedAt, started.StartedAt)
	}
	if oldStart, oldStartErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(first),
	); oldStartErr != nil || oldStart.Decision != workercontrol.Stop ||
		oldStart.StopReason != workercontrol.StopInvalidAuthority {
		t.Fatalf("old Start authority = %#v error=%v", oldStart, oldStartErr)
	}
	if oldHeartbeat, oldHeartbeatErr := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(first), validHeartbeatObservation(2),
	); oldHeartbeatErr != nil || oldHeartbeat.Decision != workercontrol.HeartbeatStop ||
		oldHeartbeat.StopReason != workercontrol.StopInvalidAuthority {
		t.Fatalf("old Heartbeat authority = %#v error=%v", oldHeartbeat, oldHeartbeatErr)
	}
	changedObservation := observation
	changedObservation.ErrorSummary = "late old authority after replacement"
	if oldFail, oldFailErr := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(first), changedObservation,
	); oldFailErr != nil || oldFail.Disposition != workercontrol.RetryDispositionRejectedStaleLease {
		t.Fatalf("old Fail authority = %#v error=%v", oldFail, oldFailErr)
	}
}

func TestReconcileJobExpiryFencesRunningAttempt(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-running-job-expiry", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || result.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", result, startErr)
	}
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		assignment.JobID,
		"job_expires_at = clock_timestamp()",
	)

	result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile running Job Expiry: %v", err)
	}
	if !result.Processed || result.Source != workercontrol.FailureSourceJobExpired ||
		result.Decision.Disposition != workercontrol.RetryDispositionFailed ||
		result.Decision.AttemptID != assignment.AttemptID ||
		result.Decision.AttemptState != workercontrol.FailedAttempt ||
		result.Decision.AttemptComputeSeconds != 1 || result.Decision.TotalComputeSeconds != 1 ||
		result.Decision.NextRetryAt != nil ||
		result.Decision.JobFence != assignment.LeaseFence+1 || result.Decision.JobVersion != 4 {
		t.Fatalf("running Job Expiry result = %#v", result)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.AttemptState != "FAILED" || !state.AttemptEndedAt.Valid ||
		!state.LeaseRevokedAt.Valid || state.JobState != "FAILED" ||
		state.ProjectRunning != 0 || state.ProjectRetryWait != 0 || state.PoolRetryWait != 0 ||
		state.CreditReservationState != "RELEASED" || state.OrganizationReservedMinor != 0 ||
		state.WorkerLifecycle != "DRAINING" || state.WorkerReachability != "HEALTHY" ||
		state.DecisionCount != 1 || state.OutboxCount != 0 || state.FailedOutboxCount != 1 {
		t.Fatalf("running Job Expiry state = %#v", state)
	}
}

func TestReconcileExpiresRetryWaitJobWithoutAttempt(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-retry-wait-expiry", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if decision, failErr := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), validFailureObservation(),
	); failErr != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("schedule Retry = %#v error=%v", decision, failErr)
	}
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		assignment.JobID,
		"job_expires_at = clock_timestamp()",
	)

	result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile RETRY_WAIT Job Expiry: %v", err)
	}
	if !result.Processed || result.Source != workercontrol.FailureSourceJobExpired ||
		result.Decision.Disposition != workercontrol.RetryDispositionFailed ||
		result.Decision.AttemptID != uuid.Nil || result.Decision.AttemptState != "" ||
		result.Decision.AttemptComputeSeconds != 0 || result.Decision.TotalComputeSeconds != 0 ||
		result.Decision.NextRetryAt != nil || result.Decision.JobFence != assignment.LeaseFence+2 ||
		result.Decision.JobVersion != 4 {
		t.Fatalf("RETRY_WAIT Job Expiry result = %#v", result)
	}
	state := readJobExpiryState(t, fixture.database.Admin, assignment.JobID)
	if state.JobState != "FAILED" || state.JobFence != result.Decision.JobFence ||
		state.JobVersion != result.Decision.JobVersion ||
		state.ProjectQueued != 0 || state.ProjectRetryWait != 0 || state.ProjectRunning != 0 ||
		state.PoolQueued != 0 || state.PoolRetryWait != 0 ||
		state.CreditReservationState != "RELEASED" || state.OrganizationReservedMinor != 0 ||
		state.DecisionCount != 1 || state.FailedOutboxCount != 1 {
		t.Fatalf("RETRY_WAIT Job Expiry state = %#v", state)
	}
}

func TestConcurrentFailReplaysOneDurableDecision(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-concurrent-replay", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	observation := validFailureObservation()
	start := make(chan struct{})
	decisions := make(chan workercontrol.RetryDecision, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			decision, failErr := fixture.service.Fail(
				context.Background(), fixture.worker, leaseCredentials(assignment), observation,
			)
			decisions <- decision
			errorsFound <- failErr
		}()
	}
	close(start)
	wait.Wait()
	close(decisions)
	close(errorsFound)
	for failErr := range errorsFound {
		if failErr != nil {
			t.Fatalf("concurrent Fail: %v", failErr)
		}
	}
	all := make([]workercontrol.RetryDecision, 0, 2)
	for decision := range decisions {
		all = append(all, decision)
	}
	if len(all) != 2 || !reflect.DeepEqual(all[0], all[1]) ||
		all[0].Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("concurrent Fail decisions = %#v", all)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.DecisionCount != 1 || state.OutboxCount != 1 ||
		state.ComputeSecondsConsumed != all[0].TotalComputeSeconds ||
		state.ProjectRetryWait != 1 || state.ProjectRunning != 0 || state.PoolRetryWait != 1 {
		t.Fatalf("concurrent Fail state = %#v", state)
	}
}

func TestConcurrentTerminalFailReleasesCreditExactlyOnce(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-concurrent-terminal", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	observation := validFailureObservation()
	observation.FailureClass = "FATAL_BACKEND"
	observation.FailureFingerprint = "failure.concurrent.terminal"
	start := make(chan struct{})
	decisions := make(chan workercontrol.RetryDecision, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			decision, failErr := fixture.service.Fail(
				context.Background(), fixture.worker, leaseCredentials(assignment), observation,
			)
			decisions <- decision
			errorsFound <- failErr
		}()
	}
	close(start)
	wait.Wait()
	close(decisions)
	close(errorsFound)
	for failErr := range errorsFound {
		if failErr != nil {
			t.Fatalf("concurrent terminal Fail: %v", failErr)
		}
	}
	all := make([]workercontrol.RetryDecision, 0, 2)
	for decision := range decisions {
		all = append(all, decision)
	}
	if len(all) != 2 || !reflect.DeepEqual(all[0], all[1]) ||
		all[0].Disposition != workercontrol.RetryDispositionFailed {
		t.Fatalf("concurrent terminal Fail decisions = %#v", all)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.DecisionCount != 1 || state.FailedOutboxCount != 1 || state.OutboxCount != 0 ||
		state.JobState != "FAILED" || state.ProjectRunning != 0 || state.ProjectRetryWait != 0 ||
		state.PoolRetryWait != 0 || state.CreditReservationState != "RELEASED" ||
		state.OrganizationReservedMinor != 0 {
		t.Fatalf("concurrent terminal Fail state = %#v", state)
	}
}

func TestConcurrentReconcilersCommitOneLeaseLoss(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-concurrent-lease-loss", 7)
	service := newFailureReconciler(t, fixture.database, 2*time.Second)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	forceLeaseExpiry(t, fixture.database.Admin, assignment.AttemptID, 3*time.Second)
	start := make(chan struct{})
	results := make(chan workercontrol.ReconciliationResult, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, reconcileErr := service.ReconcileNextExecutionFailure(context.Background())
			results <- result
			errorsFound <- reconcileErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for reconcileErr := range errorsFound {
		if reconcileErr != nil {
			t.Fatalf("concurrent reconciliation: %v", reconcileErr)
		}
	}
	processed := 0
	for result := range results {
		if result.Processed {
			processed++
			if result.Source != workercontrol.FailureSourceExecutionLeaseExpired ||
				result.Decision.AttemptState != workercontrol.LostAttempt {
				t.Fatalf("processed reconciliation = %#v", result)
			}
		}
	}
	if processed != 1 {
		t.Fatalf("processed reconciliations = %d, want 1", processed)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.DecisionCount != 1 || state.OutboxCount != 1 ||
		state.ComputeSecondsConsumed != 0 || state.ProjectRetryWait != 1 ||
		state.ProjectRunning != 0 || state.PoolRetryWait != 1 {
		t.Fatalf("concurrent reconciliation state = %#v", state)
	}
}

func TestLeaseLossWinsLateFailAndHeartbeatWithoutDeadlock(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-lease-loss-race", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	forceLeaseExpiry(t, fixture.database.Admin, assignment.AttemptID, 31*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type failResult struct {
		decision workercontrol.RetryDecision
		err      error
	}
	type heartbeatResult struct {
		result workercontrol.HeartbeatResult
		err    error
	}
	failResults := make(chan failResult, 1)
	heartbeatResults := make(chan heartbeatResult, 1)
	reconcileResults := make(chan workercontrol.ReconciliationResult, 1)
	reconcileErrors := make(chan error, 1)
	var wait sync.WaitGroup
	wait.Add(3)
	go func() {
		defer wait.Done()
		<-start
		decision, failErr := fixture.service.Fail(
			ctx, fixture.worker, leaseCredentials(assignment), validFailureObservation(),
		)
		failResults <- failResult{decision: decision, err: failErr}
	}()
	go func() {
		defer wait.Done()
		<-start
		result, heartbeatErr := fixture.service.Heartbeat(
			ctx, fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
		)
		heartbeatResults <- heartbeatResult{result: result, err: heartbeatErr}
	}()
	go func() {
		defer wait.Done()
		<-start
		result, reconcileErr := fixture.service.ReconcileNextExecutionFailure(ctx)
		reconcileResults <- result
		reconcileErrors <- reconcileErr
	}()
	close(start)
	wait.Wait()

	fail := <-failResults
	if fail.err != nil || fail.decision.Disposition != workercontrol.RetryDispositionRejectedStaleLease {
		t.Fatalf("late Fail = %#v error=%v", fail.decision, fail.err)
	}
	heartbeat := <-heartbeatResults
	if heartbeat.err != nil || heartbeat.result.Decision != workercontrol.HeartbeatStop ||
		(heartbeat.result.StopReason != workercontrol.StopLeaseExpired &&
			heartbeat.result.StopReason != workercontrol.StopInvalidAuthority) {
		t.Fatalf("late Heartbeat = %#v error=%v", heartbeat.result, heartbeat.err)
	}
	if reconcileErr := <-reconcileErrors; reconcileErr != nil {
		t.Fatalf("Lease-loss reconciliation: %v", reconcileErr)
	}
	reconciled := <-reconcileResults
	if !reconciled.Processed ||
		reconciled.Source != workercontrol.FailureSourceExecutionLeaseExpired ||
		reconciled.Decision.AttemptState != workercontrol.LostAttempt {
		t.Fatalf("Lease-loss reconciliation = %#v", reconciled)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.JobState != "RETRY_WAIT" || state.AttemptState != "LOST" ||
		state.DecisionCount != 1 || state.OutboxCount != 1 ||
		state.ProjectRetryWait != 1 || state.ProjectRunning != 0 || state.PoolRetryWait != 1 {
		t.Fatalf("Lease-loss race state = %#v", state)
	}
}

func TestJobExpiryWinsLateFailAndHeartbeatWithoutDeadlock(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-expiry-race", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || result.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", result, startErr)
	}
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		assignment.JobID,
		"job_expires_at = clock_timestamp()",
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type failResult struct {
		decision workercontrol.RetryDecision
		err      error
	}
	type heartbeatResult struct {
		result workercontrol.HeartbeatResult
		err    error
	}
	failResults := make(chan failResult, 1)
	heartbeatResults := make(chan heartbeatResult, 1)
	reconcileResults := make(chan workercontrol.ReconciliationResult, 1)
	reconcileErrors := make(chan error, 1)
	var wait sync.WaitGroup
	wait.Add(3)
	go func() {
		defer wait.Done()
		<-start
		decision, failErr := fixture.service.Fail(
			ctx, fixture.worker, leaseCredentials(assignment), validFailureObservation(),
		)
		failResults <- failResult{decision: decision, err: failErr}
	}()
	go func() {
		defer wait.Done()
		<-start
		result, heartbeatErr := fixture.service.Heartbeat(
			ctx, fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
		)
		heartbeatResults <- heartbeatResult{result: result, err: heartbeatErr}
	}()
	go func() {
		defer wait.Done()
		<-start
		result, reconcileErr := fixture.service.ReconcileNextExecutionFailure(ctx)
		reconcileResults <- result
		reconcileErrors <- reconcileErr
	}()
	close(start)
	wait.Wait()
	fail := <-failResults
	if fail.err != nil || fail.decision.Disposition != workercontrol.RetryDispositionRejectedStaleLease {
		t.Fatalf("late Fail = %#v error=%v", fail.decision, fail.err)
	}
	heartbeat := <-heartbeatResults
	if heartbeat.err != nil || heartbeat.result.Decision != workercontrol.HeartbeatStop ||
		(heartbeat.result.StopReason != workercontrol.StopJobExpired &&
			heartbeat.result.StopReason != workercontrol.StopInvalidAuthority) {
		t.Fatalf("late Heartbeat = %#v error=%v", heartbeat.result, heartbeat.err)
	}
	if reconcileErr := <-reconcileErrors; reconcileErr != nil {
		t.Fatalf("Job Expiry reconciliation: %v", reconcileErr)
	}
	reconciled := <-reconcileResults
	if !reconciled.Processed || reconciled.Source != workercontrol.FailureSourceJobExpired {
		t.Fatalf("Job Expiry reconciliation = %#v", reconciled)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.JobState != "FAILED" || state.DecisionCount != 1 || state.FailedOutboxCount != 1 ||
		state.ProjectRunning != 0 || state.OrganizationReservedMinor != 0 {
		t.Fatalf("Job Expiry race state = %#v", state)
	}
}

func TestLeaseLossCapsRunningComputeAtLeaseExpiryAndRoundsUpOnce(t *testing.T) {
	fixture := newAssignmentFixture(t, "reconcile-running-compute", 7)
	service := newFailureReconciler(t, fixture.database, 2*time.Second)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || result.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", result, startErr)
	}
	forceRunningLeaseComputeWindow(t, fixture.database.Admin, assignment.AttemptID)

	result, err := service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile RUNNING Lease loss: %v", err)
	}
	if !result.Processed || result.Decision.AttemptState != workercontrol.LostAttempt ||
		result.Decision.AttemptComputeSeconds != 2 || result.Decision.TotalComputeSeconds != 2 {
		t.Fatalf("RUNNING Lease-loss decision = %#v", result)
	}
	replayed, err := service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("replay RUNNING Lease-loss scan: %v", err)
	}
	if replayed.Processed {
		t.Fatalf("replayed RUNNING Lease-loss scan = %#v", replayed)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.ComputeSecondsConsumed != 2 || state.DecisionCount != 1 || state.OutboxCount != 1 {
		t.Fatalf("RUNNING Lease-loss replay state = %#v", state)
	}
}

func TestFailureReceiptsAreImmutableAndInaccessibleToRequestRole(t *testing.T) {
	fixture := newAssignmentFixture(t, "failure-receipt-isolation", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if _, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), validFailureObservation(),
	); err != nil {
		t.Fatalf("create failure receipt: %v", err)
	}
	setLeaseRenewalProtocolGate(
		t,
		fixture.database.Admin,
		false,
		"verify execution failure isolation during N-1 expand",
	)
	for _, table := range []string{"execution_failure_decisions", "execution_retry_evidence"} {
		var rowSecurity, forceRowSecurity bool
		if err := fixture.database.Admin.QueryRow(`
			SELECT relrowsecurity, relforcerowsecurity
			FROM pg_class
			WHERE oid = $1::regclass
		`, table).Scan(&rowSecurity, &forceRowSecurity); err != nil {
			t.Fatalf("read %s RLS flags: %v", table, err)
		}
		if !rowSecurity || !forceRowSecurity {
			t.Fatalf("%s RLS flags = enabled %t forced %t", table, rowSecurity, forceRowSecurity)
		}
	}
	var publicExcluded, publicFingerprints, protectedExcluded, protectedFingerprints int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			jsonb_array_length(runtime.excluded_workers),
			jsonb_array_length(runtime.failure_fingerprints),
			jsonb_array_length(evidence.excluded_workers),
			jsonb_array_length(evidence.failure_fingerprints)
		FROM retry_runtime_states AS runtime
		JOIN execution_retry_evidence AS evidence USING (job_id)
		WHERE runtime.job_id = $1
	`, assignment.JobID).Scan(
		&publicExcluded,
		&publicFingerprints,
		&protectedExcluded,
		&protectedFingerprints,
	); err != nil {
		t.Fatalf("read split retry evidence: %v", err)
	}
	if publicExcluded != 0 || publicFingerprints != 0 ||
		protectedExcluded != 1 || protectedFingerprints != 1 {
		t.Fatalf(
			"split retry evidence = public exclusions %d fingerprints %d, protected exclusions %d fingerprints %d",
			publicExcluded,
			publicFingerprints,
			protectedExcluded,
			protectedFingerprints,
		)
	}

	requestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_request_login",
		"vela-request-password",
	)
	requestTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request-role failure receipt transaction: %v", err)
	}
	defer func() { _ = requestTx.Rollback(context.Background()) }()
	if _, err := requestTx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		"jobs:read",
	); err != nil {
		t.Fatalf("establish request context: %v", err)
	}
	var requestExcluded, requestFingerprints int
	if err := requestTx.QueryRow(
		context.Background(),
		`SELECT jsonb_array_length(excluded_workers), jsonb_array_length(failure_fingerprints)
		 FROM retry_runtime_states
		 WHERE job_id = $1`,
		assignment.JobID,
	).Scan(&requestExcluded, &requestFingerprints); err != nil {
		t.Fatalf("request role read public RetryRuntimeState projection: %v", err)
	}
	if requestExcluded != 0 || requestFingerprints != 0 {
		t.Fatalf(
			"request role observed raw retry evidence: exclusions=%d fingerprints=%d",
			requestExcluded,
			requestFingerprints,
		)
	}
	if _, err := requestTx.Exec(
		context.Background(),
		"SELECT error_summary, failure_fingerprint, worker_id FROM execution_failure_decisions",
	); err == nil {
		t.Fatal("request role read raw failure receipt")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
			t.Fatalf("request-role receipt read error = %v, want SQLSTATE 42501", err)
		}
	}
	for name, mutation := range map[string]string{
		"failure receipt": `
			UPDATE execution_failure_decisions
			SET error_summary = 'request-role rewrite'
			WHERE attempt_id = $1
		`,
		"RetryRuntimeState": `
			UPDATE retry_runtime_states
			SET next_retry_at = NULL
			WHERE job_id = (SELECT job_id FROM attempts WHERE id = $1)
		`,
		"protected retry evidence": `
			UPDATE execution_retry_evidence
			SET excluded_workers = '[]'::jsonb
			WHERE job_id = (SELECT job_id FROM attempts WHERE id = $1)
		`,
	} {
		if _, err := requestPool.Exec(context.Background(), mutation, assignment.AttemptID); err == nil {
			t.Fatalf("request role mutated %s", name)
		} else {
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
				t.Fatalf("request-role %s mutation error = %v, want SQLSTATE 42501", name, err)
			}
		}
	}
	if _, err := requestPool.Exec(
		context.Background(),
		"SELECT excluded_workers, failure_fingerprints FROM execution_retry_evidence",
	); err == nil {
		t.Fatal("request role read protected retry evidence")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
			t.Fatalf("request-role protected evidence read error = %v, want SQLSTATE 42501", err)
		}
	}

	for _, mutation := range []string{
		"UPDATE execution_failure_decisions SET error_summary = 'rewritten' WHERE attempt_id = $1",
		"DELETE FROM execution_failure_decisions WHERE attempt_id = $1",
	} {
		if _, err := fixture.database.Admin.Exec(mutation, assignment.AttemptID); err == nil {
			t.Fatalf("immutable failure receipt accepted %q", mutation)
		} else {
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
				t.Fatalf("failure receipt mutation error = %v, want SQLSTATE P0001", err)
			}
		}
	}
	if _, err := fixture.database.Admin.Exec(
		"UPDATE attempts SET ended_at = NULL WHERE id = $1", assignment.AttemptID,
	); err == nil {
		t.Fatal("terminal Attempt accepted null ended_at")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
			t.Fatalf("terminal Attempt ended_at error = %v, want SQLSTATE 23514", err)
		}
	}
}

func TestRequestRoleCannotSeedProtectedRetryEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "request-role-canonical-runtime", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"establish an authenticated Job template"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit template Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var template jobResponse
	if err := json.Unmarshal(accepted.Body, &template); err != nil {
		t.Fatalf("decode template Job: %v", err)
	}

	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	requestTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request-role malicious seed transaction: %v", err)
	}
	defer func() { _ = requestTx.Rollback(context.Background()) }()
	if _, err := requestTx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		"jobs:submit",
	); err != nil {
		t.Fatalf("establish jobs:submit context: %v", err)
	}
	maliciousJobID := uuid.New()
	if err := cloneRequestRoleJob(
		context.Background(), requestTx, maliciousJobID, template.JobID,
	); err != nil {
		t.Fatalf("clone request-role Job for malicious runtime seed: %v", err)
	}
	_, err = requestTx.Exec(context.Background(), `
		INSERT INTO retry_runtime_states (
			job_id,
			organization_id,
			project_id,
			attempts_started,
			compute_seconds_consumed,
			finalization_seconds_consumed,
			finalization_retry_count,
			next_retry_at,
			excluded_workers,
			failure_fingerprints,
			circuit_breaker_state,
			last_failure_class,
			version
		) VALUES (
			$1, $2, $3, 1, 2, 3, 4,
			clock_timestamp() + interval '1 minute',
			'[{
				"worker_id":"00000000-0000-0000-0000-000000000999",
				"worker_epoch":99,
				"reason":"forged",
				"expires_at":"2099-01-01T00:00:00Z"
			}]'::jsonb,
			'["forged.fingerprint"]'::jsonb,
			'{"forged":true}'::jsonb,
			'FORGED_FAILURE',
			2
		)
	`, maliciousJobID, testOrganizationID, testProjectID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23514" ||
		postgresError.ConstraintName != "retry_runtime_states_canonical_initial_state" {
		t.Fatalf(
			"request-role malicious RetryRuntimeState error = %v, want SQLSTATE 23514 canonical constraint",
			err,
		)
	}
}

func cloneRequestRoleJob(
	ctx context.Context,
	tx pgx.Tx,
	newJobID uuid.UUID,
	templateJobID string,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id, state, version,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at, created_at, updated_at, current_fence, billable_started_at
		)
		SELECT
			$1, organization_id, project_id, created_by_principal_id, state, version,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at, created_at, updated_at, current_fence, billable_started_at
		FROM jobs AS source
		WHERE source.id = $2
	`, newJobID, templateJobID)
	return err
}

func cloneRequestRoleCreditReservation(
	ctx context.Context,
	tx pgx.Tx,
	newReservationID uuid.UUID,
	newJobID uuid.UUID,
	templateJobID string,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO credit_reservations (
			id, organization_id, project_id, job_id, amount_minor, currency,
			state, created_at, updated_at
		)
		SELECT
			$1, organization_id, project_id, $2, amount_minor, currency,
			state, created_at, updated_at
		FROM credit_reservations
		WHERE job_id = $3
	`, newReservationID, newJobID, templateJobID)
	return err
}

func TestRequestRoleCannotCommitNormalQueueOverflow(t *testing.T) {
	for _, test := range []struct {
		name       string
		statement  string
		argument   string
		constraint string
	}{
		{
			name: "Project",
			statement: `
				UPDATE projects
				SET queued_count = queued_limit + retry_wait_count + 1
				WHERE id = $1
			`,
			argument:   testProjectID,
			constraint: "projects_normal_queue_bounded",
		},
		{
			name: "Worker pool",
			statement: `
				UPDATE worker_pools
				SET queued_count = queued_limit + retry_wait_count + 1
				WHERE stable_id = $1
			`,
			argument:   "h3-primary",
			constraint: "worker_pools_normal_queue_bounded",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			seedAdmissionFixture(t, database.Admin)
			requestPool := newRolePool(
				t, database.DSN, "vela_request_login", "vela-request-password",
			)
			requestTx, err := requestPool.Begin(context.Background())
			if err != nil {
				t.Fatalf("begin request-role queue overflow transaction: %v", err)
			}
			defer func() { _ = requestTx.Rollback(context.Background()) }()
			if _, err := requestTx.Exec(
				context.Background(),
				"SELECT * FROM vela_set_request_context($1, $2, $3)",
				testCredentialID,
				credentialDigest([]byte(testCredentialSecret)),
				"jobs:submit",
			); err != nil {
				t.Fatalf("establish jobs:submit context: %v", err)
			}
			if _, err := requestTx.Exec(context.Background(), test.statement, test.argument); err != nil {
				t.Fatalf("create transaction-local normal queue overflow: %v", err)
			}
			err = requestTx.Commit(context.Background())
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "23514" ||
				postgresError.ConstraintName != test.constraint {
				t.Fatalf(
					"normal queue overflow commit error = %v, want SQLSTATE 23514 constraint %s",
					err,
					test.constraint,
				)
			}
		})
	}
}

func TestFailRollsBackEveryTransactionalWriteBoundary(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		action   string
		terminal bool
	}{
		{name: "Attempt", table: "attempts", action: "UPDATE"},
		{name: "Lease", table: "attempt_leases", action: "UPDATE"},
		{name: "Worker", table: "workers", action: "UPDATE"},
		{name: "RetryRuntimeState", table: "retry_runtime_states", action: "UPDATE"},
		{name: "protected retry evidence", table: "execution_retry_evidence", action: "UPDATE"},
		{name: "Project counter", table: "projects", action: "UPDATE"},
		{name: "pool counter", table: "worker_pools", action: "UPDATE"},
		{name: "Job", table: "jobs", action: "UPDATE"},
		{name: "decision receipt", table: "execution_failure_decisions", action: "INSERT"},
		{name: "Outbox", table: "outbox_events", action: "INSERT"},
		{name: "CreditReservation", table: "credit_reservations", action: "UPDATE", terminal: true},
		{name: "Organization credit", table: "organization_credit_accounts", action: "UPDATE", terminal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, "failure-rollback-"+test.table, 7)
			assignment, err := fixture.service.Acquire(
				context.Background(), fixture.worker, 7, &fixture.candidate,
			)
			if err != nil {
				t.Fatalf("create Assignment: %v", err)
			}
			before := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
			if _, err := fixture.database.Admin.Exec(fmt.Sprintf(`
				CREATE FUNCTION vela_test_reject_failure_write() RETURNS trigger
				LANGUAGE plpgsql AS $$
				BEGIN
					RAISE EXCEPTION 'injected failure write rejection';
				END
				$$;
				CREATE TRIGGER vela_test_reject_failure_write
				BEFORE %s ON %s
				FOR EACH ROW EXECUTE FUNCTION vela_test_reject_failure_write();
			`, test.action, test.table)); err != nil {
				t.Fatalf("install %s failure trigger: %v", test.name, err)
			}
			observation := validFailureObservation()
			if test.terminal {
				observation.FailureClass = "FATAL_BACKEND"
				observation.FailureFingerprint = "failure.rollback.terminal"
			}
			decision, failErr := fixture.service.Fail(
				context.Background(), fixture.worker, leaseCredentials(assignment), observation,
			)
			if failErr == nil || decision.Disposition != "" {
				t.Fatalf("Fail with rejected %s write = %#v error=%v", test.name, decision, failErr)
			}
			after := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected %s write committed partial state: before=%#v after=%#v", test.name, before, after)
			}
		})
	}
}

func TestFailRejectsBigintOverflowWithoutMutation(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		prepare   func(*testing.T, assignmentFixture) workercontrol.Assignment
		errorText string
	}{
		{
			name: "compute accounting",
			key:  "failure-overflow-compute",
			prepare: func(t *testing.T, fixture assignmentFixture) workercontrol.Assignment {
				assignment, err := fixture.service.Acquire(
					context.Background(), fixture.worker, 7, &fixture.candidate,
				)
				if err != nil {
					t.Fatalf("create Assignment: %v", err)
				}
				if result, startErr := fixture.service.Start(
					context.Background(), fixture.worker, leaseCredentials(assignment),
				); startErr != nil || result.Decision != workercontrol.StartGranted {
					t.Fatalf("Start = %#v error=%v", result, startErr)
				}
				forceJobPolicySnapshot(
					t,
					fixture.database.Admin,
					assignment.JobID,
					fmt.Sprintf("execution_max_total_compute_seconds = %d", int64(math.MaxInt64)),
				)
				if result, updateErr := fixture.database.Admin.Exec(`
					UPDATE retry_runtime_states
					SET compute_seconds_consumed = $2
					WHERE job_id = $1
				`, assignment.JobID, int64(math.MaxInt64)); updateErr != nil {
					t.Fatalf("set compute overflow boundary: %v", updateErr)
				} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
					t.Fatalf("compute overflow fixture rows = %d error=%v", rows, rowsErr)
				}
				return assignment
			},
			errorText: "compute accounting overflows seconds",
		},
		{
			name: "Job fence",
			key:  "failure-overflow-fence",
			prepare: func(t *testing.T, fixture assignmentFixture) workercontrol.Assignment {
				forceJobPolicySnapshot(
					t,
					fixture.database.Admin,
					fixture.candidate.JobID,
					fmt.Sprintf("current_fence = %d", int64(math.MaxInt64-1)),
				)
				assignment, err := fixture.service.Acquire(
					context.Background(), fixture.worker, 7, &fixture.candidate,
				)
				if err != nil {
					t.Fatalf("create max-fence Assignment: %v", err)
				}
				if assignment.LeaseFence != math.MaxInt64 {
					t.Fatalf("max-fence Assignment = %#v", assignment)
				}
				return assignment
			},
			errorText: "job fence overflows bigint",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, test.key, 7)
			assignment := test.prepare(t, fixture)
			before := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
			decision, err := fixture.service.Fail(
				context.Background(), fixture.worker, leaseCredentials(assignment), validFailureObservation(),
			)
			if err == nil || !strings.Contains(err.Error(), test.errorText) || decision.Disposition != "" {
				t.Fatalf("Fail at bigint boundary = %#v error=%v", decision, err)
			}
			after := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("bigint overflow committed state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func forceRunningLeaseComputeWindow(t *testing.T, db *sql.DB, attemptID uuid.UUID) {
	t.Helper()
	var anchor time.Time
	if err := db.QueryRow("SELECT clock_timestamp()").Scan(&anchor); err != nil {
		t.Fatalf("read compute-window PostgreSQL anchor: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin compute-window fault fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable immutable execution triggers for compute-window fixture: %v", err)
	}
	if result, err := tx.Exec(`
		UPDATE attempts
		SET assigned_at = $2, started_at = $3
		WHERE id = $1
	`, attemptID, anchor.Add(-10*time.Second), anchor.Add(-4500*time.Millisecond)); err != nil {
		t.Fatalf("set Attempt compute window: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("Attempt compute-window rows = %d error=%v", rows, rowsErr)
	}
	if result, err := tx.Exec(`
		UPDATE attempt_leases
		SET issued_at = $2, expires_at = $3
		WHERE attempt_id = $1
	`, attemptID, anchor.Add(-9*time.Second), anchor.Add(-3250*time.Millisecond)); err != nil {
		t.Fatalf("set Lease compute cap: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("Lease compute-cap rows = %d error=%v", rows, rowsErr)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit compute-window fault fixture: %v", err)
	}
}

func newFailureReconciler(
	t *testing.T,
	database testDatabase,
	workerLostGrace time.Duration,
) *workercontrol.Service {
	t.Helper()
	pool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := workercontrol.NewService(context.Background(), pool, workercontrol.Config{
		LeaseTTL:         2 * time.Minute,
		WorkerLostGrace:  workerLostGrace,
		ActiveLeaseKeyID: "lease-key-v1",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
		},
	})
	if err != nil {
		t.Fatalf("create failure reconciler: %v", err)
	}
	return service
}

func forceLeaseExpiry(t *testing.T, db *sql.DB, attemptID uuid.UUID, age time.Duration) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin Lease expiry fault fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable immutable Lease triggers for fault fixture: %v", err)
	}
	if result, err := tx.Exec(`
		UPDATE attempt_leases
			SET issued_at = clock_timestamp()
					- ($2::bigint * interval '1 microsecond')
					- interval '10 seconds',
				expires_at = clock_timestamp() - $2::bigint * interval '1 microsecond'
			WHERE attempt_id = $1
			  AND revoked_at IS NULL
		`, attemptID, age.Microseconds()); err != nil {
		t.Fatalf("set Lease expiry fault fixture: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("Lease expiry fault fixture rows = %d error=%v", rows, rowsErr)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Lease expiry fault fixture: %v", err)
	}
}

type jobExpiryState struct {
	JobState                  string
	JobFence                  int64
	JobVersion                int64
	ProjectQueued             int
	ProjectRetryWait          int
	ProjectRunning            int
	PoolQueued                int
	PoolRetryWait             int
	CreditReservationState    string
	OrganizationReservedMinor int64
	DecisionCount             int
	FailedOutboxCount         int
}

func readJobExpiryState(t *testing.T, db *sql.DB, jobID uuid.UUID) jobExpiryState {
	t.Helper()
	var state jobExpiryState
	if err := db.QueryRow(`
		SELECT
			j.state,
			j.current_fence,
			j.version,
			p.queued_count,
			p.retry_wait_count,
			p.running_count,
			wp.queued_count,
			wp.retry_wait_count,
			cr.state,
			oca.reserved_minor,
			(SELECT count(*) FROM execution_failure_decisions AS d
			 WHERE d.job_id = j.id AND d.attempt_id IS NULL),
			(SELECT count(*) FROM outbox_events AS oe
			 WHERE oe.aggregate_id = j.id AND oe.event_type = 'job.failed')
		FROM jobs AS j
		JOIN projects AS p ON p.id = j.project_id
		JOIN worker_pools AS wp ON wp.id = j.worker_pool_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, jobID).Scan(
		&state.JobState,
		&state.JobFence,
		&state.JobVersion,
		&state.ProjectQueued,
		&state.ProjectRetryWait,
		&state.ProjectRunning,
		&state.PoolQueued,
		&state.PoolRetryWait,
		&state.CreditReservationState,
		&state.OrganizationReservedMinor,
		&state.DecisionCount,
		&state.FailedOutboxCount,
	); err != nil {
		t.Fatalf("read Job Expiry state: %v", err)
	}
	return state
}

func forceJobPolicySnapshot(t *testing.T, db *sql.DB, jobID uuid.UUID, assignment string) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin Job policy fault fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable immutable snapshot trigger for fault fixture: %v", err)
	}
	query := "UPDATE jobs SET " + assignment + " WHERE id = $1"
	if result, err := tx.Exec(query, jobID); err != nil {
		t.Fatalf("set Job policy fault fixture: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("Job policy fault fixture rows = %d error=%v", rows, rowsErr)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Job policy fault fixture: %v", err)
	}
}

func validFailureObservation() workercontrol.FailureObservation {
	return workercontrol.FailureObservation{
		FailureClass:             "TRANSIENT_BACKEND",
		FailureFingerprint:       "sglang.process.exit.137",
		ErrorSummary:             "backend process exited before completing the Attempt",
		BackendStage:             "generation",
		GPUUUIDs:                 []string{"GPU-00000000-0000-0000-0000-000000000001"},
		InferenceBackendRevision: "sglang-vela@abc123",
		RetryRecommended:         true,
		WorkerReusable:           true,
	}
}

type failureState struct {
	AttemptState              string
	AttemptEndedAt            sql.NullTime
	LeaseRevokedAt            sql.NullTime
	JobState                  string
	JobFence                  int64
	JobVersion                int64
	AttemptsStarted           int
	ComputeSecondsConsumed    int64
	NextRetryAt               sql.NullTime
	ProjectQueued             int
	ProjectRetryWait          int
	ProjectRunning            int
	PoolQueued                int
	PoolRetryWait             int
	CreditReservationState    string
	OrganizationReservedMinor int64
	WorkerLifecycle           string
	WorkerReachability        string
	DecisionCount             int
	OutboxCount               int
	FailedOutboxCount         int
}

func readFailureState(t *testing.T, db *sql.DB, attemptID uuid.UUID) failureState {
	t.Helper()
	var state failureState
	err := db.QueryRow(`
		SELECT
			a.state,
			a.ended_at,
			l.revoked_at,
			j.state,
			j.current_fence,
			j.version,
			rts.attempts_started,
			rts.compute_seconds_consumed,
			rts.next_retry_at,
			p.queued_count,
			p.retry_wait_count,
			p.running_count,
			wp.queued_count,
			wp.retry_wait_count,
			cr.state,
			oca.reserved_minor,
			w.lifecycle_state,
			w.reachability_condition,
			(SELECT count(*) FROM execution_failure_decisions AS d WHERE d.attempt_id = a.id),
			(SELECT count(*) FROM outbox_events AS oe
			 WHERE oe.aggregate_id = j.id AND oe.event_type = 'job.retry_wait'),
			(SELECT count(*) FROM outbox_events AS oe
			 WHERE oe.aggregate_id = j.id AND oe.event_type = 'job.failed')
		FROM attempts AS a
		JOIN attempt_leases AS l ON l.attempt_id = a.id
		JOIN jobs AS j ON j.id = a.job_id
		JOIN retry_runtime_states AS rts ON rts.job_id = j.id
		JOIN projects AS p ON p.id = j.project_id
		JOIN worker_pools AS wp ON wp.id = j.worker_pool_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		JOIN workers AS w ON w.id = a.worker_id
		WHERE a.id = $1
	`, attemptID).Scan(
		&state.AttemptState,
		&state.AttemptEndedAt,
		&state.LeaseRevokedAt,
		&state.JobState,
		&state.JobFence,
		&state.JobVersion,
		&state.AttemptsStarted,
		&state.ComputeSecondsConsumed,
		&state.NextRetryAt,
		&state.ProjectQueued,
		&state.ProjectRetryWait,
		&state.ProjectRunning,
		&state.PoolQueued,
		&state.PoolRetryWait,
		&state.CreditReservationState,
		&state.OrganizationReservedMinor,
		&state.WorkerLifecycle,
		&state.WorkerReachability,
		&state.DecisionCount,
		&state.OutboxCount,
		&state.FailedOutboxCount,
	)
	if err != nil {
		t.Fatalf("read failure state: %v", err)
	}
	return state
}

func canonicalFailureObservation(t *testing.T, observation workercontrol.FailureObservation) []byte {
	t.Helper()
	payload, err := json.Marshal(observation)
	if err != nil {
		t.Fatalf("marshal FailureObservation: %v", err)
	}
	return payload
}
