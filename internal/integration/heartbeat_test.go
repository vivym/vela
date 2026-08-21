//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestHeartbeatRenewsAssignmentAndPersistsPreparingProgress(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-preparing", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	progress := 0.25
	estimatedRemaining := int64(90)
	observation := workercontrol.HeartbeatObservation{
		Sequence:                  1,
		BackendStage:              "model-preparation",
		BackendStageProgress:      &progress,
		EstimatedRemainingSeconds: &estimatedRemaining,
		GPUHealthSummary:          json.RawMessage(`{"healthy":true,"device_count":8}`),
		LocalArtifactState:        json.RawMessage(`{"state":"empty"}`),
		ScratchFreeBytes:          1 << 40,
		ArtifactStoreReachable:    true,
	}

	result, err := fixture.service.Heartbeat(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		observation,
	)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if result.Decision != workercontrol.HeartbeatContinue ||
		result.AttemptID != assignment.AttemptID ||
		result.JobID != assignment.JobID ||
		result.WorkerID != assignment.WorkerID ||
		result.WorkerEpoch != assignment.WorkerEpoch ||
		result.LeaseFence != assignment.LeaseFence ||
		result.HeartbeatSequence != observation.Sequence ||
		result.ExecutionPhase != workercontrol.ExecutionPhasePreparing {
		t.Fatalf("Heartbeat result = %#v", result)
	}
	if !result.LeaseExpiresAt.After(assignment.LeaseExpiresAt) {
		t.Fatalf("renewed Lease expiry = %s, want after %s", result.LeaseExpiresAt, assignment.LeaseExpiresAt)
	}
	if result.LeaseValidFor <= 0 || result.LeaseValidFor > 2*time.Minute {
		t.Fatalf("Heartbeat lease_valid_for = %s, want (0, 2m]", result.LeaseValidFor)
	}

	var (
		leaseExpiresAt, tokenClaimExpiresAt   time.Time
		sequence                              int64
		backendStage, executionPhase          string
		storedProgress                        sql.NullFloat64
		storedEstimatedRemaining              sql.NullInt64
		storedGPUHealth, storedArtifactState  []byte
		scratchFreeBytes                      int64
		artifactStoreReachable                bool
		progressUpdatedAt, progressValidUntil time.Time
		workerHeartbeatAt                     sql.NullTime
		jobVersion, outboxEvents              int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			l.expires_at,
			l.token_claim_expires_at,
			p.heartbeat_sequence,
			p.backend_stage,
			p.execution_phase,
			p.phase_progress,
			p.estimated_remaining_seconds,
			p.gpu_health_summary,
			p.local_artifact_state,
			p.scratch_free_bytes,
			p.artifact_store_reachable,
			p.progress_updated_at,
			p.progress_valid_until,
			w.last_heartbeat_at,
			j.version,
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = j.id)
		FROM attempt_leases AS l
		JOIN attempt_progress AS p ON p.attempt_id = l.attempt_id
		JOIN attempts AS a ON a.id = l.attempt_id
		JOIN jobs AS j ON j.id = a.job_id
		JOIN workers AS w ON w.id = l.worker_id
		WHERE l.attempt_id = $1
	`, assignment.AttemptID).Scan(
		&leaseExpiresAt,
		&tokenClaimExpiresAt,
		&sequence,
		&backendStage,
		&executionPhase,
		&storedProgress,
		&storedEstimatedRemaining,
		&storedGPUHealth,
		&storedArtifactState,
		&scratchFreeBytes,
		&artifactStoreReachable,
		&progressUpdatedAt,
		&progressValidUntil,
		&workerHeartbeatAt,
		&jobVersion,
		&outboxEvents,
	); err != nil {
		t.Fatalf("read Heartbeat state: %v", err)
	}
	if !leaseExpiresAt.Equal(result.LeaseExpiresAt) ||
		!tokenClaimExpiresAt.Equal(assignment.LeaseExpiresAt) ||
		sequence != 1 || backendStage != observation.BackendStage ||
		executionPhase != string(workercontrol.ExecutionPhasePreparing) ||
		!storedProgress.Valid || storedProgress.Float64 != progress ||
		!storedEstimatedRemaining.Valid || storedEstimatedRemaining.Int64 != estimatedRemaining ||
		scratchFreeBytes != observation.ScratchFreeBytes ||
		!artifactStoreReachable || !progressUpdatedAt.Equal(result.ProgressUpdatedAt) ||
		!progressValidUntil.Equal(result.LeaseExpiresAt) ||
		!workerHeartbeatAt.Valid || !workerHeartbeatAt.Time.Equal(result.ProgressUpdatedAt) ||
		jobVersion != 2 || outboxEvents != 2 {
		t.Fatalf("persisted Heartbeat state is inconsistent with result: %#v", result)
	}
	var gpuHealth, artifactState map[string]any
	if err := json.Unmarshal(storedGPUHealth, &gpuHealth); err != nil || gpuHealth["healthy"] != true {
		t.Fatalf("stored GPU health = %s error=%v", storedGPUHealth, err)
	}
	if err := json.Unmarshal(storedArtifactState, &artifactState); err != nil || artifactState["state"] != "empty" {
		t.Fatalf("stored local Artifact state = %s error=%v", storedArtifactState, err)
	}
}

func TestLeaseRenewalIsStageGatedDuringNMinusOneCoexistence(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-n-minus-one-stage-gate", 7)
	setLeaseRenewalProtocolGate(t, fixture.database.Admin, false, "exercise N-1 coexistence")
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	before := readHeartbeatMutationState(t, fixture.database.Admin, assignment)

	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	compatibilityService, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
		LeaseTTL:         2 * time.Minute,
		ActiveLeaseKeyID: "lease-key-v1",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
		},
	})
	if err != nil {
		t.Fatalf("create compatibility-mode Worker coordinator: %v", err)
	}
	result, err := compatibilityService.Heartbeat(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		validHeartbeatObservation(1),
	)
	if err != nil {
		t.Fatalf("stage-gated Heartbeat: %v", err)
	}
	if result.Decision != workercontrol.HeartbeatStop ||
		result.StopReason != workercontrol.StopProtocolMigration {
		t.Fatalf("stage-gated Heartbeat result = %#v", result)
	}
	after := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stage-gated Heartbeat mutated state: before=%#v after=%#v", before, after)
	}

	replayed, err := compatibilityService.Acquire(context.Background(), fixture.worker, 7, nil)
	if err != nil {
		t.Fatalf("replay Assignment in compatibility mode: %v", err)
	}
	if replayed.LeaseToken != assignment.LeaseToken ||
		!replayed.LeaseExpiresAt.Equal(assignment.LeaseExpiresAt) {
		t.Fatalf("compatibility-mode replay = %#v, want token and expiry from %#v", replayed, assignment)
	}
}

func TestLeaseRenewalProtocolGateEnforcesSwitchAndRollbackBoundaries(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-protocol-gate-boundaries", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}

	_, err = fixture.database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(false, 'unsafe rollback attempt')",
	)
	requirePostgresOperationalConstraint(t, err, "execution_lease_renewal_protocol_active_leases")

	_, err = fixture.database.Admin.Exec(`
		INSERT INTO attempt_leases (
			id, organization_id, project_id, attempt_id, worker_id, worker_epoch,
			phase, owner_kind, owner_id, fence, token_digest, signing_key_id,
			issued_at, expires_at
		) VALUES (
			$1, $2, $3, $4, $5, 7,
			'EXECUTION', 'WORKER', 'spiffe://vela.internal/worker/legacy', 1,
			decode(repeat('00', 32), 'hex'), 'lease-key-v1',
			clock_timestamp(), clock_timestamp() + interval '2 minutes'
		)
	`, uuid.New(), testOrganizationID, testProjectID, uuid.New(), uuid.New())
	requirePostgresOperationalConstraint(t, err, "execution_lease_renewal_protocol_legacy_writer")

	if _, err := fixture.database.Admin.Exec(`
		UPDATE attempt_leases SET revoked_at = clock_timestamp() WHERE attempt_id = $1
	`, assignment.AttemptID); err != nil {
		t.Fatalf("revoke active Lease before protocol rollback: %v", err)
	}
	setLeaseRenewalProtocolGate(t, fixture.database.Admin, false, "integration rollback after drain")

	_, err = fixture.database.Admin.Exec(`
		UPDATE attempt_leases
		SET expires_at = expires_at + interval '1 second'
		WHERE attempt_id = $1
	`, assignment.AttemptID)
	requirePostgresOperationalConstraint(t, err, "execution_lease_renewal_protocol_disabled")
}

func TestHeartbeatAndProtocolTransitionShareLeaseGateLockOrder(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-protocol-lock-order", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, heartbeatErr := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
	); heartbeatErr != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("seed progress Heartbeat = %#v error=%v", result, heartbeatErr)
	}

	progressLock, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin progress lock: %v", err)
	}
	defer func() { _ = progressLock.Rollback() }()
	if _, err := progressLock.Exec(
		"SELECT 1 FROM attempt_progress WHERE attempt_id = $1 FOR UPDATE",
		assignment.AttemptID,
	); err != nil {
		t.Fatalf("lock Attempt progress: %v", err)
	}

	heartbeatContext, cancelHeartbeat := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHeartbeat()
	heartbeatResults := make(chan workercontrol.HeartbeatResult, 1)
	heartbeatErrors := make(chan error, 1)
	go func() {
		result, heartbeatErr := fixture.service.Heartbeat(
			heartbeatContext,
			fixture.worker,
			leaseCredentials(assignment),
			validHeartbeatObservation(2),
		)
		heartbeatResults <- result
		heartbeatErrors <- heartbeatErr
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")

	transitionContext, cancelTransition := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelTransition()
	transitionErrors := make(chan error, 1)
	go func() {
		_, transitionErr := fixture.database.Admin.ExecContext(
			transitionContext,
			"SELECT vela_transition_execution_lease_renewal_protocol(false, 'concurrent lock-order test')",
		)
		transitionErrors <- transitionErr
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "postgres")
	if err := progressLock.Commit(); err != nil {
		t.Fatalf("release Attempt progress lock: %v", err)
	}

	result := <-heartbeatResults
	if heartbeatErr := <-heartbeatErrors; heartbeatErr != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("concurrent Heartbeat = %#v error=%v", result, heartbeatErr)
	}
	requirePostgresOperationalConstraint(
		t,
		<-transitionErrors,
		"execution_lease_renewal_protocol_active_leases",
	)
}

func TestHeartbeatReplayAndAssignmentReplayPreserveRenewedAuthority(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-replay", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	progress := 0.4
	observation := workercontrol.HeartbeatObservation{
		Sequence:               4,
		BackendStage:           "preparing-runtime",
		BackendStageProgress:   &progress,
		GPUHealthSummary:       json.RawMessage(`{"healthy":true}`),
		LocalArtifactState:     json.RawMessage(`{"state":"empty"}`),
		ScratchFreeBytes:       1 << 39,
		ArtifactStoreReachable: true,
	}
	first, err := fixture.service.Heartbeat(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		observation,
	)
	if err != nil || first.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("first Heartbeat = %#v error=%v", first, err)
	}

	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	restarted, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("restart Worker coordinator: %v", err)
	}
	replayed, err := restarted.Heartbeat(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		observation,
	)
	if err != nil {
		t.Fatalf("replay Heartbeat after restart: %v", err)
	}
	if !sameHeartbeatDurableResult(replayed, first) {
		t.Fatalf("Heartbeat replay = %#v, want durable fields %#v", replayed, first)
	}
	if replayed.LeaseValidFor <= 0 || replayed.LeaseValidFor > first.LeaseValidFor {
		t.Fatalf("replayed lease_valid_for = %s, want (0, %s]", replayed.LeaseValidFor, first.LeaseValidFor)
	}

	replayedAssignment, err := restarted.Acquire(context.Background(), fixture.worker, 7, nil)
	if err != nil {
		t.Fatalf("replay Assignment after Lease renewal: %v", err)
	}
	if replayedAssignment.LeaseToken != assignment.LeaseToken ||
		replayedAssignment.LeaseExpiresAt != first.LeaseExpiresAt ||
		replayedAssignment.AttemptID != assignment.AttemptID ||
		replayedAssignment.LeaseFence != assignment.LeaseFence {
		t.Fatalf("Assignment replay after renewal = %#v, original=%#v Heartbeat=%#v", replayedAssignment, assignment, first)
	}

	var (
		storedExpiry, storedProgressTime, workerHeartbeatAt time.Time
		sequence                                            int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT l.expires_at, p.heartbeat_sequence, p.progress_updated_at, w.last_heartbeat_at
		FROM attempt_leases AS l
		JOIN attempt_progress AS p ON p.attempt_id = l.attempt_id
		JOIN workers AS w ON w.id = l.worker_id
		WHERE l.attempt_id = $1
	`, assignment.AttemptID).Scan(
		&storedExpiry,
		&sequence,
		&storedProgressTime,
		&workerHeartbeatAt,
	); err != nil {
		t.Fatalf("read replayed Heartbeat state: %v", err)
	}
	if !storedExpiry.Equal(first.LeaseExpiresAt) || sequence != observation.Sequence ||
		!storedProgressTime.Equal(first.ProgressUpdatedAt) || !workerHeartbeatAt.Equal(first.ProgressUpdatedAt) {
		t.Fatalf("Heartbeat replay mutated durable state")
	}
}

func TestConcurrentDuplicateHeartbeatCommitsOneRenewal(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-concurrent-replay", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	observation := workercontrol.HeartbeatObservation{
		Sequence:               8,
		BackendStage:           "warming-model",
		GPUHealthSummary:       json.RawMessage(`{"healthy":true}`),
		LocalArtifactState:     json.RawMessage(`{"state":"empty"}`),
		ScratchFreeBytes:       1 << 38,
		ArtifactStoreReachable: true,
	}

	start := make(chan struct{})
	results := make(chan workercontrol.HeartbeatResult, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, heartbeatErr := fixture.service.Heartbeat(
				context.Background(),
				fixture.worker,
				leaseCredentials(assignment),
				observation,
			)
			results <- result
			errorsFound <- heartbeatErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for heartbeatErr := range errorsFound {
		if heartbeatErr != nil {
			t.Fatalf("concurrent Heartbeat: %v", heartbeatErr)
		}
	}
	continued := make([]workercontrol.HeartbeatResult, 0, 2)
	for result := range results {
		continued = append(continued, result)
	}
	if len(continued) != 2 ||
		continued[0].Decision != workercontrol.HeartbeatContinue ||
		continued[1].Decision != workercontrol.HeartbeatContinue ||
		!sameHeartbeatDurableResult(continued[0], continued[1]) {
		t.Fatalf("concurrent Heartbeat results = %#v", continued)
	}

	var progressRows, sequence int64
	var leaseExpiry, progressUpdatedAt, workerHeartbeatAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM attempt_progress WHERE attempt_id = $1),
			p.heartbeat_sequence,
			l.expires_at,
			p.progress_updated_at,
			w.last_heartbeat_at
		FROM attempt_progress AS p
		JOIN attempt_leases AS l ON l.attempt_id = p.attempt_id
		JOIN workers AS w ON w.id = p.worker_id
		WHERE p.attempt_id = $1
	`, assignment.AttemptID).Scan(
		&progressRows,
		&sequence,
		&leaseExpiry,
		&progressUpdatedAt,
		&workerHeartbeatAt,
	); err != nil {
		t.Fatalf("read concurrent Heartbeat state: %v", err)
	}
	if progressRows != 1 || sequence != observation.Sequence ||
		!leaseExpiry.Equal(continued[0].LeaseExpiresAt) ||
		!progressUpdatedAt.Equal(continued[0].ProgressUpdatedAt) ||
		!workerHeartbeatAt.Equal(continued[0].ProgressUpdatedAt) {
		t.Fatalf("concurrent Heartbeat committed more than one mutation")
	}
}

func TestHeartbeatSequenceAndCanonicalReplayRules(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-sequence", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	firstObservation := workercontrol.HeartbeatObservation{
		Sequence:               5,
		BackendStage:           "preparing",
		GPUHealthSummary:       json.RawMessage(`{"device_count":8,"healthy":true}`),
		LocalArtifactState:     json.RawMessage(`{"bytes":0,"state":"empty"}`),
		ScratchFreeBytes:       1 << 37,
		ArtifactStoreReachable: true,
	}
	first, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), firstObservation,
	)
	if err != nil || first.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("first Heartbeat = %#v error=%v", first, err)
	}

	canonicalReplay := firstObservation
	canonicalReplay.GPUHealthSummary = json.RawMessage(`{ "healthy": true, "device_count": 8 }`)
	canonicalReplay.LocalArtifactState = json.RawMessage(`{"state":"empty","bytes":0}`)
	replayed, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), canonicalReplay,
	)
	if err != nil || !sameHeartbeatDurableResult(replayed, first) {
		t.Fatalf("canonical Heartbeat replay = %#v error=%v, want %#v", replayed, err, first)
	}

	before := readHeartbeatState(t, fixture.database.Admin, assignment.AttemptID)
	conflict := firstObservation
	conflict.BackendStage = "different-stage"
	for _, test := range []struct {
		name        string
		observation workercontrol.HeartbeatObservation
	}{
		{name: "same sequence with different content", observation: conflict},
		{name: "lower sequence", observation: func() workercontrol.HeartbeatObservation {
			lower := firstObservation
			lower.Sequence = 4
			return lower
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, heartbeatErr := fixture.service.Heartbeat(
				context.Background(), fixture.worker, leaseCredentials(assignment), test.observation,
			)
			if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatStop ||
				result.StopReason != workercontrol.StopStaleHeartbeat {
				t.Fatalf("stale Heartbeat = %#v error=%v", result, heartbeatErr)
			}
		})
	}
	afterRejected := readHeartbeatState(t, fixture.database.Admin, assignment.AttemptID)
	if !reflect.DeepEqual(afterRejected, before) {
		t.Fatalf("stale Heartbeat mutated state: before=%#v after=%#v", before, afterRejected)
	}

	higher := firstObservation
	higher.Sequence = 9
	higher.BackendStage = "generation-ready"
	updated, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), higher,
	)
	if err != nil || updated.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("higher-sequence Heartbeat = %#v error=%v", updated, err)
	}
	afterHigher := readHeartbeatState(t, fixture.database.Admin, assignment.AttemptID)
	if afterHigher.Sequence != higher.Sequence || afterHigher.BackendStage != higher.BackendStage ||
		!afterHigher.ProgressUpdatedAt.After(before.ProgressUpdatedAt) ||
		afterHigher.LeaseExpiresAt.Before(before.LeaseExpiresAt) {
		t.Fatalf("higher sequence did not replace progress: before=%#v after=%#v", before, afterHigher)
	}

	largeNumber := higher
	largeNumber.Sequence = 10
	largeNumber.GPUHealthSummary = json.RawMessage(`{"counter":9007199254740992}`)
	if result, heartbeatErr := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), largeNumber,
	); heartbeatErr != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("large-number Heartbeat = %#v error=%v", result, heartbeatErr)
	}
	precisionConflict := largeNumber
	precisionConflict.GPUHealthSummary = json.RawMessage(`{"counter":9007199254740993}`)
	result, heartbeatErr := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), precisionConflict,
	)
	if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatStop ||
		result.StopReason != workercontrol.StopStaleHeartbeat {
		t.Fatalf("precision-distinct Heartbeat = %#v error=%v", result, heartbeatErr)
	}
}

func TestHeartbeatRejectsInvalidAuthorityWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *assignmentFixture, *workercontrol.AuthenticatedWorker, *workercontrol.LeaseCredentials, workercontrol.Assignment)
	}{
		{
			name: "different authenticated Worker",
			mutate: func(t *testing.T, fixture *assignmentFixture, worker *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials, _ workercontrol.Assignment) {
				t.Helper()
				worker.ID = uuid.New()
				if _, err := fixture.database.Admin.Exec(`
					INSERT INTO workers (
						id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
					) VALUES ($1, '00000000-0000-0000-0000-000000000005', $2, 7, 'READY', 'HEALTHY')
				`, worker.ID, "spiffe://vela.internal/worker/"+worker.ID.String()); err != nil {
					t.Fatalf("seed different Worker: %v", err)
				}
			},
		},
		{
			name: "stale Worker epoch",
			mutate: func(_ *testing.T, _ *assignmentFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials, _ workercontrol.Assignment) {
				credentials.WorkerEpoch++
			},
		},
		{
			name: "different Attempt",
			mutate: func(_ *testing.T, _ *assignmentFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials, _ workercontrol.Assignment) {
				credentials.AttemptID = uuid.New()
			},
		},
		{
			name: "stale fence",
			mutate: func(_ *testing.T, _ *assignmentFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials, _ workercontrol.Assignment) {
				credentials.Fence++
			},
		},
		{
			name: "incorrect opaque token",
			mutate: func(_ *testing.T, _ *assignmentFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials, _ workercontrol.Assignment) {
				credentials.Token = "not-the-issued-lease-token"
			},
		},
		{
			name: "revoked Lease",
			mutate: func(t *testing.T, fixture *assignmentFixture, _ *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE attempt_leases SET revoked_at = clock_timestamp() WHERE attempt_id = $1
				`, assignment.AttemptID); err != nil {
					t.Fatalf("revoke Lease: %v", err)
				}
			},
		},
		{
			name: "FINALIZATION Lease",
			mutate: func(t *testing.T, fixture *assignmentFixture, _ *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE attempt_leases SET phase = 'FINALIZATION' WHERE attempt_id = $1
				`, assignment.AttemptID); err != nil {
					t.Fatalf("change Lease phase: %v", err)
				}
			},
		},
		{
			name: "Reconciler-owned Lease",
			mutate: func(t *testing.T, fixture *assignmentFixture, _ *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials, assignment workercontrol.Assignment) {
				t.Helper()
				tx, err := fixture.database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin Reconciler Lease fixture: %v", err)
				}
				defer func() { _ = tx.Rollback() }()
				if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
					t.Fatalf("disable Lease identity trigger: %v", err)
				}
				if _, err := tx.Exec(`
					UPDATE attempt_leases SET owner_kind = 'RECONCILER' WHERE attempt_id = $1
				`, assignment.AttemptID); err != nil {
					t.Fatalf("change Lease owner: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit Reconciler Lease fixture: %v", err)
				}
			},
		},
		{
			name: "stale Job fence",
			mutate: func(t *testing.T, fixture *assignmentFixture, _ *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE jobs SET current_fence = current_fence + 1 WHERE id = $1
				`, assignment.JobID); err != nil {
					t.Fatalf("advance Job fence: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, fmt.Sprintf("heartbeat-invalid-authority-%d", index), 7)
			assignment, err := fixture.service.Acquire(
				context.Background(), fixture.worker, 7, &fixture.candidate,
			)
			if err != nil {
				t.Fatalf("create Assignment: %v", err)
			}
			worker := fixture.worker
			credentials := leaseCredentials(assignment)
			test.mutate(t, &fixture, &worker, &credentials, assignment)
			before := readHeartbeatMutationState(t, fixture.database.Admin, assignment)

			result, heartbeatErr := fixture.service.Heartbeat(
				context.Background(), worker, credentials, validHeartbeatObservation(1),
			)
			if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatStop ||
				result.StopReason != workercontrol.StopInvalidAuthority {
				t.Fatalf("invalid-authority Heartbeat = %#v error=%v", result, heartbeatErr)
			}
			after := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("invalid Heartbeat mutated state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestHeartbeatValidatesAuthorityBeforeProgress(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-authority-before-progress", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	credentials := leaseCredentials(assignment)
	credentials.Token = "not-the-issued-lease-token"
	invalidObservation := workercontrol.HeartbeatObservation{}

	result, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, credentials, invalidObservation,
	)
	if err != nil || result.Decision != workercontrol.HeartbeatStop ||
		result.StopReason != workercontrol.StopInvalidAuthority {
		t.Fatalf("invalid authority and progress Heartbeat = %#v error=%v", result, err)
	}
}

func TestHeartbeatRejectsInvalidProgressWithoutSideEffects(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-invalid-progress", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	floatPointer := func(value float64) *float64 { return &value }
	intPointer := func(value int64) *int64 { return &value }
	oversizedObject := json.RawMessage(`{"value":"` + strings.Repeat("x", 17*1024) + `"}`)
	tests := []struct {
		name   string
		mutate func(*workercontrol.HeartbeatObservation)
	}{
		{name: "zero sequence", mutate: func(value *workercontrol.HeartbeatObservation) { value.Sequence = 0 }},
		{name: "blank backend stage", mutate: func(value *workercontrol.HeartbeatObservation) { value.BackendStage = "  " }},
		{name: "control character", mutate: func(value *workercontrol.HeartbeatObservation) { value.BackendStage = "bad\nstage" }},
		{name: "non-printable Unicode", mutate: func(value *workercontrol.HeartbeatObservation) { value.BackendStage = "\u200b" }},
		{name: "backend stage too long", mutate: func(value *workercontrol.HeartbeatObservation) { value.BackendStage = strings.Repeat("x", 101) }},
		{name: "negative progress", mutate: func(value *workercontrol.HeartbeatObservation) { value.BackendStageProgress = floatPointer(-0.01) }},
		{name: "one hundred percent progress", mutate: func(value *workercontrol.HeartbeatObservation) { value.BackendStageProgress = floatPointer(1) }},
		{name: "NaN progress", mutate: func(value *workercontrol.HeartbeatObservation) { value.BackendStageProgress = floatPointer(math.NaN()) }},
		{name: "infinite progress", mutate: func(value *workercontrol.HeartbeatObservation) {
			value.BackendStageProgress = floatPointer(math.Inf(1))
		}},
		{name: "negative estimate", mutate: func(value *workercontrol.HeartbeatObservation) { value.EstimatedRemainingSeconds = intPointer(-1) }},
		{name: "overflowing estimate", mutate: func(value *workercontrol.HeartbeatObservation) {
			value.EstimatedRemainingSeconds = intPointer(math.MaxInt64)
		}},
		{name: "missing GPU health", mutate: func(value *workercontrol.HeartbeatObservation) { value.GPUHealthSummary = nil }},
		{name: "invalid GPU health JSON", mutate: func(value *workercontrol.HeartbeatObservation) { value.GPUHealthSummary = json.RawMessage(`{`) }},
		{name: "non-object GPU health", mutate: func(value *workercontrol.HeartbeatObservation) { value.GPUHealthSummary = json.RawMessage(`[]`) }},
		{name: "oversized GPU health", mutate: func(value *workercontrol.HeartbeatObservation) { value.GPUHealthSummary = oversizedObject }},
		{name: "non-object local Artifact state", mutate: func(value *workercontrol.HeartbeatObservation) { value.LocalArtifactState = json.RawMessage(`"empty"`) }},
		{name: "negative scratch bytes", mutate: func(value *workercontrol.HeartbeatObservation) { value.ScratchFreeBytes = -1 }},
	}
	before := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := validHeartbeatObservation(1)
			test.mutate(&observation)
			result, heartbeatErr := fixture.service.Heartbeat(
				context.Background(), fixture.worker, leaseCredentials(assignment), observation,
			)
			if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatStop ||
				result.StopReason != workercontrol.StopInvalidProgress {
				t.Fatalf("invalid-progress Heartbeat = %#v error=%v", result, heartbeatErr)
			}
		})
	}
	after := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("invalid progress mutated state: before=%#v after=%#v", before, after)
	}
}

func TestHeartbeatMapsCurrentPhaseAndContinuesDuringRoutineDrain(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-phase-and-drain", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	preparing, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
	)
	if err != nil || preparing.Decision != workercontrol.HeartbeatContinue ||
		preparing.ExecutionPhase != workercontrol.ExecutionPhasePreparing {
		t.Fatalf("preparing Heartbeat = %#v error=%v", preparing, err)
	}
	started, err := fixture.service.Start(context.Background(), fixture.worker, leaseCredentials(assignment))
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE workers SET lifecycle_state = 'DRAINING', updated_at = clock_timestamp() WHERE id = $1
	`, fixture.worker.ID); err != nil {
		t.Fatalf("drain Worker: %v", err)
	}
	generatingObservation := validHeartbeatObservation(2)
	generatingObservation.BackendStage = "dit-step"
	progress := 0.6
	generatingObservation.BackendStageProgress = &progress
	generating, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), generatingObservation,
	)
	if err != nil || generating.Decision != workercontrol.HeartbeatContinue ||
		generating.ExecutionPhase != workercontrol.ExecutionPhaseGenerating {
		t.Fatalf("generating Heartbeat during drain = %#v error=%v", generating, err)
	}
	state := readHeartbeatState(t, fixture.database.Admin, assignment.AttemptID)
	if state.Sequence != 2 || state.ExecutionPhase != string(workercontrol.ExecutionPhaseGenerating) ||
		state.JobVersion != 3 || state.OutboxEvents != 3 {
		t.Fatalf("Heartbeat phase state = %#v", state)
	}
}

func TestHeartbeatStopsWhenAssignmentIsNotHeartbeatable(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *assignmentFixture, workercontrol.Assignment)
	}{
		{
			name: "Job is terminal",
			mutate: func(t *testing.T, fixture *assignmentFixture, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE jobs SET state = 'CANCELED', version = version + 1, updated_at = clock_timestamp()
					WHERE id = $1
				`, assignment.JobID); err != nil {
					t.Fatalf("cancel Job fixture: %v", err)
				}
			},
		},
		{
			name: "Attempt is terminal",
			mutate: func(t *testing.T, fixture *assignmentFixture, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE attempts SET state = 'FAILED', ended_at = clock_timestamp(), updated_at = clock_timestamp()
					WHERE id = $1
				`, assignment.AttemptID); err != nil {
					t.Fatalf("fail Attempt fixture: %v", err)
				}
			},
		},
		{
			name: "Credit Reservation is released",
			mutate: func(t *testing.T, fixture *assignmentFixture, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE credit_reservations SET state = 'RELEASED', updated_at = clock_timestamp()
					WHERE job_id = $1
				`, assignment.JobID); err != nil {
					t.Fatalf("release Credit Reservation fixture: %v", err)
				}
			},
		},
		{
			name: "Job and Attempt are FINALIZING",
			mutate: func(t *testing.T, fixture *assignmentFixture, assignment workercontrol.Assignment) {
				t.Helper()
				started, err := fixture.service.Start(
					context.Background(), fixture.worker, leaseCredentials(assignment),
				)
				if err != nil || started.Decision != workercontrol.StartGranted {
					t.Fatalf("Start finalizing fixture = %#v error=%v", started, err)
				}
				tx, err := fixture.database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin finalizing fixture: %v", err)
				}
				defer func() { _ = tx.Rollback() }()
				if _, err := tx.Exec(`
					UPDATE attempts SET state = 'FINALIZING', updated_at = clock_timestamp() WHERE id = $1
				`, assignment.AttemptID); err != nil {
					t.Fatalf("finalize Attempt fixture: %v", err)
				}
				if _, err := tx.Exec(`
					UPDATE jobs SET state = 'FINALIZING', version = version + 1, updated_at = clock_timestamp() WHERE id = $1
				`, assignment.JobID); err != nil {
					t.Fatalf("finalize Job fixture: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit finalizing fixture: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, fmt.Sprintf("heartbeat-not-heartbeatable-%d", index), 7)
			assignment, err := fixture.service.Acquire(
				context.Background(), fixture.worker, 7, &fixture.candidate,
			)
			if err != nil {
				t.Fatalf("create Assignment: %v", err)
			}
			test.mutate(t, &fixture, assignment)
			before := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
			result, heartbeatErr := fixture.service.Heartbeat(
				context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
			)
			if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatStop ||
				result.StopReason != workercontrol.StopNotHeartbeatable {
				t.Fatalf("non-heartbeatable result = %#v error=%v", result, heartbeatErr)
			}
			after := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("non-heartbeatable Heartbeat mutated state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestHeartbeatRenewalIsCappedAndNeverShortensAuthority(t *testing.T) {
	t.Run("capped by Job Expiry", func(t *testing.T) {
		fixture := newAssignmentFixture(t, "heartbeat-job-expiry-cap", 7)
		assignment, err := fixture.service.Acquire(
			context.Background(), fixture.worker, 7, &fixture.candidate,
		)
		if err != nil {
			t.Fatalf("create Assignment: %v", err)
		}
		var jobExpiresAt time.Time
		if err := fixture.database.Admin.QueryRow(
			"SELECT job_expires_at FROM jobs WHERE id = $1", assignment.JobID,
		).Scan(&jobExpiresAt); err != nil {
			t.Fatalf("read Job Expiry: %v", err)
		}
		internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
		longTTLService, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
			LeaseTTL:         30 * 24 * time.Hour,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
		})
		if err != nil {
			t.Fatalf("create long-TTL coordinator: %v", err)
		}
		result, err := longTTLService.Heartbeat(
			context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
		)
		if err != nil || result.Decision != workercontrol.HeartbeatContinue {
			t.Fatalf("capped Heartbeat = %#v error=%v", result, err)
		}
		if !result.LeaseExpiresAt.Equal(jobExpiresAt) || result.LeaseValidFor <= 0 ||
			result.LeaseValidFor > jobExpiresAt.Sub(result.ProgressUpdatedAt) {
			t.Fatalf("capped Lease = expiry %s valid_for %s, Job Expiry %s", result.LeaseExpiresAt, result.LeaseValidFor, jobExpiresAt)
		}
	})

	t.Run("shorter adjacent TTL does not shorten", func(t *testing.T) {
		fixture := newAssignmentFixture(t, "heartbeat-nonshortening", 7)
		assignment, err := fixture.service.Acquire(
			context.Background(), fixture.worker, 7, &fixture.candidate,
		)
		if err != nil {
			t.Fatalf("create Assignment: %v", err)
		}
		internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
		shortTTLService, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
			LeaseTTL:         time.Second,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
		})
		if err != nil {
			t.Fatalf("create short-TTL coordinator: %v", err)
		}
		result, err := shortTTLService.Heartbeat(
			context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
		)
		if err != nil || result.Decision != workercontrol.HeartbeatContinue {
			t.Fatalf("non-shortening Heartbeat = %#v error=%v", result, err)
		}
		if !result.LeaseExpiresAt.Equal(assignment.LeaseExpiresAt) ||
			result.LeaseValidFor <= 0 || result.LeaseValidFor > assignment.LeaseValidFor {
			t.Fatalf("short-TTL renewal changed authority: Assignment=%#v Heartbeat=%#v", assignment, result)
		}
	})
}

func TestHeartbeatRechecksExpiryAfterRowLockWait(t *testing.T) {
	tests := []struct {
		name       string
		stopReason workercontrol.StopReason
		expire     func(*testing.T, *sql.Tx, workercontrol.Assignment)
	}{
		{
			name:       "Lease",
			stopReason: workercontrol.StopLeaseExpired,
			expire: func(t *testing.T, tx *sql.Tx, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := tx.Exec(`
					UPDATE attempt_leases
					SET expires_at = issued_at + interval '1 microsecond'
					WHERE attempt_id = $1
				`, assignment.AttemptID); err != nil {
					t.Fatalf("expire Lease while holding row lock: %v", err)
				}
			},
		},
		{
			name:       "Job",
			stopReason: workercontrol.StopJobExpired,
			expire: func(t *testing.T, tx *sql.Tx, assignment workercontrol.Assignment) {
				t.Helper()
				if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
					t.Fatalf("disable immutable Job Expiry trigger: %v", err)
				}
				if _, err := tx.Exec(`
					UPDATE jobs
					SET job_expires_at = created_at + interval '1 microsecond'
					WHERE id = $1
				`, assignment.JobID); err != nil {
					t.Fatalf("expire Job while holding row lock: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, fmt.Sprintf("heartbeat-lock-expiry-%d", index), 7)
			assignment, err := fixture.service.Acquire(
				context.Background(), fixture.worker, 7, &fixture.candidate,
			)
			if err != nil {
				t.Fatalf("create Assignment: %v", err)
			}
			lock, err := fixture.database.Admin.Begin()
			if err != nil {
				t.Fatalf("begin expiry lock transaction: %v", err)
			}
			defer func() { _ = lock.Rollback() }()
			test.expire(t, lock, assignment)

			resultChannel := make(chan workercontrol.HeartbeatResult, 1)
			errorChannel := make(chan error, 1)
			go func() {
				result, heartbeatErr := fixture.service.Heartbeat(
					context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
				)
				resultChannel <- result
				errorChannel <- heartbeatErr
			}()
			waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
			if err := lock.Commit(); err != nil {
				t.Fatalf("commit expiry while Heartbeat waits: %v", err)
			}
			result := <-resultChannel
			heartbeatErr := <-errorChannel
			if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatStop ||
				result.StopReason != test.stopReason {
				t.Fatalf("Heartbeat after %s expiry = %#v error=%v", test.name, result, heartbeatErr)
			}
			state := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
			if state.ProgressRows != 0 || state.WorkerHeartbeatAt.Valid ||
				state.JobVersion != 2 || state.OutboxEvents != 2 {
				t.Fatalf("expired Heartbeat mutated state: %#v", state)
			}
		})
	}
}

func TestHeartbeatExpiryDuringProgressWriteRollsBack(t *testing.T) {
	tests := []struct {
		name       string
		stopReason workercontrol.StopReason
		prepare    func(*testing.T, *assignmentFixture, workercontrol.Assignment)
	}{
		{name: "Lease", stopReason: workercontrol.StopLeaseExpired},
		{
			name:       "Job",
			stopReason: workercontrol.StopJobExpired,
			prepare: func(t *testing.T, fixture *assignmentFixture, assignment workercontrol.Assignment) {
				t.Helper()
				tx, err := fixture.database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin near Job Expiry fixture: %v", err)
				}
				defer func() { _ = tx.Rollback() }()
				if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
					t.Fatalf("disable immutable Job Expiry trigger: %v", err)
				}
				if _, err := tx.Exec(`
					UPDATE jobs
					SET job_expires_at = clock_timestamp() + interval '900 milliseconds'
					WHERE id = $1
				`, assignment.JobID); err != nil {
					t.Fatalf("set near Job Expiry: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit near Job Expiry fixture: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, fmt.Sprintf("heartbeat-write-expiry-%d", index), 7)
			if _, err := fixture.database.Admin.Exec(`
				CREATE FUNCTION vela_test_delay_heartbeat_progress() RETURNS trigger
				LANGUAGE plpgsql AS $$
				BEGIN
					PERFORM pg_sleep(1.1);
					RETURN NEW;
				END
				$$
			`); err != nil {
				t.Fatalf("create delayed progress function: %v", err)
			}
			if _, err := fixture.database.Admin.Exec(`
				CREATE TRIGGER delay_heartbeat_progress
				BEFORE INSERT OR UPDATE ON attempt_progress
				FOR EACH ROW EXECUTE FUNCTION vela_test_delay_heartbeat_progress()
			`); err != nil {
				t.Fatalf("create delayed progress trigger: %v", err)
			}
			internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
			shortTTLService, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
				LeaseTTL:         time.Second,
				ActiveLeaseKeyID: "lease-key-v1",
				LeaseKeys: map[string][]byte{
					"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
				},
			})
			if err != nil {
				t.Fatalf("create short-TTL coordinator: %v", err)
			}
			assignment, err := shortTTLService.Acquire(
				context.Background(), fixture.worker, 7, &fixture.candidate,
			)
			if err != nil {
				t.Fatalf("create short Assignment: %v", err)
			}
			if test.prepare != nil {
				test.prepare(t, &fixture, assignment)
			}
			before := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
			result, heartbeatErr := shortTTLService.Heartbeat(
				context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
			)
			if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatStop ||
				result.StopReason != test.stopReason {
				t.Fatalf("Heartbeat across delayed %s expiry = %#v error=%v", test.name, result, heartbeatErr)
			}
			after := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("expired delayed Heartbeat was partial: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestHeartbeatProgressWriteFailureRollsBack(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-progress-write-failure", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_reject_heartbeat_progress() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'injected progress write failure';
		END
		$$
	`); err != nil {
		t.Fatalf("create progress failure function: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		CREATE TRIGGER reject_heartbeat_progress
		BEFORE INSERT OR UPDATE ON attempt_progress
		FOR EACH ROW EXECUTE FUNCTION vela_test_reject_heartbeat_progress()
	`); err != nil {
		t.Fatalf("create progress failure trigger: %v", err)
	}
	before := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
	result, heartbeatErr := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
	)
	if heartbeatErr == nil || result.Decision != "" {
		t.Fatalf("Heartbeat with failed progress write = %#v error=%v", result, heartbeatErr)
	}
	after := readHeartbeatMutationState(t, fixture.database.Admin, assignment)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("failed progress transaction was partial: before=%#v after=%#v", before, after)
	}
}

func TestJobViewProjectsCurrentProgressAndMakesStaleValuesUnknown(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-job-view", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	started, err := fixture.service.Start(context.Background(), fixture.worker, leaseCredentials(assignment))
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	progress := 0.55
	estimatedRemaining := int64(120)
	observation := validHeartbeatObservation(1)
	observation.BackendStage = "internal-dit-stage"
	observation.BackendStageProgress = &progress
	observation.EstimatedRemainingSeconds = &estimatedRemaining
	observation.GPUHealthSummary = json.RawMessage(`{"gpu_uuids":["secret-device"],"healthy":true}`)
	observation.LocalArtifactState = json.RawMessage(`{"private_path":"/scratch/customer/job"}`)
	continued, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil || continued.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("Heartbeat = %#v error=%v", continued, err)
	}

	server := admissionServerForDatabase(t, fixture.database)
	view, raw := fetchJobView(t, server.URL, assignment.JobID)
	if view.State != "RUNNING" || view.Phase == nil || *view.Phase != "GENERATING" ||
		view.PhaseProgress == nil || *view.PhaseProgress != progress ||
		view.AttemptsStarted != 1 || view.NextRetryAt != nil ||
		view.EstimatedFinishAt == nil ||
		!view.EstimatedFinishAt.Equal(continued.ProgressUpdatedAt.Add(120*time.Second)) ||
		view.ProgressUpdatedAt == nil || !view.ProgressUpdatedAt.Equal(continued.ProgressUpdatedAt) {
		t.Fatalf("current Job progress view = %#v", view)
	}
	for _, forbidden := range []string{
		"backend_stage", "gpu_health_summary", "local_artifact_state",
		"worker_id", "worker_epoch", "lease_fence", "lease_token",
	} {
		if _, present := raw[forbidden]; present {
			t.Fatalf("Job view exposed internal field %q: %s", forbidden, raw[forbidden])
		}
	}

	staleFixture, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin stale progress fixture: %v", err)
	}
	defer func() { _ = staleFixture.Rollback() }()
	if _, err := staleFixture.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable progress sequence trigger: %v", err)
	}
	if _, err := staleFixture.Exec(`
		UPDATE attempt_progress
		SET progress_valid_until = progress_updated_at + interval '1 microsecond'
		WHERE attempt_id = $1
	`, assignment.AttemptID); err != nil {
		t.Fatalf("make progress stale: %v", err)
	}
	if err := staleFixture.Commit(); err != nil {
		t.Fatalf("commit stale progress fixture: %v", err)
	}
	waitForDatabaseTimeAfter(t, fixture.database.Admin, continued.ProgressUpdatedAt.Add(time.Microsecond))
	staleView, _ := fetchJobView(t, server.URL, assignment.JobID)
	if staleView.Phase == nil || *staleView.Phase != "GENERATING" ||
		staleView.PhaseProgress != nil || staleView.EstimatedFinishAt != nil ||
		staleView.ProgressUpdatedAt == nil || !staleView.ProgressUpdatedAt.Equal(continued.ProgressUpdatedAt) {
		t.Fatalf("stale Job progress view = %#v", staleView)
	}
}

func TestJobViewMapsLifecyclePhasesAndRetryProgressResets(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-job-view-retry", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	assignedView, _ := fetchJobView(t, server.URL, assignment.JobID)
	if assignedView.Phase == nil || *assignedView.Phase != "PREPARING" ||
		assignedView.PhaseProgress != nil || assignedView.AttemptsStarted != 1 {
		t.Fatalf("ASSIGNED Job view = %#v", assignedView)
	}
	preparingObservation := validHeartbeatObservation(1)
	preparingProgress := 0.7
	preparingObservation.BackendStageProgress = &preparingProgress
	if result, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), preparingObservation,
	); err != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("preparing Heartbeat = %#v error=%v", result, err)
	}
	if result, err := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); err != nil || result.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", result, err)
	}
	runningBeforeProgress, _ := fetchJobView(t, server.URL, assignment.JobID)
	if runningBeforeProgress.Phase == nil || *runningBeforeProgress.Phase != "GENERATING" ||
		runningBeforeProgress.PhaseProgress != nil || runningBeforeProgress.ProgressUpdatedAt != nil {
		t.Fatalf("RUNNING Job reused PREPARING progress: %#v", runningBeforeProgress)
	}
	generatingObservation := validHeartbeatObservation(2)
	generatingProgress := 0.2
	generatingObservation.BackendStageProgress = &generatingProgress
	if result, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), generatingObservation,
	); err != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("generating Heartbeat = %#v error=%v", result, err)
	}

	retryFixture, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Retry fixture: %v", err)
	}
	defer func() { _ = retryFixture.Rollback() }()
	retryStatements := []struct {
		query string
		args  []any
	}{
		{query: `UPDATE attempt_leases SET revoked_at = clock_timestamp(), updated_at = clock_timestamp() WHERE attempt_id = $1`, args: []any{assignment.AttemptID}},
		{query: `UPDATE attempts SET state = 'FAILED', ended_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1`, args: []any{assignment.AttemptID}},
		{query: `UPDATE jobs SET state = 'RETRY_WAIT', version = version + 1, updated_at = clock_timestamp() WHERE id = $1`, args: []any{assignment.JobID}},
		{query: `UPDATE workers SET lifecycle_state = 'READY', updated_at = clock_timestamp() WHERE id = $1`, args: []any{fixture.worker.ID}},
		{query: `UPDATE projects SET queued_count = queued_count + 1, retry_wait_count = retry_wait_count + 1, running_count = running_count - 1 WHERE id = $1`, args: []any{testProjectID}},
		{query: `UPDATE worker_pools SET queued_count = queued_count + 1, retry_wait_count = retry_wait_count + 1 WHERE id = '00000000-0000-0000-0000-000000000005'`},
		{query: `UPDATE retry_runtime_states SET next_retry_at = clock_timestamp() + interval '30 seconds', version = version + 1, updated_at = clock_timestamp() WHERE job_id = $1`, args: []any{assignment.JobID}},
	}
	for _, statement := range retryStatements {
		if _, err := retryFixture.Exec(statement.query, statement.args...); err != nil {
			t.Fatalf("stage Retry Wait: %v", err)
		}
	}
	if err := retryFixture.Commit(); err != nil {
		t.Fatalf("commit Retry Wait: %v", err)
	}
	retryView, _ := fetchJobView(t, server.URL, assignment.JobID)
	if retryView.Phase == nil || *retryView.Phase != "RETRY_WAIT" ||
		retryView.PhaseProgress != nil || retryView.EstimatedFinishAt != nil ||
		retryView.ProgressUpdatedAt != nil || retryView.NextRetryAt == nil ||
		retryView.AttemptsStarted != 1 {
		t.Fatalf("RETRY_WAIT Job view = %#v", retryView)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states SET next_retry_at = clock_timestamp() - interval '1 second' WHERE job_id = $1
	`, assignment.JobID); err != nil {
		t.Fatalf("make Retry due: %v", err)
	}
	retryCandidate := fixture.candidate
	retryCandidate.ExpectedJobVersion = 4
	secondAssignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &retryCandidate,
	)
	if err != nil {
		t.Fatalf("create replacement Assignment: %v", err)
	}
	secondView, _ := fetchJobView(t, server.URL, assignment.JobID)
	if secondAssignment.LeaseFence != 2 || secondView.Phase == nil ||
		*secondView.Phase != "PREPARING" || secondView.PhaseProgress != nil ||
		secondView.ProgressUpdatedAt != nil || secondView.NextRetryAt != nil ||
		secondView.AttemptsStarted != 2 {
		t.Fatalf("replacement Attempt Job view = %#v Assignment=%#v", secondView, secondAssignment)
	}
}

func TestRequestRoleCanReadOnlyNeutralProgressProjection(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-progress-rls", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(1),
	); err != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("Heartbeat = %#v error=%v", result, err)
	}

	requestPool := newRolePool(t, fixture.database.DSN, "vela_request_login", "vela-request-password")
	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request-role progress read: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		"jobs:read",
	); err != nil {
		t.Fatalf("establish jobs:read context: %v", err)
	}
	var progress sql.NullFloat64
	var progressUpdatedAt time.Time
	if err := tx.QueryRow(
		context.Background(),
		`SELECT phase_progress, progress_updated_at FROM vela_request_job_progress WHERE job_id = $1`,
		assignment.JobID,
	).Scan(&progress, &progressUpdatedAt); err != nil {
		t.Fatalf("read neutral progress projection: %v", err)
	}
	if progress.Valid || progressUpdatedAt.IsZero() {
		t.Fatalf("neutral progress projection = progress %#v updated_at %s", progress, progressUpdatedAt)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit neutral progress read: %v", err)
	}

	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "backend stage", query: "SELECT backend_stage FROM attempt_progress"},
		{name: "GPU health", query: "SELECT gpu_health_summary FROM attempt_progress"},
		{name: "local Artifact state", query: "SELECT local_artifact_state FROM attempt_progress"},
		{name: "Attempt fence", query: "SELECT fence FROM attempt_progress"},
		{name: "progress validity deadline", query: "SELECT progress_valid_until FROM attempt_progress"},
		{name: "Retry failure fingerprints", query: "SELECT failure_fingerprints FROM retry_runtime_states"},
		{name: "progress mutation", query: "UPDATE attempt_progress SET phase_progress = 0.9"},
	} {
		t.Run(test.name, func(t *testing.T) {
			restrictedTx, beginErr := requestPool.Begin(context.Background())
			if beginErr != nil {
				t.Fatalf("begin restricted progress query: %v", beginErr)
			}
			defer func() { _ = restrictedTx.Rollback(context.Background()) }()
			if _, contextErr := restrictedTx.Exec(
				context.Background(),
				"SELECT * FROM vela_set_request_context($1, $2, $3)",
				testCredentialID,
				credentialDigest([]byte(testCredentialSecret)),
				"jobs:read",
			); contextErr != nil {
				t.Fatalf("establish restricted jobs:read context: %v", contextErr)
			}
			_, queryErr := restrictedTx.Exec(context.Background(), test.query)
			var postgresError *pgconn.PgError
			if !errors.As(queryErr, &postgresError) || postgresError.Code != "42501" {
				t.Fatalf("restricted %s query error = %v, want SQLSTATE 42501", test.name, queryErr)
			}
		})
	}
}

func TestHeartbeatDatabaseInvariantsRejectDirectMutation(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-database-invariants", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	if result, err := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), validHeartbeatObservation(2),
	); err != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("Heartbeat = %#v error=%v", result, err)
	}
	before := readHeartbeatInvariantState(t, fixture.database.Admin, assignment.AttemptID)
	for _, test := range []struct {
		name  string
		query string
		args  []any
	}{
		{
			name:  "Lease token claim expiry",
			query: "UPDATE attempt_leases SET token_claim_expires_at = token_claim_expires_at + interval '1 second' WHERE attempt_id = $1",
			args:  []any{assignment.AttemptID},
		},
		{
			name:  "progress identity",
			query: "UPDATE attempt_progress SET job_id = $1, heartbeat_sequence = heartbeat_sequence + 1, progress_updated_at = progress_updated_at + interval '1 second', updated_at = updated_at + interval '1 second' WHERE attempt_id = $2",
			args:  []any{uuid.New(), assignment.AttemptID},
		},
		{
			name:  "progress sequence regression",
			query: "UPDATE attempt_progress SET heartbeat_sequence = heartbeat_sequence - 1, progress_updated_at = progress_updated_at + interval '1 second', updated_at = updated_at + interval '1 second' WHERE attempt_id = $1",
			args:  []any{assignment.AttemptID},
		},
		{
			name:  "progress time regression",
			query: "UPDATE attempt_progress SET heartbeat_sequence = heartbeat_sequence + 1 WHERE attempt_id = $1",
			args:  []any{assignment.AttemptID},
		},
		{
			name:  "Visible Completion percentage",
			query: "UPDATE attempt_progress SET phase_progress = 1, heartbeat_sequence = heartbeat_sequence + 1, progress_updated_at = progress_updated_at + interval '1 second', updated_at = updated_at + interval '1 second' WHERE attempt_id = $1",
			args:  []any{assignment.AttemptID},
		},
		{
			name:  "estimated remaining duration overflow",
			query: "UPDATE attempt_progress SET estimated_remaining_seconds = 9223372037, heartbeat_sequence = heartbeat_sequence + 1, progress_updated_at = progress_updated_at + interval '1 second', updated_at = updated_at + interval '1 second' WHERE attempt_id = $1",
			args:  []any{assignment.AttemptID},
		},
		{
			name:  "non-object GPU health",
			query: "UPDATE attempt_progress SET gpu_health_summary = '[]'::jsonb, heartbeat_sequence = heartbeat_sequence + 1, progress_updated_at = progress_updated_at + interval '1 second', updated_at = updated_at + interval '1 second' WHERE attempt_id = $1",
			args:  []any{assignment.AttemptID},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, mutationErr := fixture.database.Admin.Exec(test.query, test.args...); mutationErr == nil {
				t.Fatalf("direct %s mutation was accepted", test.name)
			}
		})
	}
	after := readHeartbeatInvariantState(t, fixture.database.Admin, assignment.AttemptID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected Heartbeat invariant mutation changed state: before=%#v after=%#v", before, after)
	}
}

func TestAttemptProgressCannotReferenceAnotherJob(t *testing.T) {
	fixture := newAssignmentFixture(t, "heartbeat-progress-job-binding", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	accepted := submitJob(t, server.URL, "heartbeat-other-job", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"must not receive another Attempt progress"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit other Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var otherJob jobResponse
	if err := json.Unmarshal(accepted.Body, &otherJob); err != nil {
		t.Fatalf("decode other Job: %v", err)
	}
	_, err = fixture.database.Admin.Exec(`
		INSERT INTO attempt_progress (
			attempt_id, organization_id, project_id, job_id, worker_id, worker_epoch,
			fence, heartbeat_sequence, request_hash, backend_stage, execution_phase,
			gpu_health_summary, local_artifact_state, scratch_free_bytes,
			artifact_store_reachable, progress_updated_at, progress_valid_until, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, 7,
			1, 1, decode(repeat('00', 32), 'hex'), 'preparing', 'PREPARING',
			'{}', '{}', 1, true, clock_timestamp(), clock_timestamp() + interval '1 minute',
			clock_timestamp()
		)
	`,
		assignment.AttemptID,
		testOrganizationID,
		testProjectID,
		otherJob.JobID,
		assignment.WorkerID,
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23503" ||
		postgresError.ConstraintName != "attempt_progress_attempt_identity_fkey" {
		t.Fatalf("cross-Job Attempt progress error = %v, want attempt identity foreign key", err)
	}
}

type heartbeatInvariantState struct {
	TokenClaimExpiresAt time.Time
	JobID               uuid.UUID
	WorkerID            uuid.UUID
	WorkerEpoch         int64
	Fence               int64
	Sequence            int64
	ProgressUpdatedAt   time.Time
	BackendStage        string
	GPUHealthSummary    string
}

func readHeartbeatInvariantState(t *testing.T, db *sql.DB, attemptID uuid.UUID) heartbeatInvariantState {
	t.Helper()
	var state heartbeatInvariantState
	if err := db.QueryRow(`
		SELECT
			l.token_claim_expires_at,
			p.job_id,
			p.worker_id,
			p.worker_epoch,
			p.fence,
			p.heartbeat_sequence,
			p.progress_updated_at,
			p.backend_stage,
			p.gpu_health_summary::text
		FROM attempt_progress AS p
		JOIN attempt_leases AS l ON l.attempt_id = p.attempt_id
		WHERE p.attempt_id = $1
	`, attemptID).Scan(
		&state.TokenClaimExpiresAt,
		&state.JobID,
		&state.WorkerID,
		&state.WorkerEpoch,
		&state.Fence,
		&state.Sequence,
		&state.ProgressUpdatedAt,
		&state.BackendStage,
		&state.GPUHealthSummary,
	); err != nil {
		t.Fatalf("read Heartbeat invariant state: %v", err)
	}
	return state
}

func fetchJobView(t *testing.T, serverURL string, jobID uuid.UUID) (jobResponse, map[string]json.RawMessage) {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodGet,
		serverURL+"/v1/projects/"+testProjectID+"/jobs/"+jobID.String(),
		nil,
	)
	if err != nil {
		t.Fatalf("create Job GET request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET Job: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET Job status = %d, want 200", response.StatusCode)
	}
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&raw); err != nil {
		t.Fatalf("decode raw Job view: %v", err)
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("re-encode Job view: %v", err)
	}
	var view jobResponse
	if err := json.Unmarshal(payload, &view); err != nil {
		t.Fatalf("decode Job view: %v", err)
	}
	return view, raw
}

type heartbeatState struct {
	LeaseExpiresAt    time.Time
	Sequence          int64
	BackendStage      string
	ExecutionPhase    string
	ProgressUpdatedAt time.Time
	WorkerHeartbeatAt time.Time
	JobVersion        int64
	OutboxEvents      int64
}

type heartbeatMutationState struct {
	LeaseExpiresAt    time.Time
	ProgressRows      int64
	WorkerHeartbeatAt sql.NullTime
	JobVersion        int64
	OutboxEvents      int64
}

func readHeartbeatMutationState(
	t *testing.T,
	db *sql.DB,
	assignment workercontrol.Assignment,
) heartbeatMutationState {
	t.Helper()
	var state heartbeatMutationState
	if err := db.QueryRow(`
		SELECT
			(SELECT expires_at FROM attempt_leases WHERE attempt_id = $1),
			(SELECT count(*) FROM attempt_progress WHERE attempt_id = $1),
			(SELECT last_heartbeat_at FROM workers WHERE id = $2),
			(SELECT version FROM jobs WHERE id = $3),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = $3)
	`, assignment.AttemptID, assignment.WorkerID, assignment.JobID).Scan(
		&state.LeaseExpiresAt,
		&state.ProgressRows,
		&state.WorkerHeartbeatAt,
		&state.JobVersion,
		&state.OutboxEvents,
	); err != nil {
		t.Fatalf("read Heartbeat mutation state: %v", err)
	}
	return state
}

func validHeartbeatObservation(sequence int64) workercontrol.HeartbeatObservation {
	return workercontrol.HeartbeatObservation{
		Sequence:               sequence,
		BackendStage:           "generation",
		GPUHealthSummary:       json.RawMessage(`{"healthy":true}`),
		LocalArtifactState:     json.RawMessage(`{"state":"empty"}`),
		ScratchFreeBytes:       1 << 36,
		ArtifactStoreReachable: true,
	}
}

func readHeartbeatState(t *testing.T, db *sql.DB, attemptID uuid.UUID) heartbeatState {
	t.Helper()
	var state heartbeatState
	if err := db.QueryRow(`
		SELECT
			l.expires_at,
			p.heartbeat_sequence,
			p.backend_stage,
			p.execution_phase,
			p.progress_updated_at,
			w.last_heartbeat_at,
			j.version,
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = j.id)
		FROM attempt_leases AS l
		JOIN attempt_progress AS p ON p.attempt_id = l.attempt_id
		JOIN attempts AS a ON a.id = l.attempt_id
		JOIN jobs AS j ON j.id = a.job_id
		JOIN workers AS w ON w.id = l.worker_id
		WHERE l.attempt_id = $1
	`, attemptID).Scan(
		&state.LeaseExpiresAt,
		&state.Sequence,
		&state.BackendStage,
		&state.ExecutionPhase,
		&state.ProgressUpdatedAt,
		&state.WorkerHeartbeatAt,
		&state.JobVersion,
		&state.OutboxEvents,
	); err != nil {
		t.Fatalf("read Heartbeat state: %v", err)
	}
	return state
}

func sameHeartbeatDurableResult(left, right workercontrol.HeartbeatResult) bool {
	left.LeaseValidFor = 0
	right.LeaseValidFor = 0
	return reflect.DeepEqual(left, right)
}

func requirePostgresOperationalConstraint(t *testing.T, err error, constraint string) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.Code != "55000" ||
		postgresError.ConstraintName != constraint {
		t.Fatalf("protocol constraint error = %v, want SQLSTATE 55000 from %s", err, constraint)
	}
}
