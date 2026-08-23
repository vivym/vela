//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/artifactcleanup"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/finalizationreconciler"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestLaunchOutputSpecCannotDisableRequiredThumbnail(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	if _, err := database.Admin.Exec(`
		INSERT INTO output_specs (
			id, stable_id, revision, state, width, height, duration_milliseconds,
			frame_rate_milli, codec, container, thumbnail_required
		)
		SELECT
			$1, stable_id || '-without-thumbnail', 1, state, width, height,
			duration_milliseconds, frame_rate_milli, codec, container, false
		FROM output_specs
		WHERE stable_id = 'video-1080p-5s-24fps'
	`, uuid.New()); err == nil || !strings.Contains(err.Error(), "output_specs_thumbnail_required") {
		t.Fatalf("disable required thumbnail error = %v", err)
	}
}

func TestBeginFinalizationCommitsOneFixedArtifactPlanAndReplays(t *testing.T) {
	fixture := newStartFixture(t, "begin-finalization-replay", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}

	first, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil {
		t.Fatalf("begin finalization: %v", err)
	}
	if first.Decision != workercontrol.FinalizationGranted ||
		first.AttemptID != fixture.assignment.AttemptID ||
		first.JobID != fixture.assignment.JobID ||
		first.JobVersion != 4 ||
		len(first.Artifacts) != 2 {
		t.Fatalf("first FinalizationPlan = %#v", first)
	}
	if !first.FinalizationDeadlineAt.Equal(first.FinalizationStartedAt.Add(10 * time.Minute)) {
		t.Fatalf(
			"finalization deadline = %s, want start %s + 10m",
			first.FinalizationDeadlineAt,
			first.FinalizationStartedAt,
		)
	}
	if first.Artifacts[0].Kind != workercontrol.ArtifactKindVideo ||
		first.Artifacts[0].Ordinal != 0 ||
		first.Artifacts[1].Kind != workercontrol.ArtifactKindThumbnail ||
		first.Artifacts[1].Ordinal != 0 {
		t.Fatalf("required Artifact plan = %#v", first.Artifacts)
	}
	for _, artifact := range first.Artifacts {
		if artifact.ArtifactID == artifact.UploadID ||
			artifact.ObjectKey == "" ||
			!artifact.ExpiresAt.Equal(first.FinalizationDeadlineAt) {
			t.Fatalf("invalid planned Artifact = %#v", artifact)
		}
	}

	replayed, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil {
		t.Fatalf("replay finalization: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed FinalizationPlan = %#v, want %#v", replayed, first)
	}

	var (
		jobState, attemptState, leasePhase string
		jobVersion                         int64
		startedAt, deadlineAt              time.Time
		artifacts, uploads                 int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state::text,
			j.version,
			a.state::text,
			a.finalization_started_at,
			a.finalization_deadline_at,
			l.phase::text,
			(SELECT count(*) FROM artifacts WHERE attempt_id = a.id),
			(SELECT count(*) FROM artifact_uploads WHERE attempt_id = a.id)
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		JOIN attempt_leases AS l ON l.attempt_id = a.id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&attemptState,
		&startedAt,
		&deadlineAt,
		&leasePhase,
		&artifacts,
		&uploads,
	); err != nil {
		t.Fatalf("read committed FinalizationPlan: %v", err)
	}
	if jobState != "FINALIZING" || jobVersion != first.JobVersion ||
		attemptState != "FINALIZING" || leasePhase != "FINALIZATION" ||
		!startedAt.Equal(first.FinalizationStartedAt) ||
		!deadlineAt.Equal(first.FinalizationDeadlineAt) ||
		artifacts != 2 || uploads != 2 {
		t.Fatalf(
			"committed finalization = job %s/%d Attempt %s Lease %s times %s/%s rows %d/%d",
			jobState,
			jobVersion,
			attemptState,
			leasePhase,
			startedAt,
			deadlineAt,
			artifacts,
			uploads,
		)
	}
}

func TestArtifactAndUploadDefinitionsAreDatabaseImmutable(t *testing.T) {
	fixture := newStartFixture(t, "artifact-definition-immutable", 7)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	internal := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	for _, mutation := range []struct {
		name      string
		statement string
		argument  uuid.UUID
	}{
		{
			name:      "Artifact object key",
			statement: "UPDATE artifacts SET object_key = object_key || '.changed' WHERE id = $1",
			argument:  plan.Artifacts[0].ArtifactID,
		},
		{
			name:      "ArtifactUpload id",
			statement: "UPDATE artifact_uploads SET id = gen_random_uuid() WHERE id = $1",
			argument:  plan.Artifacts[0].UploadID,
		},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			if _, err := internal.Exec(context.Background(), mutation.statement, mutation.argument); err == nil ||
				!strings.Contains(err.Error(), "immutable") {
				t.Fatalf("database accepted %s mutation: %v", mutation.name, err)
			}
		})
	}
}

func TestFinalizationHeartbeatRenewsWorkerLeaseWithinImmutableDeadline(t *testing.T) {
	fixture := newStartFixture(t, "finalization-heartbeat-renewal", 7)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}

	var originalLeaseExpiresAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT expires_at
		FROM attempt_leases
		WHERE attempt_id = $1 AND revoked_at IS NULL
	`, plan.AttemptID).Scan(&originalLeaseExpiresAt); err != nil {
		t.Fatalf("read original Finalization Lease expiry: %v", err)
	}
	heartbeat := validHeartbeatObservation(1)
	heartbeat.BackendStage = "artifact-upload"
	heartbeat.LocalArtifactState = json.RawMessage(`{"state":"uploading","completed":0,"total":2}`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := fixture.service.Heartbeat(
		ctx,
		fixture.worker,
		fixture.credentials,
		heartbeat,
	)
	if err != nil {
		t.Fatalf("Finalization Heartbeat: %v", err)
	}
	if result.Decision != workercontrol.HeartbeatContinue ||
		result.AttemptID != plan.AttemptID || result.JobID != plan.JobID ||
		result.ExecutionPhase != workercontrol.ExecutionPhaseFinalizing ||
		result.HeartbeatSequence != heartbeat.Sequence ||
		!result.LeaseExpiresAt.After(originalLeaseExpiresAt) ||
		result.LeaseExpiresAt.After(plan.FinalizationDeadlineAt) ||
		result.LeaseValidFor <= 0 {
		t.Fatalf(
			"Finalization Heartbeat = %#v, original expiry %s deadline %s",
			result,
			originalLeaseExpiresAt,
			plan.FinalizationDeadlineAt,
		)
	}

	var leasePhase, progressPhase string
	var storedLeaseExpiresAt time.Time
	var heartbeatSequence int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT lease.phase::text, lease.expires_at,
			progress.execution_phase::text, progress.heartbeat_sequence
		FROM attempt_leases AS lease
		JOIN attempt_progress AS progress ON progress.attempt_id = lease.attempt_id
		WHERE lease.attempt_id = $1 AND lease.revoked_at IS NULL
	`, plan.AttemptID).Scan(
		&leasePhase,
		&storedLeaseExpiresAt,
		&progressPhase,
		&heartbeatSequence,
	); err != nil {
		t.Fatalf("read persisted Finalization Heartbeat: %v", err)
	}
	if leasePhase != "FINALIZATION" || progressPhase != "FINALIZING" ||
		heartbeatSequence != heartbeat.Sequence ||
		!storedLeaseExpiresAt.Equal(result.LeaseExpiresAt) {
		t.Fatalf(
			"persisted Finalization Heartbeat = phase %s/%s sequence %d expiry %s",
			leasePhase,
			progressPhase,
			heartbeatSequence,
			storedLeaseExpiresAt,
		)
	}

	forceFinalizationFailureWindow(
		t,
		fixture.database.Admin,
		plan.JobID,
		plan.AttemptID,
		false,
		false,
	)
	replayedAfterDeadline, err := fixture.service.Heartbeat(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		heartbeat,
	)
	if err != nil {
		t.Fatalf("replay Finalization Heartbeat after deadline: %v", err)
	}
	if replayedAfterDeadline.Decision != workercontrol.HeartbeatStop ||
		replayedAfterDeadline.StopReason != workercontrol.StopNotHeartbeatable {
		t.Fatalf("Finalization Heartbeat replay after deadline = %#v", replayedAfterDeadline)
	}
}

func TestBeginFinalizationCapsLongExecutionLeaseAtImmutableDeadline(t *testing.T) {
	fixture := newAssignmentFixture(t, "finalization-caps-long-lease", 7)
	internal := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	service, err := workercontrol.NewService(
		context.Background(),
		internal,
		workercontrol.Config{
			LeaseTTL:         20 * time.Minute,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
		},
	)
	if err != nil {
		t.Fatalf("create long-Lease Worker coordinator: %v", err)
	}
	assignment, err := service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create long Assignment: %v", err)
	}
	credentials := leaseCredentials(assignment)
	if started, startErr := service.Start(
		context.Background(), fixture.worker, credentials,
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, startErr)
	}
	plan, err := service.BeginFinalization(context.Background(), fixture.worker, credentials)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	if !assignment.LeaseExpiresAt.After(plan.FinalizationDeadlineAt) {
		t.Fatalf(
			"test fixture Lease expiry %s does not exceed deadline %s",
			assignment.LeaseExpiresAt,
			plan.FinalizationDeadlineAt,
		)
	}
	var finalizationLeaseExpiresAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT expires_at
		FROM attempt_leases
		WHERE attempt_id = $1 AND revoked_at IS NULL
	`, assignment.AttemptID).Scan(&finalizationLeaseExpiresAt); err != nil {
		t.Fatalf("read capped Finalization Lease: %v", err)
	}
	if !finalizationLeaseExpiresAt.Equal(plan.FinalizationDeadlineAt) {
		t.Fatalf(
			"Finalization Lease expiry = %s, want immutable deadline %s",
			finalizationLeaseExpiresAt,
			plan.FinalizationDeadlineAt,
		)
	}
}

func TestFinalizingJobExpiryFencesAuthorityAccountsBothPhasesAndDoesNotCharge(t *testing.T) {
	fixture := newStartFixture(t, "finalizing-job-expiry", 7)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	forceFinalizationFailureWindow(t, fixture.database.Admin, plan.JobID, plan.AttemptID, true, false)

	result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile FINALIZING Job Expiry: %v", err)
	}
	if !result.Processed || result.Source != workercontrol.FailureSourceJobExpired ||
		result.Decision.Disposition != workercontrol.RetryDispositionFailed ||
		result.Decision.AttemptID != plan.AttemptID ||
		result.Decision.AttemptState != workercontrol.FailedAttempt ||
		result.Decision.AttemptComputeSeconds != 6 ||
		result.Decision.TotalComputeSeconds != 6 ||
		result.Decision.AttemptFinalizationSeconds != 5 ||
		result.Decision.TotalFinalizationSeconds != 5 ||
		result.Decision.JobFence != fixture.assignment.LeaseFence+1 ||
		result.Decision.JobVersion != plan.JobVersion+1 {
		t.Fatalf("FINALIZING Job Expiry result = %#v", result)
	}
	state := readFinalizationFailureState(t, fixture.database.Admin, plan.JobID, plan.AttemptID)
	if state.JobState != "FAILED" || state.AttemptState != "FAILED" ||
		!state.AttemptEnded || !state.LeaseRevoked ||
		state.ComputeSeconds != 6 || state.FinalizationSeconds != 5 ||
		state.FinalizationRetryCount != 0 || state.ProjectRunning != 0 ||
		state.ProjectRetryWait != 0 || state.PoolRetryWait != 0 ||
		state.ReservationState != "RELEASED" || state.ReservedMinor != 0 ||
		state.WorkerState != "DRAINING" || state.Artifacts != 2 || state.Uploads != 2 ||
		state.StagingArtifacts != 2 || state.InitiatedUploads != 2 ||
		state.Charges != 0 || state.Decisions != 1 || state.FailedEvents != 1 {
		t.Fatalf("FINALIZING Job Expiry state = %#v", state)
	}
}

func TestFinalizationDeadlineUsesRetryBudgetAndPreservesTemporaryArtifacts(t *testing.T) {
	tests := []struct {
		name              string
		key               string
		exhaustAttempts   bool
		wantDisposition   workercontrol.RetryDisposition
		wantJobState      string
		wantRetryWait     int
		wantReservation   string
		wantReservedMinor int64
		wantRetryEvents   int
		wantFailedEvents  int
	}{
		{
			name:              "Retry budget remains",
			key:               "finalization-deadline-retry",
			wantDisposition:   workercontrol.RetryDispositionRetryWait,
			wantJobState:      "RETRY_WAIT",
			wantRetryWait:     1,
			wantReservation:   "RESERVED",
			wantReservedMinor: 1250,
			wantRetryEvents:   1,
		},
		{
			name:             "Attempt budget exhausted",
			key:              "finalization-deadline-terminal",
			exhaustAttempts:  true,
			wantDisposition:  workercontrol.RetryDispositionFailed,
			wantJobState:     "FAILED",
			wantReservation:  "RELEASED",
			wantFailedEvents: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStartFixture(t, test.key, 7)
			if started, err := fixture.service.Start(
				context.Background(), fixture.worker, fixture.credentials,
			); err != nil || started.Decision != workercontrol.StartGranted {
				t.Fatalf("Start = %#v error=%v", started, err)
			}
			plan, err := fixture.service.BeginFinalization(
				context.Background(), fixture.worker, fixture.credentials,
			)
			if err != nil || plan.Decision != workercontrol.FinalizationGranted {
				t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
			}
			forceFinalizationFailureWindow(
				t,
				fixture.database.Admin,
				plan.JobID,
				plan.AttemptID,
				false,
				test.exhaustAttempts,
			)

			result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
			if err != nil {
				t.Fatalf("reconcile finalization deadline: %v", err)
			}
			if !result.Processed ||
				result.Source != workercontrol.FailureSource("FINALIZATION_DEADLINE_EXPIRED") ||
				result.Decision.Disposition != test.wantDisposition ||
				result.Decision.AttemptID != plan.AttemptID ||
				result.Decision.AttemptState != workercontrol.FailedAttempt ||
				result.Decision.AttemptComputeSeconds != 6 ||
				result.Decision.TotalComputeSeconds != 6 ||
				result.Decision.AttemptFinalizationSeconds != 5 ||
				result.Decision.TotalFinalizationSeconds != 5 ||
				result.Decision.JobFence != fixture.assignment.LeaseFence+1 ||
				result.Decision.JobVersion != plan.JobVersion+1 {
				t.Fatalf("finalization deadline result = %#v", result)
			}
			state := readFinalizationFailureState(
				t,
				fixture.database.Admin,
				plan.JobID,
				plan.AttemptID,
			)
			if state.JobState != test.wantJobState || state.AttemptState != "FAILED" ||
				!state.AttemptEnded || !state.LeaseRevoked ||
				state.ComputeSeconds != 6 || state.FinalizationSeconds != 5 ||
				state.FinalizationRetryCount != test.wantRetryWait ||
				state.ProjectRunning != 0 || state.ProjectRetryWait != test.wantRetryWait ||
				state.PoolRetryWait != test.wantRetryWait ||
				state.ReservationState != test.wantReservation ||
				state.ReservedMinor != test.wantReservedMinor || state.WorkerState != "DRAINING" ||
				state.Artifacts != 2 || state.Uploads != 2 ||
				state.StagingArtifacts != 2 || state.InitiatedUploads != 2 ||
				state.Charges != 0 || state.Decisions != 1 ||
				state.RetryEvents != test.wantRetryEvents ||
				state.FailedEvents != test.wantFailedEvents {
				t.Fatalf("finalization deadline state = %#v", state)
			}
		})
	}
}

func TestReconcilerTakesOverExpiredWorkerFinalizationAndCompletesSameAttempt(t *testing.T) {
	fixture := newStartFixture(t, "reconciler-finalization-takeover", 7)
	service := visibleCompletionService(t, fixture.database.DSN)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	artifactIDs := uploadFinalizationPlanWithoutVerification(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	workerVerificationID := uuid.New()
	workerVerified, err := service.VerifyArtifact(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		workerVerificationID,
	)
	if err != nil || workerVerified.Decision != workercontrol.ArtifactVerified {
		t.Fatalf("Worker verifies first Artifact before loss = %#v error=%v", workerVerified, err)
	}
	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)

	reconciler := workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	}
	takeover, err := service.ReconcileNextFinalization(context.Background(), reconciler)
	if err != nil {
		t.Fatalf("ReconcileNextFinalization: %v", err)
	}
	if takeover.Decision != workercontrol.FinalizationTakeoverGranted ||
		takeover.LeaseID == uuid.Nil || takeover.Credentials.AttemptID != plan.AttemptID ||
		takeover.Credentials.WorkerID != fixture.worker.ID ||
		takeover.Credentials.WorkerEpoch != fixture.credentials.WorkerEpoch ||
		takeover.Credentials.Fence != fixture.credentials.Fence ||
		takeover.Credentials.Token == "" || takeover.Plan.JobID != plan.JobID ||
		takeover.Plan.JobVersion != plan.JobVersion ||
		!takeover.Plan.FinalizationStartedAt.Equal(plan.FinalizationStartedAt) ||
		!takeover.Plan.FinalizationDeadlineAt.Equal(plan.FinalizationDeadlineAt) ||
		!reflect.DeepEqual(takeover.Plan.Artifacts, plan.Artifacts) ||
		!takeover.LeaseExpiresAt.After(time.Now()) ||
		takeover.LeaseExpiresAt.After(plan.FinalizationDeadlineAt) {
		t.Fatalf("Finalization takeover = %#v, original plan %#v", takeover, plan)
	}
	replayedTakeover, err := service.ReconcileNextFinalization(context.Background(), reconciler)
	if err != nil {
		t.Fatalf("replay ReconcileNextFinalization: %v", err)
	}
	if replayedTakeover.Decision != workercontrol.FinalizationTakeoverGranted ||
		replayedTakeover.LeaseID != takeover.LeaseID ||
		replayedTakeover.Credentials != takeover.Credentials ||
		!replayedTakeover.LeaseExpiresAt.Equal(takeover.LeaseExpiresAt) ||
		!reflect.DeepEqual(replayedTakeover.Plan, takeover.Plan) {
		t.Fatalf("replayed Finalization takeover = %#v, want %#v", replayedTakeover, takeover)
	}

	var (
		leaseCount, revokedWorkerLeases, activeReconcilerLeases int
		activeFence                                             int64
		activeOwnerID                                           string
		workerState                                             string
		startedAt, deadlineAt                                   time.Time
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*),
			count(*) FILTER (WHERE owner_kind = 'WORKER' AND revoked_at IS NOT NULL),
			count(*) FILTER (WHERE owner_kind = 'RECONCILER' AND revoked_at IS NULL),
			max(lease.fence) FILTER (WHERE owner_kind = 'RECONCILER' AND revoked_at IS NULL),
			max(owner_id) FILTER (WHERE owner_kind = 'RECONCILER' AND revoked_at IS NULL),
			max(worker.lifecycle_state::text),
			max(attempt.finalization_started_at),
			max(attempt.finalization_deadline_at)
		FROM attempt_leases AS lease
		JOIN attempts AS attempt ON attempt.id = lease.attempt_id
		JOIN workers AS worker ON worker.id = attempt.worker_id
		WHERE lease.attempt_id = $1
	`, plan.AttemptID).Scan(
		&leaseCount,
		&revokedWorkerLeases,
		&activeReconcilerLeases,
		&activeFence,
		&activeOwnerID,
		&workerState,
		&startedAt,
		&deadlineAt,
	); err != nil {
		t.Fatalf("read Reconciler takeover state: %v", err)
	}
	if leaseCount != 2 || revokedWorkerLeases != 1 || activeReconcilerLeases != 1 ||
		activeFence != fixture.credentials.Fence || activeOwnerID != reconciler.ID ||
		workerState != "DRAINING" || !startedAt.Equal(plan.FinalizationStartedAt) ||
		!deadlineAt.Equal(plan.FinalizationDeadlineAt) {
		t.Fatalf(
			"takeover state = leases %d revoked Worker %d active Reconciler %d fence %d owner %q Worker %s times %s/%s",
			leaseCount,
			revokedWorkerLeases,
			activeReconcilerLeases,
			activeFence,
			activeOwnerID,
			workerState,
			startedAt,
			deadlineAt,
		)
	}
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID:       uuid.New(),
		ExpectedJobVersion: plan.JobVersion,
		ArtifactIDs:        artifactIDs,
	}
	lateWorker, err := service.CompleteVisibleCompletion(
		context.Background(), fixture.worker, fixture.credentials, candidate,
	)
	if err != nil || lateWorker.Decision != workercontrol.VisibleCompletionRejectedStaleLease {
		t.Fatalf("late Worker completion = %#v error=%v", lateWorker, err)
	}
	verifications, completed := verifyAndCommitVisibleCompletionAsReconciler(
		t,
		service,
		fixture.database.Admin,
		reconciler,
		takeover.Credentials,
		plan,
		candidate,
	)
	if verifications[0].VerificationID != workerVerificationID {
		t.Fatalf(
			"Reconciler replayed verification ID = %s, want immutable Worker receipt %s",
			verifications[0].VerificationID,
			workerVerificationID,
		)
	}
	lateWorker, err = service.CompleteVisibleCompletion(
		context.Background(), fixture.worker, fixture.credentials, candidate,
	)
	if err != nil || lateWorker.Decision != workercontrol.VisibleCompletionAlreadySucceeded ||
		lateWorker.CompletionID != candidate.CompletionID ||
		lateWorker.ArtifactSetID != completed.ArtifactSetID ||
		lateWorker.ChargeID != completed.ChargeID {
		t.Fatalf("late Worker replay after Reconciler completion = %#v error=%v", lateWorker, err)
	}
}

func TestReconcilerReclaimsExpiredReconcilerLeaseWithoutResettingDeadline(t *testing.T) {
	fixture := newStartFixture(t, "reconciler-finalization-reclaim", 7)
	service := visibleCompletionService(t, fixture.database.DSN)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	uploadFinalizationPlanWithoutVerification(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)

	firstIdentity := workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization-a",
	}
	first, err := service.ReconcileNextFinalization(context.Background(), firstIdentity)
	if err != nil || first.Decision != workercontrol.FinalizationTakeoverGranted {
		t.Fatalf("first ReconcileNextFinalization = %#v error=%v", first, err)
	}
	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)

	secondIdentity := workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization-b",
	}
	second, err := service.ReconcileNextFinalization(context.Background(), secondIdentity)
	if err != nil {
		t.Fatalf("second ReconcileNextFinalization: %v", err)
	}
	if second.Decision != workercontrol.FinalizationTakeoverGranted ||
		second.LeaseID == uuid.Nil || second.LeaseID == first.LeaseID ||
		second.Credentials.AttemptID != first.Credentials.AttemptID ||
		second.Credentials.WorkerID != first.Credentials.WorkerID ||
		second.Credentials.WorkerEpoch != first.Credentials.WorkerEpoch ||
		second.Credentials.Fence != first.Credentials.Fence ||
		second.Credentials.Token == "" || second.Credentials.Token == first.Credentials.Token ||
		!reflect.DeepEqual(second.Plan, first.Plan) ||
		!second.Plan.FinalizationStartedAt.Equal(plan.FinalizationStartedAt) ||
		!second.Plan.FinalizationDeadlineAt.Equal(plan.FinalizationDeadlineAt) ||
		second.LeaseExpiresAt.After(plan.FinalizationDeadlineAt) {
		t.Fatalf("reclaimed Finalization = %#v, first %#v, original plan %#v", second, first, plan)
	}

	var active, revokedWorker, revokedReconciler int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE revoked_at IS NULL),
			count(*) FILTER (WHERE owner_kind = 'WORKER' AND revoked_at IS NOT NULL),
			count(*) FILTER (WHERE owner_kind = 'RECONCILER' AND revoked_at IS NOT NULL)
		FROM attempt_leases
		WHERE attempt_id = $1
	`, plan.AttemptID).Scan(&active, &revokedWorker, &revokedReconciler); err != nil {
		t.Fatalf("read reclaimed Reconciler Leases: %v", err)
	}
	if active != 1 || revokedWorker != 1 || revokedReconciler != 1 {
		t.Fatalf(
			"reclaimed Reconciler Leases = active %d revoked Worker %d revoked Reconciler %d",
			active,
			revokedWorker,
			revokedReconciler,
		)
	}

	staleVerification, err := service.VerifyArtifactAsReconciler(
		context.Background(),
		firstIdentity,
		first.Credentials,
		plan.Artifacts[0].UploadID,
		uuid.New(),
	)
	if err != nil || staleVerification.Decision != workercontrol.ArtifactVerificationRejected {
		t.Fatalf("expired Reconciler verification = %#v error=%v", staleVerification, err)
	}
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID:       uuid.New(),
		ExpectedJobVersion: second.Plan.JobVersion,
		ArtifactIDs:        plannedArtifactIDs(second.Plan),
	}
	verifyAndCommitVisibleCompletionAsReconciler(
		t,
		service,
		fixture.database.Admin,
		secondIdentity,
		second.Credentials,
		second.Plan,
		candidate,
	)
}

func TestReconcilerRecordsUnrecoverableArtifactFinalizationExactlyOnce(t *testing.T) {
	fixture := newStartFixture(t, "reconciler-unrecoverable-artifact", 7)
	webhookSubscriptionID := createTerminalWebhookSubscription(
		t, fixture.database, "job.failed",
	)
	service := visibleCompletionService(t, fixture.database.DSN)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	uploadFinalizationPlanWithoutVerification(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)
	reconciler := workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	}
	takeover, err := service.ReconcileNextFinalization(context.Background(), reconciler)
	if err != nil || takeover.Decision != workercontrol.FinalizationTakeoverGranted {
		t.Fatalf("ReconcileNextFinalization = %#v error=%v", takeover, err)
	}
	failure := workercontrol.ArtifactFinalizationFailure{
		ArtifactID:   plan.Artifacts[0].ArtifactID,
		UploadID:     plan.Artifacts[0].UploadID,
		Code:         workercontrol.ArtifactFinalizationFailureCompletedObjectMismatch,
		ErrorSummary: "completed object version does not match the persisted completion intent",
	}

	first, err := service.RecordUnrecoverableArtifactFinalizationAsReconciler(
		context.Background(), reconciler, takeover.Credentials, failure,
	)
	if err != nil || first.Disposition != workercontrol.RetryDispositionFailed ||
		first.AttemptID != plan.AttemptID || first.JobID != plan.JobID ||
		first.AttemptState != workercontrol.FailedAttempt || first.FailureClass != "ARTIFACT_UNRECOVERABLE" {
		t.Fatalf("record unrecoverable Artifact finalization = %#v error=%v", first, err)
	}
	replayed, err := service.RecordUnrecoverableArtifactFinalizationAsReconciler(
		context.Background(), reconciler, takeover.Credentials, failure,
	)
	if err != nil || !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replay unrecoverable Artifact finalization = %#v error=%v, want %#v", replayed, err, first)
	}
	conflict := failure
	conflict.ErrorSummary = "different outcome for the same terminal Attempt"
	rejected, err := service.RecordUnrecoverableArtifactFinalizationAsReconciler(
		context.Background(), reconciler, takeover.Credentials, conflict,
	)
	if err != nil || rejected.Disposition != workercontrol.RetryDispositionRejectedStaleLease {
		t.Fatalf("conflicting unrecoverable Artifact finalization = %#v error=%v", rejected, err)
	}

	var (
		jobState, attemptState, reservationState string
		leaseRevoked, attemptEnded               bool
		charges, decisions, failedEvents         int
		source, code, summary                    string
		artifactID, uploadID                     uuid.UUID
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			attempt.state::text,
			attempt.ended_at IS NOT NULL,
			lease.revoked_at IS NOT NULL,
			reservation.state::text,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM execution_failure_decisions WHERE attempt_id = attempt.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.failed'),
			decision.source::text,
			decision.finalization_failure_code,
			decision.error_summary,
			decision.artifact_id,
			decision.artifact_upload_id
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $2 AND attempt.job_id = job.id
		JOIN attempt_leases AS lease
		  ON lease.attempt_id = attempt.id AND lease.owner_kind = 'RECONCILER'
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN execution_failure_decisions AS decision ON decision.attempt_id = attempt.id
		WHERE job.id = $1
	`, plan.JobID, plan.AttemptID).Scan(
		&jobState,
		&attemptState,
		&attemptEnded,
		&leaseRevoked,
		&reservationState,
		&charges,
		&decisions,
		&failedEvents,
		&source,
		&code,
		&summary,
		&artifactID,
		&uploadID,
	); err != nil {
		t.Fatalf("read unrecoverable Artifact finalization: %v", err)
	}
	if jobState != "FAILED" || attemptState != "FAILED" || !attemptEnded || !leaseRevoked ||
		reservationState != "RELEASED" || charges != 0 || decisions != 1 || failedEvents != 1 ||
		source != "ARTIFACT_RECOVERY_UNRECOVERABLE" || code != string(failure.Code) ||
		summary != failure.ErrorSummary || artifactID != failure.ArtifactID || uploadID != failure.UploadID {
		t.Fatalf(
			"unrecoverable Artifact finalization state = %s/%s ended=%t revoked=%t reservation=%s charges=%d decisions=%d events=%d source=%s code=%s summary=%q Artifact=%s Upload=%s",
			jobState,
			attemptState,
			attemptEnded,
			leaseRevoked,
			reservationState,
			charges,
			decisions,
			failedEvents,
			source,
			code,
			summary,
			artifactID,
			uploadID,
		)
	}
	assertTerminalWebhookDelivery(
		t,
		fixture.database,
		webhookSubscriptionID,
		plan.JobID,
		"job.failed",
		"FAILED",
	)
}

func TestProductionArtifactReconcilerPublishesMixedDurableArtifactSet(t *testing.T) {
	fixture := newStartFixture(t, "production-artifact-reconciler-mixed-set", 7)
	service := visibleCompletionService(t, fixture.database.DSN)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	uploadFinalizationPlanWithoutVerification(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	workerVerificationID := uuid.New()
	workerVerified, err := service.VerifyArtifact(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		workerVerificationID,
	)
	if err != nil || workerVerified.Decision != workercontrol.ArtifactVerified {
		t.Fatalf("Worker verification = %#v error=%v", workerVerified, err)
	}
	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)

	identity := workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	}
	reconciler, err := finalizationreconciler.New(
		service,
		&unexpectedArtifactCompletionStore{},
		identity,
	)
	if err != nil {
		t.Fatalf("create production Artifact Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("production Artifact Reconciler: %v", err)
	}
	if result.Takeover != workercontrol.FinalizationTakeoverGranted ||
		result.AttemptID != plan.AttemptID || result.JobID != plan.JobID ||
		result.Verified != len(plan.Artifacts) ||
		result.Completion.Decision != workercontrol.VisibleCompletionCommitted ||
		result.Completion.ArtifactSetID == uuid.Nil || result.Completion.ChargeID == uuid.Nil {
		t.Fatalf("production Artifact Reconciler result = %#v", result)
	}
	var (
		jobState, reservationState string
		charges, artifactSets      int
		storedVerificationID       uuid.UUID
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			reservation.state::text,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
			(SELECT verification_id FROM artifact_uploads WHERE id = $2)
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, plan.JobID, plan.Artifacts[0].UploadID).Scan(
		&jobState,
		&reservationState,
		&charges,
		&artifactSets,
		&storedVerificationID,
	); err != nil {
		t.Fatalf("read production Artifact Reconciler state: %v", err)
	}
	if jobState != "SUCCEEDED" || reservationState != "CONSUMED" ||
		charges != 1 || artifactSets != 1 || storedVerificationID != workerVerificationID {
		t.Fatalf(
			"production Artifact Reconciler state = %s/%s charges=%d sets=%d verification=%s",
			jobState,
			reservationState,
			charges,
			artifactSets,
			storedVerificationID,
		)
	}
}

func TestProductionArtifactReconcilerRecoversCompletedMultipartAfterWorkerResponseLoss(t *testing.T) {
	minio := newMinIOFixture(t, "vela-reconciler-response-loss")
	minio.enableVersioning(t)
	if err := minio.store.ValidateBucket(context.Background()); err != nil {
		t.Fatalf("validate Reconciler Artifact Store: %v", err)
	}
	fixture := newStartFixture(t, "reconciler-completed-multipart-response-loss", 7)
	service := visibleCompletionService(t, fixture.database.DSN)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}

	var lostResponseVersion artifactstore.ObjectVersion
	for index, artifact := range plan.Artifacts {
		claimID := uuid.New()
		claim, claimErr := service.ClaimArtifactUpload(
			context.Background(), fixture.worker, fixture.credentials, artifact.UploadID, claimID,
		)
		if claimErr != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
			t.Fatalf("claim Artifact %d upload = %#v error=%v", index, claim, claimErr)
		}
		session, sessionErr := minio.store.CreateMultipartUpload(
			context.Background(), artifact.ObjectKey, claim.ExpectedContentType,
		)
		if sessionErr != nil {
			t.Fatalf("create Artifact %d multipart session: %v", index, sessionErr)
		}
		recordedSession, sessionErr := service.RecordArtifactMultipartSession(
			context.Background(), fixture.worker, fixture.credentials,
			artifact.UploadID, claimID, session.UploadID,
		)
		if sessionErr != nil || recordedSession.Decision != workercontrol.ArtifactMultipartSessionRecorded {
			t.Fatalf("record Artifact %d multipart session = %#v error=%v", index, recordedSession, sessionErr)
		}
		payload := []byte(fmt.Sprintf("durable response-loss Artifact %d", index))
		digest := sha256.Sum256(payload)
		part, uploadErr := minio.store.UploadPart(
			context.Background(), session, 1, bytes.NewReader(payload), int64(len(payload)), digest,
		)
		if uploadErr != nil {
			t.Fatalf("upload Artifact %d multipart part: %v", index, uploadErr)
		}
		report := workercontrol.ArtifactUploadReport{
			SizeBytes:   int64(len(payload)),
			SHA256:      digest,
			ContentType: claim.ExpectedContentType,
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number:         part.Number,
				ETag:           part.ETag,
				SizeBytes:      part.SizeBytes,
				ChecksumSHA256: part.ChecksumSHA256,
			}},
		}
		intent, intentErr := service.RecordArtifactCompletionIntent(
			context.Background(), fixture.worker, fixture.credentials, artifact.UploadID, report,
		)
		if intentErr != nil || intent.Decision != workercontrol.ArtifactCompletionIntentRecorded {
			t.Fatalf("record Artifact %d completion intent = %#v error=%v", index, intent, intentErr)
		}
		version, completeErr := minio.store.CompleteMultipartUpload(
			context.Background(), session, []artifactstore.CompletedPart{part},
		)
		if completeErr != nil {
			t.Fatalf("complete Artifact %d multipart upload: %v", index, completeErr)
		}
		if index == 0 {
			lostResponseVersion = version
			continue
		}
		report.ObjectVersionID = version.VersionID
		uploaded, uploadErr := service.RecordArtifactUploaded(
			context.Background(), fixture.worker, fixture.credentials, artifact.UploadID, report,
		)
		if uploadErr != nil || uploaded.Decision != workercontrol.ArtifactUploadRecorded {
			t.Fatalf("record Artifact %d uploaded receipt = %#v error=%v", index, uploaded, uploadErr)
		}
	}
	var firstState string
	var firstVersion *string
	if err := fixture.database.Admin.QueryRow(`
		SELECT state::text, object_version_id
		FROM artifact_uploads
		WHERE id = $1
	`, plan.Artifacts[0].UploadID).Scan(&firstState, &firstVersion); err != nil {
		t.Fatalf("read response-loss upload state: %v", err)
	}
	if firstState != "UPLOADING" || firstVersion != nil || lostResponseVersion.VersionID == "" {
		t.Fatalf("response-loss upload = state %s version %v S3 %#v", firstState, firstVersion, lostResponseVersion)
	}

	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)
	identity := workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	}
	reconciler, err := finalizationreconciler.New(service, minio.store, identity)
	if err != nil {
		t.Fatalf("create production Artifact Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("recover response-loss Artifact finalization: %v", err)
	}
	if result.Takeover != workercontrol.FinalizationTakeoverGranted ||
		result.AttemptID != plan.AttemptID || result.JobID != plan.JobID ||
		result.Verified != len(plan.Artifacts) ||
		result.Completion.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("response-loss reconciliation = %#v", result)
	}
	var recoveredState, recoveredVersion string
	if err := fixture.database.Admin.QueryRow(`
		SELECT state::text, object_version_id
		FROM artifact_uploads
		WHERE id = $1
	`, plan.Artifacts[0].UploadID).Scan(&recoveredState, &recoveredVersion); err != nil {
		t.Fatalf("read recovered upload receipt: %v", err)
	}
	if recoveredState != "VERIFIED" || recoveredVersion != lostResponseVersion.VersionID {
		t.Fatalf("recovered upload = state %s version %s, want %s", recoveredState, recoveredVersion, lostResponseVersion.VersionID)
	}
	state := readVisibleCompletionMutationState(
		t, fixture.database.Admin, plan.JobID, plan.AttemptID,
	)
	if state.JobState != "SUCCEEDED" || state.AttemptState != "SUCCEEDED" ||
		state.ReservationState != "CONSUMED" || state.Charges != 1 || state.ArtifactSets != 1 ||
		state.CommittedArtifacts != len(plan.Artifacts) || state.TerminalEvents != 3 {
		t.Fatalf("response-loss Visible Completion state = %#v", state)
	}
}

func TestConcurrentReconcilersCreateOneSameFenceFinalizationLease(t *testing.T) {
	fixture := newStartFixture(t, "concurrent-reconciler-finalization-takeover", 7)
	service := visibleCompletionService(t, fixture.database.DSN)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	uploadAndVerifyFinalizationPlan(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)

	reconcilers := []workercontrol.AuthenticatedReconciler{
		{ID: "spiffe://vela.internal/reconciler/artifact-a"},
		{ID: "spiffe://vela.internal/reconciler/artifact-b"},
	}
	start := make(chan struct{})
	results := make(chan workercontrol.FinalizationTakeover, len(reconcilers))
	errorsFound := make(chan error, len(reconcilers))
	var wait sync.WaitGroup
	for _, reconciler := range reconcilers {
		wait.Add(1)
		go func(reconciler workercontrol.AuthenticatedReconciler) {
			defer wait.Done()
			<-start
			result, reconcileErr := service.ReconcileNextFinalization(
				context.Background(),
				reconciler,
			)
			results <- result
			errorsFound <- reconcileErr
		}(reconciler)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for reconcileErr := range errorsFound {
		if reconcileErr != nil {
			t.Fatalf("concurrent ReconcileNextFinalization: %v", reconcileErr)
		}
	}
	granted := 0
	noWork := 0
	for result := range results {
		switch result.Decision {
		case workercontrol.FinalizationTakeoverGranted:
			granted++
			if result.Credentials.Fence != fixture.credentials.Fence ||
				result.Credentials.AttemptID != plan.AttemptID ||
				!result.Plan.FinalizationDeadlineAt.Equal(plan.FinalizationDeadlineAt) {
				t.Fatalf("winning Reconciler Lease = %#v", result)
			}
		case workercontrol.FinalizationTakeoverNoWork:
			noWork++
		default:
			t.Fatalf("concurrent Reconciler result = %#v", result)
		}
	}
	if granted != 1 || noWork != 1 {
		t.Fatalf("concurrent Reconciler outcomes = granted %d no-work %d", granted, noWork)
	}
	var active, revoked int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE owner_kind = 'RECONCILER' AND revoked_at IS NULL),
			count(*) FILTER (WHERE owner_kind = 'WORKER' AND revoked_at IS NOT NULL)
		FROM attempt_leases
		WHERE attempt_id = $1
	`, plan.AttemptID).Scan(&active, &revoked); err != nil {
		t.Fatalf("read concurrent Reconciler Leases: %v", err)
	}
	if active != 1 || revoked != 1 {
		t.Fatalf("concurrent Reconciler Lease rows = active %d revoked %d", active, revoked)
	}
}

func TestWorkerAndReconcilerCompletionPublishOneSameFenceResult(t *testing.T) {
	fixture := newStartFixture(t, "worker-reconciler-completion-race", 7)
	service := visibleCompletionService(t, fixture.database.DSN)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	forceLeaseExpiry(t, fixture.database.Admin, plan.AttemptID, time.Second)
	reconciler := workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/completion-race",
	}
	takeover, err := service.ReconcileNextFinalization(context.Background(), reconciler)
	if err != nil || takeover.Decision != workercontrol.FinalizationTakeoverGranted {
		t.Fatalf("ReconcileNextFinalization = %#v error=%v", takeover, err)
	}
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID:       uuid.New(),
		ExpectedJobVersion: plan.JobVersion,
		ArtifactIDs:        artifactIDs,
	}

	type completionCall struct {
		result workercontrol.VisibleCompletionResult
		err    error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	workerResult := make(chan completionCall, 1)
	reconcilerResult := make(chan completionCall, 1)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		result, completeErr := service.CompleteVisibleCompletion(
			ctx,
			fixture.worker,
			fixture.credentials,
			candidate,
		)
		workerResult <- completionCall{result: result, err: completeErr}
	}()
	go func() {
		defer wait.Done()
		<-start
		result, completeErr := service.CompleteVisibleCompletionAsReconciler(
			ctx,
			reconciler,
			takeover.Credentials,
			candidate,
		)
		reconcilerResult <- completionCall{result: result, err: completeErr}
	}()
	close(start)
	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("Worker/Reconciler completion race deadlocked: %v", ctx.Err())
	}
	workerCompletion := <-workerResult
	reconcilerCompletion := <-reconcilerResult
	if workerCompletion.err != nil || reconcilerCompletion.err != nil {
		t.Fatalf(
			"Worker/Reconciler completion errors = %v / %v",
			workerCompletion.err,
			reconcilerCompletion.err,
		)
	}
	if workerCompletion.result.Decision != workercontrol.VisibleCompletionRejectedStaleLease &&
		workerCompletion.result.Decision != workercontrol.VisibleCompletionAlreadySucceeded {
		t.Fatalf("old Worker completion = %#v", workerCompletion.result)
	}
	if reconcilerCompletion.result.Decision != workercontrol.VisibleCompletionCommitted ||
		reconcilerCompletion.result.CompletionID != candidate.CompletionID ||
		reconcilerCompletion.result.ArtifactSetID == uuid.Nil ||
		reconcilerCompletion.result.ChargeID == uuid.Nil {
		t.Fatalf("Reconciler completion = %#v", reconcilerCompletion.result)
	}

	state := readVisibleCompletionMutationState(
		t,
		fixture.database.Admin,
		plan.JobID,
		plan.AttemptID,
	)
	if state.JobState != "SUCCEEDED" || state.JobVersion != plan.JobVersion+1 ||
		state.ResultArtifactSetID == nil || state.AttemptState != "SUCCEEDED" ||
		!state.AttemptEnded || !state.LeaseRevoked || state.WorkerState != "DRAINING" ||
		state.ProjectRunning != 0 || state.ReservationState != "CONSUMED" ||
		state.ReservedMinor != 0 || state.UnsettledPostedMinor != 1250 ||
		state.CommittedArtifacts != len(plan.Artifacts) || state.ArtifactSets != 1 ||
		state.ArtifactSetItems != len(plan.Artifacts) || state.VisibleCompletions != 1 ||
		state.AccessGrants != 1 || state.Charges != 1 || state.TerminalEvents != 3 {
		t.Fatalf("Worker/Reconciler completion state = %#v", state)
	}
	var totalLeases, activeLeases, failureDecisions int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*),
			count(*) FILTER (WHERE revoked_at IS NULL),
			(SELECT count(*) FROM execution_failure_decisions WHERE attempt_id = $1)
		FROM attempt_leases
		WHERE attempt_id = $1
	`, plan.AttemptID).Scan(&totalLeases, &activeLeases, &failureDecisions); err != nil {
		t.Fatalf("read Worker/Reconciler completion Lease state: %v", err)
	}
	if totalLeases != 2 || activeLeases != 0 || failureDecisions != 0 {
		t.Fatalf(
			"Worker/Reconciler completion authority = leases %d active %d failures %d",
			totalLeases,
			activeLeases,
			failureDecisions,
		)
	}
}

func TestArtifactUploadClaimIsDurableAndIdempotentBeforeMultipartCreate(t *testing.T) {
	fixture := newStartFixture(t, "artifact-upload-claim", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}

	claimID := uuid.New()
	first, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		claimID,
	)
	if err != nil {
		t.Fatalf("claim ArtifactUpload: %v", err)
	}
	if first.Decision != workercontrol.ArtifactUploadClaimGranted ||
		first.ClaimID != claimID ||
		first.UploadID != plan.Artifacts[0].UploadID ||
		first.ArtifactID != plan.Artifacts[0].ArtifactID ||
		first.ObjectKey != plan.Artifacts[0].ObjectKey ||
		first.ExpectedContentType != "video/mp4" ||
		first.Version != 2 ||
		!first.ClaimExpiresAt.After(time.Now()) ||
		first.ClaimExpiresAt.After(plan.FinalizationDeadlineAt) {
		t.Fatalf("first ArtifactUpload claim = %#v", first)
	}

	replayed, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		claimID,
	)
	if err != nil {
		t.Fatalf("replay ArtifactUpload claim: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed ArtifactUpload claim = %#v, want %#v", replayed, first)
	}

	competing, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		uuid.New(),
	)
	if err != nil {
		t.Fatalf("competing ArtifactUpload claim: %v", err)
	}
	if competing.Decision != workercontrol.ArtifactUploadClaimBusy {
		t.Fatalf("competing ArtifactUpload claim = %#v", competing)
	}
}

func TestMultipartSessionCASPersistsOneExternalCreateAndReplays(t *testing.T) {
	fixture := newStartFixture(t, "artifact-multipart-session-cas", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	claimID := uuid.New()
	claim, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		claimID,
	)
	if err != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
		t.Fatalf("ClaimArtifactUpload = %#v error=%v", claim, err)
	}

	const multipartID = "minio-multipart-session-01"
	first, err := fixture.service.RecordArtifactMultipartSession(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		claimID,
		multipartID,
	)
	if err != nil {
		t.Fatalf("record multipart session: %v", err)
	}
	if first.Decision != workercontrol.ArtifactMultipartSessionRecorded ||
		first.UploadID != claim.UploadID ||
		first.ArtifactID != claim.ArtifactID ||
		first.MultipartUploadID != multipartID ||
		first.Version != 3 {
		t.Fatalf("first multipart session = %#v", first)
	}
	resumedClaim, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		uuid.New(),
	)
	if err != nil || resumedClaim.Decision != workercontrol.ArtifactUploadClaimGranted ||
		resumedClaim.MultipartUploadID != multipartID ||
		!resumedClaim.UploadExpiresAt.Equal(plan.Artifacts[0].ExpiresAt) {
		t.Fatalf("resumed ArtifactUpload claim = %#v error=%v", resumedClaim, err)
	}

	replayed, err := fixture.service.RecordArtifactMultipartSession(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		claimID,
		multipartID,
	)
	if err != nil {
		t.Fatalf("replay multipart session: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed multipart session = %#v, want %#v", replayed, first)
	}

	conflict, err := fixture.service.RecordArtifactMultipartSession(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		claimID,
		"different-multipart-session",
	)
	if err != nil {
		t.Fatalf("conflicting multipart session: %v", err)
	}
	if conflict.Decision != workercontrol.ArtifactMultipartSessionConflict {
		t.Fatalf("conflicting multipart session = %#v", conflict)
	}

	var (
		storedMultipartID string
		state             string
		version           int64
		storedClaimID     *string
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT multipart_upload_id, state::text, version, claim_id::text
		FROM artifact_uploads
		WHERE id = $1
	`, claim.UploadID).Scan(
		&storedMultipartID,
		&state,
		&version,
		&storedClaimID,
	); err != nil {
		t.Fatalf("read persisted multipart session: %v", err)
	}
	if storedMultipartID != multipartID || state != "UPLOADING" ||
		version != first.Version || storedClaimID != nil {
		t.Fatalf(
			"persisted multipart session = id %q state %s version %d claim %v",
			storedMultipartID,
			state,
			version,
			storedClaimID,
		)
	}
}

func TestUploadedArtifactRecordsExactObjectVersionWithoutVerifyingIt(t *testing.T) {
	fixture := newStartFixture(t, "artifact-uploaded-exact-version", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	claimID := uuid.New()
	claim, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		claimID,
	)
	if err != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
		t.Fatalf("ClaimArtifactUpload = %#v error=%v", claim, err)
	}
	const multipartID = "minio-multipart-uploaded-01"
	if recorded, recordErr := fixture.service.RecordArtifactMultipartSession(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		claimID,
		multipartID,
	); recordErr != nil || recorded.Decision != workercontrol.ArtifactMultipartSessionRecorded {
		t.Fatalf("RecordArtifactMultipartSession = %#v error=%v", recorded, recordErr)
	}
	registry, err := artifactcleanup.NewPostgresRegistry(newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	))
	if err != nil {
		t.Fatalf("NewPostgresRegistry: %v", err)
	}
	registered, err := registry.IsMultipartUploadRecorded(
		context.Background(),
		claim.ObjectKey,
		multipartID,
	)
	if err != nil || !registered {
		t.Fatalf("registered multipart lookup = %t error=%v", registered, err)
	}
	registered, err = registry.IsMultipartUploadRecorded(
		context.Background(),
		claim.ObjectKey,
		"unrecorded-multipart-id",
	)
	if err != nil || registered {
		t.Fatalf("unrecorded multipart lookup = %t error=%v", registered, err)
	}

	payloadDigest := sha256.Sum256([]byte("immutable video object bytes"))
	report := workercontrol.ArtifactUploadReport{
		ObjectVersionID: "01J-immutable-object-version",
		SizeBytes:       4_194_304,
		SHA256:          payloadDigest,
		ContentType:     "video/mp4",
		CompletedParts: []workercontrol.ArtifactUploadPart{
			{Number: 1, ETag: "part-etag-1", SizeBytes: 4_194_304},
		},
	}
	intentReport := report
	intentReport.ObjectVersionID = ""
	intent, err := fixture.service.RecordArtifactCompletionIntent(
		context.Background(), fixture.worker, fixture.credentials, claim.UploadID, intentReport,
	)
	if err != nil || intent.Decision != workercontrol.ArtifactCompletionIntentRecorded ||
		intent.UploadID != claim.UploadID || intent.ArtifactID != claim.ArtifactID || intent.Version != 4 {
		t.Fatalf("record Artifact completion intent = %#v error=%v", intent, err)
	}
	replayedIntent, err := fixture.service.RecordArtifactCompletionIntent(
		context.Background(), fixture.worker, fixture.credentials, claim.UploadID, intentReport,
	)
	if err != nil || !reflect.DeepEqual(replayedIntent, intent) {
		t.Fatalf("replay Artifact completion intent = %#v error=%v, want %#v", replayedIntent, err, intent)
	}
	conflictingIntentReport := intentReport
	conflictingIntentReport.SHA256 = sha256.Sum256([]byte("different immutable video object bytes"))
	conflictingIntent, err := fixture.service.RecordArtifactCompletionIntent(
		context.Background(), fixture.worker, fixture.credentials, claim.UploadID, conflictingIntentReport,
	)
	if err != nil || conflictingIntent.Decision != workercontrol.ArtifactCompletionIntentConflict {
		t.Fatalf("conflicting Artifact completion intent = %#v error=%v", conflictingIntent, err)
	}
	first, err := fixture.service.RecordArtifactUploaded(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		report,
	)
	if err != nil {
		t.Fatalf("record uploaded Artifact: %v", err)
	}
	if first.Decision != workercontrol.ArtifactUploadRecorded ||
		first.UploadID != claim.UploadID ||
		first.ArtifactID != claim.ArtifactID ||
		first.ObjectVersionID != report.ObjectVersionID ||
		first.Version != 5 {
		t.Fatalf("recorded uploaded Artifact = %#v", first)
	}
	status, err := fixture.service.InspectArtifactUpload(
		context.Background(), fixture.worker, fixture.credentials, claim.UploadID,
	)
	if err != nil || status.Decision != workercontrol.ArtifactUploadStatusFound ||
		status.UploadID != claim.UploadID || status.ArtifactID != claim.ArtifactID ||
		status.ObjectKey != claim.ObjectKey || status.MultipartUploadID != multipartID ||
		status.ObjectVersionID != report.ObjectVersionID || status.SizeBytes != report.SizeBytes ||
		status.SHA256 != report.SHA256 || status.ContentType != report.ContentType ||
		!status.CompletionIntentRecorded || !reflect.DeepEqual(status.CompletedParts, report.CompletedParts) {
		t.Fatalf("inspect uploaded Artifact = %#v error=%v", status, err)
	}
	replayed, err := fixture.service.RecordArtifactUploaded(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		report,
	)
	if err != nil {
		t.Fatalf("replay uploaded Artifact: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed uploaded Artifact = %#v, want %#v", replayed, first)
	}

	conflictingReport := report
	conflictingReport.ObjectVersionID = "different-object-version"
	conflict, err := fixture.service.RecordArtifactUploaded(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		conflictingReport,
	)
	if err != nil {
		t.Fatalf("conflicting uploaded Artifact: %v", err)
	}
	if conflict.Decision != workercontrol.ArtifactUploadConflict {
		t.Fatalf("conflicting uploaded Artifact = %#v", conflict)
	}

	var (
		artifactState, uploadState, objectVersionID, contentType string
		sizeBytes                                                int64
		storedDigest                                             []byte
		version                                                  int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			a.state::text,
			u.state::text,
			a.object_version_id,
			a.size_bytes,
			a.sha256,
			a.content_type,
			u.version
		FROM artifacts AS a
		JOIN artifact_uploads AS u ON u.artifact_id = a.id
		WHERE u.id = $1
	`, claim.UploadID).Scan(
		&artifactState,
		&uploadState,
		&objectVersionID,
		&sizeBytes,
		&storedDigest,
		&contentType,
		&version,
	); err != nil {
		t.Fatalf("read uploaded Artifact identity: %v", err)
	}
	if artifactState != "UPLOADED" || uploadState != "UPLOADED" ||
		objectVersionID != report.ObjectVersionID || sizeBytes != report.SizeBytes ||
		!reflect.DeepEqual(storedDigest, report.SHA256[:]) ||
		contentType != report.ContentType || version != first.Version {
		t.Fatalf(
			"uploaded Artifact = states %s/%s version %q size %d digest %x type %q row version %d",
			artifactState,
			uploadState,
			objectVersionID,
			sizeBytes,
			storedDigest,
			contentType,
			version,
		)
	}
}

func TestArtifactVerificationInspectsExactVersionOutsidePublicationTransactionAndReplays(t *testing.T) {
	fixture := newStartFixture(t, "artifact-verification-exact-version", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	claimID := uuid.New()
	claim, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		claimID,
	)
	if err != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
		t.Fatalf("ClaimArtifactUpload = %#v error=%v", claim, err)
	}
	const multipartID = "minio-verify-exact-version"
	if recorded, recordErr := fixture.service.RecordArtifactMultipartSession(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		claimID,
		multipartID,
	); recordErr != nil || recorded.Decision != workercontrol.ArtifactMultipartSessionRecorded {
		t.Fatalf("RecordArtifactMultipartSession = %#v error=%v", recorded, recordErr)
	}
	payloadDigest := sha256.Sum256([]byte("verified immutable video bytes"))
	report := workercontrol.ArtifactUploadReport{
		ObjectVersionID: "01J-verified-object-version",
		SizeBytes:       8_388_608,
		SHA256:          payloadDigest,
		ContentType:     "video/mp4",
		CompletedParts: []workercontrol.ArtifactUploadPart{
			{Number: 1, ETag: "verified-part-etag", SizeBytes: 8_388_608},
		},
	}
	uploaded, err := fixture.service.RecordArtifactUploaded(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		report,
	)
	if err != nil || uploaded.Decision != workercontrol.ArtifactUploadRecorded {
		t.Fatalf("RecordArtifactUploaded = %#v error=%v", uploaded, err)
	}

	inspector := &recordingArtifactInspector{inspection: workercontrol.ArtifactInspection{
		ObjectVersionID:   report.ObjectVersionID,
		SizeBytes:         report.SizeBytes,
		SHA256:            report.SHA256,
		ContentType:       report.ContentType,
		Width:             1920,
		Height:            1080,
		DurationMillis:    5000,
		FrameRateMilli:    24000,
		FrameCount:        120,
		Codec:             "h264",
		Container:         "mp4",
		ValidatorRevision: "ffprobe-verified-v1",
	}}
	verificationPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	verificationService, err := workercontrol.NewService(
		context.Background(),
		verificationPool,
		workercontrol.Config{
			LeaseTTL:         2 * time.Minute,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
			ArtifactInspector: inspector,
		},
	)
	if err != nil {
		t.Fatalf("create verification coordinator: %v", err)
	}
	verificationID := uuid.New()
	first, err := verificationService.VerifyArtifact(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		verificationID,
	)
	if err != nil {
		t.Fatalf("verify Artifact: %v", err)
	}
	if first.Decision != workercontrol.ArtifactVerified ||
		first.UploadID != claim.UploadID ||
		first.ArtifactID != claim.ArtifactID ||
		first.VerificationID != verificationID ||
		first.ObjectVersionID != report.ObjectVersionID {
		t.Fatalf("verified Artifact = %#v", first)
	}
	if len(inspector.requests) != 1 {
		t.Fatalf("Artifact inspector requests = %d, want 1", len(inspector.requests))
	}
	request := inspector.requests[0]
	if request.ObjectKey != claim.ObjectKey ||
		request.ObjectVersionID != report.ObjectVersionID ||
		request.Kind != workercontrol.ArtifactKindVideo ||
		request.ExpectedWidth != 1920 || request.ExpectedHeight != 1080 ||
		request.ExpectedDurationMillis != 5000 || request.ExpectedFrameRateMilli != 24000 ||
		request.ExpectedFrameCount != 120 || request.ExpectedCodec != "h264" ||
		request.ExpectedContainer != "mp4" {
		t.Fatalf("Artifact inspection request = %#v", request)
	}

	replayed, err := verificationService.VerifyArtifact(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		verificationID,
	)
	if err != nil {
		t.Fatalf("replay Artifact verification: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) || len(inspector.requests) != 1 {
		t.Fatalf(
			"replayed Artifact verification = %#v, calls=%d, want %#v and one call",
			replayed,
			len(inspector.requests),
			first,
		)
	}

	var artifactState, uploadState string
	var receipt []byte
	if err := fixture.database.Admin.QueryRow(`
		SELECT a.state::text, u.state::text, a.validation_receipt
		FROM artifacts AS a
		JOIN artifact_uploads AS u ON u.artifact_id = a.id
		WHERE u.id = $1
	`, claim.UploadID).Scan(&artifactState, &uploadState, &receipt); err != nil {
		t.Fatalf("read verified Artifact: %v", err)
	}
	if artifactState != "VERIFIED" || uploadState != "VERIFIED" || len(receipt) == 0 {
		t.Fatalf(
			"verified Artifact state = %s/%s receipt=%s",
			artifactState,
			uploadState,
			receipt,
		)
	}
}

func TestArtifactValidationFailureKeepsUploadUnverifiedAndReleasesClaim(t *testing.T) {
	fixture := newStartFixture(t, "artifact-verification-rejects-mismatch", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	claimID := uuid.New()
	claim, err := fixture.service.ClaimArtifactUpload(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0].UploadID,
		claimID,
	)
	if err != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
		t.Fatalf("ClaimArtifactUpload = %#v error=%v", claim, err)
	}
	if recorded, recordErr := fixture.service.RecordArtifactMultipartSession(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		claimID,
		"minio-verification-mismatch",
	); recordErr != nil || recorded.Decision != workercontrol.ArtifactMultipartSessionRecorded {
		t.Fatalf("RecordArtifactMultipartSession = %#v error=%v", recorded, recordErr)
	}
	digest := sha256.Sum256([]byte("immutable video bytes for mismatch validation"))
	report := workercontrol.ArtifactUploadReport{
		ObjectVersionID: "01J-validation-mismatch-version",
		SizeBytes:       16_777_216,
		SHA256:          digest,
		ContentType:     "video/mp4",
		CompletedParts: []workercontrol.ArtifactUploadPart{
			{Number: 1, ETag: "validation-mismatch-etag", SizeBytes: 16_777_216},
		},
	}
	if uploaded, uploadErr := fixture.service.RecordArtifactUploaded(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		claim.UploadID,
		report,
	); uploadErr != nil || uploaded.Decision != workercontrol.ArtifactUploadRecorded {
		t.Fatalf("RecordArtifactUploaded = %#v error=%v", uploaded, uploadErr)
	}

	valid := workercontrol.ArtifactInspection{
		ObjectVersionID:   report.ObjectVersionID,
		SizeBytes:         report.SizeBytes,
		SHA256:            report.SHA256,
		ContentType:       report.ContentType,
		Width:             1920,
		Height:            1080,
		DurationMillis:    5000,
		FrameRateMilli:    24000,
		FrameCount:        120,
		Codec:             "h264",
		Container:         "mp4",
		ValidatorRevision: "ffprobe-validation-v1",
	}
	verificationPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	tests := []struct {
		name   string
		mutate func(*workercontrol.ArtifactInspection)
	}{
		{name: "object version", mutate: func(got *workercontrol.ArtifactInspection) { got.ObjectVersionID = "wrong-version" }},
		{name: "size", mutate: func(got *workercontrol.ArtifactInspection) { got.SizeBytes++ }},
		{name: "checksum", mutate: func(got *workercontrol.ArtifactInspection) { got.SHA256[0] ^= 0xff }},
		{name: "content type", mutate: func(got *workercontrol.ArtifactInspection) { got.ContentType = "video/webm" }},
		{name: "width", mutate: func(got *workercontrol.ArtifactInspection) { got.Width++ }},
		{name: "height", mutate: func(got *workercontrol.ArtifactInspection) { got.Height++ }},
		{name: "duration", mutate: func(got *workercontrol.ArtifactInspection) { got.DurationMillis++ }},
		{name: "frame rate", mutate: func(got *workercontrol.ArtifactInspection) { got.FrameRateMilli++ }},
		{name: "frame count", mutate: func(got *workercontrol.ArtifactInspection) { got.FrameCount++ }},
		{name: "codec", mutate: func(got *workercontrol.ArtifactInspection) { got.Codec = "av1" }},
		{name: "container", mutate: func(got *workercontrol.ArtifactInspection) { got.Container = "webm" }},
		{name: "validator revision", mutate: func(got *workercontrol.ArtifactInspection) { got.ValidatorRevision = "" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inspection := valid
			test.mutate(&inspection)
			inspector := &recordingArtifactInspector{inspection: inspection}
			service, serviceErr := workercontrol.NewService(
				context.Background(),
				verificationPool,
				workercontrol.Config{
					LeaseTTL:         2 * time.Minute,
					ActiveLeaseKeyID: "lease-key-v1",
					LeaseKeys: map[string][]byte{
						"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
					},
					ArtifactInspector: inspector,
				},
			)
			if serviceErr != nil {
				t.Fatalf("create verification coordinator: %v", serviceErr)
			}
			result, verifyErr := service.VerifyArtifact(
				context.Background(),
				fixture.worker,
				fixture.credentials,
				claim.UploadID,
				uuid.New(),
			)
			if verifyErr != nil || result.Decision != workercontrol.ArtifactValidationFailed {
				t.Fatalf("VerifyArtifact = %#v error=%v", result, verifyErr)
			}
			var artifactState, uploadState string
			var storedClaimID *uuid.UUID
			if queryErr := fixture.database.Admin.QueryRow(`
				SELECT artifact.state::text, upload.state::text, upload.claim_id
				FROM artifact_uploads AS upload
				JOIN artifacts AS artifact ON artifact.id = upload.artifact_id
				WHERE upload.id = $1
			`, claim.UploadID).Scan(&artifactState, &uploadState, &storedClaimID); queryErr != nil {
				t.Fatalf("read rejected verification state: %v", queryErr)
			}
			if artifactState != "UPLOADED" || uploadState != "UPLOADED" || storedClaimID != nil {
				t.Fatalf(
					"rejected verification state = %s/%s claim=%v",
					artifactState,
					uploadState,
					storedClaimID,
				)
			}
		})
	}
}

func TestCompleteVisibleCompletionAtomicallyPublishesAndReplays(t *testing.T) {
	fixture := newStartFixture(t, "visible-completion-atomic-publication", 7)
	webhookSubscriptionID := createTerminalWebhookSubscription(
		t, fixture.database, "job.succeeded",
	)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}

	verificationPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	verificationService, err := workercontrol.NewService(
		context.Background(),
		verificationPool,
		workercontrol.Config{
			LeaseTTL:         2 * time.Minute,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
			ArtifactInspector: artifactInspectorFunc(func(
				_ context.Context,
				request workercontrol.ArtifactInspectionRequest,
			) (workercontrol.ArtifactInspection, error) {
				return validInspectionForRequest(request), nil
			}),
		},
	)
	if err != nil {
		t.Fatalf("create verification coordinator: %v", err)
	}
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		verificationService,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID:       uuid.New(),
		ExpectedJobVersion: plan.JobVersion,
		ArtifactIDs:        artifactIDs,
	}
	first, err := verificationService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		candidate,
	)
	if err != nil {
		t.Fatalf("complete Visible Completion: %v", err)
	}
	if first.Decision != workercontrol.VisibleCompletionCommitted ||
		first.CompletionID != candidate.CompletionID ||
		first.JobID != fixture.assignment.JobID ||
		first.AttemptID != fixture.assignment.AttemptID ||
		first.ArtifactSetID == uuid.Nil || first.ChargeID == uuid.Nil ||
		first.JobVersion != plan.JobVersion+1 || first.CompletedAt.IsZero() ||
		first.ManifestSHA256 == [sha256.Size]byte{} || len(first.Artifacts) != len(plan.Artifacts) {
		t.Fatalf("Visible Completion = %#v", first)
	}

	var (
		jobState, attemptState, workerState, reservationState string
		jobVersion, projectRunning, reservedMinor             int64
		unsettledMinor, quoteMinor                            int64
		resultSetID, chargeSetID, completionSetID             uuid.UUID
		attemptEnded, leaseRevoked                            bool
		artifactSets, artifactItems, visibleCompletions       int
		committedArtifacts, accessGrants, charges             int
		successEvents, chargeEvents, invoiceEvents            int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			job.version,
			job.result_artifact_set_id,
			attempt.state::text,
			attempt.ended_at IS NOT NULL,
			lease.revoked_at IS NOT NULL,
			worker.lifecycle_state::text,
			project.running_count,
			reservation.state::text,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			job.pricing_quoted_amount_minor,
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_set_items WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id),
			(SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
			(SELECT count(*) FROM artifact_access_grants WHERE job_id = job.id AND revoked_at IS NULL),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'job.succeeded'),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'charge.posted'),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'invoice.export_requested'),
			(SELECT artifact_set_id FROM charges WHERE job_id = job.id),
			(SELECT artifact_set_id FROM visible_completions WHERE job_id = job.id)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $2
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN workers AS worker ON worker.id = attempt.worker_id
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
			ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, fixture.assignment.JobID, fixture.assignment.AttemptID).Scan(
		&jobState,
		&jobVersion,
		&resultSetID,
		&attemptState,
		&attemptEnded,
		&leaseRevoked,
		&workerState,
		&projectRunning,
		&reservationState,
		&reservedMinor,
		&unsettledMinor,
		&quoteMinor,
		&artifactSets,
		&artifactItems,
		&visibleCompletions,
		&committedArtifacts,
		&accessGrants,
		&charges,
		&successEvents,
		&chargeEvents,
		&invoiceEvents,
		&chargeSetID,
		&completionSetID,
	); err != nil {
		t.Fatalf("read Visible Completion business result: %v", err)
	}
	if jobState != "SUCCEEDED" || jobVersion != first.JobVersion ||
		resultSetID != first.ArtifactSetID || attemptState != "SUCCEEDED" ||
		!attemptEnded || !leaseRevoked || workerState != "READY" || projectRunning != 0 ||
		reservationState != "CONSUMED" || reservedMinor != 0 ||
		unsettledMinor != quoteMinor || artifactSets != 1 ||
		artifactItems != len(plan.Artifacts) || visibleCompletions != 1 ||
		committedArtifacts != len(plan.Artifacts) || accessGrants != 1 || charges != 1 ||
		successEvents != 1 || chargeEvents != 1 || invoiceEvents != 1 ||
		chargeSetID != first.ArtifactSetID || completionSetID != first.ArtifactSetID {
		t.Fatalf(
			"Visible Completion state = job %s/%d set %s attempt %s ended/revoked %t/%t worker %s running %d credit %s %d/%d quote %d rows set/items/completion/artifacts/access/charges %d/%d/%d/%d/%d/%d events %d/%d/%d links %s/%s",
			jobState,
			jobVersion,
			resultSetID,
			attemptState,
			attemptEnded,
			leaseRevoked,
			workerState,
			projectRunning,
			reservationState,
			reservedMinor,
			unsettledMinor,
			quoteMinor,
			artifactSets,
			artifactItems,
			visibleCompletions,
			committedArtifacts,
			accessGrants,
			charges,
			successEvents,
			chargeEvents,
			invoiceEvents,
			chargeSetID,
			completionSetID,
		)
	}
	assertVisibleCompletionEvents(t, fixture.database.Admin, first, quoteMinor)
	assertTerminalWebhookDelivery(
		t,
		fixture.database,
		webhookSubscriptionID,
		fixture.assignment.JobID,
		"job.succeeded",
		"SUCCEEDED",
	)

	replayed, err := verificationService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		candidate,
	)
	if err != nil {
		t.Fatalf("replay Visible Completion: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed Visible Completion = %#v, want %#v", replayed, first)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope after success: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate cancellation principal after success: %v", err)
	}
	canceled, err := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		verificationPool,
	).Cancel(
		context.Background(),
		principal,
		principal.ProjectID,
		fixture.assignment.JobID,
	)
	if err != nil || canceled.Decision != cancellation.DecisionAlreadySucceeded ||
		canceled.ArtifactSet == nil || canceled.ArtifactSet.ID != first.ArtifactSetID ||
		canceled.ArtifactSet.ManifestSHA256 != first.ManifestSHA256 ||
		len(canceled.ArtifactSet.Artifacts) != len(first.Artifacts) {
		t.Fatalf("cancellation after Visible Completion = %#v error=%v", canceled, err)
	}
}

func assertVisibleCompletionEvents(
	t *testing.T,
	db *sql.DB,
	completion workercontrol.VisibleCompletionResult,
	quoteMinor int64,
) {
	t.Helper()
	rows, err := db.Query(`
		SELECT event_type, payload
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type IN ('job.succeeded', 'charge.posted', 'invoice.export_requested')
		ORDER BY event_type
	`, completion.JobID)
	if err != nil {
		t.Fatalf("read Visible Completion events: %v", err)
	}
	defer rows.Close()
	seen := make(map[string]bool, 3)
	for rows.Next() {
		var eventType string
		var payload []byte
		if err := rows.Scan(&eventType, &payload); err != nil {
			t.Fatalf("scan Visible Completion event: %v", err)
		}
		var envelope velav1.EventEnvelope
		if err := proto.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("decode %s event: %v", eventType, err)
		}
		if envelope.GetEventId() == "" || envelope.GetAggregateType() != "Job" ||
			envelope.GetAggregateId() != completion.JobID.String() ||
			envelope.GetAggregateVersion() != uint64(completion.JobVersion) ||
			envelope.GetEventType() != eventType || envelope.GetSchemaVersion() != 1 ||
			!envelope.GetOccurredAt().AsTime().Equal(completion.CompletedAt) {
			t.Fatalf("%s EventEnvelope = %#v", eventType, &envelope)
		}
		seen[eventType] = true
		switch eventType {
		case "job.succeeded":
			succeeded := envelope.GetJobSucceeded()
			if succeeded == nil || succeeded.GetJobId() != completion.JobID.String() ||
				succeeded.GetAttemptId() != completion.AttemptID.String() ||
				succeeded.GetArtifactSetId() != completion.ArtifactSetID.String() ||
				succeeded.GetChargeId() != completion.ChargeID.String() ||
				succeeded.GetAttemptFence() == 0 ||
				!bytes.Equal(succeeded.GetManifestSha256(), completion.ManifestSHA256[:]) ||
				!succeeded.GetCompletedAt().AsTime().Equal(completion.CompletedAt) ||
				len(succeeded.GetArtifacts()) != len(completion.Artifacts) {
				t.Fatalf("job.succeeded payload = %#v", succeeded)
			}
			for index, artifact := range completion.Artifacts {
				snapshot := succeeded.GetArtifacts()[index]
				if snapshot.GetArtifactId() != artifact.ArtifactID.String() ||
					snapshot.GetKind() != string(artifact.Kind) ||
					snapshot.GetOrdinal() != uint32(artifact.Ordinal) ||
					snapshot.GetObjectKey() != artifact.ObjectKey ||
					snapshot.GetObjectVersionId() != artifact.ObjectVersionID ||
					snapshot.GetSizeBytes() != uint64(artifact.SizeBytes) ||
					!bytes.Equal(snapshot.GetSha256(), artifact.SHA256[:]) ||
					snapshot.GetContentType() != artifact.ContentType {
					t.Fatalf("job.succeeded Artifact %d = %#v", index, snapshot)
				}
			}
		case "charge.posted":
			charge := envelope.GetChargePosted()
			if charge == nil || charge.GetJobId() != completion.JobID.String() ||
				charge.GetChargeId() != completion.ChargeID.String() ||
				charge.GetCancellationId() != "" || charge.GetAmountMinor() != uint64(quoteMinor) ||
				charge.GetCurrency() != "CNY" || charge.GetReason() != "VISIBLE_COMPLETION" ||
				!charge.GetPostedAt().AsTime().Equal(completion.CompletedAt) {
				t.Fatalf("charge.posted payload = %#v", charge)
			}
		case "invoice.export_requested":
			invoice := envelope.GetInvoiceExportRequested()
			if invoice == nil || invoice.GetJobId() != completion.JobID.String() ||
				invoice.GetChargeId() != completion.ChargeID.String() ||
				!invoice.GetRequestedAt().AsTime().Equal(completion.CompletedAt) {
				t.Fatalf("invoice.export_requested payload = %#v", invoice)
			}
		default:
			t.Fatalf("unexpected Visible Completion event %q", eventType)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Visible Completion events: %v", err)
	}
	if len(seen) != 3 || !seen["job.succeeded"] || !seen["charge.posted"] ||
		!seen["invoice.export_requested"] {
		t.Fatalf("Visible Completion event set = %#v", seen)
	}
}

func TestProjectArtifactHTTPReturnsCommittedExactVersionsWithShortLivedURLs(t *testing.T) {
	fixture := newStartFixture(t, "artifact-http-exact-version", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	service := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	completed, err := service.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'artifacts:read')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant artifacts:read scope: %v", err)
	}

	signer := &recordingArtifactSigner{}
	authPool := newRolePool(
		t, fixture.database.DSN, "vela_auth_login", "vela-auth-password",
	)
	requestPool := newRolePool(
		t, fixture.database.DSN, "vela_request_login", "vela-request-password",
	)
	webhookRequestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_webhook_request_login",
		"vela-webhook-request-password",
	)
	artifactPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_artifact_request_login",
		"vela-artifact-request-password",
	)
	cancelPool := newRolePool(
		t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password",
	)
	internalPool := newRolePool(
		t, fixture.database.DSN, "vela_internal_login", "vela-internal-password",
	)
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              artifactaccess.NewService(artifactPool, signer),
		Webhooks:               testWebhookService(t, webhookRequestPool),
	})
	if err != nil {
		t.Fatalf("create Artifact HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	request, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/jobs/"+
			fixture.assignment.JobID.String()+"/artifacts",
		bytes.NewReader(nil),
	)
	if err != nil {
		t.Fatalf("create Artifact request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET committed ArtifactSet: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read committed ArtifactSet response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ArtifactSet status = %d, want 200; body=%s", response.StatusCode, body)
	}
	var payload struct {
		ArtifactSetID      uuid.UUID `json:"artifact_set_id"`
		JobID              uuid.UUID `json:"job_id"`
		RetentionExpiresAt time.Time `json:"retention_expires_at"`
		Artifacts          []struct {
			ArtifactID           uuid.UUID `json:"artifact_id"`
			Kind                 string    `json:"kind"`
			Ordinal              int32     `json:"ordinal"`
			ObjectVersionID      string    `json:"object_version_id"`
			SizeBytes            int64     `json:"size_bytes"`
			SHA256               string    `json:"sha256"`
			ContentType          string    `json:"content_type"`
			DownloadURL          string    `json:"download_url"`
			DownloadURLExpiresAt time.Time `json:"download_url_expires_at"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode committed ArtifactSet response: %v; body=%s", err, body)
	}
	if payload.ArtifactSetID != completed.ArtifactSetID ||
		payload.JobID != completed.JobID || payload.RetentionExpiresAt.IsZero() ||
		len(payload.Artifacts) != len(completed.Artifacts) || len(signer.calls) != len(completed.Artifacts) {
		t.Fatalf("ArtifactSet response = %#v signer calls=%#v", payload, signer.calls)
	}
	for index, artifact := range completed.Artifacts {
		got := payload.Artifacts[index]
		call := signer.calls[index]
		if got.ArtifactID != artifact.ArtifactID || got.Kind != string(artifact.Kind) ||
			got.Ordinal != artifact.Ordinal || got.ObjectVersionID != artifact.ObjectVersionID ||
			got.SizeBytes != artifact.SizeBytes || got.SHA256 == "" ||
			got.ContentType != artifact.ContentType || got.DownloadURL == "" ||
			got.DownloadURLExpiresAt.IsZero() || call.objectKey != artifact.ObjectKey ||
			call.versionID != artifact.ObjectVersionID {
			t.Fatalf("Artifact %d response=%#v signer=%#v want=%#v", index, got, call, artifact)
		}
	}
}

func TestArtifactHTTPHidesStagingRevokedAndExpiredContent(t *testing.T) {
	fixture := newStartFixture(t, "artifact-http-negative-lifecycle", 7)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'artifacts:read')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant artifacts:read scope: %v", err)
	}

	signer := &recordingArtifactSigner{}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator: identity.NewAuthenticator(
			newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
			testCredentialPepper,
		),
		IdentityAdministration: &identity.AdministrationService{},
		Admission: admission.NewLegacyService(
			newRolePool(t, fixture.database.DSN, "vela_request_login", "vela-request-password"),
		),
		Cancellation: cancellation.NewService(
			newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
			newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
		),
		Artifacts: artifactaccess.NewService(
			newRolePool(
				t,
				fixture.database.DSN,
				"vela_artifact_request_login",
				"vela-artifact-request-password",
			),
			signer,
		),
		Webhooks: testWebhookService(
			t,
			newRolePool(
				t,
				fixture.database.DSN,
				"vela_webhook_request_login",
				"vela-webhook-request-password",
			),
		),
	})
	if err != nil {
		t.Fatalf("create Artifact HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	artifactURL := server.URL + "/v1/projects/" + testProjectID + "/jobs/" +
		fixture.assignment.JobID.String() + "/artifacts"

	status, body := getArtifactsHTTP(t, artifactURL)
	if status != http.StatusNotFound || len(signer.calls) != 0 {
		t.Fatalf("staging Artifact HTTP = status %d body=%s signer=%#v", status, body, signer.calls)
	}

	service := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	completed, err := service.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}
	status, body = getArtifactsHTTP(t, artifactURL)
	if status != http.StatusOK || len(signer.calls) != len(plan.Artifacts) {
		t.Fatalf("committed Artifact HTTP = status %d body=%s signer=%#v", status, body, signer.calls)
	}
	signedBeforeNegativeCases := len(signer.calls)

	var eligibleAt, retentionExpiresAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT eligible_at, retention_expires_at
		FROM artifact_access_grants
		WHERE job_id = $1
	`, plan.JobID).Scan(&eligibleAt, &retentionExpiresAt); err != nil {
		t.Fatalf("read Artifact access grant lifetime: %v", err)
	}
	forceArtifactAccessGrantLifetime(
		t,
		fixture.database.Admin,
		plan.JobID,
		time.Now().Add(-48*time.Hour),
		time.Now().Add(-24*time.Hour),
	)
	status, body = getArtifactsHTTP(t, artifactURL)
	if status != http.StatusNotFound || len(signer.calls) != signedBeforeNegativeCases {
		t.Fatalf("expired Artifact HTTP = status %d body=%s signer=%#v", status, body, signer.calls)
	}
	forceArtifactAccessGrantLifetime(
		t,
		fixture.database.Admin,
		plan.JobID,
		eligibleAt,
		retentionExpiresAt,
	)
	if result, err := fixture.database.Admin.Exec(`
		UPDATE artifact_access_grants
		SET revoked_at = clock_timestamp()
		WHERE job_id = $1 AND revoked_at IS NULL
	`, plan.JobID); err != nil {
		t.Fatalf("revoke Artifact access grant: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("revoke Artifact access grant rows = %d error=%v", rows, rowsErr)
	}
	status, body = getArtifactsHTTP(t, artifactURL)
	if status != http.StatusNotFound || len(signer.calls) != signedBeforeNegativeCases {
		t.Fatalf("revoked Artifact HTTP = status %d body=%s signer=%#v", status, body, signer.calls)
	}
}

func getArtifactsHTTP(t *testing.T, artifactURL string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, artifactURL, bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("create Artifact HTTP request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET ArtifactSet: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read ArtifactSet response: %v", err)
	}
	return response.StatusCode, body
}

func forceArtifactAccessGrantLifetime(
	t *testing.T,
	db *sql.DB,
	jobID uuid.UUID,
	eligibleAt time.Time,
	retentionExpiresAt time.Time,
) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin Artifact access lifetime fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable Artifact access immutability trigger: %v", err)
	}
	if result, err := tx.Exec(`
		UPDATE artifact_access_grants
		SET eligible_at = $2, retention_expires_at = $3
		WHERE job_id = $1
	`, jobID, eligibleAt, retentionExpiresAt); err != nil {
		t.Fatalf("set Artifact access lifetime fixture: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("Artifact access lifetime fixture rows = %d error=%v", rows, rowsErr)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Artifact access lifetime fixture: %v", err)
	}
}

func TestArtifactAccessReauthorizesCredentialBeforeSigningAndHidesOtherProjects(t *testing.T) {
	fixture := newStartFixture(t, "artifact-access-reauthorization", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		completionService,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	completed, err := completionService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'artifacts:read')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant artifacts:read scope: %v", err)
	}
	authPool := newRolePool(
		t, fixture.database.DSN, "vela_auth_login", "vela-auth-password",
	)
	artifactPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_artifact_request_login",
		"vela-artifact-request-password",
	)
	authenticator := identity.NewAuthenticator(authPool, testCredentialPepper)
	signer := &recordingArtifactSigner{}
	accessService := artifactaccess.NewService(artifactPool, signer)

	principal, err := authenticator.Authenticate(context.Background(), testBearerCredential())
	if err != nil || !principal.HasScope(identity.ScopeArtifactsRead) {
		t.Fatalf("authenticate artifacts:read Principal = %#v error=%v", principal, err)
	}
	_, err = accessService.Get(
		context.Background(), principal, uuid.New(), fixture.assignment.JobID,
	)
	assertArtifactAccessFailure(t, err, artifactaccess.FailureNotFound)
	if len(signer.calls) != 0 {
		t.Fatalf("cross-Project Artifact read signed %d URLs", len(signer.calls))
	}

	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_remove(scopes, 'artifacts:read')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("remove artifacts:read scope after authentication: %v", err)
	}
	_, err = accessService.Get(
		context.Background(), principal, uuid.MustParse(testProjectID), fixture.assignment.JobID,
	)
	assertArtifactAccessFailure(t, err, artifactaccess.FailureForbidden)

	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'artifacts:read'),
			expires_at = clock_timestamp() + interval '1 day'
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("restore active artifacts:read credential: %v", err)
	}
	principal, err = authenticator.Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate before credential expiry: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("expire credential after authentication: %v", err)
	}
	_, err = accessService.Get(
		context.Background(), principal, uuid.MustParse(testProjectID), fixture.assignment.JobID,
	)
	assertArtifactAccessFailure(t, err, artifactaccess.FailureUnauthorized)

	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET expires_at = clock_timestamp() + interval '1 day', revoked_at = NULL
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("restore credential before revocation case: %v", err)
	}
	principal, err = authenticator.Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate before credential revocation: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET revoked_at = clock_timestamp()
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("revoke credential after authentication: %v", err)
	}
	_, err = accessService.Get(
		context.Background(), principal, uuid.MustParse(testProjectID), fixture.assignment.JobID,
	)
	assertArtifactAccessFailure(t, err, artifactaccess.FailureUnauthorized)
	if len(signer.calls) != 0 {
		t.Fatalf("unauthorized Artifact reads signed %d URLs", len(signer.calls))
	}
}

func assertArtifactAccessFailure(
	t *testing.T,
	err error,
	want artifactaccess.FailureCode,
) {
	t.Helper()
	var failure *artifactaccess.Failure
	if !errors.As(err, &failure) || failure.Code != want {
		t.Fatalf("Artifact access error = %v, want %s", err, want)
	}
}

func TestVisibleCompletionSerializesWithFinalizationAuthorityRaces(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		competitor string
	}{
		{
			name:       "Job Expiry",
			key:        "visible-completion-job-expiry-race",
			competitor: "job-expiry",
		},
		{
			name:       "Fail",
			key:        "visible-completion-fail-race",
			competitor: "fail",
		},
		{
			name:       "Heartbeat",
			key:        "visible-completion-heartbeat-race",
			competitor: "heartbeat",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStartFixture(t, test.key, 7)
			service := visibleCompletionService(t, fixture.database.DSN)
			if started, err := fixture.service.Start(
				context.Background(), fixture.worker, fixture.credentials,
			); err != nil || started.Decision != workercontrol.StartGranted {
				t.Fatalf("Start = %#v error=%v", started, err)
			}
			plan, err := service.BeginFinalization(
				context.Background(), fixture.worker, fixture.credentials,
			)
			if err != nil || plan.Decision != workercontrol.FinalizationGranted {
				t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
			}
			artifactIDs := uploadAndVerifyFinalizationPlan(
				t,
				service,
				fixture.worker,
				fixture.credentials,
				plan,
			)
			candidate := workercontrol.VisibleCompletionCandidate{
				CompletionID:       uuid.New(),
				ExpectedJobVersion: plan.JobVersion,
				ArtifactIDs:        artifactIDs,
			}
			if test.competitor == "job-expiry" {
				forceFinalizationFailureWindow(
					t,
					fixture.database.Admin,
					plan.JobID,
					plan.AttemptID,
					true,
					false,
				)
			}

			type completionCall struct {
				result workercontrol.VisibleCompletionResult
				err    error
			}
			type authorityCall struct {
				heartbeat      workercontrol.HeartbeatResult
				failure        workercontrol.RetryDecision
				reconciliation workercontrol.ReconciliationResult
				err            error
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			start := make(chan struct{})
			completionResult := make(chan completionCall, 1)
			authorityResult := make(chan authorityCall, 1)
			var wait sync.WaitGroup
			wait.Add(2)
			go func() {
				defer wait.Done()
				<-start
				result, completeErr := service.CompleteVisibleCompletion(
					ctx,
					fixture.worker,
					fixture.credentials,
					candidate,
				)
				completionResult <- completionCall{result: result, err: completeErr}
			}()
			go func() {
				defer wait.Done()
				<-start
				call := authorityCall{}
				switch test.competitor {
				case "job-expiry":
					call.reconciliation, call.err = service.ReconcileNextExecutionFailure(ctx)
				case "fail":
					call.failure, call.err = service.Fail(
						ctx,
						fixture.worker,
						fixture.credentials,
						validFailureObservation(),
					)
				case "heartbeat":
					heartbeat := validHeartbeatObservation(1)
					heartbeat.BackendStage = "artifact-upload"
					heartbeat.LocalArtifactState = json.RawMessage(
						`{"state":"verified","completed":2,"total":2}`,
					)
					call.heartbeat, call.err = service.Heartbeat(
						ctx,
						fixture.worker,
						fixture.credentials,
						heartbeat,
					)
				default:
					call.err = fmt.Errorf("unknown finalization authority competitor %q", test.competitor)
				}
				authorityResult <- call
			}()
			close(start)
			done := make(chan struct{})
			go func() {
				wait.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatalf("Visible Completion/%s race deadlocked: %v", test.competitor, ctx.Err())
			}
			completed := <-completionResult
			authority := <-authorityResult
			if completed.err != nil || authority.err != nil {
				t.Fatalf(
					"Visible Completion/%s race errors = %v / %v",
					test.competitor,
					completed.err,
					authority.err,
				)
			}

			switch test.competitor {
			case "job-expiry":
				if completed.result.Decision != workercontrol.VisibleCompletionRejectedStaleLease &&
					completed.result.Decision != workercontrol.VisibleCompletionAlreadyFailed {
					t.Fatalf("late Visible Completion after Job Expiry = %#v", completed.result)
				}
				if !authority.reconciliation.Processed ||
					authority.reconciliation.Source != workercontrol.FailureSourceJobExpired ||
					authority.reconciliation.Decision.Disposition != workercontrol.RetryDispositionFailed {
					t.Fatalf("Job Expiry winner = %#v", authority.reconciliation)
				}
				state := readFinalizationFailureState(
					t,
					fixture.database.Admin,
					plan.JobID,
					plan.AttemptID,
				)
				if state.JobState != "FAILED" || state.AttemptState != "FAILED" ||
					!state.AttemptEnded || !state.LeaseRevoked ||
					state.ProjectRunning != 0 || state.ProjectRetryWait != 0 ||
					state.ReservationState != "RELEASED" || state.ReservedMinor != 0 ||
					state.Charges != 0 || state.Decisions != 1 || state.FailedEvents != 1 {
					t.Fatalf("Job Expiry race state = %#v", state)
				}
			case "fail":
				if completed.result.Decision != workercontrol.VisibleCompletionCommitted ||
					authority.failure.Disposition != workercontrol.RetryDispositionRejectedStaleLease {
					t.Fatalf(
						"Visible Completion/Fail decisions = %s / %s",
						completed.result.Decision,
						authority.failure.Disposition,
					)
				}
				assertSuccessfulVisibleCompletionRaceState(t, fixture, plan)
			case "heartbeat":
				if completed.result.Decision != workercontrol.VisibleCompletionCommitted ||
					(authority.heartbeat.Decision != workercontrol.HeartbeatContinue &&
						authority.heartbeat.Decision != workercontrol.HeartbeatStop) {
					t.Fatalf(
						"Visible Completion/Heartbeat decisions = %s / %#v",
						completed.result.Decision,
						authority.heartbeat,
					)
				}
				assertSuccessfulVisibleCompletionRaceState(t, fixture, plan)
			}
		})
	}
}

func assertSuccessfulVisibleCompletionRaceState(
	t *testing.T,
	fixture startFixture,
	plan workercontrol.FinalizationPlan,
) {
	t.Helper()
	state := readVisibleCompletionMutationState(
		t,
		fixture.database.Admin,
		plan.JobID,
		plan.AttemptID,
	)
	if state.JobState != "SUCCEEDED" || state.JobVersion != plan.JobVersion+1 ||
		state.ResultArtifactSetID == nil || state.AttemptState != "SUCCEEDED" ||
		!state.AttemptEnded || !state.LeaseRevoked || state.WorkerState != "READY" ||
		state.ProjectRunning != 0 || state.ReservationState != "CONSUMED" ||
		state.ReservedMinor != 0 || state.UnsettledPostedMinor != 1250 ||
		state.VerifiedArtifacts != 0 || state.CommittedArtifacts != len(plan.Artifacts) ||
		state.ArtifactSets != 1 || state.ArtifactSetItems != len(plan.Artifacts) ||
		state.VisibleCompletions != 1 || state.AccessGrants != 1 || state.Charges != 1 ||
		state.TerminalEvents != 3 {
		t.Fatalf("successful finalization authority race state = %#v", state)
	}
	var failureDecisions int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*)
		FROM execution_failure_decisions
		WHERE attempt_id = $1
	`, plan.AttemptID).Scan(&failureDecisions); err != nil {
		t.Fatalf("read successful race failure decisions: %v", err)
	}
	if failureDecisions != 0 {
		t.Fatalf("successful finalization authority race recorded %d failures", failureDecisions)
	}
}

func TestVisibleCompletionAndCustomerCancellationHaveOneChargeWinner(t *testing.T) {
	fixture := newStartFixture(t, "visible-completion-cancellation-race", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	internalPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	completionService, err := workercontrol.NewService(
		context.Background(),
		internalPool,
		workercontrol.Config{
			LeaseTTL:         2 * time.Minute,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
			ArtifactInspector: artifactInspectorFunc(func(
				_ context.Context,
				request workercontrol.ArtifactInspectionRequest,
			) (workercontrol.ArtifactInspection, error) {
				return validInspectionForRequest(request), nil
			}),
		},
	)
	if err != nil {
		t.Fatalf("create completion coordinator: %v", err)
	}
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		completionService,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID:       uuid.New(),
		ExpectedJobVersion: plan.JobVersion,
		ArtifactIDs:        artifactIDs,
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	authPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_auth_login",
		"vela-auth-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(),
		testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate cancellation principal: %v", err)
	}
	cancelService := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		internalPool,
	)

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var completionResult workercontrol.VisibleCompletionResult
	var completionErr error
	go func() {
		defer wait.Done()
		<-start
		completionResult, completionErr = completionService.CompleteVisibleCompletion(
			context.Background(),
			fixture.worker,
			fixture.credentials,
			candidate,
		)
	}()
	var cancellationResult cancellation.Result
	var cancellationErr error
	go func() {
		defer wait.Done()
		<-start
		cancellationResult, cancellationErr = cancelService.Cancel(
			context.Background(),
			principal,
			principal.ProjectID,
			fixture.assignment.JobID,
		)
	}()
	close(start)
	wait.Wait()
	if completionErr != nil || cancellationErr != nil {
		t.Fatalf(
			"Visible Completion/cancellation race errors = %v / %v",
			completionErr,
			cancellationErr,
		)
	}
	completionWon := completionResult.Decision == workercontrol.VisibleCompletionCommitted &&
		cancellationResult.Decision == cancellation.DecisionAlreadySucceeded
	cancellationWon := completionResult.Decision == workercontrol.VisibleCompletionCancellationWon &&
		cancellationResult.Decision == cancellation.DecisionCanceling
	if completionWon == cancellationWon {
		t.Fatalf(
			"Visible Completion/cancellation decisions = %s / %s",
			completionResult.Decision,
			cancellationResult.Decision,
		)
	}

	var jobState, chargeReason, reservationState string
	var artifactSets, visibleCompletions, cancellationDecisions, charges int
	var reservedMinor, unsettledMinor, quoteMinor int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT reason::text FROM charges WHERE job_id = job.id),
			reservation.state::text,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			job.pricing_quoted_amount_minor
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
			ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&artifactSets,
		&visibleCompletions,
		&cancellationDecisions,
		&charges,
		&chargeReason,
		&reservationState,
		&reservedMinor,
		&unsettledMinor,
		&quoteMinor,
	); err != nil {
		t.Fatalf("read completion/cancellation winner: %v", err)
	}
	if charges != 1 || reservationState != "CONSUMED" || reservedMinor != 0 ||
		unsettledMinor != quoteMinor {
		t.Fatalf(
			"winner ledger = charges %d reservation %s credit %d/%d quote %d",
			charges,
			reservationState,
			reservedMinor,
			unsettledMinor,
			quoteMinor,
		)
	}
	if completionWon && (jobState != "SUCCEEDED" || artifactSets != 1 ||
		visibleCompletions != 1 || cancellationDecisions != 0 ||
		chargeReason != "VISIBLE_COMPLETION") {
		t.Fatalf(
			"completion winner state = %s sets/completions/cancellations %d/%d/%d reason %s",
			jobState,
			artifactSets,
			visibleCompletions,
			cancellationDecisions,
			chargeReason,
		)
	}
	if cancellationWon && (jobState != "CANCELING" || artifactSets != 0 ||
		visibleCompletions != 0 || cancellationDecisions != 1 ||
		chargeReason != "CUSTOMER_CANCELLATION") {
		t.Fatalf(
			"cancellation winner state = %s sets/completions/cancellations %d/%d/%d reason %s",
			jobState,
			artifactSets,
			visibleCompletions,
			cancellationDecisions,
			chargeReason,
		)
	}
}

func TestConcurrentVisibleCompletionCommitsOneImmutableResult(t *testing.T) {
	fixture := newStartFixture(t, "concurrent-visible-completion", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	service := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID:       uuid.New(),
		ExpectedJobVersion: plan.JobVersion,
		ArtifactIDs:        artifactIDs,
	}

	start := make(chan struct{})
	results := make(chan workercontrol.VisibleCompletionResult, 2)
	errorsByCall := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, completeErr := service.CompleteVisibleCompletion(
				context.Background(),
				fixture.worker,
				fixture.credentials,
				candidate,
			)
			results <- result
			errorsByCall <- completeErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsByCall)
	for callErr := range errorsByCall {
		if callErr != nil {
			t.Fatalf("concurrent Visible Completion error: %v", callErr)
		}
	}
	var first *workercontrol.VisibleCompletionResult
	for result := range results {
		if result.Decision != workercontrol.VisibleCompletionCommitted {
			t.Fatalf("concurrent Visible Completion decision = %s", result.Decision)
		}
		if first == nil {
			copyResult := result
			first = &copyResult
			continue
		}
		if !reflect.DeepEqual(result, *first) {
			t.Fatalf("concurrent Visible Completion results = %#v / %#v", *first, result)
		}
	}
	if first == nil {
		t.Fatal("concurrent Visible Completion returned no result")
	}

	differentCompletion, err := service.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || differentCompletion.Decision != workercontrol.VisibleCompletionAlreadySucceeded ||
		differentCompletion.ArtifactSetID != first.ArtifactSetID {
		t.Fatalf("different completion after success = %#v error=%v", differentCompletion, err)
	}
	conflictingCandidate, err := service.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       candidate.CompletionID,
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs[:1],
		},
	)
	if err != nil || conflictingCandidate.Decision != workercontrol.VisibleCompletionCandidateConflict ||
		conflictingCandidate.ArtifactSetID != first.ArtifactSetID {
		t.Fatalf("conflicting completion replay = %#v error=%v", conflictingCandidate, err)
	}

	var sets, completions, charges, succeededEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM artifact_sets WHERE job_id = $1),
			(SELECT count(*) FROM visible_completions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'job.succeeded')
	`, fixture.assignment.JobID).Scan(&sets, &completions, &charges, &succeededEvents); err != nil {
		t.Fatalf("read concurrent Visible Completion counts: %v", err)
	}
	if sets != 1 || completions != 1 || charges != 1 || succeededEvents != 1 {
		t.Fatalf(
			"concurrent Visible Completion counts = %d/%d/%d/%d",
			sets,
			completions,
			charges,
			succeededEvents,
		)
	}
}

func TestIncompleteVisibleCompletionRollsBackEveryPublicationWrite(t *testing.T) {
	fixture := newStartFixture(t, "incomplete-visible-completion", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	service := visibleCompletionService(t, fixture.database.DSN)
	uploadAndVerifyOneArtifact(
		t,
		service,
		fixture.worker,
		fixture.credentials,
		plan.Artifacts[0],
		0,
	)
	artifactIDs := make([]uuid.UUID, 0, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
	}
	result, err := service.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || result.Decision != workercontrol.VisibleCompletionIncompleteArtifact {
		t.Fatalf("incomplete Visible Completion = %#v error=%v", result, err)
	}

	var jobState, attemptState, reservationState string
	var jobVersion, projectRunning int64
	var leaseRevoked bool
	var sets, items, completions, committedArtifacts, accessGrants, charges, terminalEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			job.version,
			attempt.state::text,
			lease.revoked_at IS NOT NULL,
			project.running_count,
			reservation.state::text,
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_set_items WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id),
			(SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
			(SELECT count(*) FROM artifact_access_grants WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type IN (
				'job.succeeded', 'charge.posted', 'invoice.export_requested'
			))
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $2
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, fixture.assignment.JobID, fixture.assignment.AttemptID).Scan(
		&jobState,
		&jobVersion,
		&attemptState,
		&leaseRevoked,
		&projectRunning,
		&reservationState,
		&sets,
		&items,
		&completions,
		&committedArtifacts,
		&accessGrants,
		&charges,
		&terminalEvents,
	); err != nil {
		t.Fatalf("read incomplete Visible Completion state: %v", err)
	}
	if jobState != "FINALIZING" || jobVersion != plan.JobVersion ||
		attemptState != "FINALIZING" || leaseRevoked || projectRunning != 1 ||
		reservationState != "RESERVED" || sets != 0 || items != 0 || completions != 0 ||
		committedArtifacts != 0 || accessGrants != 0 || charges != 0 || terminalEvents != 0 {
		t.Fatalf(
			"incomplete publication = job %s/%d attempt %s revoked %t running %d reservation %s rows %d/%d/%d/%d/%d/%d events %d",
			jobState,
			jobVersion,
			attemptState,
			leaseRevoked,
			projectRunning,
			reservationState,
			sets,
			items,
			completions,
			committedArtifacts,
			accessGrants,
			charges,
			terminalEvents,
		)
	}
}

func TestVisibleCompletionRollsBackEveryTransactionalWriteBoundary(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		action    string
		condition string
	}{
		{name: "ArtifactSet", table: "artifact_sets", action: "INSERT"},
		{name: "ArtifactSet item", table: "artifact_set_items", action: "INSERT"},
		{name: "Artifact commit", table: "artifacts", action: "UPDATE"},
		{name: "Charge", table: "charges", action: "INSERT"},
		{name: "access eligibility", table: "artifact_access_grants", action: "INSERT"},
		{name: "Visible Completion", table: "visible_completions", action: "INSERT"},
		{name: "Attempt", table: "attempts", action: "UPDATE"},
		{name: "Lease", table: "attempt_leases", action: "UPDATE"},
		{name: "Worker", table: "workers", action: "UPDATE"},
		{name: "Project counter", table: "projects", action: "UPDATE"},
		{name: "CreditReservation", table: "credit_reservations", action: "UPDATE"},
		{name: "Organization credit", table: "organization_credit_accounts", action: "UPDATE"},
		{
			name:      "job.succeeded Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'job.succeeded'",
		},
		{
			name:      "charge.posted Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'charge.posted'",
		},
		{
			name:      "invoice.export_requested Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'invoice.export_requested'",
		},
		{name: "Job", table: "jobs", action: "UPDATE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStartFixture(t, "visible-completion-rollback-"+test.table, 7)
			started, err := fixture.service.Start(
				context.Background(), fixture.worker, fixture.credentials,
			)
			if err != nil || started.Decision != workercontrol.StartGranted {
				t.Fatalf("Start = %#v error=%v", started, err)
			}
			plan, err := fixture.service.BeginFinalization(
				context.Background(), fixture.worker, fixture.credentials,
			)
			if err != nil || plan.Decision != workercontrol.FinalizationGranted {
				t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
			}
			service := visibleCompletionService(t, fixture.database.DSN)
			artifactIDs := uploadAndVerifyFinalizationPlan(
				t,
				service,
				fixture.worker,
				fixture.credentials,
				plan,
			)
			before := readVisibleCompletionMutationState(
				t,
				fixture.database.Admin,
				fixture.assignment.JobID,
				fixture.assignment.AttemptID,
			)
			whenClause := ""
			if test.condition != "" {
				whenClause = "WHEN (" + test.condition + ")"
			}
			if _, err := fixture.database.Admin.Exec(fmt.Sprintf(`
				CREATE FUNCTION vela_test_reject_visible_completion_write() RETURNS trigger
				LANGUAGE plpgsql AS $$
				BEGIN
					RAISE EXCEPTION 'injected Visible Completion write rejection';
				END
				$$;
				CREATE TRIGGER vela_test_reject_visible_completion_write
				BEFORE %s ON %s
				FOR EACH ROW %s EXECUTE FUNCTION vela_test_reject_visible_completion_write();
			`, test.action, test.table, whenClause)); err != nil {
				t.Fatalf("install %s rejection trigger: %v", test.name, err)
			}
			result, completeErr := service.CompleteVisibleCompletion(
				context.Background(),
				fixture.worker,
				fixture.credentials,
				workercontrol.VisibleCompletionCandidate{
					CompletionID:       uuid.New(),
					ExpectedJobVersion: plan.JobVersion,
					ArtifactIDs:        artifactIDs,
				},
			)
			if completeErr == nil || result.Decision != "" {
				t.Fatalf(
					"Visible Completion with rejected %s write = %#v error=%v",
					test.name,
					result,
					completeErr,
				)
			}
			after := readVisibleCompletionMutationState(
				t,
				fixture.database.Admin,
				fixture.assignment.JobID,
				fixture.assignment.AttemptID,
			)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf(
					"rejected %s write committed partial Visible Completion: before=%#v after=%#v",
					test.name,
					before,
					after,
				)
			}
		})
	}
}

func plannedArtifactIDs(plan workercontrol.FinalizationPlan) []uuid.UUID {
	artifactIDs := make([]uuid.UUID, 0, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
	}
	return artifactIDs
}

func verifyAndCommitVisibleCompletionAsReconciler(
	t *testing.T,
	service *workercontrol.Service,
	db *sql.DB,
	reconciler workercontrol.AuthenticatedReconciler,
	credentials workercontrol.ReconcilerFinalizationCredentials,
	plan workercontrol.FinalizationPlan,
	candidate workercontrol.VisibleCompletionCandidate,
) ([]workercontrol.ArtifactVerificationResult, workercontrol.VisibleCompletionResult) {
	t.Helper()
	verifications := make([]workercontrol.ArtifactVerificationResult, 0, len(plan.Artifacts))
	for index, artifact := range plan.Artifacts {
		verified, err := service.VerifyArtifactAsReconciler(
			context.Background(),
			reconciler,
			credentials,
			artifact.UploadID,
			uuid.New(),
		)
		if err != nil || verified.Decision != workercontrol.ArtifactVerified ||
			verified.ArtifactID != artifact.ArtifactID {
			t.Fatalf("Reconciler verify Artifact %d = %#v error=%v", index, verified, err)
		}
		verifications = append(verifications, verified)
	}
	completed, err := service.CompleteVisibleCompletionAsReconciler(
		context.Background(), reconciler, credentials, candidate,
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted ||
		completed.AttemptID != plan.AttemptID || completed.JobID != plan.JobID ||
		completed.JobVersion != plan.JobVersion+1 || completed.ArtifactSetID == uuid.Nil ||
		completed.ChargeID == uuid.Nil {
		t.Fatalf("Reconciler Visible Completion = %#v error=%v", completed, err)
	}
	state := readVisibleCompletionMutationState(t, db, plan.JobID, plan.AttemptID)
	if state.JobState != "SUCCEEDED" || state.JobVersion != plan.JobVersion+1 ||
		state.ResultArtifactSetID == nil || *state.ResultArtifactSetID != completed.ArtifactSetID ||
		state.AttemptState != "SUCCEEDED" || !state.AttemptEnded || !state.LeaseRevoked ||
		state.WorkerState != "DRAINING" || state.ProjectRunning != 0 ||
		state.ReservationState != "CONSUMED" || state.ReservedMinor != 0 ||
		state.UnsettledPostedMinor != 1250 || state.CommittedArtifacts != len(plan.Artifacts) ||
		state.ArtifactSets != 1 || state.ArtifactSetItems != len(plan.Artifacts) ||
		state.VisibleCompletions != 1 || state.AccessGrants != 1 || state.Charges != 1 ||
		state.TerminalEvents != 3 {
		t.Fatalf("Reconciler Visible Completion state = %#v", state)
	}
	return verifications, completed
}

type visibleCompletionMutationState struct {
	JobState             string
	JobVersion           int64
	ResultArtifactSetID  *uuid.UUID
	AttemptState         string
	AttemptEnded         bool
	LeaseRevoked         bool
	WorkerState          string
	ProjectRunning       int64
	ReservationState     string
	ReservedMinor        int64
	UnsettledPostedMinor int64
	VerifiedArtifacts    int
	CommittedArtifacts   int
	ArtifactSets         int
	ArtifactSetItems     int
	VisibleCompletions   int
	AccessGrants         int
	Charges              int
	TerminalEvents       int
}

func readVisibleCompletionMutationState(
	t *testing.T,
	db *sql.DB,
	jobID, attemptID uuid.UUID,
) visibleCompletionMutationState {
	t.Helper()
	var state visibleCompletionMutationState
	if err := db.QueryRow(`
		SELECT
			job.state::text,
			job.version,
			job.result_artifact_set_id,
			attempt.state::text,
			attempt.ended_at IS NOT NULL,
			lease.revoked_at IS NOT NULL,
			worker.lifecycle_state::text,
			project.running_count,
			reservation.state::text,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			(SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'VERIFIED'),
			(SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_set_items WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_access_grants WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type IN (
				'job.succeeded', 'charge.posted', 'invoice.export_requested'
			))
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $2
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN workers AS worker ON worker.id = attempt.worker_id
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
			ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, jobID, attemptID).Scan(
		&state.JobState,
		&state.JobVersion,
		&state.ResultArtifactSetID,
		&state.AttemptState,
		&state.AttemptEnded,
		&state.LeaseRevoked,
		&state.WorkerState,
		&state.ProjectRunning,
		&state.ReservationState,
		&state.ReservedMinor,
		&state.UnsettledPostedMinor,
		&state.VerifiedArtifacts,
		&state.CommittedArtifacts,
		&state.ArtifactSets,
		&state.ArtifactSetItems,
		&state.VisibleCompletions,
		&state.AccessGrants,
		&state.Charges,
		&state.TerminalEvents,
	); err != nil {
		t.Fatalf("read Visible Completion mutation state: %v", err)
	}
	return state
}

type artifactInspectorFunc func(
	context.Context,
	workercontrol.ArtifactInspectionRequest,
) (workercontrol.ArtifactInspection, error)

type unexpectedArtifactCompletionStore struct{}

func (*unexpectedArtifactCompletionStore) ListParts(
	context.Context,
	artifactstore.MultipartUpload,
) ([]artifactstore.CompletedPart, error) {
	return nil, errors.New("unexpected multipart recovery")
}

func (*unexpectedArtifactCompletionStore) CompleteMultipartUpload(
	context.Context,
	artifactstore.MultipartUpload,
	[]artifactstore.CompletedPart,
) (artifactstore.ObjectVersion, error) {
	return artifactstore.ObjectVersion{}, errors.New("unexpected multipart recovery")
}

func (*unexpectedArtifactCompletionStore) HeadCurrentVersion(
	context.Context,
	string,
) (artifactstore.ObjectVersion, error) {
	return artifactstore.ObjectVersion{}, errors.New("unexpected multipart recovery")
}

type artifactSignerCall struct {
	objectKey string
	versionID string
}

type recordingArtifactSigner struct {
	calls []artifactSignerCall
}

func (signer *recordingArtifactSigner) PresignExactVersion(
	_ context.Context,
	objectKey string,
	versionID string,
) (artifactstore.SignedRead, error) {
	issuedAt := time.Now().UTC()
	signer.calls = append(signer.calls, artifactSignerCall{
		objectKey: objectKey,
		versionID: versionID,
	})
	return artifactstore.SignedRead{
		URL:       "https://download.invalid/exact/" + versionID,
		IssuedAt:  issuedAt,
		ExpiresAt: issuedAt.Add(15 * time.Minute),
	}, nil
}

func testArtifactAccessService(pool *pgxpool.Pool) *artifactaccess.Service {
	return artifactaccess.NewService(pool, &recordingArtifactSigner{})
}

func (inspect artifactInspectorFunc) Inspect(
	ctx context.Context,
	request workercontrol.ArtifactInspectionRequest,
) (workercontrol.ArtifactInspection, error) {
	return inspect(ctx, request)
}

func validInspectionForRequest(
	request workercontrol.ArtifactInspectionRequest,
) workercontrol.ArtifactInspection {
	return workercontrol.ArtifactInspection{
		ObjectVersionID:   request.ObjectVersionID,
		SizeBytes:         request.ExpectedSizeBytes,
		SHA256:            request.ExpectedSHA256,
		ContentType:       request.ExpectedContentType,
		Width:             request.ExpectedWidth,
		Height:            request.ExpectedHeight,
		DurationMillis:    request.ExpectedDurationMillis,
		FrameRateMilli:    request.ExpectedFrameRateMilli,
		FrameCount:        request.ExpectedFrameCount,
		Codec:             request.ExpectedCodec,
		Container:         request.ExpectedContainer,
		ValidatorRevision: "ffprobe-visible-completion-v1",
	}
}

func visibleCompletionService(t *testing.T, dsn string) *workercontrol.Service {
	t.Helper()
	service, err := workercontrol.NewService(
		context.Background(),
		newRolePool(t, dsn, "vela_internal_login", "vela-internal-password"),
		workercontrol.Config{
			LeaseTTL:         2 * time.Minute,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
			ArtifactInspector: artifactInspectorFunc(func(
				_ context.Context,
				request workercontrol.ArtifactInspectionRequest,
			) (workercontrol.ArtifactInspection, error) {
				return validInspectionForRequest(request), nil
			}),
		},
	)
	if err != nil {
		t.Fatalf("create Visible Completion coordinator: %v", err)
	}
	return service
}

func uploadAndVerifyFinalizationPlan(
	t *testing.T,
	service *workercontrol.Service,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	plan workercontrol.FinalizationPlan,
) []uuid.UUID {
	t.Helper()
	artifactIDs := make([]uuid.UUID, 0, len(plan.Artifacts))
	for index, artifact := range plan.Artifacts {
		uploadAndVerifyOneArtifact(t, service, worker, credentials, artifact, index)
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
	}
	return artifactIDs
}

func uploadFinalizationPlanWithoutVerification(
	t *testing.T,
	service *workercontrol.Service,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	plan workercontrol.FinalizationPlan,
) []uuid.UUID {
	t.Helper()
	artifactIDs := make([]uuid.UUID, 0, len(plan.Artifacts))
	for index, artifact := range plan.Artifacts {
		uploadOneArtifact(t, service, worker, credentials, artifact, index, false)
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
	}
	return artifactIDs
}

func uploadAndVerifyOneArtifact(
	t *testing.T,
	service *workercontrol.Service,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	artifact workercontrol.PlannedArtifact,
	index int,
) {
	t.Helper()
	uploadOneArtifact(t, service, worker, credentials, artifact, index, true)
}

func uploadOneArtifact(
	t *testing.T,
	service *workercontrol.Service,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	artifact workercontrol.PlannedArtifact,
	index int,
	verify bool,
) {
	t.Helper()
	claimID := uuid.New()
	claim, err := service.ClaimArtifactUpload(
		context.Background(), worker, credentials, artifact.UploadID, claimID,
	)
	if err != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
		t.Fatalf("claim Artifact %d upload = %#v error=%v", index, claim, err)
	}
	multipartID := "visible-completion-multipart-" + artifact.ArtifactID.String()
	if session, sessionErr := service.RecordArtifactMultipartSession(
		context.Background(),
		worker,
		credentials,
		artifact.UploadID,
		claimID,
		multipartID,
	); sessionErr != nil || session.Decision != workercontrol.ArtifactMultipartSessionRecorded {
		t.Fatalf("record Artifact %d multipart = %#v error=%v", index, session, sessionErr)
	}
	payload := sha256.Sum256([]byte("visible-completion-" + artifact.ArtifactID.String()))
	contentType := "video/mp4"
	if artifact.Kind == workercontrol.ArtifactKindThumbnail {
		contentType = "image/webp"
	}
	report := workercontrol.ArtifactUploadReport{
		ObjectVersionID: "version-" + artifact.ArtifactID.String(),
		SizeBytes:       int64(1_048_576 + index),
		SHA256:          payload,
		ContentType:     contentType,
		CompletedParts: []workercontrol.ArtifactUploadPart{{
			Number: 1, ETag: "etag-" + artifact.ArtifactID.String(), SizeBytes: int64(1_048_576 + index),
		}},
	}
	if uploaded, uploadErr := service.RecordArtifactUploaded(
		context.Background(), worker, credentials, artifact.UploadID, report,
	); uploadErr != nil || uploaded.Decision != workercontrol.ArtifactUploadRecorded {
		t.Fatalf("record Artifact %d upload = %#v error=%v", index, uploaded, uploadErr)
	}
	if !verify {
		return
	}
	if verified, verifyErr := service.VerifyArtifact(
		context.Background(), worker, credentials, artifact.UploadID, uuid.New(),
	); verifyErr != nil || verified.Decision != workercontrol.ArtifactVerified {
		t.Fatalf("verify Artifact %d = %#v error=%v", index, verified, verifyErr)
	}
}

type recordingArtifactInspector struct {
	inspection workercontrol.ArtifactInspection
	requests   []workercontrol.ArtifactInspectionRequest
}

func (inspector *recordingArtifactInspector) Inspect(
	_ context.Context,
	request workercontrol.ArtifactInspectionRequest,
) (workercontrol.ArtifactInspection, error) {
	inspector.requests = append(inspector.requests, request)
	return inspector.inspection, nil
}

func forceFinalizationFailureWindow(
	t *testing.T,
	db *sql.DB,
	jobID uuid.UUID,
	attemptID uuid.UUID,
	expireJob bool,
	exhaustAttempts bool,
) {
	t.Helper()
	var anchor time.Time
	if err := db.QueryRow("SELECT clock_timestamp()").Scan(&anchor); err != nil {
		t.Fatalf("read finalization failure PostgreSQL anchor: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin finalization failure fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable immutable triggers for finalization failure fixture: %v", err)
	}
	if result, err := tx.Exec(`
		UPDATE attempts
		SET assigned_at = $2,
			started_at = $3,
			finalization_started_at = $4,
			finalization_deadline_at = $5
		WHERE id = $1
	`,
		attemptID,
		anchor.Add(-20*time.Second),
		anchor.Add(-10_750*time.Millisecond),
		anchor.Add(-5_250*time.Millisecond),
		anchor.Add(-250*time.Millisecond),
	); err != nil {
		t.Fatalf("set finalization accounting window: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("finalization accounting-window rows = %d error=%v", rows, rowsErr)
	}
	maxAttempts := 3
	if exhaustAttempts {
		maxAttempts = 1
	}
	if result, err := tx.Exec(`
		UPDATE jobs
		SET job_expires_at = CASE
				WHEN $2::boolean THEN clock_timestamp()
				ELSE $3
			END,
			execution_max_attempts = $4,
			execution_retryable_failure_classes =
				array_append(execution_retryable_failure_classes, 'FINALIZATION_TIMEOUT')
		WHERE id = $1
	`, jobID, expireJob, anchor.Add(time.Minute), maxAttempts); err != nil {
		t.Fatalf("set finalization Retry Budget fixture: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("finalization Retry Budget rows = %d error=%v", rows, rowsErr)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit finalization failure fixture: %v", err)
	}
}

type finalizationFailureState struct {
	JobState               string
	AttemptState           string
	AttemptEnded           bool
	LeaseRevoked           bool
	ComputeSeconds         int64
	FinalizationSeconds    int64
	FinalizationRetryCount int
	ProjectRunning         int
	ProjectRetryWait       int
	PoolRetryWait          int
	ReservationState       string
	ReservedMinor          int64
	WorkerState            string
	Artifacts              int
	Uploads                int
	StagingArtifacts       int
	InitiatedUploads       int
	Charges                int
	Decisions              int
	RetryEvents            int
	FailedEvents           int
}

func readFinalizationFailureState(
	t *testing.T,
	db *sql.DB,
	jobID uuid.UUID,
	attemptID uuid.UUID,
) finalizationFailureState {
	t.Helper()
	var state finalizationFailureState
	if err := db.QueryRow(`
		SELECT
			job.state::text,
			attempt.state::text,
			attempt.ended_at IS NOT NULL,
			lease.revoked_at IS NOT NULL,
			retry.compute_seconds_consumed,
			retry.finalization_seconds_consumed,
			retry.finalization_retry_count,
			project.running_count,
			project.retry_wait_count,
			pool.retry_wait_count,
			reservation.state::text,
			credit.reserved_minor,
			worker.lifecycle_state::text,
			(SELECT count(*) FROM artifacts WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_uploads WHERE job_id = job.id),
			(SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'STAGING'),
			(SELECT count(*) FROM artifact_uploads WHERE job_id = job.id AND state = 'INITIATED'),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM execution_failure_decisions WHERE attempt_id = attempt.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.retry_wait'),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.failed')
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $2 AND attempt.job_id = job.id
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN retry_runtime_states AS retry ON retry.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		JOIN workers AS worker ON worker.id = attempt.worker_id
		WHERE job.id = $1
	`, jobID, attemptID).Scan(
		&state.JobState,
		&state.AttemptState,
		&state.AttemptEnded,
		&state.LeaseRevoked,
		&state.ComputeSeconds,
		&state.FinalizationSeconds,
		&state.FinalizationRetryCount,
		&state.ProjectRunning,
		&state.ProjectRetryWait,
		&state.PoolRetryWait,
		&state.ReservationState,
		&state.ReservedMinor,
		&state.WorkerState,
		&state.Artifacts,
		&state.Uploads,
		&state.StagingArtifacts,
		&state.InitiatedUploads,
		&state.Charges,
		&state.Decisions,
		&state.RetryEvents,
		&state.FailedEvents,
	); err != nil {
		t.Fatalf("read finalization failure state: %v", err)
	}
	return state
}
