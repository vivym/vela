//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/workeragent"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workerrecovery"
	"github.com/vivym/vela/internal/workertransport"
)

func TestWorkerAgentResumesSameWorkerMultipartAfterProcessLoss(t *testing.T) {
	fixture := newAssignmentFixture(t, "worker-agent-multipart-process-loss", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create exact Assignment: %v", err)
	}

	controlFixture := newWorkerRecoveryControlFixture(
		t, fixture, "vela-worker-process-recovery",
	)

	recoveryRoot := filepath.Join(t.TempDir(), "recovery")
	outputRoot := filepath.Join(t.TempDir(), "outputs")
	for _, root := range []string{recoveryRoot, outputRoot} {
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatalf("create Worker root %s: %v", root, err)
		}
	}
	recovery := newWorkerRecoveryConformanceManager(t, recoveryRoot, fixture.worker.ID, 7)
	agentFixture := workerRecoveryAgentFixture{
		workerID: fixture.worker.ID, workerEpoch: 7, recovery: recovery,
		control: controlFixture.control, outputRoot: outputRoot,
	}
	attemptRoot := filepath.Join(outputRoot, assignment.AttemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	video := make([]byte, 6<<20)
	for index := range video {
		video[index] = byte(index % 251)
	}
	thumbnail := []byte("same-worker-multipart-recovery-thumbnail")
	outputs := []runnertransport.Output{
		writeWorkerRecoveryOutput(t, attemptRoot, "video.mp4", "VIDEO", video, "video/mp4"),
		writeWorkerRecoveryOutput(t, attemptRoot, "thumbnail.webp", "THUMBNAIL", thumbnail, "image/webp"),
	}

	processLost := errors.New("simulated Worker Agent process loss")
	firstUploader := &workerRecoveryPartUploader{
		delegate: controlFixture.uploader, failAtCall: 2, failure: processLost,
	}
	firstRunner := &workerRecoveryRunner{outputs: outputs}
	firstAgent := agentFixture.newAgent(t, firstRunner, firstUploader)
	if _, err := firstAgent.RunOnce(context.Background()); !errors.Is(err, processLost) {
		t.Fatalf("first Worker Agent run error = %v, want process loss", err)
	}
	if firstUploader.successfulCalls != 1 {
		t.Fatalf("parts uploaded before process loss = %d, want 1", firstUploader.successfulCalls)
	}
	if firstRunner.prepareCalls != 1 || firstRunner.startCalls != 1 || firstRunner.collectCalls != 1 {
		t.Fatalf("first runner calls = prepare %d start %d collect %d",
			firstRunner.prepareCalls, firstRunner.startCalls, firstRunner.collectCalls)
	}
	active, err := recovery.ActiveHandles(context.Background())
	if err != nil || len(active) != 1 {
		t.Fatalf("active Local Recovery State after process loss = %d error=%v", len(active), err)
	}

	resumedUploader := &workerRecoveryPartUploader{delegate: controlFixture.uploader}
	resumedRunner := &workerRecoveryRunner{rejectCalls: true}
	resumedAgent := agentFixture.newAgent(t, resumedRunner, resumedUploader)
	result, err := resumedAgent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("resume Worker Agent finalization: %v", err)
	}
	if result.Outcome != workeragent.OutcomeVisibleCompletion ||
		result.AttemptID != assignment.AttemptID || result.JobID != assignment.JobID ||
		result.VisibleCompletion.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("resumed Worker Agent result = %#v", result)
	}
	if resumedRunner.totalCalls() != 0 {
		t.Fatalf("resumed finalization called runner %d times", resumedRunner.totalCalls())
	}
	if resumedUploader.successfulCalls != 2 {
		t.Fatalf("parts uploaded after restart = %d, want only remaining video and thumbnail parts",
			resumedUploader.successfulCalls)
	}

	var (
		jobState, attemptState string
		attempts, completions  int
		charges, artifactSets  int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			attempt.state::text,
			(SELECT count(*) FROM attempts WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		WHERE job.id = $1
	`, assignment.JobID).Scan(
		&jobState, &attemptState, &attempts, &completions, &charges, &artifactSets,
	); err != nil {
		t.Fatalf("read same-Worker recovery authority: %v", err)
	}
	if jobState != "SUCCEEDED" || attemptState != "SUCCEEDED" || attempts != 1 ||
		completions != 1 || charges != 1 || artifactSets != 1 {
		t.Fatalf("same-Worker recovery authority = Job %s Attempt %s attempts=%d completions=%d charges=%d sets=%d",
			jobState, attemptState, attempts, completions, charges, artifactSets)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: assignment.AttemptID, WorkerID: fixture.worker.ID,
		WorkerEpoch: 7, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("completed Local Recovery State reopened: %v", err)
	}
	for _, output := range outputs {
		if _, err := os.Lstat(output.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("terminal runner output remains at %s: %v", output.Path, err)
		}
	}
}

func TestWorkerNodeAndNVMeLossCreatesWholeJobReplacementAttempt(t *testing.T) {
	fixture := newAssignmentFixture(t, "worker-node-nvme-loss-recompute", 7)
	first, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create first Assignment: %v", err)
	}
	controlFixture := newWorkerRecoveryControlFixture(t, fixture, "vela-worker-node-loss")

	firstRecoveryRoot := filepath.Join(t.TempDir(), "first-worker-nvme")
	firstOutputRoot := filepath.Join(t.TempDir(), "first-worker-outputs")
	for _, root := range []string{firstRecoveryRoot, firstOutputRoot} {
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatalf("create first Worker root %s: %v", root, err)
		}
	}
	firstRecovery := newWorkerRecoveryConformanceManager(
		t, firstRecoveryRoot, fixture.worker.ID, 7,
	)
	firstAgentFixture := workerRecoveryAgentFixture{
		workerID: fixture.worker.ID, workerEpoch: 7, recovery: firstRecovery,
		control: controlFixture.control, outputRoot: firstOutputRoot,
	}
	nodeLost := errors.New("simulated Worker node loss")
	firstRunner := &workerRecoveryRunner{statusError: nodeLost}
	firstAgent := firstAgentFixture.newAgent(
		t, firstRunner, &workerRecoveryPartUploader{delegate: controlFixture.uploader},
	)
	if _, err := firstAgent.RunOnce(context.Background()); !errors.Is(err, nodeLost) {
		t.Fatalf("first Worker Agent run error = %v, want node loss", err)
	}
	if firstRunner.prepareCalls != 1 || firstRunner.startCalls != 1 || firstRunner.statusCalls != 1 {
		t.Fatalf("first Worker execution calls = prepare %d start %d status %d",
			firstRunner.prepareCalls, firstRunner.startCalls, firstRunner.statusCalls)
	}
	firstHandles, err := firstRecovery.ActiveHandles(context.Background())
	if err != nil || len(firstHandles) != 1 {
		t.Fatalf("first Worker Local Recovery State = %d handles error=%v", len(firstHandles), err)
	}
	if _, err := firstHandles[0].Write(
		context.Background(), workerrecovery.StageDiT, "latent.state",
		strings.NewReader("worker-local-only-state"),
	); err != nil {
		t.Fatalf("persist first Worker Local Recovery State: %v", err)
	}
	detachedNVMeRoot := firstRecoveryRoot + ".detached"
	if err := os.Rename(firstRecoveryRoot, detachedNVMeRoot); err != nil {
		t.Fatalf("detach first Worker NVMe root: %v", err)
	}

	forceLeaseExpiry(t, fixture.database.Admin, first.AttemptID, 3*time.Second)
	alignRunningAttemptWithExpiredLease(t, fixture.database.Admin, first.AttemptID)
	reconciler := newFailureReconciler(t, fixture.database, 2*time.Second)
	lost, err := reconciler.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile lost Worker execution: %v", err)
	}
	if !lost.Processed || lost.Source != workercontrol.FailureSourceExecutionLeaseExpired ||
		lost.Decision.AttemptID != first.AttemptID ||
		lost.Decision.AttemptState != workercontrol.LostAttempt ||
		lost.Decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		lost.Decision.JobFence <= first.LeaseFence {
		t.Fatalf("node-loss reconciliation = %#v", lost)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() - interval '1 second'
		WHERE job_id = $1
	`, first.JobID); err != nil {
		t.Fatalf("make whole-Job retry eligible: %v", err)
	}

	secondWorkerID := uuid.MustParse("30020000-0000-0000-0000-000000000021")
	secondSPIFFE := mustParseWorkerSPIFFEID(
		t, "spiffe://vela.internal/worker/h3-node-loss-replacement",
	)
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES ($1, $2, $3, 1, 'READY', 'HEALTHY')
	`, secondWorkerID, controlFixture.poolID, secondSPIFFE.String()); err != nil {
		t.Fatalf("seed replacement Worker: %v", err)
	}
	replacement, err := fixture.service.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: secondWorkerID},
		1,
		&workercontrol.AssignmentCandidate{
			JobID: first.JobID, ExpectedJobVersion: lost.Decision.JobVersion,
			ExecutionProfileRevisionID: first.ExecutionProfileRevisionID,
		},
	)
	if err != nil {
		t.Fatalf("create replacement Assignment: %v", err)
	}
	if replacement.AttemptID == first.AttemptID || replacement.AttemptNumber != 2 ||
		replacement.WorkerID != secondWorkerID || replacement.WorkerEpoch != 1 ||
		replacement.LeaseFence <= lost.Decision.JobFence ||
		replacement.LeaseFence <= first.LeaseFence {
		t.Fatalf("whole-Job replacement Assignment = %#v", replacement)
	}

	secondControl := newWorkerTransportTestClientForSPIFFE(
		t, fixture.database, controlFixture.coordinator, controlFixture.uploadStore,
		secondSPIFFE, controlFixture.fleetService,
	)
	secondRecoveryRoot := filepath.Join(t.TempDir(), "replacement-worker-nvme")
	secondOutputRoot := filepath.Join(t.TempDir(), "replacement-worker-outputs")
	for _, root := range []string{secondRecoveryRoot, secondOutputRoot} {
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatalf("create replacement Worker root %s: %v", root, err)
		}
	}
	secondRecovery := newWorkerRecoveryConformanceManager(
		t, secondRecoveryRoot, secondWorkerID, 1,
	)
	secondAgentFixture := workerRecoveryAgentFixture{
		workerID: secondWorkerID, workerEpoch: 1, recovery: secondRecovery,
		control: secondControl, outputRoot: secondOutputRoot,
	}
	if handles, err := secondRecovery.ActiveHandles(context.Background()); err != nil || len(handles) != 0 {
		t.Fatalf("replacement Worker inherited local state: handles=%d error=%v", len(handles), err)
	}
	replacementAttemptRoot := filepath.Join(secondOutputRoot, replacement.AttemptID.String())
	if err := os.Mkdir(replacementAttemptRoot, 0o700); err != nil {
		t.Fatalf("create replacement Attempt output root: %v", err)
	}
	video := []byte("replacement-worker-recomputed-video")
	thumbnail := []byte("replacement-worker-recomputed-thumbnail")
	replacementOutputs := []runnertransport.Output{
		writeWorkerRecoveryOutput(
			t, replacementAttemptRoot, "video.mp4", "VIDEO", video, "video/mp4",
		),
		writeWorkerRecoveryOutput(
			t, replacementAttemptRoot, "thumbnail.webp", "THUMBNAIL", thumbnail, "image/webp",
		),
	}
	secondRunner := &workerRecoveryRunner{outputs: replacementOutputs}
	secondAgent := secondAgentFixture.newAgent(
		t, secondRunner, &workerRecoveryPartUploader{delegate: controlFixture.uploader},
	)
	completed, err := secondAgent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run whole-Job replacement: %v", err)
	}
	if completed.Outcome != workeragent.OutcomeVisibleCompletion ||
		completed.AttemptID != replacement.AttemptID || completed.JobID != first.JobID ||
		secondRunner.prepareCalls != 1 || secondRunner.startCalls != 1 ||
		secondRunner.resumedLocalState {
		t.Fatalf("replacement execution = result %#v runner %#v", completed, secondRunner)
	}
	if len(secondRunner.preparedIdentities) != 1 ||
		secondRunner.preparedIdentities[0].AttemptID != replacement.AttemptID ||
		secondRunner.preparedIdentities[0].WorkerID != secondWorkerID ||
		secondRunner.preparedIdentities[0].LeaseFence != replacement.LeaseFence {
		t.Fatalf("replacement runner authority = %#v", secondRunner.preparedIdentities)
	}

	var (
		jobState                       string
		attempts, lostAttempts         int
		succeededAttempts, chargeCount int
		completions                    int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			(SELECT count(*) FROM attempts WHERE job_id = job.id),
			(SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'LOST'),
			(SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'SUCCEEDED'),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id)
		FROM jobs AS job
		WHERE job.id = $1
	`, first.JobID).Scan(
		&jobState, &attempts, &lostAttempts, &succeededAttempts, &chargeCount, &completions,
	); err != nil {
		t.Fatalf("read node-loss recompute authority: %v", err)
	}
	if jobState != "SUCCEEDED" || attempts != 2 || lostAttempts != 1 ||
		succeededAttempts != 1 || chargeCount != 1 || completions != 1 {
		t.Fatalf("node-loss recompute authority = Job %s attempts=%d lost=%d succeeded=%d charges=%d completions=%d",
			jobState, attempts, lostAttempts, succeededAttempts, chargeCount, completions)
	}
	preserved, err := os.ReadFile(filepath.Join(
		detachedNVMeRoot, "attempts", first.AttemptID.String(), "dit--latent.state",
	))
	if err != nil || string(preserved) != "worker-local-only-state" {
		t.Fatalf("detached Worker-local state changed or disappeared: %q error=%v", preserved, err)
	}
}

type workerRecoveryControlFixture struct {
	poolID       uuid.UUID
	coordinator  *workercontrol.Service
	uploadStore  workertransport.ArtifactUploadStore
	fleetService *fleet.Service
	control      *workertransport.Client
	uploader     workeragent.ArtifactPartUploader
}

func newWorkerRecoveryControlFixture(
	t *testing.T,
	assignment assignmentFixture,
	bucketName string,
) workerRecoveryControlFixture {
	t.Helper()
	minio := newMinIOFixture(t, bucketName)
	minio.enableVersioning(t)
	if err := minio.store.ValidateBucket(context.Background()); err != nil {
		t.Fatalf("validate Worker recovery Artifact Store: %v", err)
	}
	coordinator := visibleCompletionService(t, assignment.database.DSN)
	fleetService, err := fleet.NewService(newRolePool(
		t, assignment.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	configureTestCapacityPolicies(t, fleetService, poolID)
	uploader, err := workeragent.NewHTTPArtifactPartUploader(
		workeragent.HTTPArtifactPartUploaderConfig{AllowHTTP: true, Timeout: 30 * time.Second},
	)
	if err != nil {
		t.Fatalf("create Artifact part uploader: %v", err)
	}
	return workerRecoveryControlFixture{
		poolID: poolID, coordinator: coordinator, uploadStore: minio.store,
		fleetService: fleetService,
		control: newWorkerTransportTestClient(
			t, assignment.database, coordinator, minio.store, fleetService,
		),
		uploader: uploader,
	}
}

func alignRunningAttemptWithExpiredLease(
	t *testing.T,
	database *sql.DB,
	attemptID uuid.UUID,
) {
	t.Helper()
	transaction, err := database.Begin()
	if err != nil {
		t.Fatalf("begin node-loss timestamp fixture: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable immutable Attempt timestamps for node-loss fixture: %v", err)
	}
	result, err := transaction.Exec(`
		UPDATE attempts AS attempt
		SET assigned_at = lease.issued_at - interval '1 second',
			started_at = lease.issued_at + interval '1 second'
		FROM attempt_leases AS lease
		WHERE attempt.id = $1
		  AND lease.attempt_id = attempt.id
		  AND lease.revoked_at IS NULL
	`, attemptID)
	if err != nil {
		t.Fatalf("align RUNNING Attempt with expired Lease: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("aligned RUNNING Attempt rows = %d error=%v", rows, err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit node-loss timestamp fixture: %v", err)
	}
}

func newWorkerRecoveryConformanceManager(
	t *testing.T,
	root string,
	workerID uuid.UUID,
	workerEpoch int64,
) *workerrecovery.Manager {
	t.Helper()
	manager, err := workerrecovery.New(workerrecovery.Config{
		Root: root, WorkerID: workerID, WorkerEpoch: workerEpoch,
		AttemptQuotaBytes: 1 << 20, MaxEntryBytes: 1 << 18, MaxEntries: 32,
		HighWatermarkBytes: 800, LowWatermarkBytes: 400,
		CriticalFreeBytes: 50, TerminalRetention: time.Hour,
		SpaceProbe: func(string) (workerrecovery.Space, error) {
			return workerrecovery.Space{TotalBytes: 1 << 30, FreeBytes: 1 << 30}, nil
		},
	})
	if err != nil {
		t.Fatalf("create Worker Local Recovery manager: %v", err)
	}
	return manager
}

type workerRecoveryAgentFixture struct {
	workerID    uuid.UUID
	workerEpoch int64
	recovery    *workerrecovery.Manager
	control     *workertransport.Client
	outputRoot  string
}

func (fixture workerRecoveryAgentFixture) newAgent(
	t *testing.T,
	runner workeragent.Runner,
	uploader workeragent.ArtifactPartUploader,
) *workeragent.Agent {
	t.Helper()
	agent, err := workeragent.New(workeragent.Config{
		WorkerID: fixture.workerID, WorkerEpoch: fixture.workerEpoch, Recovery: fixture.recovery,
		Control: fixture.control, Runner: runner, HeartbeatInterval: 10 * time.Second,
		CapacityReportInterval: time.Minute,
		ArtifactStoreReachable: func(context.Context) bool { return true },
		OutputRoot:             fixture.outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		OutputCleanupMaxBytes:    32 << 20,
		InferenceBackendRevision: "sglang-h3-worker-recovery-conformance",
		Finalization:             fixture.control, PartUploader: uploader, ArtifactPartSize: 5 << 20,
	})
	if err != nil {
		t.Fatalf("create Worker Agent: %v", err)
	}
	return agent
}

func writeWorkerRecoveryOutput(
	t *testing.T,
	root string,
	name string,
	kind string,
	payload []byte,
	contentType string,
) runnertransport.Output {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write %s runner output: %v", kind, err)
	}
	return runnertransport.Output{
		Kind: kind, Path: path, SizeBytes: int64(len(payload)),
		SHA256: sha256.Sum256(payload), ContentType: contentType,
	}
}

type workerRecoveryRunner struct {
	outputs            []runnertransport.Output
	rejectCalls        bool
	statusError        error
	resumedLocalState  bool
	preparedIdentities []runnertransport.AttemptIdentity
	prepareCalls       int
	startCalls         int
	statusCalls        int
	collectCalls       int
	cancelCalls        int
}

func (runner *workerRecoveryRunner) ProbeReadiness(
	context.Context,
	runnertransport.ReadinessIdentity,
	runnertransport.ReadinessCheck,
) (runnertransport.ReadinessResult, error) {
	if runner.rejectCalls {
		return runnertransport.ReadinessResult{}, errors.New("runner called during finalization recovery")
	}
	return runnertransport.ReadinessResult{
		Decision: runnertransport.CommandAccepted, Passed: true,
		Evidence: json.RawMessage(`{"ready":true}`),
	}, nil
}

func (runner *workerRecoveryRunner) Prepare(
	_ context.Context,
	identity runnertransport.AttemptIdentity,
	_ runnertransport.ExecutionSpec,
	_ bool,
) (runnertransport.PrepareResult, error) {
	runner.prepareCalls++
	runner.preparedIdentities = append(runner.preparedIdentities, identity)
	if runner.rejectCalls {
		return runnertransport.PrepareResult{}, errors.New("runner called during finalization recovery")
	}
	return runnertransport.PrepareResult{
		Decision: runnertransport.CommandAccepted, ResumedLocalState: runner.resumedLocalState,
	}, nil
}

func (runner *workerRecoveryRunner) Start(
	context.Context,
	runnertransport.AttemptIdentity,
) (runnertransport.CommandResult, error) {
	runner.startCalls++
	if runner.rejectCalls {
		return runnertransport.CommandResult{}, errors.New("runner called during finalization recovery")
	}
	return runnertransport.CommandResult{Decision: runnertransport.CommandAccepted}, nil
}

func (runner *workerRecoveryRunner) Cancel(
	context.Context,
	runnertransport.AttemptIdentity,
	runnertransport.CancelReason,
) (runnertransport.CommandResult, error) {
	runner.cancelCalls++
	if runner.rejectCalls {
		return runnertransport.CommandResult{}, errors.New("runner called during finalization recovery")
	}
	return runnertransport.CommandResult{Decision: runnertransport.CommandAccepted}, nil
}

func (runner *workerRecoveryRunner) Status(
	context.Context,
	runnertransport.AttemptIdentity,
) (runnertransport.Status, error) {
	runner.statusCalls++
	if runner.rejectCalls {
		return runnertransport.Status{}, errors.New("runner called during finalization recovery")
	}
	if runner.statusError != nil {
		return runnertransport.Status{}, runner.statusError
	}
	return runnertransport.Status{
		State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
		GPUHealth:          json.RawMessage(`{"healthy":true}`),
		LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
	}, nil
}

func (runner *workerRecoveryRunner) CollectOutputs(
	context.Context,
	runnertransport.AttemptIdentity,
) (runnertransport.CollectOutputsResult, error) {
	runner.collectCalls++
	if runner.rejectCalls {
		return runnertransport.CollectOutputsResult{}, errors.New("runner called during finalization recovery")
	}
	return runnertransport.CollectOutputsResult{
		Decision: runnertransport.CommandAccepted,
		Outputs:  append([]runnertransport.Output(nil), runner.outputs...),
	}, nil
}

func (runner *workerRecoveryRunner) totalCalls() int {
	return runner.prepareCalls + runner.startCalls + runner.statusCalls +
		runner.collectCalls + runner.cancelCalls
}

type workerRecoveryPartUploader struct {
	delegate        workeragent.ArtifactPartUploader
	failAtCall      int
	failure         error
	calls           int
	successfulCalls int
}

func (uploader *workerRecoveryPartUploader) Upload(
	ctx context.Context,
	part workertransport.SignedArtifactUploadPart,
	payload []byte,
) (workercontrol.ArtifactUploadPart, error) {
	uploader.calls++
	if uploader.calls == uploader.failAtCall {
		return workercontrol.ArtifactUploadPart{}, uploader.failure
	}
	completed, err := uploader.delegate.Upload(ctx, part, payload)
	if err == nil {
		uploader.successfulCalls++
	}
	return completed, err
}
