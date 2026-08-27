//go:build integration

package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	nMinusOneRenewalControlCommit         = "450dd5c379ed7d26588e2a76140f0b3281acfbb2"
	nMinusOneFailureControlCommit         = "9cb1a522e20490ef41bab535fde206a947118d11"
	nMinusOneCancellationCommit           = "d0a8c0105a09b7f538e79400a7affd2a6c700744"
	finalizationFixedPointCommit          = "c94e140c8e841e88bdfcc41725bd7aa5ea7ac068"
	hierarchicalSchedulerNMinusOneCommit  = "8d4fd9199348d5ccdca48c40ef3b4a19ee5c5284"
	invoiceExportNMinusOneCommit          = "96760832c3b652827a9c5ce1732fbfa773ed0759"
	profileCircuitNMinusOneCommit         = "e1c50543d854cbad0cc5a98fdaac51e78e2c837c"
	webhookNMinusOneCommit                = "53f5d650adc30bacdcf9478786d71c1dcf6c1def"
	humanOIDCNMinusOneCommit              = "cedd0e8031d013a9a12327259b75fdc9749053ee"
	identityAdministrationNMinusOneCommit = "395887177ec9fb5f703eac055b04c02f2086fa8b"
	humanMembershipNMinusOneCommit        = "d8537d96cc8aeb7b7d4980e5059cf48efa713d6f"
	organizationReportingNMinusOneCommit  = "1ce496ce06c4ba33038be91a5fd5f7be502bee85"
	retentionNMinusOneCommit              = "87d1f27be568c96a31dcbda9d9c74ce7d2ed3f96"
	incompleteArtifactNMinusOneCommit     = "d038fb9f4fb9eb64d9e3b816e75d737783b9ccf5"
	debugDumpNMinusOneCommit              = "31991452e60c4254b3b67f72a98ee73e56f7915b"
	adjacentRolloutNMinusOneCommit        = debugDumpNMinusOneCommit
	breakGlassNMinusOneCommit             = "e0e9cfc80032890d63ed21da2dce1013cb623f57"
	financeReconciliationNMinusOneCommit  = "afe83d146ae8550c32bcf9ddc42fe17bf3e28b67"
	fleetControllerNMinusOneCommit        = "37b2689ba199b2d234b5827d1e4f24cbfefb4334"
)

func TestExactFleetNMinusOneAssignmentWriterAcrossProtocolGate(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, fleetControllerNMinusOneCommit)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)

	gateOffJob := submitJob(t, server.URL, "fleet-n-minus-one-gate-off", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"legacy Assignment writer before Fleet enforcement"
	}`))
	gateOnJob := submitJob(t, server.URL, "fleet-n-minus-one-gate-on", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"legacy Assignment writer after Fleet enforcement"
	}`))
	if gateOffJob.StatusCode != 202 || gateOnJob.StatusCode != 202 {
		t.Fatalf(
			"Fleet N-1 Admission statuses = %d/%d; bodies=%s / %s",
			gateOffJob.StatusCode,
			gateOnJob.StatusCode,
			gateOffJob.Body,
			gateOnJob.Body,
		)
	}
	var gateOffResponse, gateOnResponse jobResponse
	if err := json.Unmarshal(gateOffJob.Body, &gateOffResponse); err != nil {
		t.Fatalf("decode gate-off Fleet N-1 Job: %v", err)
	}
	if err := json.Unmarshal(gateOnJob.Body, &gateOnResponse); err != nil {
		t.Fatalf("decode gate-on Fleet N-1 Job: %v", err)
	}
	firstJobID := uuid.MustParse(gateOffResponse.JobID)
	secondJobID := uuid.MustParse(gateOnResponse.JobID)
	firstWorker := uuid.MustParse("23000000-0000-0000-0000-000000000051")
	secondWorker := uuid.MustParse("23000000-0000-0000-0000-000000000052")
	seedNMinusOneProfileCircuitWorker(t, database.Admin, firstWorker, "fleet-gate-off")
	seedNMinusOneProfileCircuitWorker(t, database.Admin, secondWorker, "fleet-gate-on")

	firstAttempt := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		firstJobID.String(),
		firstWorker.String(),
		7,
		1,
	)
	var firstProtocol int
	if err := database.Admin.QueryRow(`
		SELECT fleet_protocol_version FROM attempts WHERE id = $1
	`, firstAttempt).Scan(&firstProtocol); err != nil {
		t.Fatalf("read gate-off Fleet protocol version: %v", err)
	}
	if firstProtocol != 1 {
		t.Fatalf("gate-off Fleet protocol version = %d", firstProtocol)
	}

	if _, err := database.Admin.Exec(`
		SELECT vela_transition_fleet_assignment_protocol(
			true, 'exact 37b2689 Assignment writers drained', 0
		)
	`); err != nil {
		t.Fatalf("enforce Fleet Assignment protocol: %v", err)
	}
	before := readAssignmentState(t, database.Admin, secondJobID, secondWorker)
	output, probeErr := runNMinusOneAssignmentProbeProcess(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		secondJobID.String(),
		secondWorker.String(),
		7,
		1,
	)
	if probeErr == nil || !strings.Contains(
		string(output),
		"Assignment writer does not match the active Fleet protocol",
	) {
		t.Fatalf("gate-on Fleet N-1 writer error=%v\n%s", probeErr, output)
	}
	after := readAssignmentState(t, database.Admin, secondJobID, secondWorker)
	if after != before {
		t.Fatalf("rejected Fleet N-1 writer changed state: before=%+v after=%+v", before, after)
	}

	if _, err := database.Admin.Exec(`
		SELECT vela_transition_fleet_assignment_protocol(
			false, 'exact 37b2689 rollback verified', 0
		)
	`); err != nil {
		t.Fatalf("roll back Fleet Assignment protocol: %v", err)
	}
	secondAttempt := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		secondJobID.String(),
		secondWorker.String(),
		7,
		1,
	)
	var secondProtocol int
	if err := database.Admin.QueryRow(`
		SELECT fleet_protocol_version FROM attempts WHERE id = $1
	`, secondAttempt).Scan(&secondProtocol); err != nil {
		t.Fatalf("read rollback Fleet protocol version: %v", err)
	}
	if secondProtocol != 1 {
		t.Fatalf("rollback Fleet protocol version = %d", secondProtocol)
	}
}

func TestExactFinanceReconciliationNMinusOneControlAndRequestRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, financeReconciliationNMinusOneCommit)
	assertProfileCircuitNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "finance-reconciliation")
	jobID := uuid.MustParse(runNMinusOneAdmissionProbe(
		t,
		nMinusOne.AdmissionProbe,
		database.DSN,
	))
	var persistedJobID uuid.UUID
	if err := database.Admin.QueryRow(`SELECT id FROM jobs WHERE id = $1`, jobID).
		Scan(&persistedJobID); err != nil {
		t.Fatalf("read Finance Reconciliation N-1 Admission Job: %v", err)
	}
	if persistedJobID != jobID {
		t.Fatalf("Finance Reconciliation N-1 Admission Job = %s, want %s", persistedJobID, jobID)
	}
}

func TestCurrentFinanceReconciliationControlFailsClosedAgainstSchema17(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 17); err != nil {
		t.Fatalf("contract Finance Reconciliation schema before current-control probe: %v", err)
	}
	binary := filepath.Join(t.TempDir(), "vela-control-current-finance-reconciliation")
	build := exec.Command("go", "build", "-o", binary, "./cmd/vela-control")
	build.Dir = repositoryRoot(t)
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build current Finance Reconciliation control: %v\n%s", err, output)
	}
	output := runCurrentControlStartupProbe(t, binary, database.DSN)
	if !strings.Contains(output, "open Finance Reconciliation database pool") ||
		!strings.Contains(output, "Finance Reconciliation transaction privilege boundary") {
		t.Fatalf("current Finance Reconciliation control did not fail closed against schema 17:\n%s", output)
	}
}

func TestExactBreakGlassNMinusOneControlAndCustomerRequestRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, breakGlassNMinusOneCommit)
	assertProfileCircuitNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "break-glass")
	jobID := uuid.MustParse(runNMinusOneAdmissionProbe(
		t,
		nMinusOne.AdmissionProbe,
		database.DSN,
	))
	var persistedJobID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM jobs WHERE id = $1
	`, jobID).Scan(&persistedJobID); err != nil {
		t.Fatalf("read Break-glass N-1 Admission Job: %v", err)
	}
	if persistedJobID != jobID {
		t.Fatalf("Break-glass N-1 Admission Job = %s, want %s", persistedJobID, jobID)
	}
}

func TestCurrentBreakGlassControlFailsClosedAgainstSchema16(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 16); err != nil {
		t.Fatalf("contract Break-glass schema before current-control probe: %v", err)
	}
	binary := filepath.Join(t.TempDir(), "vela-control-current-break-glass")
	build := exec.Command("go", "build", "-o", binary, "./cmd/vela-control")
	build.Dir = repositoryRoot(t)
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build current Break-glass control: %v\n%s", err, output)
	}
	output := runCurrentControlStartupProbe(t, binary, database.DSN)
	if !strings.Contains(output, "open Platform Operator auth database pool") ||
		!strings.Contains(output, "Platform Operator authentication transaction privilege boundary") {
		t.Fatalf("current Break-glass control did not fail closed against schema 16:\n%s", output)
	}
}

func TestExactRetentionNMinusOneControlAdmissionAndVisibleCompletionRemainCompatible(
	t *testing.T,
) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "retention N-1 compatibility")
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.New()
	seedNMinusOneProfileCircuitWorker(t, database.Admin, workerID, "retention")
	nMinusOne := buildNMinusOneBinaries(t, retentionNMinusOneCommit)
	assertProfileCircuitNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	jobID := uuid.MustParse(runNMinusOneAdmissionProbe(
		t, nMinusOne.AdmissionProbe, database.DSN,
	))

	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	service, err := workercontrol.NewService(
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
		t.Fatalf("create retention N-1 fixture coordinator: %v", err)
	}
	worker := workercontrol.AuthenticatedWorker{ID: workerID}
	assignment, err := service.Acquire(
		context.Background(),
		worker,
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      jobID,
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
		},
	)
	if err != nil {
		t.Fatalf("assign retention N-1 Job: %v", err)
	}
	credentials := leaseCredentials(assignment)
	if started, startErr := service.Start(
		context.Background(), worker, credentials,
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start retention N-1 Job = %#v error=%v", started, startErr)
	}
	plan, err := service.BeginFinalization(context.Background(), worker, credentials)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("begin retention N-1 finalization = %#v error=%v", plan, err)
	}
	artifactIDs := uploadAndVerifyFinalizationPlan(t, service, worker, credentials, plan)
	completed := runNMinusOneVisibleCompletionProbe(
		t,
		nMinusOne.VisibleCompletionProbe,
		database.DSN,
		worker,
		credentials,
		plan.JobVersion,
		artifactIDs,
	)
	if completed.Decision != string(workercontrol.VisibleCompletionCommitted) ||
		completed.ArtifactSetID == uuid.Nil || completed.ChargeID == uuid.Nil ||
		completed.CompletedAt.IsZero() {
		t.Fatalf("retention N-1 Visible Completion = %#v", completed)
	}

	var (
		policyID, policyRevision                string
		artifactDays, requestDays               int
		requestContentExpiresAt, admissionAt    time.Time
		artifactSetExpiresAt, artifactMinExpiry time.Time
	)
	if err := database.Admin.QueryRow(`
		SELECT
			job.retention_policy_revision_id::text,
			job.retention_artifact_days,
			job.retention_request_content_days,
			job.request_content_expires_at,
			ready.occurred_at,
			artifact_set.retention_policy_revision,
			artifact_set.retention_expires_at,
			min(artifact.retention_expires_at)
		FROM jobs AS job
		JOIN outbox_events AS ready
		  ON ready.aggregate_id = job.id
		 AND ready.aggregate_version = 1
		 AND ready.event_type = 'job.ready'
		JOIN artifact_sets AS artifact_set ON artifact_set.job_id = job.id
		JOIN artifacts AS artifact ON artifact.job_id = job.id
		WHERE job.id = $1
		GROUP BY job.id, artifact_set.id, ready.event_id
	`, jobID).Scan(
		&policyID,
		&artifactDays,
		&requestDays,
		&requestContentExpiresAt,
		&admissionAt,
		&policyRevision,
		&artifactSetExpiresAt,
		&artifactMinExpiry,
	); err != nil {
		t.Fatalf("read retention N-1 default snapshots: %v", err)
	}
	if policyID != "00000000-0000-0000-0000-000000001630" ||
		artifactDays != 30 || requestDays != 30 ||
		!requestContentExpiresAt.Equal(admissionAt.Add(30*24*time.Hour)) ||
		policyRevision != "artifact-success-30d-v1" ||
		!artifactSetExpiresAt.Equal(completed.CompletedAt.Add(30*24*time.Hour)) ||
		!artifactMinExpiry.Equal(artifactSetExpiresAt) {
		t.Fatalf(
			"retention N-1 snapshots = policy %s days %d/%d request %s Admission %s Artifact %s expiry %s/%s",
			policyID,
			artifactDays,
			requestDays,
			requestContentExpiresAt,
			admissionAt,
			policyRevision,
			artifactSetExpiresAt,
			artifactMinExpiry,
		)
	}
}

func TestExactIncompleteArtifactNMinusOneControlCompletesJobAndRunsRetention(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, incompleteArtifactNMinusOneCommit)
	output := runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN)
	if strings.Contains(output, "database pool") ||
		!strings.Contains(output, "open Node Agent endpoint file") {
		t.Fatalf("Slice 23 N-1 control did not reach the Node Agent resolver sentinel:\n%s", output)
	}

	setLeaseRenewalProtocolGate(t, database.Admin, true, "incomplete Artifact N-1 compatibility")
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.New()
	seedNMinusOneProfileCircuitWorker(t, database.Admin, workerID, "incomplete-artifact")
	jobID := uuid.MustParse(runNMinusOneAdmissionProbe(
		t,
		nMinusOne.AdmissionProbe,
		database.DSN,
	))
	service, err := workercontrol.NewService(
		context.Background(),
		newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password"),
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
		t.Fatalf("create incomplete Artifact N-1 coordinator: %v", err)
	}
	worker := workercontrol.AuthenticatedWorker{ID: workerID}
	assignment, err := service.Acquire(
		context.Background(),
		worker,
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      jobID,
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
		},
	)
	if err != nil {
		t.Fatalf("assign incomplete Artifact N-1 Job: %v", err)
	}
	credentials := leaseCredentials(assignment)
	if started, startErr := service.Start(
		context.Background(),
		worker,
		credentials,
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start incomplete Artifact N-1 Job = %#v error=%v", started, startErr)
	}
	plan, err := service.BeginFinalization(context.Background(), worker, credentials)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("begin incomplete Artifact N-1 finalization = %#v error=%v", plan, err)
	}
	artifactIDs := uploadAndVerifyFinalizationPlan(t, service, worker, credentials, plan)
	completed := runNMinusOneVisibleCompletionProbe(
		t,
		nMinusOne.VisibleCompletionProbe,
		database.DSN,
		worker,
		credentials,
		plan.JobVersion,
		artifactIDs,
	)
	if completed.Decision != string(workercontrol.VisibleCompletionCommitted) ||
		completed.ArtifactSetID == uuid.Nil || completed.ChargeID == uuid.Nil ||
		completed.CompletedAt.IsZero() {
		t.Fatalf("incomplete Artifact N-1 Visible Completion = %#v", completed)
	}
	failedJobID := uuid.MustParse(runNMinusOneAdmissionProbe(
		t,
		nMinusOne.AdmissionProbe,
		database.DSN,
	))
	failedAssignment, err := service.Acquire(
		context.Background(),
		worker,
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      failedJobID,
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
		},
	)
	if err != nil {
		t.Fatalf("assign N-1 ignored incomplete Artifact Job: %v", err)
	}
	failedCredentials := leaseCredentials(failedAssignment)
	if started, startErr := service.Start(
		context.Background(),
		worker,
		failedCredentials,
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start N-1 ignored incomplete Artifact Job = %#v error=%v", started, startErr)
	}
	failedPlan, err := service.BeginFinalization(
		context.Background(),
		worker,
		failedCredentials,
	)
	if err != nil || failedPlan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("begin N-1 ignored incomplete Artifact finalization = %#v error=%v", failedPlan, err)
	}
	failed, err := service.Fail(
		context.Background(),
		worker,
		failedCredentials,
		workercontrol.FailureObservation{
			FailureClass: "ARTIFACT_VALIDATION_FAILED",
			FailureFingerprint: fmt.Sprintf(
				"n-minus-one.artifact.validation/%s",
				failedPlan.Artifacts[0].ArtifactID,
			),
			ErrorSummary:             "N-1 compatibility fixture failed certified Artifact validation",
			BackendStage:             "finalization",
			InferenceBackendRevision: "sglang@n-minus-one-incomplete-artifact",
			RetryRecommended:         true,
			WorkerReusable:           true,
		},
	)
	if err != nil || failed.Disposition != workercontrol.RetryDispositionFailed {
		t.Fatalf("fail N-1 ignored incomplete Artifact Job = %#v error=%v", failed, err)
	}

	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin N-1 Artifact expiry setup: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatalf("disable immutable triggers for N-1 Artifact expiry: %v", err)
	}
	for relation, timestampColumns := range map[string]string{
		"artifact_sets":          "committed_at = clock_timestamp() - interval '8 days', retention_expires_at",
		"artifacts":              "verified_at = clock_timestamp() - interval '8 days', retention_expires_at",
		"artifact_access_grants": "eligible_at = clock_timestamp() - interval '8 days', retention_expires_at",
	} {
		statement := fmt.Sprintf(
			"UPDATE %s SET %s = clock_timestamp() - interval '1 day' WHERE job_id = $1",
			relation,
			timestampColumns,
		)
		if _, err := tx.Exec(statement, jobID); err != nil {
			t.Fatalf("move %s retention into past: %v", relation, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit N-1 Artifact expiry setup: %v", err)
	}
	retained := runNMinusOneRetentionProbe(t, nMinusOne.RetentionProbe, database.DSN)
	wantTargets := len(artifactIDs) + 1
	if retained.RequestContentExpired != 0 || retained.ArtifactRequestsCreated != 1 ||
		retained.Claimed != wantTargets || retained.Completed != wantTargets ||
		retained.Failed != 0 || retained.Deleted != len(artifactIDs) || retained.Aborted != 0 {
		t.Fatalf("incomplete Artifact N-1 retention = %#v, want %d targets", retained, wantTargets)
	}
	var ignoredRequests, ignoredStagingArtifacts int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM content_deletion_requests
			 WHERE job_id = $1 AND source = 'RETENTION_INCOMPLETE_ARTIFACT'),
			(SELECT count(*) FROM artifacts
			 WHERE job_id = $1 AND state = 'STAGING')
	`, failedJobID).Scan(&ignoredRequests, &ignoredStagingArtifacts); err != nil {
		t.Fatalf("read N-1 ignored incomplete Artifact evidence: %v", err)
	}
	if ignoredRequests != 0 || ignoredStagingArtifacts != len(failedPlan.Artifacts) {
		t.Fatalf(
			"N-1 ignored incomplete Artifacts = requests/staging %d/%d, want 0/%d",
			ignoredRequests,
			ignoredStagingArtifacts,
			len(failedPlan.Artifacts),
		)
	}
	currentStore := &recordingRetentionStore{}
	currentReconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		currentStore,
		retention.ReconcilerConfig{
			InstanceID: "current-incomplete-artifact-protocol-probe",
			BatchSize:  len(failedPlan.Artifacts) + 1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create current incomplete Artifact protocol probe: %v", err)
	}
	currentResult, err := currentReconciler.ReconcileBatch(context.Background())
	if err != nil || currentResult.RequestContentExpired != 0 ||
		currentResult.ContentDeletionRequestsCreated != 1 ||
		currentResult.Claimed != len(failedPlan.Artifacts)+1 ||
		currentResult.Completed != currentResult.Claimed || currentResult.Failed != 0 {
		t.Fatalf("current incomplete Artifact protocol result = %#v error=%v", currentResult, err)
	}
	var jobState, deletionState, deletionSource string
	var deletedArtifacts, receipts int
	if err := database.Admin.QueryRow(`
		SELECT
			job.state::text,
			deletion.state::text,
			deletion.source::text,
			(SELECT count(*) FROM artifacts
			 WHERE job_id = job.id AND state = 'DELETED'),
			(SELECT count(*) FROM content_deletion_receipts
			 WHERE request_id = deletion.id)
		FROM jobs AS job
		JOIN content_deletion_requests AS deletion
		  ON deletion.job_id = job.id
		 AND deletion.source = 'RETENTION_ARTIFACT'
		WHERE job.id = $1
	`, jobID).Scan(
		&jobState,
		&deletionState,
		&deletionSource,
		&deletedArtifacts,
		&receipts,
	); err != nil {
		t.Fatalf("read incomplete Artifact N-1 retention evidence: %v", err)
	}
	if jobState != "SUCCEEDED" || deletionState != "COMPLETED" ||
		deletionSource != "RETENTION_ARTIFACT" ||
		deletedArtifacts != len(artifactIDs) || receipts != 1 {
		t.Fatalf(
			"incomplete Artifact N-1 evidence = job/deletion/source %s/%s/%s artifacts/receipts %d/%d",
			jobState,
			deletionState,
			deletionSource,
			deletedArtifacts,
			receipts,
		)
	}
}

func TestExactDebugDumpNMinusOneRetentionProcessesAdditiveTargets(t *testing.T) {
	fixture := newAssignmentFixture(t, "debug-dump-n-minus-one-retention", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	dumpID, _, _, _, _ := persistAvailableDebugDump(t, fixture, authorizationID)
	expireDebugDumpAuthorization(t, fixture, authorizationID, dumpID)

	nMinusOne := buildNMinusOneBinaries(t, debugDumpNMinusOneCommit)
	retained := runNMinusOneRetentionProbe(
		t, nMinusOne.RetentionProbe, fixture.database.DSN,
	)
	if retained.RequestContentExpired != 0 || retained.ArtifactRequestsCreated != 1 ||
		retained.Claimed != 2 || retained.Completed != 2 || retained.Failed != 0 ||
		retained.Deleted != 1 || retained.Aborted != 0 {
		t.Fatalf("debug dump N-1 retention result = %#v", retained)
	}
	var requestState, dumpState string
	var receipts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			request.state::text,
			dump.state::text,
			(SELECT count(*) FROM content_deletion_receipts
			 WHERE request_id = request.id)
		FROM content_deletion_requests AS request
		JOIN debug_dumps AS dump ON dump.id = $2
		WHERE request.debug_dump_authorization_id = $1
		  AND request.source = 'RETENTION_DEBUG_DUMP'
	`, authorizationID, dumpID).Scan(&requestState, &dumpState, &receipts); err != nil {
		t.Fatalf("read debug dump N-1 retention evidence: %v", err)
	}
	if requestState != "COMPLETED" || dumpState != "DELETED" || receipts != 1 {
		t.Fatalf(
			"debug dump N-1 request/dump/receipts = %s/%s/%d",
			requestState,
			dumpState,
			receipts,
		)
	}
}

func TestCurrentRetentionControlFailsClosedAgainstSchema15(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 15); err != nil {
		t.Fatalf("contract retention schema before current-control probe: %v", err)
	}
	binary := filepath.Join(t.TempDir(), "vela-control-current-retention")
	build := exec.Command("go", "build", "-o", binary, "./cmd/vela-control")
	build.Dir = repositoryRoot(t)
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build current retention control: %v\n%s", err, output)
	}
	output := runCurrentControlStartupProbe(t, binary, database.DSN)
	if !strings.Contains(output, "open retention request database pool") ||
		!strings.Contains(
			output,
			"Retention Policy and Content Deletion request transaction privilege boundary",
		) {
		t.Fatalf("current retention control did not fail closed against schema 15:\n%s", output)
	}
}

func TestExactOrganizationReportingNMinusOneControlServiceAndHumanRequestsRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, organizationReportingNMinusOneCommit)
	assertWebhookNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "organization-reporting")
	developerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		developerID,
		"n-minus-one-organization-reporting-developer",
		nil,
		map[string][]string{testProjectID: {"Developer"}},
	)
	serviceJobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	humanJobID, humanPrincipalID := runNMinusOneHumanAdmissionProbe(
		t,
		nMinusOne.HumanAdmissionProbe,
		database.DSN,
		"n-minus-one-organization-reporting-developer",
	)
	if humanPrincipalID != developerID {
		t.Fatalf("Organization reporting N-1 Human Principal = %s, want %s", humanPrincipalID, developerID)
	}

	for label, expected := range map[string]struct {
		jobID       string
		principalID uuid.UUID
		kind        string
	}{
		"Service": {jobID: serviceJobID, principalID: uuid.MustParse(testPrincipalID), kind: "SERVICE"},
		"Human":   {jobID: humanJobID, principalID: developerID, kind: "HUMAN"},
	} {
		var createdBy uuid.UUID
		var actorKind string
		if err := database.Admin.QueryRow(`
			SELECT job.created_by_principal_id, attribution.principal_kind::text
			FROM jobs AS job
			JOIN project_principal_attributions AS attribution
			  ON attribution.organization_id = job.organization_id
			 AND attribution.project_id = job.project_id
			 AND attribution.principal_id = job.created_by_principal_id
			WHERE job.id = $1
		`, expected.jobID).Scan(&createdBy, &actorKind); err != nil {
			t.Fatalf("read %s Organization reporting N-1 Job attribution: %v", label, err)
		}
		if createdBy != expected.principalID || actorKind != expected.kind {
			t.Fatalf(
				"%s Organization reporting N-1 Job attribution = principal %s kind %s",
				label,
				createdBy,
				actorKind,
			)
		}
	}
}

func TestExactHumanMembershipNMinusOneControlServiceAndHumanRequestsRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, humanMembershipNMinusOneCommit)
	assertWebhookNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "human-membership")
	developerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		developerID,
		"n-minus-one-membership-developer",
		nil,
		map[string][]string{testProjectID: {"Developer"}},
	)
	serviceJobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	humanJobID, humanPrincipalID := runNMinusOneHumanAdmissionProbe(
		t,
		nMinusOne.HumanAdmissionProbe,
		database.DSN,
		"n-minus-one-membership-developer",
	)
	if humanPrincipalID != developerID {
		t.Fatalf("N-1 Human probe Principal = %s, want %s", humanPrincipalID, developerID)
	}

	for label, expected := range map[string]struct {
		jobID       string
		principalID uuid.UUID
		kind        string
	}{
		"Service": {jobID: serviceJobID, principalID: uuid.MustParse(testPrincipalID), kind: "SERVICE"},
		"Human":   {jobID: humanJobID, principalID: developerID, kind: "HUMAN"},
	} {
		var createdBy uuid.UUID
		var actorKind string
		if err := database.Admin.QueryRow(`
			SELECT job.created_by_principal_id, attribution.principal_kind::text
			FROM jobs AS job
			JOIN project_principal_attributions AS attribution
			  ON attribution.organization_id = job.organization_id
			 AND attribution.project_id = job.project_id
			 AND attribution.principal_id = job.created_by_principal_id
			WHERE job.id = $1
		`, expected.jobID).Scan(&createdBy, &actorKind); err != nil {
			t.Fatalf("read %s N-1 Job attribution: %v", label, err)
		}
		if createdBy != expected.principalID || actorKind != expected.kind {
			t.Fatalf(
				"%s N-1 Job attribution = principal %s kind %s",
				label, createdBy, actorKind,
			)
		}
	}
}

func TestExactIdentityAdministrationNMinusOneControlAndServiceRequestRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, identityAdministrationNMinusOneCommit)
	assertWebhookNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "identity-administration")
	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)

	var createdByPrincipalID, actorKind string
	if err := database.Admin.QueryRow(`
		SELECT job.created_by_principal_id::text, attribution.principal_kind::text
		FROM jobs AS job
		JOIN project_principal_attributions AS attribution
		  ON attribution.organization_id = job.organization_id
		 AND attribution.project_id = job.project_id
		 AND attribution.principal_id = job.created_by_principal_id
		WHERE job.id = $1
	`, jobID).Scan(&createdByPrincipalID, &actorKind); err != nil {
		t.Fatalf("read identity-administration N-1 Service Job: %v", err)
	}
	if createdByPrincipalID != testPrincipalID || actorKind != "SERVICE" {
		t.Fatalf(
			"identity-administration N-1 Job attribution = principal %s kind %s",
			createdByPrincipalID,
			actorKind,
		)
	}
	var identityEvents int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM project_identity_events`).Scan(
		&identityEvents,
	); err != nil {
		t.Fatalf("count N-1 identity administration events: %v", err)
	}
	if identityEvents != 0 {
		t.Fatalf("N-1 Service request created %d identity administration events", identityEvents)
	}
}

func TestExactHumanOIDCNMinusOneControlAndServiceRequestRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, humanOIDCNMinusOneCommit)
	assertWebhookNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "human-oidc")
	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	var createdByPrincipalID, actorKind string
	if err := database.Admin.QueryRow(`
		SELECT job.created_by_principal_id::text, attribution.principal_kind::text
		FROM jobs AS job
		JOIN project_principal_attributions AS attribution
		  ON attribution.organization_id = job.organization_id
		 AND attribution.project_id = job.project_id
		 AND attribution.principal_id = job.created_by_principal_id
		WHERE job.id = $1
	`, jobID).Scan(&createdByPrincipalID, &actorKind); err != nil {
		t.Fatalf("read Human-slice N-1 Service Job: %v", err)
	}
	if createdByPrincipalID != testPrincipalID || actorKind != "SERVICE" {
		t.Fatalf(
			"Human-slice N-1 Job attribution = principal %s kind %s",
			createdByPrincipalID,
			actorKind,
		)
	}
}

func TestExactWebhookNMinusOneControlAndTerminalWriterRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, webhookNMinusOneCommit)
	assertWebhookNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "webhook")
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope for webhook N-1 writer: %v", err)
	}
	subscriptionID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO webhook_subscriptions (
			id, organization_id, project_id, endpoint_url, event_types,
			created_by_principal_id, created_by_credential_id
		) VALUES (
			$1, $2, $3, 'https://hooks.example.com/n-minus-one',
			ARRAY['job.canceled']::webhook_event_type[], $4, $5
		)
	`, subscriptionID, testOrganizationID, testProjectID, testPrincipalID, testCredentialID); err != nil {
		t.Fatalf("seed webhook N-1 Subscription: %v", err)
	}

	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	result := runNMinusOneInvoiceChargeProbe(
		t,
		nMinusOne.InvoiceChargeProbe,
		database.DSN,
		uuid.MustParse(jobID),
	)
	if result.Decision != "CANCELED" || result.ChargeID != "" {
		t.Fatalf("webhook N-1 cancellation result = %#v", result)
	}

	var eventType, deliveryState, deliveryJobID string
	var deliveryCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*), min(delivery.event_type), min(delivery.state::text),
			min(delivery.job_id::text)
		FROM webhook_deliveries AS delivery
		WHERE delivery.subscription_id = $1
	`, subscriptionID).Scan(&deliveryCount, &eventType, &deliveryState, &deliveryJobID); err != nil {
		t.Fatalf("read webhook N-1 Delivery: %v", err)
	}
	if deliveryCount != 1 || eventType != "job.canceled" || deliveryState != "PENDING" ||
		deliveryJobID != jobID {
		t.Fatalf(
			"webhook N-1 Delivery count=%d type=%s state=%s job=%s",
			deliveryCount,
			eventType,
			deliveryState,
			deliveryJobID,
		)
	}
}

func assertWebhookNMinusOneDatabaseStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "open auth database pool") ||
		strings.Contains(output, "open request database pool") ||
		strings.Contains(output, "open Artifact request database pool") ||
		strings.Contains(output, "open cancellation database pool") ||
		strings.Contains(output, "open internal database pool") ||
		strings.Contains(output, "open Scheduler database pool") ||
		strings.Contains(output, "open billing database pool") ||
		!strings.Contains(output, "open Invoice export bearer token file") {
		t.Fatalf("webhook N-1 startup did not reach Invoice token sentinel:\n%s", output)
	}
}

func TestExactProfileCircuitNMinusOneWriterAcrossProtocolGate(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, profileCircuitNMinusOneCommit)
	assertProfileCircuitNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)

	gateOffWorkerID := uuid.New()
	seedNMinusOneProfileCircuitWorker(t, database.Admin, gateOffWorkerID, "gate-off")
	gateOffJobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	gateOff, err := runNMinusOneFailureProbeProcess(
		t,
		nMinusOne.FailureProbe,
		database.DSN,
		gateOffJobID,
		gateOffWorkerID.String(),
	)
	if err != nil {
		t.Fatalf("run gate-off Profile circuit N-1 writer: %v\n%s", err, gateOff)
	}
	var gateOffResult nMinusOneFailureProbeResult
	if err := json.Unmarshal(gateOff, &gateOffResult); err != nil {
		t.Fatalf("decode gate-off Profile circuit N-1 result: %v\n%s", err, gateOff)
	}
	if gateOffResult.Disposition != "RETRY_WAIT" || gateOffResult.AttemptID == "" {
		t.Fatalf("gate-off Profile circuit N-1 result = %#v", gateOffResult)
	}
	var protocol int
	var workerWasHealthy bool
	var certificationState string
	var openingCount int
	if err := database.Admin.QueryRow(`
		SELECT
			decision.circuit_protocol_version,
			decision.worker_was_healthy,
			certification.state::text,
			(SELECT count(*) FROM profile_certification_circuit_openings)
		FROM execution_failure_decisions AS decision
		JOIN attempts AS attempt ON attempt.id = decision.attempt_id
		JOIN profile_certifications AS certification
		  ON certification.id = attempt.profile_certification_id
		WHERE decision.attempt_id = $1
	`, gateOffResult.AttemptID).Scan(
		&protocol,
		&workerWasHealthy,
		&certificationState,
		&openingCount,
	); err != nil {
		t.Fatalf("read gate-off Profile circuit N-1 decision: %v", err)
	}
	if protocol != 1 || workerWasHealthy || certificationState != "ACTIVE" || openingCount != 0 {
		t.Fatalf(
			"gate-off N-1 decision = protocol %d healthy %t certification %s openings %d",
			protocol,
			workerWasHealthy,
			certificationState,
			openingCount,
		)
	}

	setProfileCircuitProtocolGate(
		t,
		database.Admin,
		true,
		"exact e1c5054 failure writers drained",
	)
	assertProfileCircuitNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	gateOnWorkerID := uuid.New()
	seedNMinusOneProfileCircuitWorker(t, database.Admin, gateOnWorkerID, "gate-on")
	gateOnJobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	gateOn, gateOnErr := runNMinusOneFailureProbeProcess(
		t,
		nMinusOne.FailureProbe,
		database.DSN,
		gateOnJobID,
		gateOnWorkerID.String(),
	)
	if gateOnErr == nil ||
		!strings.Contains(string(gateOn), "failure decision writer does not match the active Profile circuit protocol") ||
		!strings.Contains(string(gateOn), "SQLSTATE 55000") {
		t.Fatalf("gate-on Profile circuit N-1 writer error=%v\n%s", gateOnErr, gateOn)
	}

	var (
		jobState          string
		attemptState      string
		leaseRevoked      bool
		workerState       string
		decisionCount     int
		jobOpeningCount   int
		circuitState      string
		projectRunning    int
		creditReservation string
	)
	if err := database.Admin.QueryRow(`
		SELECT
			job.state::text,
			attempt.state::text,
			lease.revoked_at IS NOT NULL,
			worker.lifecycle_state::text,
			(SELECT count(*) FROM execution_failure_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE triggering_job_id = job.id),
			runtime.circuit_breaker_state::text,
			project.running_count,
			reservation.state::text
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN workers AS worker ON worker.id = attempt.worker_id
		JOIN retry_runtime_states AS runtime ON runtime.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, gateOnJobID).Scan(
		&jobState,
		&attemptState,
		&leaseRevoked,
		&workerState,
		&decisionCount,
		&jobOpeningCount,
		&circuitState,
		&projectRunning,
		&creditReservation,
	); err != nil {
		t.Fatalf("read rejected gate-on Profile circuit N-1 state: %v", err)
	}
	if jobState != "RUNNING" || attemptState != "RUNNING" || leaseRevoked ||
		workerState != "BUSY" || decisionCount != 0 || jobOpeningCount != 0 ||
		circuitState != "{}" || projectRunning != 1 || creditReservation != "RESERVED" {
		t.Fatalf(
			"rejected gate-on N-1 state = job %s attempt %s revoked %t worker %s decisions/openings %d/%d circuit %s running %d reservation %s",
			jobState,
			attemptState,
			leaseRevoked,
			workerState,
			decisionCount,
			jobOpeningCount,
			circuitState,
			projectRunning,
			creditReservation,
		)
	}
}

func assertProfileCircuitNMinusOneDatabaseStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "open auth database pool") ||
		strings.Contains(output, "open request database pool") ||
		strings.Contains(output, "open Artifact request database pool") ||
		strings.Contains(output, "open cancellation database pool") ||
		strings.Contains(output, "open internal database pool") ||
		strings.Contains(output, "open Scheduler database pool") ||
		strings.Contains(output, "open billing database pool") ||
		!strings.Contains(output, "open Invoice export bearer token file") {
		t.Fatalf("Profile circuit N-1 startup did not reach Invoice token sentinel:\n%s", output)
	}
}

func seedNMinusOneProfileCircuitWorker(t *testing.T, database *sql.DB, workerID uuid.UUID, label string) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1,
			'00000000-0000-0000-0000-000000000005',
			$2,
			7,
			'READY',
			'HEALTHY'
		)
	`, workerID, "spiffe://vela.internal/worker/profile-circuit-n-minus-one-"+label); err != nil {
		t.Fatalf("seed %s Profile circuit N-1 Worker: %v", label, err)
	}
	if _, err := database.Exec(`
		INSERT INTO worker_profile_readiness (
			worker_id, worker_epoch, execution_profile_revision_id, readiness
		) VALUES (
			$1, 7, '00000000-0000-0000-0000-000000000014', 'WARM'
		)
	`, workerID); err != nil {
		t.Fatalf("seed %s Profile circuit N-1 Worker readiness: %v", label, err)
	}
}

func TestExactInvoiceExportNMinusOneWriterCreatesExportAuthority(t *testing.T) {
	fixture := newStartFixture(t, "invoice-export-n-minus-one-writer", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope for Invoice N-1 writer: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start Invoice N-1 writer fixture = %#v error=%v", started, err)
	}
	nMinusOne := buildNMinusOneBinaries(t, invoiceExportNMinusOneCommit)
	result := runNMinusOneInvoiceChargeProbe(
		t,
		nMinusOne.InvoiceChargeProbe,
		fixture.database.DSN,
		fixture.assignment.JobID,
	)
	if result.Decision != "CANCELING" || result.ChargeID == "" {
		t.Fatalf("Invoice N-1 Charge result = %#v", result)
	}
	chargeID, err := uuid.Parse(result.ChargeID)
	if err != nil {
		t.Fatalf("parse Invoice N-1 Charge id: %v", err)
	}
	var state string
	var attempts, exports, invoiceEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			export.state,
			export.attempts,
			(SELECT count(*) FROM invoice_exports WHERE charge_id = export.charge_id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = export.job_id AND event_type = 'invoice.export_requested')
		FROM invoice_exports AS export
		WHERE export.charge_id = $1
	`, chargeID).Scan(&state, &attempts, &exports, &invoiceEvents); err != nil {
		t.Fatalf("read Invoice N-1 export authority: %v", err)
	}
	if state != "PENDING" || attempts != 0 || exports != 1 || invoiceEvents != 1 {
		t.Fatalf(
			"Invoice N-1 authority = state %s attempts %d exports/events %d/%d",
			state,
			attempts,
			exports,
			invoiceEvents,
		)
	}
}

func TestNMinusOneControlStartupAcrossRenewalProtocolTransition(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneRenewalControlCommit)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role during expand: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "n-minus-one-writer-replay", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"prove the fixed-point writer and replay against the expanded schema"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit N-1 probe Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode N-1 probe Job: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed N-1 probe Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t, nMinusOne.AssignmentProbe, database.DSN, job.JobID, testWorkerID, 7, 1,
	)
	var protocolVersion int16
	var claimMatchesExpiry bool
	if err := database.Admin.QueryRow(`
		SELECT renewal_protocol_version, token_claim_expires_at = expires_at
		FROM attempt_leases
		WHERE attempt_id = $1
	`, attemptID).Scan(&protocolVersion, &claimMatchesExpiry); err != nil {
		t.Fatalf("read N-1 Assignment protocol state: %v", err)
	}
	if protocolVersion != 1 || !claimMatchesExpiry {
		t.Fatalf("N-1 Assignment protocol version=%d claim_matches_expiry=%t", protocolVersion, claimMatchesExpiry)
	}
	if _, err := database.Admin.Exec(
		"UPDATE attempt_leases SET revoked_at = clock_timestamp() WHERE attempt_id = $1",
		attemptID,
	); err != nil {
		t.Fatalf("drain N-1 probe Lease before switch: %v", err)
	}

	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(true, 'N-1 startup integration switch')",
	); err != nil {
		t.Fatalf("enable renewal protocol: %v", err)
	}
	assertNMinusOneRequestStartupRejected(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role after switch: %v", err)
	}
	if _, err := database.Admin.Exec("GRANT SELECT ON retry_runtime_states TO vela_request"); err != nil {
		t.Fatalf("inject legacy request privilege after switch: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("new request-role verification accepted legacy table access after switch")
	}
	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(true, 'repair switched privilege boundary')",
	); err != nil {
		t.Fatalf("repair switched request privilege boundary: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify repaired new request role after switch: %v", err)
	}

	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(false, 'N-1 startup integration rollback')",
	); err != nil {
		t.Fatalf("disable renewal protocol: %v", err)
	}
	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role after rollback: %v", err)
	}
}

func TestNMinusOneControlStartsAndWritesAfterExecutionFailureExpansion(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneFailureControlCommit)

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	seedAdmissionFixture(t, database.Admin)
	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-failure-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed N-1 failure-expansion Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t, nMinusOne.AssignmentProbe, database.DSN, jobID, testWorkerID, 7, 1,
	)

	var jobState, attemptState string
	var decisionCount int
	if err := database.Admin.QueryRow(`
		SELECT j.state, a.state, (SELECT count(*) FROM execution_failure_decisions)
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		WHERE j.id = $1 AND a.id = $2
	`, jobID, attemptID).Scan(&jobState, &attemptState, &decisionCount); err != nil {
		t.Fatalf("read N-1 failure-expansion writes: %v", err)
	}
	if jobState != "ASSIGNED" || attemptState != "ASSIGNED" || decisionCount != 0 {
		t.Fatalf(
			"N-1 failure-expansion state = job %s attempt %s decisions %d",
			jobState, attemptState, decisionCount,
		)
	}
}

func TestNMinusOneControlStartsAndWritesAfterCustomerCancellationExpansion(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneCancellationCommit)

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	seedAdmissionFixture(t, database.Admin)
	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-cancellation-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed N-1 cancellation-expansion Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t, nMinusOne.AssignmentProbe, database.DSN, jobID, testWorkerID, 7, 1,
	)

	var jobState, attemptState string
	var cancellationDecisions, charges, stopReceipts int
	if err := database.Admin.QueryRow(`
		SELECT
			job.state,
			attempt.state,
			(SELECT count(*) FROM job_cancellation_decisions),
			(SELECT count(*) FROM charges),
			(SELECT count(*) FROM cancellation_stop_receipts)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		WHERE job.id = $1 AND attempt.id = $2
	`, jobID, attemptID).Scan(
		&jobState,
		&attemptState,
		&cancellationDecisions,
		&charges,
		&stopReceipts,
	); err != nil {
		t.Fatalf("read N-1 cancellation-expansion writes: %v", err)
	}
	if jobState != "ASSIGNED" || attemptState != "ASSIGNED" ||
		cancellationDecisions != 0 || charges != 0 || stopReceipts != 0 {
		t.Fatalf(
			"N-1 cancellation-expansion state = job %s attempt %s decisions/charges/receipts %d/%d/%d",
			jobState,
			attemptState,
			cancellationDecisions,
			charges,
			stopReceipts,
		)
	}
}

func TestExactFinalizationFixedPointStartsWritesAndForwardsUnknownSuccess(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, finalizationFixedPointCommit)

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	seedAdmissionFixture(t, database.Admin)
	jobIDText := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-finalization-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed exact fixed-point Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		jobIDText,
		testWorkerID,
		7,
		1,
	)
	jobID := uuid.MustParse(jobIDText)
	var jobState, attemptState string
	var artifacts, artifactSets int
	if err := database.Admin.QueryRow(`
		SELECT
			job.state,
			attempt.state,
			(SELECT count(*) FROM artifacts),
			(SELECT count(*) FROM artifact_sets)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		WHERE job.id = $1 AND attempt.id = $2
	`, jobID, attemptID).Scan(&jobState, &attemptState, &artifacts, &artifactSets); err != nil {
		t.Fatalf("read exact fixed-point expansion writes: %v", err)
	}
	if jobState != "ASSIGNED" || attemptState != "ASSIGNED" || artifacts != 0 || artifactSets != 0 {
		t.Fatalf(
			"exact fixed-point expansion state = %s/%s artifacts/sets %d/%d",
			jobState,
			attemptState,
			artifacts,
			artifactSets,
		)
	}

	eventID := uuid.New()
	successPayload, err := proto.Marshal(&velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: 3,
		EventType:        "job.succeeded",
		SchemaVersion:    1,
		Payload: &velav1.EventEnvelope_JobSucceeded{JobSucceeded: &velav1.JobSucceeded{
			OrganizationId: testOrganizationID,
			ProjectId:      testProjectID,
			JobId:          jobID.String(),
			AttemptId:      attemptID,
			AttemptFence:   1,
			ArtifactSetId:  uuid.NewString(),
			ManifestSha256: bytes.Repeat([]byte{0x2a}, 32),
			ChargeId:       uuid.NewString(),
			Artifacts: []*velav1.ArtifactSnapshot{{
				ArtifactId:      uuid.NewString(),
				Kind:            "VIDEO",
				Ordinal:         0,
				ObjectKey:       "artifacts/org/project/job/attempt/artifact/video.mp4",
				ObjectVersionId: "version-0001",
				SizeBytes:       1024,
				Sha256:          bytes.Repeat([]byte{0x7b}, 32),
				ContentType:     "video/mp4",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal fixed-point unknown success event: %v", err)
	}
	if _, err := database.Admin.Exec("DELETE FROM outbox_events WHERE aggregate_id = $1", jobID); err != nil {
		t.Fatalf("remove fixed-point Admission event: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at
		) VALUES ($1, $2, $3, 'Job', $4, 3, 'job.succeeded', 1, $5, clock_timestamp())
	`, eventID, testOrganizationID, testProjectID, jobID, successPayload); err != nil {
		t.Fatalf("insert fixed-point unknown success event: %v", err)
	}
	forwarded := runNMinusOneOutboxProbe(t, nMinusOne.OutboxProbe, database.DSN)
	if forwarded.Subject != "vela.events.job.succeeded" ||
		forwarded.MessageID != eventID.String() || forwarded.KnownPayload ||
		forwarded.UnknownBytes == 0 || !bytes.Equal(forwarded.Payload, successPayload) {
		t.Fatalf("fixed-point success forwarding = %#v", forwarded)
	}
	var published bool
	var stream string
	var sequence int64
	var attempts int
	if err := database.Admin.QueryRow(`
		SELECT published_at IS NOT NULL, broker_stream, broker_sequence, publish_attempts
		FROM outbox_events
		WHERE event_id = $1
	`, eventID).Scan(&published, &stream, &sequence, &attempts); err != nil {
		t.Fatalf("read fixed-point success PubAck receipt: %v", err)
	}
	if !published || stream != "N_MINUS_ONE_PROBE" || sequence != 1 || attempts != 1 {
		t.Fatalf(
			"fixed-point success PubAck = published %t stream %s sequence %d attempts %d",
			published,
			stream,
			sequence,
			attempts,
		)
	}
}

func TestExactSchedulerNMinusOneAssignmentWriterAcrossProtocolGate(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, hierarchicalSchedulerNMinusOneCommit)
	startupOutput := runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN)
	if strings.Contains(startupOutput, "VELA_SCHEDULER_") ||
		strings.Contains(startupOutput, "open auth database pool") ||
		strings.Contains(startupOutput, "open request database pool") ||
		strings.Contains(startupOutput, "open Artifact request database pool") ||
		strings.Contains(startupOutput, "open cancellation database pool") ||
		strings.Contains(startupOutput, "open internal database pool") ||
		!strings.Contains(startupOutput, "open Artifact S3 access key ID file") {
		t.Fatalf("exact Scheduler N-1 startup did not reach Artifact sentinel:\n%s", startupOutput)
	}
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	firstJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-n-minus-one-gate-off",
	)
	secondJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-n-minus-one-gate-on",
	)
	firstWorker := uuid.MustParse("00000000-0000-0000-0000-000000000430")
	secondWorker := uuid.MustParse("00000000-0000-0000-0000-000000000431")
	for index, workerID := range []uuid.UUID{firstWorker, secondWorker} {
		if _, err := database.Admin.Exec(`
			INSERT INTO workers (
				id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
			) VALUES (
				$1, '00000000-0000-0000-0000-000000000005', $2, 7, 'READY', 'HEALTHY'
			)
		`, workerID, fmt.Sprintf("spiffe://vela.internal/worker/scheduler-n-minus-one-%d", index)); err != nil {
			t.Fatalf("seed Scheduler N-1 Worker %d: %v", index, err)
		}
	}

	firstAttemptID := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		firstJob.String(),
		firstWorker.String(),
		7,
		1,
	)
	var hasDispatchIntent bool
	if err := database.Admin.QueryRow(`
		SELECT scheduler_dispatch_intent_id IS NOT NULL
		FROM attempts WHERE id = $1
	`, firstAttemptID).Scan(&hasDispatchIntent); err != nil {
		t.Fatalf("read gate-off N-1 Attempt: %v", err)
	}
	if hasDispatchIntent {
		t.Fatal("gate-off N-1 Attempt unexpectedly has a Scheduler dispatch intent")
	}

	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'exact 8d4fd91 writer drained before Scheduler protocol switch'
		)
	`); err != nil {
		t.Fatalf("enable Scheduler dispatch protocol: %v", err)
	}
	before := readAssignmentState(t, database.Admin, secondJob, secondWorker)
	output, probeErr := runNMinusOneAssignmentProbeProcess(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		secondJob.String(),
		secondWorker.String(),
		7,
		1,
	)
	if probeErr == nil || !strings.Contains(
		string(output),
		"Assignment requires a live Scheduler dispatch claim",
	) {
		t.Fatalf("gate-on Scheduler N-1 writer error=%v\n%s", probeErr, output)
	}
	after := readAssignmentState(t, database.Admin, secondJob, secondWorker)
	if after != before {
		t.Fatalf("rejected Scheduler N-1 writer changed state: before=%+v after=%+v", before, after)
	}
}

func TestNMinusOneOutboxPublisherForwardsUnknownCancellationPayloadBytes(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneCancellationCommit)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "n-minus-one-unknown-cancellation-event", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"old publisher must preserve the new cancellation event bytes"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit N-1 Outbox probe Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode N-1 Outbox probe Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)
	eventID := uuid.New()
	cancellationID := uuid.New()
	chargeID := uuid.New()
	payload, err := proto.Marshal(&velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: 2,
		EventType:        "job.canceling",
		SchemaVersion:    1,
		Payload: &velav1.EventEnvelope_JobCanceling{JobCanceling: &velav1.JobCanceling{
			OrganizationId:    testOrganizationID,
			ProjectId:         testProjectID,
			JobId:             jobID.String(),
			CancellationId:    cancellationID.String(),
			CancellationFence: 8,
			ChargeId:          chargeID.String(),
		}},
	})
	if err != nil {
		t.Fatalf("marshal unknown-to-N-1 cancellation event: %v", err)
	}
	if _, err := database.Admin.Exec("DELETE FROM outbox_events WHERE aggregate_id = $1", jobID); err != nil {
		t.Fatalf("remove Admission event before N-1 Outbox probe: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id,
			organization_id,
			project_id,
			aggregate_type,
			aggregate_id,
			aggregate_version,
			event_type,
			schema_version,
			payload,
			occurred_at
		) VALUES ($1, $2, $3, 'Job', $4, 2, 'job.canceling', 1, $5, clock_timestamp())
	`, eventID, testOrganizationID, testProjectID, jobID, payload); err != nil {
		t.Fatalf("insert unknown-to-N-1 cancellation event: %v", err)
	}

	forwarded := runNMinusOneOutboxProbe(t, nMinusOne.OutboxProbe, database.DSN)
	if forwarded.Subject != "vela.events.job.canceling" || forwarded.MessageID != eventID.String() {
		t.Fatalf("N-1 publisher metadata = %q/%q", forwarded.Subject, forwarded.MessageID)
	}
	if forwarded.KnownPayload || forwarded.UnknownBytes == 0 {
		t.Fatalf(
			"N-1 descriptor classified cancellation payload as known=%t unknown_bytes=%d",
			forwarded.KnownPayload,
			forwarded.UnknownBytes,
		)
	}
	if !bytes.Equal(forwarded.Payload, payload) {
		t.Fatalf("N-1 publisher changed unknown cancellation payload bytes: got %x want %x", forwarded.Payload, payload)
	}

	var published bool
	var stream string
	var sequence int64
	var attempts int
	if err := database.Admin.QueryRow(`
		SELECT published_at IS NOT NULL, broker_stream, broker_sequence, publish_attempts
		FROM outbox_events
		WHERE event_id = $1
	`, eventID).Scan(&published, &stream, &sequence, &attempts); err != nil {
		t.Fatalf("read N-1 Outbox publish receipt: %v", err)
	}
	if !published || stream != "N_MINUS_ONE_PROBE" || sequence != 1 || attempts != 1 {
		t.Fatalf("N-1 Outbox receipt = published %t stream %s sequence %d attempts %d", published, stream, sequence, attempts)
	}

	successEventID := uuid.New()
	artifactSetID := uuid.New()
	successChargeID := uuid.New()
	artifactID := uuid.New()
	successPayload, err := proto.Marshal(&velav1.EventEnvelope{
		EventId:          successEventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: 3,
		EventType:        "job.succeeded",
		SchemaVersion:    1,
		Payload: &velav1.EventEnvelope_JobSucceeded{JobSucceeded: &velav1.JobSucceeded{
			OrganizationId: testOrganizationID,
			ProjectId:      testProjectID,
			JobId:          jobID.String(),
			AttemptId:      uuid.NewString(),
			AttemptFence:   8,
			ArtifactSetId:  artifactSetID.String(),
			ManifestSha256: bytes.Repeat([]byte{0x2a}, 32),
			ChargeId:       successChargeID.String(),
			Artifacts: []*velav1.ArtifactSnapshot{{
				ArtifactId:      artifactID.String(),
				Kind:            "VIDEO",
				Ordinal:         0,
				ObjectKey:       "artifacts/org/project/job/attempt/artifact/video.mp4",
				ObjectVersionId: "version-0001",
				SizeBytes:       1024,
				Sha256:          bytes.Repeat([]byte{0x7b}, 32),
				ContentType:     "video/mp4",
			}},
		},
		},
	})
	if err != nil {
		t.Fatalf("marshal unknown-to-N-1 success event: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id,
			organization_id,
			project_id,
			aggregate_type,
			aggregate_id,
			aggregate_version,
			event_type,
			schema_version,
			payload,
			occurred_at
		) VALUES ($1, $2, $3, 'Job', $4, 3, 'job.succeeded', 1, $5, clock_timestamp())
	`, successEventID, testOrganizationID, testProjectID, jobID, successPayload); err != nil {
		t.Fatalf("insert unknown-to-N-1 success event: %v", err)
	}
	forwarded = runNMinusOneOutboxProbe(t, nMinusOne.OutboxProbe, database.DSN)
	if forwarded.Subject != "vela.events.job.succeeded" ||
		forwarded.MessageID != successEventID.String() {
		t.Fatalf("N-1 success publisher metadata = %q/%q", forwarded.Subject, forwarded.MessageID)
	}
	if forwarded.KnownPayload || forwarded.UnknownBytes == 0 {
		t.Fatalf(
			"N-1 descriptor classified success payload as known=%t unknown_bytes=%d",
			forwarded.KnownPayload,
			forwarded.UnknownBytes,
		)
	}
	if !bytes.Equal(forwarded.Payload, successPayload) {
		t.Fatalf(
			"N-1 publisher changed unknown success payload bytes: got %x want %x",
			forwarded.Payload,
			successPayload,
		)
	}
	if err := database.Admin.QueryRow(`
		SELECT published_at IS NOT NULL, broker_stream, broker_sequence, publish_attempts
		FROM outbox_events
		WHERE event_id = $1
	`, successEventID).Scan(&published, &stream, &sequence, &attempts); err != nil {
		t.Fatalf("read N-1 success Outbox publish receipt: %v", err)
	}
	if !published || stream != "N_MINUS_ONE_PROBE" || sequence != 1 || attempts != 1 {
		t.Fatalf(
			"N-1 success Outbox receipt = published %t stream %s sequence %d attempts %d",
			published,
			stream,
			sequence,
			attempts,
		)
	}
}

func TestNMinusOneAssignmentConsumesRetryWaitCreatedByNewControl(t *testing.T) {
	fixture := newAssignmentFixture(t, "n-minus-one-consumes-new-retry", 7)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneFailureControlCommit)
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
		t.Fatalf("start first Assignment = %#v error=%v", started, err)
	}
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		first.JobID,
		`execution_retry_backoff_policy = '{"kind":"exponential","initial_seconds":1,"max_seconds":1}'::jsonb`,
	)
	decision, err := fixture.service.Fail(
		context.Background(),
		fixture.worker,
		leaseCredentials(first),
		validFailureObservation(),
	)
	if err != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		decision.NextRetryAt == nil {
		t.Fatalf("new control RetryDecision = %#v error=%v", decision, err)
	}
	assertWaitingCompatibilityCounters(t, fixture.database.Admin, first.JobID, 1, 1, 1, 1)
	waitForDatabaseTimeAfter(t, fixture.database.Admin, *decision.NextRetryAt)

	output, probeErr := runNMinusOneAssignmentProbeProcess(
		t,
		nMinusOne.AssignmentProbe,
		fixture.database.DSN,
		first.JobID.String(),
		first.WorkerID.String(),
		7,
		decision.JobVersion,
	)
	if probeErr == nil || !strings.Contains(string(output), "Worker is excluded by protected Job retry evidence") {
		t.Fatalf("N-1 excluded Worker probe error=%v\n%s", probeErr, output)
	}
	assertWaitingCompatibilityCounters(t, fixture.database.Admin, first.JobID, 1, 1, 1, 1)
	server := admissionServerForDatabase(t, fixture.database)
	for index := range 10 {
		accepted := submitJob(t, server.URL, fmt.Sprintf("n-minus-one-full-normal-queue-%d", index), []byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"fill the real normal queue while an N-created Retry waits"
		}`))
		if accepted.StatusCode != 202 {
			t.Fatalf("submit normal queued Job %d status = %d; body=%s", index, accepted.StatusCode, accepted.Body)
		}
	}
	var normalQueuedJobs int
	if err := fixture.database.Admin.QueryRow(
		"SELECT count(*) FROM jobs WHERE state = 'QUEUED'",
	).Scan(&normalQueuedJobs); err != nil {
		t.Fatalf("count real normal queued Jobs: %v", err)
	}
	if normalQueuedJobs != 10 {
		t.Fatalf("normal queued Jobs = %d, want 10", normalQueuedJobs)
	}
	assertWaitingCompatibilityCounters(t, fixture.database.Admin, first.JobID, 11, 1, 11, 1)

	const replacementWorkerID = "00000000-0000-0000-0000-000000000291"
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-retry-replacement',
			7, 'READY', 'HEALTHY'
		)
	`, replacementWorkerID); err != nil {
		t.Fatalf("seed N-1 replacement Worker: %v", err)
	}
	replacementAttemptID := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		fixture.database.DSN,
		first.JobID.String(),
		replacementWorkerID,
		7,
		decision.JobVersion,
	)

	var (
		jobState                string
		replacementWorker       string
		replacementFence        int64
		attemptsStarted         int
		projectQueued           int
		projectQueuedLimit      int
		projectRetryWait        int
		projectRunning          int
		poolQueued              int
		poolQueuedLimit         int
		poolRetryWait           int
		billableStartPreserved  bool
		visibleProgressRowCount int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			a.worker_id::text,
			a.fence,
			runtime.attempts_started,
			project.queued_count,
			project.queued_limit,
			project.retry_wait_count,
			project.running_count,
			pool.queued_count,
			pool.queued_limit,
			pool.retry_wait_count,
			j.billable_started_at = $3,
			(
				SELECT count(*)
				FROM attempt_progress AS progress
				WHERE progress.job_id = j.id
				  AND progress.fence = j.current_fence
			)
		FROM jobs AS j
		JOIN attempts AS a ON a.id = $2
		JOIN retry_runtime_states AS runtime ON runtime.job_id = j.id
		JOIN projects AS project ON project.id = j.project_id
		JOIN worker_pools AS pool ON pool.id = j.worker_pool_id
		WHERE j.id = $1
	`, first.JobID, replacementAttemptID, started.StartedAt).Scan(
		&jobState,
		&replacementWorker,
		&replacementFence,
		&attemptsStarted,
		&projectQueued,
		&projectQueuedLimit,
		&projectRetryWait,
		&projectRunning,
		&poolQueued,
		&poolQueuedLimit,
		&poolRetryWait,
		&billableStartPreserved,
		&visibleProgressRowCount,
	); err != nil {
		t.Fatalf("read N-1 retry replacement state: %v", err)
	}
	if jobState != "ASSIGNED" || replacementWorker != replacementWorkerID ||
		replacementFence <= decision.JobFence || attemptsStarted != 2 ||
		projectQueued != projectQueuedLimit || projectRetryWait != 0 || projectRunning != 1 ||
		poolQueued != poolQueuedLimit || poolRetryWait != 0 || !billableStartPreserved ||
		visibleProgressRowCount != 0 {
		t.Fatalf(
			"N-1 retry replacement = state %s worker %s fence %d attempts %d project %d(limit %d)/%d/%d pool %d(limit %d)/%d billable %t progress %d",
			jobState,
			replacementWorker,
			replacementFence,
			attemptsStarted,
			projectQueued,
			projectQueuedLimit,
			projectRetryWait,
			projectRunning,
			poolQueued,
			poolQueuedLimit,
			poolRetryWait,
			billableStartPreserved,
			visibleProgressRowCount,
		)
	}
}

func assertWaitingCompatibilityCounters(
	t *testing.T,
	database *sql.DB,
	jobID uuid.UUID,
	projectQueued int,
	projectRetryWait int,
	poolQueued int,
	poolRetryWait int,
) {
	t.Helper()
	var actualProjectQueued, actualProjectRetryWait, actualPoolQueued, actualPoolRetryWait int
	if err := database.QueryRow(`
		SELECT
			project.queued_count,
			project.retry_wait_count,
			pool.queued_count,
			pool.retry_wait_count
		FROM jobs AS job
		JOIN projects AS project ON project.id = job.project_id
		JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
		WHERE job.id = $1
	`, jobID).Scan(
		&actualProjectQueued,
		&actualProjectRetryWait,
		&actualPoolQueued,
		&actualPoolRetryWait,
	); err != nil {
		t.Fatalf("read waiting compatibility counters: %v", err)
	}
	if actualProjectQueued != projectQueued || actualProjectRetryWait != projectRetryWait ||
		actualPoolQueued != poolQueued || actualPoolRetryWait != poolRetryWait {
		t.Fatalf(
			"waiting compatibility counters = project %d/%d pool %d/%d, want project %d/%d pool %d/%d",
			actualProjectQueued,
			actualProjectRetryWait,
			actualPoolQueued,
			actualPoolRetryWait,
			projectQueued,
			projectRetryWait,
			poolQueued,
			poolRetryWait,
		)
	}
}

type nMinusOneBinaries struct {
	Control                string
	AdmissionProbe         string
	HumanAdmissionProbe    string
	AssignmentProbe        string
	OutboxProbe            string
	JetStreamOutboxProbe   string
	SchedulerProbe         string
	WorkerTransportProbe   string
	InvoiceChargeProbe     string
	FailureProbe           string
	VisibleCompletionProbe string
	RetentionProbe         string
}

type adjacentNMinusOneProbeDescriptor struct {
	CommandName string
	SourceName  string
	BinaryName  string
	assign      func(*nMinusOneBinaries, string)
}

func adjacentNMinusOneProbeDescriptors() []adjacentNMinusOneProbeDescriptor {
	return []adjacentNMinusOneProbeDescriptor{
		{
			CommandName: "vela-nminusone-jetstream-outbox-probe",
			SourceName:  "nminusone_jetstream_outbox_probe.go.txt",
			BinaryName:  "vela-jetstream-outbox-probe-n-minus-one",
			assign: func(binaries *nMinusOneBinaries, path string) {
				binaries.JetStreamOutboxProbe = path
			},
		},
		{
			CommandName: "vela-nminusone-scheduler-probe",
			SourceName:  "nminusone_scheduler_probe.go.txt",
			BinaryName:  "vela-scheduler-probe-n-minus-one",
			assign: func(binaries *nMinusOneBinaries, path string) {
				binaries.SchedulerProbe = path
			},
		},
		{
			CommandName: "vela-nminusone-worker-transport-probe",
			SourceName:  "nminusone_worker_transport_probe.go.txt",
			BinaryName:  "vela-worker-transport-probe-n-minus-one",
			assign: func(binaries *nMinusOneBinaries, path string) {
				binaries.WorkerTransportProbe = path
			},
		},
	}
}

type nMinusOneVisibleCompletionResult struct {
	Decision      string    `json:"decision"`
	ArtifactSetID uuid.UUID `json:"artifact_set_id"`
	ChargeID      uuid.UUID `json:"charge_id"`
	CompletedAt   time.Time `json:"completed_at"`
}

type nMinusOneFailureProbeResult struct {
	Disposition string `json:"disposition"`
	AttemptID   string `json:"attempt_id"`
}

type nMinusOneInvoiceChargeResult struct {
	Decision string `json:"decision"`
	ChargeID string `json:"charge_id"`
}

type nMinusOneOutboxProbeResult struct {
	Subject      string `json:"subject"`
	MessageID    string `json:"message_id"`
	Payload      []byte `json:"payload"`
	KnownPayload bool   `json:"known_payload"`
	UnknownBytes int    `json:"unknown_bytes"`
}

type nMinusOneJetStreamOutboxProbeResult struct {
	EventID  uuid.UUID `json:"event_id"`
	Stream   string    `json:"stream"`
	Sequence int64     `json:"sequence"`
}

type nMinusOneAdmissionProbeResult struct {
	JobID    string `json:"job_id"`
	SQLState string `json:"sqlstate"`
	Error    string `json:"error"`
}

type nMinusOneSchedulerProbeResult struct {
	Dispatched  bool      `json:"dispatched"`
	SQLState    string    `json:"sqlstate"`
	Error       string    `json:"error"`
	IntentID    uuid.UUID `json:"intent_id"`
	AttemptID   uuid.UUID `json:"attempt_id"`
	JobID       uuid.UUID `json:"job_id"`
	WorkerID    uuid.UUID `json:"worker_id"`
	WorkerEpoch int64     `json:"worker_epoch"`
	LeaseFence  int64     `json:"lease_fence"`
	LeaseToken  string    `json:"lease_token"`
}

type nMinusOneWorkerProbeRequest struct {
	Action      string
	WorkerEpoch int64
	AttemptID   uuid.UUID
	LeaseFence  int64
	LeaseToken  string
}

type nMinusOneWorkerProbeResult struct {
	AttemptID          uuid.UUID                       `json:"attempt_id"`
	JobID              uuid.UUID                       `json:"job_id"`
	WorkerID           uuid.UUID                       `json:"worker_id"`
	WorkerEpoch        int64                           `json:"worker_epoch"`
	LeaseFence         int64                           `json:"lease_fence"`
	LeaseToken         string                          `json:"lease_token"`
	StartDecision      workercontrol.StartDecision     `json:"start_decision"`
	HeartbeatDecision  workercontrol.HeartbeatDecision `json:"heartbeat_decision"`
	FailureDisposition workercontrol.RetryDisposition  `json:"failure_disposition"`
}

type nMinusOneRetentionProbeResult struct {
	RequestContentExpired   int `json:"request_content_expired"`
	ArtifactRequestsCreated int `json:"artifact_requests_created"`
	Claimed                 int `json:"claimed"`
	Completed               int `json:"completed"`
	Failed                  int `json:"failed"`
	Deleted                 int `json:"deleted"`
	Aborted                 int `json:"aborted"`
}

func buildNMinusOneBinaries(t *testing.T, commit string) nMinusOneBinaries {
	t.Helper()
	adjacentProbes := adjacentNMinusOneProbeDescriptors()
	sourceRoot := t.TempDir()
	archive := exec.Command("git", "archive", "--format=tar", commit)
	archive.Dir = repositoryRoot(t)
	stdout, err := archive.StdoutPipe()
	if err != nil {
		t.Fatalf("open N-1 source archive: %v", err)
	}
	if err := archive.Start(); err != nil {
		t.Fatalf("start N-1 source archive: %v", err)
	}
	reader := tar.NewReader(stdout)
	for {
		header, readErr := reader.Next()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read N-1 source archive: %v", readErr)
		}
		name := filepath.Clean(header.Name)
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			t.Fatalf("N-1 source archive contains unsafe path %q", header.Name)
		}
		target := filepath.Join(sourceRoot, name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				t.Fatalf("create N-1 source directory: %v", err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatalf("create N-1 source parent: %v", err)
			}
			file, openErr := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_EXCL, header.FileInfo().Mode())
			if openErr != nil {
				t.Fatalf("create N-1 source file: %v", openErr)
			}
			_, copyErr := io.Copy(file, reader)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				t.Fatalf("extract N-1 source file: copy=%v close=%v", copyErr, closeErr)
			}
		}
	}
	if err := archive.Wait(); err != nil {
		t.Fatalf("archive N-1 source: %v", err)
	}

	assignmentProbeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", "nminusone_assignment_probe.go.txt",
	))
	if err != nil {
		t.Fatalf("read N-1 Assignment probe: %v", err)
	}
	probeDirectory := filepath.Join(sourceRoot, "cmd", "vela-nminusone-assignment-probe")
	if err := os.MkdirAll(probeDirectory, 0o755); err != nil {
		t.Fatalf("create N-1 Assignment probe directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(probeDirectory, "main.go"), assignmentProbeSource, 0o600); err != nil {
		t.Fatalf("write N-1 Assignment probe: %v", err)
	}
	admissionProbeSourceName := "nminusone_admission_probe.go.txt"
	if commit == financeReconciliationNMinusOneCommit {
		admissionProbeSourceName = "nminusone_finance_reconciliation_admission_probe.go.txt"
	} else if commit == profileCircuitNMinusOneCommit || commit == webhookNMinusOneCommit ||
		commit == humanOIDCNMinusOneCommit ||
		commit == identityAdministrationNMinusOneCommit ||
		commit == humanMembershipNMinusOneCommit ||
		commit == organizationReportingNMinusOneCommit ||
		commit == retentionNMinusOneCommit || commit == incompleteArtifactNMinusOneCommit ||
		commit == debugDumpNMinusOneCommit ||
		commit == breakGlassNMinusOneCommit ||
		commit == fleetControllerNMinusOneCommit {
		admissionProbeSourceName = "nminusone_profile_circuit_admission_probe.go.txt"
	}
	admissionProbeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", admissionProbeSourceName,
	))
	if err != nil {
		t.Fatalf("read N-1 Admission probe: %v", err)
	}
	admissionProbeDirectory := filepath.Join(sourceRoot, "cmd", "vela-nminusone-admission-probe")
	if err := os.MkdirAll(admissionProbeDirectory, 0o755); err != nil {
		t.Fatalf("create N-1 Admission probe directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(admissionProbeDirectory, "main.go"), admissionProbeSource, 0o600); err != nil {
		t.Fatalf("write N-1 Admission probe: %v", err)
	}
	var humanAdmissionProbeDirectory string
	if commit == humanMembershipNMinusOneCommit || commit == organizationReportingNMinusOneCommit {
		humanAdmissionProbeSource, err := os.ReadFile(filepath.Join(
			repositoryRoot(t),
			"internal",
			"integration",
			"testdata",
			"nminusone_human_admission_probe.go.txt",
		))
		if err != nil {
			t.Fatalf("read N-1 Human Admission probe: %v", err)
		}
		humanAdmissionProbeDirectory = filepath.Join(
			sourceRoot, "cmd", "vela-nminusone-human-admission-probe",
		)
		if err := os.MkdirAll(humanAdmissionProbeDirectory, 0o755); err != nil {
			t.Fatalf("create N-1 Human Admission probe directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(humanAdmissionProbeDirectory, "main.go"),
			humanAdmissionProbeSource,
			0o600,
		); err != nil {
			t.Fatalf("write N-1 Human Admission probe: %v", err)
		}
	}
	outboxProbeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", "nminusone_outbox_probe.go.txt",
	))
	if err != nil {
		t.Fatalf("read N-1 Outbox probe: %v", err)
	}
	outboxProbeDirectory := filepath.Join(sourceRoot, "cmd", "vela-nminusone-outbox-probe")
	if err := os.MkdirAll(outboxProbeDirectory, 0o755); err != nil {
		t.Fatalf("create N-1 Outbox probe directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outboxProbeDirectory, "main.go"), outboxProbeSource, 0o600); err != nil {
		t.Fatalf("write N-1 Outbox probe: %v", err)
	}
	var invoiceChargeProbeDirectory string
	if commit == invoiceExportNMinusOneCommit || commit == webhookNMinusOneCommit {
		invoiceChargeProbeSource, err := os.ReadFile(filepath.Join(
			repositoryRoot(t),
			"internal",
			"integration",
			"testdata",
			"nminusone_invoice_charge_probe.go.txt",
		))
		if err != nil {
			t.Fatalf("read N-1 Invoice Charge probe: %v", err)
		}
		invoiceChargeProbeDirectory = filepath.Join(sourceRoot, "cmd", "vela-nminusone-invoice-charge-probe")
		if err := os.MkdirAll(invoiceChargeProbeDirectory, 0o755); err != nil {
			t.Fatalf("create N-1 Invoice Charge probe directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(invoiceChargeProbeDirectory, "main.go"),
			invoiceChargeProbeSource,
			0o600,
		); err != nil {
			t.Fatalf("write N-1 Invoice Charge probe: %v", err)
		}
	}
	var failureProbeDirectory string
	if commit == profileCircuitNMinusOneCommit {
		failureProbeSource, err := os.ReadFile(filepath.Join(
			repositoryRoot(t),
			"internal",
			"integration",
			"testdata",
			"nminusone_failure_probe.go.txt",
		))
		if err != nil {
			t.Fatalf("read N-1 failure probe: %v", err)
		}
		failureProbeDirectory = filepath.Join(sourceRoot, "cmd", "vela-nminusone-failure-probe")
		if err := os.MkdirAll(failureProbeDirectory, 0o755); err != nil {
			t.Fatalf("create N-1 failure probe directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(failureProbeDirectory, "main.go"),
			failureProbeSource,
			0o600,
		); err != nil {
			t.Fatalf("write N-1 failure probe: %v", err)
		}
	}
	var visibleCompletionProbeDirectory string
	if commit == retentionNMinusOneCommit || commit == incompleteArtifactNMinusOneCommit {
		visibleCompletionProbeSource, err := os.ReadFile(filepath.Join(
			repositoryRoot(t),
			"internal",
			"integration",
			"testdata",
			"nminusone_visible_completion_probe.go.txt",
		))
		if err != nil {
			t.Fatalf("read N-1 Visible Completion probe: %v", err)
		}
		visibleCompletionProbeDirectory = filepath.Join(
			sourceRoot, "cmd", "vela-nminusone-visible-completion-probe",
		)
		if err := os.MkdirAll(visibleCompletionProbeDirectory, 0o755); err != nil {
			t.Fatalf("create N-1 Visible Completion probe directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(visibleCompletionProbeDirectory, "main.go"),
			visibleCompletionProbeSource,
			0o600,
		); err != nil {
			t.Fatalf("write N-1 Visible Completion probe: %v", err)
		}
	}
	var retentionProbeDirectory string
	if commit == incompleteArtifactNMinusOneCommit || commit == debugDumpNMinusOneCommit {
		retentionProbeSource, err := os.ReadFile(filepath.Join(
			repositoryRoot(t),
			"internal",
			"integration",
			"testdata",
			"nminusone_retention_probe.go.txt",
		))
		if err != nil {
			t.Fatalf("read N-1 retention probe: %v", err)
		}
		retentionProbeDirectory = filepath.Join(
			sourceRoot, "cmd", "vela-nminusone-retention-probe",
		)
		if err := os.MkdirAll(retentionProbeDirectory, 0o755); err != nil {
			t.Fatalf("create N-1 retention probe directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(retentionProbeDirectory, "main.go"),
			retentionProbeSource,
			0o600,
		); err != nil {
			t.Fatalf("write N-1 retention probe: %v", err)
		}
	}
	adjacentProbeDirectories := map[string]string{}
	if commit == adjacentRolloutNMinusOneCommit {
		for _, probe := range adjacentProbes {
			source, readErr := os.ReadFile(filepath.Join(
				repositoryRoot(t), "internal", "integration", "testdata", probe.SourceName,
			))
			if readErr != nil {
				t.Fatalf("read adjacent N-1 %s: %v", probe.SourceName, readErr)
			}
			directory := filepath.Join(sourceRoot, "cmd", probe.CommandName)
			if err := os.MkdirAll(directory, 0o755); err != nil {
				t.Fatalf("create adjacent N-1 %s directory: %v", probe.CommandName, err)
			}
			if err := os.WriteFile(filepath.Join(directory, "main.go"), source, 0o600); err != nil {
				t.Fatalf("write adjacent N-1 %s: %v", probe.CommandName, err)
			}
			adjacentProbeDirectories[probe.CommandName] = directory
		}
	}

	binaryDirectory := t.TempDir()
	binaries := nMinusOneBinaries{
		Control:         filepath.Join(binaryDirectory, "vela-control-n-minus-one"),
		AdmissionProbe:  filepath.Join(binaryDirectory, "vela-admission-probe-n-minus-one"),
		AssignmentProbe: filepath.Join(binaryDirectory, "vela-assignment-probe-n-minus-one"),
		OutboxProbe:     filepath.Join(binaryDirectory, "vela-outbox-probe-n-minus-one"),
	}
	if commit == humanMembershipNMinusOneCommit || commit == organizationReportingNMinusOneCommit {
		binaries.HumanAdmissionProbe = filepath.Join(
			binaryDirectory, "vela-human-admission-probe-n-minus-one",
		)
	}
	if commit == invoiceExportNMinusOneCommit || commit == webhookNMinusOneCommit {
		binaries.InvoiceChargeProbe = filepath.Join(
			binaryDirectory,
			"vela-invoice-charge-probe-n-minus-one",
		)
	}
	if commit == profileCircuitNMinusOneCommit {
		binaries.FailureProbe = filepath.Join(binaryDirectory, "vela-failure-probe-n-minus-one")
	}
	if commit == retentionNMinusOneCommit || commit == incompleteArtifactNMinusOneCommit {
		binaries.VisibleCompletionProbe = filepath.Join(
			binaryDirectory, "vela-visible-completion-probe-n-minus-one",
		)
	}
	if commit == incompleteArtifactNMinusOneCommit || commit == debugDumpNMinusOneCommit {
		binaries.RetentionProbe = filepath.Join(
			binaryDirectory, "vela-retention-probe-n-minus-one",
		)
	}
	if commit == adjacentRolloutNMinusOneCommit {
		for _, probe := range adjacentProbes {
			probe.assign(&binaries, filepath.Join(binaryDirectory, probe.BinaryName))
		}
	}
	build := exec.Command(
		"go", "build",
		"-o", binaries.Control, "./cmd/vela-control",
	)
	build.Dir = sourceRoot
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build N-1 vela-control: %v\n%s", err, output)
	}
	if commit != invoiceExportNMinusOneCommit {
		build = exec.Command(
			"go", "build",
			"-o", binaries.AdmissionProbe, "./cmd/vela-nminusone-admission-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Admission probe: %v\n%s", err, output)
		}
		if commit == humanMembershipNMinusOneCommit || commit == organizationReportingNMinusOneCommit {
			build = exec.Command(
				"go", "build",
				"-o", binaries.HumanAdmissionProbe, "./cmd/vela-nminusone-human-admission-probe",
			)
			build.Dir = sourceRoot
			build.Env = environmentWith(map[string]string{"GOWORK": "off"})
			if output, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build N-1 Human Admission probe: %v\n%s", err, output)
			}
		}
		build = exec.Command(
			"go", "build",
			"-o", binaries.AssignmentProbe, "./cmd/vela-nminusone-assignment-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Assignment probe: %v\n%s", err, output)
		}
		build = exec.Command(
			"go", "build",
			"-o", binaries.OutboxProbe, "./cmd/vela-nminusone-outbox-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Outbox probe: %v\n%s", err, output)
		}
	}
	if commit == invoiceExportNMinusOneCommit || commit == webhookNMinusOneCommit {
		build = exec.Command(
			"go",
			"build",
			"-o",
			binaries.InvoiceChargeProbe,
			"./cmd/vela-nminusone-invoice-charge-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Invoice Charge probe: %v\n%s", err, output)
		}
	}
	if commit == profileCircuitNMinusOneCommit {
		build = exec.Command(
			"go",
			"build",
			"-o",
			binaries.FailureProbe,
			"./cmd/vela-nminusone-failure-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 failure probe: %v\n%s", err, output)
		}
	}
	if commit == retentionNMinusOneCommit || commit == incompleteArtifactNMinusOneCommit {
		build = exec.Command(
			"go",
			"build",
			"-o",
			binaries.VisibleCompletionProbe,
			"./cmd/vela-nminusone-visible-completion-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Visible Completion probe: %v\n%s", err, output)
		}
	}
	if commit == incompleteArtifactNMinusOneCommit || commit == debugDumpNMinusOneCommit {
		build = exec.Command(
			"go",
			"build",
			"-o",
			binaries.RetentionProbe,
			"./cmd/vela-nminusone-retention-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 retention probe: %v\n%s", err, output)
		}
	}
	if commit == adjacentRolloutNMinusOneCommit {
		for _, probe := range adjacentProbes {
			binary := filepath.Join(binaryDirectory, probe.BinaryName)
			build = exec.Command("go", "build", "-o", binary, "./cmd/"+probe.CommandName)
			build.Dir = sourceRoot
			build.Env = environmentWith(map[string]string{"GOWORK": "off"})
			if output, err := build.CombinedOutput(); err != nil {
				t.Fatalf(
					"build adjacent N-1 %s from %s: %v\n%s",
					probe.CommandName,
					adjacentProbeDirectories[probe.CommandName],
					err,
					output,
				)
			}
		}
	}
	return binaries
}

func runNMinusOneVisibleCompletionProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	expectedJobVersion int64,
	artifactIDs []uuid.UUID,
) nMinusOneVisibleCompletionResult {
	t.Helper()
	encodedArtifactIDs := make([]string, len(artifactIDs))
	for index, artifactID := range artifactIDs {
		encodedArtifactIDs[index] = artifactID.String()
	}
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_WORKER_ID":            worker.ID.String(),
		"VELA_ATTEMPT_ID":           credentials.AttemptID.String(),
		"VELA_WORKER_EPOCH":         fmt.Sprint(credentials.WorkerEpoch),
		"VELA_LEASE_FENCE":          fmt.Sprint(credentials.Fence),
		"VELA_LEASE_TOKEN":          credentials.Token,
		"VELA_EXPECTED_JOB_VERSION": fmt.Sprint(expectedJobVersion),
		"VELA_ARTIFACT_IDS":         strings.Join(encodedArtifactIDs, ","),
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Visible Completion probe: %v\n%s", err, output)
	}
	var result nMinusOneVisibleCompletionResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Visible Completion probe: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneRetentionProbe(
	t *testing.T,
	binary string,
	adminDSN string,
) nMinusOneRetentionProbeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_RETENTION_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_retention_login",
			"vela-retention-password",
		),
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 retention probe: %v\n%s", err, output)
	}
	var result nMinusOneRetentionProbeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 retention probe: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneFailureProbeProcess(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID string,
	workerID string,
) ([]byte, error) {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_internal_login",
			"vela-internal-password",
		),
		"VELA_JOB_ID":    jobID,
		"VELA_WORKER_ID": workerID,
	})
	return command.CombinedOutput()
}

func runNMinusOneInvoiceChargeProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID uuid.UUID,
) nMinusOneInvoiceChargeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_CANCEL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_cancel_login", "vela-cancel-password",
		),
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(testCredentialPepper),
		"VELA_BEARER_CREDENTIAL":        testBearerCredential(),
		"VELA_PROJECT_ID":               testProjectID,
		"VELA_JOB_ID":                   jobID.String(),
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Invoice Charge writer probe: %v\n%s", err, output)
	}
	var result nMinusOneInvoiceChargeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Invoice Charge result: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneAdmissionProbe(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	return runNMinusOneAdmissionProbeWithKey(t, binary, adminDSN, "")
}

func runNMinusOneAdmissionProbeWithKey(
	t *testing.T,
	binary string,
	adminDSN string,
	idempotencyKey string,
) string {
	t.Helper()
	command := exec.Command(binary)
	command.Env = nMinusOneAdmissionProbeEnvironment(t, adminDSN, idempotencyKey, false)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Admission writer probe: %v\n%s", err, output)
	}
	var result struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Admission probe output: %v\n%s", err, output)
	}
	if _, err := uuid.Parse(result.JobID); err != nil {
		t.Fatalf("N-1 Admission probe returned invalid Job id %q", result.JobID)
	}
	return result.JobID
}

func runNMinusOneAdmissionFailureProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	idempotencyKey string,
) nMinusOneAdmissionProbeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = nMinusOneAdmissionProbeEnvironment(t, adminDSN, idempotencyKey, true)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Admission failure probe: %v\n%s", err, output)
	}
	var result nMinusOneAdmissionProbeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Admission failure probe: %v\n%s", err, output)
	}
	return result
}

func nMinusOneAdmissionProbeEnvironment(
	t *testing.T,
	adminDSN string,
	idempotencyKey string,
	expectDatabaseFailure bool,
) []string {
	t.Helper()
	overrides := map[string]string{
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_request_login", "vela-request-password",
		),
		"VELA_SCHEDULER_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_login", "vela-scheduler-password",
		),
		"VELA_SCHEDULER_INBOX_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_inbox_login", "vela-scheduler-inbox-password",
		),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(testCredentialPepper),
		"VELA_BEARER_CREDENTIAL":        testBearerCredential(),
		"VELA_PROJECT_ID":               testProjectID,
		"VELA_IDEMPOTENCY_KEY":          idempotencyKey,
	}
	if expectDatabaseFailure {
		overrides["VELA_EXPECT_DATABASE_FAILURE"] = "true"
	}
	return environmentWith(overrides)
}

func runNMinusOneHumanAdmissionProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	subject string,
) (string, uuid.UUID) {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_HUMAN_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_human_auth_login", "vela-human-auth-password",
		),
		"VELA_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_request_login", "vela-request-password",
		),
		"VELA_SCHEDULER_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_login", "vela-scheduler-password",
		),
		"VELA_SCHEDULER_INBOX_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_inbox_login", "vela-scheduler-inbox-password",
		),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(testCredentialPepper),
		"VELA_OIDC_ISSUER":              "https://identity.example.com",
		"VELA_OIDC_SUBJECT":             subject,
		"VELA_PROJECT_ID":               testProjectID,
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Human Admission probe: %v\n%s", err, output)
	}
	var result struct {
		JobID       string    `json:"job_id"`
		PrincipalID uuid.UUID `json:"principal_id"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Human Admission output: %v\n%s", err, output)
	}
	if _, err := uuid.Parse(result.JobID); err != nil || result.PrincipalID == uuid.Nil {
		t.Fatalf(
			"N-1 Human Admission returned Job %q Principal %s error=%v",
			result.JobID, result.PrincipalID, err,
		)
	}
	return result.JobID, result.PrincipalID
}

func runNMinusOneAssignmentProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID string,
	workerID string,
	workerEpoch int64,
	expectedJobVersion int64,
) string {
	t.Helper()
	output, err := runNMinusOneAssignmentProbeProcess(
		t, binary, adminDSN, jobID, workerID, workerEpoch, expectedJobVersion,
	)
	if err != nil {
		t.Fatalf("run N-1 Assignment writer/replay probe: %v\n%s", err, output)
	}
	var result struct {
		AttemptID string `json:"attempt_id"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Assignment probe output: %v\n%s", err, output)
	}
	if _, err := uuid.Parse(result.AttemptID); err != nil {
		t.Fatalf("N-1 Assignment probe returned invalid Attempt id %q", result.AttemptID)
	}
	return result.AttemptID
}

func runNMinusOneAssignmentProbeProcess(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID string,
	workerID string,
	workerEpoch int64,
	expectedJobVersion int64,
) ([]byte, error) {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_WORKER_ID":            workerID,
		"VELA_WORKER_EPOCH":         fmt.Sprintf("%d", workerEpoch),
		"VELA_EXPECTED_JOB_VERSION": fmt.Sprintf("%d", expectedJobVersion),
		"VELA_JOB_ID":               jobID,
		"VELA_PROFILE_REVISION_ID":  "00000000-0000-0000-0000-000000000014",
	})
	return command.CombinedOutput()
}

func runNMinusOneOutboxProbe(
	t *testing.T,
	binary, adminDSN string,
) nMinusOneOutboxProbeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Outbox publisher probe: %v\n%s", err, output)
	}
	var result nMinusOneOutboxProbeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Outbox probe output: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneJetStreamOutboxProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	natsURL string,
) nMinusOneJetStreamOutboxProbeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_NATS_URL": natsURL,
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run adjacent N-1 JetStream Outbox probe: %v\n%s", err, output)
	}
	var result nMinusOneJetStreamOutboxProbeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode adjacent N-1 JetStream Outbox probe: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneSchedulerProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	workerPoolID uuid.UUID,
	schedulerID string,
) nMinusOneSchedulerProbeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_SCHEDULER_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_login", "vela-scheduler-password",
		),
		"VELA_WORKER_POOL_ID": workerPoolID.String(),
		"VELA_SCHEDULER_ID":   schedulerID,
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run adjacent N-1 Scheduler probe: %v\n%s", err, output)
	}
	var result nMinusOneSchedulerProbeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode adjacent N-1 Scheduler probe: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneWorkerTransportProbe(
	t *testing.T,
	binary string,
	endpoint mixedVersionWorkerEndpoint,
	request nMinusOneWorkerProbeRequest,
) nMinusOneWorkerProbeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_WORKER_CONTROL_ADDRESS":  endpoint.Address,
		"VELA_WORKER_TLS_CERT_FILE":    endpoint.ClientCertificateFile,
		"VELA_WORKER_TLS_KEY_FILE":     endpoint.ClientKeyFile,
		"VELA_WORKER_TLS_ROOT_CA_FILE": endpoint.ServerCAFile,
		"VELA_WORKER_TLS_SERVER_NAME":  endpoint.ServerName,
		"VELA_WORKER_PROBE_ACTION":     request.Action,
		"VELA_WORKER_EPOCH":            fmt.Sprint(request.WorkerEpoch),
		"VELA_ATTEMPT_ID":              request.AttemptID.String(),
		"VELA_LEASE_FENCE":             fmt.Sprint(request.LeaseFence),
		"VELA_LEASE_TOKEN":             request.LeaseToken,
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run adjacent N-1 Worker transport probe: %v\n%s", err, output)
	}
	var result nMinusOneWorkerProbeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode adjacent N-1 Worker transport probe: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneControl(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	temporary := t.TempDir()
	credentialsFile := filepath.Join(temporary, "invalid.creds")
	rootCAFile := filepath.Join(temporary, "invalid-ca.pem")
	if err := os.WriteFile(credentialsFile, []byte("not-a-nats-credential\n"), 0o600); err != nil {
		t.Fatalf("write NATS credential sentinel: %v", err)
	}
	if err := os.WriteFile(rootCAFile, []byte("not-a-ca\n"), 0o600); err != nil {
		t.Fatalf("write NATS root CA sentinel: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = environmentWith(map[string]string{
		"VELA_HTTP_ADDRESS":          "127.0.0.1:0",
		"VELA_AUTH_DATABASE_URL":     roleDatabaseURL(t, adminDSN, "vela_auth_login", "vela-auth-password"),
		"VELA_REQUEST_DATABASE_URL":  roleDatabaseURL(t, adminDSN, "vela_request_login", "vela-request-password"),
		"VELA_CANCEL_DATABASE_URL":   roleDatabaseURL(t, adminDSN, "vela_cancel_login", "vela-cancel-password"),
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(t, adminDSN, "vela_internal_login", "vela-internal-password"),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(
			[]byte("0123456789abcdef0123456789abcdef"),
		),
		"VELA_NATS_URL":              "nats://127.0.0.1:1",
		"VELA_NATS_CREDENTIALS_FILE": credentialsFile,
		"VELA_NATS_ROOT_CA_FILE":     rootCAFile,
	})
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("N-1 vela-control startup timed out: %v\n%s", ctx.Err(), output)
	}
	if err == nil {
		t.Fatalf("N-1 vela-control unexpectedly passed the deliberate NATS sentinel")
	}
	return string(output)
}

type controlStartupProbeOptions struct {
	currentDebugDumpRoles bool
	currentNodeAgentFile  bool
}

func runSchedulerNMinusOneStartupProbe(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	return runControlStartupProbe(t, binary, adminDSN, controlStartupProbeOptions{})
}

func runAdjacentNMinusOneStartupProbe(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	return runControlStartupProbe(t, binary, adminDSN, controlStartupProbeOptions{
		currentNodeAgentFile: true,
	})
}

func runCurrentControlStartupProbe(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	return runControlStartupProbe(t, binary, adminDSN, controlStartupProbeOptions{
		currentDebugDumpRoles: true,
		currentNodeAgentFile:  true,
	})
}

func runControlStartupProbe(
	t *testing.T,
	binary, adminDSN string,
	options controlStartupProbeOptions,
) string {
	t.Helper()
	temporary := t.TempDir()
	webhookKeyringFile := filepath.Join(temporary, "webhook-keyring.json")
	webhookKey := base64.StdEncoding.EncodeToString(
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err := os.WriteFile(
		webhookKeyringFile,
		[]byte(`{"webhook-key-v1":"`+webhookKey+`"}`),
		0o600,
	); err != nil {
		t.Fatalf("write N-1 Webhook keyring: %v", err)
	}
	nodeAgentFile := "/missing/remediation-node-agents.json"
	if options.currentNodeAgentFile {
		nodeIdentity := "slice-29-node"
		nodeWorkerID := uuid.MustParse("23000000-0000-0000-0000-000000000443")
		nodeAgentFile = filepath.Join(temporary, "node-agents.json")
		nodeAgentFixture, err := json.Marshal(map[string]map[string]any{
			nodeIdentity: {
				"address":      "127.0.0.1:1",
				"server_name":  "node-agent.internal",
				"worker_id":    nodeWorkerID,
				"worker_epoch": 1,
				"spiffe_identity": "spiffe://vela.internal/node-agent/" +
					base64.RawURLEncoding.EncodeToString([]byte(nodeIdentity)) + "/" + nodeWorkerID.String(),
			},
		})
		if err != nil {
			t.Fatalf("encode current Node Agent endpoint fixture: %v", err)
		}
		if err := os.WriteFile(nodeAgentFile, nodeAgentFixture, 0o600); err != nil {
			t.Fatalf("write current Node Agent endpoint fixture: %v", err)
		}
	}
	debugDumpRequestDatabaseURL := ""
	debugDumpAuditRequestDatabaseURL := ""
	if options.currentDebugDumpRoles {
		debugDumpRequestDatabaseURL = roleDatabaseURL(
			t, adminDSN, "vela_debug_dump_request_login", "vela-debug-dump-request-password",
		)
		debugDumpAuditRequestDatabaseURL = roleDatabaseURL(
			t,
			adminDSN,
			"vela_debug_dump_audit_request_login",
			"vela-debug-dump-audit-request-password",
		)
	}
	caCertificate, caKey, caPEM := issueWorkerTransportTestCA(t)
	financeCertificate, financeKey := issueWorkerTransportTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "finance-reconciliation.internal"},
		[]string{"finance-reconciliation.internal"},
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	financeCertificateFile := filepath.Join(temporary, "finance-reconciliation.crt")
	financeKeyFile := filepath.Join(temporary, "finance-reconciliation.key")
	financeClientCAFile := filepath.Join(temporary, "finance-reconciliation-client-ca.crt")
	for path, contents := range map[string][]byte{
		financeCertificateFile: financeCertificate,
		financeKeyFile:         financeKey,
		financeClientCAFile:    caPEM,
	} {
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatalf("write N-1 Finance Reconciliation TLS fixture %s: %v", path, err)
		}
	}
	financeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve N-1 Finance Reconciliation address: %v", err)
	}
	financeAddress := financeListener.Addr().String()
	if err := financeListener.Close(); err != nil {
		t.Fatalf("release N-1 Finance Reconciliation address: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = environmentWith(map[string]string{
		"VELA_HTTP_ADDRESS": "127.0.0.1:0",
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_HUMAN_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_human_auth_login", "vela-human-auth-password",
		),
		"VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_human_membership_auth_login",
			"vela-human-membership-auth-password",
		),
		"VELA_IDENTITY_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_identity_request_login", "vela-identity-request-password",
		),
		"VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_human_membership_request_login",
			"vela-human-membership-request-password",
		),
		"VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_organization_billing_request_login",
			"vela-organization-billing-request-password",
		),
		"VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_organization_audit_request_login",
			"vela-organization-audit-request-password",
		),
		"VELA_RETENTION_REQUEST_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_retention_request_login",
			"vela-retention-request-password",
		),
		"VELA_DEBUG_DUMP_REQUEST_DATABASE_URL":       debugDumpRequestDatabaseURL,
		"VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL": debugDumpAuditRequestDatabaseURL,
		"VELA_RETENTION_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_retention_login", "vela-retention-password",
		),
		"VELA_RETENTION_RECONCILER_ID": "current-retention-startup-probe",
		"VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_platform_operator_auth_login", "vela-platform-operator-auth-password",
		),
		"VELA_BREAK_GLASS_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_break_glass_request_login", "vela-break-glass-request-password",
		),
		"VELA_BREAK_GLASS_AUDIT_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_break_glass_audit_request_login", "vela-break-glass-audit-request-password",
		),
		"VELA_OIDC_ISSUER":            "https://identity.example.com",
		"VELA_OIDC_AUDIENCE":          "vela-control",
		"VELA_OIDC_JWKS_URL":          "https://identity.example.com/.well-known/jwks.json",
		"VELA_PLATFORM_OIDC_ISSUER":   "https://platform-identity.example.com",
		"VELA_PLATFORM_OIDC_AUDIENCE": "vela-platform-control",
		"VELA_PLATFORM_OIDC_JWKS_URL": "https://platform-identity.example.com/.well-known/jwks.json",
		"VELA_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_request_login", "vela-request-password",
		),
		"VELA_ARTIFACT_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_artifact_request_login", "vela-artifact-request-password",
		),
		"VELA_CANCEL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_cancel_login", "vela-cancel-password",
		),
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_REMEDIATION_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_remediation_login", "vela-remediation-password",
		),
		"VELA_REMEDIATION_ACTOR_IDENTITY":   "controller/n-minus-one-startup-probe",
		"VELA_REMEDIATION_NODE_AGENTS_FILE": nodeAgentFile,
		"VELA_REMEDIATION_TLS_CERT_FILE":    "/missing/remediation-client.crt",
		"VELA_REMEDIATION_TLS_KEY_FILE":     "/missing/remediation-client.key",
		"VELA_REMEDIATION_TLS_ROOT_CA_FILE": "/missing/remediation-root-ca.crt",
		"VELA_SCHEDULER_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_login", "vela-scheduler-password",
		),
		"VELA_SCHEDULER_INBOX_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_inbox_login", "vela-scheduler-inbox-password",
		),
		"VELA_SCHEDULER_ID": "n-minus-one-startup-probe",
		"VELA_BILLING_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_billing_login", "vela-billing-password",
		),
		"VELA_FINANCE_RECONCILIATION_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_finance_reconciliation_login",
			"vela-finance-reconciliation-password",
		),
		"VELA_FINANCE_RECONCILIATION_ADDR":             financeAddress,
		"VELA_FINANCE_RECONCILIATION_SERVER_CERT_FILE": financeCertificateFile,
		"VELA_FINANCE_RECONCILIATION_SERVER_KEY_FILE":  financeKeyFile,
		"VELA_FINANCE_RECONCILIATION_CLIENT_CA_FILE":   financeClientCAFile,
		"VELA_WEBHOOK_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_webhook_request_login", "vela-webhook-request-password",
		),
		"VELA_WEBHOOK_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_webhook_login", "vela-webhook-password",
		),
		"VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID":       "webhook-key-v1",
		"VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE":        webhookKeyringFile,
		"VELA_WEBHOOK_DISPATCHER_ID":                  "n-minus-one-startup-probe",
		"VELA_INVOICE_EXPORTER_ID":                    "n-minus-one-startup-probe",
		"VELA_INVOICE_EXPORT_ENDPOINT":                "https://127.0.0.1:1/invoices",
		"VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE":       "/missing/invoice-export-token",
		"VELA_CREDENTIAL_PEPPER_BASE64":               base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"VELA_NATS_URL":                               "nats://127.0.0.1:1",
		"VELA_NATS_CREDENTIALS_FILE":                  "/missing/nats.creds",
		"VELA_NATS_ROOT_CA_FILE":                      "/missing/nats-ca.pem",
		"VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY":         "schema-probe-account",
		"VELA_NATS_OUTBOX_ACCOUNT_SIGNER_PUBLIC_KEYS": "schema-probe-signer",
		"VELA_NATS_OUTBOX_USER_PUBLIC_KEYS":           "schema-probe-user",
		"VELA_NATS_SCHEDULER_CREDENTIALS_FILE":        "/missing/nats-scheduler.creds",
		"VELA_NATS_SCHEDULER_USER_PUBLIC_KEYS":        "schema-probe-scheduler-user",
		"VELA_ARTIFACT_S3_ENDPOINT":                   "http://127.0.0.1:1",
		"VELA_ARTIFACT_S3_REGION":                     "us-east-1",
		"VELA_ARTIFACT_S3_BUCKET":                     "vela-artifacts",
		"VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE":         "/missing/s3-access-key-id",
		"VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE":     "/missing/s3-secret-access-key",
		"VELA_LEASE_ACTIVE_KEY_ID":                    "lease-key-v1",
		"VELA_LEASE_KEYRING_FILE":                     "/missing/lease-keyring.json",
		"VELA_ARTIFACT_VALIDATOR_HELPER_PATH":         "/missing/vela-artifact-validator",
		"VELA_ARTIFACT_FFPROBE_PATH":                  "/missing/ffprobe",
		"VELA_ARTIFACT_SANDBOX_ROOT":                  "/missing/sandbox",
		"VELA_ARTIFACT_SPOOL_DIRECTORY":               "/missing/spool",
		"VELA_ARTIFACT_FFPROBE_VERSION":               "8.0.1",
		"VELA_ARTIFACT_VALIDATOR_REVISION":            "ffprobe-8.0.1-sandbox-v1",
		"VELA_ARTIFACT_RECONCILER_ID":                 "spiffe://vela.internal/reconciler/artifact-finalization",
		"VELA_WORKER_GRPC_TLS_CERT_FILE":              "/missing/worker-control.crt",
		"VELA_WORKER_GRPC_TLS_KEY_FILE":               "/missing/worker-control.key",
		"VELA_WORKER_GRPC_CLIENT_CA_FILE":             "/missing/worker-client-ca.crt",
		"VELA_FLEET_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_fleet_login", "vela-fleet-password",
		),
		"VELA_FLEET_GRPC_TLS_CERT_FILE":        "/missing/fleet-control.crt",
		"VELA_FLEET_GRPC_TLS_KEY_FILE":         "/missing/fleet-control.key",
		"VELA_FLEET_GRPC_CLIENT_CA_FILE":       "/missing/fleet-client-ca.crt",
		"VELA_FLEET_CONTROLLER_SPIFFE_ID":      "spiffe://vela.internal/fleet-controller/startup-probe",
		"VELA_FLEET_CONTROLLER_ACTOR_IDENTITY": "fleet-controller/startup-probe",
	})
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("exact Scheduler N-1 startup timed out: %v\n%s", ctx.Err(), output)
	}
	if err == nil {
		t.Fatalf("exact Scheduler N-1 startup unexpectedly passed the Artifact sentinel")
	}
	return string(output)
}

func assertNMinusOneDatabaseStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "open auth database pool") ||
		strings.Contains(output, "open request database pool") ||
		strings.Contains(output, "open internal database pool") {
		t.Fatalf("N-1 database startup failed before NATS sentinel:\n%s", output)
	}
	if !strings.Contains(output, "connect NATS") {
		t.Fatalf("N-1 startup did not reach the NATS sentinel:\n%s", output)
	}
}

func assertAdjacentNMinusOneControlStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "database pool") {
		t.Fatalf("adjacent N-1 control startup failed database role preflight:\n%s", output)
	}
	if !strings.Contains(output, "open Invoice export bearer token file") {
		t.Fatalf("adjacent N-1 control did not reach the post-role-preflight sentinel:\n%s", output)
	}
}

func assertNMinusOneRequestStartupRejected(t *testing.T, output string) {
	t.Helper()
	if !strings.Contains(output, "open request database pool") ||
		!strings.Contains(output, "request transaction privilege boundary") {
		t.Fatalf("N-1 startup was not rejected at the request-role boundary:\n%s", output)
	}
}

func roleDatabaseURL(t *testing.T, adminDSN, username, password string) string {
	t.Helper()
	dsn, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	dsn.User = url.UserPassword(username, password)
	return dsn.String()
}

func environmentWith(overrides map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		if _, overridden := overrides[name]; !overridden {
			environment = append(environment, entry)
		}
	}
	for name, value := range overrides {
		environment = append(environment, fmt.Sprintf("%s=%s", name, value))
	}
	return environment
}
