//go:build integration

package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	nMinusOneRenewalControlCommit = "450dd5c379ed7d26588e2a76140f0b3281acfbb2"
	nMinusOneFailureControlCommit = "9cb1a522e20490ef41bab535fde206a947118d11"
	nMinusOneCancellationCommit   = "d0a8c0105a09b7f538e79400a7affd2a6c700744"
)

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
	Control         string
	AdmissionProbe  string
	AssignmentProbe string
	OutboxProbe     string
}

type nMinusOneOutboxProbeResult struct {
	Subject      string `json:"subject"`
	MessageID    string `json:"message_id"`
	Payload      []byte `json:"payload"`
	KnownPayload bool   `json:"known_payload"`
	UnknownBytes int    `json:"unknown_bytes"`
}

func buildNMinusOneBinaries(t *testing.T, commit string) nMinusOneBinaries {
	t.Helper()
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
	admissionProbeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", "nminusone_admission_probe.go.txt",
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

	binaryDirectory := t.TempDir()
	binaries := nMinusOneBinaries{
		Control:         filepath.Join(binaryDirectory, "vela-control-n-minus-one"),
		AdmissionProbe:  filepath.Join(binaryDirectory, "vela-admission-probe-n-minus-one"),
		AssignmentProbe: filepath.Join(binaryDirectory, "vela-assignment-probe-n-minus-one"),
		OutboxProbe:     filepath.Join(binaryDirectory, "vela-outbox-probe-n-minus-one"),
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
	build = exec.Command(
		"go", "build",
		"-o", binaries.AdmissionProbe, "./cmd/vela-nminusone-admission-probe",
	)
	build.Dir = sourceRoot
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build N-1 Admission probe: %v\n%s", err, output)
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
	return binaries
}

func runNMinusOneAdmissionProbe(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_request_login", "vela-request-password",
		),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(testCredentialPepper),
		"VELA_BEARER_CREDENTIAL":        testBearerCredential(),
		"VELA_PROJECT_ID":               testProjectID,
	})
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
