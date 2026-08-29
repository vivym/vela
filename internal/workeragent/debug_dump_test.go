package workeragent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/debugdumpcontract"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workerrecovery"
	"github.com/vivym/vela/internal/workertransport"
)

const testDebugDumpPayload = `{"authorization_id":"61000000-0000-0000-0000-000000000004",` +
	`"attempt_id":"61000000-0000-0000-0000-000000000002",` +
	`"backend_stage":"dit","failure_class":"TRANSIENT_BACKEND",` +
	`"failure_fingerprint":"dit/timeout","gpu_uuids":[],` +
	`"inference_backend_revision":"sglang@abc",` +
	`"job_id":"61000000-0000-0000-0000-000000000003",` +
	`"lease_fence":3,"retry_recommended":true,"schema_version":1,` +
	`"worker_epoch":13,"worker_id":"61000000-0000-0000-0000-000000000001",` +
	`"worker_reusable":true}`

func TestRunOnceUploadsAuthorizedDebugDumpBeforeFail(t *testing.T) {
	events := []string{}
	agent, control, runner, debugControl, debugUploader := newDebugDumpFailureAgent(
		t, &events, debugDumpFailureMode{},
	)

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeRetryScheduled {
		t.Fatalf("RunOnce outcome = %q", result.Outcome)
	}
	wantEvents := []string{
		"control.acquire", "runner.prepare", "control.start", "runner.start", "runner.status",
		"debug.claim", "debug.put", "debug.complete", "control.fail",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("failure events = %#v, want %#v", events, wantEvents)
	}
	if runner.spec.DebugDumpAuthorization == nil ||
		runner.spec.DebugDumpAuthorization.AuthorizationID != debugControl.authorizationID {
		t.Fatalf("Runner ExecutionSpec authorization = %#v", runner.spec.DebugDumpAuthorization)
	}
	if len(debugUploader.payloads) != 1 ||
		string(debugUploader.payloads[0]) != testDebugDumpPayload {
		t.Fatalf("debug dump payloads = %#v", debugUploader.payloads)
	}
	if len(debugControl.intents) != 1 || len(debugControl.reports) != 1 ||
		debugControl.intents[0].DebugDumpID != debugControl.reports[0].DebugDumpID ||
		debugControl.reports[0].Report.CompletedParts[0].ChecksumSHA256 !=
			base64.StdEncoding.EncodeToString(debugControl.reports[0].Report.SHA256[:]) {
		t.Fatalf("debug dump control receipts = intents %#v reports %#v", debugControl.intents, debugControl.reports)
	}
	if control.failureObservation.FailureClass != "TRANSIENT_BACKEND" {
		t.Fatalf("failure observation = %#v", control.failureObservation)
	}
}

func TestRunOnceDebugDumpFailureNeverBlocksFail(t *testing.T) {
	tests := []struct {
		name string
		mode debugDumpFailureMode
	}{
		{name: "claim rejected", mode: debugDumpFailureMode{claimRejected: true}},
		{name: "claim failed", mode: debugDumpFailureMode{claimError: errors.New("claim unavailable")}},
		{name: "object store failed", mode: debugDumpFailureMode{uploadError: errors.New("PUT unavailable")}},
		{name: "completion failed", mode: debugDumpFailureMode{completeError: errors.New("completion unavailable")}},
		{name: "upload timed out", mode: debugDumpFailureMode{blockUpload: true, timeout: 10 * time.Millisecond}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			agent, _, _, _, _ := newDebugDumpFailureAgent(t, &events, test.mode)

			result, err := agent.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if result.Outcome != OutcomeRetryScheduled || len(events) == 0 ||
				events[len(events)-1] != "control.fail" {
				t.Fatalf("RunOnce result = %#v events = %#v", result, events)
			}
		})
	}
}

type debugDumpFailureMode struct {
	claimRejected bool
	claimError    error
	uploadError   error
	completeError error
	blockUpload   bool
	timeout       time.Duration
}

func newDebugDumpFailureAgent(
	t *testing.T,
	events *[]string,
	mode debugDumpFailureMode,
) (*Agent, *recordingControlPlane, *recordingRunner, *recordingDebugDumpControl, *recordingDebugDumpUploader) {
	t.Helper()
	workerID := uuid.MustParse("61000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("61000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("61000000-0000-0000-0000-000000000003")
	authorizationID := uuid.MustParse("61000000-0000-0000-0000-000000000004")
	assignment := validTestAssignment(workerID, attemptID, jobID, 13, time.Minute)
	assignment.DebugDumpAuthorization = &workercontrol.DebugDumpAuthorizationSnapshot{
		AuthorizationID: authorizationID, ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	control := &recordingControlPlane{
		assignment: &assignment, events: events,
		startResult: grantedTestStart(assignment),
		failResult: workercontrol.RetryDecision{
			Disposition:  workercontrol.RetryDispositionRetryWait,
			FailureClass: "TRANSIENT_BACKEND", AttemptID: attemptID, JobID: jobID,
			AttemptState: workercontrol.FailedAttempt, JobFence: 4, JobVersion: 5,
			DecidedAt: time.Date(2026, 8, 27, 7, 3, 0, 0, time.UTC),
		},
	}
	payload := []byte(testDebugDumpPayload)
	digest := sha256.Sum256(payload)
	runner := &recordingRunner{
		events:        events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionFailed, Sequence: 3, BackendStage: "dit",
			Failure: &runnertransport.Failure{
				FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "dit/timeout",
				ErrorSummary: "backend timed out", BackendStage: "dit",
				InferenceBackendRevision: "sglang@abc", RetryRecommended: true, WorkerReusable: true,
			},
			DebugDump: &runnertransport.DebugDump{
				Content: payload, SizeBytes: int64(len(payload)), SHA256: digest,
				ContentType: debugdumpcontract.ContentType,
			},
		}},
	}
	debugControl := &recordingDebugDumpControl{
		events: events, authorizationID: authorizationID,
		claimRejected: mode.claimRejected, claimError: mode.claimError,
		completeError: mode.completeError,
	}
	debugUploader := &recordingDebugDumpUploader{
		events: events, err: mode.uploadError, block: mode.blockUpload,
	}
	reportedErrors := []error{}
	uploadTimeout := mode.timeout
	if uploadTimeout == 0 {
		uploadTimeout = time.Second
	}
	recovery := newTestRecoveryManager(t, workerID, 13, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 13, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow: func() time.Duration { return 10 * time.Second },
		OutputRoot:   t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
		DebugDumps: debugControl, DebugDumpUploader: debugUploader,
		DebugDumpUploadTimeout: uploadTimeout,
		ReportDebugDumpError:   func(err error) { reportedErrors = append(reportedErrors, err) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if mode != (debugDumpFailureMode{}) {
		t.Cleanup(func() {
			if len(reportedErrors) != 1 {
				t.Errorf("reported debug dump errors = %#v", reportedErrors)
			}
		})
	}
	return agent, control, runner, debugControl, debugUploader
}

type recordingDebugDumpControl struct {
	events          *[]string
	authorizationID uuid.UUID
	claimRejected   bool
	claimError      error
	completeError   error
	intents         []workercontrol.DebugDumpUploadIntent
	reports         []debugDumpCompletion
}

type debugDumpCompletion struct {
	DebugDumpID uuid.UUID
	ClaimID     uuid.UUID
	Report      workercontrol.DebugDumpUploadReport
}

func (control *recordingDebugDumpControl) ClaimDebugDumpUpload(
	_ context.Context,
	_ workercontrol.LeaseCredentials,
	intent workercontrol.DebugDumpUploadIntent,
	claimID uuid.UUID,
	part workertransport.DebugDumpUploadPartIntent,
) (workertransport.DebugDumpUploadClaim, error) {
	*control.events = append(*control.events, "debug.claim")
	control.intents = append(control.intents, intent)
	if control.claimError != nil {
		return workertransport.DebugDumpUploadClaim{}, control.claimError
	}
	if control.claimRejected {
		return workertransport.DebugDumpUploadClaim{
			DebugDumpUploadClaim: workercontrol.DebugDumpUploadClaim{
				Decision: workercontrol.DebugDumpUploadClaimRejected,
			},
		}, nil
	}
	return workertransport.DebugDumpUploadClaim{
		DebugDumpUploadClaim: workercontrol.DebugDumpUploadClaim{
			Decision: workercontrol.DebugDumpUploadClaimGranted,
			ClaimID:  claimID, DebugDumpID: intent.DebugDumpID,
			AuthorizationID:   intent.AuthorizationID,
			ObjectKey:         "debug-dumps/org/project/job/authorization/attempt/dump",
			ExpectedSizeBytes: intent.SizeBytes, ExpectedSHA256: intent.SHA256,
			ExpectedContentType: intent.ContentType, MultipartUploadID: "multipart-debug-1",
			ClaimExpiresAt: time.Now().Add(time.Minute), UploadExpiresAt: time.Now().Add(time.Minute),
			Version: 2,
		},
		UploadPart: &workertransport.SignedDebugDumpUploadPart{
			Number: part.Number, SizeBytes: part.SizeBytes, SHA256: part.SHA256,
			URL: "https://objects.internal/debug-dump",
			RequiredHeaders: map[string]string{
				"Content-Length":        strconv.FormatInt(part.SizeBytes, 10),
				"X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(part.SHA256[:]),
			},
			ExpiresAt: time.Now().Add(time.Minute),
		},
	}, nil
}

func (control *recordingDebugDumpControl) CompleteDebugDumpMultipartUpload(
	_ context.Context,
	_ workercontrol.LeaseCredentials,
	debugDumpID, authorizationID, claimID uuid.UUID,
	report workercontrol.DebugDumpUploadReport,
) (workercontrol.DebugDumpUploadResult, error) {
	*control.events = append(*control.events, "debug.complete")
	control.reports = append(control.reports, debugDumpCompletion{
		DebugDumpID: debugDumpID, ClaimID: claimID, Report: report,
	})
	if control.completeError != nil {
		return workercontrol.DebugDumpUploadResult{}, control.completeError
	}
	return workercontrol.DebugDumpUploadResult{
		Decision:    workercontrol.DebugDumpUploadRecorded,
		DebugDumpID: debugDumpID, AuthorizationID: authorizationID,
		ObjectVersionID: "debug-version-1", Version: 3,
	}, nil
}

type recordingDebugDumpUploader struct {
	events   *[]string
	err      error
	block    bool
	payloads [][]byte
}

func (uploader *recordingDebugDumpUploader) UploadDebugDump(
	ctx context.Context,
	part workertransport.SignedDebugDumpUploadPart,
	payload []byte,
) (workercontrol.DebugDumpUploadPart, error) {
	*uploader.events = append(*uploader.events, "debug.put")
	uploader.payloads = append(uploader.payloads, append([]byte(nil), payload...))
	if uploader.block {
		<-ctx.Done()
		return workercontrol.DebugDumpUploadPart{}, ctx.Err()
	}
	if uploader.err != nil {
		return workercontrol.DebugDumpUploadPart{}, uploader.err
	}
	return workercontrol.DebugDumpUploadPart{
		Number: part.Number, ETag: "etag-debug-1", SizeBytes: int64(len(payload)),
		ChecksumSHA256: base64.StdEncoding.EncodeToString(part.SHA256[:]),
	}, nil
}
