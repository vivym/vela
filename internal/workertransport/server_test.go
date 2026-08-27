package workertransport

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConnectReportsCapacityUnderMTLSWorkerIdentity(t *testing.T) {
	const spiffeID = "spiffe://vela.internal/worker/h3-capacity-01"
	observedAt := time.Date(2026, 8, 26, 1, 2, 3, 0, time.UTC)
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	poolID := uuid.MustParse("10000000-0000-0000-0000-000000000002")
	recorder := &recordingFleetCapacityService{result: fleet.CapacityResult{
		WorkerPoolID: poolID, WorkerState: fleet.CapacityAdmittable,
		PoolState: fleet.CapacityAdmittable, WorkerAssignmentAllowed: true,
		PoolAssignmentAllowed: true,
	}}
	server, err := NewServer(
		&recordingIdentityResolver{worker: workercontrol.AuthenticatedWorker{
			ID: workerID, PoolID: poolID, SPIFFEID: spiffeID,
		}},
		&recordingFinalizationCoordinator{},
		&recordingArtifactUploadStore{},
		recorder,
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	requestID := uuid.NewString()
	stream := &recordingConnectStream{
		ctx: mtlsPeerContext(t, spiffeID),
		requests: []*velav1.ConnectRequest{{
			RequestId: requestID,
			Operation: &velav1.ConnectRequest_ReportCapacity{
				ReportCapacity: &velav1.ReportWorkerCapacityRequest{
					WorkerEpoch: 7, ObservationSequence: 3,
					ObservedAt:     timestamppb.New(observedAt),
					WatermarkState: velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_NORMAL,
					TotalBytes:     1000, FreeBytes: 700,
					HighWatermarkBytes: 800, LowWatermarkBytes: 400,
					CriticalFreeBytes: 100, ArtifactStoreReachable: true,
				},
			},
		}},
	}
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	want := fleet.CapacityObservation{
		WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 7, Sequence: 3,
		ObservedAt: observedAt, WatermarkState: fleet.ScratchWatermarkNormal,
		TotalBytes: 1000, FreeBytes: 700, HighWatermarkBytes: 800,
		LowWatermarkBytes: 400, CriticalFreeBytes: 100,
		ArtifactStoreReachable: true, ObservedBy: spiffeID,
	}
	if recorder.observation != want {
		t.Fatalf("capacity observation = %#v, want %#v", recorder.observation, want)
	}
	if len(stream.responses) != 1 || stream.responses[0].GetRequestId() != requestID ||
		stream.responses[0].GetCapacityResult().GetWorkerPoolId() != poolID.String() ||
		!stream.responses[0].GetCapacityResult().GetWorkerAssignmentAllowed() ||
		!stream.responses[0].GetCapacityResult().GetPoolAssignmentAllowed() {
		t.Fatalf("capacity response = %#v", stream.responses)
	}
}

func TestConnectGetsAndReportsReadinessUnderExactMTLSWorkerIdentity(t *testing.T) {
	const spiffeID = "spiffe://vela.internal/worker/h3-readiness-01"
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	poolID := uuid.MustParse("10000000-0000-0000-0000-000000000002")
	cycleID := uuid.MustParse("f98bf4ac-94c4-5360-a476-cac78358adbd")
	profileID := uuid.MustParse("10000000-0000-0000-0000-000000000003")
	deadline := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	digest := sha256.Sum256([]byte("identity readiness evidence"))
	recorder := &recordingFleetCapacityService{
		readinessWork: fleet.ReadinessWork{
			Available: true, CycleID: cycleID, Check: fleet.ReadinessIdentity,
			WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 7,
			NodeIdentity: "h3-node-01", ExecutionProfileRevisionID: profileID,
			InferenceBackendRevision: "sglang-h3-v3", Deadline: deadline,
		},
		readinessResult: fleet.ReadinessResult{
			CycleID: cycleID, State: fleet.ReadinessChecking, NextCheck: fleet.ReadinessDevice,
			WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
		},
	}
	server, err := NewServer(
		&recordingIdentityResolver{worker: workercontrol.AuthenticatedWorker{
			ID: workerID, PoolID: poolID, SPIFFEID: spiffeID,
		}},
		&recordingFinalizationCoordinator{},
		&recordingArtifactUploadStore{},
		recorder,
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	getRequestID := uuid.NewString()
	reportRequestID := uuid.NewString()
	stream := &recordingConnectStream{
		ctx: mtlsPeerContext(t, spiffeID),
		requests: []*velav1.ConnectRequest{
			{
				RequestId: getRequestID,
				Operation: &velav1.ConnectRequest_GetReadinessWork{
					GetReadinessWork: &velav1.GetWorkerReadinessRequest{WorkerEpoch: 7},
				},
			},
			{
				RequestId: reportRequestID,
				Operation: &velav1.ConnectRequest_ReportReadiness{
					ReportReadiness: &velav1.ReportWorkerReadinessRequest{
						WorkerEpoch: 7, CycleId: cycleID.String(),
						Check:  velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY,
						Passed: true, EvidenceDigest: digest[:],
					},
				},
			},
		},
	}
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !reflect.DeepEqual(recorder.readinessWorkers, []readinessWorkerRequest{
		{WorkerID: workerID, WorkerEpoch: 7},
		{WorkerID: workerID, WorkerEpoch: 7},
	}) {
		t.Fatalf("readiness Worker lookups = %#v", recorder.readinessWorkers)
	}
	wantEvidence := fleet.ReadinessEvidence{
		CycleID: cycleID, Check: fleet.ReadinessIdentity, Passed: true,
		EvidenceDigest: digest[:], ObservedBy: spiffeID,
	}
	if !reflect.DeepEqual(recorder.readinessEvidence, []fleet.ReadinessEvidence{wantEvidence}) {
		t.Fatalf("readiness evidence = %#v", recorder.readinessEvidence)
	}
	if len(stream.responses) != 2 ||
		stream.responses[0].GetRequestId() != getRequestID ||
		stream.responses[0].GetReadinessWork().GetWorkerId() != workerID.String() ||
		stream.responses[0].GetReadinessWork().GetWorkerPoolId() != poolID.String() ||
		stream.responses[1].GetRequestId() != reportRequestID ||
		stream.responses[1].GetReadinessResult().GetNextCheck() !=
			velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_DEVICE ||
		stream.responses[1].GetReadinessResult().GetWorkerLifecycle() !=
			velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING ||
		stream.responses[1].GetReadinessResult().GetWorkerReachability() !=
			velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT {
		t.Fatalf("readiness responses = %#v", stream.responses)
	}
}

func TestConnectDispatchesFinalizationOperationsUnderMTLSWorkerIdentity(t *testing.T) {
	const spiffeID = "spiffe://vela.internal/worker/h3-primary-01"
	workerID := uuid.MustParse("00000000-0000-0000-0000-000000000020")
	attemptID := uuid.New()
	uploadID := uuid.New()
	claimID := uuid.New()
	verificationID := uuid.New()
	completionID := uuid.New()
	artifactID := uuid.New()
	lease := &velav1.WorkerLeaseCredentials{
		AttemptId: attemptID.String(), WorkerEpoch: 7, Fence: 3, Token: "opaque-lease-token",
	}
	partDigest := sha256.Sum256([]byte("upload-part-two"))
	objectDigest := sha256.Sum256([]byte("complete-object"))
	requests := []*velav1.ConnectRequest{
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_BeginFinalization{
				BeginFinalization: &velav1.BeginFinalizationRequest{Lease: lease},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_ClaimArtifactUpload{
				ClaimArtifactUpload: &velav1.ClaimArtifactUploadRequest{
					Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
					Part: &velav1.ArtifactUploadPartIntent{
						Number: 2, SizeBytes: 5, Sha256: partDigest[:],
					},
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_RecordArtifactMultipartSession{
				RecordArtifactMultipartSession: &velav1.RecordArtifactMultipartSessionRequest{
					Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
					MultipartUploadId: "s3-multipart-session",
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_RecordArtifactUploaded{
				RecordArtifactUploaded: &velav1.RecordArtifactUploadedRequest{
					Lease:    lease,
					UploadId: uploadID.String(),
					Report: &velav1.ArtifactUploadReport{
						ObjectVersionId: "version-1",
						SizeBytes:       12,
						Sha256:          make([]byte, sha256.Size),
						ContentType:     "video/mp4",
						CompletedParts: []*velav1.ArtifactUploadPartReport{{
							Number: 1, Etag: "etag-1", SizeBytes: 12,
						}},
					},
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_CompleteArtifactMultipartUpload{
				CompleteArtifactMultipartUpload: &velav1.CompleteArtifactMultipartUploadRequest{
					Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
					SizeBytes: 15, Sha256: objectDigest[:], ContentType: "video/mp4",
					CompletedParts: []*velav1.ArtifactUploadPartReport{
						{
							Number: 1, Etag: "etag-1", SizeBytes: 10,
							ChecksumSha256: sha256Bytes("completed-part-one"),
						},
						{
							Number: 2, Etag: "etag-2", SizeBytes: 5,
							ChecksumSha256: partDigest[:],
						},
					},
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_VerifyArtifact{
				VerifyArtifact: &velav1.VerifyArtifactRequest{
					Lease: lease, UploadId: uploadID.String(), VerificationId: verificationID.String(),
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_CompleteVisibleCompletion{
				CompleteVisibleCompletion: &velav1.CompleteVisibleCompletionRequest{
					Lease: lease,
					Candidate: &velav1.VisibleCompletionCandidate{
						CompletionId: completionID.String(), ExpectedJobVersion: 4,
						ArtifactIds: []string{artifactID.String()},
					},
				},
			},
		},
	}
	resolver := &recordingIdentityResolver{worker: workercontrol.AuthenticatedWorker{ID: workerID}}
	coordinator := &recordingFinalizationCoordinator{
		attemptID: attemptID, uploadID: uploadID, claimID: claimID,
		verificationID: verificationID, completionID: completionID, artifactID: artifactID,
	}
	uploadStore := &recordingArtifactUploadStore{}
	server, err := NewServer(resolver, coordinator, uploadStore)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	stream := &recordingConnectStream{ctx: mtlsPeerContext(t, spiffeID), requests: requests}
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if resolver.spiffeID != spiffeID {
		t.Fatalf("resolved SPIFFE ID = %q", resolver.spiffeID)
	}
	if len(coordinator.workers) != len(requests)+3 {
		t.Fatalf("coordinator calls = %d, want %d", len(coordinator.workers), len(requests)+3)
	}
	for _, worker := range coordinator.workers {
		if worker.ID != workerID {
			t.Fatalf("coordinator Worker = %s, want mTLS identity %s", worker.ID, workerID)
		}
	}
	if len(stream.responses) != len(requests) {
		t.Fatalf("responses = %d, want %d", len(stream.responses), len(requests))
	}
	for index, response := range stream.responses {
		if response.RequestId != requests[index].RequestId || response.GetOperationError() != nil {
			t.Fatalf("response %d = %#v", index, response)
		}
	}
	if stream.responses[0].GetFinalizationPlan() == nil ||
		stream.responses[1].GetArtifactUploadClaim() == nil ||
		stream.responses[2].GetArtifactMultipartSession() == nil ||
		stream.responses[3].GetArtifactUploadResult() == nil ||
		stream.responses[4].GetArtifactUploadResult() == nil ||
		stream.responses[5].GetArtifactVerificationResult() == nil ||
		stream.responses[6].GetVisibleCompletionResult() == nil {
		t.Fatalf("typed finalization responses = %#v", stream.responses)
	}
	claimResponse := stream.responses[1].GetArtifactUploadClaim()
	if claimResponse.GetMultipartUploadId() != "s3-multipart-session" ||
		len(claimResponse.GetCompletedParts()) != 1 || claimResponse.GetUploadPart() == nil ||
		claimResponse.GetUploadPart().GetNumber() != 2 ||
		claimResponse.GetUploadPart().GetRequiredHeaders()["Host"] != "" ||
		claimResponse.GetUploadPart().GetRequiredHeaders()["X-Amz-Checksum-Sha256"] == "" ||
		len(uploadStore.created) != 1 || len(uploadStore.presigned) != 1 ||
		len(uploadStore.completed) != 1 {
		t.Fatalf("production Artifact upload flow = claim %#v store %#v", claimResponse, uploadStore)
	}
}

func TestConnectDispatchesExecutionOperationsUnderMTLSWorkerIdentity(t *testing.T) {
	const spiffeID = "spiffe://vela.internal/worker/h3-execution-01"
	workerID := uuid.MustParse("11000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("12000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("13000000-0000-0000-0000-000000000003")
	now := time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC)
	lease := &velav1.WorkerLeaseCredentials{
		AttemptId: attemptID.String(), WorkerEpoch: 7, Fence: 3, Token: "opaque-lease-token",
	}
	requests := []*velav1.ConnectRequest{
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_Acquire{
				Acquire: &velav1.AcquireRequest{WorkerEpoch: 7},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_Start{
				Start: &velav1.StartWorkerRequest{Lease: lease},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_Heartbeat{
				Heartbeat: &velav1.HeartbeatRequest{
					Lease: lease, Sequence: 4, BackendStage: "dit",
					BackendStageProgress: ptr(0.25), EstimatedRemainingSeconds: ptr(int64(90)),
					GpuHealthJson:          []byte(`{"healthy":true}`),
					LocalArtifactStateJson: []byte(`{"dit":"running"}`),
					ScratchFreeBytes:       1 << 30, ArtifactStoreReachable: true,
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_Fail{
				Fail: &velav1.FailRequest{
					Lease: lease,
					Observation: &velav1.WorkerFailureObservation{
						FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "backend/timeout",
						ErrorSummary: "runner timed out", BackendStage: "dit",
						GpuUuids: []string{"GPU-1"}, InferenceBackendRevision: "sglang@abc",
						RetryRecommended: true, WorkerReusable: true,
					},
				},
			},
		},
	}
	coordinator := &recordingFinalizationCoordinator{
		assignment: workercontrol.Assignment{
			AttemptID: attemptID, JobID: jobID, WorkerID: workerID, WorkerEpoch: 7,
			ModelRevisionID:            uuid.MustParse("14000000-0000-0000-0000-000000000004"),
			GenerationPresetRevisionID: uuid.MustParse("15000000-0000-0000-0000-000000000005"),
			ExecutionProfileRevisionID: uuid.MustParse("16000000-0000-0000-0000-000000000006"),
			OutputSpecID:               uuid.MustParse("17000000-0000-0000-0000-000000000007"),
			RequestContent:             `{"prompt":"private"}`, AttemptNumber: 2,
			LeaseToken: "opaque-lease-token", LeaseFence: 3,
			LeaseExpiresAt: now.Add(time.Minute), LeaseValidFor: 45 * time.Second,
			DebugDumpAuthorization: &workercontrol.DebugDumpAuthorizationSnapshot{
				AuthorizationID: uuid.MustParse("17000000-0000-0000-0000-000000000008"),
				ExpiresAt:       now.Add(72 * time.Hour),
			},
		},
		startResult: workercontrol.StartResult{
			Decision: workercontrol.StartGranted, AttemptID: attemptID, JobID: jobID,
			WorkerID: workerID, WorkerEpoch: 7, LeaseFence: 3, StartedAt: now,
		},
		heartbeatResult: workercontrol.HeartbeatResult{
			Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
			WorkerID: workerID, WorkerEpoch: 7, LeaseFence: 3, HeartbeatSequence: 4,
			ExecutionPhase: workercontrol.ExecutionPhaseGenerating, ProgressUpdatedAt: now,
			LeaseExpiresAt: now.Add(time.Minute), LeaseValidFor: 50 * time.Second,
		},
		retryDecision: workercontrol.RetryDecision{
			Disposition:  workercontrol.RetryDispositionRetryWait,
			FailureClass: "TRANSIENT_BACKEND", AttemptID: attemptID, JobID: jobID,
			AttemptState: workercontrol.FailedAttempt, AttemptComputeSeconds: 10,
			TotalComputeSeconds: 20, JobFence: 4, JobVersion: 5, DecidedAt: now,
		},
	}
	server, err := NewServer(
		&recordingIdentityResolver{worker: workercontrol.AuthenticatedWorker{ID: workerID}},
		coordinator,
		&recordingArtifactUploadStore{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	stream := &recordingConnectStream{ctx: mtlsPeerContext(t, spiffeID), requests: requests}
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if len(stream.responses) != 4 || stream.responses[0].GetAssignment() == nil ||
		stream.responses[1].GetStartResult() == nil ||
		stream.responses[2].GetHeartbeatResult() == nil ||
		stream.responses[3].GetRetryDecision() == nil {
		t.Fatalf("typed execution responses = %#v", stream.responses)
	}
	assignment := stream.responses[0].GetAssignment()
	if assignment.GetAttemptId() != attemptID.String() || assignment.GetJobId() != jobID.String() ||
		assignment.GetRequestContentJson() == nil ||
		assignment.GetLeaseValidFor().AsDuration() != 45*time.Second ||
		assignment.GetDebugDumpAuthorization().GetAuthorizationId() !=
			"17000000-0000-0000-0000-000000000008" ||
		!assignment.GetDebugDumpAuthorization().GetExpiresAt().AsTime().Equal(now.Add(72*time.Hour)) {
		t.Fatalf("Assignment response = %#v", assignment)
	}
	if coordinator.acquireEpoch != 7 || coordinator.acquireCandidatePresent ||
		len(coordinator.workers) != 4 || coordinator.heartbeatObservation.Sequence != 4 ||
		coordinator.failureObservation.FailureClass != "TRANSIENT_BACKEND" {
		t.Fatalf("execution coordinator calls = %#v", coordinator)
	}
}

func TestConnectRejectsHeartbeatProgressOutsideTheControlPlaneContract(t *testing.T) {
	const spiffeID = "spiffe://vela.internal/worker/h3-progress-boundary"
	workerID := uuid.MustParse("18000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("18000000-0000-0000-0000-000000000002")
	terminalProgress := 1.0
	tooLong := int64(math.MaxInt64/int64(time.Second) + 1)
	tests := []struct {
		name      string
		progress  *float64
		remaining *int64
	}{
		{name: "terminal stage progress", progress: &terminalProgress},
		{name: "remaining time over duration limit", remaining: &tooLong},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator := &recordingFinalizationCoordinator{}
			server, err := NewServer(
				&recordingIdentityResolver{
					worker: workercontrol.AuthenticatedWorker{ID: workerID},
				},
				coordinator,
				&recordingArtifactUploadStore{},
			)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			requestID := uuid.NewString()
			stream := &recordingConnectStream{
				ctx: mtlsPeerContext(t, spiffeID),
				requests: []*velav1.ConnectRequest{{
					RequestId: requestID,
					Operation: &velav1.ConnectRequest_Heartbeat{
						Heartbeat: &velav1.HeartbeatRequest{
							Lease: &velav1.WorkerLeaseCredentials{
								AttemptId: attemptID.String(), WorkerEpoch: 7,
								Fence: 3, Token: "opaque-lease-token",
							},
							Sequence: 4, BackendStage: "dit",
							BackendStageProgress:      test.progress,
							EstimatedRemainingSeconds: test.remaining,
							GpuHealthJson:             []byte(`{"healthy":true}`),
							LocalArtifactStateJson:    []byte(`{"dit":"running"}`),
							ScratchFreeBytes:          1 << 30,
						},
					},
				}},
			}

			if err := server.Connect(stream); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			if len(stream.responses) != 1 ||
				stream.responses[0].GetRequestId() != requestID ||
				stream.responses[0].GetOperationError().GetCode() != "INVALID_REQUEST" {
				t.Fatalf("Connect response = %#v", stream.responses)
			}
			if len(coordinator.workers) != 0 {
				t.Fatalf("invalid Heartbeat reached coordinator: %#v", coordinator.workers)
			}
		})
	}
}

func TestArtifactUploadClaimAbortsOrphanAndResumesWinningMultipartSession(t *testing.T) {
	uploadID := uuid.New()
	artifactID := uuid.New()
	coordinator := &recordingFinalizationCoordinator{
		multipartDecision: workercontrol.ArtifactMultipartSessionConflict,
		inspection: &workercontrol.ArtifactUploadStatus{
			Decision: workercontrol.ArtifactUploadStatusFound,
			UploadID: uploadID, ArtifactID: artifactID, State: workercontrol.ArtifactUploadStateUploading,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4", MultipartUploadID: "winning-session",
			UploadExpiresAt: time.Now().Add(10 * time.Minute), Version: 4,
		},
	}
	uploadStore := &recordingArtifactUploadStore{createdUploadID: "orphan-session"}
	server := &Server{coordinator: coordinator, uploadStore: uploadStore}
	claim, err := server.prepareArtifactUploadClaim(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.New()},
		workercontrol.LeaseCredentials{
			AttemptID: uuid.New(), WorkerEpoch: 2, Fence: 3, Token: "lease-token",
		},
		uuid.New(),
		workercontrol.ArtifactUploadClaim{
			Decision: workercontrol.ArtifactUploadClaimGranted,
			ClaimID:  uuid.New(), UploadID: uploadID, ArtifactID: artifactID,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4",
			UploadExpiresAt:     time.Now().Add(10 * time.Minute), Version: 2,
		},
		nil,
	)
	if err != nil {
		t.Fatalf("prepare Artifact upload claim: %v", err)
	}
	if claim.GetDecision() != string(workercontrol.ArtifactUploadClaimGranted) ||
		claim.GetMultipartUploadId() != "winning-session" || claim.GetVersion() != 4 ||
		len(uploadStore.aborted) != 1 || uploadStore.aborted[0].UploadID != "orphan-session" ||
		len(uploadStore.created) != 1 {
		t.Fatalf("resumed claim = %#v store = %#v", claim, uploadStore)
	}
}

func TestArtifactMultipartCompletionRecoversVersionAfterLostResponse(t *testing.T) {
	uploadID := uuid.New()
	artifactID := uuid.New()
	partDigest := sha256.Sum256([]byte("completed-part"))
	parts := []artifactstore.CompletedPart{{
		Number: 1, ETag: "etag-1", SizeBytes: 15,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(partDigest[:]),
	}}
	coordinator := &recordingFinalizationCoordinator{
		uploadID:   uploadID,
		artifactID: artifactID,
		inspection: &workercontrol.ArtifactUploadStatus{
			Decision: workercontrol.ArtifactUploadStatusFound,
			UploadID: uploadID, ArtifactID: artifactID, State: workercontrol.ArtifactUploadStateUploading,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4", MultipartUploadID: "completed-session",
		},
	}
	uploadStore := &recordingArtifactUploadStore{
		listedParts: [][]artifactstore.CompletedPart{parts},
		completeErr: errors.New("connection closed after publication"),
		headVersion: artifactstore.ObjectVersion{
			ObjectKey: "artifacts/org/project/job/attempt/artifact/video.mp4",
			VersionID: "recovered-version", SizeBytes: 15, ContentType: "video/mp4",
			ChecksumSHA256: mustMultipartChecksum(t, parts),
		},
	}
	uploadStore.beforeComplete = func() {
		if len(coordinator.completionIntents) != 1 {
			t.Fatalf("multipart completion ran before its durable intent: %#v", coordinator)
		}
	}
	server := &Server{coordinator: coordinator, uploadStore: uploadStore}
	objectDigest := sha256.Sum256([]byte("completed-object"))
	result, err := server.completeArtifactMultipartUpload(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.New()},
		workercontrol.LeaseCredentials{
			AttemptID: uuid.New(), WorkerEpoch: 2, Fence: 3, Token: "lease-token",
		},
		uploadID,
		workercontrol.ArtifactUploadReport{
			SizeBytes: 15, SHA256: objectDigest, ContentType: "video/mp4",
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number: 1, ETag: "etag-1", SizeBytes: 15,
			}},
		},
		parts,
	)
	if err != nil {
		t.Fatalf("complete Artifact multipart upload: %v", err)
	}
	if result.Decision != workercontrol.ArtifactUploadRecorded ||
		result.ObjectVersionID != "recovered-version" || uploadStore.headCalls != 1 ||
		len(uploadStore.completed) != 1 || len(coordinator.uploadReports) != 1 ||
		coordinator.uploadReports[0].ObjectVersionID != "recovered-version" {
		t.Fatalf("recovered result = %#v store = %#v coordinator = %#v", result, uploadStore, coordinator)
	}
}

func TestArtifactMultipartCompletionReplaysPersistedReceiptWithoutS3Mutation(t *testing.T) {
	uploadID := uuid.New()
	coordinator := &recordingFinalizationCoordinator{
		inspection: &workercontrol.ArtifactUploadStatus{
			Decision: workercontrol.ArtifactUploadStatusFound,
			UploadID: uploadID, ArtifactID: uuid.New(), State: workercontrol.ArtifactUploadStateVerified,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4", ObjectVersionID: "persisted-version",
		},
	}
	uploadStore := &recordingArtifactUploadStore{}
	server := &Server{coordinator: coordinator, uploadStore: uploadStore}
	digest := sha256.Sum256([]byte("object"))
	result, err := server.completeArtifactMultipartUpload(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.New()},
		workercontrol.LeaseCredentials{
			AttemptID: uuid.New(), WorkerEpoch: 2, Fence: 3, Token: "lease-token",
		},
		uploadID,
		workercontrol.ArtifactUploadReport{
			SizeBytes: 15, SHA256: digest, ContentType: "video/mp4",
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number: 1, ETag: "etag-1", SizeBytes: 15,
			}},
		},
		[]artifactstore.CompletedPart{{
			Number: 1, ETag: "etag-1", SizeBytes: 15, ChecksumSHA256: "unused",
		}},
	)
	if err != nil {
		t.Fatalf("replay Artifact multipart completion: %v", err)
	}
	if result.ObjectVersionID != "persisted-version" || uploadStore.listed != 0 ||
		len(uploadStore.completed) != 0 || uploadStore.headCalls != 0 {
		t.Fatalf("replayed result = %#v store = %#v", result, uploadStore)
	}
}

type recordingIdentityResolver struct {
	spiffeID string
	worker   workercontrol.AuthenticatedWorker
}

func (resolver *recordingIdentityResolver) ResolveWorker(
	_ context.Context,
	spiffeID string,
) (workercontrol.AuthenticatedWorker, error) {
	resolver.spiffeID = spiffeID
	return resolver.worker, nil
}

type recordingFinalizationCoordinator struct {
	attemptID, uploadID, claimID, verificationID, completionID, artifactID uuid.UUID
	workers                                                                []workercontrol.AuthenticatedWorker
	multipartDecision                                                      workercontrol.ArtifactMultipartSessionDecision
	inspection                                                             *workercontrol.ArtifactUploadStatus
	completionIntents                                                      []workercontrol.ArtifactUploadReport
	uploadReports                                                          []workercontrol.ArtifactUploadReport
	assignment                                                             workercontrol.Assignment
	startResult                                                            workercontrol.StartResult
	heartbeatResult                                                        workercontrol.HeartbeatResult
	retryDecision                                                          workercontrol.RetryDecision
	acquireEpoch                                                           int64
	acquireCandidatePresent                                                bool
	heartbeatObservation                                                   workercontrol.HeartbeatObservation
	failureObservation                                                     workercontrol.FailureObservation
}

type recordingFleetCapacityService struct {
	observation       fleet.CapacityObservation
	result            fleet.CapacityResult
	readinessWork     fleet.ReadinessWork
	readinessResult   fleet.ReadinessResult
	readinessWorkers  []readinessWorkerRequest
	readinessEvidence []fleet.ReadinessEvidence
}

type readinessWorkerRequest struct {
	WorkerID    uuid.UUID
	WorkerEpoch int64
}

func (service *recordingFleetCapacityService) ObserveCapacity(
	_ context.Context,
	observation fleet.CapacityObservation,
) (fleet.CapacityResult, error) {
	service.observation = observation
	return service.result, nil
}

func (service *recordingFleetCapacityService) GetWorkerReadinessWork(
	_ context.Context,
	workerID uuid.UUID,
	workerEpoch int64,
) (fleet.ReadinessWork, error) {
	service.readinessWorkers = append(service.readinessWorkers, readinessWorkerRequest{
		WorkerID: workerID, WorkerEpoch: workerEpoch,
	})
	return service.readinessWork, nil
}

func (service *recordingFleetCapacityService) ReportReadiness(
	_ context.Context,
	evidence fleet.ReadinessEvidence,
) (fleet.ReadinessResult, error) {
	service.readinessEvidence = append(service.readinessEvidence, evidence)
	return service.readinessResult, nil
}

func (coordinator *recordingFinalizationCoordinator) record(
	worker workercontrol.AuthenticatedWorker,
) {
	coordinator.workers = append(coordinator.workers, worker)
}

func (coordinator *recordingFinalizationCoordinator) Acquire(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	workerEpoch int64,
	candidate *workercontrol.AssignmentCandidate,
) (workercontrol.Assignment, error) {
	coordinator.record(worker)
	coordinator.acquireEpoch = workerEpoch
	coordinator.acquireCandidatePresent = candidate != nil
	return coordinator.assignment, nil
}

func (coordinator *recordingFinalizationCoordinator) Start(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
) (workercontrol.StartResult, error) {
	coordinator.record(worker)
	return coordinator.startResult, nil
}

func (coordinator *recordingFinalizationCoordinator) Heartbeat(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	observation workercontrol.HeartbeatObservation,
) (workercontrol.HeartbeatResult, error) {
	coordinator.record(worker)
	coordinator.heartbeatObservation = observation
	return coordinator.heartbeatResult, nil
}

func (coordinator *recordingFinalizationCoordinator) Fail(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	observation workercontrol.FailureObservation,
) (workercontrol.RetryDecision, error) {
	coordinator.record(worker)
	coordinator.failureObservation = observation
	return coordinator.retryDecision, nil
}

func (coordinator *recordingFinalizationCoordinator) BeginFinalization(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
) (workercontrol.FinalizationPlan, error) {
	coordinator.record(worker)
	return workercontrol.FinalizationPlan{
		Decision: workercontrol.FinalizationGranted, AttemptID: coordinator.attemptID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) ClaimArtifactUpload(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_, _ uuid.UUID,
) (workercontrol.ArtifactUploadClaim, error) {
	coordinator.record(worker)
	return workercontrol.ArtifactUploadClaim{
		Decision: workercontrol.ArtifactUploadClaimGranted, ClaimID: coordinator.claimID,
		UploadID: coordinator.uploadID, ArtifactID: coordinator.artifactID,
		ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
		ExpectedContentType: "video/mp4", UploadExpiresAt: time.Now().Add(10 * time.Minute),
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) RecordArtifactMultipartSession(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_, _ uuid.UUID,
	multipartUploadID string,
) (workercontrol.ArtifactMultipartSession, error) {
	coordinator.record(worker)
	decision := coordinator.multipartDecision
	if decision == "" {
		decision = workercontrol.ArtifactMultipartSessionRecorded
	}
	return workercontrol.ArtifactMultipartSession{
		Decision: decision, UploadID: coordinator.uploadID,
		ArtifactID: coordinator.artifactID, MultipartUploadID: multipartUploadID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) InspectArtifactUpload(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ uuid.UUID,
) (workercontrol.ArtifactUploadStatus, error) {
	coordinator.record(worker)
	if coordinator.inspection != nil {
		return *coordinator.inspection, nil
	}
	return workercontrol.ArtifactUploadStatus{
		Decision: workercontrol.ArtifactUploadStatusFound,
		UploadID: coordinator.uploadID, ArtifactID: coordinator.artifactID,
		State:               workercontrol.ArtifactUploadStateUploading,
		ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
		ExpectedContentType: "video/mp4", MultipartUploadID: "s3-multipart-session",
		UploadExpiresAt: time.Now().Add(10 * time.Minute),
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) RecordArtifactCompletionIntent(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ uuid.UUID,
	report workercontrol.ArtifactUploadReport,
) (workercontrol.ArtifactCompletionIntentResult, error) {
	coordinator.record(worker)
	coordinator.completionIntents = append(coordinator.completionIntents, report)
	return workercontrol.ArtifactCompletionIntentResult{
		Decision:   workercontrol.ArtifactCompletionIntentRecorded,
		UploadID:   coordinator.uploadID,
		ArtifactID: coordinator.artifactID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) RecordArtifactUploaded(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ uuid.UUID,
	report workercontrol.ArtifactUploadReport,
) (workercontrol.ArtifactUploadResult, error) {
	coordinator.record(worker)
	coordinator.uploadReports = append(coordinator.uploadReports, report)
	return workercontrol.ArtifactUploadResult{
		Decision: workercontrol.ArtifactUploadRecorded, UploadID: coordinator.uploadID,
		ObjectVersionID: report.ObjectVersionID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) VerifyArtifact(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_, _ uuid.UUID,
) (workercontrol.ArtifactVerificationResult, error) {
	coordinator.record(worker)
	return workercontrol.ArtifactVerificationResult{
		Decision: workercontrol.ArtifactVerified, VerificationID: coordinator.verificationID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) CompleteVisibleCompletion(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	coordinator.record(worker)
	return workercontrol.VisibleCompletionResult{
		Decision: workercontrol.VisibleCompletionCommitted, CompletionID: coordinator.completionID,
	}, nil
}

type recordingArtifactUploadStore struct {
	createdUploadID string
	created         []artifactstore.MultipartUpload
	listed          int
	listedParts     [][]artifactstore.CompletedPart
	presigned       []artifactstore.MultipartUpload
	completed       []artifactstore.MultipartUpload
	completeErr     error
	completeVersion artifactstore.ObjectVersion
	headVersion     artifactstore.ObjectVersion
	headCalls       int
	beforeComplete  func()
	aborted         []artifactstore.MultipartUpload
}

func (store *recordingArtifactUploadStore) CreateMultipartUpload(
	_ context.Context,
	objectKey string,
	contentType string,
) (artifactstore.MultipartUpload, error) {
	uploadID := store.createdUploadID
	if uploadID == "" {
		uploadID = "s3-multipart-session"
	}
	upload := artifactstore.MultipartUpload{
		ObjectKey: objectKey, UploadID: uploadID, ContentType: contentType,
	}
	store.created = append(store.created, upload)
	return upload, nil
}

func (store *recordingArtifactUploadStore) ListParts(
	_ context.Context,
	upload artifactstore.MultipartUpload,
) ([]artifactstore.CompletedPart, error) {
	store.listed++
	if len(store.listedParts) != 0 {
		index := store.listed - 1
		if index >= len(store.listedParts) {
			index = len(store.listedParts) - 1
		}
		return append([]artifactstore.CompletedPart(nil), store.listedParts[index]...), nil
	}
	firstDigest := sha256.Sum256([]byte("completed-part-one"))
	parts := []artifactstore.CompletedPart{{
		Number: 1, ETag: "etag-1", SizeBytes: 10,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(firstDigest[:]),
	}}
	if store.listed > 1 {
		secondDigest := sha256.Sum256([]byte("upload-part-two"))
		parts = append(parts, artifactstore.CompletedPart{
			Number: 2, ETag: "etag-2", SizeBytes: 5,
			ChecksumSHA256: base64.StdEncoding.EncodeToString(secondDigest[:]),
		})
	}
	return parts, nil
}

func (store *recordingArtifactUploadStore) PresignUploadPart(
	_ context.Context,
	upload artifactstore.MultipartUpload,
	_ int32,
	sizeBytes int64,
	digest [sha256.Size]byte,
	expiresAt time.Time,
) (artifactstore.SignedUploadPart, error) {
	store.presigned = append(store.presigned, upload)
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	return artifactstore.SignedUploadPart{
		URL: "https://s3.invalid/signed-part", Method: http.MethodPut,
		Headers: http.Header{
			"Host":                  []string{"s3.invalid"},
			"Content-Length":        []string{strconv.FormatInt(sizeBytes, 10)},
			"X-Amz-Checksum-Sha256": []string{checksum},
		},
		IssuedAt: time.Now(), ExpiresAt: expiresAt,
	}, nil
}

func (store *recordingArtifactUploadStore) CompleteMultipartUpload(
	_ context.Context,
	upload artifactstore.MultipartUpload,
	parts []artifactstore.CompletedPart,
) (artifactstore.ObjectVersion, error) {
	if store.beforeComplete != nil {
		store.beforeComplete()
	}
	store.completed = append(store.completed, upload)
	if store.completeErr != nil {
		return artifactstore.ObjectVersion{}, store.completeErr
	}
	if store.completeVersion.VersionID != "" {
		version := store.completeVersion
		if version.ChecksumSHA256 == "" {
			version.ChecksumSHA256 = mustMultipartChecksumForStore(parts)
		}
		return version, nil
	}
	return artifactstore.ObjectVersion{
		ObjectKey: upload.ObjectKey, VersionID: "version-1", SizeBytes: 15, ContentType: "video/mp4",
		ChecksumSHA256: mustMultipartChecksumForStore(parts),
	}, nil
}

func (store *recordingArtifactUploadStore) HeadCurrentVersion(
	_ context.Context,
	objectKey string,
) (artifactstore.ObjectVersion, error) {
	store.headCalls++
	if store.headVersion.VersionID != "" {
		return store.headVersion, nil
	}
	return artifactstore.ObjectVersion{
		ObjectKey: objectKey, VersionID: "version-1", SizeBytes: 15, ContentType: "video/mp4",
	}, nil
}

func (store *recordingArtifactUploadStore) AbortMultipartUpload(
	_ context.Context,
	upload artifactstore.MultipartUpload,
) error {
	store.aborted = append(store.aborted, upload)
	return nil
}

type recordingConnectStream struct {
	grpc.ServerStream
	ctx       context.Context
	requests  []*velav1.ConnectRequest
	responses []*velav1.ConnectResponse
	index     int
}

func (stream *recordingConnectStream) Context() context.Context {
	return stream.ctx
}

func (stream *recordingConnectStream) Recv() (*velav1.ConnectRequest, error) {
	if stream.index == len(stream.requests) {
		return nil, io.EOF
	}
	request := stream.requests[stream.index]
	stream.index++
	return request, nil
}

func (stream *recordingConnectStream) Send(response *velav1.ConnectResponse) error {
	stream.responses = append(stream.responses, response)
	return nil
}

func sha256Bytes(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

func mustMultipartChecksum(t *testing.T, parts []artifactstore.CompletedPart) string {
	t.Helper()
	checksum, err := artifactstore.MultipartCompositeChecksum(parts)
	if err != nil {
		t.Fatalf("multipart checksum: %v", err)
	}
	return checksum
}

func mustMultipartChecksumForStore(parts []artifactstore.CompletedPart) string {
	checksum, _ := artifactstore.MultipartCompositeChecksum(parts)
	return checksum
}

func mtlsPeerContext(t *testing.T, spiffeID string) context.Context {
	t.Helper()
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse SPIFFE ID: %v", err)
	}
	certificate := &x509.Certificate{URIs: []*url.URL{uri}}
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			HandshakeComplete: true,
			PeerCertificates:  []*x509.Certificate{certificate},
			VerifiedChains:    [][]*x509.Certificate{{certificate}},
		}},
	})
}

var _ velav1.WorkerControlService_ConnectServer = (*recordingConnectStream)(nil)

func ptr[T any](value T) *T {
	return &value
}
