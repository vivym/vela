package workertransport

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClientExchangesExecutionOperationsOnOneCorrelatedStream(t *testing.T) {
	workerID := uuid.MustParse("71000000-0000-0000-0000-000000000001")
	poolID := uuid.MustParse("70000000-0000-0000-0000-000000000001")
	cycleID := uuid.MustParse("f98bf4ac-94c4-5360-a476-cac78358adbd")
	profileID := uuid.MustParse("76000000-0000-0000-0000-000000000006")
	attemptID := uuid.MustParse("72000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("73000000-0000-0000-0000-000000000003")
	debugAuthorizationID := uuid.MustParse("73000000-0000-0000-0000-000000000004")
	now := time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
	server := &executionClientTestServer{
		workerID: workerID, poolID: poolID, cycleID: cycleID, profileID: profileID,
		attemptID: attemptID, jobID: jobID, now: now,
	}
	grpcServer := grpc.NewServer()
	velav1.RegisterWorkerControlServiceServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-serveDone; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve: %v", serveErr)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := DialClient(ctx, listener.Addr().String(), insecure.NewCredentials())
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	capacity, err := client.ReportCapacity(ctx, workercontrol.CapacityObservation{
		WorkerEpoch: 7, Sequence: 9, ObservedAt: now, TotalBytes: 1000, FreeBytes: 700,
		WatermarkState:     workercontrol.ScratchWatermarkNormal,
		HighWatermarkBytes: 800, LowWatermarkBytes: 400, CriticalFreeBytes: 100,
		ArtifactStoreReachable: true,
	})
	if err != nil || capacity.WorkerPoolID != poolID ||
		capacity.WorkerState != workercontrol.CapacityAdmittable ||
		capacity.PoolState != workercontrol.CapacityAdmittable ||
		!capacity.WorkerAssignmentAllowed || !capacity.PoolAssignmentAllowed {
		t.Fatalf("ReportCapacity = %#v error=%v", capacity, err)
	}
	readiness, err := client.GetReadinessWork(ctx, 7)
	if err != nil || !readiness.Available || readiness.CycleID != cycleID ||
		readiness.Check != workercontrol.ReadinessIdentity || readiness.WorkerID != workerID ||
		readiness.WorkerPoolID != poolID || readiness.WorkerEpoch != 7 ||
		readiness.NodeIdentity != "h3-node-01" ||
		readiness.ExecutionProfileRevisionID != profileID ||
		readiness.InferenceBackendRevision != "sglang-h3-v3" ||
		!readiness.Deadline.Equal(now.Add(30*time.Minute)) {
		t.Fatalf("GetReadinessWork = %#v error=%v", readiness, err)
	}
	digest := sha256.Sum256([]byte("identity readiness evidence"))
	readinessResult, err := client.ReportReadiness(ctx, workercontrol.ReadinessEvidence{
		WorkerEpoch: 7, CycleID: cycleID, Check: workercontrol.ReadinessIdentity,
		Passed: true, EvidenceDigest: digest,
	})
	if err != nil || readinessResult.CycleID != cycleID ||
		readinessResult.State != workercontrol.ReadinessChecking ||
		readinessResult.NextCheck != workercontrol.ReadinessDevice ||
		readinessResult.WorkerLifecycle != "WARMING" ||
		readinessResult.WorkerReachability != "SUSPECT" {
		t.Fatalf("ReportReadiness = %#v error=%v", readinessResult, err)
	}
	assignment, err := client.Acquire(ctx, 7)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if assignment.AttemptID != attemptID || assignment.JobID != jobID ||
		assignment.WorkerID != workerID || assignment.WorkerEpoch != 7 ||
		assignment.LeaseValidFor != 45*time.Second ||
		assignment.RequestContent != `{"prompt":"private"}` ||
		assignment.DebugDumpAuthorization == nil ||
		assignment.DebugDumpAuthorization.AuthorizationID != debugAuthorizationID ||
		!assignment.DebugDumpAuthorization.ExpiresAt.Equal(now.Add(72*time.Hour)) {
		t.Fatalf("Assignment = %#v", assignment)
	}
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}
	started, err := client.Start(ctx, credentials)
	if err != nil || started.Decision != workercontrol.StartGranted || started.StartedAt != now {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	progress := 0.5
	remaining := int64(30)
	heartbeat, err := client.Heartbeat(ctx, credentials, workercontrol.HeartbeatObservation{
		Sequence: 2, BackendStage: "dit", BackendStageProgress: &progress,
		EstimatedRemainingSeconds: &remaining,
		GPUHealthSummary:          []byte(`{"healthy":true}`),
		LocalArtifactState:        []byte(`{"dit":"running"}`),
		ScratchFreeBytes:          1024, ArtifactStoreReachable: true,
	})
	if err != nil || heartbeat.Decision != workercontrol.HeartbeatContinue ||
		heartbeat.LeaseValidFor != 50*time.Second || heartbeat.HeartbeatSequence != 2 {
		t.Fatalf("Heartbeat = %#v error=%v", heartbeat, err)
	}
	decision, err := client.Fail(ctx, credentials, workercontrol.FailureObservation{
		FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "dit/timeout",
		ErrorSummary: "timed out", BackendStage: "dit", GPUUUIDs: []string{"GPU-1"},
		InferenceBackendRevision: "sglang@abc", RetryRecommended: true, WorkerReusable: true,
	})
	if err != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		decision.AttemptID != attemptID || decision.JobID != jobID {
		t.Fatalf("Fail = %#v error=%v", decision, err)
	}
	if server.requests != 7 {
		t.Fatalf("stream requests = %d, want 7", server.requests)
	}
}

func TestClientOperationContextInterruptsBlockedResponse(t *testing.T) {
	client := newClientTestConnection(t, blockingClientTestServer{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Acquire(ctx, 7)
		done <- err
	}()
	select {
	case err := <-done:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("Acquire error = %v, want DeadlineExceeded", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Acquire did not honor its operation context")
	}
}

func TestClientReconnectsAfterStreamFailure(t *testing.T) {
	workerID := uuid.MustParse("70800000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("70800000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("70800000-0000-0000-0000-000000000003")
	server := &reconnectingClientTestServer{
		workerID: workerID, attemptID: attemptID, jobID: jobID,
	}
	client := newClientTestConnection(t, server)
	if _, err := client.Acquire(context.Background(), 7); status.Code(err) != codes.Internal {
		t.Fatalf("first Acquire error = %v, want Internal", err)
	}
	assignment, err := client.Acquire(context.Background(), 7)
	if err != nil {
		t.Fatalf("reconnected Acquire: %v", err)
	}
	if assignment.WorkerID != workerID || assignment.AttemptID != attemptID || assignment.JobID != jobID {
		t.Fatalf("reconnected Assignment = %#v", assignment)
	}
}

func TestClientRejectsUnknownStopReason(t *testing.T) {
	attemptID := uuid.MustParse("77400000-0000-0000-0000-000000000001")
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}
	for _, testCase := range []struct {
		name     string
		response func(*velav1.ConnectRequest) *velav1.ConnectResponse
		invoke   func(*Client) error
	}{
		{
			name: "Start",
			response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
				return &velav1.ConnectResponse{
					RequestId: request.GetRequestId(),
					Result: &velav1.ConnectResponse_StartResult{StartResult: &velav1.StartWorkerResult{
						Decision: string(workercontrol.Stop), StopReason: "UNKNOWN_FUTURE_REASON",
					}},
				}
			},
			invoke: func(client *Client) error {
				_, err := client.Start(context.Background(), credentials)
				return err
			},
		},
		{
			name: "Heartbeat",
			response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
				return &velav1.ConnectResponse{
					RequestId: request.GetRequestId(),
					Result: &velav1.ConnectResponse_HeartbeatResult{HeartbeatResult: &velav1.HeartbeatResult{
						Decision: string(workercontrol.HeartbeatStop), StopReason: "UNKNOWN_FUTURE_REASON",
					}},
				}
			},
			invoke: func(client *Client) error {
				_, err := client.Heartbeat(
					context.Background(), credentials, workercontrol.HeartbeatObservation{
						Sequence: 1, BackendStage: "dit",
						GPUHealthSummary:   []byte(`{"healthy":true}`),
						LocalArtifactState: []byte(`{"dit":"running"}`),
						ScratchFreeBytes:   1024, ArtifactStoreReachable: true,
					},
				)
				return err
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client := newClientTestConnection(t, &oneResponseClientTestServer{response: testCase.response})
			if err := testCase.invoke(client); err == nil {
				t.Fatal("worker transport Client accepted an unknown STOP reason")
			}
		})
	}
}

func TestClientRejectsUnspecifiedOrUnknownReadinessWorkerStateEnums(t *testing.T) {
	cycleID := uuid.MustParse("77410000-0000-0000-0000-000000000001")
	digest := sha256.Sum256([]byte("typed readiness enum evidence"))
	for _, testCase := range []struct {
		name   string
		mutate func(*velav1.WorkerReadinessResult)
	}{
		{
			name: "unspecified lifecycle",
			mutate: func(result *velav1.WorkerReadinessResult) {
				result.WorkerLifecycle = velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_UNSPECIFIED
			},
		},
		{
			name: "unknown lifecycle",
			mutate: func(result *velav1.WorkerReadinessResult) {
				result.WorkerLifecycle = velav1.FleetWorkerLifecycle(99)
			},
		},
		{
			name: "unspecified reachability",
			mutate: func(result *velav1.WorkerReadinessResult) {
				result.WorkerReachability = velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_UNSPECIFIED
			},
		},
		{
			name: "unknown reachability",
			mutate: func(result *velav1.WorkerReadinessResult) {
				result.WorkerReachability = velav1.FleetWorkerReachability(99)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
				result := &velav1.WorkerReadinessResult{
					CycleId: cycleID.String(), State: velav1.FleetReadinessState_FLEET_READINESS_STATE_CHECKING,
					NextCheck:          velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_DEVICE,
					WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING,
					WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
				}
				testCase.mutate(result)
				return &velav1.ConnectResponse{
					RequestId: request.GetRequestId(),
					Result:    &velav1.ConnectResponse_ReadinessResult{ReadinessResult: result},
				}
			}}
			client := newClientTestConnection(t, server)
			_, err := client.ReportReadiness(context.Background(), workercontrol.ReadinessEvidence{
				WorkerEpoch: 7, CycleID: cycleID, Check: workercontrol.ReadinessIdentity,
				Passed: true, EvidenceDigest: digest,
			})
			if err == nil {
				t.Fatal("Worker client accepted an unspecified or unknown Worker state enum")
			}
		})
	}
}

func TestClientRejectsMalformedFailDecision(t *testing.T) {
	attemptID := uuid.MustParse("77500000-0000-0000-0000-000000000001")
	jobID := uuid.MustParse("77500000-0000-0000-0000-000000000002")
	decidedAt := time.Date(2026, 8, 25, 7, 0, 0, 0, time.UTC)
	valid := func() *velav1.RetryDecision {
		return &velav1.RetryDecision{
			Disposition:  string(workercontrol.RetryDispositionRetryWait),
			FailureClass: "TRANSIENT_BACKEND", AttemptId: attemptID.String(), JobId: jobID.String(),
			AttemptState:          string(workercontrol.FailedAttempt),
			AttemptComputeSeconds: 2, TotalComputeSeconds: 5,
			AttemptFinalizationSeconds: 1, TotalFinalizationSeconds: 3,
			NextRetryAt: timestamppb.New(decidedAt.Add(time.Second)),
			JobFence:    4, JobVersion: 5, DecidedAt: timestamppb.New(decidedAt),
		}
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*velav1.RetryDecision)
	}{
		{name: "retry wait omits next retry", mutate: func(got *velav1.RetryDecision) { got.NextRetryAt = nil }},
		{name: "failed includes next retry", mutate: func(got *velav1.RetryDecision) {
			got.Disposition = string(workercontrol.RetryDispositionFailed)
		}},
		{name: "invalid Attempt state", mutate: func(got *velav1.RetryDecision) { got.AttemptState = "SUCCEEDED" }},
		{name: "negative accounting", mutate: func(got *velav1.RetryDecision) { got.AttemptComputeSeconds = -1 }},
		{name: "total accounting regresses", mutate: func(got *velav1.RetryDecision) { got.TotalFinalizationSeconds = 0 }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
				decision := valid()
				testCase.mutate(decision)
				return &velav1.ConnectResponse{
					RequestId: request.GetRequestId(),
					Result:    &velav1.ConnectResponse_RetryDecision{RetryDecision: decision},
				}
			}}
			client := newClientTestConnection(t, server)
			_, err := client.Fail(
				context.Background(),
				workercontrol.LeaseCredentials{
					AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
				},
				workercontrol.FailureObservation{
					FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "dit/timeout",
					ErrorSummary: "timed out", BackendStage: "dit",
					InferenceBackendRevision: "sglang@abc", RetryRecommended: true, WorkerReusable: true,
				},
			)
			if err == nil {
				t.Fatal("Client.Fail accepted malformed RetryDecision")
			}
		})
	}
}

func TestClientBeginsExactFinalizationPlanOnCorrelatedStream(t *testing.T) {
	attemptID := uuid.MustParse("78000000-0000-0000-0000-000000000001")
	jobID := uuid.MustParse("78000000-0000-0000-0000-000000000002")
	artifactID := uuid.MustParse("78000000-0000-0000-0000-000000000003")
	uploadID := uuid.MustParse("78000000-0000-0000-0000-000000000004")
	startedAt := time.Date(2026, 8, 25, 8, 0, 0, 0, time.UTC)
	deadlineAt := startedAt.Add(10 * time.Minute)
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		operation, ok := request.GetOperation().(*velav1.ConnectRequest_BeginFinalization)
		if !ok || operation.BeginFinalization.GetLease().GetAttemptId() != attemptID.String() {
			return nil
		}
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_FinalizationPlan{FinalizationPlan: &velav1.FinalizationPlan{
				Decision: string(workercontrol.FinalizationGranted), AttemptId: attemptID.String(),
				JobId: jobID.String(), JobVersion: 9,
				FinalizationStartedAt:  timestamppb.New(startedAt),
				FinalizationDeadlineAt: timestamppb.New(deadlineAt),
				Artifacts: []*velav1.PlannedArtifact{{
					ArtifactId: artifactID.String(), UploadId: uploadID.String(),
					Kind: string(workercontrol.ArtifactKindVideo), Ordinal: 0,
					ObjectKey: "artifacts/org/project/job/attempt/artifact/video.mp4",
					ExpiresAt: timestamppb.New(deadlineAt),
				}},
			}},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}

	plan, err := client.BeginFinalization(context.Background(), credentials)
	if err != nil {
		t.Fatalf("BeginFinalization: %v", err)
	}
	if plan.Decision != workercontrol.FinalizationGranted || plan.AttemptID != attemptID ||
		plan.JobID != jobID || plan.JobVersion != 9 || plan.FinalizationStartedAt != startedAt ||
		plan.FinalizationDeadlineAt != deadlineAt || len(plan.Artifacts) != 1 ||
		plan.Artifacts[0].ArtifactID != artifactID || plan.Artifacts[0].UploadID != uploadID {
		t.Fatalf("FinalizationPlan = %#v", plan)
	}
}

func TestClientClaimsOneChecksumBoundArtifactPart(t *testing.T) {
	attemptID := uuid.MustParse("79000000-0000-0000-0000-000000000001")
	uploadID := uuid.MustParse("79000000-0000-0000-0000-000000000002")
	artifactID := uuid.MustParse("79000000-0000-0000-0000-000000000003")
	claimID := uuid.MustParse("79000000-0000-0000-0000-000000000004")
	digest := sha256.Sum256([]byte("artifact-part"))
	expiresAt := time.Date(2026, 8, 25, 8, 10, 0, 0, time.UTC)
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		operation, ok := request.GetOperation().(*velav1.ConnectRequest_ClaimArtifactUpload)
		if !ok || operation.ClaimArtifactUpload.GetPart().GetNumber() != 1 ||
			operation.ClaimArtifactUpload.GetPart().GetSizeBytes() != 13 ||
			string(operation.ClaimArtifactUpload.GetPart().GetSha256()) != string(digest[:]) {
			return nil
		}
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_ArtifactUploadClaim{ArtifactUploadClaim: &velav1.ArtifactUploadClaim{
				Decision: string(workercontrol.ArtifactUploadClaimGranted), ClaimId: claimID.String(),
				UploadId: uploadID.String(), ArtifactId: artifactID.String(),
				ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
				ExpectedContentType: "video/mp4", MultipartUploadId: "multipart-1",
				ClaimExpiresAt: timestamppb.New(expiresAt), UploadExpiresAt: timestamppb.New(expiresAt),
				Version: 2,
				UploadPart: &velav1.SignedArtifactUploadPart{
					Number: 1, SizeBytes: 13, Sha256: digest[:],
					Url: "https://objects.internal/upload?partNumber=1",
					RequiredHeaders: map[string]string{
						"Content-Length":        "13",
						"X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(digest[:]),
					},
					ExpiresAt: timestamppb.New(expiresAt),
				},
			}},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}

	claim, err := client.ClaimArtifactUpload(
		context.Background(), credentials, uploadID, claimID,
		ArtifactUploadPartIntent{Number: 1, SizeBytes: 13, SHA256: digest},
	)
	if err != nil {
		t.Fatalf("ClaimArtifactUpload: %v", err)
	}
	if claim.Decision != workercontrol.ArtifactUploadClaimGranted || claim.ClaimID != claimID ||
		claim.UploadID != uploadID || claim.ArtifactID != artifactID || claim.MultipartUploadID != "multipart-1" ||
		claim.UploadPart == nil || claim.UploadPart.Number != 1 || claim.UploadPart.SHA256 != digest ||
		claim.UploadPart.URL != "https://objects.internal/upload?partNumber=1" {
		t.Fatalf("ArtifactUploadClaim = %#v", claim)
	}
}

func TestClientCompletesChecksumBoundMultipartUpload(t *testing.T) {
	attemptID := uuid.MustParse("7a000000-0000-0000-0000-000000000001")
	uploadID := uuid.MustParse("7a000000-0000-0000-0000-000000000002")
	artifactID := uuid.MustParse("7a000000-0000-0000-0000-000000000003")
	claimID := uuid.MustParse("7a000000-0000-0000-0000-000000000004")
	digest := sha256.Sum256([]byte("artifact-part"))
	report := workercontrol.ArtifactUploadReport{
		SizeBytes: 13, SHA256: digest, ContentType: "video/mp4",
		CompletedParts: []workercontrol.ArtifactUploadPart{{
			Number: 1, ETag: "etag-1", SizeBytes: 13,
			ChecksumSHA256: base64.StdEncoding.EncodeToString(digest[:]),
		}},
	}
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		operation, ok := request.GetOperation().(*velav1.ConnectRequest_CompleteArtifactMultipartUpload)
		if !ok || operation.CompleteArtifactMultipartUpload.GetUploadId() != uploadID.String() ||
			operation.CompleteArtifactMultipartUpload.GetClaimId() != claimID.String() ||
			len(operation.CompleteArtifactMultipartUpload.GetCompletedParts()) != 1 ||
			string(operation.CompleteArtifactMultipartUpload.GetSha256()) != string(digest[:]) {
			return nil
		}
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_ArtifactUploadResult{ArtifactUploadResult: &velav1.ArtifactUploadResult{
				Decision: string(workercontrol.ArtifactUploadRecorded), UploadId: uploadID.String(),
				ArtifactId: artifactID.String(), ObjectVersionId: "version-1", Version: 3,
			}},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}

	result, err := client.CompleteArtifactMultipartUpload(
		context.Background(), credentials, uploadID, claimID, report,
	)
	if err != nil {
		t.Fatalf("CompleteArtifactMultipartUpload: %v", err)
	}
	if result.Decision != workercontrol.ArtifactUploadRecorded || result.UploadID != uploadID ||
		result.ArtifactID != artifactID || result.ObjectVersionID != "version-1" || result.Version != 3 {
		t.Fatalf("ArtifactUploadResult = %#v", result)
	}
}

func TestClientVerifiesExactUploadedArtifact(t *testing.T) {
	attemptID := uuid.MustParse("7b000000-0000-0000-0000-000000000001")
	uploadID := uuid.MustParse("7b000000-0000-0000-0000-000000000002")
	artifactID := uuid.MustParse("7b000000-0000-0000-0000-000000000003")
	verificationID := uuid.MustParse("7b000000-0000-0000-0000-000000000004")
	verifiedAt := time.Date(2026, 8, 25, 8, 20, 0, 0, time.UTC)
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		operation, ok := request.GetOperation().(*velav1.ConnectRequest_VerifyArtifact)
		if !ok || operation.VerifyArtifact.GetUploadId() != uploadID.String() ||
			operation.VerifyArtifact.GetVerificationId() != verificationID.String() {
			return nil
		}
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_ArtifactVerificationResult{
				ArtifactVerificationResult: &velav1.ArtifactVerificationResult{
					Decision: string(workercontrol.ArtifactVerified), VerificationId: verificationID.String(),
					UploadId: uploadID.String(), ArtifactId: artifactID.String(),
					ObjectVersionId: "version-1", Version: 4, VerifiedAt: timestamppb.New(verifiedAt),
				},
			},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}

	result, err := client.VerifyArtifact(
		context.Background(), credentials, uploadID, verificationID,
	)
	if err != nil {
		t.Fatalf("VerifyArtifact: %v", err)
	}
	if result.Decision != workercontrol.ArtifactVerified || result.VerificationID != verificationID ||
		result.UploadID != uploadID || result.ArtifactID != artifactID ||
		result.ObjectVersionID != "version-1" || result.Version != 4 || result.VerifiedAt != verifiedAt {
		t.Fatalf("ArtifactVerificationResult = %#v", result)
	}
}

func TestClientPreservesExactArtifactValidationFailureIdentity(t *testing.T) {
	attemptID := uuid.MustParse("7b100000-0000-0000-0000-000000000001")
	uploadID := uuid.MustParse("7b100000-0000-0000-0000-000000000002")
	artifactID := uuid.MustParse("7b100000-0000-0000-0000-000000000003")
	verificationID := uuid.MustParse("7b100000-0000-0000-0000-000000000004")
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_ArtifactVerificationResult{
				ArtifactVerificationResult: &velav1.ArtifactVerificationResult{
					Decision:       string(workercontrol.ArtifactValidationFailed),
					VerificationId: verificationID.String(), UploadId: uploadID.String(),
					ArtifactId: artifactID.String(), ObjectVersionId: "invalid-version-1",
				},
			},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}

	result, err := client.VerifyArtifact(
		context.Background(), credentials, uploadID, verificationID,
	)
	if err != nil {
		t.Fatalf("VerifyArtifact: %v", err)
	}
	if result.Decision != workercontrol.ArtifactValidationFailed ||
		result.VerificationID != verificationID || result.UploadID != uploadID ||
		result.ArtifactID != artifactID || result.ObjectVersionID != "invalid-version-1" {
		t.Fatalf("Artifact validation failure = %#v", result)
	}
}

func TestClientCommitsExactVisibleCompletionReceipt(t *testing.T) {
	attemptID := uuid.MustParse("7c000000-0000-0000-0000-000000000001")
	jobID := uuid.MustParse("7c000000-0000-0000-0000-000000000002")
	artifactID := uuid.MustParse("7c000000-0000-0000-0000-000000000003")
	completionID := uuid.MustParse("7c000000-0000-0000-0000-000000000004")
	artifactSetID := uuid.MustParse("7c000000-0000-0000-0000-000000000005")
	chargeID := uuid.MustParse("7c000000-0000-0000-0000-000000000006")
	digest := sha256.Sum256([]byte("artifact"))
	manifest := sha256.Sum256([]byte("manifest"))
	completedAt := time.Date(2026, 8, 25, 8, 30, 0, 0, time.UTC)
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		operation, ok := request.GetOperation().(*velav1.ConnectRequest_CompleteVisibleCompletion)
		if !ok || operation.CompleteVisibleCompletion.GetCandidate().GetCompletionId() != completionID.String() ||
			operation.CompleteVisibleCompletion.GetCandidate().GetExpectedJobVersion() != 9 {
			return nil
		}
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_VisibleCompletionResult{VisibleCompletionResult: &velav1.VisibleCompletionResult{
				Decision: string(workercontrol.VisibleCompletionCommitted), CompletionId: completionID.String(),
				JobId: jobID.String(), AttemptId: attemptID.String(), ArtifactSetId: artifactSetID.String(),
				ChargeId: chargeID.String(), JobVersion: 10, ManifestSha256: manifest[:],
				CompletedAt: timestamppb.New(completedAt),
				Artifacts: []*velav1.WorkerCommittedArtifact{{
					ArtifactId: artifactID.String(), Kind: string(workercontrol.ArtifactKindVideo), Ordinal: 0,
					ObjectKey:       "artifacts/org/project/job/attempt/artifact/video.mp4",
					ObjectVersionId: "version-1", SizeBytes: 8, Sha256: digest[:], ContentType: "video/mp4",
				}},
			}},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID: completionID, ExpectedJobVersion: 9, ArtifactIDs: []uuid.UUID{artifactID},
	}

	result, err := client.CompleteVisibleCompletion(context.Background(), credentials, candidate)
	if err != nil {
		t.Fatalf("CompleteVisibleCompletion: %v", err)
	}
	if result.Decision != workercontrol.VisibleCompletionCommitted || result.CompletionID != completionID ||
		result.JobID != jobID || result.AttemptID != attemptID || result.ArtifactSetID != artifactSetID ||
		result.ChargeID != chargeID || result.JobVersion != 10 || result.ManifestSHA256 != manifest ||
		len(result.Artifacts) != 1 || result.Artifacts[0].ArtifactID != artifactID ||
		result.CompletedAt != completedAt {
		t.Fatalf("VisibleCompletionResult = %#v", result)
	}
}

func TestClientRejectsVisibleCompletionForDifferentArtifactSet(t *testing.T) {
	attemptID := uuid.MustParse("7d000000-0000-0000-0000-000000000001")
	jobID := uuid.MustParse("7d000000-0000-0000-0000-000000000002")
	candidateArtifactID := uuid.MustParse("7d000000-0000-0000-0000-000000000003")
	returnedArtifactID := uuid.MustParse("7d000000-0000-0000-0000-000000000004")
	completionID := uuid.MustParse("7d000000-0000-0000-0000-000000000005")
	digest := sha256.Sum256([]byte("artifact"))
	manifest := sha256.Sum256([]byte("manifest"))
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_VisibleCompletionResult{VisibleCompletionResult: &velav1.VisibleCompletionResult{
				Decision: string(workercontrol.VisibleCompletionCommitted), CompletionId: completionID.String(),
				JobId: jobID.String(), AttemptId: attemptID.String(),
				ArtifactSetId: uuid.NewString(), ChargeId: uuid.NewString(), JobVersion: 10,
				ManifestSha256: manifest[:], CompletedAt: timestamppb.Now(),
				Artifacts: []*velav1.WorkerCommittedArtifact{{
					ArtifactId: returnedArtifactID.String(), Kind: string(workercontrol.ArtifactKindVideo),
					Ordinal: 0, ObjectKey: "artifacts/org/project/job/attempt/artifact/video.mp4",
					ObjectVersionId: "version-1", SizeBytes: 8, Sha256: digest[:], ContentType: "video/mp4",
				}},
			}},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID: completionID, ExpectedJobVersion: 9,
		ArtifactIDs: []uuid.UUID{candidateArtifactID},
	}

	if _, err := client.CompleteVisibleCompletion(context.Background(), credentials, candidate); err == nil {
		t.Fatal("CompleteVisibleCompletion accepted an Artifact outside the candidate set")
	}
}

type oneResponseClientTestServer struct {
	velav1.UnimplementedWorkerControlServiceServer
	response func(*velav1.ConnectRequest) *velav1.ConnectResponse
}

type blockingClientTestServer struct {
	velav1.UnimplementedWorkerControlServiceServer
}

func (blockingClientTestServer) Connect(stream velav1.WorkerControlService_ConnectServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

type reconnectingClientTestServer struct {
	velav1.UnimplementedWorkerControlServiceServer
	mu                         sync.Mutex
	connects                   int
	workerID, attemptID, jobID uuid.UUID
}

func (server *reconnectingClientTestServer) Connect(stream velav1.WorkerControlService_ConnectServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	server.mu.Lock()
	server.connects++
	connect := server.connects
	server.mu.Unlock()
	if connect == 1 {
		return status.Error(codes.Internal, "injected stream failure")
	}
	now := time.Now().UTC()
	return stream.Send(&velav1.ConnectResponse{
		RequestId: request.GetRequestId(),
		Result: &velav1.ConnectResponse_Assignment{Assignment: &velav1.WorkerAssignment{
			AttemptId: server.attemptID.String(), JobId: server.jobID.String(),
			WorkerId: server.workerID.String(), WorkerEpoch: 7,
			ModelRevisionId:            "70800000-0000-0000-0000-000000000004",
			GenerationPresetRevisionId: "70800000-0000-0000-0000-000000000005",
			ExecutionProfileRevisionId: "70800000-0000-0000-0000-000000000006",
			OutputSpecId:               "70800000-0000-0000-0000-000000000007",
			RequestContentJson:         []byte(`{"prompt":"reconnected"}`),
			AttemptNumber:              1, LeaseToken: "lease-token", LeaseFence: 3,
			LeaseExpiresAt: timestamppb.New(now.Add(time.Minute)),
			LeaseValidFor:  durationpb.New(time.Minute),
		}},
	})
}

func (server *oneResponseClientTestServer) Connect(
	stream velav1.WorkerControlService_ConnectServer,
) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	response := server.response(request)
	if response == nil {
		return io.ErrUnexpectedEOF
	}
	return stream.Send(response)
}

func newClientTestConnection(
	t *testing.T,
	server velav1.WorkerControlServiceServer,
) *Client {
	t.Helper()
	grpcServer := grpc.NewServer()
	velav1.RegisterWorkerControlServiceServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-serveDone; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve: %v", serveErr)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	client, err := DialClient(ctx, listener.Addr().String(), insecure.NewCredentials())
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type executionClientTestServer struct {
	velav1.UnimplementedWorkerControlServiceServer
	workerID, poolID, cycleID, profileID, attemptID, jobID uuid.UUID
	now                                                    time.Time
	requests                                               int
}

func (server *executionClientTestServer) Connect(
	stream velav1.WorkerControlService_ConnectServer,
) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		server.requests++
		response := &velav1.ConnectResponse{RequestId: request.GetRequestId()}
		switch operation := request.GetOperation().(type) {
		case *velav1.ConnectRequest_ReportCapacity:
			if operation.ReportCapacity.GetWorkerEpoch() != 7 ||
				operation.ReportCapacity.GetObservationSequence() != 9 ||
				!operation.ReportCapacity.GetObservedAt().AsTime().Equal(server.now) ||
				operation.ReportCapacity.GetWatermarkState() !=
					velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_NORMAL ||
				operation.ReportCapacity.GetTotalBytes() != 1000 ||
				operation.ReportCapacity.GetFreeBytes() != 700 ||
				!operation.ReportCapacity.GetArtifactStoreReachable() {
				return io.ErrUnexpectedEOF
			}
			response.Result = &velav1.ConnectResponse_CapacityResult{
				CapacityResult: &velav1.ReportWorkerCapacityResult{
					WorkerPoolId:            "70000000-0000-0000-0000-000000000001",
					WorkerState:             string(workercontrol.CapacityAdmittable),
					PoolState:               string(workercontrol.CapacityAdmittable),
					WorkerAssignmentAllowed: true, PoolAssignmentAllowed: true,
				},
			}
		case *velav1.ConnectRequest_GetReadinessWork:
			if operation.GetReadinessWork.GetWorkerEpoch() != 7 {
				return io.ErrUnexpectedEOF
			}
			response.Result = &velav1.ConnectResponse_ReadinessWork{
				ReadinessWork: &velav1.WorkerReadinessWork{
					Available: true, CycleId: server.cycleID.String(),
					Check:    velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY,
					WorkerId: server.workerID.String(), WorkerPoolId: server.poolID.String(),
					WorkerEpoch: 7, NodeIdentity: "h3-node-01",
					ExecutionProfileRevisionId: server.profileID.String(),
					InferenceBackendRevision:   "sglang-h3-v3",
					Deadline:                   timestamppb.New(server.now.Add(30 * time.Minute)),
				},
			}
		case *velav1.ConnectRequest_ReportReadiness:
			if operation.ReportReadiness.GetWorkerEpoch() != 7 ||
				operation.ReportReadiness.GetCycleId() != server.cycleID.String() ||
				operation.ReportReadiness.GetCheck() !=
					velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY ||
				!operation.ReportReadiness.GetPassed() ||
				len(operation.ReportReadiness.GetEvidenceDigest()) != sha256.Size {
				return io.ErrUnexpectedEOF
			}
			response.Result = &velav1.ConnectResponse_ReadinessResult{
				ReadinessResult: &velav1.WorkerReadinessResult{
					CycleId:            server.cycleID.String(),
					State:              velav1.FleetReadinessState_FLEET_READINESS_STATE_CHECKING,
					NextCheck:          velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_DEVICE,
					WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING,
					WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
				},
			}
		case *velav1.ConnectRequest_Acquire:
			if operation.Acquire.GetWorkerEpoch() != 7 {
				return io.ErrUnexpectedEOF
			}
			response.Result = &velav1.ConnectResponse_Assignment{Assignment: &velav1.WorkerAssignment{
				AttemptId: server.attemptID.String(), JobId: server.jobID.String(),
				WorkerId: server.workerID.String(), WorkerEpoch: 7,
				ModelRevisionId:            "74000000-0000-0000-0000-000000000004",
				GenerationPresetRevisionId: "75000000-0000-0000-0000-000000000005",
				ExecutionProfileRevisionId: "76000000-0000-0000-0000-000000000006",
				OutputSpecId:               "77000000-0000-0000-0000-000000000007",
				RequestContentJson:         []byte(`{"prompt":"private"}`),
				AttemptNumber:              1, LeaseToken: "lease-token", LeaseFence: 3,
				LeaseExpiresAt: timestamppb.New(server.now.Add(time.Minute)),
				LeaseValidFor:  durationpb.New(45 * time.Second),
				DebugDumpAuthorization: &velav1.DebugDumpAuthorizationSnapshot{
					AuthorizationId: "73000000-0000-0000-0000-000000000004",
					ExpiresAt:       timestamppb.New(server.now.Add(72 * time.Hour)),
				},
			}}
		case *velav1.ConnectRequest_Start:
			response.Result = &velav1.ConnectResponse_StartResult{StartResult: &velav1.StartWorkerResult{
				Decision: string(workercontrol.StartGranted), AttemptId: server.attemptID.String(),
				JobId: server.jobID.String(), WorkerId: server.workerID.String(),
				WorkerEpoch: 7, LeaseFence: 3, StartedAt: timestamppb.New(server.now),
			}}
		case *velav1.ConnectRequest_Heartbeat:
			response.Result = &velav1.ConnectResponse_HeartbeatResult{HeartbeatResult: &velav1.HeartbeatResult{
				Decision: string(workercontrol.HeartbeatContinue), AttemptId: server.attemptID.String(),
				JobId: server.jobID.String(), WorkerId: server.workerID.String(),
				WorkerEpoch: 7, LeaseFence: 3, HeartbeatSequence: operation.Heartbeat.GetSequence(),
				ExecutionPhase:    string(workercontrol.ExecutionPhaseGenerating),
				ProgressUpdatedAt: timestamppb.New(server.now),
				LeaseExpiresAt:    timestamppb.New(server.now.Add(time.Minute)),
				LeaseValidFor:     durationpb.New(50 * time.Second),
			}}
		case *velav1.ConnectRequest_Fail:
			response.Result = &velav1.ConnectResponse_RetryDecision{RetryDecision: &velav1.RetryDecision{
				Disposition:  string(workercontrol.RetryDispositionRetryWait),
				FailureClass: operation.Fail.GetObservation().GetFailureClass(),
				AttemptId:    server.attemptID.String(), JobId: server.jobID.String(),
				AttemptState: string(workercontrol.FailedAttempt), JobFence: 4, JobVersion: 5,
				NextRetryAt: timestamppb.New(server.now.Add(time.Second)), DecidedAt: timestamppb.New(server.now),
			}}
		default:
			return io.ErrUnexpectedEOF
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}
