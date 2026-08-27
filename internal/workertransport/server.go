package workertransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/executionprogress"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerIdentityResolver interface {
	ResolveWorker(context.Context, string) (workercontrol.AuthenticatedWorker, error)
}

type WorkerCoordinator interface {
	Acquire(
		context.Context,
		workercontrol.AuthenticatedWorker,
		int64,
		*workercontrol.AssignmentCandidate,
	) (workercontrol.Assignment, error)
	Start(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
	) (workercontrol.StartResult, error)
	Heartbeat(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		workercontrol.HeartbeatObservation,
	) (workercontrol.HeartbeatResult, error)
	Fail(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		workercontrol.FailureObservation,
	) (workercontrol.RetryDecision, error)
	BeginFinalization(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
	) (workercontrol.FinalizationPlan, error)
	ClaimArtifactUpload(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		uuid.UUID,
	) (workercontrol.ArtifactUploadClaim, error)
	RecordArtifactMultipartSession(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		uuid.UUID,
		string,
	) (workercontrol.ArtifactMultipartSession, error)
	InspectArtifactUpload(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
	) (workercontrol.ArtifactUploadStatus, error)
	RecordArtifactCompletionIntent(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		workercontrol.ArtifactUploadReport,
	) (workercontrol.ArtifactCompletionIntentResult, error)
	RecordArtifactUploaded(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		workercontrol.ArtifactUploadReport,
	) (workercontrol.ArtifactUploadResult, error)
	VerifyArtifact(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		uuid.UUID,
	) (workercontrol.ArtifactVerificationResult, error)
	CompleteVisibleCompletion(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		workercontrol.VisibleCompletionCandidate,
	) (workercontrol.VisibleCompletionResult, error)
}

type ArtifactUploadStore interface {
	CreateMultipartUpload(context.Context, string, string) (artifactstore.MultipartUpload, error)
	ListParts(context.Context, artifactstore.MultipartUpload) ([]artifactstore.CompletedPart, error)
	PresignUploadPart(
		context.Context,
		artifactstore.MultipartUpload,
		int32,
		int64,
		[sha256.Size]byte,
		time.Time,
	) (artifactstore.SignedUploadPart, error)
	CompleteMultipartUpload(
		context.Context,
		artifactstore.MultipartUpload,
		[]artifactstore.CompletedPart,
	) (artifactstore.ObjectVersion, error)
	HeadCurrentVersion(context.Context, string) (artifactstore.ObjectVersion, error)
	AbortMultipartUpload(context.Context, artifactstore.MultipartUpload) error
}

type DebugDumpCoordinator interface {
	ClaimDebugDumpUpload(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		workercontrol.DebugDumpUploadIntent,
		uuid.UUID,
	) (workercontrol.DebugDumpUploadClaim, error)
	RecordDebugDumpMultipartSession(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		uuid.UUID,
		string,
	) (workercontrol.DebugDumpMultipartSession, error)
	InspectDebugDumpUpload(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
	) (workercontrol.DebugDumpUploadStatus, error)
	RecordDebugDumpCompletionIntent(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		workercontrol.DebugDumpUploadReport,
	) (workercontrol.DebugDumpCompletionIntentResult, error)
	RecordDebugDumpUploaded(
		context.Context,
		workercontrol.AuthenticatedWorker,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		workercontrol.DebugDumpUploadReport,
	) (workercontrol.DebugDumpUploadResult, error)
}

type WorkerFleetService interface {
	ObserveCapacity(context.Context, fleet.CapacityObservation) (fleet.CapacityResult, error)
	GetWorkerReadinessWork(context.Context, uuid.UUID, int64) (fleet.ReadinessWork, error)
	ReportReadiness(context.Context, fleet.ReadinessEvidence) (fleet.ReadinessResult, error)
}

type Server struct {
	velav1.UnimplementedWorkerControlServiceServer
	resolver    WorkerIdentityResolver
	coordinator WorkerCoordinator
	debugDumps  DebugDumpCoordinator
	uploadStore ArtifactUploadStore
	fleet       WorkerFleetService
}

func NewServer(
	resolver WorkerIdentityResolver,
	coordinator WorkerCoordinator,
	uploadStore ArtifactUploadStore,
	fleetServices ...WorkerFleetService,
) (*Server, error) {
	if resolver == nil {
		return nil, errors.New("worker identity resolver is required")
	}
	if coordinator == nil {
		return nil, errors.New("worker finalization coordinator is required")
	}
	if uploadStore == nil {
		return nil, errors.New("artifact upload store is required")
	}
	if len(fleetServices) > 1 || len(fleetServices) == 1 && fleetServices[0] == nil {
		return nil, errors.New("at most one configured Worker Fleet service is allowed")
	}
	var fleetService WorkerFleetService
	if len(fleetServices) == 1 {
		fleetService = fleetServices[0]
	}
	debugDumps, _ := coordinator.(DebugDumpCoordinator)
	return &Server{
		resolver: resolver, coordinator: coordinator, uploadStore: uploadStore,
		debugDumps: debugDumps, fleet: fleetService,
	}, nil
}

func (server *Server) Connect(stream velav1.WorkerControlService_ConnectServer) error {
	if server == nil || server.resolver == nil || server.coordinator == nil ||
		server.uploadStore == nil || stream == nil {
		return status.Error(codes.FailedPrecondition, "Worker control server is not configured")
	}
	worker, err := authenticatedWorker(stream.Context(), server.resolver)
	if err != nil {
		return status.Error(codes.Unauthenticated, "verified Worker mTLS identity is required")
	}
	for {
		request, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			return nil
		}
		if receiveErr != nil {
			return receiveErr
		}
		response, dispatchErr := server.dispatch(stream.Context(), worker, request)
		if dispatchErr != nil {
			slog.ErrorContext(
				stream.Context(),
				"Worker operation failed",
				"worker_id", worker.ID,
				"request_id", request.GetRequestId(),
				"error", dispatchErr,
			)
			return status.Error(codes.Internal, "Worker operation could not be completed")
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (server *Server) dispatch(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	request *velav1.ConnectRequest,
) (*velav1.ConnectResponse, error) {
	if request == nil {
		return invalidResponse("", "request is required"), nil
	}
	requestID := request.GetRequestId()
	if _, err := uuid.Parse(requestID); err != nil {
		return invalidResponse(requestID, "request_id must be a UUID"), nil
	}
	switch operation := request.GetOperation().(type) {
	case *velav1.ConnectRequest_ReportCapacity:
		if server.fleet == nil {
			return invalidResponse(requestID, "Worker capacity reporting is unavailable"), nil
		}
		observation, ok := parseCapacityObservation(operation.ReportCapacity)
		if !ok || worker.PoolID == uuid.Nil || !validObservedBy(worker.SPIFFEID) {
			return invalidResponse(requestID, "valid authenticated Worker capacity is required"), nil
		}
		result, err := server.fleet.ObserveCapacity(ctx, fleet.CapacityObservation{
			WorkerID: worker.ID, WorkerPoolID: worker.PoolID,
			WorkerEpoch: observation.WorkerEpoch, Sequence: observation.Sequence,
			ObservedAt:     observation.ObservedAt,
			WatermarkState: fleet.ScratchWatermarkState(observation.WatermarkState),
			TotalBytes:     observation.TotalBytes, FreeBytes: observation.FreeBytes,
			HighWatermarkBytes:     observation.HighWatermarkBytes,
			LowWatermarkBytes:      observation.LowWatermarkBytes,
			CriticalFreeBytes:      observation.CriticalFreeBytes,
			ArtifactStoreReachable: observation.ArtifactStoreReachable,
			ObservedBy:             worker.SPIFFEID,
		})
		if err != nil {
			return nil, err
		}
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_CapacityResult{
				CapacityResult: &velav1.ReportWorkerCapacityResult{
					WorkerPoolId: result.WorkerPoolID.String(), Replayed: result.Replayed,
					WorkerState: string(result.WorkerState), PoolState: string(result.PoolState),
					WorkerAssignmentAllowed: result.WorkerAssignmentAllowed,
					PoolReadinessAllowed:    result.PoolReadinessAllowed,
					PoolAssignmentAllowed:   result.PoolAssignmentAllowed,
				},
			},
		}, nil
	case *velav1.ConnectRequest_GetReadinessWork:
		if server.fleet == nil {
			return invalidResponse(requestID, "Worker readiness is unavailable"), nil
		}
		workerEpoch := operation.GetReadinessWork.GetWorkerEpoch()
		if workerEpoch <= 0 || worker.PoolID == uuid.Nil || !validObservedBy(worker.SPIFFEID) {
			return invalidResponse(requestID, "valid authenticated Worker readiness identity is required"), nil
		}
		work, err := server.fleet.GetWorkerReadinessWork(ctx, worker.ID, workerEpoch)
		if err != nil {
			return nil, err
		}
		message, err := workerReadinessWork(work, worker, workerEpoch)
		if err != nil {
			return nil, err
		}
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result:    &velav1.ConnectResponse_ReadinessWork{ReadinessWork: message},
		}, nil
	case *velav1.ConnectRequest_ReportReadiness:
		if server.fleet == nil {
			return invalidResponse(requestID, "Worker readiness is unavailable"), nil
		}
		workerEpoch := operation.ReportReadiness.GetWorkerEpoch()
		cycleID, cycleOK := parseRequiredUUID(operation.ReportReadiness.GetCycleId())
		check, checkOK := fleetReadinessCheckFromProto(operation.ReportReadiness.GetCheck())
		digest := operation.ReportReadiness.GetEvidenceDigest()
		if workerEpoch <= 0 || !cycleOK || !checkOK || len(digest) != sha256.Size ||
			worker.PoolID == uuid.Nil || !validObservedBy(worker.SPIFFEID) {
			return invalidResponse(requestID, "valid authenticated Worker readiness evidence is required"), nil
		}
		work, err := server.fleet.GetWorkerReadinessWork(ctx, worker.ID, workerEpoch)
		if err != nil {
			return nil, err
		}
		if !validWorkerReadinessWork(work, worker, workerEpoch) ||
			work.CycleID != cycleID || work.Check != check {
			return invalidResponse(requestID, "Worker readiness evidence does not match current work"), nil
		}
		result, err := server.fleet.ReportReadiness(ctx, fleet.ReadinessEvidence{
			CycleID: cycleID, Check: check, Passed: operation.ReportReadiness.GetPassed(),
			EvidenceDigest: append([]byte(nil), digest...), ObservedBy: worker.SPIFFEID,
		})
		if err != nil {
			return nil, err
		}
		message, err := workerReadinessResult(result, cycleID)
		if err != nil {
			return nil, err
		}
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result:    &velav1.ConnectResponse_ReadinessResult{ReadinessResult: message},
		}, nil
	case *velav1.ConnectRequest_Acquire:
		if operation.Acquire == nil || operation.Acquire.GetWorkerEpoch() <= 0 {
			return invalidResponse(requestID, "positive Worker epoch is required"), nil
		}
		result, err := server.coordinator.Acquire(
			ctx, worker, operation.Acquire.GetWorkerEpoch(), nil,
		)
		if err != nil {
			var failure *workercontrol.Failure
			if errors.As(err, &failure) {
				return operationErrorResponse(requestID, string(failure.Code), failure.Message), nil
			}
			return nil, err
		}
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_Assignment{
				Assignment: workerAssignment(result),
			},
		}, nil
	case *velav1.ConnectRequest_Start:
		credentials, ok := parseLeaseCredentials(operation.Start.GetLease())
		if !ok {
			return invalidResponse(requestID, "valid Lease credentials are required"), nil
		}
		result, err := server.coordinator.Start(ctx, worker, credentials)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_StartResult{
				StartResult: startWorkerResult(result),
			},
		}, err
	case *velav1.ConnectRequest_Heartbeat:
		credentials, ok := parseLeaseCredentials(operation.Heartbeat.GetLease())
		observation, observationOK := parseHeartbeatObservation(operation.Heartbeat)
		if !ok || !observationOK {
			return invalidResponse(requestID, "valid Lease and Heartbeat observation are required"), nil
		}
		result, err := server.coordinator.Heartbeat(ctx, worker, credentials, observation)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_HeartbeatResult{
				HeartbeatResult: heartbeatResult(result),
			},
		}, err
	case *velav1.ConnectRequest_Fail:
		credentials, ok := parseLeaseCredentials(operation.Fail.GetLease())
		observation, observationOK := parseFailureObservation(operation.Fail.GetObservation())
		if !ok || !observationOK {
			return invalidResponse(requestID, "valid Lease and Failure observation are required"), nil
		}
		result, err := server.coordinator.Fail(ctx, worker, credentials, observation)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_RetryDecision{
				RetryDecision: retryDecision(result),
			},
		}, err
	case *velav1.ConnectRequest_BeginFinalization:
		credentials, ok := parseLeaseCredentials(operation.BeginFinalization.GetLease())
		if !ok {
			return invalidResponse(requestID, "valid Lease credentials are required"), nil
		}
		result, err := server.coordinator.BeginFinalization(ctx, worker, credentials)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_FinalizationPlan{
				FinalizationPlan: finalizationPlan(result),
			},
		}, err
	case *velav1.ConnectRequest_ClaimArtifactUpload:
		credentials, ok := parseLeaseCredentials(operation.ClaimArtifactUpload.GetLease())
		uploadID, uploadOK := parseRequiredUUID(operation.ClaimArtifactUpload.GetUploadId())
		claimID, claimOK := parseRequiredUUID(operation.ClaimArtifactUpload.GetClaimId())
		part, partOK := parseArtifactUploadPartIntent(operation.ClaimArtifactUpload.GetPart())
		if !ok || !uploadOK || !claimOK || !partOK {
			return invalidResponse(requestID, "valid Lease, upload, and claim identities are required"), nil
		}
		result, err := server.coordinator.ClaimArtifactUpload(
			ctx, worker, credentials, uploadID, claimID,
		)
		if err != nil {
			return nil, err
		}
		claim, err := server.prepareArtifactUploadClaim(
			ctx, worker, credentials, claimID, result, part,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_ArtifactUploadClaim{
				ArtifactUploadClaim: claim,
			},
		}, err
	case *velav1.ConnectRequest_RecordArtifactMultipartSession:
		credentials, ok := parseLeaseCredentials(operation.RecordArtifactMultipartSession.GetLease())
		uploadID, uploadOK := parseRequiredUUID(operation.RecordArtifactMultipartSession.GetUploadId())
		claimID, claimOK := parseRequiredUUID(operation.RecordArtifactMultipartSession.GetClaimId())
		multipartID := operation.RecordArtifactMultipartSession.GetMultipartUploadId()
		if !ok || !uploadOK || !claimOK || multipartID == "" || len(multipartID) > 2000 ||
			strings.ContainsRune(multipartID, '\x00') {
			return invalidResponse(requestID, "valid Lease and multipart identities are required"), nil
		}
		result, err := server.coordinator.RecordArtifactMultipartSession(
			ctx, worker, credentials, uploadID, claimID, multipartID,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_ArtifactMultipartSession{
				ArtifactMultipartSession: artifactMultipartSession(result),
			},
		}, err
	case *velav1.ConnectRequest_RecordArtifactUploaded:
		credentials, ok := parseLeaseCredentials(operation.RecordArtifactUploaded.GetLease())
		uploadID, uploadOK := parseRequiredUUID(operation.RecordArtifactUploaded.GetUploadId())
		report, reportOK := parseArtifactUploadReport(operation.RecordArtifactUploaded.GetReport())
		if !ok || !uploadOK || !reportOK {
			return invalidResponse(requestID, "valid Lease, upload identity, and report are required"), nil
		}
		result, err := server.coordinator.RecordArtifactUploaded(
			ctx, worker, credentials, uploadID, report,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_ArtifactUploadResult{
				ArtifactUploadResult: artifactUploadResult(result),
			},
		}, err
	case *velav1.ConnectRequest_CompleteArtifactMultipartUpload:
		credentials, ok := parseLeaseCredentials(
			operation.CompleteArtifactMultipartUpload.GetLease(),
		)
		uploadID, uploadOK := parseRequiredUUID(
			operation.CompleteArtifactMultipartUpload.GetUploadId(),
		)
		_, claimOK := parseRequiredUUID(
			operation.CompleteArtifactMultipartUpload.GetClaimId(),
		)
		report, completedParts, reportOK := parseMultipartCompletion(
			operation.CompleteArtifactMultipartUpload,
		)
		if !ok || !uploadOK || !claimOK || !reportOK {
			return invalidResponse(
				requestID,
				"valid Lease, upload identity, claim identity, and multipart receipt are required",
			), nil
		}
		result, err := server.completeArtifactMultipartUpload(
			ctx, worker, credentials, uploadID, report, completedParts,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_ArtifactUploadResult{
				ArtifactUploadResult: artifactUploadResult(result),
			},
		}, err
	case *velav1.ConnectRequest_ClaimDebugDumpUpload:
		if server.debugDumps == nil {
			return nil, errors.New("debug dump upload coordinator is not configured")
		}
		request := operation.ClaimDebugDumpUpload
		credentials, credentialsOK := parseLeaseCredentials(request.GetLease())
		debugDumpID, dumpOK := parseRequiredUUID(request.GetDebugDumpId())
		authorizationID, authorizationOK := parseRequiredUUID(request.GetAuthorizationId())
		claimID, claimOK := parseRequiredUUID(request.GetClaimId())
		intent, part, intentOK := parseDebugDumpUploadIntent(request)
		if !credentialsOK || !dumpOK || !authorizationOK || !claimOK || !intentOK ||
			intent.DebugDumpID != debugDumpID || intent.AuthorizationID != authorizationID {
			return invalidResponse(requestID, "valid Lease and debug dump upload intent are required"), nil
		}
		result, err := server.debugDumps.ClaimDebugDumpUpload(
			ctx, worker, credentials, intent, claimID,
		)
		if err != nil {
			return nil, err
		}
		claim, err := server.prepareDebugDumpUploadClaim(
			ctx, worker, credentials, claimID, result, part,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_DebugDumpUploadClaim{
				DebugDumpUploadClaim: claim,
			},
		}, err
	case *velav1.ConnectRequest_RecordDebugDumpMultipartSession:
		if server.debugDumps == nil {
			return nil, errors.New("debug dump upload coordinator is not configured")
		}
		request := operation.RecordDebugDumpMultipartSession
		credentials, credentialsOK := parseLeaseCredentials(request.GetLease())
		debugDumpID, dumpOK := parseRequiredUUID(request.GetDebugDumpId())
		claimID, claimOK := parseRequiredUUID(request.GetClaimId())
		multipartID := request.GetMultipartUploadId()
		if !credentialsOK || !dumpOK || !claimOK || multipartID == "" ||
			len(multipartID) > 1000 || strings.ContainsRune(multipartID, '\x00') {
			return invalidResponse(requestID, "valid Lease and debug dump multipart identities are required"), nil
		}
		result, err := server.debugDumps.RecordDebugDumpMultipartSession(
			ctx, worker, credentials, debugDumpID, claimID, multipartID,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_DebugDumpMultipartSession{
				DebugDumpMultipartSession: debugDumpMultipartSession(result),
			},
		}, err
	case *velav1.ConnectRequest_CompleteDebugDumpMultipartUpload:
		if server.debugDumps == nil {
			return nil, errors.New("debug dump upload coordinator is not configured")
		}
		request := operation.CompleteDebugDumpMultipartUpload
		credentials, credentialsOK := parseLeaseCredentials(request.GetLease())
		debugDumpID, dumpOK := parseRequiredUUID(request.GetDebugDumpId())
		_, claimOK := parseRequiredUUID(request.GetClaimId())
		authorizationID, authorizationOK := parseRequiredUUID(request.GetAuthorizationId())
		report, completedParts, reportOK := parseDebugDumpMultipartCompletion(request)
		if !credentialsOK || !dumpOK || !claimOK || !authorizationOK || !reportOK {
			return invalidResponse(requestID, "valid Lease and debug dump multipart receipt are required"), nil
		}
		result, err := server.completeDebugDumpMultipartUpload(
			ctx, worker, credentials, debugDumpID, authorizationID, report, completedParts,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_DebugDumpUploadResult{
				DebugDumpUploadResult: debugDumpUploadResult(result),
			},
		}, err
	case *velav1.ConnectRequest_VerifyArtifact:
		credentials, ok := parseLeaseCredentials(operation.VerifyArtifact.GetLease())
		uploadID, uploadOK := parseRequiredUUID(operation.VerifyArtifact.GetUploadId())
		verificationID, verificationOK := parseRequiredUUID(operation.VerifyArtifact.GetVerificationId())
		if !ok || !uploadOK || !verificationOK {
			return invalidResponse(requestID, "valid Lease, upload, and verification identities are required"), nil
		}
		result, err := server.coordinator.VerifyArtifact(
			ctx, worker, credentials, uploadID, verificationID,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_ArtifactVerificationResult{
				ArtifactVerificationResult: artifactVerificationResult(result),
			},
		}, err
	case *velav1.ConnectRequest_CompleteVisibleCompletion:
		credentials, ok := parseLeaseCredentials(operation.CompleteVisibleCompletion.GetLease())
		candidate, candidateOK := parseVisibleCompletionCandidate(
			operation.CompleteVisibleCompletion.GetCandidate(),
		)
		if !ok || !candidateOK {
			return invalidResponse(requestID, "valid Lease and Visible Completion candidate are required"), nil
		}
		result, err := server.coordinator.CompleteVisibleCompletion(
			ctx, worker, credentials, candidate,
		)
		return &velav1.ConnectResponse{
			RequestId: requestID,
			Result: &velav1.ConnectResponse_VisibleCompletionResult{
				VisibleCompletionResult: visibleCompletionResult(result),
			},
		}, err
	default:
		return invalidResponse(requestID, "operation is required"), nil
	}
}

type artifactUploadPartIntent struct {
	number    int32
	sizeBytes int64
	digest    [sha256.Size]byte
}

func (server *Server) prepareArtifactUploadClaim(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	claimID uuid.UUID,
	claim workercontrol.ArtifactUploadClaim,
	part *artifactUploadPartIntent,
) (*velav1.ArtifactUploadClaim, error) {
	if claim.Decision != workercontrol.ArtifactUploadClaimGranted {
		return artifactUploadClaim(claim, nil, nil, false)
	}
	if claim.UploadID == uuid.Nil || claim.ArtifactID == uuid.Nil || claim.ObjectKey == "" ||
		claim.ExpectedContentType == "" || claim.UploadExpiresAt.IsZero() {
		return nil, errors.New("granted Artifact upload claim is incomplete")
	}

	upload := artifactstore.MultipartUpload{
		ObjectKey: claim.ObjectKey, UploadID: claim.MultipartUploadID,
		ContentType: claim.ExpectedContentType,
	}
	if upload.UploadID == "" {
		created, err := server.uploadStore.CreateMultipartUpload(
			ctx, claim.ObjectKey, claim.ExpectedContentType,
		)
		if err != nil {
			return nil, err
		}
		if created.UploadID == "" || created.ObjectKey != claim.ObjectKey ||
			created.ContentType != claim.ExpectedContentType {
			return nil, server.abortOrphanMultipart(
				ctx, created, errors.New("created multipart session does not match its claim"),
			)
		}
		recorded, err := server.coordinator.RecordArtifactMultipartSession(
			ctx, worker, credentials, claim.UploadID, claimID, created.UploadID,
		)
		if err != nil {
			return nil, server.abortOrphanMultipart(ctx, created, err)
		}
		switch recorded.Decision {
		case workercontrol.ArtifactMultipartSessionRecorded:
			if recorded.MultipartUploadID != created.UploadID ||
				recorded.UploadID != claim.UploadID || recorded.ArtifactID != claim.ArtifactID {
				return nil, server.abortOrphanMultipart(
					ctx, created, errors.New("recorded multipart session does not match its claim"),
				)
			}
			upload = created
			claim.MultipartUploadID = created.UploadID
			claim.Version = recorded.Version
		case workercontrol.ArtifactMultipartSessionConflict:
			if err := server.abortOrphanMultipart(ctx, created, nil); err != nil {
				return nil, err
			}
			status, err := server.coordinator.InspectArtifactUpload(
				ctx, worker, credentials, claim.UploadID,
			)
			if err != nil {
				return nil, err
			}
			if status.Decision != workercontrol.ArtifactUploadStatusFound ||
				status.State != workercontrol.ArtifactUploadStateUploading || status.MultipartUploadID == "" ||
				status.UploadID != claim.UploadID || status.ArtifactID != claim.ArtifactID ||
				status.ObjectKey != claim.ObjectKey ||
				status.ExpectedContentType != claim.ExpectedContentType {
				claim.Decision = workercontrol.ArtifactUploadClaimBusy
				return artifactUploadClaim(claim, nil, nil, false)
			}
			claim.MultipartUploadID = status.MultipartUploadID
			claim.UploadExpiresAt = status.UploadExpiresAt
			claim.Version = status.Version
			upload.UploadID = status.MultipartUploadID
		case workercontrol.ArtifactMultipartSessionRejected:
			if err := server.abortOrphanMultipart(ctx, created, nil); err != nil {
				return nil, err
			}
			claim.Decision = workercontrol.ArtifactUploadClaimRejectedStaleLease
			return artifactUploadClaim(claim, nil, nil, false)
		default:
			return nil, server.abortOrphanMultipart(
				ctx, created, errors.New("unknown multipart session decision"),
			)
		}
	}

	completedParts, err := server.uploadStore.ListParts(ctx, upload)
	if err != nil {
		return nil, err
	}
	if _, err := artifactUploadPartReports(completedParts); err != nil {
		return nil, err
	}
	if part == nil {
		return artifactUploadClaim(claim, completedParts, nil, false)
	}

	wantedChecksum := base64.StdEncoding.EncodeToString(part.digest[:])
	for _, completed := range completedParts {
		if completed.Number != part.number {
			continue
		}
		if completed.SizeBytes == part.sizeBytes && completed.ChecksumSHA256 == wantedChecksum {
			return artifactUploadClaim(claim, completedParts, nil, true)
		}
		break
	}
	signed, err := server.uploadStore.PresignUploadPart(
		ctx, upload, part.number, part.sizeBytes, part.digest, claim.UploadExpiresAt,
	)
	if err != nil {
		return nil, err
	}
	if signed.Method != http.MethodPut || signed.URL == "" || signed.IssuedAt.IsZero() ||
		!signed.ExpiresAt.After(signed.IssuedAt) ||
		signed.ExpiresAt.After(claim.UploadExpiresAt) ||
		signed.Headers.Get("Content-Length") != strconv.FormatInt(part.sizeBytes, 10) ||
		signed.Headers.Get("X-Amz-Checksum-Sha256") != wantedChecksum {
		return nil, errors.New("signed Artifact upload part does not match its intent")
	}
	return artifactUploadClaim(claim, completedParts, &signedArtifactUploadPart{
		intent: *part,
		signed: signed,
	}, false)
}

func (server *Server) completeArtifactMultipartUpload(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	uploadID uuid.UUID,
	report workercontrol.ArtifactUploadReport,
	completedParts []artifactstore.CompletedPart,
) (workercontrol.ArtifactUploadResult, error) {
	statusResult, err := server.coordinator.InspectArtifactUpload(
		ctx, worker, credentials, uploadID,
	)
	if err != nil {
		return workercontrol.ArtifactUploadResult{}, err
	}
	if statusResult.Decision != workercontrol.ArtifactUploadStatusFound {
		return workercontrol.ArtifactUploadResult{
			Decision: workercontrol.ArtifactUploadRejected,
		}, nil
	}
	if statusResult.ObjectKey == "" || statusResult.ExpectedContentType != report.ContentType ||
		statusResult.UploadID != uploadID || statusResult.ArtifactID == uuid.Nil {
		return workercontrol.ArtifactUploadResult{
			Decision: workercontrol.ArtifactUploadConflict,
		}, nil
	}

	switch statusResult.State {
	case workercontrol.ArtifactUploadStateUploaded, workercontrol.ArtifactUploadStateVerified:
		if statusResult.ObjectVersionID == "" {
			return workercontrol.ArtifactUploadResult{}, errors.New(
				"persisted Artifact upload receipt has no object version",
			)
		}
		report.ObjectVersionID = statusResult.ObjectVersionID
		return server.coordinator.RecordArtifactUploaded(
			ctx, worker, credentials, uploadID, report,
		)
	case workercontrol.ArtifactUploadStateUploading:
		if statusResult.MultipartUploadID == "" {
			return workercontrol.ArtifactUploadResult{}, errors.New(
				"uploading Artifact has no multipart session",
			)
		}
	default:
		return workercontrol.ArtifactUploadResult{
			Decision: workercontrol.ArtifactUploadConflict,
		}, nil
	}

	upload := artifactstore.MultipartUpload{
		ObjectKey: statusResult.ObjectKey, UploadID: statusResult.MultipartUploadID,
		ContentType: statusResult.ExpectedContentType,
	}
	listedParts, err := server.uploadStore.ListParts(ctx, upload)
	if err != nil {
		return workercontrol.ArtifactUploadResult{}, err
	}
	if !artifactstore.EqualCompletedParts(listedParts, completedParts) {
		return workercontrol.ArtifactUploadResult{
			Decision: workercontrol.ArtifactUploadConflict,
		}, nil
	}
	intent, err := server.coordinator.RecordArtifactCompletionIntent(
		ctx, worker, credentials, uploadID, report,
	)
	if err != nil {
		return workercontrol.ArtifactUploadResult{}, err
	}
	switch intent.Decision {
	case workercontrol.ArtifactCompletionIntentRecorded:
		if intent.UploadID != uploadID || intent.ArtifactID != statusResult.ArtifactID {
			return workercontrol.ArtifactUploadResult{}, errors.New(
				"recorded Artifact completion intent does not match its upload",
			)
		}
	case workercontrol.ArtifactCompletionIntentConflict:
		return workercontrol.ArtifactUploadResult{Decision: workercontrol.ArtifactUploadConflict}, nil
	case workercontrol.ArtifactCompletionIntentRejected:
		return workercontrol.ArtifactUploadResult{Decision: workercontrol.ArtifactUploadRejected}, nil
	default:
		return workercontrol.ArtifactUploadResult{}, errors.New("unknown Artifact completion intent decision")
	}

	objectVersion, completeErr := server.uploadStore.CompleteMultipartUpload(
		ctx, upload, completedParts,
	)
	if completeErr != nil {
		objectVersion, err = server.uploadStore.HeadCurrentVersion(ctx, statusResult.ObjectKey)
		if err != nil {
			return workercontrol.ArtifactUploadResult{}, errors.Join(
				completeErr,
				fmt.Errorf("recover completed Artifact object version: %w", err),
			)
		}
	}
	if objectVersion.ObjectKey != statusResult.ObjectKey || objectVersion.VersionID == "" ||
		objectVersion.SizeBytes != report.SizeBytes ||
		strings.TrimSpace(objectVersion.ContentType) != report.ContentType {
		return workercontrol.ArtifactUploadResult{
			Decision: workercontrol.ArtifactUploadConflict,
		}, nil
	}
	expectedChecksum, err := artifactstore.MultipartCompositeChecksum(completedParts)
	if err != nil {
		return workercontrol.ArtifactUploadResult{}, err
	}
	if objectVersion.ChecksumSHA256 != expectedChecksum {
		return workercontrol.ArtifactUploadResult{
			Decision: workercontrol.ArtifactUploadConflict,
		}, nil
	}
	report.ObjectVersionID = objectVersion.VersionID
	return server.coordinator.RecordArtifactUploaded(
		ctx, worker, credentials, uploadID, report,
	)
}

func (server *Server) abortOrphanMultipart(
	ctx context.Context,
	upload artifactstore.MultipartUpload,
	cause error,
) error {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := server.uploadStore.AbortMultipartUpload(cleanupContext, upload); err != nil {
		return errors.Join(cause, fmt.Errorf("abort orphan multipart upload: %w", err))
	}
	return cause
}

func authenticatedWorker(
	ctx context.Context,
	resolver WorkerIdentityResolver,
) (workercontrol.AuthenticatedWorker, error) {
	connectionPeer, ok := peer.FromContext(ctx)
	if !ok || connectionPeer.AuthInfo == nil {
		return workercontrol.AuthenticatedWorker{}, errors.New("mTLS peer is absent")
	}
	var tlsInfo credentials.TLSInfo
	switch typed := connectionPeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = typed
	case *credentials.TLSInfo:
		if typed == nil {
			return workercontrol.AuthenticatedWorker{}, errors.New("mTLS peer is absent")
		}
		tlsInfo = *typed
	default:
		return workercontrol.AuthenticatedWorker{}, errors.New("peer authentication is not TLS")
	}
	state := tlsInfo.State
	if !state.HandshakeComplete || len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return workercontrol.AuthenticatedWorker{}, errors.New("worker certificate chain is not verified")
	}
	leaf := state.PeerCertificates[0]
	verifiedLeaf := false
	for _, chain := range state.VerifiedChains {
		if len(chain) != 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verifiedLeaf = true
			break
		}
	}
	if !verifiedLeaf || len(leaf.URIs) != 1 || !validSPIFFEID(leaf.URIs[0]) {
		return workercontrol.AuthenticatedWorker{}, errors.New("worker certificate has no unique SPIFFE ID")
	}
	worker, err := resolver.ResolveWorker(ctx, leaf.URIs[0].String())
	if err != nil || worker.ID == uuid.Nil {
		return workercontrol.AuthenticatedWorker{}, errors.New("worker SPIFFE ID is not registered")
	}
	worker.SPIFFEID = leaf.URIs[0].String()
	return worker, nil
}

func workerReadinessWork(
	work fleet.ReadinessWork,
	worker workercontrol.AuthenticatedWorker,
	workerEpoch int64,
) (*velav1.WorkerReadinessWork, error) {
	if !work.Available {
		if work != (fleet.ReadinessWork{}) {
			return nil, errors.New("unavailable Worker readiness work is malformed")
		}
		return &velav1.WorkerReadinessWork{}, nil
	}
	if !validWorkerReadinessWork(work, worker, workerEpoch) {
		return nil, errors.New("authoritative Worker readiness work is invalid")
	}
	check, ok := fleetReadinessCheckToProto(work.Check)
	if !ok {
		return nil, errors.New("authoritative Worker readiness check is invalid")
	}
	deadline := timestamppb.New(work.Deadline.UTC())
	if deadline.CheckValid() != nil {
		return nil, errors.New("authoritative Worker readiness deadline is invalid")
	}
	return &velav1.WorkerReadinessWork{
		Available: true, CycleId: work.CycleID.String(), Check: check,
		WorkerId: work.WorkerID.String(), WorkerPoolId: work.WorkerPoolID.String(),
		WorkerEpoch: work.WorkerEpoch, NodeIdentity: work.NodeIdentity,
		ExecutionProfileRevisionId: work.ExecutionProfileRevisionID.String(),
		InferenceBackendRevision:   work.InferenceBackendRevision, Deadline: deadline,
	}, nil
}

func validWorkerReadinessWork(
	work fleet.ReadinessWork,
	worker workercontrol.AuthenticatedWorker,
	workerEpoch int64,
) bool {
	_, checkOK := fleetReadinessCheckToProto(work.Check)
	return work.Available && work.CycleID != uuid.Nil && checkOK &&
		work.WorkerID == worker.ID && work.WorkerPoolID == worker.PoolID &&
		work.WorkerEpoch == workerEpoch && work.ExecutionProfileRevisionID != uuid.Nil &&
		validObservedBy(work.NodeIdentity) && validReadinessRevision(work.InferenceBackendRevision, 200) &&
		!work.Deadline.IsZero()
}

func workerReadinessResult(
	result fleet.ReadinessResult,
	expectedCycleID uuid.UUID,
) (*velav1.WorkerReadinessResult, error) {
	state, stateOK := fleetReadinessStateToProto(result.State)
	nextCheck, nextOK := fleetReadinessCheckToProto(result.NextCheck)
	workerLifecycle, lifecycleOK := workerLifecycleToProto(result.WorkerLifecycle)
	workerReachability, reachabilityOK := workerReachabilityToProto(result.WorkerReachability)
	if result.CycleID != expectedCycleID || !stateOK || !lifecycleOK || !reachabilityOK ||
		(result.State == fleet.ReadinessChecking && !nextOK) ||
		(result.State != fleet.ReadinessChecking && result.NextCheck != "") {
		return nil, errors.New("authoritative Worker readiness result is invalid")
	}
	return &velav1.WorkerReadinessResult{
		CycleId: result.CycleID.String(), Replayed: result.Replayed, State: state,
		NextCheck: nextCheck, WorkerLifecycle: workerLifecycle,
		WorkerReachability: workerReachability,
	}, nil
}

func fleetReadinessCheckFromProto(
	check velav1.FleetReadinessCheck,
) (fleet.ReadinessCheck, bool) {
	switch check {
	case velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY:
		return fleet.ReadinessIdentity, true
	case velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_DEVICE:
		return fleet.ReadinessDevice, true
	case velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_INFERENCE_BACKEND:
		return fleet.ReadinessInferenceBackend, true
	case velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_MODEL_WARMUP:
		return fleet.ReadinessModelWarmup, true
	case velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_CANARY:
		return fleet.ReadinessCanary, true
	default:
		return "", false
	}
}

func fleetReadinessCheckToProto(
	check fleet.ReadinessCheck,
) (velav1.FleetReadinessCheck, bool) {
	switch check {
	case fleet.ReadinessIdentity:
		return velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY, true
	case fleet.ReadinessDevice:
		return velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_DEVICE, true
	case fleet.ReadinessInferenceBackend:
		return velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_INFERENCE_BACKEND, true
	case fleet.ReadinessModelWarmup:
		return velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_MODEL_WARMUP, true
	case fleet.ReadinessCanary:
		return velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_CANARY, true
	default:
		return velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_UNSPECIFIED, false
	}
}

func fleetReadinessStateToProto(
	state fleet.ReadinessState,
) (velav1.FleetReadinessState, bool) {
	switch state {
	case fleet.ReadinessChecking:
		return velav1.FleetReadinessState_FLEET_READINESS_STATE_CHECKING, true
	case fleet.ReadinessReady:
		return velav1.FleetReadinessState_FLEET_READINESS_STATE_READY, true
	case fleet.ReadinessFailed:
		return velav1.FleetReadinessState_FLEET_READINESS_STATE_FAILED, true
	case fleet.ReadinessExpired:
		return velav1.FleetReadinessState_FLEET_READINESS_STATE_EXPIRED, true
	default:
		return velav1.FleetReadinessState_FLEET_READINESS_STATE_UNSPECIFIED, false
	}
}

func workerLifecycleToProto(value string) (velav1.FleetWorkerLifecycle, bool) {
	switch value {
	case "REGISTERING":
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_REGISTERING, true
	case "WARMING":
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING, true
	case "READY":
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_READY, true
	case "BUSY":
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_BUSY, true
	case "DRAINING":
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_DRAINING, true
	case "RECOVERING":
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_RECOVERING, true
	case "QUARANTINED":
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_QUARANTINED, true
	default:
		return velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_UNSPECIFIED, false
	}
}

func workerReachabilityToProto(value string) (velav1.FleetWorkerReachability, bool) {
	switch value {
	case "HEALTHY":
		return velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_HEALTHY, true
	case "SUSPECT":
		return velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT, true
	case "OFFLINE":
		return velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_OFFLINE, true
	default:
		return velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_UNSPECIFIED, false
	}
}

func validReadinessRevision(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func parseCapacityObservation(
	request *velav1.ReportWorkerCapacityRequest,
) (workercontrol.CapacityObservation, bool) {
	if request == nil {
		return workercontrol.CapacityObservation{}, false
	}
	observedAt := time.Time{}
	if request.GetObservedAt() != nil {
		observedAt = request.GetObservedAt().AsTime()
	}
	watermarkState, watermarkOK := scratchWatermarkStateFromProto(request.GetWatermarkState())
	observation := workercontrol.CapacityObservation{
		WorkerEpoch: request.GetWorkerEpoch(), Sequence: request.GetObservationSequence(),
		ObservedAt: observedAt, WatermarkState: watermarkState,
		TotalBytes: request.GetTotalBytes(), FreeBytes: request.GetFreeBytes(),
		HighWatermarkBytes:     request.GetHighWatermarkBytes(),
		LowWatermarkBytes:      request.GetLowWatermarkBytes(),
		CriticalFreeBytes:      request.GetCriticalFreeBytes(),
		ArtifactStoreReachable: request.GetArtifactStoreReachable(),
	}
	valid := watermarkOK && request.GetObservedAt() != nil && request.GetObservedAt().CheckValid() == nil &&
		observation.WorkerEpoch > 0 && observation.Sequence > 0 &&
		observation.TotalBytes > 0 && observation.FreeBytes >= 0 &&
		observation.FreeBytes <= observation.TotalBytes &&
		observation.HighWatermarkBytes > 0 &&
		observation.HighWatermarkBytes < observation.TotalBytes &&
		observation.LowWatermarkBytes >= 0 &&
		observation.LowWatermarkBytes < observation.HighWatermarkBytes &&
		observation.CriticalFreeBytes >= 0 &&
		observation.CriticalFreeBytes < observation.TotalBytes
	return observation, valid
}

func scratchWatermarkStateFromProto(
	state velav1.FleetScratchWatermarkState,
) (workercontrol.ScratchWatermarkState, bool) {
	switch state {
	case velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_NORMAL:
		return workercontrol.ScratchWatermarkNormal, true
	case velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_PRESSURED:
		return workercontrol.ScratchWatermarkPressured, true
	case velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_CRITICAL:
		return workercontrol.ScratchWatermarkCritical, true
	default:
		return "", false
	}
}

func validObservedBy(value string) bool {
	return value != "" && len(value) <= 500 && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func validSPIFFEID(identity *url.URL) bool {
	return identity != nil && identity.Scheme == "spiffe" && identity.Host != "" &&
		identity.Path != "" && identity.User == nil && identity.RawQuery == "" && identity.Fragment == ""
}

func parseLeaseCredentials(lease *velav1.WorkerLeaseCredentials) (workercontrol.LeaseCredentials, bool) {
	if lease == nil || lease.GetWorkerEpoch() <= 0 || lease.GetFence() <= 0 || lease.GetToken() == "" ||
		len(lease.GetToken()) > 4096 || strings.ContainsRune(lease.GetToken(), '\x00') {
		return workercontrol.LeaseCredentials{}, false
	}
	attemptID, ok := parseRequiredUUID(lease.GetAttemptId())
	if !ok {
		return workercontrol.LeaseCredentials{}, false
	}
	return workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: lease.GetWorkerEpoch(),
		Fence: lease.GetFence(), Token: lease.GetToken(),
	}, true
}

func parseArtifactUploadPartIntent(
	part *velav1.ArtifactUploadPartIntent,
) (*artifactUploadPartIntent, bool) {
	if part == nil {
		return nil, true
	}
	if part.GetNumber() <= 0 || part.GetNumber() > 10_000 || part.GetSizeBytes() <= 0 ||
		len(part.GetSha256()) != sha256.Size {
		return nil, false
	}
	var digest [sha256.Size]byte
	copy(digest[:], part.GetSha256())
	if digest == [sha256.Size]byte{} {
		return nil, false
	}
	return &artifactUploadPartIntent{
		number: part.GetNumber(), sizeBytes: part.GetSizeBytes(), digest: digest,
	}, true
}

func parseMultipartCompletion(
	request *velav1.CompleteArtifactMultipartUploadRequest,
) (workercontrol.ArtifactUploadReport, []artifactstore.CompletedPart, bool) {
	if request == nil || request.GetSizeBytes() <= 0 || len(request.GetSha256()) != sha256.Size ||
		len(request.GetCompletedParts()) == 0 || len(request.GetCompletedParts()) > 10_000 {
		return workercontrol.ArtifactUploadReport{}, nil, false
	}
	contentType := strings.TrimSpace(request.GetContentType())
	if contentType == "" || len(contentType) > 200 || strings.ContainsRune(contentType, '\x00') {
		return workercontrol.ArtifactUploadReport{}, nil, false
	}
	var objectDigest [sha256.Size]byte
	copy(objectDigest[:], request.GetSha256())
	if objectDigest == [sha256.Size]byte{} {
		return workercontrol.ArtifactUploadReport{}, nil, false
	}

	storageParts := make([]artifactstore.CompletedPart, len(request.GetCompletedParts()))
	reportParts := make([]workercontrol.ArtifactUploadPart, len(request.GetCompletedParts()))
	var totalSize int64
	for index, part := range request.GetCompletedParts() {
		if part == nil || part.GetNumber() != int32(index+1) || part.GetSizeBytes() <= 0 ||
			part.GetEtag() == "" || len(part.GetEtag()) > 1000 ||
			strings.ContainsRune(part.GetEtag(), '\x00') ||
			len(part.GetChecksumSha256()) != sha256.Size ||
			totalSize > request.GetSizeBytes()-part.GetSizeBytes() {
			return workercontrol.ArtifactUploadReport{}, nil, false
		}
		var partDigest [sha256.Size]byte
		copy(partDigest[:], part.GetChecksumSha256())
		if partDigest == [sha256.Size]byte{} {
			return workercontrol.ArtifactUploadReport{}, nil, false
		}
		totalSize += part.GetSizeBytes()
		storageParts[index] = artifactstore.CompletedPart{
			Number: part.GetNumber(), ETag: part.GetEtag(), SizeBytes: part.GetSizeBytes(),
			ChecksumSHA256: base64.StdEncoding.EncodeToString(partDigest[:]),
		}
		reportParts[index] = workercontrol.ArtifactUploadPart{
			Number: part.GetNumber(), ETag: part.GetEtag(), SizeBytes: part.GetSizeBytes(),
			ChecksumSHA256: storageParts[index].ChecksumSHA256,
		}
	}
	if totalSize != request.GetSizeBytes() {
		return workercontrol.ArtifactUploadReport{}, nil, false
	}
	return workercontrol.ArtifactUploadReport{
		SizeBytes: request.GetSizeBytes(), SHA256: objectDigest,
		ContentType: contentType, CompletedParts: reportParts,
	}, storageParts, true
}

func parseArtifactUploadReport(report *velav1.ArtifactUploadReport) (workercontrol.ArtifactUploadReport, bool) {
	if report == nil || len(report.GetSha256()) != sha256.Size || len(report.GetCompletedParts()) == 0 ||
		len(report.GetCompletedParts()) > 10_000 {
		return workercontrol.ArtifactUploadReport{}, false
	}
	var digest [sha256.Size]byte
	copy(digest[:], report.GetSha256())
	parts := make([]workercontrol.ArtifactUploadPart, len(report.GetCompletedParts()))
	for index, part := range report.GetCompletedParts() {
		if part == nil {
			return workercontrol.ArtifactUploadReport{}, false
		}
		checksum := ""
		if len(part.GetChecksumSha256()) != 0 {
			if len(part.GetChecksumSha256()) != sha256.Size {
				return workercontrol.ArtifactUploadReport{}, false
			}
			var checksumDigest [sha256.Size]byte
			copy(checksumDigest[:], part.GetChecksumSha256())
			if checksumDigest == [sha256.Size]byte{} {
				return workercontrol.ArtifactUploadReport{}, false
			}
			checksum = base64.StdEncoding.EncodeToString(part.GetChecksumSha256())
		}
		parts[index] = workercontrol.ArtifactUploadPart{
			Number: part.GetNumber(), ETag: part.GetEtag(), SizeBytes: part.GetSizeBytes(),
			ChecksumSHA256: checksum,
		}
	}
	return workercontrol.ArtifactUploadReport{
		ObjectVersionID: report.GetObjectVersionId(), SizeBytes: report.GetSizeBytes(),
		SHA256: digest, ContentType: report.GetContentType(), CompletedParts: parts,
	}, true
}

func parseVisibleCompletionCandidate(
	candidate *velav1.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionCandidate, bool) {
	if candidate == nil || candidate.GetExpectedJobVersion() <= 0 || len(candidate.GetArtifactIds()) == 0 ||
		len(candidate.GetArtifactIds()) > 10_000 {
		return workercontrol.VisibleCompletionCandidate{}, false
	}
	completionID, ok := parseRequiredUUID(candidate.GetCompletionId())
	if !ok {
		return workercontrol.VisibleCompletionCandidate{}, false
	}
	artifactIDs := make([]uuid.UUID, len(candidate.GetArtifactIds()))
	for index, raw := range candidate.GetArtifactIds() {
		artifactID, valid := parseRequiredUUID(raw)
		if !valid {
			return workercontrol.VisibleCompletionCandidate{}, false
		}
		artifactIDs[index] = artifactID
	}
	return workercontrol.VisibleCompletionCandidate{
		CompletionID: completionID, ExpectedJobVersion: candidate.GetExpectedJobVersion(),
		ArtifactIDs: artifactIDs,
	}, true
}

func parseRequiredUUID(raw string) (uuid.UUID, bool) {
	parsed, err := uuid.Parse(raw)
	return parsed, err == nil && parsed != uuid.Nil
}

func invalidResponse(requestID string, message string) *velav1.ConnectResponse {
	return operationErrorResponse(requestID, "INVALID_REQUEST", message)
}

func operationErrorResponse(requestID, code, message string) *velav1.ConnectResponse {
	return &velav1.ConnectResponse{
		RequestId: requestID,
		Result: &velav1.ConnectResponse_OperationError{OperationError: &velav1.WorkerOperationError{
			Code: code, Message: message,
		}},
	}
}

func parseHeartbeatObservation(request *velav1.HeartbeatRequest) (workercontrol.HeartbeatObservation, bool) {
	if request == nil || request.GetSequence() <= 0 || request.GetBackendStage() == "" ||
		len(request.GetGpuHealthJson()) == 0 || len(request.GetGpuHealthJson()) > 16*1024 ||
		len(request.GetLocalArtifactStateJson()) == 0 ||
		len(request.GetLocalArtifactStateJson()) > 16*1024 || request.GetScratchFreeBytes() < 0 {
		return workercontrol.HeartbeatObservation{}, false
	}
	var progress *float64
	if request.BackendStageProgress != nil {
		value := request.GetBackendStageProgress()
		if !executionprogress.ValidStageProgress(value) {
			return workercontrol.HeartbeatObservation{}, false
		}
		progress = &value
	}
	var remaining *int64
	if request.EstimatedRemainingSeconds != nil {
		value := request.GetEstimatedRemainingSeconds()
		if !executionprogress.ValidEstimatedRemainingSeconds(value) {
			return workercontrol.HeartbeatObservation{}, false
		}
		remaining = &value
	}
	return workercontrol.HeartbeatObservation{
		Sequence: request.GetSequence(), BackendStage: request.GetBackendStage(),
		BackendStageProgress: progress, EstimatedRemainingSeconds: remaining,
		GPUHealthSummary:       append([]byte(nil), request.GetGpuHealthJson()...),
		LocalArtifactState:     append([]byte(nil), request.GetLocalArtifactStateJson()...),
		ScratchFreeBytes:       request.GetScratchFreeBytes(),
		ArtifactStoreReachable: request.GetArtifactStoreReachable(),
	}, true
}

func parseFailureObservation(
	observation *velav1.WorkerFailureObservation,
) (workercontrol.FailureObservation, bool) {
	if observation == nil || observation.GetFailureClass() == "" ||
		observation.GetFailureFingerprint() == "" || observation.GetErrorSummary() == "" ||
		observation.GetBackendStage() == "" || observation.GetInferenceBackendRevision() == "" ||
		len(observation.GetGpuUuids()) > 8 {
		return workercontrol.FailureObservation{}, false
	}
	return workercontrol.FailureObservation{
		FailureClass:       observation.GetFailureClass(),
		FailureFingerprint: observation.GetFailureFingerprint(),
		ErrorSummary:       observation.GetErrorSummary(), BackendStage: observation.GetBackendStage(),
		GPUUUIDs:                 append([]string(nil), observation.GetGpuUuids()...),
		InferenceBackendRevision: observation.GetInferenceBackendRevision(),
		RetryRecommended:         observation.GetRetryRecommended(),
		WorkerReusable:           observation.GetWorkerReusable(),
	}, true
}

func workerAssignment(result workercontrol.Assignment) *velav1.WorkerAssignment {
	message := &velav1.WorkerAssignment{
		AttemptId: result.AttemptID.String(), JobId: result.JobID.String(),
		WorkerId: result.WorkerID.String(), WorkerEpoch: result.WorkerEpoch,
		ModelRevisionId:            result.ModelRevisionID.String(),
		GenerationPresetRevisionId: result.GenerationPresetRevisionID.String(),
		ExecutionProfileRevisionId: result.ExecutionProfileRevisionID.String(),
		OutputSpecId:               result.OutputSpecID.String(),
		RequestContentJson:         []byte(result.RequestContent), AttemptNumber: result.AttemptNumber,
		LeaseToken: result.LeaseToken, LeaseFence: result.LeaseFence,
		LeaseExpiresAt: timestamp(result.LeaseExpiresAt),
		LeaseValidFor:  durationpb.New(result.LeaseValidFor),
	}
	if result.DebugDumpAuthorization != nil {
		message.DebugDumpAuthorization = &velav1.DebugDumpAuthorizationSnapshot{
			AuthorizationId: result.DebugDumpAuthorization.AuthorizationID.String(),
			ExpiresAt:       timestamp(result.DebugDumpAuthorization.ExpiresAt),
		}
	}
	return message
}

func startWorkerResult(result workercontrol.StartResult) *velav1.StartWorkerResult {
	return &velav1.StartWorkerResult{
		Decision: string(result.Decision), StopReason: string(result.StopReason),
		AttemptId: optionalUUID(result.AttemptID), JobId: optionalUUID(result.JobID),
		WorkerId: optionalUUID(result.WorkerID), WorkerEpoch: result.WorkerEpoch,
		LeaseFence: result.LeaseFence, StartedAt: timestamp(result.StartedAt),
	}
}

func heartbeatResult(result workercontrol.HeartbeatResult) *velav1.HeartbeatResult {
	return &velav1.HeartbeatResult{
		Decision: string(result.Decision), StopReason: string(result.StopReason),
		AttemptId: optionalUUID(result.AttemptID), JobId: optionalUUID(result.JobID),
		WorkerId: optionalUUID(result.WorkerID), WorkerEpoch: result.WorkerEpoch,
		LeaseFence: result.LeaseFence, HeartbeatSequence: result.HeartbeatSequence,
		ExecutionPhase:    string(result.ExecutionPhase),
		ProgressUpdatedAt: timestamp(result.ProgressUpdatedAt),
		LeaseExpiresAt:    timestamp(result.LeaseExpiresAt),
		LeaseValidFor:     durationpb.New(result.LeaseValidFor),
	}
}

func retryDecision(result workercontrol.RetryDecision) *velav1.RetryDecision {
	return &velav1.RetryDecision{
		Disposition: string(result.Disposition), FailureClass: result.FailureClass,
		AttemptId: optionalUUID(result.AttemptID), JobId: optionalUUID(result.JobID),
		AttemptState:               string(result.AttemptState),
		AttemptComputeSeconds:      result.AttemptComputeSeconds,
		TotalComputeSeconds:        result.TotalComputeSeconds,
		AttemptFinalizationSeconds: result.AttemptFinalizationSeconds,
		TotalFinalizationSeconds:   result.TotalFinalizationSeconds,
		NextRetryAt:                timestampPointer(result.NextRetryAt), JobFence: result.JobFence,
		JobVersion: result.JobVersion, DecidedAt: timestamp(result.DecidedAt),
	}
}

func finalizationPlan(result workercontrol.FinalizationPlan) *velav1.FinalizationPlan {
	artifacts := make([]*velav1.PlannedArtifact, len(result.Artifacts))
	for index, artifact := range result.Artifacts {
		artifacts[index] = &velav1.PlannedArtifact{
			ArtifactId: artifact.ArtifactID.String(), UploadId: artifact.UploadID.String(),
			Kind: string(artifact.Kind), Ordinal: artifact.Ordinal, ObjectKey: artifact.ObjectKey,
			ExpiresAt: timestamp(artifact.ExpiresAt),
		}
	}
	return &velav1.FinalizationPlan{
		Decision: string(result.Decision), AttemptId: optionalUUID(result.AttemptID),
		JobId: optionalUUID(result.JobID), JobVersion: result.JobVersion,
		FinalizationStartedAt:  timestamp(result.FinalizationStartedAt),
		FinalizationDeadlineAt: timestamp(result.FinalizationDeadlineAt), Artifacts: artifacts,
	}
}

type signedArtifactUploadPart struct {
	intent artifactUploadPartIntent
	signed artifactstore.SignedUploadPart
}

func artifactUploadClaim(
	result workercontrol.ArtifactUploadClaim,
	completed []artifactstore.CompletedPart,
	signed *signedArtifactUploadPart,
	partAlreadyUploaded bool,
) (*velav1.ArtifactUploadClaim, error) {
	completedParts, err := artifactUploadPartReports(completed)
	if err != nil {
		return nil, err
	}
	claim := &velav1.ArtifactUploadClaim{
		Decision: string(result.Decision), ClaimId: optionalUUID(result.ClaimID),
		UploadId: optionalUUID(result.UploadID), ArtifactId: optionalUUID(result.ArtifactID),
		ObjectKey: result.ObjectKey, ExpectedContentType: result.ExpectedContentType,
		ClaimExpiresAt: timestamp(result.ClaimExpiresAt), Version: result.Version,
		MultipartUploadId: result.MultipartUploadID, CompletedParts: completedParts,
		UploadExpiresAt:     timestamp(result.UploadExpiresAt),
		PartAlreadyUploaded: partAlreadyUploaded,
	}
	if signed == nil {
		return claim, nil
	}
	if signed.signed.URL == "" || signed.signed.Method != http.MethodPut ||
		!signed.signed.ExpiresAt.After(signed.signed.IssuedAt) {
		return nil, errors.New("signed Artifact upload part is incomplete")
	}
	requiredHeaders, err := transferableSignedUploadHeaders(signed.signed)
	if err != nil {
		return nil, fmt.Errorf("validate signed Artifact upload part headers: %w", err)
	}
	claim.UploadPart = &velav1.SignedArtifactUploadPart{
		Number: signed.intent.number, SizeBytes: signed.intent.sizeBytes,
		Sha256: append([]byte(nil), signed.intent.digest[:]...), Url: signed.signed.URL,
		RequiredHeaders: requiredHeaders, ExpiresAt: timestamp(signed.signed.ExpiresAt),
	}
	return claim, nil
}

func transferableSignedUploadHeaders(
	signed artifactstore.SignedUploadPart,
) (map[string]string, error) {
	parsed, err := url.Parse(signed.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("signed upload URL is invalid")
	}
	required := make(map[string]string, len(signed.Headers))
	for name, values := range signed.Headers {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" || len(values) == 0 {
			return nil, errors.New("signed upload part has invalid headers")
		}
		value := strings.Join(values, ",")
		if value == "" || strings.ContainsRune(value, '\x00') {
			return nil, errors.New("signed upload part has invalid headers")
		}
		if canonical == "Host" {
			if len(values) != 1 || value != parsed.Host {
				return nil, errors.New("signed upload Host does not match its URL")
			}
			continue
		}
		if _, duplicate := required[canonical]; duplicate {
			return nil, errors.New("signed upload part has duplicate headers")
		}
		required[canonical] = value
	}
	if len(required) == 0 {
		return nil, errors.New("signed upload part has no transferable headers")
	}
	return required, nil
}

func artifactUploadPartReports(
	parts []artifactstore.CompletedPart,
) ([]*velav1.ArtifactUploadPartReport, error) {
	if len(parts) > 10_000 {
		return nil, errors.New("too many completed Artifact upload parts")
	}
	reports := make([]*velav1.ArtifactUploadPartReport, len(parts))
	var previous int32
	for index, part := range parts {
		checksum, err := base64.StdEncoding.DecodeString(part.ChecksumSHA256)
		if err != nil || len(checksum) != sha256.Size || part.Number <= previous ||
			part.Number > 10_000 || part.ETag == "" || len(part.ETag) > 1000 ||
			strings.ContainsRune(part.ETag, '\x00') || part.SizeBytes <= 0 {
			return nil, errors.New("completed Artifact upload part is invalid")
		}
		previous = part.Number
		reports[index] = &velav1.ArtifactUploadPartReport{
			Number: part.Number, Etag: part.ETag, SizeBytes: part.SizeBytes,
			ChecksumSha256: checksum,
		}
	}
	return reports, nil
}

func artifactMultipartSession(
	result workercontrol.ArtifactMultipartSession,
) *velav1.ArtifactMultipartSession {
	return &velav1.ArtifactMultipartSession{
		Decision: string(result.Decision), UploadId: optionalUUID(result.UploadID),
		ArtifactId: optionalUUID(result.ArtifactID), MultipartUploadId: result.MultipartUploadID,
		Version: result.Version,
	}
}

func artifactUploadResult(result workercontrol.ArtifactUploadResult) *velav1.ArtifactUploadResult {
	return &velav1.ArtifactUploadResult{
		Decision: string(result.Decision), UploadId: optionalUUID(result.UploadID),
		ArtifactId: optionalUUID(result.ArtifactID), ObjectVersionId: result.ObjectVersionID,
		Version: result.Version,
	}
}

func artifactVerificationResult(
	result workercontrol.ArtifactVerificationResult,
) *velav1.ArtifactVerificationResult {
	return &velav1.ArtifactVerificationResult{
		Decision: string(result.Decision), VerificationId: optionalUUID(result.VerificationID),
		UploadId: optionalUUID(result.UploadID), ArtifactId: optionalUUID(result.ArtifactID),
		ObjectVersionId: result.ObjectVersionID, Version: result.Version,
		VerifiedAt: timestamp(result.VerifiedAt),
	}
}

func visibleCompletionResult(
	result workercontrol.VisibleCompletionResult,
) *velav1.VisibleCompletionResult {
	artifacts := make([]*velav1.WorkerCommittedArtifact, len(result.Artifacts))
	for index, artifact := range result.Artifacts {
		artifacts[index] = &velav1.WorkerCommittedArtifact{
			ArtifactId: artifact.ArtifactID.String(), Kind: string(artifact.Kind), Ordinal: artifact.Ordinal,
			ObjectKey: artifact.ObjectKey, ObjectVersionId: artifact.ObjectVersionID,
			SizeBytes: artifact.SizeBytes, Sha256: append([]byte(nil), artifact.SHA256[:]...),
			ContentType: artifact.ContentType,
		}
	}
	return &velav1.VisibleCompletionResult{
		Decision: string(result.Decision), CompletionId: optionalUUID(result.CompletionID),
		JobId: optionalUUID(result.JobID), AttemptId: optionalUUID(result.AttemptID),
		ArtifactSetId: optionalUUID(result.ArtifactSetID), ChargeId: optionalUUID(result.ChargeID),
		JobVersion: result.JobVersion, ManifestSha256: optionalDigest(result.ManifestSHA256),
		Artifacts: artifacts, CompletedAt: timestamp(result.CompletedAt),
	}
}

func optionalUUID(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func optionalDigest(digest [sha256.Size]byte) []byte {
	if digest == [sha256.Size]byte{} {
		return nil
	}
	return append([]byte(nil), digest[:]...)
}

func timestamp(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func timestampPointer(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamp(*value)
}

var _ velav1.WorkerControlServiceServer = (*Server)(nil)
