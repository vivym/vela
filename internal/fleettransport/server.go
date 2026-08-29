package fleettransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maximumActorBytes                 = 500
	maximumIdentityBytes              = 500
	maximumReasonBytes                = 1000
	maximumRequestUIDBytes            = 200
	maximumWorkerPodUIDBytes          = 128
	maximumNameBytes                  = 253
	maximumDrainOperations            = 4096
	maximumDeadline                   = 24 * time.Hour
	maximumWorkerRegistryPayloadBytes = 1 << 20
)

type workerRegistryService interface {
	Apply(context.Context, fleet.ApprovedResidencyPlan) (fleet.ActuationPlan, error)
	Observe(context.Context, fleet.WorkerInstanceEvidence) (fleet.WorkerInstanceDecision, error)
}

type Service interface {
	ResolveWorkerIdentity(
		context.Context,
		fleet.WorkerIdentityRequest,
	) (fleet.WorkerIdentity, error)
	ConfigureCapacityPolicy(
		context.Context,
		fleet.CapacityPolicy,
	) (fleet.CapacityPolicyResult, error)
	ObserveCapacity(context.Context, fleet.CapacityObservation) (fleet.CapacityResult, error)
	BeginReadiness(context.Context, fleet.ReadinessRequest) (fleet.ReadinessResult, error)
	ReportReadiness(context.Context, fleet.ReadinessEvidence) (fleet.ReadinessResult, error)
	GetReadiness(context.Context, uuid.UUID) (fleet.ReadinessResult, error)
	RequestDrain(context.Context, fleet.DrainRequest) (fleet.DrainResult, error)
	ReconcileDrain(context.Context, uuid.UUID, string) (fleet.DrainResult, error)
	GetDrain(context.Context, uuid.UUID) (fleet.DrainResult, error)
	AuthorizeMutation(
		context.Context,
		fleet.MutationAuthorizationRequest,
	) (fleet.MutationAuthorizationResult, error)
	HasRetirementAuthorization(
		context.Context,
		fleet.RetirementAuthorizationRequest,
	) (bool, error)
	RecordRetirementCompletion(
		context.Context,
		fleet.RetirementCompletionRequest,
	) (fleet.RetirementCompletionResult, error)
	HasRetirementCompletion(
		context.Context,
		fleet.RetirementAuthorizationRequest,
	) (bool, error)
}

func (server *Server) ApplyResidencyPlan(
	ctx context.Context,
	request *velav1.ApplyResidencyPlanRequest,
) (*velav1.ApplyResidencyPlanResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	registry, ok := server.service.(workerRegistryService)
	if !ok || request == nil {
		return nil, invalidRequest("ResidencyPlan apply")
	}
	var plan fleet.ApprovedResidencyPlan
	if !decodeWorkerRegistryPayload(request.GetApprovedPlanJson(), &plan) {
		return nil, invalidRequest("ResidencyPlan apply")
	}
	result, err := registry.Apply(ctx, plan)
	if err != nil {
		return nil, mapServiceError("apply approved ResidencyPlan", err)
	}
	if result.PlanRevisionID != plan.ID || result.WorkerInstanceCount <= 0 {
		return nil, invalidAuthoritativeResult("ResidencyPlan apply")
	}
	return &velav1.ApplyResidencyPlanResponse{
		PlanRevisionId:      result.PlanRevisionID.String(),
		WorkerInstanceCount: int32(result.WorkerInstanceCount),
	}, nil
}

func (server *Server) ObserveWorkerInstance(
	ctx context.Context,
	request *velav1.ObserveWorkerInstanceRequest,
) (*velav1.ObserveWorkerInstanceResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	registry, ok := server.service.(workerRegistryService)
	if !ok || request == nil {
		return nil, invalidRequest("WorkerInstance observation")
	}
	var evidence fleet.WorkerInstanceEvidence
	if !decodeWorkerRegistryPayload(request.GetEvidenceJson(), &evidence) {
		return nil, invalidRequest("WorkerInstance observation")
	}
	evidence.ObservedBy = server.actorIdentity
	decision, err := registry.Observe(ctx, evidence)
	if err != nil {
		return nil, mapServiceError("observe WorkerInstance", err)
	}
	if decision.WorkerInstanceID != evidence.WorkerInstanceID || decision.InstanceEpoch <= 0 ||
		decision.ControlSessionEpoch <= 0 || decision.ModelRuntimeEpoch <= 0 {
		return nil, invalidAuthoritativeResult("WorkerInstance observation")
	}
	return &velav1.ObserveWorkerInstanceResponse{
		WorkerInstanceId:    decision.WorkerInstanceID.String(),
		InstanceEpoch:       decision.InstanceEpoch,
		ControlSessionEpoch: decision.ControlSessionEpoch,
		ModelRuntimeEpoch:   decision.ModelRuntimeEpoch,
		Readiness:           string(decision.Readiness),
	}, nil
}

func decodeWorkerRegistryPayload(encoded []byte, target any) bool {
	if len(encoded) == 0 || len(encoded) > maximumWorkerRegistryPayloadBytes || target == nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}

func (server *Server) ConfigureCapacityPolicy(
	ctx context.Context,
	request *velav1.ConfigureCapacityPolicyRequest,
) (*velav1.ConfigureCapacityPolicyResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("capacity policy")
	}
	workerPoolID, poolErr := parseUUID(request.GetWorkerPoolId())
	policy := fleet.CapacityPolicy{
		WorkerPoolID: workerPoolID, Revision: request.GetRevision(),
		WorkerHighWatermarkBytes: request.GetWorkerHighWatermarkBytes(),
		WorkerLowWatermarkBytes:  request.GetWorkerLowWatermarkBytes(),
		WorkerCriticalFreeBytes:  request.GetWorkerCriticalFreeBytes(),
		PoolHighWatermarkBytes:   request.GetPoolHighWatermarkBytes(),
		PoolLowWatermarkBytes:    request.GetPoolLowWatermarkBytes(),
		ObservationMaxAge:        time.Duration(request.GetObservationMaxAgeSeconds()) * time.Second,
		ConfiguredBy:             server.actorIdentity,
	}
	if poolErr != nil || len(policy.Revision) != 64 ||
		strings.Trim(policy.Revision, "0123456789abcdef") != "" ||
		policy.WorkerHighWatermarkBytes <= 0 || policy.WorkerLowWatermarkBytes < 0 ||
		policy.WorkerLowWatermarkBytes >= policy.WorkerHighWatermarkBytes ||
		policy.WorkerCriticalFreeBytes < 0 || policy.PoolHighWatermarkBytes <= 0 ||
		policy.PoolLowWatermarkBytes < 0 ||
		policy.PoolLowWatermarkBytes >= policy.PoolHighWatermarkBytes ||
		policy.ObservationMaxAge < 10*time.Second || policy.ObservationMaxAge > 10*time.Minute {
		return nil, invalidRequest("capacity policy")
	}
	result, err := server.service.ConfigureCapacityPolicy(ctx, policy)
	if err != nil {
		return nil, mapServiceError("configure Fleet capacity policy", err)
	}
	return &velav1.ConfigureCapacityPolicyResponse{
		WorkerPoolId: result.WorkerPoolID.String(), Revision: result.Revision,
		Replayed: result.Replayed,
	}, nil
}

func (server *Server) ResolveWorkerIdentity(
	ctx context.Context,
	request *velav1.ResolveWorkerIdentityRequest,
) (*velav1.ResolveWorkerIdentityResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("Worker identity resolution")
	}
	workerPoolID, err := parseUUID(request.GetWorkerPoolId())
	parsed := fleet.WorkerIdentityRequest{
		NodeIdentity: request.GetNodeIdentity(), WorkerPoolID: workerPoolID,
		KubernetesUID: request.GetKubernetesUid(), Namespace: request.GetNamespace(),
		Name: request.GetName(),
	}
	if err != nil || !validText(parsed.NodeIdentity, maximumIdentityBytes) ||
		!validText(parsed.KubernetesUID, maximumWorkerPodUIDBytes) ||
		!validText(parsed.Namespace, maximumNameBytes) ||
		!validText(parsed.Name, maximumNameBytes) {
		return nil, invalidRequest("Worker identity resolution")
	}
	identity, err := server.service.ResolveWorkerIdentity(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("resolve Fleet Worker identity", err)
	}
	return &velav1.ResolveWorkerIdentityResponse{
		WorkerId: identity.WorkerID.String(), WorkerPoolId: identity.WorkerPoolID.String(),
		WorkerEpoch: identity.WorkerEpoch, NodeIdentity: identity.NodeIdentity,
	}, nil
}

type Config struct {
	SPIFFEIdentity string
	ActorIdentity  string
}

type Server struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	service        Service
	spiffeIdentity string
	actorIdentity  string
	clock          func() time.Time
}

func NewServer(service Service, config Config) (*Server, error) {
	if service == nil {
		return nil, errors.New("fleet maintenance service is required")
	}
	if !validSPIFFEIdentity(config.SPIFFEIdentity) {
		return nil, errors.New("fleet Controller SPIFFE identity is invalid")
	}
	if !validText(config.ActorIdentity, maximumActorBytes) {
		return nil, errors.New("fleet Controller actor identity is invalid")
	}
	return &Server{
		service: service, spiffeIdentity: config.SPIFFEIdentity,
		actorIdentity: config.ActorIdentity, clock: time.Now,
	}, nil
}

func (server *Server) ObserveCapacity(
	ctx context.Context,
	request *velav1.ObserveCapacityRequest,
) (*velav1.ObserveCapacityResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("capacity observation")
	}
	workerID, workerErr := parseUUID(request.GetWorkerId())
	workerPoolID, poolErr := parseUUID(request.GetWorkerPoolId())
	observedAt := time.Time{}
	if request.GetObservedAt() != nil && request.GetObservedAt().IsValid() {
		observedAt = request.GetObservedAt().AsTime().UTC()
	}
	watermarkState, watermarkOK := scratchWatermarkStateFromProto(request.GetWatermarkState())
	observation := fleet.CapacityObservation{
		WorkerID: workerID, WorkerPoolID: workerPoolID,
		WorkerEpoch: request.GetWorkerEpoch(), Sequence: request.GetObservationSequence(),
		ObservedAt: observedAt, WatermarkState: watermarkState,
		TotalBytes: request.GetTotalBytes(), FreeBytes: request.GetFreeBytes(),
		HighWatermarkBytes:     request.GetHighWatermarkBytes(),
		LowWatermarkBytes:      request.GetLowWatermarkBytes(),
		CriticalFreeBytes:      request.GetCriticalFreeBytes(),
		ArtifactStoreReachable: request.GetArtifactStoreReachable(),
		ObservedBy:             server.actorIdentity,
	}
	if workerErr != nil || poolErr != nil || !watermarkOK || observation.ObservedAt.IsZero() ||
		observation.WorkerEpoch <= 0 ||
		observation.Sequence <= 0 || observation.TotalBytes <= 0 ||
		observation.FreeBytes < 0 || observation.FreeBytes > observation.TotalBytes ||
		observation.HighWatermarkBytes <= 0 ||
		observation.HighWatermarkBytes >= observation.TotalBytes ||
		observation.LowWatermarkBytes < 0 ||
		observation.LowWatermarkBytes >= observation.HighWatermarkBytes ||
		observation.CriticalFreeBytes < 0 ||
		observation.CriticalFreeBytes >= observation.TotalBytes {
		return nil, invalidRequest("capacity observation")
	}
	result, err := server.service.ObserveCapacity(ctx, observation)
	if err != nil {
		return nil, mapServiceError("record Fleet capacity observation", err)
	}
	workerState, workerStateOK := capacityState(result.WorkerState)
	poolState, poolStateOK := capacityState(result.PoolState)
	if !workerStateOK || !poolStateOK {
		return nil, invalidAuthoritativeResult("Fleet capacity")
	}
	return &velav1.ObserveCapacityResponse{
		WorkerPoolId: result.WorkerPoolID.String(), Replayed: result.Replayed,
		WorkerState: workerState, PoolState: poolState,
		WorkerAssignmentAllowed: result.WorkerAssignmentAllowed,
		PoolReadinessAllowed:    result.PoolReadinessAllowed,
		PoolAssignmentAllowed:   result.PoolAssignmentAllowed,
	}, nil
}

func scratchWatermarkStateFromProto(
	state velav1.FleetScratchWatermarkState,
) (fleet.ScratchWatermarkState, bool) {
	switch state {
	case velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_NORMAL:
		return fleet.ScratchWatermarkNormal, true
	case velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_PRESSURED:
		return fleet.ScratchWatermarkPressured, true
	case velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_CRITICAL:
		return fleet.ScratchWatermarkCritical, true
	default:
		return "", false
	}
}

func (server *Server) BeginReadiness(
	ctx context.Context,
	request *velav1.BeginReadinessRequest,
) (*velav1.BeginReadinessResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("readiness begin")
	}
	cycleID, cycleErr := parseUUID(request.GetCycleId())
	workerID, workerErr := parseUUID(request.GetWorkerId())
	workerPoolID, poolErr := parseUUID(request.GetWorkerPoolId())
	profileID, profileErr := parseUUID(request.GetExecutionProfileRevisionId())
	deadline, deadlineErr := boundedDeadline(request.GetDeadline(), server.clock().UTC())
	parsed := fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: workerPoolID,
		WorkerEpoch: request.GetWorkerEpoch(), NodeIdentity: request.GetNodeIdentity(),
		ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision:   request.GetInferenceBackendRevision(),
		RequestedBy:                server.actorIdentity, Deadline: deadline,
	}
	if cycleErr != nil || workerErr != nil || poolErr != nil || profileErr != nil ||
		deadlineErr != nil || parsed.WorkerEpoch <= 0 ||
		!validText(parsed.NodeIdentity, maximumIdentityBytes) ||
		!validText(parsed.InferenceBackendRevision, 200) {
		return nil, invalidRequest("readiness begin")
	}
	result, err := server.service.BeginReadiness(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("begin Worker readiness", err)
	}
	response, err := readinessResult(result)
	if err != nil {
		return nil, err
	}
	return &velav1.BeginReadinessResponse{Result: response}, nil
}

func (server *Server) ReportReadiness(
	ctx context.Context,
	request *velav1.ReportReadinessRequest,
) (*velav1.ReportReadinessResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("readiness evidence")
	}
	cycleID, cycleErr := parseUUID(request.GetCycleId())
	check, checkOK := readinessCheckFromProto(request.GetCheck())
	parsed := fleet.ReadinessEvidence{
		CycleID: cycleID, Check: check, Passed: request.GetPassed(),
		EvidenceDigest: append([]byte(nil), request.GetEvidenceDigest()...),
		ObservedBy:     server.actorIdentity,
	}
	if cycleErr != nil || !checkOK || len(parsed.EvidenceDigest) != 32 {
		return nil, invalidRequest("readiness evidence")
	}
	result, err := server.service.ReportReadiness(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("report Worker readiness", err)
	}
	response, err := readinessResult(result)
	if err != nil {
		return nil, err
	}
	return &velav1.ReportReadinessResponse{Result: response}, nil
}

func (server *Server) GetReadiness(
	ctx context.Context,
	request *velav1.GetReadinessRequest,
) (*velav1.GetReadinessResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("readiness lookup")
	}
	cycleID, err := parseUUID(request.GetCycleId())
	if err != nil {
		return nil, invalidRequest("readiness lookup")
	}
	result, err := server.service.GetReadiness(ctx, cycleID)
	if err != nil {
		return nil, mapServiceError("get Worker readiness", err)
	}
	response, err := readinessResult(result)
	if err != nil {
		return nil, err
	}
	return &velav1.GetReadinessResponse{Result: response}, nil
}

func (server *Server) RequestDrain(
	ctx context.Context,
	request *velav1.RequestDrainRequest,
) (*velav1.RequestDrainResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("drain request")
	}
	operationID, operationErr := parseUUID(request.GetOperationId())
	workerID, workerErr := parseUUID(request.GetWorkerId())
	now := server.clock().UTC()
	deadline, deadlineErr := boundedDeadline(request.GetDeadline(), now)
	if operationErr == nil && workerErr == nil && deadlineErr != nil {
		absolute, absoluteErr := absoluteDeadline(request.GetDeadline())
		if absoluteErr == nil && !absolute.After(now) {
			existing, err := server.service.GetDrain(ctx, operationID)
			if err != nil {
				return nil, mapServiceError("get Worker drain for request replay", err)
			}
			if existing.OperationID == operationID && existing.WorkerID == workerID &&
				existing.WorkerEpoch == request.GetExpectedEpoch() &&
				existing.Deadline.Equal(absolute) {
				deadline = absolute
				deadlineErr = nil
			}
		}
	}
	parsed := fleet.DrainRequest{
		OperationID: operationID, WorkerID: workerID,
		ExpectedEpoch: request.GetExpectedEpoch(), Reason: request.GetReason(),
		Deadline: deadline, RequestedBy: server.actorIdentity,
	}
	if operationErr != nil || workerErr != nil || deadlineErr != nil ||
		parsed.ExpectedEpoch <= 0 || !validText(parsed.Reason, maximumReasonBytes) {
		return nil, invalidRequest("drain request")
	}
	result, err := server.service.RequestDrain(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("request Worker drain", err)
	}
	response, err := drainResult(result)
	if err != nil {
		return nil, err
	}
	return &velav1.RequestDrainResponse{Result: response}, nil
}

func (server *Server) ReconcileDrain(
	ctx context.Context,
	request *velav1.ReconcileDrainRequest,
) (*velav1.ReconcileDrainResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("drain reconciliation")
	}
	operationID, err := parseUUID(request.GetOperationId())
	if err != nil {
		return nil, invalidRequest("drain reconciliation")
	}
	result, err := server.service.ReconcileDrain(ctx, operationID, server.actorIdentity)
	if err != nil {
		return nil, mapServiceError("reconcile Worker drain", err)
	}
	response, err := drainResult(result)
	if err != nil {
		return nil, err
	}
	return &velav1.ReconcileDrainResponse{Result: response}, nil
}

func (server *Server) GetDrain(
	ctx context.Context,
	request *velav1.GetDrainRequest,
) (*velav1.GetDrainResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("drain lookup")
	}
	operationID, err := parseUUID(request.GetOperationId())
	if err != nil {
		return nil, invalidRequest("drain lookup")
	}
	result, err := server.service.GetDrain(ctx, operationID)
	if err != nil {
		return nil, mapServiceError("get Worker drain", err)
	}
	response, err := drainResult(result)
	if err != nil {
		return nil, err
	}
	return &velav1.GetDrainResponse{Result: response}, nil
}

func (server *Server) AuthorizeMutation(
	ctx context.Context,
	request *velav1.AuthorizeMutationRequest,
) (*velav1.AuthorizeMutationResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil || !validText(request.GetRequestUid(), maximumRequestUIDBytes) ||
		!validText(request.GetKubernetesUid(), maximumRequestUIDBytes) ||
		!validText(request.GetNamespace(), maximumNameBytes) ||
		!validText(request.GetName(), maximumNameBytes) ||
		len(request.GetDrainOperationIds()) > maximumDrainOperations ||
		len(request.GetRequestDigest()) != 32 {
		return nil, invalidRequest("mutation authorization")
	}
	resourceKind, kindOK := protectedResourceKindFromProto(request.GetResourceKind())
	operation, operationOK := mutationOperationFromProto(request.GetOperation())
	workerPoolID, poolErr := parseOptionalUUID(request.GetWorkerPoolId())
	workerID, workerErr := parseOptionalUUID(request.GetWorkerId())
	workerInstanceID, workerInstanceErr := parseOptionalUUID(request.GetWorkerInstanceId())
	planID, planErr := parseOptionalUUID(request.GetResidencyPlanRevisionId())
	bundleID, bundleErr := parseOptionalUUID(request.GetWorkerBundleId())
	memberID, memberErr := parseOptionalUUID(request.GetWorkerMemberId())
	drainIDs, drainErr := parseDistinctUUIDs(request.GetDrainOperationIds())
	parsed := fleet.MutationAuthorizationRequest{
		RequestUID: request.GetRequestUid(), ActorIdentity: server.actorIdentity,
		ResourceKind: resourceKind, Operation: operation,
		KubernetesUID: request.GetKubernetesUid(), Namespace: request.GetNamespace(),
		Name: request.GetName(), WorkerPoolID: workerPoolID, WorkerID: workerID,
		WorkerEpoch: request.GetWorkerEpoch(), DrainOperationIDs: drainIDs,
		WorkerInstanceID: workerInstanceID, WorkerInstanceEpoch: request.GetWorkerInstanceEpoch(),
		ResidencyPlanRevisionID: planID, WorkerBundleID: bundleID, WorkerMemberID: memberID,
		RequestDigest: append([]byte(nil), request.GetRequestDigest()...),
	}
	if !kindOK || !operationOK || poolErr != nil || workerErr != nil ||
		workerInstanceErr != nil || planErr != nil || bundleErr != nil || memberErr != nil ||
		drainErr != nil || validateMutationTransportShape(parsed) != nil {
		return nil, invalidRequest("mutation authorization")
	}
	result, err := server.service.AuthorizeMutation(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("authorize protected Fleet mutation", err)
	}
	return &velav1.AuthorizeMutationResponse{
		RequestUid: result.RequestUID, Replayed: result.Replayed, Authorized: result.Authorized,
	}, nil
}

func validateMutationTransportShape(request fleet.MutationAuthorizationRequest) error {
	workerInstanceMutation := request.WorkerInstanceID != uuid.Nil || request.WorkerInstanceEpoch != 0 ||
		request.ResidencyPlanRevisionID != uuid.Nil || request.WorkerBundleID != uuid.Nil ||
		request.WorkerMemberID != uuid.Nil
	if workerInstanceMutation {
		if request.ResourceKind != fleet.ProtectedPod || request.WorkerInstanceID == uuid.Nil ||
			request.WorkerInstanceEpoch <= 0 || request.ResidencyPlanRevisionID == uuid.Nil ||
			request.WorkerBundleID == uuid.Nil || request.WorkerMemberID == uuid.Nil ||
			request.WorkerPoolID != uuid.Nil || request.WorkerID != uuid.Nil || request.WorkerEpoch != 0 ||
			len(request.DrainOperationIDs) != 0 {
			return errors.New("WorkerInstance mutation transport shape is invalid")
		}
		return nil
	}
	if request.WorkerPoolID == uuid.Nil || len(request.DrainOperationIDs) == 0 {
		return errors.New("legacy mutation transport shape is invalid")
	}
	if request.ResourceKind == fleet.ProtectedPod {
		if request.WorkerID == uuid.Nil || request.WorkerEpoch <= 0 || len(request.DrainOperationIDs) != 1 {
			return errors.New("legacy Pod mutation transport shape is invalid")
		}
		return nil
	}
	if request.WorkerID != uuid.Nil || request.WorkerEpoch != 0 {
		return errors.New("legacy pool mutation transport shape is invalid")
	}
	return nil
}

func (server *Server) HasRetirementAuthorization(
	ctx context.Context,
	request *velav1.HasRetirementAuthorizationRequest,
) (*velav1.HasRetirementAuthorizationResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("retirement authorization lookup")
	}
	parsed, ok := parseRetirementResource(
		request.GetResourceKind(), request.GetKubernetesUid(), request.GetNamespace(),
		request.GetName(), request.GetWorkerPoolId(), request.GetWorkerId(),
		request.GetWorkerEpoch(), request.GetDrainOperationIds(),
	)
	if !ok {
		return nil, invalidRequest("retirement authorization lookup")
	}
	authorized, err := server.service.HasRetirementAuthorization(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("check protected Fleet retirement authorization", err)
	}
	return &velav1.HasRetirementAuthorizationResponse{Authorized: authorized}, nil
}

func (server *Server) RecordRetirementCompletion(
	ctx context.Context,
	request *velav1.RecordRetirementCompletionRequest,
) (*velav1.RecordRetirementCompletionResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("retirement completion")
	}
	resource, ok := parseRetirementResource(
		request.GetResourceKind(), request.GetKubernetesUid(), request.GetNamespace(),
		request.GetName(), request.GetWorkerPoolId(), request.GetWorkerId(),
		request.GetWorkerEpoch(), request.GetDrainOperationIds(),
	)
	if !ok {
		return nil, invalidRequest("retirement completion")
	}
	result, err := server.service.RecordRetirementCompletion(ctx, fleet.RetirementCompletionRequest{
		RetirementAuthorizationRequest: resource,
		ObservedBy:                     server.actorIdentity,
	})
	if err != nil {
		return nil, mapServiceError("record protected Fleet retirement completion", err)
	}
	completedAt := timestamppb.New(result.CompletedAt)
	if result.CompletedAt.IsZero() || completedAt.CheckValid() != nil {
		return nil, status.Error(codes.Internal, "Fleet retirement completion result is invalid")
	}
	return &velav1.RecordRetirementCompletionResponse{
		Replayed: result.Replayed, CompletedAt: completedAt,
	}, nil
}

func (server *Server) HasRetirementCompletion(
	ctx context.Context,
	request *velav1.HasRetirementCompletionRequest,
) (*velav1.HasRetirementCompletionResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("retirement completion lookup")
	}
	resource, ok := parseRetirementResource(
		request.GetResourceKind(), request.GetKubernetesUid(), request.GetNamespace(),
		request.GetName(), request.GetWorkerPoolId(), request.GetWorkerId(),
		request.GetWorkerEpoch(), request.GetDrainOperationIds(),
	)
	if !ok {
		return nil, invalidRequest("retirement completion lookup")
	}
	completed, err := server.service.HasRetirementCompletion(ctx, resource)
	if err != nil {
		return nil, mapServiceError("check protected Fleet retirement completion", err)
	}
	return &velav1.HasRetirementCompletionResponse{Completed: completed}, nil
}

func parseRetirementResource(
	resourceKindValue velav1.FleetProtectedResourceKind,
	kubernetesUID string,
	namespace string,
	name string,
	workerPoolIDValue string,
	workerIDValue string,
	workerEpoch int64,
	drainOperationIDValues []string,
) (fleet.RetirementAuthorizationRequest, bool) {
	if !validText(kubernetesUID, maximumRequestUIDBytes) ||
		!validText(namespace, maximumNameBytes) || !validText(name, maximumNameBytes) ||
		len(drainOperationIDValues) == 0 || len(drainOperationIDValues) > maximumDrainOperations {
		return fleet.RetirementAuthorizationRequest{}, false
	}
	resourceKind, kindOK := protectedResourceKindFromProto(resourceKindValue)
	workerPoolID, poolErr := parseUUID(workerPoolIDValue)
	workerID, workerErr := parseOptionalUUID(workerIDValue)
	drainIDs, drainErr := parseDistinctUUIDs(drainOperationIDValues)
	parsed := fleet.RetirementAuthorizationRequest{
		ResourceKind: resourceKind, KubernetesUID: kubernetesUID,
		Namespace: namespace, Name: name, WorkerPoolID: workerPoolID, WorkerID: workerID,
		WorkerEpoch: workerEpoch, DrainOperationIDs: drainIDs,
	}
	if !kindOK || poolErr != nil || workerErr != nil || drainErr != nil ||
		resourceKind == fleet.ProtectedPod &&
			(workerID == uuid.Nil || workerEpoch <= 0 || len(drainIDs) != 1) ||
		resourceKind != fleet.ProtectedPod && (workerID != uuid.Nil || workerEpoch != 0) {
		return fleet.RetirementAuthorizationRequest{}, false
	}
	return parsed, true
}

func (server *Server) authenticate(ctx context.Context) error {
	if server == nil || server.service == nil || !validSPIFFEIdentity(server.spiffeIdentity) ||
		!validText(server.actorIdentity, maximumActorBytes) {
		return status.Error(codes.FailedPrecondition, "Fleet maintenance server is not configured")
	}
	if ctx == nil {
		return status.Error(codes.Unauthenticated, "verified Fleet Controller mTLS identity is required")
	}
	connectionPeer, ok := peer.FromContext(ctx)
	if !ok || connectionPeer.AuthInfo == nil {
		return status.Error(codes.Unauthenticated, "verified Fleet Controller mTLS identity is required")
	}
	var tlsInfo credentials.TLSInfo
	switch typed := connectionPeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = typed
	case *credentials.TLSInfo:
		if typed == nil {
			return status.Error(codes.Unauthenticated, "verified Fleet Controller mTLS identity is required")
		}
		tlsInfo = *typed
	default:
		return status.Error(codes.Unauthenticated, "verified Fleet Controller mTLS identity is required")
	}
	state := tlsInfo.State
	if !state.HandshakeComplete || len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return status.Error(codes.Unauthenticated, "verified Fleet Controller mTLS identity is required")
	}
	leaf := state.PeerCertificates[0]
	verifiedLeaf := false
	for _, chain := range state.VerifiedChains {
		if len(chain) != 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verifiedLeaf = true
			break
		}
	}
	if !verifiedLeaf || len(leaf.URIs) != 1 || leaf.URIs[0] == nil ||
		leaf.URIs[0].String() != server.spiffeIdentity {
		return status.Error(codes.Unauthenticated, "verified Fleet Controller mTLS identity is required")
	}
	return nil
}

func readinessResult(result fleet.ReadinessResult) (*velav1.FleetReadinessResult, error) {
	state, stateOK := readinessState(result.State)
	nextCheck, nextCheckOK := readinessCheck(result.NextCheck)
	lifecycle, lifecycleOK := workerLifecycle(result.WorkerLifecycle)
	reachability, reachabilityOK := workerReachability(result.WorkerReachability)
	if result.CycleID == uuid.Nil || !stateOK || !lifecycleOK || !reachabilityOK ||
		(result.State == fleet.ReadinessChecking && !nextCheckOK) ||
		(result.State != fleet.ReadinessChecking && result.NextCheck != "") {
		return nil, invalidAuthoritativeResult("Fleet readiness")
	}
	return &velav1.FleetReadinessResult{
		CycleId: result.CycleID.String(), Replayed: result.Replayed,
		State: state, NextCheck: nextCheck,
		WorkerLifecycle: lifecycle, WorkerReachability: reachability,
	}, nil
}

func drainResult(result fleet.DrainResult) (*velav1.FleetDrainResult, error) {
	state, stateOK := drainState(result.State)
	lifecycle, lifecycleOK := workerLifecycle(result.WorkerLifecycle)
	reachability, reachabilityOK := workerReachability(result.WorkerReachability)
	if result.OperationID == uuid.Nil || result.WorkerID == uuid.Nil || result.WorkerEpoch <= 0 ||
		!stateOK || !lifecycleOK || !reachabilityOK {
		return nil, invalidAuthoritativeResult("Fleet drain")
	}
	response := &velav1.FleetDrainResult{
		OperationId: result.OperationID.String(), Replayed: result.Replayed,
		State: state, WorkerId: result.WorkerID.String(),
		WorkerEpoch:        result.WorkerEpoch,
		WorkerLifecycle:    lifecycle,
		WorkerReachability: reachability,
	}
	if !result.Deadline.IsZero() {
		deadline := timestamppb.New(result.Deadline)
		if deadline.CheckValid() != nil {
			return nil, invalidAuthoritativeResult("Fleet drain")
		}
		response.Deadline = deadline
	}
	return response, nil
}

func capacityState(value fleet.CapacityState) (velav1.FleetCapacityState, bool) {
	switch value {
	case fleet.CapacityAdmittable:
		return velav1.FleetCapacityState_FLEET_CAPACITY_STATE_ADMITTABLE, true
	case fleet.CapacityScratchPressured:
		return velav1.FleetCapacityState_FLEET_CAPACITY_STATE_SCRATCH_PRESSURED, true
	case fleet.CapacityScratchCritical:
		return velav1.FleetCapacityState_FLEET_CAPACITY_STATE_SCRATCH_CRITICAL, true
	case fleet.CapacityStorageUnavailable:
		return velav1.FleetCapacityState_FLEET_CAPACITY_STATE_STORAGE_UNAVAILABLE, true
	case fleet.CapacityMultipleBlockers:
		return velav1.FleetCapacityState_FLEET_CAPACITY_STATE_MULTIPLE_BLOCKERS, true
	default:
		return velav1.FleetCapacityState_FLEET_CAPACITY_STATE_UNSPECIFIED, false
	}
}

func readinessState(value fleet.ReadinessState) (velav1.FleetReadinessState, bool) {
	switch value {
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

func readinessCheck(value fleet.ReadinessCheck) (velav1.FleetReadinessCheck, bool) {
	switch value {
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

func readinessCheckFromProto(value velav1.FleetReadinessCheck) (fleet.ReadinessCheck, bool) {
	switch value {
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

func drainState(value fleet.DrainState) (velav1.FleetDrainState, bool) {
	switch value {
	case fleet.DrainDraining:
		return velav1.FleetDrainState_FLEET_DRAIN_STATE_DRAINING, true
	case fleet.DrainComplete:
		return velav1.FleetDrainState_FLEET_DRAIN_STATE_COMPLETE, true
	case fleet.DrainExpired:
		return velav1.FleetDrainState_FLEET_DRAIN_STATE_EXPIRED, true
	default:
		return velav1.FleetDrainState_FLEET_DRAIN_STATE_UNSPECIFIED, false
	}
}

func workerLifecycle(value string) (velav1.FleetWorkerLifecycle, bool) {
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

func workerReachability(value string) (velav1.FleetWorkerReachability, bool) {
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

func invalidAuthoritativeResult(name string) error {
	return status.Error(codes.Internal, "authoritative "+name+" result is invalid")
}

func protectedResourceKindFromProto(
	value velav1.FleetProtectedResourceKind,
) (fleet.ProtectedResourceKind, bool) {
	switch value {
	case velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_POD:
		return fleet.ProtectedPod, true
	case velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_DAEMONSET:
		return fleet.ProtectedDaemonSet, true
	case velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_WORKER_POOL:
		return fleet.ProtectedWorkerPool, true
	default:
		return "", false
	}
}

func mutationOperationFromProto(
	value velav1.FleetMutationOperation,
) (fleet.MutationOperation, bool) {
	switch value {
	case velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE:
		return fleet.MutationDelete, true
	case velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_PATCH_SELECTOR:
		return fleet.MutationPatchSelector, true
	case velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_PATCH_IMAGE:
		return fleet.MutationPatchImage, true
	case velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_REMOVE_FINALIZER:
		return fleet.MutationRemoveFinalizer, true
	default:
		return "", false
	}
}

func parseUUID(value string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, errors.New("UUID is invalid")
	}
	return parsed, nil
}

func parseOptionalUUID(value string) (uuid.UUID, error) {
	if value == "" {
		return uuid.Nil, nil
	}
	return parseUUID(value)
}

func parseDistinctUUIDs(values []string) ([]uuid.UUID, error) {
	parsed := make([]uuid.UUID, 0, len(values))
	seen := make(map[uuid.UUID]struct{}, len(values))
	for _, value := range values {
		id, err := parseUUID(value)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			return nil, errors.New("UUID is duplicated")
		}
		seen[id] = struct{}{}
		parsed = append(parsed, id)
	}
	return parsed, nil
}

func boundedDeadline(value *timestamppb.Timestamp, now time.Time) (time.Time, error) {
	deadline, err := absoluteDeadline(value)
	if err != nil {
		return time.Time{}, errors.New("deadline is invalid")
	}
	if !deadline.After(now) || deadline.After(now.Add(maximumDeadline)) {
		return time.Time{}, errors.New("deadline is outside the allowed window")
	}
	return deadline, nil
}

func absoluteDeadline(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || !value.IsValid() {
		return time.Time{}, errors.New("deadline is invalid")
	}
	return value.AsTime().UTC(), nil
}

func validSPIFFEIdentity(value string) bool {
	identity, err := url.Parse(value)
	return err == nil && identity != nil && identity.Scheme == "spiffe" &&
		identity.Host != "" && identity.Path != "" && identity.Path != "/" &&
		identity.User == nil && identity.RawQuery == "" && identity.Fragment == "" &&
		identity.String() == value
}

func validText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func invalidRequest(operation string) error {
	return status.Error(codes.InvalidArgument, "Fleet "+operation+" request is invalid")
}

func mapServiceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, operation+" was canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, operation+" exceeded its deadline")
	}
	var failure *fleet.Failure
	if errors.As(err, &failure) {
		switch failure.Code {
		case fleet.FailureInvalid:
			return status.Error(codes.InvalidArgument, operation+" was rejected")
		case fleet.FailureConflict:
			return status.Error(codes.Aborted, operation+" conflicted with current authority")
		case fleet.FailureNotFound:
			return status.Error(codes.NotFound, operation+" target does not exist")
		}
	}
	return status.Error(codes.Unavailable, operation+" is temporarily unavailable")
}
