package fleettransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (client *Client) Apply(
	ctx context.Context,
	plan fleet.ApprovedResidencyPlan,
) (fleet.ActuationPlan, error) {
	if client == nil || client.service == nil {
		return fleet.ActuationPlan{}, errors.New("fleet maintenance client is not configured")
	}
	encoded, err := json.Marshal(plan)
	if err != nil || len(encoded) == 0 || len(encoded) > maximumWorkerRegistryPayloadBytes {
		return fleet.ActuationPlan{}, errors.New("approved ResidencyPlan payload is invalid")
	}
	response, err := client.service.ApplyResidencyPlan(ctx, &velav1.ApplyResidencyPlanRequest{
		ApprovedPlanJson: encoded,
	})
	if err != nil {
		return fleet.ActuationPlan{}, err
	}
	planID, parseErr := parseUUID(response.GetPlanRevisionId())
	if parseErr != nil || planID != plan.ID || response.GetWorkerInstanceCount() <= 0 {
		return fleet.ActuationPlan{}, errors.New("applied ResidencyPlan response is invalid")
	}
	return fleet.ActuationPlan{
		PlanRevisionID: planID, WorkerInstanceCount: int(response.GetWorkerInstanceCount()),
	}, nil
}

func (client *Client) Observe(
	ctx context.Context,
	evidence fleet.WorkerInstanceEvidence,
) (fleet.WorkerInstanceDecision, error) {
	if client == nil || client.service == nil {
		return fleet.WorkerInstanceDecision{}, errors.New("fleet maintenance client is not configured")
	}
	encoded, err := json.Marshal(evidence)
	if err != nil || len(encoded) == 0 || len(encoded) > maximumWorkerRegistryPayloadBytes {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance evidence payload is invalid")
	}
	response, err := client.service.ObserveWorkerInstance(ctx, &velav1.ObserveWorkerInstanceRequest{
		EvidenceJson: encoded,
	})
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	workerID, parseErr := parseUUID(response.GetWorkerInstanceId())
	decision := fleet.WorkerInstanceDecision{
		WorkerInstanceID: workerID, InstanceEpoch: response.GetInstanceEpoch(),
		ControlSessionEpoch: response.GetControlSessionEpoch(),
		ModelRuntimeEpoch:   response.GetModelRuntimeEpoch(),
		Readiness:           fleet.WorkerInstanceLifecycle(response.GetReadiness()),
	}
	if parseErr != nil || workerID != evidence.WorkerInstanceID || decision.InstanceEpoch <= 0 ||
		decision.ControlSessionEpoch <= 0 || decision.ModelRuntimeEpoch <= 0 ||
		!validWorkerInstanceLifecycle(decision.Readiness) {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance observation response is invalid")
	}
	return decision, nil
}

func validWorkerInstanceLifecycle(state fleet.WorkerInstanceLifecycle) bool {
	switch state {
	case fleet.WorkerInstanceProvisioning, fleet.WorkerInstanceWarming,
		fleet.WorkerInstanceReady, fleet.WorkerInstanceDraining,
		fleet.WorkerInstanceFenced, fleet.WorkerInstanceRetired:
		return true
	default:
		return false
	}
}

type Client struct {
	connection *grpc.ClientConn
	service    velav1.FleetMaintenanceServiceClient
}

func NewClient(connection *grpc.ClientConn) (*Client, error) {
	if connection == nil {
		return nil, errors.New("fleet maintenance gRPC connection is required")
	}
	return &Client{
		connection: connection,
		service:    velav1.NewFleetMaintenanceServiceClient(connection),
	}, nil
}

func DialClient(
	ctx context.Context,
	address string,
	transportCredentials credentials.TransportCredentials,
) (*Client, error) {
	if ctx == nil || transportCredentials == nil {
		return nil, errors.New("fleet maintenance client context and transport credentials are required")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return nil, errors.New("fleet maintenance address must contain a host and port")
	}
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1<<20),
			grpc.MaxCallSendMsgSize(1<<20),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("configure Fleet maintenance client: %w", err)
	}
	connection.Connect()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("fleet maintenance connection shut down during startup")
		}
		if !connection.WaitForStateChange(ctx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("connect to Fleet maintenance service: %w", ctx.Err())
		}
	}
	return NewClient(connection)
}

func (client *Client) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func (client *Client) ResolveWorkerIdentity(
	ctx context.Context,
	request fleet.WorkerIdentityRequest,
) (fleet.WorkerIdentity, error) {
	if client == nil || client.service == nil {
		return fleet.WorkerIdentity{}, errors.New("fleet maintenance client is not configured")
	}
	if !validText(request.NodeIdentity, maximumIdentityBytes) || request.WorkerPoolID == uuid.Nil ||
		!validText(request.KubernetesUID, maximumWorkerPodUIDBytes) ||
		!validText(request.Namespace, maximumNameBytes) ||
		!validText(request.Name, maximumNameBytes) {
		return fleet.WorkerIdentity{}, errors.New("fleet Worker identity request is invalid")
	}
	response, err := client.service.ResolveWorkerIdentity(ctx, &velav1.ResolveWorkerIdentityRequest{
		NodeIdentity:  request.NodeIdentity,
		WorkerPoolId:  request.WorkerPoolID.String(),
		KubernetesUid: request.KubernetesUID,
		Namespace:     request.Namespace,
		Name:          request.Name,
	})
	if err != nil {
		return fleet.WorkerIdentity{}, err
	}
	workerID, workerErr := parseUUID(response.GetWorkerId())
	workerPoolID, poolErr := parseUUID(response.GetWorkerPoolId())
	identity := fleet.WorkerIdentity{
		WorkerID: workerID, WorkerPoolID: workerPoolID,
		WorkerEpoch: response.GetWorkerEpoch(), NodeIdentity: response.GetNodeIdentity(),
	}
	if workerErr != nil || poolErr != nil || identity.WorkerEpoch <= 0 ||
		!validText(identity.NodeIdentity, maximumIdentityBytes) ||
		identity.WorkerPoolID != request.WorkerPoolID || identity.NodeIdentity != request.NodeIdentity {
		return fleet.WorkerIdentity{}, errors.New("fleet Worker identity response is invalid")
	}
	return identity, nil
}

func (client *Client) ConfigureCapacityPolicy(
	ctx context.Context,
	policy fleet.CapacityPolicy,
) (fleet.CapacityPolicyResult, error) {
	if client == nil || client.service == nil {
		return fleet.CapacityPolicyResult{}, errors.New("fleet maintenance client is not configured")
	}
	if policy.WorkerPoolID == uuid.Nil || len(policy.Revision) != 64 ||
		strings.Trim(policy.Revision, "0123456789abcdef") != "" || policy.ConfiguredBy != "" ||
		policy.WorkerHighWatermarkBytes <= 0 || policy.WorkerLowWatermarkBytes < 0 ||
		policy.WorkerLowWatermarkBytes >= policy.WorkerHighWatermarkBytes ||
		policy.WorkerCriticalFreeBytes < 0 || policy.PoolHighWatermarkBytes <= 0 ||
		policy.PoolLowWatermarkBytes < 0 ||
		policy.PoolLowWatermarkBytes >= policy.PoolHighWatermarkBytes ||
		policy.ObservationMaxAge < 10*time.Second || policy.ObservationMaxAge > 10*time.Minute ||
		policy.ObservationMaxAge%time.Second != 0 {
		return fleet.CapacityPolicyResult{}, errors.New("fleet capacity policy is invalid")
	}
	response, err := client.service.ConfigureCapacityPolicy(ctx, &velav1.ConfigureCapacityPolicyRequest{
		WorkerPoolId: policy.WorkerPoolID.String(), Revision: policy.Revision,
		WorkerHighWatermarkBytes: policy.WorkerHighWatermarkBytes,
		WorkerLowWatermarkBytes:  policy.WorkerLowWatermarkBytes,
		WorkerCriticalFreeBytes:  policy.WorkerCriticalFreeBytes,
		PoolHighWatermarkBytes:   policy.PoolHighWatermarkBytes,
		PoolLowWatermarkBytes:    policy.PoolLowWatermarkBytes,
		ObservationMaxAgeSeconds: int64(policy.ObservationMaxAge / time.Second),
	})
	if err != nil {
		return fleet.CapacityPolicyResult{}, err
	}
	workerPoolID, poolErr := parseUUID(response.GetWorkerPoolId())
	if poolErr != nil || workerPoolID != policy.WorkerPoolID ||
		response.GetRevision() != policy.Revision {
		return fleet.CapacityPolicyResult{}, errors.New("fleet capacity policy response is invalid")
	}
	return fleet.CapacityPolicyResult{
		WorkerPoolID: workerPoolID, Revision: response.GetRevision(),
		Replayed: response.GetReplayed(),
	}, nil
}

func (client *Client) BeginReadiness(
	ctx context.Context,
	request fleet.ReadinessRequest,
) (fleet.ReadinessResult, error) {
	if client == nil || client.service == nil {
		return fleet.ReadinessResult{}, errors.New("fleet maintenance client is not configured")
	}
	deadline := timestamppb.New(request.Deadline.UTC())
	if request.CycleID == uuid.Nil || request.WorkerID == uuid.Nil ||
		request.WorkerPoolID == uuid.Nil || request.WorkerEpoch <= 0 ||
		request.ExecutionProfileRevisionID == uuid.Nil ||
		!validText(request.NodeIdentity, maximumIdentityBytes) ||
		!validText(request.InferenceBackendRevision, 200) || deadline.CheckValid() != nil {
		return fleet.ReadinessResult{}, errors.New("fleet readiness begin request is invalid")
	}
	response, err := client.service.BeginReadiness(ctx, &velav1.BeginReadinessRequest{
		CycleId: request.CycleID.String(), WorkerId: request.WorkerID.String(),
		WorkerPoolId: request.WorkerPoolID.String(), WorkerEpoch: request.WorkerEpoch,
		NodeIdentity:               request.NodeIdentity,
		ExecutionProfileRevisionId: request.ExecutionProfileRevisionID.String(),
		InferenceBackendRevision:   request.InferenceBackendRevision,
		Deadline:                   deadline,
	})
	if err != nil {
		return fleet.ReadinessResult{}, err
	}
	return readinessResultFromProto(response.GetResult(), request.CycleID)
}

func (client *Client) GetDrain(
	ctx context.Context,
	operationID uuid.UUID,
) (fleet.DrainResult, error) {
	if client == nil || client.service == nil {
		return fleet.DrainResult{}, errors.New("fleet maintenance client is not configured")
	}
	if operationID == uuid.Nil {
		return fleet.DrainResult{}, errors.New("drain operation id is required")
	}
	response, err := client.service.GetDrain(ctx, &velav1.GetDrainRequest{
		OperationId: operationID.String(),
	})
	if err != nil {
		return fleet.DrainResult{}, err
	}
	return drainResultFromProto(response.GetResult(), operationID)
}

func (client *Client) RequestDrain(
	ctx context.Context,
	request fleet.DrainRequest,
) (fleet.DrainResult, error) {
	if client == nil || client.service == nil {
		return fleet.DrainResult{}, errors.New("fleet maintenance client is not configured")
	}
	deadline := timestamppb.New(request.Deadline.UTC())
	if request.OperationID == uuid.Nil || request.WorkerID == uuid.Nil ||
		request.ExpectedEpoch <= 0 || !validText(request.Reason, maximumReasonBytes) ||
		deadline.CheckValid() != nil {
		return fleet.DrainResult{}, errors.New("fleet drain request is invalid")
	}
	response, err := client.service.RequestDrain(ctx, &velav1.RequestDrainRequest{
		OperationId: request.OperationID.String(), WorkerId: request.WorkerID.String(),
		ExpectedEpoch: request.ExpectedEpoch, Reason: request.Reason, Deadline: deadline,
	})
	if err != nil {
		return fleet.DrainResult{}, err
	}
	return drainResultFromProto(response.GetResult(), request.OperationID)
}

func (client *Client) ReconcileDrain(
	ctx context.Context,
	operationID uuid.UUID,
) (fleet.DrainResult, error) {
	if client == nil || client.service == nil {
		return fleet.DrainResult{}, errors.New("fleet maintenance client is not configured")
	}
	if operationID == uuid.Nil {
		return fleet.DrainResult{}, errors.New("drain operation id is required")
	}
	response, err := client.service.ReconcileDrain(ctx, &velav1.ReconcileDrainRequest{
		OperationId: operationID.String(),
	})
	if err != nil {
		return fleet.DrainResult{}, err
	}
	return drainResultFromProto(response.GetResult(), operationID)
}

func (client *Client) AuthorizeMutation(
	ctx context.Context,
	request fleet.MutationAuthorizationRequest,
) (fleet.MutationAuthorizationResult, error) {
	if client == nil || client.service == nil {
		return fleet.MutationAuthorizationResult{}, errors.New("fleet maintenance client is not configured")
	}
	resourceKind, kindOK := protectedResourceKindToProto(request.ResourceKind)
	operation, operationOK := mutationOperationToProto(request.Operation)
	if !kindOK || !operationOK || !validText(request.RequestUID, maximumRequestUIDBytes) ||
		!validText(request.ActorIdentity, maximumActorBytes) ||
		!validText(request.KubernetesUID, maximumRequestUIDBytes) ||
		!validText(request.Namespace, maximumNameBytes) ||
		!validText(request.Name, maximumNameBytes) ||
		len(request.DrainOperationIDs) > maximumDrainOperations || len(request.RequestDigest) != 32 {
		return fleet.MutationAuthorizationResult{}, errors.New("fleet mutation authorization request is invalid")
	}
	workerID := ""
	workerPoolID := ""
	workerInstanceID := ""
	residencyPlanRevisionID := ""
	workerBundleID := ""
	workerMemberID := ""
	workerInstanceMutation := request.WorkerInstanceID != uuid.Nil || request.WorkerInstanceEpoch != 0 ||
		request.ResidencyPlanRevisionID != uuid.Nil || request.WorkerBundleID != uuid.Nil ||
		request.WorkerMemberID != uuid.Nil
	if workerInstanceMutation {
		if request.ResourceKind != fleet.ProtectedPod || request.WorkerInstanceID == uuid.Nil ||
			request.WorkerInstanceEpoch <= 0 || request.ResidencyPlanRevisionID == uuid.Nil ||
			request.WorkerBundleID == uuid.Nil || request.WorkerMemberID == uuid.Nil ||
			request.WorkerPoolID != uuid.Nil || request.WorkerID != uuid.Nil || request.WorkerEpoch != 0 ||
			len(request.DrainOperationIDs) != 0 {
			return fleet.MutationAuthorizationResult{}, errors.New("fleet mutation authorization request is invalid")
		}
		workerInstanceID = request.WorkerInstanceID.String()
		residencyPlanRevisionID = request.ResidencyPlanRevisionID.String()
		workerBundleID = request.WorkerBundleID.String()
		workerMemberID = request.WorkerMemberID.String()
	} else if request.ResourceKind == fleet.ProtectedPod {
		if request.WorkerID == uuid.Nil || request.WorkerEpoch <= 0 ||
			request.WorkerPoolID == uuid.Nil || len(request.DrainOperationIDs) != 1 {
			return fleet.MutationAuthorizationResult{}, errors.New("fleet mutation authorization request is invalid")
		}
		workerPoolID = request.WorkerPoolID.String()
		workerID = request.WorkerID.String()
	} else if request.WorkerPoolID == uuid.Nil || request.WorkerID != uuid.Nil ||
		request.WorkerEpoch != 0 || len(request.DrainOperationIDs) == 0 {
		return fleet.MutationAuthorizationResult{}, errors.New("fleet mutation authorization request is invalid")
	} else {
		workerPoolID = request.WorkerPoolID.String()
	}
	drainIDs := make([]string, 0, len(request.DrainOperationIDs))
	for _, operationID := range request.DrainOperationIDs {
		if operationID == uuid.Nil {
			return fleet.MutationAuthorizationResult{}, errors.New("fleet mutation authorization request is invalid")
		}
		drainIDs = append(drainIDs, operationID.String())
	}
	response, err := client.service.AuthorizeMutation(ctx, &velav1.AuthorizeMutationRequest{
		RequestUid: request.RequestUID, ResourceKind: resourceKind, Operation: operation,
		KubernetesUid: request.KubernetesUID, Namespace: request.Namespace, Name: request.Name,
		WorkerPoolId: workerPoolID, WorkerId: workerID,
		WorkerEpoch: request.WorkerEpoch, DrainOperationIds: drainIDs,
		RequestDigest:    append([]byte(nil), request.RequestDigest...),
		WorkerInstanceId: workerInstanceID, WorkerInstanceEpoch: request.WorkerInstanceEpoch,
		ResidencyPlanRevisionId: residencyPlanRevisionID, WorkerBundleId: workerBundleID,
		WorkerMemberId: workerMemberID,
	})
	if err != nil {
		return fleet.MutationAuthorizationResult{}, err
	}
	if response.GetRequestUid() != request.RequestUID || !response.GetAuthorized() {
		return fleet.MutationAuthorizationResult{}, errors.New("fleet mutation authorization response is invalid")
	}
	return fleet.MutationAuthorizationResult{
		RequestUID: response.GetRequestUid(),
		Replayed:   response.GetReplayed(),
		Authorized: response.GetAuthorized(),
	}, nil
}

func (client *Client) HasRetirementAuthorization(
	ctx context.Context,
	request fleet.RetirementAuthorizationRequest,
) (bool, error) {
	if client == nil || client.service == nil {
		return false, errors.New("fleet maintenance client is not configured")
	}
	resource, err := retirementResourceToProto(request)
	if err != nil {
		return false, errors.New("fleet retirement authorization request is invalid")
	}
	response, err := client.service.HasRetirementAuthorization(
		ctx,
		&velav1.HasRetirementAuthorizationRequest{
			ResourceKind: resource.kind, KubernetesUid: resource.kubernetesUID,
			Namespace: resource.namespace, Name: resource.name,
			WorkerPoolId: resource.workerPoolID, WorkerId: resource.workerID,
			WorkerEpoch: resource.workerEpoch, DrainOperationIds: resource.drainOperationIDs,
		},
	)
	if err != nil {
		return false, err
	}
	if response == nil {
		return false, errors.New("fleet retirement authorization response is missing")
	}
	return response.GetAuthorized(), nil
}

func (client *Client) RecordRetirementCompletion(
	ctx context.Context,
	request fleet.RetirementAuthorizationRequest,
) (fleet.RetirementCompletionResult, error) {
	if client == nil || client.service == nil {
		return fleet.RetirementCompletionResult{}, errors.New("fleet maintenance client is not configured")
	}
	resource, err := retirementResourceToProto(request)
	if err != nil {
		return fleet.RetirementCompletionResult{}, errors.New("fleet retirement completion request is invalid")
	}
	response, err := client.service.RecordRetirementCompletion(
		ctx,
		&velav1.RecordRetirementCompletionRequest{
			ResourceKind: resource.kind, KubernetesUid: resource.kubernetesUID,
			Namespace: resource.namespace, Name: resource.name,
			WorkerPoolId: resource.workerPoolID, WorkerId: resource.workerID,
			WorkerEpoch: resource.workerEpoch, DrainOperationIds: resource.drainOperationIDs,
		},
	)
	if err != nil {
		return fleet.RetirementCompletionResult{}, err
	}
	completedAt := response.GetCompletedAt()
	if completedAt == nil || completedAt.CheckValid() != nil {
		return fleet.RetirementCompletionResult{}, errors.New("fleet retirement completion response is invalid")
	}
	return fleet.RetirementCompletionResult{
		Replayed: response.GetReplayed(), CompletedAt: completedAt.AsTime(),
	}, nil
}

func (client *Client) HasRetirementCompletion(
	ctx context.Context,
	request fleet.RetirementAuthorizationRequest,
) (bool, error) {
	if client == nil || client.service == nil {
		return false, errors.New("fleet maintenance client is not configured")
	}
	resource, err := retirementResourceToProto(request)
	if err != nil {
		return false, errors.New("fleet retirement completion lookup is invalid")
	}
	response, err := client.service.HasRetirementCompletion(
		ctx,
		&velav1.HasRetirementCompletionRequest{
			ResourceKind: resource.kind, KubernetesUid: resource.kubernetesUID,
			Namespace: resource.namespace, Name: resource.name,
			WorkerPoolId: resource.workerPoolID, WorkerId: resource.workerID,
			WorkerEpoch: resource.workerEpoch, DrainOperationIds: resource.drainOperationIDs,
		},
	)
	if err != nil {
		return false, err
	}
	if response == nil {
		return false, errors.New("fleet retirement completion response is missing")
	}
	return response.GetCompleted(), nil
}

type retirementResourceProto struct {
	kind              velav1.FleetProtectedResourceKind
	kubernetesUID     string
	namespace         string
	name              string
	workerPoolID      string
	workerID          string
	workerEpoch       int64
	drainOperationIDs []string
}

func retirementResourceToProto(
	request fleet.RetirementAuthorizationRequest,
) (retirementResourceProto, error) {
	resourceKind, kindOK := protectedResourceKindToProto(request.ResourceKind)
	if !kindOK || !validText(request.KubernetesUID, maximumRequestUIDBytes) ||
		!validText(request.Namespace, maximumNameBytes) ||
		!validText(request.Name, maximumNameBytes) || request.WorkerPoolID == uuid.Nil ||
		len(request.DrainOperationIDs) == 0 ||
		len(request.DrainOperationIDs) > maximumDrainOperations {
		return retirementResourceProto{}, errors.New("fleet retirement resource is invalid")
	}
	workerID := ""
	if request.ResourceKind == fleet.ProtectedPod {
		if request.WorkerID == uuid.Nil || request.WorkerEpoch <= 0 ||
			len(request.DrainOperationIDs) != 1 {
			return retirementResourceProto{}, errors.New("fleet retirement resource is invalid")
		}
		workerID = request.WorkerID.String()
	} else if request.WorkerID != uuid.Nil || request.WorkerEpoch != 0 {
		return retirementResourceProto{}, errors.New("fleet retirement resource is invalid")
	}
	drainIDs := make([]string, 0, len(request.DrainOperationIDs))
	seenDrainOperations := make(map[uuid.UUID]struct{}, len(request.DrainOperationIDs))
	for _, operationID := range request.DrainOperationIDs {
		if operationID == uuid.Nil {
			return retirementResourceProto{}, errors.New("fleet retirement resource is invalid")
		}
		if _, exists := seenDrainOperations[operationID]; exists {
			return retirementResourceProto{}, errors.New("fleet retirement resource is invalid")
		}
		seenDrainOperations[operationID] = struct{}{}
		drainIDs = append(drainIDs, operationID.String())
	}
	return retirementResourceProto{
		kind: resourceKind, kubernetesUID: request.KubernetesUID,
		namespace: request.Namespace, name: request.Name,
		workerPoolID: request.WorkerPoolID.String(), workerID: workerID,
		workerEpoch: request.WorkerEpoch, drainOperationIDs: drainIDs,
	}, nil
}

func drainResultFromProto(
	result *velav1.FleetDrainResult,
	expectedOperationID uuid.UUID,
) (fleet.DrainResult, error) {
	if result == nil {
		return fleet.DrainResult{}, errors.New("fleet drain response is missing")
	}
	operationID, operationErr := parseUUID(result.GetOperationId())
	workerID, workerErr := parseUUID(result.GetWorkerId())
	state, stateOK := drainStateFromProto(result.GetState())
	workerLifecycle, lifecycleOK := workerLifecycleFromProto(result.GetWorkerLifecycle())
	workerReachability, reachabilityOK := workerReachabilityFromProto(result.GetWorkerReachability())
	deadline := result.GetDeadline()
	if operationErr != nil || operationID != expectedOperationID || workerErr != nil ||
		!stateOK || !lifecycleOK || !reachabilityOK || result.GetWorkerEpoch() <= 0 || deadline == nil ||
		deadline.CheckValid() != nil {
		return fleet.DrainResult{}, errors.New("fleet drain response is invalid")
	}
	return fleet.DrainResult{
		OperationID: operationID, Replayed: result.GetReplayed(), State: state,
		WorkerID: workerID, WorkerEpoch: result.GetWorkerEpoch(),
		WorkerLifecycle:    workerLifecycle,
		WorkerReachability: workerReachability,
		Deadline:           deadline.AsTime(),
	}, nil
}

func readinessResultFromProto(
	result *velav1.FleetReadinessResult,
	expectedCycleID uuid.UUID,
) (fleet.ReadinessResult, error) {
	if result == nil {
		return fleet.ReadinessResult{}, errors.New("fleet readiness response is missing")
	}
	cycleID, cycleErr := parseUUID(result.GetCycleId())
	state, stateOK := readinessStateFromProto(result.GetState())
	nextCheck, nextOK := readinessCheckFromProto(result.GetNextCheck())
	workerLifecycle, lifecycleOK := workerLifecycleFromProto(result.GetWorkerLifecycle())
	workerReachability, reachabilityOK := workerReachabilityFromProto(result.GetWorkerReachability())
	if cycleErr != nil || cycleID != expectedCycleID || !stateOK || !lifecycleOK || !reachabilityOK ||
		(state == fleet.ReadinessChecking && !nextOK) ||
		(state != fleet.ReadinessChecking && result.GetNextCheck() !=
			velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_UNSPECIFIED) {
		return fleet.ReadinessResult{}, errors.New("fleet readiness response is invalid")
	}
	return fleet.ReadinessResult{
		CycleID: cycleID, Replayed: result.GetReplayed(), State: state,
		NextCheck: nextCheck, WorkerLifecycle: workerLifecycle,
		WorkerReachability: workerReachability,
	}, nil
}

func readinessStateFromProto(state velav1.FleetReadinessState) (fleet.ReadinessState, bool) {
	switch state {
	case velav1.FleetReadinessState_FLEET_READINESS_STATE_CHECKING:
		return fleet.ReadinessChecking, true
	case velav1.FleetReadinessState_FLEET_READINESS_STATE_READY:
		return fleet.ReadinessReady, true
	case velav1.FleetReadinessState_FLEET_READINESS_STATE_FAILED:
		return fleet.ReadinessFailed, true
	case velav1.FleetReadinessState_FLEET_READINESS_STATE_EXPIRED:
		return fleet.ReadinessExpired, true
	default:
		return "", false
	}
}

func drainStateFromProto(state velav1.FleetDrainState) (fleet.DrainState, bool) {
	switch state {
	case velav1.FleetDrainState_FLEET_DRAIN_STATE_DRAINING:
		return fleet.DrainDraining, true
	case velav1.FleetDrainState_FLEET_DRAIN_STATE_COMPLETE:
		return fleet.DrainComplete, true
	case velav1.FleetDrainState_FLEET_DRAIN_STATE_EXPIRED:
		return fleet.DrainExpired, true
	default:
		return "", false
	}
}

func workerLifecycleFromProto(value velav1.FleetWorkerLifecycle) (string, bool) {
	switch value {
	case velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_REGISTERING:
		return "REGISTERING", true
	case velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING:
		return "WARMING", true
	case velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_READY:
		return "READY", true
	case velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_BUSY:
		return "BUSY", true
	case velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_DRAINING:
		return "DRAINING", true
	case velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_RECOVERING:
		return "RECOVERING", true
	case velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_QUARANTINED:
		return "QUARANTINED", true
	default:
		return "", false
	}
}

func workerReachabilityFromProto(value velav1.FleetWorkerReachability) (string, bool) {
	switch value {
	case velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_HEALTHY:
		return "HEALTHY", true
	case velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT:
		return "SUSPECT", true
	case velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_OFFLINE:
		return "OFFLINE", true
	default:
		return "", false
	}
}

func protectedResourceKindToProto(
	kind fleet.ProtectedResourceKind,
) (velav1.FleetProtectedResourceKind, bool) {
	switch kind {
	case fleet.ProtectedPod:
		return velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_POD, true
	case fleet.ProtectedDaemonSet:
		return velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_DAEMONSET, true
	case fleet.ProtectedWorkerPool:
		return velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_WORKER_POOL, true
	default:
		return velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_UNSPECIFIED, false
	}
}

func mutationOperationToProto(
	operation fleet.MutationOperation,
) (velav1.FleetMutationOperation, bool) {
	switch operation {
	case fleet.MutationDelete:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE, true
	case fleet.MutationPatchSelector:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_PATCH_SELECTOR, true
	case fleet.MutationPatchImage:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_PATCH_IMAGE, true
	case fleet.MutationRemoveFinalizer:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_REMOVE_FINALIZER, true
	default:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_UNSPECIFIED, false
	}
}
