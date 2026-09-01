package fleettransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
)

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
			grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(1<<20),
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

func (client *Client) Apply(
	ctx context.Context,
	plan fleet.ApprovedResidencyPlan,
) (fleet.ActuationPlan, error) {
	if client == nil || client.service == nil {
		return fleet.ActuationPlan{}, errors.New("fleet maintenance client is not configured")
	}
	payload, err := json.Marshal(plan)
	if err != nil || len(payload) == 0 || len(payload) > maximumWorkerRegistryPayloadBytes {
		return fleet.ActuationPlan{}, errors.New("approved ResidencyPlan is invalid")
	}
	response, err := client.service.ApplyResidencyPlan(ctx, &velav1.ApplyResidencyPlanRequest{
		ApprovedPlanJson: payload,
	})
	if err != nil {
		return fleet.ActuationPlan{}, err
	}
	planID, idErr := parseUUID(response.GetPlanRevisionId())
	if idErr != nil || planID != plan.ID || response.GetWorkerInstanceCount() <= 0 {
		return fleet.ActuationPlan{}, errors.New("ResidencyPlan apply response is invalid")
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
	payload, err := json.Marshal(evidence)
	if err != nil || len(payload) == 0 || len(payload) > maximumWorkerRegistryPayloadBytes {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance evidence is invalid")
	}
	response, err := client.service.ObserveWorkerInstance(
		ctx,
		&velav1.ObserveWorkerInstanceRequest{EvidenceJson: payload},
	)
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	workerInstanceID, idErr := parseUUID(response.GetWorkerInstanceId())
	decision := fleet.WorkerInstanceDecision{
		WorkerInstanceID:    workerInstanceID,
		InstanceEpoch:       response.GetInstanceEpoch(),
		ControlSessionEpoch: response.GetControlSessionEpoch(),
		ModelRuntimeEpoch:   response.GetModelRuntimeEpoch(),
		Readiness:           fleet.WorkerInstanceLifecycle(response.GetReadiness()),
	}
	if idErr != nil || workerInstanceID != evidence.WorkerInstanceID ||
		decision.InstanceEpoch <= 0 || decision.ControlSessionEpoch <= 0 ||
		decision.ModelRuntimeEpoch <= 0 || !validWorkerInstanceLifecycle(decision.Readiness) {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance observation response is invalid")
	}
	return decision, nil
}

func (client *Client) AuthorizeMutation(
	ctx context.Context,
	request fleet.MutationAuthorizationRequest,
) (fleet.MutationAuthorizationResult, error) {
	if client == nil || client.service == nil {
		return fleet.MutationAuthorizationResult{}, errors.New("fleet maintenance client is not configured")
	}
	operation, operationOK := mutationOperationToProto(request.Operation)
	if !operationOK || !validText(request.RequestUID, maximumRequestUIDBytes) ||
		request.ActorIdentity != "" || !validText(request.KubernetesUID, maximumRequestUIDBytes) ||
		!validText(request.Namespace, maximumNameBytes) || !validText(request.Name, maximumNameBytes) ||
		request.WorkerInstanceID == uuid.Nil || request.WorkerInstanceEpoch <= 0 ||
		request.ResidencyPlanRevisionID == uuid.Nil || request.WorkerBundleID == uuid.Nil ||
		request.WorkerMemberID == uuid.Nil || len(request.RequestDigest) != 32 {
		return fleet.MutationAuthorizationResult{}, errors.New(
			"WorkerInstance Pod mutation authorization request is invalid",
		)
	}
	response, err := client.service.AuthorizeMutation(ctx, &velav1.AuthorizeMutationRequest{
		RequestUid: request.RequestUID, Operation: operation,
		KubernetesUid: request.KubernetesUID, Namespace: request.Namespace, Name: request.Name,
		RequestDigest:           append([]byte(nil), request.RequestDigest...),
		WorkerInstanceId:        request.WorkerInstanceID.String(),
		WorkerInstanceEpoch:     request.WorkerInstanceEpoch,
		ResidencyPlanRevisionId: request.ResidencyPlanRevisionID.String(),
		WorkerBundleId:          request.WorkerBundleID.String(), WorkerMemberId: request.WorkerMemberID.String(),
	})
	if err != nil {
		return fleet.MutationAuthorizationResult{}, err
	}
	if response.GetRequestUid() != request.RequestUID || !response.GetAuthorized() {
		return fleet.MutationAuthorizationResult{}, errors.New(
			"WorkerInstance Pod mutation authorization response is invalid",
		)
	}
	return fleet.MutationAuthorizationResult{
		RequestUID: response.GetRequestUid(), Replayed: response.GetReplayed(),
		Authorized: response.GetAuthorized(),
	}, nil
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

func mutationOperationToProto(
	operation fleet.MutationOperation,
) (velav1.FleetMutationOperation, bool) {
	switch operation {
	case fleet.MutationDelete:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE, true
	case fleet.MutationRemoveFinalizer:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_REMOVE_FINALIZER, true
	default:
		return velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_UNSPECIFIED, false
	}
}
