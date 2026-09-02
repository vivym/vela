package fleettransport

import (
	"context"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type recordingFleetRPCServer struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	apply    *velav1.ApplyResidencyPlanRequest
	observe  *velav1.ObserveWorkerInstanceRequest
	mutation *velav1.AuthorizeMutationRequest
}

func (server *recordingFleetRPCServer) ApplyResidencyPlan(
	_ context.Context,
	request *velav1.ApplyResidencyPlanRequest,
) (*velav1.ApplyResidencyPlanResponse, error) {
	server.apply = request
	var plan fleet.ApprovedResidencyPlan
	if !decodeWorkerRegistryPayload(request.GetApprovedPlanJson(), &plan) {
		return nil, context.Canceled
	}
	return &velav1.ApplyResidencyPlanResponse{
		PlanRevisionId: plan.ID.String(), WorkerInstanceCount: int32(len(plan.WorkerInstances)),
	}, nil
}

func (server *recordingFleetRPCServer) ObserveWorkerInstance(
	_ context.Context,
	request *velav1.ObserveWorkerInstanceRequest,
) (*velav1.ObserveWorkerInstanceResponse, error) {
	server.observe = request
	var evidence fleet.WorkerInstanceEvidence
	if !decodeWorkerRegistryPayload(request.GetEvidenceJson(), &evidence) {
		return nil, context.Canceled
	}
	modelRuntimeEpoch := int64(1)
	if len(evidence.Residencies) != 0 {
		modelRuntimeEpoch = evidence.Residencies[0].ModelRuntimeEpoch
	}
	return &velav1.ObserveWorkerInstanceResponse{
		WorkerInstanceId: evidence.WorkerInstanceID.String(), InstanceEpoch: evidence.InstanceEpoch,
		ControlSessionEpoch: evidence.ControlSessionEpoch, ModelRuntimeEpoch: modelRuntimeEpoch,
		Readiness: string(fleet.WorkerInstanceReady),
	}, nil
}

func (server *recordingFleetRPCServer) AuthorizeMutation(
	_ context.Context,
	request *velav1.AuthorizeMutationRequest,
) (*velav1.AuthorizeMutationResponse, error) {
	server.mutation = request
	return &velav1.AuthorizeMutationResponse{RequestUid: request.GetRequestUid(), Authorized: true}, nil
}

func TestClientCarriesOnlyWorkerInstanceFleetContracts(t *testing.T) {
	rpc := &recordingFleetRPCServer{}
	client := newTestClient(t, rpc)
	planID := uuid.MustParse("20000000-0000-0000-0000-000000000001")
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	plan := fleet.ApprovedResidencyPlan{
		SchemaVersion: 1, ID: planID,
		WorkerInstances: []fleet.PlannedWorkerInstance{{ID: workerID}},
	}
	if result, err := client.Apply(context.Background(), plan); err != nil ||
		result.PlanRevisionID != planID || rpc.apply == nil {
		t.Fatalf("apply result=%#v request=%#v error=%v", result, rpc.apply, err)
	}
	evidence := fleet.WorkerInstanceEvidence{
		WorkerInstanceID: workerID, InstanceEpoch: 7,
		ControlSessionEpoch: 8,
		Residencies:         []fleet.ModelResidencyEvidence{{ModelRuntimeEpoch: 9}},
	}
	if result, err := client.Observe(context.Background(), evidence); err != nil ||
		result.Readiness != fleet.WorkerInstanceReady || rpc.observe == nil {
		t.Fatalf("observe result=%#v request=%#v error=%v", result, rpc.observe, err)
	}
	request := fleet.MutationAuthorizationRequest{
		RequestUID: "request-1", Operation: fleet.MutationDelete,
		KubernetesUID: "pod-uid", Namespace: "vela-system", Name: "worker-instance-1",
		WorkerInstanceID: workerID, WorkerInstanceEpoch: 7,
		ResidencyPlanRevisionID: planID,
		WorkerBundleID:          uuid.MustParse("30000000-0000-0000-0000-000000000001"),
		WorkerMemberID:          uuid.MustParse("40000000-0000-0000-0000-000000000001"),
		RequestDigest:           make([]byte, 32),
	}
	if result, err := client.AuthorizeMutation(context.Background(), request); err != nil ||
		!result.Authorized || rpc.mutation == nil {
		t.Fatalf("mutation result=%#v request=%#v error=%v", result, rpc.mutation, err)
	}
}

func newTestClient(t *testing.T, service velav1.FleetMaintenanceServiceServer) *Client {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	velav1.RegisterFleetMaintenanceServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create gRPC connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := NewClient(connection)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return client
}
