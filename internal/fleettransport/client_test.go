package fleettransport_test

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleettransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClientCarriesApprovedPlanAndWorkerInstanceEvidence(t *testing.T) {
	planID := uuid.MustParse("49200000-0000-0000-0000-000000000201")
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000204")
	server := &recordingFleetRPCServer{
		applyResidencyPlanResponse: &velav1.ApplyResidencyPlanResponse{
			PlanRevisionId: planID.String(), WorkerInstanceCount: 8,
		},
		observeWorkerInstanceResponse: &velav1.ObserveWorkerInstanceResponse{
			WorkerInstanceId: workerID.String(), InstanceEpoch: 1,
			ControlSessionEpoch: 2, ModelRuntimeEpoch: 3,
			Readiness: string(fleet.WorkerInstanceReady),
		},
	}
	client := newFleetBufconnClient(t, server)
	plan := fleet.ApprovedResidencyPlan{SchemaVersion: 1, ID: planID}
	result, err := client.Apply(context.Background(), plan)
	if err != nil || result.PlanRevisionID != planID || result.WorkerInstanceCount != 8 ||
		len(server.applyResidencyPlanRequests) != 1 {
		t.Fatalf("apply ResidencyPlan result=%#v requests=%d error=%v", result, len(server.applyResidencyPlanRequests), err)
	}
	var carriedPlan fleet.ApprovedResidencyPlan
	if err := json.Unmarshal(server.applyResidencyPlanRequests[0].GetApprovedPlanJson(), &carriedPlan); err != nil || carriedPlan.ID != planID {
		t.Fatalf("carried ResidencyPlan=%#v error=%v", carriedPlan, err)
	}
	evidence := fleet.WorkerInstanceEvidence{SchemaVersion: 1, WorkerInstanceID: workerID}
	decision, err := client.Observe(context.Background(), evidence)
	if err != nil || decision.WorkerInstanceID != workerID ||
		decision.Readiness != fleet.WorkerInstanceReady ||
		len(server.observeWorkerInstanceRequests) != 1 {
		t.Fatalf("observe WorkerInstance decision=%#v requests=%d error=%v", decision, len(server.observeWorkerInstanceRequests), err)
	}
}

func TestClientMapsIdentityDrainAndMutationThroughTypedFleetRPC(t *testing.T) {
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000041")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000042")
	deadline := time.Date(2026, 8, 25, 20, 0, 0, 0, time.UTC)
	completedAt := deadline.Add(time.Minute)
	server := &recordingFleetRPCServer{
		capacityPolicyResponse: &velav1.ConfigureCapacityPolicyResponse{
			WorkerPoolId: poolID.String(), Revision: strings.Repeat("a", 64),
		},
		identityResponse: &velav1.ResolveWorkerIdentityResponse{
			WorkerId: workerID.String(), WorkerPoolId: poolID.String(),
			WorkerEpoch: 20, NodeIdentity: "h3-node-01",
		},
		readinessResponse: &velav1.BeginReadinessResponse{Result: &velav1.FleetReadinessResult{
			CycleId:            "f98bf4ac-94c4-5360-a476-cac78358adbd",
			State:              velav1.FleetReadinessState_FLEET_READINESS_STATE_CHECKING,
			NextCheck:          velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY,
			WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING,
			WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
		}},
		requestDrainResponse: &velav1.RequestDrainResponse{Result: &velav1.FleetDrainResult{
			OperationId: drainID.String(), State: velav1.FleetDrainState_FLEET_DRAIN_STATE_DRAINING,
			WorkerId: workerID.String(), WorkerEpoch: 20,
			WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_DRAINING,
			WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
			Deadline:           timestamppb.New(deadline),
		}},
		reconcileDrainResponse: &velav1.ReconcileDrainResponse{Result: &velav1.FleetDrainResult{
			OperationId: drainID.String(), State: velav1.FleetDrainState_FLEET_DRAIN_STATE_COMPLETE,
			WorkerId: workerID.String(), WorkerEpoch: 20,
			WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_DRAINING,
			WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
			Deadline:           timestamppb.New(deadline),
		}},
		getDrainResponse: &velav1.GetDrainResponse{Result: &velav1.FleetDrainResult{
			OperationId: drainID.String(), State: velav1.FleetDrainState_FLEET_DRAIN_STATE_COMPLETE,
			WorkerId: workerID.String(), WorkerEpoch: 20,
			WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_DRAINING,
			WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
			Deadline:           timestamppb.New(deadline),
		}},
		mutationResponse: &velav1.AuthorizeMutationResponse{
			RequestUid: "admission-bind-delete", Authorized: true,
		},
		retirementAuthorizationResponse: &velav1.HasRetirementAuthorizationResponse{
			Authorized: true,
		},
		retirementCompletionResponse: &velav1.RecordRetirementCompletionResponse{
			CompletedAt: timestamppb.New(completedAt),
		},
		retirementCompletionLookupResponse: &velav1.HasRetirementCompletionResponse{
			Completed: true,
		},
	}
	client := newFleetBufconnClient(t, server)
	policy := fleet.CapacityPolicy{
		WorkerPoolID: poolID, Revision: strings.Repeat("a", 64),
		WorkerHighWatermarkBytes: 800, WorkerLowWatermarkBytes: 400,
		WorkerCriticalFreeBytes: 50, PoolHighWatermarkBytes: 1200,
		PoolLowWatermarkBytes: 800, ObservationMaxAge: 2 * time.Minute,
	}
	configured, err := client.ConfigureCapacityPolicy(context.Background(), policy)
	if err != nil || configured.WorkerPoolID != poolID || configured.Revision != policy.Revision ||
		configured.Replayed {
		t.Fatalf("configured Fleet capacity policy = %#v error=%v", configured, err)
	}

	identityRequest := fleet.WorkerIdentityRequest{
		NodeIdentity: "h3-node-01", WorkerPoolID: poolID,
		KubernetesUID: "kubernetes-worker-pod-uid-1",
		Namespace:     "vela-system", Name: "h3-worker-node-1",
	}
	identity, err := client.ResolveWorkerIdentity(context.Background(), identityRequest)
	if err != nil || identity.WorkerID != workerID || identity.WorkerPoolID != poolID ||
		identity.WorkerEpoch != 20 || identity.NodeIdentity != "h3-node-01" {
		t.Fatalf("resolved Fleet identity = %#v error=%v", identity, err)
	}
	readinessRequest := fleet.ReadinessRequest{
		CycleID:  uuid.MustParse("f98bf4ac-94c4-5360-a476-cac78358adbd"),
		WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 20,
		NodeIdentity:               "h3-node-01",
		ExecutionProfileRevisionID: uuid.MustParse("23000000-0000-0000-0000-000000000043"),
		InferenceBackendRevision:   "sglang-h3-v3", Deadline: deadline,
	}
	readiness, err := client.BeginReadiness(context.Background(), readinessRequest)
	if err != nil || readiness.CycleID != readinessRequest.CycleID ||
		readiness.State != fleet.ReadinessChecking || readiness.NextCheck != fleet.ReadinessIdentity ||
		readiness.WorkerLifecycle != "WARMING" || readiness.WorkerReachability != "SUSPECT" {
		t.Fatalf("Fleet readiness begin = %#v error=%v", readiness, err)
	}
	requestedDrain, err := client.RequestDrain(context.Background(), fleet.DrainRequest{
		OperationID: drainID, WorkerID: workerID, ExpectedEpoch: 20,
		Reason: "replace immutable Worker revision", Deadline: deadline,
	})
	if err != nil || requestedDrain.State != fleet.DrainDraining ||
		requestedDrain.WorkerID != workerID || requestedDrain.WorkerEpoch != 20 {
		t.Fatalf("requested Fleet drain = %#v error=%v", requestedDrain, err)
	}
	reconciledDrain, err := client.ReconcileDrain(context.Background(), drainID)
	if err != nil || reconciledDrain.State != fleet.DrainComplete ||
		reconciledDrain.WorkerID != workerID || reconciledDrain.WorkerEpoch != 20 {
		t.Fatalf("reconciled Fleet drain = %#v error=%v", reconciledDrain, err)
	}
	drain, err := client.GetDrain(context.Background(), drainID)
	if err != nil || drain.OperationID != drainID || drain.State != fleet.DrainComplete ||
		drain.WorkerID != workerID || drain.WorkerEpoch != 20 || !drain.Deadline.Equal(deadline) {
		t.Fatalf("Fleet drain = %#v error=%v", drain, err)
	}
	digest := make([]byte, 32)
	mutationRequest := fleet.MutationAuthorizationRequest{
		RequestUID: "admission-bind-delete", ActorIdentity: "fleet-controller/primary",
		ResourceKind: fleet.ProtectedPod, Operation: fleet.MutationDelete,
		KubernetesUID: "kubernetes-worker-pod-uid-1", Namespace: "vela-system",
		Name: "h3-worker-node-1", WorkerPoolID: poolID,
		WorkerID: workerID, WorkerEpoch: 20,
		DrainOperationIDs: []uuid.UUID{drainID}, RequestDigest: digest,
	}
	mutation, err := client.AuthorizeMutation(context.Background(), mutationRequest)
	if err != nil || !mutation.Authorized || mutation.RequestUID != mutationRequest.RequestUID {
		t.Fatalf("Fleet mutation authorization = %#v error=%v", mutation, err)
	}
	retirementRequest := fleet.RetirementAuthorizationRequest{
		ResourceKind: fleet.ProtectedPod, KubernetesUID: mutationRequest.KubernetesUID,
		Namespace: mutationRequest.Namespace, Name: mutationRequest.Name,
		WorkerPoolID: mutationRequest.WorkerPoolID, WorkerID: mutationRequest.WorkerID,
		WorkerEpoch:       mutationRequest.WorkerEpoch,
		DrainOperationIDs: append([]uuid.UUID(nil), mutationRequest.DrainOperationIDs...),
	}
	retirementAuthorized, err := client.HasRetirementAuthorization(
		context.Background(), retirementRequest,
	)
	if err != nil || !retirementAuthorized {
		t.Fatalf("Fleet retirement authorization = %t error=%v", retirementAuthorized, err)
	}
	completion, err := client.RecordRetirementCompletion(context.Background(), retirementRequest)
	if err != nil || completion.Replayed || !completion.CompletedAt.Equal(completedAt) {
		t.Fatalf("Fleet retirement completion = %#v error=%v", completion, err)
	}
	retirementCompleted, err := client.HasRetirementCompletion(
		context.Background(), retirementRequest,
	)
	if err != nil || !retirementCompleted {
		t.Fatalf("Fleet retirement completion lookup = %t error=%v", retirementCompleted, err)
	}
	if len(server.capacityPolicyRequests) != 1 ||
		server.capacityPolicyRequests[0].GetWorkerPoolId() != poolID.String() ||
		server.capacityPolicyRequests[0].GetObservationMaxAgeSeconds() != 120 ||
		len(server.identityRequests) != 1 ||
		server.identityRequests[0].GetNodeIdentity() != "h3-node-01" ||
		server.identityRequests[0].GetWorkerPoolId() != poolID.String() ||
		server.identityRequests[0].GetKubernetesUid() != identityRequest.KubernetesUID ||
		server.identityRequests[0].GetNamespace() != identityRequest.Namespace ||
		server.identityRequests[0].GetName() != identityRequest.Name ||
		len(server.readinessRequests) != 1 ||
		server.readinessRequests[0].GetCycleId() != readinessRequest.CycleID.String() ||
		server.readinessRequests[0].GetExecutionProfileRevisionId() !=
			readinessRequest.ExecutionProfileRevisionID.String() ||
		!server.readinessRequests[0].GetDeadline().AsTime().Equal(deadline) ||
		len(server.requestDrainRequests) != 1 ||
		server.requestDrainRequests[0].GetOperationId() != drainID.String() ||
		server.requestDrainRequests[0].GetWorkerId() != workerID.String() ||
		server.requestDrainRequests[0].GetReason() != "replace immutable Worker revision" ||
		len(server.reconcileDrainRequests) != 1 ||
		server.reconcileDrainRequests[0].GetOperationId() != drainID.String() ||
		len(server.getDrainRequests) != 1 ||
		server.getDrainRequests[0].GetOperationId() != drainID.String() ||
		len(server.mutationRequests) != 1 ||
		!reflect.DeepEqual(server.mutationRequests[0].GetRequestDigest(), digest) ||
		len(server.retirementAuthorizationRequests) != 1 ||
		server.retirementAuthorizationRequests[0].GetWorkerId() != workerID.String() ||
		len(server.retirementCompletionRequests) != 1 ||
		server.retirementCompletionRequests[0].GetWorkerId() != workerID.String() ||
		len(server.retirementCompletionLookupRequests) != 1 ||
		server.retirementCompletionLookupRequests[0].GetKubernetesUid() !=
			mutationRequest.KubernetesUID ||
		!reflect.DeepEqual(
			server.retirementAuthorizationRequests[0].GetDrainOperationIds(),
			[]string{drainID.String()},
		) {
		t.Fatalf("typed Fleet RPC requests = %#v", server)
	}
}

func TestClientCarriesWorkerInstancePodMutationAuthority(t *testing.T) {
	server := &recordingFleetRPCServer{mutationResponse: &velav1.AuthorizeMutationResponse{
		RequestUid: "worker-instance-delete-1", Authorized: true,
	}}
	client := newFleetBufconnClient(t, server)
	request := fleet.MutationAuthorizationRequest{
		RequestUID: "worker-instance-delete-1", ActorIdentity: "fleet/controller",
		ResourceKind: fleet.ProtectedPod, Operation: fleet.MutationDelete,
		KubernetesUID: "pod-uid-1", Namespace: "vela-system", Name: "wi-1-member-0",
		WorkerInstanceID:        uuid.MustParse("49320000-0000-0000-0000-000000000001"),
		WorkerInstanceEpoch:     7,
		ResidencyPlanRevisionID: uuid.MustParse("49320000-0000-0000-0000-000000000002"),
		WorkerBundleID:          uuid.MustParse("49320000-0000-0000-0000-000000000003"),
		WorkerMemberID:          uuid.MustParse("49320000-0000-0000-0000-000000000004"),
		RequestDigest:           make([]byte, 32),
	}
	result, err := client.AuthorizeMutation(context.Background(), request)
	if err != nil || !result.Authorized || len(server.mutationRequests) != 1 {
		t.Fatalf("authorize WorkerInstance Pod mutation result=%#v requests=%d error=%v", result, len(server.mutationRequests), err)
	}
	carried := server.mutationRequests[0]
	if carried.GetWorkerPoolId() != "" || carried.GetWorkerId() != "" || carried.GetWorkerEpoch() != 0 ||
		len(carried.GetDrainOperationIds()) != 0 ||
		carried.GetWorkerInstanceId() != request.WorkerInstanceID.String() ||
		carried.GetWorkerInstanceEpoch() != request.WorkerInstanceEpoch ||
		carried.GetResidencyPlanRevisionId() != request.ResidencyPlanRevisionID.String() ||
		carried.GetWorkerBundleId() != request.WorkerBundleID.String() ||
		carried.GetWorkerMemberId() != request.WorkerMemberID.String() {
		t.Fatalf("carried WorkerInstance Pod mutation = %#v", carried)
	}
}

func TestClientRejectsUnspecifiedOrUnknownWorkerStateEnums(t *testing.T) {
	cycleID := uuid.MustParse("23030000-0000-0000-0000-000000000041")
	workerID := uuid.MustParse("23030000-0000-0000-0000-000000000042")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("23030000-0000-0000-0000-000000000043")
	drainID := uuid.MustParse("23030000-0000-0000-0000-000000000044")
	deadline := time.Now().UTC().Add(time.Hour)

	for _, testCase := range []struct {
		name   string
		mutate func(*velav1.FleetReadinessResult, *velav1.FleetDrainResult)
		drain  bool
	}{
		{
			name: "readiness unspecified lifecycle",
			mutate: func(readiness *velav1.FleetReadinessResult, _ *velav1.FleetDrainResult) {
				readiness.WorkerLifecycle = velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_UNSPECIFIED
			},
		},
		{
			name: "readiness unknown lifecycle",
			mutate: func(readiness *velav1.FleetReadinessResult, _ *velav1.FleetDrainResult) {
				readiness.WorkerLifecycle = velav1.FleetWorkerLifecycle(99)
			},
		},
		{
			name: "readiness unspecified reachability",
			mutate: func(readiness *velav1.FleetReadinessResult, _ *velav1.FleetDrainResult) {
				readiness.WorkerReachability = velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_UNSPECIFIED
			},
		},
		{
			name: "readiness unknown reachability",
			mutate: func(readiness *velav1.FleetReadinessResult, _ *velav1.FleetDrainResult) {
				readiness.WorkerReachability = velav1.FleetWorkerReachability(99)
			},
		},
		{
			name: "drain unspecified lifecycle", drain: true,
			mutate: func(_ *velav1.FleetReadinessResult, drain *velav1.FleetDrainResult) {
				drain.WorkerLifecycle = velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_UNSPECIFIED
			},
		},
		{
			name: "drain unknown lifecycle", drain: true,
			mutate: func(_ *velav1.FleetReadinessResult, drain *velav1.FleetDrainResult) {
				drain.WorkerLifecycle = velav1.FleetWorkerLifecycle(99)
			},
		},
		{
			name: "drain unspecified reachability", drain: true,
			mutate: func(_ *velav1.FleetReadinessResult, drain *velav1.FleetDrainResult) {
				drain.WorkerReachability = velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_UNSPECIFIED
			},
		},
		{
			name: "drain unknown reachability", drain: true,
			mutate: func(_ *velav1.FleetReadinessResult, drain *velav1.FleetDrainResult) {
				drain.WorkerReachability = velav1.FleetWorkerReachability(99)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			readiness := &velav1.FleetReadinessResult{
				CycleId: cycleID.String(), State: velav1.FleetReadinessState_FLEET_READINESS_STATE_CHECKING,
				NextCheck:          velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY,
				WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING,
				WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
			}
			drain := &velav1.FleetDrainResult{
				OperationId: drainID.String(), State: velav1.FleetDrainState_FLEET_DRAIN_STATE_DRAINING,
				WorkerId: workerID.String(), WorkerEpoch: 7,
				WorkerLifecycle:    velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_DRAINING,
				WorkerReachability: velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT,
				Deadline:           timestamppb.New(deadline),
			}
			testCase.mutate(readiness, drain)
			server := &recordingFleetRPCServer{
				readinessResponse:    &velav1.BeginReadinessResponse{Result: readiness},
				requestDrainResponse: &velav1.RequestDrainResponse{Result: drain},
			}
			client := newFleetBufconnClient(t, server)
			var err error
			if testCase.drain {
				_, err = client.RequestDrain(context.Background(), fleet.DrainRequest{
					OperationID: drainID, WorkerID: workerID, ExpectedEpoch: 7,
					Reason: "typed enum rejection", Deadline: deadline,
				})
			} else {
				_, err = client.BeginReadiness(context.Background(), fleet.ReadinessRequest{
					CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 7,
					NodeIdentity: "h3-node-typed-enum", ExecutionProfileRevisionID: profileID,
					InferenceBackendRevision: "sglang-h3-v3", Deadline: deadline,
				})
			}
			if err == nil {
				t.Fatal("Fleet client accepted an unspecified or unknown Worker state enum")
			}
		})
	}
}

type recordingFleetRPCServer struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	applyResidencyPlanResponse         *velav1.ApplyResidencyPlanResponse
	observeWorkerInstanceResponse      *velav1.ObserveWorkerInstanceResponse
	capacityPolicyResponse             *velav1.ConfigureCapacityPolicyResponse
	identityResponse                   *velav1.ResolveWorkerIdentityResponse
	readinessResponse                  *velav1.BeginReadinessResponse
	requestDrainResponse               *velav1.RequestDrainResponse
	reconcileDrainResponse             *velav1.ReconcileDrainResponse
	getDrainResponse                   *velav1.GetDrainResponse
	mutationResponse                   *velav1.AuthorizeMutationResponse
	retirementAuthorizationResponse    *velav1.HasRetirementAuthorizationResponse
	retirementCompletionResponse       *velav1.RecordRetirementCompletionResponse
	retirementCompletionLookupResponse *velav1.HasRetirementCompletionResponse
	identityRequests                   []*velav1.ResolveWorkerIdentityRequest
	capacityPolicyRequests             []*velav1.ConfigureCapacityPolicyRequest
	readinessRequests                  []*velav1.BeginReadinessRequest
	requestDrainRequests               []*velav1.RequestDrainRequest
	reconcileDrainRequests             []*velav1.ReconcileDrainRequest
	getDrainRequests                   []*velav1.GetDrainRequest
	mutationRequests                   []*velav1.AuthorizeMutationRequest
	retirementAuthorizationRequests    []*velav1.HasRetirementAuthorizationRequest
	retirementCompletionRequests       []*velav1.RecordRetirementCompletionRequest
	retirementCompletionLookupRequests []*velav1.HasRetirementCompletionRequest
	applyResidencyPlanRequests         []*velav1.ApplyResidencyPlanRequest
	observeWorkerInstanceRequests      []*velav1.ObserveWorkerInstanceRequest
}

func (server *recordingFleetRPCServer) ApplyResidencyPlan(
	_ context.Context,
	request *velav1.ApplyResidencyPlanRequest,
) (*velav1.ApplyResidencyPlanResponse, error) {
	server.applyResidencyPlanRequests = append(server.applyResidencyPlanRequests, request)
	return server.applyResidencyPlanResponse, nil
}

func (server *recordingFleetRPCServer) ObserveWorkerInstance(
	_ context.Context,
	request *velav1.ObserveWorkerInstanceRequest,
) (*velav1.ObserveWorkerInstanceResponse, error) {
	server.observeWorkerInstanceRequests = append(server.observeWorkerInstanceRequests, request)
	return server.observeWorkerInstanceResponse, nil
}

func (server *recordingFleetRPCServer) ConfigureCapacityPolicy(
	_ context.Context,
	request *velav1.ConfigureCapacityPolicyRequest,
) (*velav1.ConfigureCapacityPolicyResponse, error) {
	server.capacityPolicyRequests = append(server.capacityPolicyRequests, request)
	return server.capacityPolicyResponse, nil
}

func (server *recordingFleetRPCServer) BeginReadiness(
	_ context.Context,
	request *velav1.BeginReadinessRequest,
) (*velav1.BeginReadinessResponse, error) {
	server.readinessRequests = append(server.readinessRequests, request)
	return server.readinessResponse, nil
}

func (server *recordingFleetRPCServer) ResolveWorkerIdentity(
	_ context.Context,
	request *velav1.ResolveWorkerIdentityRequest,
) (*velav1.ResolveWorkerIdentityResponse, error) {
	server.identityRequests = append(server.identityRequests, request)
	return server.identityResponse, nil
}

func (server *recordingFleetRPCServer) GetDrain(
	_ context.Context,
	request *velav1.GetDrainRequest,
) (*velav1.GetDrainResponse, error) {
	server.getDrainRequests = append(server.getDrainRequests, request)
	return server.getDrainResponse, nil
}

func (server *recordingFleetRPCServer) RequestDrain(
	_ context.Context,
	request *velav1.RequestDrainRequest,
) (*velav1.RequestDrainResponse, error) {
	server.requestDrainRequests = append(server.requestDrainRequests, request)
	return server.requestDrainResponse, nil
}

func (server *recordingFleetRPCServer) ReconcileDrain(
	_ context.Context,
	request *velav1.ReconcileDrainRequest,
) (*velav1.ReconcileDrainResponse, error) {
	server.reconcileDrainRequests = append(server.reconcileDrainRequests, request)
	return server.reconcileDrainResponse, nil
}

func (server *recordingFleetRPCServer) AuthorizeMutation(
	_ context.Context,
	request *velav1.AuthorizeMutationRequest,
) (*velav1.AuthorizeMutationResponse, error) {
	server.mutationRequests = append(server.mutationRequests, request)
	return server.mutationResponse, nil
}

func (server *recordingFleetRPCServer) HasRetirementAuthorization(
	_ context.Context,
	request *velav1.HasRetirementAuthorizationRequest,
) (*velav1.HasRetirementAuthorizationResponse, error) {
	server.retirementAuthorizationRequests = append(
		server.retirementAuthorizationRequests,
		request,
	)
	return server.retirementAuthorizationResponse, nil
}

func (server *recordingFleetRPCServer) RecordRetirementCompletion(
	_ context.Context,
	request *velav1.RecordRetirementCompletionRequest,
) (*velav1.RecordRetirementCompletionResponse, error) {
	server.retirementCompletionRequests = append(server.retirementCompletionRequests, request)
	return server.retirementCompletionResponse, nil
}

func (server *recordingFleetRPCServer) HasRetirementCompletion(
	_ context.Context,
	request *velav1.HasRetirementCompletionRequest,
) (*velav1.HasRetirementCompletionResponse, error) {
	server.retirementCompletionLookupRequests = append(
		server.retirementCompletionLookupRequests,
		request,
	)
	return server.retirementCompletionLookupResponse, nil
}

func newFleetBufconnClient(
	t *testing.T,
	service velav1.FleetMaintenanceServiceServer,
) *fleettransport.Client {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	velav1.RegisterFleetMaintenanceServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create Fleet bufconn client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := fleettransport.NewClient(connection)
	if err != nil {
		t.Fatalf("create Fleet maintenance client: %v", err)
	}
	return client
}
