package fleettransport_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleettransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	fleetSPIFFE = "spiffe://vela.internal/fleet-controller/primary"
	fleetActor  = "fleet-controller/primary"
)

func TestCapacityWireContractsCarryWorkerComputedWatermarkState(t *testing.T) {
	requests := []struct {
		name       string
		descriptor protoreflect.MessageDescriptor
	}{
		{
			name:       "Fleet maintenance",
			descriptor: (&velav1.ObserveCapacityRequest{}).ProtoReflect().Descriptor(),
		},
		{
			name:       "Worker control",
			descriptor: (&velav1.ReportWorkerCapacityRequest{}).ProtoReflect().Descriptor(),
		},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			field := request.descriptor.Fields().ByName("watermark_state")
			if field == nil || field.Kind() != protoreflect.EnumKind ||
				string(field.Enum().FullName()) != "vela.v1.FleetScratchWatermarkState" {
				t.Fatalf("watermark_state descriptor = %#v", field)
			}
		})
	}
}

func TestFleetWireContractsCarryTypedWorkerState(t *testing.T) {
	messages := []struct {
		name       string
		descriptor protoreflect.MessageDescriptor
	}{
		{name: "Fleet readiness", descriptor: (&velav1.FleetReadinessResult{}).ProtoReflect().Descriptor()},
		{name: "Fleet drain", descriptor: (&velav1.FleetDrainResult{}).ProtoReflect().Descriptor()},
		{name: "Worker readiness", descriptor: (&velav1.WorkerReadinessResult{}).ProtoReflect().Descriptor()},
	}
	fields := []struct {
		name     protoreflect.Name
		enumName protoreflect.FullName
	}{
		{name: "worker_lifecycle", enumName: "vela.v1.FleetWorkerLifecycle"},
		{name: "worker_reachability", enumName: "vela.v1.FleetWorkerReachability"},
	}
	for _, message := range messages {
		for _, expected := range fields {
			t.Run(message.name+"/"+string(expected.name), func(t *testing.T) {
				field := message.descriptor.Fields().ByName(expected.name)
				if field == nil || field.Kind() != protoreflect.EnumKind ||
					field.Enum().FullName() != expected.enumName {
					t.Fatalf("%s descriptor = %#v", expected.name, field)
				}
			})
		}
	}
}

func TestFleetMaintenanceWireContractExposesDurableRetirementCompletion(t *testing.T) {
	service := velav1.File_vela_v1_fleet_maintenance_proto.Services().ByName("FleetMaintenanceService")
	if service == nil {
		t.Fatal("FleetMaintenanceService descriptor is missing")
	}
	for _, methodName := range []protoreflect.Name{
		"RecordRetirementCompletion",
		"HasRetirementCompletion",
	} {
		if service.Methods().ByName(methodName) == nil {
			t.Fatalf("FleetMaintenanceService.%s descriptor is missing", methodName)
		}
	}
}

func TestServerRequiresExactConfiguredFleetControllerSPIFFEIdentity(t *testing.T) {
	service := &recordingFleetService{}
	server, err := fleettransport.NewServer(service, fleettransport.Config{
		SPIFFEIdentity: fleetSPIFFE,
		ActorIdentity:  fleetActor,
	})
	if err != nil {
		t.Fatalf("create Fleet maintenance server: %v", err)
	}
	request := &velav1.GetDrainRequest{OperationId: "23000000-0000-0000-0000-000000000042"}
	if _, err := server.GetDrain(context.Background(), request); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing Fleet mTLS identity code = %s error=%v", status.Code(err), err)
	}
	if _, err := server.GetDrain(
		fleetPeerContext(t, "spiffe://vela.internal/fleet-controller/other"),
		request,
	); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong Fleet SPIFFE identity code = %s error=%v", status.Code(err), err)
	}
	if len(service.getDrainIDs) != 0 {
		t.Fatalf("rejected Fleet identity reached service: %#v", service.getDrainIDs)
	}
}

func TestServerMapsTypedFleetMaintenanceRequestsToAuthoritativeService(t *testing.T) {
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000041")
	workerPoolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("23000000-0000-0000-0000-000000000043")
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000044")
	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000045")
	requestUID := "admission-fleet-maintenance"
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	completedAt := deadline.Add(time.Minute)
	observedAt := deadline.Add(-30 * time.Minute)
	digest := make([]byte, 32)
	for index := range digest {
		digest[index] = byte(index + 1)
	}
	service := &recordingFleetService{
		workerIdentity: fleet.WorkerIdentity{
			WorkerID: workerID, WorkerPoolID: workerPoolID,
			WorkerEpoch: 20, NodeIdentity: "node-h3-01",
		},
		capacityResult: fleet.CapacityResult{
			WorkerPoolID: workerPoolID, WorkerState: fleet.CapacityScratchPressured,
			PoolState: fleet.CapacityMultipleBlockers,
		},
		capacityPolicyResult: fleet.CapacityPolicyResult{
			WorkerPoolID: workerPoolID, Revision: strings.Repeat("a", 64),
		},
		readinessResult: fleet.ReadinessResult{
			CycleID: cycleID, State: fleet.ReadinessChecking,
			NextCheck: fleet.ReadinessDevice, WorkerLifecycle: "WARMING",
			WorkerReachability: "SUSPECT",
		},
		drainResult: fleet.DrainResult{
			OperationID: drainID, State: fleet.DrainComplete,
			WorkerID: workerID, WorkerEpoch: 20,
			WorkerLifecycle: "DRAINING", WorkerReachability: "SUSPECT",
			Deadline: deadline,
		},
		mutationResult: fleet.MutationAuthorizationResult{
			RequestUID: requestUID, Authorized: true,
		},
		retirementAuthorized: true,
		retirementCompleted:  true,
		retirementCompletionResult: fleet.RetirementCompletionResult{
			CompletedAt: completedAt,
		},
	}
	server, err := fleettransport.NewServer(service, fleettransport.Config{
		SPIFFEIdentity: fleetSPIFFE,
		ActorIdentity:  fleetActor,
	})
	if err != nil {
		t.Fatalf("create Fleet maintenance server: %v", err)
	}
	ctx := fleetPeerContext(t, fleetSPIFFE)
	identityResponse, err := server.ResolveWorkerIdentity(
		ctx,
		&velav1.ResolveWorkerIdentityRequest{
			NodeIdentity: "node-h3-01", WorkerPoolId: workerPoolID.String(),
			KubernetesUid: "kubernetes-worker-pod-uid-1",
			Namespace:     "vela-system", Name: "h3-worker-node-1",
		},
	)
	if err != nil || identityResponse.GetWorkerId() != workerID.String() ||
		identityResponse.GetWorkerPoolId() != workerPoolID.String() ||
		identityResponse.GetWorkerEpoch() != 20 ||
		identityResponse.GetNodeIdentity() != "node-h3-01" {
		t.Fatalf("ResolveWorkerIdentity response = %#v error=%v", identityResponse, err)
	}
	policyResponse, err := server.ConfigureCapacityPolicy(
		ctx,
		&velav1.ConfigureCapacityPolicyRequest{
			WorkerPoolId: workerPoolID.String(), Revision: strings.Repeat("a", 64),
			WorkerHighWatermarkBytes: 800, WorkerLowWatermarkBytes: 400,
			WorkerCriticalFreeBytes: 50, PoolHighWatermarkBytes: 1200,
			PoolLowWatermarkBytes: 800, ObservationMaxAgeSeconds: 120,
		},
	)
	if err != nil || policyResponse.GetWorkerPoolId() != workerPoolID.String() ||
		policyResponse.GetRevision() != strings.Repeat("a", 64) || policyResponse.GetReplayed() {
		t.Fatalf("ConfigureCapacityPolicy response = %#v error=%v", policyResponse, err)
	}

	capacityResponse, err := server.ObserveCapacity(ctx, &velav1.ObserveCapacityRequest{
		WorkerId: workerID.String(), WorkerPoolId: workerPoolID.String(), WorkerEpoch: 20,
		ObservationSequence: 7, ObservedAt: timestamppb.New(observedAt),
		WatermarkState: velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_PRESSURED,
		TotalBytes:     1000, FreeBytes: 100,
		HighWatermarkBytes: 800, LowWatermarkBytes: 600, CriticalFreeBytes: 50,
		ArtifactStoreReachable: false,
	})
	if err != nil || capacityResponse.GetWorkerPoolId() != workerPoolID.String() ||
		capacityResponse.GetWorkerState() != velav1.FleetCapacityState_FLEET_CAPACITY_STATE_SCRATCH_PRESSURED ||
		capacityResponse.GetPoolState() != velav1.FleetCapacityState_FLEET_CAPACITY_STATE_MULTIPLE_BLOCKERS {
		t.Fatalf("ObserveCapacity response = %#v error=%v", capacityResponse, err)
	}
	if len(service.capacityObservations) != 1 ||
		service.capacityObservations[0].WatermarkState != fleet.ScratchWatermarkPressured {
		t.Fatalf("ObserveCapacity service request = %#v", service.capacityObservations)
	}

	beginResponse, err := server.BeginReadiness(ctx, &velav1.BeginReadinessRequest{
		CycleId: cycleID.String(), WorkerId: workerID.String(), WorkerPoolId: workerPoolID.String(),
		WorkerEpoch: 20, NodeIdentity: "node-h3-01",
		ExecutionProfileRevisionId: profileID.String(),
		InferenceBackendRevision:   "sglang-h3-v3",
		Deadline:                   timestamppb.New(deadline),
	})
	if err != nil || beginResponse.GetResult().GetState() !=
		velav1.FleetReadinessState_FLEET_READINESS_STATE_CHECKING ||
		beginResponse.GetResult().GetNextCheck() != velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_DEVICE ||
		beginResponse.GetResult().GetWorkerLifecycle() !=
			velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_WARMING ||
		beginResponse.GetResult().GetWorkerReachability() !=
			velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT {
		t.Fatalf("BeginReadiness response = %#v error=%v", beginResponse, err)
	}

	if _, err := server.ReportReadiness(ctx, &velav1.ReportReadinessRequest{
		CycleId: cycleID.String(),
		Check:   velav1.FleetReadinessCheck_FLEET_READINESS_CHECK_IDENTITY,
		Passed:  true, EvidenceDigest: digest,
	}); err != nil {
		t.Fatalf("ReportReadiness: %v", err)
	}
	if _, err := server.GetReadiness(ctx, &velav1.GetReadinessRequest{
		CycleId: cycleID.String(),
	}); err != nil {
		t.Fatalf("GetReadiness: %v", err)
	}

	drainResponse, err := server.RequestDrain(ctx, &velav1.RequestDrainRequest{
		OperationId: drainID.String(), WorkerId: workerID.String(), ExpectedEpoch: 20,
		Reason: "planned image rollout", Deadline: timestamppb.New(deadline),
	})
	if err != nil || drainResponse.GetResult().GetState() !=
		velav1.FleetDrainState_FLEET_DRAIN_STATE_COMPLETE ||
		drainResponse.GetResult().GetWorkerLifecycle() !=
			velav1.FleetWorkerLifecycle_FLEET_WORKER_LIFECYCLE_DRAINING ||
		drainResponse.GetResult().GetWorkerReachability() !=
			velav1.FleetWorkerReachability_FLEET_WORKER_REACHABILITY_SUSPECT ||
		drainResponse.GetResult().GetDeadline().AsTime() != deadline {
		t.Fatalf("RequestDrain response = %#v error=%v", drainResponse, err)
	}
	if _, err := server.ReconcileDrain(ctx, &velav1.ReconcileDrainRequest{
		OperationId: drainID.String(),
	}); err != nil {
		t.Fatalf("ReconcileDrain: %v", err)
	}
	if _, err := server.GetDrain(ctx, &velav1.GetDrainRequest{
		OperationId: drainID.String(),
	}); err != nil {
		t.Fatalf("GetDrain: %v", err)
	}

	mutationResponse, err := server.AuthorizeMutation(ctx, &velav1.AuthorizeMutationRequest{
		RequestUid:    requestUID,
		ResourceKind:  velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_POD,
		Operation:     velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE,
		KubernetesUid: "kubernetes-worker-pod-uid-1", Namespace: "vela-system",
		Name: "h3-worker-node-1", WorkerPoolId: workerPoolID.String(),
		WorkerId: workerID.String(), WorkerEpoch: 20,
		DrainOperationIds: []string{drainID.String()}, RequestDigest: digest,
	})
	if err != nil || !mutationResponse.GetAuthorized() ||
		mutationResponse.GetRequestUid() != requestUID {
		t.Fatalf("AuthorizeMutation response = %#v error=%v", mutationResponse, err)
	}
	retirementResponse, err := server.HasRetirementAuthorization(
		ctx,
		&velav1.HasRetirementAuthorizationRequest{
			ResourceKind:  velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_POD,
			KubernetesUid: "kubernetes-worker-pod-uid-1", Namespace: "vela-system",
			Name: "h3-worker-node-1", WorkerPoolId: workerPoolID.String(),
			WorkerId: workerID.String(), WorkerEpoch: 20,
			DrainOperationIds: []string{drainID.String()},
		},
	)
	if err != nil || !retirementResponse.GetAuthorized() {
		t.Fatalf("HasRetirementAuthorization response = %#v error=%v", retirementResponse, err)
	}
	completionResponse, err := server.RecordRetirementCompletion(
		ctx,
		&velav1.RecordRetirementCompletionRequest{
			ResourceKind:  velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_POD,
			KubernetesUid: "kubernetes-worker-pod-uid-1", Namespace: "vela-system",
			Name: "h3-worker-node-1", WorkerPoolId: workerPoolID.String(),
			WorkerId: workerID.String(), WorkerEpoch: 20,
			DrainOperationIds: []string{drainID.String()},
		},
	)
	if err != nil || completionResponse.GetReplayed() ||
		!completionResponse.GetCompletedAt().AsTime().Equal(completedAt) {
		t.Fatalf("RecordRetirementCompletion response = %#v error=%v", completionResponse, err)
	}
	completionLookup, err := server.HasRetirementCompletion(
		ctx,
		&velav1.HasRetirementCompletionRequest{
			ResourceKind:  velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_POD,
			KubernetesUid: "kubernetes-worker-pod-uid-1", Namespace: "vela-system",
			Name: "h3-worker-node-1", WorkerPoolId: workerPoolID.String(),
			WorkerId: workerID.String(), WorkerEpoch: 20,
			DrainOperationIds: []string{drainID.String()},
		},
	)
	if err != nil || !completionLookup.GetCompleted() {
		t.Fatalf("HasRetirementCompletion response = %#v error=%v", completionLookup, err)
	}

	if !reflect.DeepEqual(service.workerIdentityRequests, []fleet.WorkerIdentityRequest{{
		NodeIdentity: "node-h3-01", WorkerPoolID: workerPoolID,
		KubernetesUID: "kubernetes-worker-pod-uid-1",
		Namespace:     "vela-system", Name: "h3-worker-node-1",
	}}) || len(service.capacityPolicies) != 1 ||
		service.capacityPolicies[0].ConfiguredBy != fleetActor ||
		len(service.capacityObservations) != 1 ||
		!service.capacityObservations[0].ObservedAt.Equal(observedAt) ||
		service.capacityObservations[0].ObservedBy != fleetActor ||
		len(service.readinessRequests) != 1 ||
		service.readinessRequests[0].RequestedBy != fleetActor ||
		len(service.readinessEvidence) != 1 ||
		service.readinessEvidence[0].ObservedBy != fleetActor ||
		len(service.drainRequests) != 1 || service.drainRequests[0].RequestedBy != fleetActor ||
		!reflect.DeepEqual(service.reconcileActors, []string{fleetActor}) ||
		len(service.mutationRequests) != 1 ||
		service.mutationRequests[0].ActorIdentity != fleetActor ||
		len(service.retirementAuthorizationRequests) != 1 ||
		service.retirementAuthorizationRequests[0].WorkerID != workerID ||
		len(service.retirementCompletionRequests) != 1 ||
		service.retirementCompletionRequests[0].ObservedBy != fleetActor ||
		service.retirementCompletionRequests[0].WorkerID != workerID ||
		len(service.retirementCompletionLookups) != 1 ||
		service.retirementCompletionLookups[0].KubernetesUID != "kubernetes-worker-pod-uid-1" ||
		!reflect.DeepEqual(service.mutationRequests[0].RequestDigest, digest) {
		t.Fatalf("Fleet service calls = %#v", service)
	}
}

func TestServerRejectsInvalidAuthoritativeFleetEnums(t *testing.T) {
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000041")
	workerPoolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("23000000-0000-0000-0000-000000000043")
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000044")
	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000045")
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	ctx := fleetPeerContext(t, fleetSPIFFE)

	tests := []struct {
		name    string
		service *recordingFleetService
		invoke  func(*fleettransport.Server) (any, error)
	}{
		{
			name: "capacity state",
			service: &recordingFleetService{capacityResult: fleet.CapacityResult{
				WorkerPoolID: workerPoolID, WorkerState: fleet.CapacityState("CORRUPT"),
				PoolState: fleet.CapacityAdmittable,
			}},
			invoke: func(server *fleettransport.Server) (any, error) {
				return server.ObserveCapacity(ctx, &velav1.ObserveCapacityRequest{
					WorkerId: workerID.String(), WorkerPoolId: workerPoolID.String(), WorkerEpoch: 1,
					ObservationSequence: 1, ObservedAt: timestamppb.Now(),
					WatermarkState: velav1.FleetScratchWatermarkState_FLEET_SCRATCH_WATERMARK_STATE_NORMAL,
					TotalBytes:     1000, FreeBytes: 900, HighWatermarkBytes: 800,
					LowWatermarkBytes: 400, CriticalFreeBytes: 50, ArtifactStoreReachable: true,
				})
			},
		},
		{
			name: "readiness state",
			service: &recordingFleetService{readinessResult: fleet.ReadinessResult{
				CycleID: cycleID, State: fleet.ReadinessState("CORRUPT"),
				WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
			}},
			invoke: func(server *fleettransport.Server) (any, error) {
				return server.BeginReadiness(ctx, &velav1.BeginReadinessRequest{
					CycleId: cycleID.String(), WorkerId: workerID.String(),
					WorkerPoolId: workerPoolID.String(), WorkerEpoch: 1,
					NodeIdentity: "node-h3-01", ExecutionProfileRevisionId: profileID.String(),
					InferenceBackendRevision: "sglang-h3-v3", Deadline: timestamppb.New(deadline),
				})
			},
		},
		{
			name: "readiness check",
			service: &recordingFleetService{readinessResult: fleet.ReadinessResult{
				CycleID: cycleID, State: fleet.ReadinessChecking,
				NextCheck:       fleet.ReadinessCheck("CORRUPT"),
				WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
			}},
			invoke: func(server *fleettransport.Server) (any, error) {
				return server.GetReadiness(ctx, &velav1.GetReadinessRequest{CycleId: cycleID.String()})
			},
		},
		{
			name: "worker lifecycle",
			service: &recordingFleetService{readinessResult: fleet.ReadinessResult{
				CycleID: cycleID, State: fleet.ReadinessChecking, NextCheck: fleet.ReadinessIdentity,
				WorkerLifecycle: "CORRUPT", WorkerReachability: "SUSPECT",
			}},
			invoke: func(server *fleettransport.Server) (any, error) {
				return server.GetReadiness(ctx, &velav1.GetReadinessRequest{CycleId: cycleID.String()})
			},
		},
		{
			name: "worker reachability",
			service: &recordingFleetService{readinessResult: fleet.ReadinessResult{
				CycleID: cycleID, State: fleet.ReadinessChecking, NextCheck: fleet.ReadinessIdentity,
				WorkerLifecycle: "WARMING", WorkerReachability: "CORRUPT",
			}},
			invoke: func(server *fleettransport.Server) (any, error) {
				return server.GetReadiness(ctx, &velav1.GetReadinessRequest{CycleId: cycleID.String()})
			},
		},
		{
			name: "drain state",
			service: &recordingFleetService{drainResult: fleet.DrainResult{
				OperationID: drainID, State: fleet.DrainState("CORRUPT"), WorkerID: workerID,
				WorkerEpoch: 1, WorkerLifecycle: "DRAINING", WorkerReachability: "SUSPECT",
				Deadline: deadline,
			}},
			invoke: func(server *fleettransport.Server) (any, error) {
				return server.GetDrain(ctx, &velav1.GetDrainRequest{OperationId: drainID.String()})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, err := fleettransport.NewServer(test.service, fleettransport.Config{
				SPIFFEIdentity: fleetSPIFFE,
				ActorIdentity:  fleetActor,
			})
			if err != nil {
				t.Fatalf("create Fleet maintenance server: %v", err)
			}
			response, err := test.invoke(server)
			responseIsNil := response == nil ||
				(reflect.ValueOf(response).Kind() == reflect.Pointer && reflect.ValueOf(response).IsNil())
			if !responseIsNil || status.Code(err) != codes.Internal {
				t.Fatalf("invalid authoritative enum response=%#v code=%s error=%v", response, status.Code(err), err)
			}
		})
	}
}

func TestServerRejectsMalformedRequestBeforeServiceAndMapsStableFailures(t *testing.T) {
	service := &recordingFleetService{}
	server, err := fleettransport.NewServer(service, fleettransport.Config{
		SPIFFEIdentity: fleetSPIFFE,
		ActorIdentity:  fleetActor,
	})
	if err != nil {
		t.Fatalf("create Fleet maintenance server: %v", err)
	}
	ctx := fleetPeerContext(t, fleetSPIFFE)
	if _, err := server.AuthorizeMutation(ctx, &velav1.AuthorizeMutationRequest{
		RequestUid:    "invalid-mutation",
		ResourceKind:  velav1.FleetProtectedResourceKind_FLEET_PROTECTED_RESOURCE_KIND_POD,
		Operation:     velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE,
		KubernetesUid: "pod-uid", Namespace: "vela-system", Name: "pod",
		WorkerPoolId: "00000000-0000-0000-0000-000000000005",
		WorkerId:     "23000000-0000-0000-0000-000000000041", WorkerEpoch: 20,
		DrainOperationIds: []string{"23000000-0000-0000-0000-000000000042"},
		RequestDigest:     []byte("short"),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed mutation code = %s error=%v", status.Code(err), err)
	}
	if len(service.mutationRequests) != 0 {
		t.Fatalf("malformed mutation reached service: %#v", service.mutationRequests)
	}

	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "invalid", err: &fleet.Failure{Code: fleet.FailureInvalid, Message: "private invalid detail"}, code: codes.InvalidArgument},
		{name: "conflict", err: &fleet.Failure{Code: fleet.FailureConflict, Message: "private conflict detail"}, code: codes.Aborted},
		{name: "missing", err: &fleet.Failure{Code: fleet.FailureNotFound, Message: "private missing detail"}, code: codes.NotFound},
		{name: "database", err: errors.New("postgres password leaked in error"), code: codes.Unavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service.getDrainErr = test.err
			_, err := server.GetDrain(ctx, &velav1.GetDrainRequest{
				OperationId: "23000000-0000-0000-0000-000000000042",
			})
			if status.Code(err) != test.code {
				t.Fatalf("Fleet failure code = %s, want %s error=%v", status.Code(err), test.code, err)
			}
			if message := status.Convert(err).Message(); message == test.err.Error() || message == "postgres password leaked in error" {
				t.Fatalf("Fleet failure leaked internal detail: %q", message)
			}
		})
	}
}

func TestServerForwardsExpiredAbsoluteDrainDeadlineForIdempotentReplay(t *testing.T) {
	operationID := uuid.MustParse("23000000-0000-0000-0000-000000000042")
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000041")
	deadline := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	service := &recordingFleetService{drainResult: fleet.DrainResult{
		OperationID: operationID, State: fleet.DrainExpired,
		WorkerID: workerID, WorkerEpoch: 20,
		WorkerLifecycle: "DRAINING", WorkerReachability: "HEALTHY",
		Deadline: deadline,
	}}
	server, err := fleettransport.NewServer(service, fleettransport.Config{
		SPIFFEIdentity: fleetSPIFFE,
		ActorIdentity:  fleetActor,
	})
	if err != nil {
		t.Fatalf("create Fleet maintenance server: %v", err)
	}
	_, err = server.RequestDrain(fleetPeerContext(t, fleetSPIFFE), &velav1.RequestDrainRequest{
		OperationId: operationID.String(), WorkerId: workerID.String(), ExpectedEpoch: 20,
		Reason: "replay expired planned retirement", Deadline: timestamppb.New(deadline),
	})
	if err != nil {
		t.Fatalf("replay expired absolute drain deadline: %v", err)
	}
	if !reflect.DeepEqual(service.getDrainIDs, []uuid.UUID{operationID}) ||
		len(service.drainRequests) != 1 || !service.drainRequests[0].Deadline.Equal(deadline) {
		t.Fatalf(
			"expired drain replay lookups=%#v service requests=%#v",
			service.getDrainIDs,
			service.drainRequests,
		)
	}
}

type recordingFleetService struct {
	workerIdentity                  fleet.WorkerIdentity
	capacityPolicyResult            fleet.CapacityPolicyResult
	capacityResult                  fleet.CapacityResult
	readinessResult                 fleet.ReadinessResult
	drainResult                     fleet.DrainResult
	mutationResult                  fleet.MutationAuthorizationResult
	retirementAuthorized            bool
	retirementCompleted             bool
	retirementCompletionResult      fleet.RetirementCompletionResult
	capacityObservations            []fleet.CapacityObservation
	capacityPolicies                []fleet.CapacityPolicy
	readinessRequests               []fleet.ReadinessRequest
	readinessEvidence               []fleet.ReadinessEvidence
	getReadinessIDs                 []uuid.UUID
	drainRequests                   []fleet.DrainRequest
	reconcileDrainIDs               []uuid.UUID
	reconcileActors                 []string
	getDrainIDs                     []uuid.UUID
	mutationRequests                []fleet.MutationAuthorizationRequest
	retirementAuthorizationRequests []fleet.RetirementAuthorizationRequest
	retirementCompletionRequests    []fleet.RetirementCompletionRequest
	retirementCompletionLookups     []fleet.RetirementAuthorizationRequest
	workerIdentityRequests          []fleet.WorkerIdentityRequest
	getDrainErr                     error
}

func (service *recordingFleetService) ConfigureCapacityPolicy(
	_ context.Context,
	policy fleet.CapacityPolicy,
) (fleet.CapacityPolicyResult, error) {
	service.capacityPolicies = append(service.capacityPolicies, policy)
	return service.capacityPolicyResult, nil
}

func (service *recordingFleetService) ResolveWorkerIdentity(
	_ context.Context,
	request fleet.WorkerIdentityRequest,
) (fleet.WorkerIdentity, error) {
	service.workerIdentityRequests = append(service.workerIdentityRequests, request)
	return service.workerIdentity, nil
}

func (service *recordingFleetService) ObserveCapacity(
	_ context.Context,
	request fleet.CapacityObservation,
) (fleet.CapacityResult, error) {
	service.capacityObservations = append(service.capacityObservations, request)
	return service.capacityResult, nil
}

func (service *recordingFleetService) BeginReadiness(
	_ context.Context,
	request fleet.ReadinessRequest,
) (fleet.ReadinessResult, error) {
	service.readinessRequests = append(service.readinessRequests, request)
	return service.readinessResult, nil
}

func (service *recordingFleetService) ReportReadiness(
	_ context.Context,
	request fleet.ReadinessEvidence,
) (fleet.ReadinessResult, error) {
	service.readinessEvidence = append(service.readinessEvidence, request)
	return service.readinessResult, nil
}

func (service *recordingFleetService) GetReadiness(
	_ context.Context,
	cycleID uuid.UUID,
) (fleet.ReadinessResult, error) {
	service.getReadinessIDs = append(service.getReadinessIDs, cycleID)
	return service.readinessResult, nil
}

func (service *recordingFleetService) RequestDrain(
	_ context.Context,
	request fleet.DrainRequest,
) (fleet.DrainResult, error) {
	service.drainRequests = append(service.drainRequests, request)
	return service.drainResult, nil
}

func (service *recordingFleetService) ReconcileDrain(
	_ context.Context,
	operationID uuid.UUID,
	actor string,
) (fleet.DrainResult, error) {
	service.reconcileDrainIDs = append(service.reconcileDrainIDs, operationID)
	service.reconcileActors = append(service.reconcileActors, actor)
	return service.drainResult, nil
}

func (service *recordingFleetService) GetDrain(
	_ context.Context,
	operationID uuid.UUID,
) (fleet.DrainResult, error) {
	service.getDrainIDs = append(service.getDrainIDs, operationID)
	return service.drainResult, service.getDrainErr
}

func (service *recordingFleetService) AuthorizeMutation(
	_ context.Context,
	request fleet.MutationAuthorizationRequest,
) (fleet.MutationAuthorizationResult, error) {
	service.mutationRequests = append(service.mutationRequests, request)
	return service.mutationResult, nil
}

func (service *recordingFleetService) HasRetirementAuthorization(
	_ context.Context,
	request fleet.RetirementAuthorizationRequest,
) (bool, error) {
	service.retirementAuthorizationRequests = append(
		service.retirementAuthorizationRequests,
		request,
	)
	return service.retirementAuthorized, nil
}

func (service *recordingFleetService) RecordRetirementCompletion(
	_ context.Context,
	request fleet.RetirementCompletionRequest,
) (fleet.RetirementCompletionResult, error) {
	service.retirementCompletionRequests = append(service.retirementCompletionRequests, request)
	return service.retirementCompletionResult, nil
}

func (service *recordingFleetService) HasRetirementCompletion(
	_ context.Context,
	request fleet.RetirementAuthorizationRequest,
) (bool, error) {
	service.retirementCompletionLookups = append(service.retirementCompletionLookups, request)
	return service.retirementCompleted, nil
}

func fleetPeerContext(t *testing.T, identity string) context.Context {
	t.Helper()
	spiffeID, err := url.Parse(identity)
	if err != nil {
		t.Fatalf("parse Fleet SPIFFE identity: %v", err)
	}
	certificate := &x509.Certificate{URIs: []*url.URL{spiffeID}, Raw: []byte(identity)}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{
			HandshakeComplete: true,
			PeerCertificates:  []*x509.Certificate{certificate},
			VerifiedChains:    [][]*x509.Certificate{{certificate}},
		},
	}})
}
