package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestProductionAgentExecutesHeartbeatsAndMaterializesAssignment(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	materialization := newMaterializingStreamControl(t, fixture.authority)
	control := &productionExecutionControl{
		materializingStreamControl: materialization,
		identity:                   runtimeIdentityFromAuthority(fixture.authority),
	}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatalf("NewFilesystemLocalOutputSource: %v", err)
	}
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatalf("NewMemoryMaterializationJournal: %v", err)
	}
	stream, err := stageworkeragent.NewMaterializingStreamAgent(
		runtimeAgent,
		control,
		stageworkeragent.MaterializationConfig{
			Validator: materialization.validator,
			Source:    source,
			Publisher: &outageOncePublisher{objectVersion: "production-l2-version"},
			Journal:   journal,
			SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(
				func(context.Context, stageworkeragent.PendingMaterialization) (
					stageworkeragent.MaterializationSourceLossEvidence, error,
				) {
					now := time.Now().UTC()
					return stageworkeragent.MaterializationSourceLossEvidence{
						FailureFingerprint:    sha256.Sum256([]byte("production source loss")),
						ConsumedResourceUnits: 1, LostAt: now, RetryAt: now.Add(time.Second),
					}, nil
				},
			),
		},
	)
	if err != nil {
		t.Fatalf("NewMaterializingStreamAgent: %v", err)
	}
	waits := 0
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: fixture.client, Stream: stream,
		RuntimeIdentity:   control.identity,
		Devices:           fixture.authority.GetDevices(),
		Members:           fixture.authority.GetMembers(),
		CapacityVector:    fixture.authority.GetCapacityVector(),
		CapacityTTL:       2 * time.Minute,
		HeartbeatInterval: 10 * time.Second,
		Now:               time.Now,
		Wait: func(context.Context, time.Duration) error {
			waits++
			fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}

	result, err := agent.RunAssignment(context.Background(), fixture.assignment)
	if err != nil {
		t.Fatalf("RunAssignment: %v", err)
	}
	if !result.Committed || !result.GPUReleased || waits != 1 ||
		control.startCalls != 1 || control.heartbeatCalls != 2 ||
		control.sealCalls != 1 || control.commitCalls != 1 {
		t.Fatalf(
			"result=%#v waits=%d start=%d heartbeat=%d seal=%d commit=%d",
			result, waits, control.startCalls, control.heartbeatCalls,
			control.sealCalls, control.commitCalls,
		)
	}
}

func TestProductionAgentRunsAssignmentsAndHonorsNoWorkRetry(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	materialization := newMaterializingStreamControl(t, fixture.authority)
	commands := make(chan *velav1.StageWorkerControlServiceConnectResponse)
	control := &productionExecutionControl{
		materializingStreamControl: materialization,
		identity:                   runtimeIdentityFromAuthority(fixture.authority),
		assignment:                 fixture.assignment,
		commands:                   commands,
	}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatalf("NewFilesystemLocalOutputSource: %v", err)
	}
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatalf("NewMemoryMaterializationJournal: %v", err)
	}
	stream, err := stageworkeragent.NewMaterializingStreamAgent(
		runtimeAgent,
		control,
		stageworkeragent.MaterializationConfig{
			Validator: materialization.validator, Source: source,
			Publisher: &outageOncePublisher{objectVersion: "run-l2-version"}, Journal: journal,
			SourceLossEvidence: testSourceLossEvidenceProvider(),
		},
	)
	if err != nil {
		t.Fatalf("NewMaterializingStreamAgent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var waits []time.Duration
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: fixture.client, Stream: stream,
		RuntimeIdentity: control.identity,
		Devices:         fixture.authority.GetDevices(), Members: fixture.authority.GetMembers(),
		CapacityVector: fixture.authority.GetCapacityVector(), CapacityTTL: 2 * time.Minute,
		HeartbeatInterval: 10 * time.Second, Now: time.Now,
		Wait: func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			switch delay {
			case 10 * time.Second:
				fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
			case 250 * time.Millisecond:
				cancel()
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}

	if err := agent.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(commands)
	if control.acquireCalls != 2 || control.startCalls != 1 ||
		control.commitCalls != 1 || !reflect.DeepEqual(
		waits,
		[]time.Duration{10 * time.Second, 250 * time.Millisecond},
	) {
		t.Fatalf(
			"acquire=%d start=%d commit=%d waits=%v",
			control.acquireCalls, control.startCalls, control.commitCalls, waits,
		)
	}
}

func TestProductionAgentReattachesAfterControlReconnectWithoutRerunning(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	materialization := newMaterializingStreamControl(t, fixture.authority)
	commands := make(chan *velav1.StageWorkerControlServiceConnectResponse)
	control := &productionExecutionControl{
		materializingStreamControl: materialization,
		identity:                   runtimeIdentityFromAuthority(fixture.authority),
		assignment:                 fixture.assignment,
		commands:                   commands,
		heartbeatFailures:          1,
	}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatalf("NewFilesystemLocalOutputSource: %v", err)
	}
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatalf("NewMemoryMaterializationJournal: %v", err)
	}
	stream, err := stageworkeragent.NewMaterializingStreamAgent(
		runtimeAgent,
		control,
		stageworkeragent.MaterializationConfig{
			Validator: materialization.validator, Source: source,
			Publisher: &outageOncePublisher{objectVersion: "reattached-l2-version"}, Journal: journal,
			SourceLossEvidence: testSourceLossEvidenceProvider(),
		},
	)
	if err != nil {
		t.Fatalf("NewMaterializingStreamAgent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var waits []time.Duration
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: fixture.client, Stream: stream,
		RuntimeIdentity: control.identity,
		Devices:         fixture.authority.GetDevices(), Members: fixture.authority.GetMembers(),
		CapacityVector: fixture.authority.GetCapacityVector(), CapacityTTL: 2 * time.Minute,
		HeartbeatInterval: 10 * time.Second,
		RetryMinimum:      time.Second, RetryMaximum: 8 * time.Second,
		Now: time.Now,
		Wait: func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			switch delay {
			case 10 * time.Second:
				fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
			case 250 * time.Millisecond:
				cancel()
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}

	if err := agent.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(commands)
	if control.acquireCalls != 2 || control.startCalls != 1 || control.reattachCalls != 1 ||
		control.commitCalls != 1 || !reflect.DeepEqual(
		waits,
		[]time.Duration{time.Second, 10 * time.Second, 250 * time.Millisecond},
	) {
		t.Fatalf(
			"acquire=%d start=%d reattach=%d commit=%d waits=%v",
			control.acquireCalls, control.startCalls, control.reattachCalls,
			control.commitCalls, waits,
		)
	}
}

func TestProductionAgentRegistersCapacityBeforeAcquiringWork(t *testing.T) {
	now := time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)
	identity := productionRuntimeIdentity()
	runtime := &readinessRuntime{identity: identity}
	control := &productionControl{identity: identity}

	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control:         control,
		Runtime:         runtime,
		RuntimeIdentity: identity,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49000000-0000-0000-0000-000000000001", DeviceEpoch: 3,
		}},
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: identity.GetWorkerMemberId(), MemberEpoch: identity.GetWorkerMemberEpoch(),
			ModelRuntimeEpoch: identity.GetModelRuntimeEpoch(),
		}},
		CapacityVector: map[string]int64{"gpu": 1, "slots": 1},
		CapacityTTL:    2 * time.Minute,
		Now:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}

	cycle, err := agent.Discover(context.Background(), 17)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if cycle.Assignment != nil || cycle.RetryAfter != 250*time.Millisecond {
		t.Fatalf("cycle = %#v", cycle)
	}
	wantOperations := []string{"register", "capacity", "acquire"}
	if !reflect.DeepEqual(control.operations, wantOperations) {
		t.Fatalf("control operations = %v, want %v", control.operations, wantOperations)
	}
	if !reflect.DeepEqual(runtime.checks, []velav1.ModelRuntimeReadinessCheck{
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY,
	}) {
		t.Fatalf("readiness checks = %v", runtime.checks)
	}
	if control.registration == nil || control.capacity == nil || control.acquire == nil {
		t.Fatal("production discovery did not send every required operation")
	}
	if !proto.Equal(control.registration.GetRuntimeIdentity(), identity) ||
		control.registration.GetCapacityObservationSequence() != 17 ||
		len(control.registration.GetReadinessEvidence()) == 0 {
		t.Fatalf("registration = %#v", control.registration)
	}
	if control.capacity.GetObservationSequence() != 17 ||
		!control.capacity.GetObservedAt().AsTime().Equal(now) ||
		!control.capacity.GetExpiresAt().AsTime().Equal(now.Add(2*time.Minute)) ||
		!reflect.DeepEqual(control.capacity.GetCapacityVector(), map[string]int64{"gpu": 1, "slots": 1}) {
		t.Fatalf("capacity = %#v", control.capacity)
	}
	if control.acquire.GetCapacityObservationSequence() != 17 ||
		control.acquire.GetWorkerInstanceId() != identity.GetWorkerInstanceId() ||
		control.acquire.GetModelResidencyId() != identity.GetModelResidencyId() ||
		control.acquire.GetStageProfileRevisionId() != identity.GetStageProfileRevisionId() {
		t.Fatalf("acquire = %#v", control.acquire)
	}
}

type readinessRuntime struct {
	identity *velav1.ModelRuntimeIdentity
	checks   []velav1.ModelRuntimeReadinessCheck
}

func (runtime *readinessRuntime) ProbeReadiness(
	_ context.Context,
	request *velav1.ModelRuntimeServiceProbeReadinessRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	if !proto.Equal(request.GetIdentity(), runtime.identity) {
		return nil, errors.New("unexpected runtime identity")
	}
	runtime.checks = append(runtime.checks, request.GetCheck())
	return &velav1.ModelRuntimeServiceProbeReadinessResponse{
		Identity: proto.Clone(runtime.identity).(*velav1.ModelRuntimeIdentity),
		Check:    request.GetCheck(), Ready: true,
		Evidence: []byte("certified:" + request.GetCheck().String()),
		Detail:   "ready",
	}, nil
}

type productionControl struct {
	identity     *velav1.ModelRuntimeIdentity
	operations   []string
	registration *velav1.RegisterWorkerEvidenceRequest
	capacity     *velav1.ReportStageCapacityObservationRequest
	acquire      *velav1.AcquireStageRequest
}

type productionExecutionControl struct {
	*materializingStreamControl
	identity          *velav1.ModelRuntimeIdentity
	heartbeatCalls    int
	heartbeatFailures int
	reattachCalls     int
	assignment        *velav1.StageAssignment
	acquireCalls      int
	commands          <-chan *velav1.StageWorkerControlServiceConnectResponse
}

func (control *productionExecutionControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return control.commands
}

func (control *productionExecutionControl) Exchange(
	ctx context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	switch request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence,
		*velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		return productionReadinessResponse(control.identity), nil
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		control.acquireCalls++
		if control.assignment != nil {
			assignment := control.assignment
			control.assignment = nil
			return &velav1.StageWorkerControlServiceConnectResponse{
				Result: &velav1.StageWorkerControlServiceConnectResponse_StageAssignment{
					StageAssignment: assignment,
				},
			}, nil
		}
		return &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_NoWork{
				NoWork: &velav1.NoStageWork{RetryAfter: duration(250 * time.Millisecond)},
			},
		}, nil
	case *velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage:
		control.heartbeatCalls++
		if control.heartbeatFailures > 0 {
			control.heartbeatFailures--
			return nil, errors.New("injected Stage Worker control reconnect")
		}
		return commandResultResponse(
			velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE,
			velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		), nil
	case *velav1.StageWorkerControlServiceConnectRequest_ReattachStage:
		control.reattachCalls++
		return commandResultResponse(
			velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE,
			velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		), nil
	}
	return control.materializingStreamControl.Exchange(ctx, request)
}

func (control *productionControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return make(chan *velav1.StageWorkerControlServiceConnectResponse)
}

func (control *productionControl) Exchange(
	_ context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	switch operation := request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		control.operations = append(control.operations, "register")
		control.registration = proto.Clone(operation.RegisterWorkerEvidence).(*velav1.RegisterWorkerEvidenceRequest)
		return productionReadinessResponse(control.identity), nil
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		control.operations = append(control.operations, "capacity")
		control.capacity = proto.Clone(operation.ReportCapacityObservation).(*velav1.ReportStageCapacityObservationRequest)
		return productionReadinessResponse(control.identity), nil
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		control.operations = append(control.operations, "acquire")
		control.acquire = proto.Clone(operation.AcquireStage).(*velav1.AcquireStageRequest)
		return &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_NoWork{
				NoWork: &velav1.NoStageWork{RetryAfter: duration(250 * time.Millisecond)},
			},
		}, nil
	default:
		return nil, errors.New("unexpected production control operation")
	}
}

func productionReadinessResponse(
	identity *velav1.ModelRuntimeIdentity,
) *velav1.StageWorkerControlServiceConnectResponse {
	return &velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{
			WorkerReadinessDecision: &velav1.WorkerReadinessDecision{
				WorkerInstanceId:    identity.GetWorkerInstanceId(),
				WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(), Ready: true, Reason: "ready",
			},
		},
	}
}

func productionRuntimeIdentity() *velav1.ModelRuntimeIdentity {
	return &velav1.ModelRuntimeIdentity{
		WorkerInstanceId:       "48000000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch:    5,
		DeviceSetDigest:        bytes.Repeat([]byte{0x81}, 32),
		MembershipDigest:       bytes.Repeat([]byte{0x82}, 32),
		ModelResidencyId:       "48000000-0000-0000-0000-000000000002",
		RuntimeIdentity:        "h3-dit-runtime-v1",
		ModelRuntimeEpoch:      7,
		StageProfileRevisionId: "48000000-0000-0000-0000-000000000003",
		WorkerMemberId:         "48000000-0000-0000-0000-000000000004",
		WorkerMemberEpoch:      11,
	}
}

func runtimeIdentityFromAuthority(authority *velav1.StageAuthority) *velav1.ModelRuntimeIdentity {
	member := authority.GetMembers()[0]
	return &velav1.ModelRuntimeIdentity{
		WorkerInstanceId:       authority.GetWorkerInstanceId(),
		WorkerInstanceEpoch:    authority.GetWorkerInstanceEpoch(),
		DeviceSetDigest:        append([]byte(nil), authority.GetDeviceSetDigest()...),
		MembershipDigest:       append([]byte(nil), authority.GetMembershipDigest()...),
		ModelResidencyId:       authority.GetModelResidencyId(),
		RuntimeIdentity:        authority.GetModelRuntimeIdentity(),
		ModelRuntimeEpoch:      member.GetModelRuntimeEpoch(),
		StageProfileRevisionId: authority.GetStageProfileRevisionId(),
		WorkerMemberId:         member.GetWorkerMemberId(), WorkerMemberEpoch: member.GetMemberEpoch(),
	}
}

func duration(value time.Duration) *durationpb.Duration {
	return durationpb.New(value)
}
