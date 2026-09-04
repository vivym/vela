package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestProductionAgentReportsStructuredRuntimeFailureBeforeReleasingAuthority(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	failedAt := time.Date(2026, 8, 30, 11, 0, 0, 0, time.UTC)
	fingerprint := bytes.Repeat([]byte{0xa1}, sha256.Size)
	runtimeClient := &failureStatusRuntimeClient{
		ModelRuntimeServiceClient: fixture.client,
		failure: &velav1.ModelRuntimeFailureEvidence{
			FailureClass: "backend_oom", FailureFingerprint: fingerprint,
			Detail: "resident DiT process exhausted device memory", WorkerReusable: true,
			ConsumedResourceUnits: 43, FailedAt: timestamppb.New(failedAt),
			RetryAt: timestamppb.New(failedAt.Add(time.Minute)),
		},
	}
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: runtimeClient}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := &productionExecutionControl{
		materializingStreamControl: newMaterializingStreamControl(t, fixture.authority),
		identity:                   runtimeIdentityFromAuthority(fixture.authority),
	}
	stream, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: runtimeClient, Stream: stream,
		RuntimeIdentity: control.identity,
		Devices:         fixture.authority.GetDevices(), Members: fixture.authority.GetMembers(),
		CapacityVector: fixture.authority.GetCapacityVector(), CapacityTTL: 2 * time.Minute,
		HeartbeatInterval: time.Second, Now: time.Now,
		Wait: func(context.Context, time.Duration) error {
			t.Fatal("failed runtime must not return to the monitor wait loop")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}

	result, err := agent.RunAssignment(context.Background(), fixture.assignment)
	failure := control.failure
	if err == nil || !result.GPUReleased || control.failCalls != 1 || failure == nil ||
		failure.GetFailureClass() != "backend_oom" ||
		!bytes.Equal(failure.GetFailureFingerprint(), fingerprint) ||
		!failure.GetWorkerReusable() || failure.GetConsumedResourceUnits() != 43 ||
		!failure.GetFailedAt().AsTime().Equal(failedAt) ||
		!failure.GetRetryAt().AsTime().Equal(failedAt.Add(time.Minute)) {
		t.Fatalf("RunAssignment = %#v failure=%#v error=%v", result, failure, err)
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
	sequenceSource := &capacitySequenceSource{values: []int64{41, 42, 53}}
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: fixture.client, Stream: stream,
		RuntimeIdentity: control.identity,
		Devices:         fixture.authority.GetDevices(), Members: fixture.authority.GetMembers(),
		CapacityVector: fixture.authority.GetCapacityVector(), CapacityTTL: 2 * time.Minute,
		HeartbeatInterval: 10 * time.Second, Now: time.Now,
		ObservationSequenceSource: sequenceSource,
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
	if !reflect.DeepEqual(control.observationSequences, []int64{41, 42}) ||
		sequenceSource.index != 2 {
		t.Fatalf("observation sequences = %v, want durable source values", control.observationSequences)
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
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{61, 72, 95, 106, 117, 118}},
		Now:                       time.Now,
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
	control := &productionControl{
		identity: identity, barrierGeneration: 13, controlSessionEpoch: 7,
	}

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
		CapacityVector:            map[string]int64{"gpu": 1, "slots": 1},
		CapacityTTL:               2 * time.Minute,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{18}},
		Now:                       func() time.Time { return now },
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
	wantOperations := []string{"capacity", "register", "capacity", "acquire"}
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
	if control.capacity.GetObservationSequence() != 18 ||
		!control.capacity.GetObservedAt().AsTime().Equal(now) ||
		!control.capacity.GetExpiresAt().AsTime().Equal(now.Add(2*time.Minute)) ||
		!reflect.DeepEqual(control.capacity.GetCapacityVector(), map[string]int64{"gpu": 1, "slots": 1}) {
		t.Fatalf("capacity = %#v", control.capacity)
	}
	if len(control.capacities) != 2 ||
		!reflect.DeepEqual(control.capacities[0].GetCapacityVector(), map[string]int64{"gpu": 0, "slots": 0}) {
		t.Fatalf("capacity publication sequence = %#v", control.capacities)
	}
	if control.acquire.GetCapacityObservationSequence() != 18 ||
		control.acquire.GetWorkerInstanceId() != identity.GetWorkerInstanceId() ||
		control.acquire.GetModelResidencyId() != identity.GetModelResidencyId() ||
		control.acquire.GetModelRuntimeEpoch() != 13 ||
		control.acquire.GetStageProfileRevisionId() != identity.GetStageProfileRevisionId() {
		t.Fatalf("acquire = %#v", control.acquire)
	}

	control.operations = nil
	control.capacities = nil
	cycle, err = agent.Discover(context.Background(), 19)
	if err != nil || cycle.Assignment != nil || cycle.RetryAfter != 250*time.Millisecond {
		t.Fatalf("second Discover = %#v error=%v", cycle, err)
	}
	if !reflect.DeepEqual(control.operations, []string{"register", "acquire"}) ||
		len(control.capacities) != 0 ||
		control.registration.GetCapacityObservationSequence() != 18 ||
		control.acquire.GetCapacityObservationSequence() != 18 {
		t.Fatalf(
			"fresh capacity reuse operations=%v registration=%#v capacities=%#v acquire=%#v",
			control.operations,
			control.registration,
			control.capacities,
			control.acquire,
		)
	}

	control.operations = nil
	control.capacities = nil
	now = now.Add(time.Minute)
	cycle, err = agent.Discover(context.Background(), 19)
	if err != nil || cycle.Assignment != nil || cycle.RetryAfter != 250*time.Millisecond {
		t.Fatalf("renewal-window Discover = %#v error=%v", cycle, err)
	}
	if !reflect.DeepEqual(control.operations, []string{"register", "capacity", "acquire"}) ||
		len(control.capacities) != 1 ||
		control.capacities[0].GetObservationSequence() != 19 ||
		!control.capacities[0].GetObservedAt().AsTime().Equal(now) ||
		control.registration.GetCapacityObservationSequence() != 18 ||
		control.acquire.GetCapacityObservationSequence() != 19 {
		t.Fatalf(
			"capacity renewal operations=%v registration=%#v capacities=%#v acquire=%#v",
			control.operations,
			control.registration,
			control.capacities,
			control.acquire,
		)
	}

	control.operations = nil
	control.capacities = nil
	now = now.Add(time.Second)
	control.controlSessionEpoch = 8
	cycle, err = agent.Discover(context.Background(), 20)
	if err != nil || cycle.Assignment != nil || cycle.RetryAfter != 250*time.Millisecond {
		t.Fatalf("new-session Discover = %#v error=%v", cycle, err)
	}
	if !reflect.DeepEqual(control.operations, []string{"register", "capacity", "acquire"}) ||
		len(control.capacities) != 1 ||
		control.capacities[0].GetObservationSequence() != 20 ||
		control.registration.GetCapacityObservationSequence() != 19 ||
		control.acquire.GetCapacityObservationSequence() != 20 {
		t.Fatalf(
			"new-session capacity operations=%v registration=%#v capacities=%#v acquire=%#v",
			control.operations,
			control.registration,
			control.capacities,
			control.acquire,
		)
	}
}

func TestProductionAgentWithdrawsCapacityAfterAcceptedSequencePersistenceFailure(t *testing.T) {
	identity := productionRuntimeIdentity()
	control := &productionControl{
		identity: identity, barrierGeneration: 13,
		capacityResponseSequences: []int64{30, 31, 32},
	}
	persistErr := errors.New("capacity high-water is unavailable")
	sequenceSource := &capacitySequenceSource{
		values:        []int64{31},
		observeErrors: map[int64]error{31: persistErr, 32: persistErr},
	}
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: &readinessRuntime{identity: identity},
		RuntimeIdentity: identity,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49000000-0000-0000-0000-000000000001", DeviceEpoch: 3,
		}},
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: identity.GetWorkerMemberId(), MemberEpoch: identity.GetWorkerMemberEpoch(),
		}},
		CapacityVector:            map[string]int64{"gpu": 1, "slots": 1},
		CapacityTTL:               2 * time.Minute,
		ObservationSequenceSource: sequenceSource,
		Now:                       time.Now,
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}

	if _, err := agent.Discover(context.Background(), 30); !errors.Is(err, persistErr) {
		t.Fatalf("Discover error = %v, want accepted-capacity persistence failure", err)
	}
	if !reflect.DeepEqual(
		control.operations,
		[]string{"capacity", "register", "capacity", "capacity"},
	) || len(control.capacities) != 3 || control.acquire != nil ||
		sequenceSource.index != 1 ||
		!reflect.DeepEqual(sequenceSource.observed, []int64{30, 31, 32}) ||
		!reflect.DeepEqual(
			control.capacities[0].GetCapacityVector(),
			map[string]int64{"gpu": 0, "slots": 0},
		) || !reflect.DeepEqual(
		control.capacities[1].GetCapacityVector(),
		map[string]int64{"gpu": 1, "slots": 1},
	) || !reflect.DeepEqual(
		control.capacities[2].GetCapacityVector(),
		map[string]int64{"gpu": 0, "slots": 0},
	) {
		t.Fatalf(
			"failed persistence operations=%v capacities=%#v acquire=%#v observed=%v",
			control.operations,
			control.capacities,
			control.acquire,
			sequenceSource.observed,
		)
	}
}

func TestProductionAgentWithdrawsCapacityAfterRegistrationFailure(t *testing.T) {
	identity := productionRuntimeIdentity()
	control := &productionControl{identity: identity, barrierGeneration: 13}
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: &readinessRuntime{identity: identity},
		RuntimeIdentity: identity,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49000000-0000-0000-0000-000000000001", DeviceEpoch: 3,
		}},
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: identity.GetWorkerMemberId(), MemberEpoch: identity.GetWorkerMemberEpoch(),
		}},
		CapacityVector:            map[string]int64{"gpu": 1, "slots": 1},
		CapacityTTL:               2 * time.Minute,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{31}},
		Now:                       time.Now,
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}
	if _, err := agent.Discover(context.Background(), 30); err != nil {
		t.Fatalf("initialize capacity authority: %v", err)
	}
	control.operations = nil
	control.capacities = nil
	control.registrationError = errors.New("runtime registration unavailable")
	if _, err := agent.Discover(context.Background(), 32); err == nil {
		t.Fatal("Discover accepted failed runtime registration")
	}
	if !reflect.DeepEqual(control.operations, []string{"register", "capacity"}) ||
		len(control.capacities) != 1 ||
		control.capacities[0].GetObservationSequence() != 32 ||
		!reflect.DeepEqual(
			control.capacities[0].GetCapacityVector(),
			map[string]int64{"gpu": 0, "slots": 0},
		) {
		t.Fatalf("capacity withdrawal operations=%v capacities=%#v", control.operations, control.capacities)
	}
}

func TestProductionAgentObservesRecoverableEvidenceErrors(t *testing.T) {
	for _, test := range []struct {
		name              string
		capacityError     error
		registrationError error
		wantOperation     string
	}{
		{name: "capacity", capacityError: errors.New("capacity unavailable"), wantOperation: "report-capacity"},
		{name: "registration", registrationError: errors.New("registration unavailable"), wantOperation: "register-worker-evidence"},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := productionRuntimeIdentity()
			control := &productionControl{
				identity: identity, barrierGeneration: 13,
				capacityError: test.capacityError, registrationError: test.registrationError,
			}
			var observedOperation string
			var observedError error
			agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
				Control: control, Runtime: &readinessRuntime{identity: identity},
				RuntimeIdentity: identity,
				Devices: []*velav1.StageAuthorityDeviceEpoch{{
					DeviceId: "49000000-0000-0000-0000-000000000001", DeviceEpoch: 3,
				}},
				Members: []*velav1.StageAuthorityMemberEpoch{{
					WorkerMemberId: identity.GetWorkerMemberId(), MemberEpoch: identity.GetWorkerMemberEpoch(),
				}},
				CapacityVector: map[string]int64{"gpu": 1, "slots": 1},
				CapacityTTL:    2 * time.Minute, Now: time.Now,
				RetryObserver: func(operation string, cause error) {
					observedOperation, observedError = operation, cause
				},
			})
			if err != nil {
				t.Fatalf("NewProductionAgent: %v", err)
			}
			_, err = agent.Discover(context.Background(), 29)
			if err == nil || observedOperation != test.wantOperation ||
				observedError == nil || !strings.Contains(observedError.Error(), "unavailable") {
				t.Fatalf("Discover error=%v observed=%q/%v", err, observedOperation, observedError)
			}
		})
	}
}

func TestProductionAgentNonLeaderRegistersWithoutCompetingForStageWork(t *testing.T) {
	identity := productionRuntimeIdentity()
	control := &productionControl{
		identity: identity, barrierGeneration: 13,
		leaderMemberID: "48000000-0000-0000-0000-000000000003",
	}
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: &readinessRuntime{identity: identity},
		RuntimeIdentity: identity,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49000000-0000-0000-0000-000000000001", DeviceEpoch: 3,
		}},
		Members: []*velav1.StageAuthorityMemberEpoch{
			{WorkerMemberId: "48000000-0000-0000-0000-000000000003", MemberEpoch: 1},
			{WorkerMemberId: identity.GetWorkerMemberId(), MemberEpoch: identity.GetWorkerMemberEpoch()},
		},
		CapacityVector: map[string]int64{"gpu": 4, "slots": 1},
		CapacityTTL:    2 * time.Minute,
		RetryMinimum:   250 * time.Millisecond,
		Now:            time.Now,
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}
	cycle, err := agent.Discover(context.Background(), 21)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if cycle.Assignment != nil || cycle.RetryAfter != 250*time.Millisecond {
		t.Fatalf("non-leader cycle = %#v", cycle)
	}
	if !reflect.DeepEqual(control.operations, []string{"register"}) ||
		control.capacity != nil || control.acquire != nil ||
		control.registration.GetCapacityObservationSequence() != 0 {
		t.Fatalf(
			"non-leader operations=%v capacity=%#v acquire=%#v",
			control.operations, control.capacity, control.acquire,
		)
	}
	if got := control.registration.GetMembers(); len(got) != 1 ||
		got[0].GetWorkerMemberId() != identity.GetWorkerMemberId() ||
		got[0].GetModelRuntimeEpoch() != identity.GetModelRuntimeEpoch() {
		t.Fatalf("non-leader local registration members = %#v", got)
	}
}

func TestDiscoverRuntimeIdentityBindsResidentRuntimeToConfiguredWorkerMember(t *testing.T) {
	identity := productionRuntimeIdentity()
	runtime := &identityDiscoveryRuntime{identity: identity}
	discovered, err := stageworkeragent.DiscoverRuntimeIdentity(
		context.Background(),
		runtime,
		stageworkeragent.RuntimeIdentityExpectation{
			WorkerInstanceID:    identity.GetWorkerInstanceId(),
			WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
			WorkerMemberID:      identity.GetWorkerMemberId(),
			WorkerMemberEpoch:   identity.GetWorkerMemberEpoch(),
		},
	)
	if err != nil || !proto.Equal(discovered, identity) {
		t.Fatalf("DiscoverRuntimeIdentity = %#v, error=%v", discovered, err)
	}
	if runtime.request.GetIdentity() != nil || runtime.request.GetCheck() !=
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE {
		t.Fatalf("identity discovery request = %#v", runtime.request)
	}

	_, err = stageworkeragent.DiscoverRuntimeIdentity(
		context.Background(),
		runtime,
		stageworkeragent.RuntimeIdentityExpectation{
			WorkerInstanceID:    identity.GetWorkerInstanceId(),
			WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch() + 1,
			WorkerMemberID:      identity.GetWorkerMemberId(),
			WorkerMemberEpoch:   identity.GetWorkerMemberEpoch(),
		},
	)
	if err == nil {
		t.Fatal("DiscoverRuntimeIdentity accepted a different WorkerInstance epoch")
	}
}

func TestProductionAgentDiscoversRegistersAndPollsAUXResidentRuntimes(t *testing.T) {
	now := time.Date(2026, 9, 1, 18, 0, 0, 0, time.UTC)
	encoder := productionRuntimeIdentity()
	encoder.RuntimeIdentity = "h3-encoder-runtime-v1"
	encoder.ModelResidencyId = "48000000-0000-0000-0000-000000000012"
	encoder.StageProfileRevisionId = "48000000-0000-0000-0000-000000000013"
	vae := proto.Clone(encoder).(*velav1.ModelRuntimeIdentity)
	vae.RuntimeIdentity = "h3-vae-runtime-v1"
	vae.ModelResidencyId = "48000000-0000-0000-0000-000000000022"
	vae.StageProfileRevisionId = "48000000-0000-0000-0000-000000000023"
	vae.ModelRuntimeEpoch = 8
	runtime := &auxRuntime{identities: []*velav1.ModelRuntimeIdentity{encoder, vae}}

	discovered, err := stageworkeragent.DiscoverRuntimeIdentities(
		context.Background(), runtime,
		stageworkeragent.RuntimeIdentityExpectation{
			WorkerInstanceID:    encoder.GetWorkerInstanceId(),
			WorkerInstanceEpoch: encoder.GetWorkerInstanceEpoch(),
			WorkerMemberID:      encoder.GetWorkerMemberId(),
			WorkerMemberEpoch:   encoder.GetWorkerMemberEpoch(),
		},
	)
	if err != nil || len(discovered) != 2 {
		t.Fatalf("DiscoverRuntimeIdentities = %#v error=%v", discovered, err)
	}
	control := &auxProductionControl{primary: encoder}
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: runtime, RuntimeIdentities: discovered,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49000000-0000-0000-0000-000000000001", DeviceEpoch: 3,
		}},
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: encoder.GetWorkerMemberId(), MemberEpoch: encoder.GetWorkerMemberEpoch(),
		}},
		CapacityVector:            map[string]int64{"gpu": 1, "slots": 1},
		CapacityTTL:               2 * time.Minute,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{20}},
		Now:                       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewProductionAgent: %v", err)
	}
	cycle, err := agent.Discover(context.Background(), 19)
	if err != nil || cycle.Assignment != nil || cycle.RetryAfter != 250*time.Millisecond {
		t.Fatalf("AUX Discover = %#v error=%v", cycle, err)
	}
	if len(control.registrations) != 2 || len(control.capacities) != 2 ||
		len(control.acquires) != 2 ||
		control.registrations[0].GetCapacityObservationSequence() != 19 ||
		control.registrations[1].GetCapacityObservationSequence() != 19 ||
		control.registrations[0].GetMembers()[0].GetModelRuntimeEpoch() != 7 ||
		control.registrations[1].GetMembers()[0].GetModelRuntimeEpoch() != 8 ||
		control.capacities[0].GetObservationSequence() != 19 ||
		!reflect.DeepEqual(control.capacities[0].GetCapacityVector(), map[string]int64{"gpu": 0, "slots": 0}) ||
		control.capacities[1].GetObservationSequence() != 20 ||
		!reflect.DeepEqual(control.capacities[1].GetCapacityVector(), map[string]int64{"gpu": 1, "slots": 1}) ||
		control.acquires[0].GetCapacityObservationSequence() != 20 ||
		control.acquires[1].GetCapacityObservationSequence() != 20 ||
		control.acquires[0].GetStageProfileRevisionId() != encoder.GetStageProfileRevisionId() ||
		control.acquires[1].GetStageProfileRevisionId() != vae.GetStageProfileRevisionId() {
		t.Fatalf("AUX registration/capacity/acquire = %#v/%#v/%#v",
			control.registrations, control.capacities, control.acquires)
	}
}

type auxRuntime struct {
	identities []*velav1.ModelRuntimeIdentity
}

func (runtime *auxRuntime) DiscoverRuntimeIdentities(
	_ context.Context,
	_ *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	response := &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{}
	for _, identity := range runtime.identities {
		response.Identities = append(
			response.Identities, proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
		)
	}
	return response, nil
}

func (runtime *auxRuntime) ProbeReadiness(
	_ context.Context,
	request *velav1.ModelRuntimeServiceProbeReadinessRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	for _, identity := range runtime.identities {
		if proto.Equal(identity, request.GetIdentity()) {
			return &velav1.ModelRuntimeServiceProbeReadinessResponse{
				Identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
				Check:    request.GetCheck(), Ready: true, Evidence: []byte("ready"),
			}, nil
		}
	}
	return nil, errors.New("unknown AUX runtime identity")
}

type auxProductionControl struct {
	primary       *velav1.ModelRuntimeIdentity
	registrations []*velav1.RegisterWorkerEvidenceRequest
	acquires      []*velav1.AcquireStageRequest
	capacities    []*velav1.ReportStageCapacityObservationRequest
}

func (control *auxProductionControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return make(chan *velav1.StageWorkerControlServiceConnectResponse)
}

func (control *auxProductionControl) Exchange(
	_ context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	switch operation := request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		control.registrations = append(control.registrations,
			proto.Clone(operation.RegisterWorkerEvidence).(*velav1.RegisterWorkerEvidenceRequest))
		return productionReadinessResponse(control.primary), nil
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		control.capacities = append(control.capacities,
			proto.Clone(operation.ReportCapacityObservation).(*velav1.ReportStageCapacityObservationRequest))
		return productionCapacityReadinessResponse(
			control.primary,
			operation.ReportCapacityObservation.GetObservationSequence(),
		), nil
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		control.acquires = append(control.acquires,
			proto.Clone(operation.AcquireStage).(*velav1.AcquireStageRequest))
		retry := 500 * time.Millisecond
		if len(control.acquires) == 2 {
			retry = 250 * time.Millisecond
		}
		return &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_NoWork{
				NoWork: &velav1.NoStageWork{RetryAfter: duration(retry)},
			},
		}, nil
	default:
		return nil, errors.New("unexpected AUX production control operation")
	}
}

type identityDiscoveryRuntime struct {
	identity *velav1.ModelRuntimeIdentity
	request  *velav1.ModelRuntimeServiceProbeReadinessRequest
}

func (runtime *identityDiscoveryRuntime) ProbeReadiness(
	_ context.Context,
	request *velav1.ModelRuntimeServiceProbeReadinessRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	runtime.request = proto.Clone(request).(*velav1.ModelRuntimeServiceProbeReadinessRequest)
	return &velav1.ModelRuntimeServiceProbeReadinessResponse{
		Identity: proto.Clone(runtime.identity).(*velav1.ModelRuntimeIdentity),
		Check:    request.GetCheck(), Detail: "resident identity disclosed; expectation required",
	}, nil
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
	identity                  *velav1.ModelRuntimeIdentity
	barrierGeneration         int64
	leaderMemberID            string
	operations                []string
	registration              *velav1.RegisterWorkerEvidenceRequest
	capacity                  *velav1.ReportStageCapacityObservationRequest
	capacities                []*velav1.ReportStageCapacityObservationRequest
	acquire                   *velav1.AcquireStageRequest
	capacityError             error
	registrationError         error
	controlSessionEpoch       int64
	capacityResponseSequences []int64
}

type productionExecutionControl struct {
	*materializingStreamControl
	identity             *velav1.ModelRuntimeIdentity
	heartbeatCalls       int
	heartbeatFailures    int
	reattachCalls        int
	assignment           *velav1.StageAssignment
	acquireCalls         int
	commands             <-chan *velav1.StageWorkerControlServiceConnectResponse
	observationSequences []int64
	failCalls            int
	failure              *velav1.FailStageRequest
}

func (control *productionExecutionControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return control.commands
}

func (control *productionExecutionControl) Exchange(
	ctx context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	switch request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		control.observationSequences = append(
			control.observationSequences,
			request.GetRegisterWorkerEvidence().GetCapacityObservationSequence(),
		)
		return productionReadinessResponse(control.identity), nil
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		return productionCapacityReadinessResponse(
			control.identity,
			request.GetReportCapacityObservation().GetObservationSequence(),
		), nil
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
	case *velav1.StageWorkerControlServiceConnectRequest_FailStage:
		control.failCalls++
		control.failure = proto.Clone(request.GetFailStage()).(*velav1.FailStageRequest)
		return commandResultResponse(
			velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_FAIL_STAGE,
			velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		), nil
	}
	return control.materializingStreamControl.Exchange(ctx, request)
}

type failureStatusRuntimeClient struct {
	velav1.ModelRuntimeServiceClient
	failure *velav1.ModelRuntimeFailureEvidence
}

func (client *failureStatusRuntimeClient) Status(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStatusRequest,
	options ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	response, err := client.ModelRuntimeServiceClient.Status(ctx, request, options...)
	if err != nil || response == nil {
		return response, err
	}
	response = proto.Clone(response).(*velav1.ModelRuntimeServiceStatusResponse)
	response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED
	response.FailureEvidence = proto.Clone(client.failure).(*velav1.ModelRuntimeFailureEvidence)
	return response, nil
}

type capacitySequenceSource struct {
	values        []int64
	index         int
	observed      []int64
	observeErrors map[int64]error
}

func (source *capacitySequenceSource) NextCapacityObservationSequence(
	context.Context,
) (int64, error) {
	if source.index >= len(source.values) {
		return 0, errors.New("test capacity sequences exhausted")
	}
	value := source.values[source.index]
	source.index++
	return value, nil
}

func (source *capacitySequenceSource) ObserveCapacityObservationSequence(
	_ context.Context,
	sequence int64,
) error {
	source.observed = append(source.observed, sequence)
	return source.observeErrors[sequence]
}

func (control *productionControl) CurrentControlSessionEpoch() int64 {
	return control.controlSessionEpoch
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
		if control.registrationError != nil {
			return nil, control.registrationError
		}
		control.registration = proto.Clone(operation.RegisterWorkerEvidence).(*velav1.RegisterWorkerEvidenceRequest)
		return productionBarrierReadinessResponse(
			control.identity, control.barrierGeneration, control.leaderMemberID,
		), nil
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		control.operations = append(control.operations, "capacity")
		if control.capacityError != nil {
			return nil, control.capacityError
		}
		control.capacity = proto.Clone(operation.ReportCapacityObservation).(*velav1.ReportStageCapacityObservationRequest)
		control.capacities = append(control.capacities, control.capacity)
		acceptedSequence := operation.ReportCapacityObservation.GetObservationSequence()
		responseIndex := len(control.capacities) - 1
		if responseIndex < len(control.capacityResponseSequences) {
			acceptedSequence = control.capacityResponseSequences[responseIndex]
		}
		return productionCapacityReadinessResponse(
			control.identity,
			acceptedSequence,
		), nil
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
	return productionBarrierReadinessResponse(
		identity, identity.GetModelRuntimeEpoch(), identity.GetWorkerMemberId(),
	)
}

func productionCapacityReadinessResponse(
	identity *velav1.ModelRuntimeIdentity,
	sequence int64,
) *velav1.StageWorkerControlServiceConnectResponse {
	response := productionReadinessResponse(identity)
	response.GetWorkerReadinessDecision().CapacityObservationSequence = sequence
	return response
}

func productionBarrierReadinessResponse(
	identity *velav1.ModelRuntimeIdentity,
	barrierGeneration int64,
	leaderMemberID string,
) *velav1.StageWorkerControlServiceConnectResponse {
	if barrierGeneration <= 0 {
		barrierGeneration = identity.GetModelRuntimeEpoch()
	}
	if leaderMemberID == "" {
		leaderMemberID = identity.GetWorkerMemberId()
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{
			WorkerReadinessDecision: &velav1.WorkerReadinessDecision{
				WorkerInstanceId:    identity.GetWorkerInstanceId(),
				WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(), Ready: true, Reason: "ready",
				ModelRuntimeBarrierGeneration: barrierGeneration,
				LeaderWorkerMemberId:          leaderMemberID,
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
