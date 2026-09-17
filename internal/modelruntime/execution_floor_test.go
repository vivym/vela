package modelruntime_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecutionFloorRejectsLateCommandsAcrossResidentProfiles(t *testing.T) {
	fixture := newExecutionFloorFixture(t, "")
	first, unseen := fixture.authorities[0], fixture.authorities[1]
	prepareFloorRuntime(t, fixture.services[0], first)
	fixture.clock.Advance(time.Second)
	renewal := renewWatchdogAuthority(t, fixture.signer, first, fixture.clock.Now())
	installation, err := fixture.supervisor.InstallExecutionFloor(context.Background(), fixture.disposition(t))
	if err != nil || installation.Cutoff != 11 {
		t.Fatalf("install floor: %+v %v", installation, err)
	}
	if err := installation.WaitAcceptedOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := fixture.backend.calls.Load()
	for _, target := range []velav1.ModelRuntimeServiceServer{fixture.supervisor, fixture.services[0]} {
		assertFloorCommandsRejected(t, target, first)
		assertFloorCommandsRejected(t, target, renewal)
	}
	assertFloorCommandsRejected(t, fixture.supervisor, unseen)
	assertFloorCommandsRejected(t, fixture.services[1], unseen)
	client, _ := serveRuntimeServer(t, fixture.supervisor)
	prepared, err := client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: unseen, ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("transport bypassed floor: %v %v", prepared, err)
	}
	if fixture.backend.calls.Load() != before {
		t.Fatal("a rejected execution command entered the backend")
	}
	// Cancellation remains possible, but cannot install an unseen renewal.
	cancel := func(authority *velav1.StageAuthority) *velav1.ModelRuntimeServiceCancelStageResponse {
		response, callErr := fixture.supervisor.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
			Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
		})
		if callErr != nil {
			t.Fatal(callErr)
		}
		return response
	}
	if response := cancel(renewal); response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("cancel renewed a closed allocation: %v", response)
	}
	if response := cancel(first); !response.GetCancellationAcknowledged() {
		t.Fatalf("floor prevented exact cancellation: %v", response)
	}
	if fixture.backend.closed.Load() {
		t.Fatal("floor installation unloaded the resident backend")
	}
}

func TestExecutionFloorWaitsForPreviouslyAdmittedCallsWithoutBlockingInstallation(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status", "seal", "cancel", "watchdog"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newExecutionFloorFixture(t, operation)
			first := fixture.authorities[0]
			if operation != "prepare" {
				prepareFloorRuntime(t, fixture.supervisor, first)
			}
			if operation == "seal" {
				if err := runFloorOperation(fixture, "start"); err != nil {
					t.Fatal(err)
				}
				fixture.backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
			}
			completed := make(chan error, 1)
			go func() { completed <- runFloorOperation(fixture, operation) }()
			select {
			case <-fixture.backend.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("backend was not entered")
			}
			installed := make(chan *modelruntime.ExecutionFloorInstallation, 1)
			installErr := make(chan error, 1)
			disposition := fixture.disposition(t)
			go func() {
				checkpoint, err := fixture.supervisor.InstallExecutionFloor(context.Background(), disposition)
				installed <- checkpoint
				installErr <- err
			}()
			var checkpoint *modelruntime.ExecutionFloorInstallation
			select {
			case checkpoint = <-installed:
				if err := <-installErr; err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("backend held the shared admission lock")
			}
			waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if err := checkpoint.WaitAcceptedOperations(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("accepted backend call was forgotten: %v", err)
			}
			fixture.backend.unblock()
			select {
			case err := <-completed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("accepted operation did not return")
			}
			waitCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := checkpoint.WaitAcceptedOperations(waitCtx); err != nil {
				t.Fatal(err)
			}
			if fixture.backend.closed.Load() {
				t.Fatal("waiting for accepted calls unloaded the model")
			}
			assertFloorCommandsRejected(t, fixture.supervisor, fixture.authorities[1])
		})
	}
}

func TestExecutionFloorRejectsSignedScopeMismatchWithoutConsumingOrder(t *testing.T) {
	mutations := map[string]func(*velav1.StageTerminalDisposition){
		"worker":             func(d *velav1.StageTerminalDisposition) { d.WorkerInstanceId = uuid.NewString() },
		"worker epoch":       func(d *velav1.StageTerminalDisposition) { d.WorkerInstanceEpoch++ },
		"device set":         func(d *velav1.StageTerminalDisposition) { d.DeviceSetDigest[0] ^= 1 },
		"device id":          func(d *velav1.StageTerminalDisposition) { d.Devices[0].DeviceId = uuid.NewString() },
		"device epoch":       func(d *velav1.StageTerminalDisposition) { d.Devices[0].DeviceEpoch++ },
		"membership":         func(d *velav1.StageTerminalDisposition) { d.MembershipDigest[0] ^= 1 },
		"unseen runtime":     func(d *velav1.StageTerminalDisposition) { d.Allocations[1].ModelRuntimeIdentity = "unknown-runtime" },
		"unseen residency":   func(d *velav1.StageTerminalDisposition) { d.Allocations[1].ModelResidencyId = uuid.NewString() },
		"unseen profile":     func(d *velav1.StageTerminalDisposition) { d.Allocations[1].StageProfileRevisionId = uuid.NewString() },
		"unseen local epoch": func(d *velav1.StageTerminalDisposition) { d.Allocations[1].Members[0].ModelRuntimeEpoch++ },
		"identity digest": func(d *velav1.StageTerminalDisposition) {
			for _, allocation := range d.Allocations {
				allocation.Members[0].IdentityDigest[0] ^= 1
			}
		},
		"device subset": func(d *velav1.StageTerminalDisposition) {
			for _, allocation := range d.Allocations {
				allocation.Members[0].DeviceSubsetDigest[0] ^= 1
			}
		},
		"member epoch": func(d *velav1.StageTerminalDisposition) {
			for _, allocation := range d.Allocations {
				allocation.Members[0].MemberEpoch++
			}
		},
		"member id": func(d *velav1.StageTerminalDisposition) {
			d.WorkerMemberId = uuid.NewString()
			for _, allocation := range d.Allocations {
				allocation.Members[0].WorkerMemberId = d.WorkerMemberId
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			fixture := newExecutionFloorFixture(t, "")
			value := fixture.disposition(t)
			value.Cutoff, value.Allocations[1].ExecutionSequence = 100, 100
			mutate(value)
			signed, err := fixture.signer.SignTerminalDisposition(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.supervisor.InstallExecutionFloor(context.Background(), signed); !errors.Is(err, stageauthority.ErrRuntimeMismatch) {
				t.Fatalf("signed mismatched scope was not rejected: %v", err)
			}
			prepareFloorRuntime(t, fixture.supervisor, fixture.authorities[0])
		})
	}
}

func TestExecutionFloorIsMonotonicAndDoesNotDependOnRequestExpiry(t *testing.T) {
	fixture := newExecutionFloorFixture(t, "")
	high := fixture.disposition(t)
	high.Cutoff, high.Allocations[1].ExecutionSequence = 100, 100
	high, err := fixture.signer.SignTerminalDisposition(high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.supervisor.InstallExecutionFloor(context.Background(), high); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		checkpoint, err := fixture.supervisor.InstallExecutionFloor(context.Background(), fixture.disposition(t))
		if err != nil || checkpoint.Cutoff != 100 {
			t.Fatalf("floor regressed: %+v %v", checkpoint, err)
		}
	}
	fixture.clock.Advance(2 * time.Minute)
	if _, err := fixture.supervisor.InstallExecutionFloor(context.Background(), high); !errors.Is(err, stageauthority.ErrStale) {
		t.Fatalf("accepted expired floor request: %v", err)
	}
	for _, sequence := range []int64{11, 99, 100} {
		late := fixture.authority(t, 1, sequence)
		assertFloorCommandsRejected(t, fixture.services[1], late)
	}
	prepareFloorRuntime(t, fixture.supervisor, fixture.authority(t, 1, 101))
}

func TestExecutionFloorRejectsUnconfiguredInvalidAndCanceledInstallation(t *testing.T) {
	fixture := newExecutionFloorFixture(t, "")
	value := fixture.disposition(t)
	value.Signature[0] ^= 1
	if _, err := fixture.supervisor.InstallExecutionFloor(context.Background(), value); !errors.Is(err, stageauthority.ErrInvalidSignature) {
		t.Fatalf("accepted tampered signature: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fixture.supervisor.InstallExecutionFloor(ctx, fixture.disposition(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled installation: %v", err)
	}
	future := fixture.disposition(t)
	future.ObservedAt, future.ExpiresAt = timestamppb.New(fixture.clock.Now().Add(stageauthority.MaxTerminalObservationSkew+time.Nanosecond)), timestamppb.New(fixture.clock.Now().Add(time.Minute))
	future, err := fixture.signer.SignTerminalDisposition(future)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.supervisor.InstallExecutionFloor(context.Background(), future); !errors.Is(err, stageauthority.ErrStale) {
		t.Fatalf("accepted future observation: %v", err)
	}
	prepareFloorRuntime(t, fixture.supervisor, fixture.authorities[0])
	service := newRuntimeService(t, fixture.clock, fixture.validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime())
	ordinary, err := modelruntime.NewSupervisor(service)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ordinary.InstallExecutionFloor(context.Background(), fixture.disposition(t)); err == nil {
		t.Fatal("ordinary supervisor accepted floor without trusted membership")
	}
}

func TestExecutionFloorRejectsStartQueuedBehindPrepare(t *testing.T) {
	f := newExecutionFloorFixture(t, "prepare")
	prepared := make(chan error, 1)
	go func() { prepared <- runFloorOperation(f, "prepare") }()
	select {
	case <-f.backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Prepare did not enter backend")
	}
	started := make(chan *velav1.ModelRuntimeServiceStartStageResponse, 1)
	startErr := make(chan error, 1)
	go func() {
		response, err := f.services[0].StartStage(context.Background(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authorities[0]})
		started <- response
		startErr <- err
	}()
	if _, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	f.backend.unblock()
	if err := <-prepared; err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-started:
		if err := <-startErr; err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
			t.Fatalf("queued Start bypassed floor: %v %v", response, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued Start did not return")
	}
	if f.backend.calls.Load() != 1 {
		t.Fatal("queued Start entered backend")
	}
}

func TestSupervisorDirectCallsShareSlotAndProfileRetirementOrder(t *testing.T) {
	for _, terminal := range []string{"sealed", "stopped"} {
		t.Run(terminal, func(t *testing.T) {
			f := newExecutionFloorFixture(t, "")
			client, _ := serveRuntimeServer(t, f.supervisor)
			first := f.authorities[0]
			prepareAndStart(t, client, first)
			busy, err := f.services[1].PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
				Authority: f.authority(t, 1, 100), ExecutionSpec: runtimeExecutionSpec(),
			})
			if err != nil || busy.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("direct call bypassed shared slot: %v %v", busy, err)
			}
			finishOrderedRuntime(t, client, f.backend.FakeRuntime, first, terminal)
			// Busy 100 did not consume the order; reusable 10 relinquished the slot.
			prepareAndStart(t, client, f.authorities[1])
			finishOrderedRuntime(t, client, f.otherBackend, f.authorities[1], terminal)
			oldOtherProfile := f.authority(t, 0, 11)
			assertRetiredRuntimeAuthority(t, client, oldOtherProfile)
			prepareFloorRuntime(t, f.services[0], f.authority(t, 0, 12))
		})
	}
}

func TestSupervisorRejectsReplacingUsedOrAttachedAdmission(t *testing.T) {
	f := newExecutionFloorFixture(t, "")
	if _, err := modelruntime.NewSupervisor(f.services...); err == nil {
		t.Fatal("supervisor attachment reset another supervisor's admission gate")
	}
	first := newRuntimeService(t, f.clock, f.validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime())
	second := newRuntimeService(t, f.clock, f.validator, f.bindings[1], modelruntime.NewFakeVAERuntime())
	prepareFloorRuntime(t, first, f.authorities[0])
	if _, err := modelruntime.NewSupervisor(first, second); err == nil {
		t.Fatal("supervisor construction reset a used standalone gate")
	}
	if _, err := modelruntime.NewSupervisor(second); err != nil {
		t.Fatalf("failed constructor partially attached unused service: %v", err)
	}
}

func TestExecutionFloorRejectsIncompleteTrustedConfiguration(t *testing.T) {
	f := newExecutionFloorFixture(t, "")
	for _, name := range []string{"verifier", "missing member", "member id", "member epoch", "identity", "subset"} {
		t.Run(name, func(t *testing.T) {
			config := modelruntime.ExecutionFloorConfig{Validator: f.validator, Members: []modelruntime.ExecutionFloorMember{{
				WorkerMemberID: f.bindings[0].WorkerMemberID, MemberEpoch: f.bindings[0].WorkerMemberEpoch,
				IdentityDigest: bytes.Repeat([]byte{0x66}, 32), DeviceSubsetDigest: bytes.Repeat([]byte{0x67}, 32),
			}}}
			switch name {
			case "verifier":
				config.Validator = nil
			case "missing member":
				config.Members = nil
			case "member id":
				config.Members[0].WorkerMemberID = uuid.NewString()
			case "member epoch":
				config.Members[0].MemberEpoch++
			case "identity":
				config.Members[0].IdentityDigest = nil
			case "subset":
				config.Members[0].DeviceSubsetDigest = nil
			}
			service := newRuntimeService(t, f.clock, f.validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime())
			if _, err := modelruntime.NewSupervisorWithExecutionFloor(config, service); err == nil {
				t.Fatal("incomplete trusted configuration accepted")
			}
			if _, err := modelruntime.NewSupervisor(service); err != nil {
				t.Fatalf("failed configuration changed service admission: %v", err)
			}
		})
	}
}

type executionFloorFixture struct {
	clock        *manualClock
	signer       *stageauthority.Signer
	validator    *stageauthority.Validator
	bindings     []stageauthority.RuntimeBinding
	services     []*modelruntime.Service
	supervisor   *modelruntime.Supervisor
	backend      *floorBlockingBackend
	otherBackend *modelruntime.FakeRuntime
	authorities  []*velav1.StageAuthority
}

func newExecutionFloorFixture(t *testing.T, blocked string) *executionFloorFixture {
	t.Helper()
	f, err := executionFloorFixtureWithState(t, blocked, nil, 9, time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func executionFloorFixtureWithState(t *testing.T, blocked string, state *modelruntime.ExecutionFloorStateConfig, epoch int64, now time.Time) (*executionFloorFixture, error) {
	t.Helper()
	f := &executionFloorFixture{clock: newManualClock(now)}
	f.signer, f.validator = runtimeAuthorityCrypto(t, f.clock)
	f.bindings = []stageauthority.RuntimeBinding{runtimeBinding(), runtimeBinding()}
	f.bindings[0].ModelRuntimeEpoch, f.bindings[1].ModelRuntimeEpoch = epoch, epoch
	f.bindings[1].ModelResidencyID = "51000000-0000-0000-0000-000000000012"
	f.bindings[1].ModelRuntimeIdentity = "h3-vae-runtime-1"
	f.bindings[1].StageProfileRevisionID = "61000000-0000-0000-0000-000000000012"
	f.backend = &floorBlockingBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), blocked: blocked, entered: make(chan struct{}), resume: make(chan struct{})}
	f.otherBackend = modelruntime.NewFakeVAERuntime()
	f.services = []*modelruntime.Service{
		newRuntimeService(t, f.clock, f.validator, f.bindings[0], f.backend),
		newRuntimeService(t, f.clock, f.validator, f.bindings[1], f.otherBackend),
	}
	// Unblock before Service cleanup, including after a failed assertion.
	t.Cleanup(f.backend.unblock)
	config := modelruntime.ExecutionFloorConfig{Validator: f.validator, State: state, Members: []modelruntime.ExecutionFloorMember{{
		WorkerMemberID: f.bindings[0].WorkerMemberID, MemberEpoch: f.bindings[0].WorkerMemberEpoch,
		IdentityDigest: bytes.Repeat([]byte{0x66}, 32), DeviceSubsetDigest: bytes.Repeat([]byte{0x67}, 32),
	}}}
	var err error
	f.supervisor, err = modelruntime.NewSupervisorWithExecutionFloor(config, f.services...)
	if err != nil {
		return nil, err
	}
	t.Cleanup(f.supervisor.Close)
	// The constructor must own copies of trusted digests.
	config.Members[0].IdentityDigest[0] ^= 1
	config.Members[0].DeviceSubsetDigest[0] ^= 1
	f.authorities = []*velav1.StageAuthority{f.authority(t, 0, 10), f.authority(t, 1, 11)}
	return f, nil
}

func (f *executionFloorFixture) authority(t *testing.T, profile int, sequence int64) *velav1.StageAuthority {
	t.Helper()
	a := signSupervisorAuthority(t, f.signer, f.clock.Now(), f.bindings[profile], "11")
	a.StageAttemptId, a.StageAllocationId, a.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
	a.ExecutionSequence = sequence
	// The barrier generation deliberately differs from the local Runtime epoch.
	a.ModelRuntimeBarrierGeneration = 73
	a, err := f.signer.Sign(a)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (f *executionFloorFixture) disposition(t *testing.T) *velav1.StageTerminalDisposition {
	t.Helper()
	a := f.authorities[0]
	digest, err := stageauthority.Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	d := &velav1.StageTerminalDisposition{
		SchemaVersion: 1, InputDisposition: velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED,
		OriginalAuthorityDigest: digest[:], OrganizationId: uuid.NewString(), ProjectId: uuid.NewString(),
		JobId: a.GetJobId(), AttemptId: a.GetAttemptId(), StageRunId: a.GetStageRunId(),
		StageAttemptId: a.GetStageAttemptId(), StageAllocationId: a.GetStageAllocationId(), StageLeaseId: a.GetStageLeaseId(),
		TerminalState: velav1.StageTerminalState_STAGE_TERMINAL_STATE_FAILED, StageFence: a.GetStageFence() + 1, StageVersion: a.GetStageVersion() + 1,
		WorkerInstanceId: a.GetWorkerInstanceId(), WorkerInstanceEpoch: a.GetWorkerInstanceEpoch(),
		WorkerMemberId: a.GetMembers()[0].GetWorkerMemberId(), ControlSessionEpoch: 7,
		DeviceSetDigest: bytes.Clone(a.GetDeviceSetDigest()), MembershipDigest: bytes.Clone(a.GetMembershipDigest()),
		Devices: []*velav1.StageAuthorityDeviceEpoch{proto.Clone(a.GetDevices()[0]).(*velav1.StageAuthorityDeviceEpoch)}, Cutoff: 11,
		ObservedAt: timestamppb.New(f.clock.Now()), ExpiresAt: timestamppb.New(f.clock.Now().Add(time.Minute)), SigningKeyId: a.GetSigningKeyId(),
	}
	for _, authority := range f.authorities {
		allocation := &velav1.StageTerminalAllocation{
			StageAttemptId: authority.GetStageAttemptId(), StageAllocationId: authority.GetStageAllocationId(), StageLeaseId: authority.GetStageLeaseId(),
			ExecutionSequence: authority.GetExecutionSequence(), ExecutionNonce: bytes.Clone(authority.GetExecutionNonce()),
			ModelResidencyId: authority.GetModelResidencyId(), ModelRuntimeIdentity: authority.GetModelRuntimeIdentity(),
			StageProfileRevisionId: authority.GetStageProfileRevisionId(), BarrierGeneration: authority.GetModelRuntimeBarrierGeneration(),
		}
		for _, member := range authority.GetMembers() {
			allocation.Members = append(allocation.Members, &velav1.StageTerminalMember{
				WorkerMemberId: member.GetWorkerMemberId(), MemberEpoch: member.GetMemberEpoch(), ModelRuntimeEpoch: member.GetModelRuntimeEpoch(),
				IdentityDigest: bytes.Clone(member.GetIdentityDigest()), DeviceSubsetDigest: bytes.Repeat([]byte{0x67}, 32),
			})
		}
		d.Allocations = append(d.Allocations, allocation)
	}
	d, err = f.signer.SignTerminalDisposition(d)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func prepareFloorRuntime(t *testing.T, server velav1.ModelRuntimeServiceServer, authority *velav1.StageAuthority) {
	t.Helper()
	response, err := server.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare: %v %v", response, err)
	}
}

func assertFloorCommandsRejected(t *testing.T, server velav1.ModelRuntimeServiceServer, authority *velav1.StageAuthority) {
	t.Helper()
	ctx := context.Background()
	prepared, err := server.PrepareStage(ctx, &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("prepare bypassed floor: %v %v", prepared, err)
	}
	started, err := server.StartStage(ctx, &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("start bypassed floor: %v %v", started, err)
	}
	status, err := server.Status(ctx, &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
	if err != nil || status.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("status bypassed floor: %v %v", status, err)
	}
	sealed, err := server.SealOutput(ctx, &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
	if err != nil || sealed.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("seal bypassed floor: %v %v", sealed, err)
	}
}

func runFloorOperation(f *executionFloorFixture, operation string) error {
	ctx, a := context.Background(), f.authorities[0]
	var decision velav1.ModelRuntimeCommandDecision
	var err error
	switch operation {
	case "prepare":
		var response *velav1.ModelRuntimeServicePrepareStageResponse
		response, err = f.supervisor.PrepareStage(ctx, &velav1.ModelRuntimeServicePrepareStageRequest{Authority: a, ExecutionSpec: runtimeExecutionSpec()})
		decision = response.GetDecision()
	case "start":
		var response *velav1.ModelRuntimeServiceStartStageResponse
		response, err = f.supervisor.StartStage(ctx, &velav1.ModelRuntimeServiceStartStageRequest{Authority: a})
		decision = response.GetDecision()
	case "status":
		var response *velav1.ModelRuntimeServiceStatusResponse
		response, err = f.supervisor.Status(ctx, &velav1.ModelRuntimeServiceStatusRequest{Authority: a})
		decision = response.GetDecision()
	case "seal":
		var response *velav1.ModelRuntimeServiceSealOutputResponse
		response, err = f.supervisor.SealOutput(ctx, &velav1.ModelRuntimeServiceSealOutputRequest{Authority: a})
		decision = response.GetDecision()
	case "cancel":
		var response *velav1.ModelRuntimeServiceCancelStageResponse
		response, err = f.supervisor.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{Authority: a, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
		decision = response.GetDecision()
	case "watchdog":
		f.clock.Advance(31 * time.Second)
		return nil
	}
	if err != nil {
		return err
	}
	if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		return errors.New("previously admitted operation did not complete")
	}
	return nil
}

type floorBlockingBackend struct {
	*modelruntime.FakeRuntime
	blocked        string
	entered        chan struct{}
	resume         chan struct{}
	enterOnce      sync.Once
	resumeOnce     sync.Once
	calls          atomic.Int64
	closed         atomic.Bool
	prepareContext context.Context
}

func (b *floorBlockingBackend) wait(operation string) {
	b.calls.Add(1)
	if operation == b.blocked {
		b.enterOnce.Do(func() { close(b.entered) })
		<-b.resume
	}
}
func (b *floorBlockingBackend) unblock()     { b.resumeOnce.Do(func() { close(b.resume) }) }
func (b *floorBlockingBackend) Close() error { b.closed.Store(true); b.unblock(); return nil }
func (b *floorBlockingBackend) Prepare(ctx context.Context, a stageauthority.Verified, spec *velav1.StageExecutionSpec) error {
	b.prepareContext = ctx
	b.wait("prepare")
	return b.FakeRuntime.Prepare(ctx, a, spec)
}
func (b *floorBlockingBackend) Start(ctx context.Context, a stageauthority.Verified) error {
	b.wait("start")
	return b.FakeRuntime.Start(ctx, a)
}
func (b *floorBlockingBackend) Status(ctx context.Context, a stageauthority.Verified) (modelruntime.BackendStatus, error) {
	b.wait("status")
	return b.FakeRuntime.Status(ctx, a)
}
func (b *floorBlockingBackend) Seal(ctx context.Context, a stageauthority.Verified) (modelruntime.SealedOutput, error) {
	b.wait("seal")
	return b.FakeRuntime.Seal(ctx, a)
}
func (b *floorBlockingBackend) Cancel(ctx context.Context, a stageauthority.Verified, reason velav1.ModelRuntimeCancelReason) error {
	if reason == velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE {
		b.wait("watchdog")
	} else {
		b.wait("cancel")
	}
	return b.FakeRuntime.Cancel(ctx, a, reason)
}
