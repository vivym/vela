package modelruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestModelRuntimeRetiredAllocationCannotReenter(t *testing.T) {
	for _, terminal := range []string{"sealed", "stopped"} {
		t.Run(terminal, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			first := orderedRuntimeAuthority(t, signer, clock.Now(), 10)
			backend := modelruntime.NewFakeDiTRuntime()
			client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
			prepareAndStart(t, client, first)
			clock.Advance(time.Second)
			unseenRenewal := renewWatchdogAuthority(t, signer, first, clock.Now())
			finishOrderedRuntime(t, client, backend, first, terminal)
			assertRetiredRuntimeAuthority(t, client, first)
			assertRetiredRuntimeAuthority(t, client, unseenRenewal)

			next := orderedRuntimeAuthority(t, signer, clock.Now(), 11)
			next.StageRunId = first.GetStageRunId()
			next.Signature = nil
			next, err := signer.Sign(next)
			if err != nil {
				t.Fatal(err)
			}
			prepareAndStart(t, client, next)
			assertRetiredRuntimeAuthority(t, client, first)
			finishOrderedRuntime(t, client, backend, next, terminal)
			assertRetiredRuntimeAuthority(t, client, first)
			assertRetiredRuntimeAuthority(t, client, unseenRenewal)
		})
	}
}

func TestModelRuntimeRejectsLegacyExecutionWithoutAllocationOrder(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	legacy := signRuntimeAuthority(t, signer, clock.Now())
	legacy.SchemaVersion, legacy.ExecutionSequence, legacy.Signature = 1, 0, nil
	legacy, err := signer.Sign(legacy)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime()))
	assertRetiredRuntimeAuthority(t, client, legacy)
}

func TestModelRuntimeBusyHigherAllocationDoesNotConsumeOrder(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := modelruntime.NewFakeDiTRuntime()
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
	first := orderedRuntimeAuthority(t, signer, clock.Now(), 10)
	prepareAndStart(t, client, first)
	assertRetiredRuntimeAuthority(t, client, orderedRuntimeAuthority(t, signer, clock.Now(), 100))
	finishOrderedRuntime(t, client, backend, first, "sealed")
	prepareAndStart(t, client, orderedRuntimeAuthority(t, signer, clock.Now(), 11))
}

func TestModelRuntimeRetirementSurvivesSealedReceiptEviction(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := modelruntime.NewFakeDiTRuntime()
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
	var oldest *velav1.StageAuthority
	for sequence := int64(1); sequence <= 300; sequence++ {
		authority := orderedRuntimeAuthority(t, signer, clock.Now(), sequence)
		if sequence == 1 {
			oldest = proto.Clone(authority).(*velav1.StageAuthority)
		}
		prepareAndStart(t, client, authority)
		finishOrderedRuntime(t, client, backend, authority, "sealed")
	}
	assertRetiredRuntimeAuthority(t, client, oldest)
}

func TestModelRuntimeFailedPrepareConsumesOnlyItsAllocation(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := &rejectOneAllocationBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
	failed := orderedRuntimeAuthority(t, signer, clock.Now(), 10)
	prepared, err := client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: failed, ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("backend rejection: %v %v", prepared, err)
	}
	assertRetiredRuntimeAuthority(t, client, failed)
	prepareAndStart(t, client, orderedRuntimeAuthority(t, signer, clock.Now(), 11))
}

func TestModelRuntimeAllocationFenceSurvivesPersistentEpochRestart(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	store, err := modelruntime.NewFileEpochStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	newResident := func() (*modelruntime.Service, *modelruntime.FakeRuntime) {
		binding := runtimeBinding()
		binding.ModelRuntimeEpoch = 0
		backend := modelruntime.NewFakeDiTRuntime()
		service, err := modelruntime.NewService(modelruntime.Config{
			Binding: binding, EpochStore: store, EpochFloor: 8,
			Validator: validator, Clock: clock, Backend: backend, CancelTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(service.Close)
		return service, backend
	}
	firstService, firstBackend := newResident()
	firstClient, _ := serveRuntime(t, firstService)
	first := orderedRuntimeAuthority(t, signer, clock.Now(), 10)
	prepareAndStart(t, firstClient, first)
	finishOrderedRuntime(t, firstClient, firstBackend, first, "sealed")
	firstService.Close()
	secondService, _ := newResident()
	secondClient, _ := serveRuntime(t, secondService)
	assertRetiredRuntimeAuthority(t, secondClient, first)
	next := orderedRuntimeAuthority(t, signer, clock.Now(), 11)
	next.Members[0].ModelRuntimeEpoch = 10
	next.ModelRuntimeBarrierGeneration = 10
	next.Signature = nil
	next, err = signer.Sign(next)
	if err != nil {
		t.Fatal(err)
	}
	prepareAndStart(t, secondClient, next)
}

type rejectOneAllocationBackend struct{ *modelruntime.FakeRuntime }

func (backend *rejectOneAllocationBackend) Prepare(ctx context.Context, authority stageauthority.Verified, spec *velav1.StageExecutionSpec) error {
	if authority.Authority.GetExecutionSequence() == 10 {
		return errors.New("backend prepare failed")
	}
	return backend.FakeRuntime.Prepare(ctx, authority, spec)
}

func orderedRuntimeAuthority(t *testing.T, signer *stageauthority.Signer, now time.Time, sequence int64) *velav1.StageAuthority {
	t.Helper()
	authority := signRuntimeAuthority(t, signer, now)
	authority.SchemaVersion, authority.ExecutionSequence = 2, sequence
	authority.StageRunId, authority.StageAttemptId = uuid.NewString(), uuid.NewString()
	authority.StageAllocationId, authority.StageLeaseId = uuid.NewString(), uuid.NewString()
	authority.Signature = nil
	signed, err := signer.Sign(authority)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func assertRetiredRuntimeAuthority(t *testing.T, client velav1.ModelRuntimeServiceClient, authority *velav1.StageAuthority) {
	t.Helper()
	prepared, err := client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("retired or unavailable allocation Prepare = %v error=%v", prepared, err)
	}
	started, err := client.StartStage(context.Background(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("retired or unavailable allocation Start = %v error=%v", started, err)
	}
}

func finishOrderedRuntime(t *testing.T, client velav1.ModelRuntimeServiceClient, backend *modelruntime.FakeRuntime, authority *velav1.StageAuthority, terminal string) {
	t.Helper()
	ctx := context.Background()
	if terminal == "sealed" {
		backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
		sealed, err := client.SealOutput(ctx, &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
		if err != nil || sealed.GetReceipt() == nil {
			t.Fatalf("seal: %v %v", sealed, err)
		}
		return
	}
	canceled, err := client.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("cancel: %v %v", canceled, err)
	}
	backend.FinishStop()
	stopped, err := client.Status(ctx, &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
	if err != nil || stopped.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
		t.Fatalf("stop: %v %v", stopped, err)
	}
}
