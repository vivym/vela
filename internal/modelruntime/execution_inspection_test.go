package modelruntime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestExecutionInspectionCannotRenewOrProgressCancellation(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := modelruntime.NewFakeDiTRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	assertInspection(t, client, authority, false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
	prepareAndStart(t, client, authority)
	clock.Advance(time.Second)
	renewed := renewWatchdogAuthority(t, signer, authority, clock.Now())
	assertInspection(t, client, renewed, false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
	assertInspection(t, client, authority, true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING)
	clock.mu.Lock()
	timers := len(clock.timers)
	clock.mu.Unlock()
	if timers != 1 {
		t.Fatalf("read-only inspection installed a watchdog: %d timers", timers)
	}
	clock.Advance(31 * time.Second)
	canceled, err := client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("exact expired cancellation lost its authority: %v %v", canceled, err)
	}
	for range 3 {
		assertInspection(t, client, authority, true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING)
	}
	backend.FinishStop()
	assertInspection(t, client, authority, true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED)
	// An observation must not update Service state or release its execution slot.
	next := orderedRuntimeAuthority(t, signer, clock.Now(), authority.GetExecutionSequence()+1)
	prepared, err := client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: next, ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("inspection changed execution admission: %v %v", prepared, err)
	}
	stale, err := client.Status(context.Background(), &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
	if err != nil || stale.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("historical inspection relaxed ordinary Status: %v %v", stale, err)
	}
}

func TestExecutionInspectionAfterFloorPreservesResidentAdmission(t *testing.T) {
	f := newExecutionFloorFixture(t, "")
	authority := f.authorities[0]
	prepareFloorRuntime(t, f.supervisor, authority)
	installation, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := installation.WaitAcceptedOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, _ := serveRuntimeServer(t, f.supervisor)
	for range 2 {
		assertInspection(t, client, authority, true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED)
	}
	assertFloorCommandsRejected(t, f.supervisor, authority)
	if f.backend.closed.Load() {
		t.Fatal("inspection unloaded the resident backend")
	}
}

func TestExecutionInspectionMissingSupersededAndEvictedHistoryIsUnknown(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := modelruntime.NewFakeDiTRuntime()
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
	first := orderedRuntimeAuthority(t, signer, clock.Now(), 1)
	prepareAndStart(t, client, first)
	clock.Advance(time.Second)
	renewed := renewWatchdogAuthority(t, signer, first, clock.Now())
	assertWatchdogRuntimeState(t, client, renewed, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING)
	assertInspection(t, client, first, false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
	assertInspection(t, client, renewed, true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING)
	sealInspectionExecution(t, client, backend, renewed)
	assertInspection(t, client, renewed, true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED)
	var latest *velav1.StageAuthority
	for sequence := int64(2); sequence <= 257; sequence++ {
		latest = orderedRuntimeAuthority(t, signer, clock.Now(), sequence)
		prepareAndStart(t, client, latest)
		sealInspectionExecution(t, client, backend, latest)
	}
	clock.Advance(time.Minute)
	assertInspection(t, client, renewed, false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
	assertInspection(t, client, latest, true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED)
	// Even a missing record at the same epoch cannot be interpreted as stopped.
	restarted, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime()))
	assertInspection(t, restarted, latest, false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
	binding := runtimeBinding()
	binding.ModelRuntimeEpoch++
	newEpoch, _ := serveRuntime(t, newRuntimeService(t, clock, validator, binding, modelruntime.NewFakeDiTRuntime()))
	response, err := newEpoch.InspectExecution(context.Background(), inspectionRequest(latest))
	if err != nil || response.GetKnown() || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED ||
		response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("new epoch supplied historical execution evidence: %v %v", response, err)
	}
}

func TestExecutionInspectionDoesNotFallBackToStatus(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := &inspectionStatusCounter{Backend: modelruntime.NewFakeDiTRuntime()}
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
	authority := signRuntimeAuthority(t, signer, clock.Now())
	prepareAndStart(t, client, authority)
	response, err := client.InspectExecution(context.Background(), inspectionRequest(authority))
	if err != nil || response.GetKnown() || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED ||
		response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || backend.calls.Load() != 0 {
		t.Fatalf("unsupported inspector fell back to Status: %v %v calls=%d", response, err, backend.calls.Load())
	}
}

func TestExecutionInspectionRejectsInvalidRequestsAndBackendEvidence(t *testing.T) {
	for _, mutation := range []string{"nil request", "schema", "unknown fields", "signature", "future issue", "unknown key"} {
		t.Run(mutation, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			backend := &fixedInspectionBackend{Backend: modelruntime.NewFakeDiTRuntime()}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			authority := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, authority)
			request := inspectionRequest(proto.Clone(authority).(*velav1.StageAuthority))
			switch mutation {
			case "nil request":
				request = nil
			case "schema":
				request.SchemaVersion++
			case "unknown fields":
				request.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "signature":
				request.Authority.Signature[0] ^= 1
			case "future issue":
				request.Authority = signRuntimeAuthority(t, signer, clock.Now().Add(time.Minute))
			case "unknown key":
				request.Authority.SigningKeyId = "missing"
			}
			response, err := service.InspectExecution(context.Background(), request)
			if err != nil || response.GetKnown() || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED ||
				response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || backend.calls.Load() != 0 {
				t.Fatalf("invalid request reached inspection: %v %v calls=%d", response, err, backend.calls.Load())
			}
		})
	}
	for _, value := range []modelruntime.ExecutionInspection{
		{Known: false, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED},
		{Known: false, Sequence: 1}, {Known: true}, {Known: true, State: 99},
		{Known: true, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED, Sequence: -1},
	} {
		clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
		signer, validator := runtimeAuthorityCrypto(t, clock)
		backend := &fixedInspectionBackend{Backend: modelruntime.NewFakeDiTRuntime(), result: value}
		client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
		authority := signRuntimeAuthority(t, signer, clock.Now())
		// Never consult a backend, even one reporting STOPPED, for missing history.
		assertInspection(t, client, authority, false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
		if backend.calls.Load() != 0 {
			t.Fatal("missing Service history was delegated to backend")
		}
		prepareAndStart(t, client, authority)
		response, err := client.InspectExecution(context.Background(), inspectionRequest(authority))
		if err != nil || response.GetKnown() || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED ||
			response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
			t.Fatalf("malformed backend evidence escaped: %v %v", response, err)
		}
	}
}

func TestExecutionInspectionDropsLateSuccessAndBackendErrors(t *testing.T) {
	for _, failure := range []string{"late success", "backend error"} {
		t.Run(failure, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			backend := &fixedInspectionBackend{Backend: modelruntime.NewFakeDiTRuntime(), result: modelruntime.ExecutionInspection{
				Known: true, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED,
			}}
			if failure == "late success" {
				backend.after = cancel
			} else {
				backend.err = errors.New("backend state unavailable")
			}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			authority := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, authority)
			response, err := service.InspectExecution(ctx, inspectionRequest(authority))
			if failure == "late success" {
				if response != nil || !errors.Is(err, context.Canceled) {
					t.Fatalf("late success escaped canceled query: %v %v", response, err)
				}
			} else if err != nil || response.GetKnown() || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("backend error produced observation: %v %v", response, err)
			}
		})
	}
}

func TestExecutionInspectionDoesNotHoldCancellationOrWatchdog(t *testing.T) {
	for _, operation := range []string{"cancel", "watchdog"} {
		t.Run(operation, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			fake := modelruntime.NewFakeDiTRuntime()
			backend := &waitingInspectionBackend{Backend: fake, entered: make(chan struct{})}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			authority := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, authority)
			ctx, cancel := context.WithCancel(context.Background())
			finished := make(chan error, 1)
			go func() {
				_, err := service.InspectExecution(ctx, inspectionRequest(authority))
				finished <- err
			}()
			t.Cleanup(func() {
				cancel()
				if err := <-finished; !errors.Is(err, context.Canceled) {
					t.Errorf("blocked inspection completion: %v", err)
				}
			})
			select {
			case <-backend.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("inspection did not reach backend")
			}
			if operation == "cancel" {
				callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
				defer callCancel()
				response, err := client.CancelStage(callCtx, &velav1.ModelRuntimeServiceCancelStageRequest{
					Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				})
				if err != nil || !response.GetCancellationAcknowledged() {
					t.Fatalf("inspection held cancellation: %v %v", response, err)
				}
			} else {
				clock.Advance(time.Minute)
			}
			verified, err := validator.ValidateSignature(authority, runtimeBinding())
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for {
				observed, err := fake.InspectExecution(context.Background(), verified)
				if err == nil && observed.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("inspection held %s: %+v %v", operation, observed, err)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

type waitingInspectionBackend struct {
	modelruntime.Backend
	entered chan struct{}
}

func (backend *waitingInspectionBackend) InspectExecution(ctx context.Context, _ stageauthority.Verified) (modelruntime.ExecutionInspection, error) {
	close(backend.entered)
	<-ctx.Done()
	return modelruntime.ExecutionInspection{}, ctx.Err()
}

type inspectionStatusCounter struct {
	modelruntime.Backend
	calls atomic.Int64
}

func (backend *inspectionStatusCounter) Status(ctx context.Context, authority stageauthority.Verified) (modelruntime.BackendStatus, error) {
	backend.calls.Add(1)
	return backend.Backend.Status(ctx, authority)
}

type fixedInspectionBackend struct {
	modelruntime.Backend
	result modelruntime.ExecutionInspection
	err    error
	after  func()
	calls  atomic.Int64
}

func (backend *fixedInspectionBackend) InspectExecution(context.Context, stageauthority.Verified) (modelruntime.ExecutionInspection, error) {
	backend.calls.Add(1)
	if backend.after != nil {
		backend.after()
	}
	return backend.result, backend.err
}

func inspectionRequest(authority *velav1.StageAuthority) *velav1.ModelRuntimeServiceInspectExecutionRequest {
	return &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: authority}
}

func assertInspection(t *testing.T, client velav1.ModelRuntimeServiceClient, authority *velav1.StageAuthority, known bool, state velav1.ModelRuntimeExecutionState) {
	t.Helper()
	response, err := client.InspectExecution(context.Background(), inspectionRequest(authority))
	digest, digestErr := stageauthority.Digest(authority)
	if err != nil || digestErr != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		response.GetKnown() != known || response.GetState() != state ||
		modelruntimetransport.ValidateExecutionInspectionResponse(digest, inspectionIdentity(authority), response) != nil {
		t.Fatalf("InspectExecution = %v error=%v, want known=%v state=%s", response, err, known, state)
	}
}

func inspectionIdentity(authority *velav1.StageAuthority) *velav1.ModelRuntimeIdentity {
	member := authority.GetMembers()[0]
	return &velav1.ModelRuntimeIdentity{
		WorkerInstanceId: authority.GetWorkerInstanceId(), WorkerInstanceEpoch: authority.GetWorkerInstanceEpoch(),
		WorkerMemberId: member.GetWorkerMemberId(), WorkerMemberEpoch: member.GetMemberEpoch(),
		DeviceSetDigest: authority.GetDeviceSetDigest(), MembershipDigest: authority.GetMembershipDigest(),
		ModelResidencyId: authority.GetModelResidencyId(), RuntimeIdentity: authority.GetModelRuntimeIdentity(),
		ModelRuntimeEpoch: member.GetModelRuntimeEpoch(), StageProfileRevisionId: authority.GetStageProfileRevisionId(),
	}
}

func sealInspectionExecution(t *testing.T, client velav1.ModelRuntimeServiceClient, backend *modelruntime.FakeRuntime, authority *velav1.StageAuthority) {
	t.Helper()
	backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
	sealed, err := client.SealOutput(context.Background(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
	if err != nil || sealed.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("seal inspection fixture: %v %v", sealed, err)
	}
}
