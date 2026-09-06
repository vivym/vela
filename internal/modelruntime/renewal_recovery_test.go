package modelruntime_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestModelRuntimeRecoversCancellationAfterUnacknowledgedRenewal(t *testing.T) {
	for _, outcome := range []string{"not-applied", "applied-response-lost"} {
		t.Run(outcome, func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), applyRenewal: outcome == "applied-response-lost",
				failStatus: true, calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			renewed := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			status, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed})
			if err != nil || status.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("injected renewal response loss was not observed: %v %v", status, err)
			}
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			canceled, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: renewed, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			})
			if err != nil || !canceled.GetCancellationAcknowledged() {
				t.Fatalf("unacknowledged renewal prevented exact cancellation recovery: %v %v", canceled, err)
			}
			actual := first
			if backend.applyRenewal {
				actual = renewed
			}
			call := <-backend.calls
			if !proto.Equal(call.authority, actual) {
				t.Fatal("cancellation did not use the backend-observed authority")
			}
			read, err := f.supervisor.InspectExecution(t.Context(), inspectionRequest(actual))
			if err != nil || !read.GetKnown() || read.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
				t.Fatalf("recovery lost the actual backend identity: %v %v", read, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, actual, false)
			backend.FinishStop()
			checkpoint, err := f.supervisor.DrainExecution(t.Context(), actual)
			if err != nil || checkpoint == nil || !proto.Equal(checkpoint.Authority, actual) {
				t.Fatalf("recovered exact cancellation could not retain original drain evidence: %+v %v", checkpoint, err)
			}
			allocation, err := f.supervisor.InspectAllocationDrain(t.Context(), renewed)
			if err != nil || allocation == nil || !proto.Equal(allocation.Authority, actual) {
				t.Fatalf("allocation lookup rewrote the actual drain identity: %+v %v", allocation, err)
			}
		})
	}
}

type renewalRecoveryBackend struct {
	*modelruntime.FakeRuntime
	applyRenewal    bool
	failStatus      bool
	calls           chan cancellationAuthorityCall
	inspectFault    atomic.Int32
	statusCalls     int
	inspected       chan struct{}
	failPrepare     bool
	reportedFailure *modelruntime.BackendStatus
}

func (backend *renewalRecoveryBackend) Prepare(ctx context.Context, authority stageauthority.Verified, spec *velav1.StageExecutionSpec) error {
	if err := backend.FakeRuntime.Prepare(ctx, authority, spec); err != nil {
		return err
	}
	if backend.failPrepare {
		return errors.New("injected partial preparation failure")
	}
	return nil
}

func (backend *renewalRecoveryBackend) Status(ctx context.Context, authority stageauthority.Verified) (modelruntime.BackendStatus, error) {
	backend.statusCalls++
	if backend.reportedFailure != nil {
		return *backend.reportedFailure, nil
	}
	if !backend.failStatus {
		return backend.FakeRuntime.Status(ctx, authority)
	}
	if backend.applyRenewal {
		if _, err := backend.FakeRuntime.Status(ctx, authority); err != nil {
			return modelruntime.BackendStatus{}, err
		}
	}
	return modelruntime.BackendStatus{}, errors.New("injected renewal response loss")
}

func (backend *renewalRecoveryBackend) InspectExecution(ctx context.Context, authority stageauthority.Verified) (modelruntime.ExecutionInspection, error) {
	fault := backend.inspectFault.Load()
	if backend.inspected != nil {
		select {
		case backend.inspected <- struct{}{}:
		default:
		}
	}
	switch fault {
	case 1:
		return modelruntime.ExecutionInspection{}, errors.New("injected inspection failure")
	case 2:
		return modelruntime.ExecutionInspection{}, nil
	case 3:
		return modelruntime.ExecutionInspection{Known: true, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED}, nil
	case 4:
		return modelruntime.ExecutionInspection{Known: true, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED, Sequence: -1}, nil
	case 5:
		<-ctx.Done()
		return modelruntime.ExecutionInspection{}, ctx.Err()
	}
	return backend.FakeRuntime.InspectExecution(ctx, authority)
}

func TestModelRuntimeUncertainRenewalRequiresUnambiguousInspection(t *testing.T) {
	for index, fault := range []string{"error", "unknown", "both-known", "invalid", "timeout"} {
		t.Run(fault, func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: true, calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			f.clock.Advance(time.Second)
			renewed := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal did not fail: %v %v", response, err)
			}
			backend.inspectFault.Store(int32(index + 1))
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			response, err := f.supervisor.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: renewed, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			})
			cancel()
			if err != nil || response.GetCancellationAcknowledged() || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || len(backend.calls) != 0 {
				t.Fatalf("uncertain inspection selected a cancellation target: %v %v", response, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, renewed, false)
			backend.inspectFault.Store(0)
			response, err = f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: renewed, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			})
			if err != nil || !response.GetCancellationAcknowledged() || !proto.Equal((<-backend.calls).authority, f.authorities[0]) {
				t.Fatalf("failed inspection poisoned later exact recovery: %v %v", response, err)
			}
		})
	}
}

func TestModelRuntimeUncertainRenewalCannotForgetPreviousBackendIdentity(t *testing.T) {
	backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: true, calls: make(chan cancellationAuthorityCall, 2)}
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	f.clock.Advance(time.Second)
	second := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
	if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: second}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("renewal did not fail: %v %v", response, err)
	}
	f.clock.Advance(time.Second)
	third := renewWatchdogAuthority(t, f.signer, second, f.clock.Now())
	if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: third}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || backend.statusCalls != 1 {
		t.Fatalf("unconfirmed renewal was overwritten by another candidate: %v %v", response, err)
	}
	read, err := f.supervisor.InspectExecution(t.Context(), inspectionRequest(f.authorities[0]))
	if err != nil || !read.GetKnown() {
		t.Fatalf("unconfirmed renewal hid the previous observed authority: %v %v", read, err)
	}
	backend.failStatus = false
	for _, authority := range []*velav1.StageAuthority{second, third} {
		response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
		if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("confirmed Status did not resume normal renewal: %v %v", response, err)
		}
	}
	read, err = f.supervisor.InspectExecution(t.Context(), inspectionRequest(f.authorities[0]))
	if err != nil || read.GetKnown() {
		t.Fatalf("confirmed renewal retained a superseded identity: %v %v", read, err)
	}
}

func TestModelRuntimeWatchdogReconcilesRenewalAndPermitsFailedRetry(t *testing.T) {
	for _, failInspection := range []bool{false, true} {
		t.Run(map[bool]string{false: "observed", true: "retry-failed-inspection"}[failInspection], func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: true,
				calls: make(chan cancellationAuthorityCall, 2), inspected: make(chan struct{}, 4)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			f.clock.Advance(time.Second)
			renewed := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal did not fail: %v %v", response, err)
			}
			if failInspection {
				backend.inspectFault.Store(1)
			}
			f.clock.Advance(time.Minute)
			select {
			case <-backend.inspected:
			case <-time.After(2 * time.Second):
				t.Fatal("watchdog did not inspect uncertain backend authority")
			}
			wantReason := velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE
			if failInspection {
				backend.inspectFault.Store(0)
				response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
					Authority: renewed, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				})
				if err != nil || !response.GetCancellationAcknowledged() {
					t.Fatalf("failed watchdog inspection prevented exact retry: %v %v", response, err)
				}
				wantReason = velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP
			}
			select {
			case call := <-backend.calls:
				if !proto.Equal(call.authority, f.authorities[0]) || call.reason != wantReason {
					t.Fatalf("recovery changed actual execution identity: %+v", call)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("watchdog/retry did not reach original backend execution")
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, renewed, false)
		})
	}
}

func TestModelRuntimeRenewedPrepareAndStartConfirmBackendAuthority(t *testing.T) {
	for _, operation := range []string{"prepare-replay", "start", "start-replay"} {
		t.Run(operation, func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			if operation == "start-replay" {
				if response, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: first}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("initial Start: %v %v", response, err)
				}
			}
			f.clock.Advance(time.Second)
			renewed := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			var decision velav1.ModelRuntimeCommandDecision
			if operation == "prepare-replay" {
				response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: renewed, ExecutionSpec: runtimeExecutionSpec()})
				if err != nil {
					t.Fatal(err)
				}
				decision = response.GetDecision()
			} else {
				response, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: renewed})
				if err != nil {
					t.Fatal(err)
				}
				decision = response.GetDecision()
			}
			want := velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
			if operation == "start" {
				want = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
			}
			if decision != want || backend.statusCalls != 1 {
				t.Fatalf("renewed command did not synchronize before replying: %s, Status calls=%d", decision, backend.statusCalls)
			}
			for _, authority := range []*velav1.StageAuthority{first, renewed} {
				read, err := f.supervisor.InspectExecution(t.Context(), inspectionRequest(authority))
				if err != nil || read.GetKnown() != (authority == renewed) {
					t.Fatalf("command accepted an unconfirmed identity: %v %v", read, err)
				}
			}
		})
	}
}

func TestModelRuntimeFailedPreparationRemainsCancellableWithoutDrain(t *testing.T) {
	for _, stop := range []string{"explicit", "watchdog"} {
		t.Run(stop, func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failPrepare: true, calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			authority := f.authorities[0]
			prepared, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: runtimeExecutionSpec()})
			if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("partial Prepare failure not observed: %v %v", prepared, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
			if stop == "explicit" {
				response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
				if err != nil || !response.GetCancellationAcknowledged() {
					t.Fatalf("FAILED without drain suppressed cleanup: %v %v", response, err)
				}
			} else {
				f.clock.Advance(time.Minute)
			}
			select {
			case call := <-backend.calls:
				if !proto.Equal(call.authority, authority) {
					t.Fatal("failed preparation cleanup changed authority")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("FAILED without drain suppressed watchdog cleanup")
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
		})
	}
}

func TestModelRuntimeReportedFailureKeepsWatchdogWithoutDrain(t *testing.T) {
	backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), calls: make(chan cancellationAuthorityCall, 2)}
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	authority := f.authorities[0]
	prepareFloorRuntime(t, f.supervisor, authority)
	backend.reportedFailure = &modelruntime.BackendStatus{
		State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED, Sequence: 2,
		FailureEvidence: &modelruntime.FailureEvidence{FailureClass: "injected_failure", FailureFingerprint: bytes.Repeat([]byte{0x51}, 32),
			WorkerReusable: false, ConsumedResourceUnits: 1, FailedAt: f.clock.Now(), RetryAt: f.clock.Now().Add(time.Minute)},
	}
	response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
		t.Fatalf("structured failure not observed: %v %v", response, err)
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
	f.clock.Advance(time.Minute)
	select {
	case call := <-backend.calls:
		if !proto.Equal(call.authority, authority) || call.reason != velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE {
			t.Fatalf("failed execution watchdog changed target: %+v", call)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FAILED without drain stopped the watchdog")
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
}

func (backend *renewalRecoveryBackend) Cancel(ctx context.Context, authority stageauthority.Verified, reason velav1.ModelRuntimeCancelReason) error {
	backend.calls <- cancellationAuthorityCall{authority: proto.Clone(authority.Authority).(*velav1.StageAuthority), reason: reason}
	return backend.FakeRuntime.Cancel(ctx, authority, reason)
}
