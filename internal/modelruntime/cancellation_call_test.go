package modelruntime_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestModelRuntimeCancelInterruptsBlockedExecutionCalls(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status", "seal"} {
		t.Run(operation, func(t *testing.T) {
			backend := &watchdogCallBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), operation: operation,
				entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{})}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			t.Cleanup(backend.unblock)
			authority := f.authorities[0]
			if operation == "seal" {
				readyDrainOutput(t, f, backend.FakeRuntime, authority)
			} else if operation != "prepare" {
				prepareFloorRuntime(t, f.supervisor, authority)
			}
			finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
			go func() { finished <- queuedAuthorityCommand(f.services[0], operation, authority) }()
			select {
			case <-backend.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("command did not reach backend")
			}
			canceled := make(chan *velav1.ModelRuntimeServiceCancelStageResponse, 1)
			go func() {
				response, _ := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
					Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				})
				canceled <- response
			}()
			select {
			case <-backend.canceled:
			case <-time.After(300 * time.Millisecond):
				backend.unblock()
				<-finished
				<-canceled
				t.Fatal("CancelStage waited behind backend without interrupting its context")
			}
			select {
			case decision := <-finished:
				if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
					t.Fatalf("interrupted command returned %s", decision)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cooperative backend did not return after CancelStage")
			}
			select {
			case response := <-canceled:
				if !response.GetCancellationAcknowledged() {
					t.Fatalf("CancelStage did not reach its installed execution: %v", response)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("CancelStage did not finish after execution returned")
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
			other := f.authority(t, 1, 12)
			response, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{
				Authority: other, ExecutionSpec: runtimeExecutionSpec(),
			})
			if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("interruption released unproven shared capacity: %v %v", response, err)
			}
		})
	}
}

func TestModelRuntimeCancelInterruptionRequiresCurrentAdmissionAndTarget(t *testing.T) {
	for _, envelope := range []string{"exact", "successor", "floor-exact", "floor-successor", "failed-journal-exact", "failed-journal-successor", "superseded", "unrelated", "signature", "reason", "future", "unseen-expired", "caller-canceled"} {
		t.Run(envelope, func(t *testing.T) {
			backend := &watchdogCallBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(),
				entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{})}
			directory := privateExecutionStateDirectory(t)
			f := newExecutionDrainFixture(t, directory, backend)
			t.Cleanup(backend.unblock)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			installed := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			status, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: installed})
			if err != nil || status.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("backend did not confirm renewal: %v %v", status, err)
			}
			backend.operation = "status"
			finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
			go func() { finished <- queuedAuthorityCommand(f.services[0], "status", installed) }()
			select {
			case <-backend.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("renewed Status did not reach backend")
			}
			f.clock.Advance(time.Second)
			request := proto.Clone(installed).(*velav1.StageAuthority)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reason := velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP
			accepted := envelope == "exact" || envelope == "successor" || envelope == "floor-exact" || envelope == "failed-journal-exact"
			switch envelope {
			case "successor", "floor-successor", "failed-journal-successor", "future", "unseen-expired":
				request = renewWatchdogAuthority(t, f.signer, installed, f.clock.Now())
			case "superseded":
				request = first
			case "unrelated":
				request.StageFence++
			case "reason":
				reason = velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_UNSPECIFIED
			case "caller-canceled":
				cancel()
			}
			if envelope == "floor-exact" || envelope == "floor-successor" {
				if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
					t.Fatal(err)
				}
			}
			if envelope == "failed-journal-exact" || envelope == "failed-journal-successor" {
				statePath := filepath.Join(directory, durableStateFileName)
				if err := os.Rename(statePath, statePath+".retained"); err != nil {
					t.Fatal(err)
				}
			}
			if envelope == "future" {
				request.IssuedAt = timestamppb.New(f.clock.Now().Add(time.Hour))
				request.ExpiresAt = timestamppb.New(f.clock.Now().Add(2 * time.Hour))
			}
			if envelope == "unseen-expired" {
				f.clock.mu.Lock()
				f.clock.now = f.clock.now.Add(2 * time.Minute)
				f.clock.mu.Unlock()
			}
			request, err = f.signer.Sign(request)
			if err != nil {
				t.Fatal(err)
			}
			if envelope == "signature" {
				request.Signature[0] ^= 1
			}
			canceled := make(chan *velav1.ModelRuntimeServiceCancelStageResponse, 1)
			go func() {
				response, _ := f.supervisor.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{Authority: request, Reason: reason})
				canceled <- response
			}()
			if !accepted {
				select {
				case response := <-canceled:
					if response.GetCancellationAcknowledged() {
						t.Fatalf("ineligible cancellation was acknowledged: %v", response)
					}
				case <-time.After(2 * time.Second):
					backend.unblock()
					<-finished
					<-canceled
					t.Fatal("ineligible cancellation waited behind execution")
				}
				select {
				case <-backend.canceled:
					t.Fatal("ineligible cancellation interrupted the backend")
				default:
				}
				backend.unblock()
				<-finished
				return
			}
			select {
			case response := <-canceled:
				if !response.GetCancellationAcknowledged() {
					t.Fatalf("eligible cancellation failed: %v", response)
				}
			case <-time.After(2 * time.Second):
				backend.unblock()
				<-finished
				<-canceled
				t.Fatal("eligible cancellation did not interrupt execution")
			}
			if decision := <-finished; decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("interrupted execution was accepted: %s", decision)
			}
			read, err := f.supervisor.InspectExecution(t.Context(), inspectionRequest(installed))
			if err != nil || !read.GetKnown() || read.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
				t.Fatalf("interruption changed the installed recovery identity: %v %v", read, err)
			}
			if envelope == "successor" {
				read, err := f.supervisor.InspectExecution(t.Context(), inspectionRequest(request))
				if err != nil || read.GetKnown() {
					t.Fatalf("cancellation installed its successor: %v %v", read, err)
				}
			}
		})
	}
}

func TestModelRuntimeCancelRetainsUncooperativeExecutionUntilActualReturn(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status", "seal"} {
		t.Run(operation, func(t *testing.T) {
			backend := &watchdogCallBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), operation: operation, ignoreCancellation: true,
				entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{})}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			t.Cleanup(backend.unblock)
			authority := f.authorities[0]
			if operation == "seal" {
				readyDrainOutput(t, f, backend.FakeRuntime, authority)
			} else if operation != "prepare" {
				prepareFloorRuntime(t, f.supervisor, authority)
			}
			finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
			go func() { finished <- queuedAuthorityCommand(f.services[0], operation, authority) }()
			select {
			case <-backend.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("command did not reach backend")
			}
			canceled := make(chan *velav1.ModelRuntimeServiceCancelStageResponse, 2)
			stop := func() {
				response, _ := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
					Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				})
				canceled <- response
			}
			go stop()
			select {
			case <-backend.canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("CancelStage did not interrupt uncooperative execution")
			}
			go stop()
			floor, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if err := floor.WaitAcceptedOperations(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cancellation replaced or released the original admitted operation: %v", err)
			}
			select {
			case <-canceled:
				t.Fatal("unreturned execution was already acknowledged as canceled")
			default:
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
			backend.unblock()
			if decision := <-finished; decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("late successful execution was accepted: %s", decision)
			}
			for range 2 {
				select {
				case response := <-canceled:
					if operation != "seal" && !response.GetCancellationAcknowledged() {
						t.Fatalf("exact cancellation failed after floor: %v", response)
					}
					if operation == "seal" && response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED {
						t.Fatalf("late Seal identity was lost: %v", response)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("cancellation did not finish after actual return")
				}
			}
			if err := floor.WaitAcceptedOperations(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
			if operation == "seal" {
				if checkpoint, err := f.supervisor.DrainExecution(t.Context(), authority); err != nil || checkpoint == nil {
					t.Fatalf("late Seal could not complete an explicit original drain: %v %v", checkpoint, err)
				}
			}
		})
	}
}

func TestModelRuntimeInterruptedRenewalRecoversObservedCancellationIdentity(t *testing.T) {
	backend := &canceledPrepareBackend{watchdogCallBackend: &watchdogCallBackend{
		FakeRuntime: modelruntime.NewFakeDiTRuntime(), operation: "status",
		entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{}),
	}, calls: make(chan cancellationAuthorityCall, 2)}
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	t.Cleanup(backend.unblock)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	f.clock.Advance(time.Second)
	renewed := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
	finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
	go func() { finished <- queuedAuthorityCommand(f.services[0], "status", renewed) }()
	select {
	case <-backend.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("renewing Status did not reach backend")
	}
	response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: renewed, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !response.GetCancellationAcknowledged() || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("interruption could not cancel the observed backend identity: %v %v", response, err)
	}
	if decision := <-finished; decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("interrupted renewal was accepted: %s", decision)
	}
	call := <-backend.calls
	if !proto.Equal(call.authority, f.authorities[0]) {
		t.Fatal("cancellation did not use the backend-observed envelope")
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, renewed, false)
	other := f.authority(t, 1, 12)
	prepared, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: other, ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("uncertain backend renewal released shared capacity: %v %v", prepared, err)
	}
}

func TestModelRuntimeCancelRechecksFloorAfterInterruption(t *testing.T) {
	backend := &canceledPrepareBackend{watchdogCallBackend: &watchdogCallBackend{
		FakeRuntime: modelruntime.NewFakeDiTRuntime(), operation: "start", ignoreCancellation: true,
		entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{}),
	}, calls: make(chan cancellationAuthorityCall, 2)}
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	t.Cleanup(backend.unblock)
	authority := f.authorities[0]
	prepareFloorRuntime(t, f.supervisor, authority)
	finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
	go func() { finished <- queuedAuthorityCommand(f.services[0], "start", authority) }()
	select {
	case <-backend.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not reach backend")
	}
	f.clock.Advance(time.Second)
	successor := renewWatchdogAuthority(t, f.signer, authority, f.clock.Now())
	canceled := make(chan *velav1.ModelRuntimeServiceCancelStageResponse, 1)
	go func() {
		response, _ := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
			Authority: successor, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
		})
		canceled <- response
	}()
	select {
	case <-backend.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("fresh successor did not interrupt execution")
	}
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	backend.unblock()
	if decision := <-finished; decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("interrupted Start returned %s", decision)
	}
	select {
	case response := <-canceled:
		if response.GetCancellationAcknowledged() || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || len(backend.calls) != 0 {
			t.Fatalf("preemption bypassed the subsequently installed floor: %v", response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not revalidate after waiting")
	}
	response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !response.GetCancellationAcknowledged() || !proto.Equal((<-backend.calls).authority, authority) {
		t.Fatalf("exact original authority could not recover cancellation: %v %v", response, err)
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
}
