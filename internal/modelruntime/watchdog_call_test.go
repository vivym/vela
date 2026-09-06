package modelruntime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestModelRuntimeWatchdogInterruptsBlockedExecutionCalls(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status", "seal"} {
		t.Run(operation, func(t *testing.T) {
			backend := &watchdogCallBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), operation: operation,
				entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{})}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			t.Cleanup(backend.unblock)
			authority := f.authorities[0]
			if operation != "prepare" && operation != "seal" {
				prepareFloorRuntime(t, f.supervisor, authority)
			}
			if operation == "seal" {
				readyDrainOutput(t, f, backend.FakeRuntime, authority)
			}
			finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
			go func() { finished <- queuedAuthorityCommand(f.services[0], operation, authority) }()
			select {
			case <-backend.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("command did not reach blocked backend")
			}
			f.clock.Advance(time.Minute)
			select {
			case <-backend.canceled:
			case <-time.After(2 * time.Second):
				backend.unblock()
				<-finished
				t.Fatal("watchdog waited behind backend instead of canceling its context")
			}
			select {
			case decision := <-finished:
				if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
					t.Fatalf("expired backend call returned %s", decision)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cooperative backend did not return after deadline")
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
			other := f.authority(t, 1, 12)
			response, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: other, ExecutionSpec: runtimeExecutionSpec()})
			if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("deadline released unproven shared slot: %v %v", response, err)
			}
		})
	}
}

func TestModelRuntimeWatchdogRetainsUncooperativeCallsAndRejectsLateSuccess(t *testing.T) {
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
			f.clock.Advance(time.Minute)
			select {
			case <-backend.canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("watchdog did not cancel uncooperative backend context")
			}
			select {
			case <-finished:
				t.Fatal("watchdog forgot a backend call that has not returned")
			default:
			}
			floor, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if err := floor.WaitAcceptedOperations(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expiration released an unfinished admitted operation: %v", err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
			backend.unblock()
			select {
			case decision := <-finished:
				if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
					t.Fatalf("late successful backend reply was accepted: %s", decision)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("released backend call did not finish")
			}
			if err := floor.WaitAcceptedOperations(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
			if operation == "seal" {
				read, err := f.supervisor.InspectExecution(t.Context(), inspectionRequest(authority))
				if err != nil || !read.GetKnown() || read.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED {
					t.Fatalf("late Seal lost its actual terminal identity: %v %v", read, err)
				}
				if checkpoint, err := f.supervisor.DrainExecution(t.Context(), authority); err != nil || checkpoint == nil {
					t.Fatalf("late Seal could not produce an explicit original drain: %v %v", checkpoint, err)
				}
			}
		})
	}
}

func TestModelRuntimeOldWatchdogCannotCancelRenewedBackendCall(t *testing.T) {
	backend := &watchdogCallBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), operation: "status",
		entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{})}
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	t.Cleanup(backend.unblock)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	f.clock.mu.Lock()
	oldTimer := f.clock.timers[len(f.clock.timers)-1]
	f.clock.mu.Unlock()
	f.clock.Advance(time.Second)
	renewed := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
	finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
	go func() { finished <- queuedAuthorityCommand(f.services[0], "status", renewed) }()
	select {
	case <-backend.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("renewed Status did not reach backend")
	}
	oldTimer.channel <- f.clock.Now()
	select {
	case <-backend.canceled:
		t.Fatal("old generation canceled a renewed backend call")
	case <-time.After(30 * time.Millisecond):
	}
	f.clock.mu.Lock()
	current := f.clock.timers[len(f.clock.timers)-1]
	f.clock.mu.Unlock()
	current.channel <- current.deadline
	select {
	case decision := <-finished:
		if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
			t.Fatalf("current watchdog failed to restrict its own call: %s", decision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("current watchdog failed to interrupt its call")
	}
}

type watchdogCallBackend struct {
	*modelruntime.FakeRuntime
	operation          string
	ignoreCancellation bool
	entered            chan struct{}
	canceled           chan struct{}
	resume             chan struct{}
	once               sync.Once
}

func (backend *watchdogCallBackend) unblock() { backend.once.Do(func() { close(backend.resume) }) }

func (backend *watchdogCallBackend) wait(ctx context.Context, operation string) error {
	if operation != backend.operation {
		return nil
	}
	close(backend.entered)
	select {
	case <-ctx.Done():
		close(backend.canceled)
		if backend.ignoreCancellation {
			<-backend.resume
			return nil
		}
		return ctx.Err()
	case <-backend.resume:
		return errors.New("test released blocked backend")
	}
}

func (backend *watchdogCallBackend) Prepare(ctx context.Context, authority stageauthority.Verified, spec *velav1.StageExecutionSpec) error {
	if err := backend.FakeRuntime.Prepare(ctx, authority, spec); err != nil {
		return err
	}
	return backend.wait(ctx, "prepare")
}

func (backend *watchdogCallBackend) Start(ctx context.Context, authority stageauthority.Verified) error {
	if err := backend.FakeRuntime.Start(ctx, authority); err != nil {
		return err
	}
	return backend.wait(ctx, "start")
}

func (backend *watchdogCallBackend) Status(ctx context.Context, authority stageauthority.Verified) (modelruntime.BackendStatus, error) {
	if err := backend.wait(ctx, "status"); err != nil {
		return modelruntime.BackendStatus{}, err
	}
	return backend.FakeRuntime.Status(ctx, authority)
}

func (backend *watchdogCallBackend) Seal(ctx context.Context, authority stageauthority.Verified) (modelruntime.SealedOutput, error) {
	if err := backend.wait(ctx, "seal"); err != nil {
		return modelruntime.SealedOutput{}, err
	}
	return backend.FakeRuntime.Seal(ctx, authority)
}
