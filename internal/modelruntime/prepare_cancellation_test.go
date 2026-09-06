package modelruntime_test

import (
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestModelRuntimeCanceledPrepareKeepsExecutionCancellable(t *testing.T) {
	for _, outcome := range []string{"canceled-error", "late-success"} {
		for _, stop := range []string{"explicit", "watchdog"} {
			t.Run(outcome+"/"+stop, func(t *testing.T) {
				backend := &canceledPrepareBackend{watchdogCallBackend: &watchdogCallBackend{
					FakeRuntime: modelruntime.NewFakeDiTRuntime(), operation: "prepare", ignoreCancellation: outcome == "late-success",
					entered: make(chan struct{}), canceled: make(chan struct{}), resume: make(chan struct{}),
				}, calls: make(chan cancellationAuthorityCall, 2)}
				f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
				t.Cleanup(backend.unblock)
				authority := f.authorities[0]
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				finished := make(chan *velav1.ModelRuntimeServicePrepareStageResponse, 1)
				go func() {
					response, _ := f.services[0].PrepareStage(ctx, &velav1.ModelRuntimeServicePrepareStageRequest{
						Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
					})
					finished <- response
				}()
				select {
				case <-backend.entered:
				case <-time.After(2 * time.Second):
					t.Fatal("Prepare did not reach backend")
				}
				cancel()
				select {
				case <-backend.canceled:
				case <-time.After(2 * time.Second):
					t.Fatal("caller cancellation did not reach backend context")
				}
				backend.unblock()
				select {
				case response := <-finished:
					if response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
						t.Fatalf("canceled Prepare was accepted: %v", response)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("canceled Prepare did not return")
				}
				assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
				started, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
				if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
					t.Fatalf("canceled Prepare allowed an unconfirmed Start: %v %v", started, err)
				}
				if stop == "explicit" {
					response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
						Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
					})
					if err != nil || !response.GetCancellationAcknowledged() {
						t.Fatalf("caller cancellation suppressed exact execution cancellation: %v %v", response, err)
					}
				} else {
					f.clock.Advance(time.Minute)
				}
				select {
				case call := <-backend.calls:
					wantReason := velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP
					if stop == "watchdog" {
						wantReason = velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE
					}
					if !proto.Equal(call.authority, authority) || call.reason != wantReason {
						t.Fatalf("canceled Prepare lost its original cancellation target: %+v", call)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("caller cancellation suppressed the original watchdog")
				}
				assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
				other := f.authority(t, 1, 12)
				response, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{
					Authority: other, ExecutionSpec: runtimeExecutionSpec(),
				})
				if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
					t.Fatalf("caller cancellation released unproven shared slot: %v %v", response, err)
				}
			})
		}
	}
}

type canceledPrepareBackend struct {
	*watchdogCallBackend
	calls chan cancellationAuthorityCall
}

func (backend *canceledPrepareBackend) Cancel(ctx context.Context, authority stageauthority.Verified, reason velav1.ModelRuntimeCancelReason) error {
	err := backend.FakeRuntime.Cancel(ctx, authority, reason)
	backend.calls <- cancellationAuthorityCall{authority: authority.Authority, reason: reason}
	return err
}
