package modelruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestModelRuntimeCancellationRequiresExactOrFreshSuccessor(t *testing.T) {
	for _, envelope := range []string{"exact", "exact-expired", "successor", "superseded", "unseen-expired", "future", "allocation", "signature"} {
		t.Run(envelope, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			backend := &cancellationAuthorityBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), calls: make(chan cancellationAuthorityCall, 4)}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			first := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, first)
			clock.Advance(time.Second)
			installed := renewWatchdogAuthority(t, signer, first, clock.Now())
			assertWatchdogRuntimeState(t, client, installed, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING)
			clock.Advance(time.Second)
			request := renewWatchdogAuthority(t, signer, installed, clock.Now())
			accepted := envelope == "exact" || envelope == "exact-expired" || envelope == "successor"
			switch envelope {
			case "exact", "exact-expired":
				request = installed
			case "superseded":
				request = first
			case "future":
				request.IssuedAt = timestamppb.New(clock.Now().Add(time.Hour))
				request.ExpiresAt = timestamppb.New(clock.Now().Add(2 * time.Hour))
			case "allocation":
				request.StageFence++
			}
			if envelope == "exact-expired" || envelope == "unseen-expired" {
				clock.mu.Lock()
				clock.now = clock.now.Add(2 * time.Minute)
				clock.mu.Unlock()
			}
			var err error
			request, err = signer.Sign(request)
			if err != nil {
				t.Fatal(err)
			}
			if envelope == "signature" {
				request.Signature[0] ^= 1
			}
			response, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: request, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			})
			if err != nil || response.GetCancellationAcknowledged() != accepted {
				t.Fatalf("cancellation authority decision: %v %v", response, err)
			}
			if !accepted {
				if len(backend.calls) != 0 {
					t.Fatal("invalid cancellation reached backend")
				}
				return
			}
			if len(backend.calls) != 1 || !proto.Equal((<-backend.calls).authority, installed) {
				t.Fatal("cancellation did not target latest installed authority")
			}
		})
	}
}

func TestModelRuntimeCancellationCannotInstallAuthorityOrExtendWatchdog(t *testing.T) {
	for _, outcome := range []string{"acknowledged", "failed", "failed-mutated"} {
		t.Run(outcome, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			backend := &cancellationAuthorityBackend{
				FakeRuntime: modelruntime.NewFakeDiTRuntime(), fail: outcome != "acknowledged", mutate: outcome == "failed-mutated", calls: make(chan cancellationAuthorityCall, 4),
			}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			installed := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, installed)
			clock.mu.Lock()
			originalTimer, timerCount := clock.timers[len(clock.timers)-1], len(clock.timers)
			clock.mu.Unlock()
			clock.Advance(time.Second)
			successor := renewWatchdogAuthority(t, signer, installed, clock.Now())
			response, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: successor, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			})
			want := velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
			state := velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING
			if outcome != "acknowledged" {
				want, state = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING
			}
			if err != nil || response.GetDecision() != want || response.GetCancellationAcknowledged() != (outcome == "acknowledged") {
				t.Errorf("cancel successor: %v %v", response, err)
			}
			call := <-backend.calls
			if !proto.Equal(call.authority, installed) {
				t.Error("cancellation forwarded an uninstalled authority to the backend")
			}
			clock.mu.Lock()
			unchanged := len(clock.timers) == timerCount && !originalTimer.stopped
			clock.mu.Unlock()
			if !unchanged {
				t.Error("cancellation replaced the installed watchdog")
			}
			for _, authority := range []*velav1.StageAuthority{installed, successor} {
				read, err := client.InspectExecution(t.Context(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: authority})
				known := authority == installed
				if err != nil || read.GetKnown() != known || known && read.GetState() != state {
					t.Errorf("cancellation changed exact inspection identity: %v %v", read, err)
				}
			}
			if t.Failed() {
				return
			}
			if outcome == "acknowledged" {
				replayed, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
					Authority: successor, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				})
				if err != nil || !replayed.GetCancellationAcknowledged() || replayed.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED || len(backend.calls) != 0 {
					t.Fatalf("compatible cancel replay: %v %v", replayed, err)
				}
				return
			}
			clock.Advance(originalTimer.deadline.Sub(clock.Now()))
			select {
			case watchdog := <-backend.calls:
				if watchdog.reason != velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE || !proto.Equal(watchdog.authority, installed) {
					t.Fatalf("original deadline did not stop its installed execution: %+v", watchdog)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("failed cancellation postponed the original watchdog")
			}
		})
	}
}

type cancellationAuthorityCall struct {
	authority *velav1.StageAuthority
	reason    velav1.ModelRuntimeCancelReason
}

type cancellationAuthorityBackend struct {
	*modelruntime.FakeRuntime
	fail   bool
	mutate bool
	calls  chan cancellationAuthorityCall
}

func (backend *cancellationAuthorityBackend) Cancel(ctx context.Context, authority stageauthority.Verified, reason velav1.ModelRuntimeCancelReason) error {
	backend.calls <- cancellationAuthorityCall{authority: proto.Clone(authority.Authority).(*velav1.StageAuthority), reason: reason}
	if backend.fail && reason != velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE {
		if backend.mutate {
			authority.Authority.LeaseToken[0] ^= 1
		}
		return errors.New("injected backend cancellation failure")
	}
	return backend.FakeRuntime.Cancel(ctx, authority, reason)
}
