package modelruntime_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestModelRuntimeRechecksAuthorityAfterWaitingForOperation(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status", "seal", "cancel-renewal"} {
		t.Run(operation, func(t *testing.T) {
			service, clock, signer, backend, observed, observing, first := queuedAuthorityFixture(t)
			prepareResult := make(chan velav1.ModelRuntimeCommandDecision, 1)
			go func() { prepareResult <- queuedAuthorityCommand(service, "prepare", first) }()
			select {
			case <-backend.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Prepare did not enter backend")
			}
			queued := first
			if operation == "cancel-renewal" {
				clock.Advance(time.Second)
				queued = renewWatchdogAuthority(t, signer, first, clock.Now())
			}
			observing.Store(true)
			result := make(chan velav1.ModelRuntimeCommandDecision, 1)
			go func() { result <- queuedAuthorityCommand(service, operation, queued) }()
			select {
			case <-observed:
			case <-time.After(5 * time.Second):
				t.Fatal("queued request did not validate its initial envelope")
			}
			if operation == "cancel-renewal" {
				select {
				case <-backend.prepareContext.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("eligible cancellation did not interrupt admitted Prepare")
				}
			}
			// The watchdog goroutine may be delayed independently of wall time.
			// Do not deliver its timer: admission must enforce expiry itself.
			clock.mu.Lock()
			clock.now = clock.now.Add(time.Minute)
			clock.mu.Unlock()
			backend.unblock()
			wantPrepare := velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
			if operation == "cancel-renewal" {
				wantPrepare = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			}
			if decision := <-prepareResult; decision != wantPrepare {
				t.Fatalf("previously admitted Prepare: %v", decision)
			}
			select {
			case decision := <-result:
				if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
					t.Fatalf("queued expired %s was %s", operation, decision)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("queued request did not return")
			}
			if backend.calls.Load() != 1 {
				t.Fatal("queued expired request reached backend")
			}
		})
	}
}

func TestModelRuntimeQueuedRenewalDoesNotExtendDeadlineByWaitTime(t *testing.T) {
	service, clock, signer, backend, observed, observing, first := queuedAuthorityFixture(t)
	prepared := make(chan velav1.ModelRuntimeCommandDecision, 1)
	go func() { prepared <- queuedAuthorityCommand(service, "prepare", first) }()
	select {
	case <-backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Prepare did not enter backend")
	}
	clock.Advance(time.Second)
	renewal := renewWatchdogAuthority(t, signer, first, clock.Now())
	observing.Store(true)
	result := make(chan velav1.ModelRuntimeCommandDecision, 1)
	go func() { result <- queuedAuthorityCommand(service, "status", renewal) }()
	select {
	case <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal did not validate before queuing")
	}
	clock.Advance(10 * time.Second)
	backend.unblock()
	if decision := <-prepared; decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("Prepare: %s", decision)
	}
	if decision := <-result; decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("still-valid queued renewal: %s", decision)
	}
	clock.mu.Lock()
	deadline := clock.timers[len(clock.timers)-1].deadline
	clock.mu.Unlock()
	expected := renewal.GetIssuedAt().AsTime().Add(renewal.GetMonotonicValidFor().AsDuration())
	if renewal.GetExpiresAt().AsTime().Before(expected) {
		expected = renewal.GetExpiresAt().AsTime()
	}
	if !deadline.Equal(expected) {
		t.Fatalf("queued renewal watchdog deadline = %s, signed time bound = %s", deadline, expected)
	}
}

func queuedAuthorityFixture(t *testing.T) (*modelruntime.Service, *manualClock, *stageauthority.Signer, *floorBlockingBackend, <-chan time.Time, *atomic.Bool, *velav1.StageAuthority) {
	t.Helper()
	clock := newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	signer, _ := runtimeAuthorityCrypto(t, clock)
	observed := make(chan time.Time, 4)
	observing := &atomic.Bool{}
	validator, err := stageauthority.NewValidator(map[string][]byte{"stage-key-9": bytes.Repeat([]byte{0x5a}, 32)}, func() time.Time {
		now := clock.Now()
		if observing.Load() {
			select {
			case observed <- now:
			default:
			}
		}
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := &floorBlockingBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), blocked: "prepare", entered: make(chan struct{}), resume: make(chan struct{})}
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	t.Cleanup(backend.unblock)
	return service, clock, signer, backend, observed, observing, signRuntimeAuthority(t, signer, clock.Now())
}

func queuedAuthorityCommand(service *modelruntime.Service, operation string, authority *velav1.StageAuthority) velav1.ModelRuntimeCommandDecision {
	ctx := context.Background()
	var response interface {
		GetDecision() velav1.ModelRuntimeCommandDecision
	}
	var err error
	switch operation {
	case "prepare":
		response, err = service.PrepareStage(ctx, &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: runtimeExecutionSpec()})
	case "start":
		response, err = service.StartStage(ctx, &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	case "status":
		response, err = service.Status(ctx, &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
	case "seal":
		response, err = service.SealOutput(ctx, &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
	case "cancel-renewal":
		response, err = service.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
	}
	if err != nil || response == nil {
		return velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_UNSPECIFIED
	}
	return response.GetDecision()
}
