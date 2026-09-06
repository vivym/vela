package modelruntime_test

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestModelRuntimeWatchdogsDrainWhileResidentServiceStaysOpen(t *testing.T) {
	for _, completion := range []string{"seal", "stop"} {
		t.Run(completion, func(t *testing.T) {
			waitForRuntimeWatchdogs(t, 0)
			clock := newManualClock(time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			backend := modelruntime.NewFakeDiTRuntime()
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			for round := 0; round < 64; round++ {
				authority := orderedRuntimeAuthority(t, signer, clock.Now(), int64(round)+1)
				prepareAndStart(t, client, authority)
				clock.Advance(time.Second)
				renewed := renewWatchdogAuthority(t, signer, authority, clock.Now())
				assertWatchdogRuntimeState(t, client, renewed,
					velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING)
				if completion == "seal" {
					backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
					sealed, err := client.SealOutput(context.Background(), &velav1.ModelRuntimeServiceSealOutputRequest{
						Authority: renewed,
					})
					if err != nil || sealed.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
						sealed.GetReceipt().GetReceiptId() == "" {
						t.Fatalf("round %d SealOutput = %v error=%v", round, sealed, err)
					}
				} else {
					canceled, err := client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
						Authority: renewed,
						Reason:    velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
					})
					if err != nil || !canceled.GetCancellationAcknowledged() {
						t.Fatalf("round %d CancelStage = %v error=%v", round, canceled, err)
					}
					backend.FinishStop()
					assertWatchdogRuntimeState(t, client, renewed,
						velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED)
				}
			}
			waitForRuntimeWatchdogs(t, 0)
		})
	}
}

func TestModelRuntimeCanceledWatchdogCannotFenceRenewedGeneration(t *testing.T) {
	waitForRuntimeWatchdogs(t, 0)
	clock := newManualClock(time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := modelruntime.NewFakeDiTRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	authority := signDistinctWatchdogAuthority(t, signer, clock.Now())
	prepareAndStart(t, client, authority)
	clock.mu.Lock()
	oldTimer := clock.timers[0]
	clock.mu.Unlock()
	clock.Advance(time.Second)
	renewed := renewWatchdogAuthority(t, signer, authority, clock.Now())
	assertWatchdogRuntimeState(t, client, renewed,
		velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING)
	// A timer may already have delivered its signal when Stop races with renewal.
	oldTimer.channel <- clock.Now()
	waitForRuntimeWatchdogs(t, 1)
	assertWatchdogRuntimeState(t, client, renewed,
		velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING)
	clock.mu.Lock()
	currentTimer := clock.timers[len(clock.timers)-1]
	clock.mu.Unlock()
	currentTimer.channel <- currentTimer.deadline
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, err := client.InspectExecution(context.Background(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: renewed})
		if err == nil && response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED &&
			response.GetState() == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("current watchdog did not cancel runtime: inspection = %v error=%v", response, err)
		}
		time.Sleep(time.Millisecond)
	}
	waitForRuntimeWatchdogs(t, 0)
}

func signDistinctWatchdogAuthority(t *testing.T, signer *stageauthority.Signer, now time.Time) *velav1.StageAuthority {
	t.Helper()
	authority := signRuntimeAuthority(t, signer, now)
	authority.StageRunId = uuid.NewString()
	authority.StageAttemptId = uuid.NewString()
	authority.StageAllocationId = uuid.NewString()
	authority.StageLeaseId = uuid.NewString()
	authority.ExecutionNonce = bytes.Repeat([]byte{0x75}, 32)
	authority.Signature = nil
	signed, err := signer.Sign(authority)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func renewWatchdogAuthority(t *testing.T, signer *stageauthority.Signer, authority *velav1.StageAuthority, now time.Time) *velav1.StageAuthority {
	t.Helper()
	renewed := proto.Clone(authority).(*velav1.StageAuthority)
	renewed.StageVersion++
	renewed.IssuedAt = timestamppb.New(now)
	renewed.ExpiresAt = timestamppb.New(now.Add(time.Minute))
	renewed.Signature = nil
	signed, err := signer.Sign(renewed)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func assertWatchdogRuntimeState(t *testing.T, client velav1.ModelRuntimeServiceClient, authority *velav1.StageAuthority, state velav1.ModelRuntimeExecutionState) {
	t.Helper()
	response, err := client.Status(context.Background(), &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || response.GetState() != state {
		t.Fatalf("Status = %v error=%v; want %s", response, err, state)
	}
}

func waitForRuntimeWatchdogs(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var stacks bytes.Buffer
		if err := pprof.Lookup("goroutine").WriteTo(&stacks, 2); err != nil {
			t.Fatal(err)
		}
		got := strings.Count(stacks.String(), "modelruntime.(*Service).resetWatchdogLocked.func1")
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("resident runtime watchdog goroutines = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}
