package modelruntime_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestJournalRemoteWatchdogRetainsStopAfterAdmissionLockWait(t *testing.T) {
	f, _, transport, _ := remoteSupervisorContextFixture(t, t.Context(), 5*time.Second)
	authority := f.authorities[0]
	prepareFloorRuntime(t, f.supervisor, authority)
	started, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("initial start: %v %v", started, err)
	}
	before := f.backend.calls.Load()
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	transport.beforeRead = func(ctx context.Context) error {
		enterOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	readDone := make(chan error, 1)
	go func() {
		member := authority.Members[0]
		_, err := f.supervisor.DiscoverRuntimeIdentities(t.Context(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
			WorkerInstanceId: authority.WorkerInstanceId, WorkerInstanceEpoch: authority.WorkerInstanceEpoch,
			WorkerMemberId: member.WorkerMemberId, WorkerMemberEpoch: member.MemberEpoch})
		readDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("discovery did not enter journal while holding admission")
	}
	f.clock.Advance(2 * time.Minute)
	// The fixture's stop budget is one second. The independently accepted
	// discovery remains blocked longer than that, then finishes normally.
	time.Sleep(1500 * time.Millisecond)
	unblock()
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for f.backend.calls.Load() == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.backend.calls.Load() != before+1 {
		t.Fatalf("watchdog lost stop after unrelated journal lock wait: before=%d after=%d", before, f.backend.calls.Load())
	}
}

func TestJournalRemoteCancellationDoesNotSpendStopBudgetOnUnavailableReads(t *testing.T) {
	for _, operation := range []string{"cancel", "watchdog"} {
		t.Run(operation, func(t *testing.T) {
			f, owner, transport, directory := remoteSupervisorContextFixture(t, t.Context(), 5*time.Second)
			authority := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, authority)
			started, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
			if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("initial start: %v %v", started, err)
			}
			beforeCalls := f.backend.calls.Load()
			statePath := filepath.Join(directory, durableStateFileName)
			before, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			var stalled atomic.Bool
			var reads atomic.Int64
			transport.beforeRead = func(ctx context.Context) error {
				if stalled.Load() {
					reads.Add(1)
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			stalled.Store(true)
			defer stalled.Store(false)
			if operation == "cancel" {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				response, err := f.supervisor.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{Authority: authority,
					Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
				cancel()
				if err != nil || !response.GetCancellationAcknowledged() {
					t.Fatalf("journal read consumed cancellation budget without stopping installed execution: %v %v", response, err)
				}
			} else {
				f.clock.Advance(2 * time.Minute)
				deadline := time.Now().Add(2 * time.Second)
				for f.backend.calls.Load() == beforeCalls && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			}
			if f.backend.calls.Load() != beforeCalls+1 || reads.Load() != 0 {
				t.Fatalf("stop depended on unavailable journal: backend calls before=%d after=%d journal reads=%d", beforeCalls, f.backend.calls.Load(), reads.Load())
			}
			// Even observed stop cannot establish durable drain during an outage.
			f.backend.FinishStop()
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			if _, err := f.supervisor.DrainExecution(ctx, authority); err == nil {
				t.Fatal("unavailable journal acknowledged drain")
			}
			cancel()
			after, err := os.ReadFile(statePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("stop or failed drain changed durable history: %v", err)
			}
			status, err := owner.Status(t.Context())
			if err != nil || status.PendingExecutions != 1 {
				t.Fatalf("stop released persistent execution: %+v %v", status, err)
			}
			stalled.Store(false)
			if result, err := f.supervisor.DrainExecution(t.Context(), authority); err != nil || result == nil {
				t.Fatalf("restored journal could not persist real stop/drain: %+v %v", result, err)
			}
		})
	}
}
