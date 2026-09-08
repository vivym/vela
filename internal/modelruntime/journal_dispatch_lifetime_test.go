package modelruntime_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestJournalRemoteCandidatePersistenceRetainsLifetimeFence(t *testing.T) {
	for _, scenario := range []struct{ event, boundary string }{
		{"watchdog", "reply"}, {"shutdown", "reply"},
		{"watchdog", "directory-sync"}, {"shutdown", "directory-sync"},
	} {
		t.Run(scenario.event+"/"+scenario.boundary, func(t *testing.T) {
			f, owner, transport, _ := remoteSupervisorContextFixture(t, t.Context(), 10*time.Second)
			// The marker records even an attempted call with a canceled context.
			f.backend.blocked = "prepare"
			f.backend.unblock()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			wait := func(ctx context.Context) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			// Admission is the first mutation. The second RPC records the
			// pre-dispatch candidate; its unchanged bytes need no new fsync.
			// The second fsync instead confirms the returned backend envelope.
			if scenario.boundary == "reply" {
				transport.afterApply = func(ctx context.Context) error {
					if transport.mutations == 2 {
						return wait(ctx)
					}
					return nil
				}
			} else {
				syncs := 0
				restore := modelruntime.SetJournalOwnerSyncHookForTest(owner, func(syncDirectory func() error) error {
					syncs++
					if syncs == 2 {
						if err := wait(t.Context()); err != nil {
							return err
						}
					}
					return syncDirectory()
				})
				// Release the blocked writer before reacquiring the owner's lock.
				defer func() { unblock(); restore() }()
			}
			finished := make(chan *velav1.ModelRuntimeServicePrepareStageResponse, 1)
			go func() {
				response, _ := f.services[0].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{
					Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec(),
				})
				finished <- response
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("Prepare did not reach the candidate persistence boundary")
			}
			if scenario.boundary == "directory-sync" {
				select {
				case <-f.backend.entered:
				default:
					t.Fatal("confirmation fsync did not follow the backend call")
				}
			}
			delivered := make(chan struct{})
			if scenario.event == "shutdown" {
				go func() { _ = f.services[0].Shutdown(); close(delivered) }()
			} else {
				// Deliver the monotonic timer while the wall clock remains
				// within the signed envelope. Wall-time revalidation alone
				// cannot catch this deadline.
				f.clock.mu.Lock()
				timer := f.clock.timers[len(f.clock.timers)-1]
				f.clock.mu.Unlock()
				timer.channel <- timer.deadline
				go func() {
					for !modelruntime.ExecutionDeadlineExpiredForTest(f.services[0]) {
						time.Sleep(time.Millisecond)
					}
					close(delivered)
				}()
			}
			select {
			case <-delivered:
			case <-time.After(2 * time.Second):
				unblock()
				<-finished
				<-delivered
				t.Fatal("candidate persistence blocked the execution lifetime signal")
			}
			unblock()
			response := <-finished
			if response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("lifetime-ended Prepare was not rejected: %v", response)
			}
			if scenario.boundary == "reply" {
				select {
				case <-f.backend.entered:
					t.Fatal("backend Prepare was dispatched after lifetime ended during persistence")
				default:
				}
			}
			status, err := owner.Status(t.Context())
			if err != nil || status.Highest != 10 || status.PendingExecutions != 1 {
				t.Fatalf("rejected dispatch lost durable admission/candidate evidence: %+v %v", status, err)
			}
		})
	}
}
