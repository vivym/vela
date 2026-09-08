package nodeagent

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestJournalServerCancellationSurvivesUnresponsiveNode(t *testing.T) {
	for _, operation := range []string{"cancel", "watchdog", "watchdog-queued"} {
		t.Run(operation, func(t *testing.T) {
			f := newJournalEndpointFixture(t)
			server, done := startJournalTestServer(t, f, 30*time.Second)
			directory := filepath.Join(filepath.Dir(f.listener.Addr().String()), "workload")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(directory, 65532, 65532); err != nil {
				t.Fatal(err)
			}
			socket := filepath.Join(directory, "runtime.sock")
			if report := journalServerRequest(t, f.runtime, journalEndpointControl{Identity: f.identity, Manifest: &f.manifest, Startup: f.startup, RuntimeSocket: socket, ControlledClock: true}); !report.SupervisorReady {
				t.Fatalf("Runtime startup: %+v", report)
			}
			started := journalServerRequest(t, f.worker, journalEndpointControl{Identity: f.identity, Authority: f.authority, WorkerSocket: socket, WorkerAction: "execute"})
			if started.Barrier == nil || !started.Barrier.BarrierPassed {
				t.Fatalf("Worker barrier: %+v", started)
			}
			if err := server.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertJournalServerJoined(t, server, done)
			// A valid root-owned socket accepts connects but supplies no challenge
			// or response. Unlike immediate refusal, this consumes a read deadline.
			path := f.listener.Addr().String()
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			if err := errors.Join(os.Chown(path, 0, 65532), os.Chmod(path, 0o660)); err != nil {
				t.Fatal(err)
			}
			statePath := filepath.Join(filepath.Dir(path), "state", "execution-admission.json")
			before, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			// Establish that the actual RPC/Unix journal read stalls and fails
			// without producing a drain checkpoint, before the stop attempt.
			var held *net.UnixConn
			if operation == "watchdog-queued" {
				if err := f.worker.input.Encode(journalEndpointControl{WorkerAction: "drain-held", Authority: f.authority}); err != nil {
					t.Fatal(err)
				}
				// Accept proves Runtime has connected inside its locked journal
				// read. Withhold the challenge until the longer RPC deadline.
				held = journalEndpointAccept(t, listener)
				defer func() { _ = held.Close() }()
			} else if report := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "drain-unavailable", Authority: f.authority}); report.Error == "" {
				t.Fatal("unserved socket did not expose journal outage")
			}
			reason := velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP
			if operation != "cancel" {
				reason = velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE
				journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "expire"})
			} else {
				journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "cancel", Authority: f.authority})
			}
			if held != nil {
				var report journalEndpointReport
				if err := f.worker.reports.Decode(&report); err != nil || report.Error == "" {
					t.Fatalf("held drain did not time out: %+v %v", report, err)
				}
				_ = held.Close()
			}
			deadline := time.Now().Add(2 * time.Second)
			var calls journalEndpointReport
			for {
				calls = journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "stats"})
				if calls.CancelCalls != 0 || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if calls.PrepareCalls != 1 || calls.StartCalls != 1 || calls.CancelCalls != 1 || calls.CancelReason != reason {
				t.Fatalf("unresponsive Node suppressed stop or changed target: %+v", calls)
			}
			journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "stop"})
			journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "drain-unavailable", Authority: f.authority})
			after, err := os.ReadFile(statePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("outage stop or failed drain changed durable state: %v", err)
			}
			status, err := f.owner.Status(t.Context())
			if err != nil || status.Highest != 1 || status.PendingExecutions != 1 {
				t.Fatalf("outage granted durable reuse: %+v %v", status, err)
			}
			f.listener = listener
			restored, restoredDone := startJournalTestServer(t, f, 30*time.Second)
			pending := uint64(2)
			if held != nil {
				pending = 1 // The parent already accepted the held read.
			}
			waitJournalServer(t, restored, func(s JournalServerStats) bool { return s.Accepted >= pending && s.InFlight == 0 })
			if report := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "drain", Authority: f.authority}); !report.Checkpoint {
				t.Fatalf("same owner could not persist actual stopped execution: %+v", report)
			}
			status, err = f.owner.Status(t.Context())
			if err != nil || status.PendingExecutions != 0 {
				t.Fatalf("restored drain remained pending: %+v %v", status, err)
			}
			journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "close", Authority: f.authority})
			if report := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "close"}); !report.SupervisorCompleted {
				t.Fatalf("Runtime close: %+v", report)
			}
			if err := restored.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertJournalServerJoined(t, restored, restoredDone)
			t.Logf("%s reached installed backend once with unserved Node socket; root journal unchanged until restored exact drain", operation)
		})
	}
}

type journalEndpointClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*journalEndpointTimer
}

type journalEndpointTimer struct {
	clock          *journalEndpointClock
	deadline       time.Time
	channel        chan time.Time
	stopped, fired bool
}

func (clock *journalEndpointClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *journalEndpointClock) NewTimer(duration time.Duration) modelruntime.Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &journalEndpointTimer{clock: clock, deadline: clock.now.Add(duration), channel: make(chan time.Time, 1)}
	clock.timers = append(clock.timers, timer)
	return timer
}

func (clock *journalEndpointClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
	for _, timer := range clock.timers {
		if !timer.stopped && !timer.fired && !clock.now.Before(timer.deadline) {
			timer.fired = true
			timer.channel <- clock.now
		}
	}
}

func (timer *journalEndpointTimer) C() <-chan time.Time { return timer.channel }

func (timer *journalEndpointTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	if timer.stopped || timer.fired {
		return false
	}
	timer.stopped = true
	return true
}
