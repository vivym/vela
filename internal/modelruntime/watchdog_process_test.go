//go:build darwin || linux

package modelruntime_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestModelRuntimeWatchdogStopsBlockedProcessAndChildWriter(t *testing.T) {
	testRuntimeStopsBlockedProcessAndChildWriter(t, false)
}

func TestModelRuntimeCancelStopsBlockedProcessAndChildWriter(t *testing.T) {
	testRuntimeStopsBlockedProcessAndChildWriter(t, true)
}

func testRuntimeStopsBlockedProcessAndChildWriter(t *testing.T, explicitCancel bool) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binding := runtimeBinding()
	backend, err := modelruntime.NewProcessBackend(t.Context(), binding, modelruntime.ProcessBackendConfig{
		Component: "ENCODER", ModelComponentRevision: "watchdog-cpu-test-v1",
		Command:      []string{executable, "-test.run=^TestProcessBackendCleanupDriverHelper$"},
		Environment:  []string{"TEST_PROCESS_BACKEND_CLEANUP=prepare_timeout", "TEST_PROCESS_BACKEND_CLEANUP_ROOT=" + root, "GORACE=atexit_sleep_ms=0"},
		LocalDevices: []modelruntime.DriverDevice{{DeviceID: binding.Devices[0].ID, DeviceEpoch: binding.Devices[0].Epoch, ResourceClass: "CPU"}},
		ScratchRoot:  root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
		InitializationTimeout: 5 * time.Second, ShutdownTimeout: 300 * time.Millisecond, Stderr: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(root, "stop-gate"), nil, 0o600)
		_ = backend.Close()
	})
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	finished := make(chan velav1.ModelRuntimeCommandDecision, 1)
	go func() { finished <- queuedAuthorityCommand(f.services[0], "prepare", f.authorities[0]) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, "prepare-blocked")); err == nil {
			break
		}
		select {
		case decision := <-finished:
			t.Fatalf("Prepare returned before the process blocked: %s", decision)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("CPU process did not receive Prepare")
		}
		time.Sleep(time.Millisecond)
	}
	if explicitCancel {
		canceled := make(chan *velav1.ModelRuntimeServiceCancelStageResponse, 1)
		go func() {
			response, _ := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: f.authorities[0], Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			})
			canceled <- response
		}()
		select {
		case response := <-canceled:
			if response.GetCancellationAcknowledged() || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("process abort fabricated a driver acknowledgement: %v", response)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("CancelStage did not interrupt the actual blocked process call")
		}
	} else {
		f.clock.Advance(time.Minute)
	}
	select {
	case decision := <-finished:
		if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
			t.Fatalf("blocked process did not fail after interruption: %s", decision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interruption did not release the actual blocked process call")
	}
	select {
	case <-backend.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("process group did not finish teardown")
	}
	before, err := os.Stat(filepath.Join(root, "child-writes"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.Stat(filepath.Join(root, "child-writes"))
	if err != nil || before.Size() != after.Size() {
		t.Fatalf("child kept writing after process teardown: %v", err)
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], false)
	other := f.authority(t, 1, 12)
	response, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: other, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("process exit alone released durable execution: %v %v", response, err)
	}
}
