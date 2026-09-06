package modelruntime_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestModelRuntimeCancellationKeepsInstalledH3ProcessIdentity(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(nativeDrainCommands(t), "h3-encoder")
	for _, name := range []string{"inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	binding := runtimeBinding()
	backend, err := modelruntime.NewProcessBackend(t.Context(), binding, modelruntime.ProcessBackendConfig{
		Component: "ENCODER", ModelComponentRevision: "h3-stage-mock-v1", Command: []string{binary},
		LocalDevices: []modelruntime.DriverDevice{{DeviceID: binding.Devices[0].ID, DeviceEpoch: binding.Devices[0].Epoch,
			GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0"}},
		ScratchRoot: root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
		InitializationTimeout: 10 * time.Second, ShutdownTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	clock := newManualClock(time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	service := newRuntimeService(t, clock, validator, binding, backend)
	client, _ := serveRuntime(t, service)
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"seed":17}`), ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`)}
	digest, err := stageauthority.ExecutionSpecDigest(spec)
	if err != nil {
		t.Fatal(err)
	}
	installed := signRuntimeAuthority(t, signer, clock.Now())
	installed.ExecutionSpecDigest = digest[:]
	installed, err = signer.Sign(installed)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: installed, ExecutionSpec: spec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare process: %v %v", prepared, err)
	}
	started, err := client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: installed})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("start process: %v %v", started, err)
	}
	clock.Advance(time.Second)
	successor := renewWatchdogAuthority(t, signer, installed, clock.Now())
	canceled, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: successor, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("cancel process with compatible successor: %v %v", canceled, err)
	}
	for _, authority := range []*velav1.StageAuthority{installed, successor} {
		read, err := client.InspectExecution(t.Context(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: authority})
		known := authority == installed
		if err != nil || read.GetKnown() != known || known && read.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
			t.Fatalf("process cancellation replaced exact identity: %v %v", read, err)
		}
	}
	if probe, err := backend.Probe(t.Context(), velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP); err != nil || !probe.Ready {
		t.Fatalf("cancellation unloaded resident process: %+v %v", probe, err)
	}
}
