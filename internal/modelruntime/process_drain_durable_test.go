package modelruntime_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestProcessDrainDurableNativeCommands(t *testing.T) {
	commands := nativeDrainCommands(t)
	for _, scenario := range []struct{ component, mode, executable, port string }{
		{"ENCODER", "success", "h3-encoder", "conditioning"},
		{"ENCODER", "hang", "h3-encoder", "conditioning"},
		{"ENCODER", "failure", "h3-encoder", "conditioning"},
		{"CPU_MEDIA", "success", "vela-lab-cpu-thumbnail-mock", "thumbnail"},
	} {
		t.Run(scenario.component+"/"+scenario.mode, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			inputRoot, outputRoot := filepath.Join(root, "inputs"), filepath.Join(root, "outputs")
			for _, path := range []string{inputRoot, outputRoot} {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			binding := runtimeBinding()
			device := modelruntime.DriverDevice{DeviceID: binding.Devices[0].ID, DeviceEpoch: binding.Devices[0].Epoch,
				GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0"}
			if scenario.component == "CPU_MEDIA" {
				device.ResourceClass, device.GPUUUID, device.PCIBDF = "CPU", "", ""
			}
			backend, err := modelruntime.NewProcessBackend(t.Context(), binding, modelruntime.ProcessBackendConfig{
				Component: scenario.component, ModelComponentRevision: "drain-native-test-v1",
				Command: []string{filepath.Join(commands, scenario.executable)}, Environment: []string{"VELA_H3_STAGE_MOCK_MODE=" + scenario.mode},
				LocalDevices: []modelruntime.DriverDevice{device}, ScratchRoot: root, InputRoot: inputRoot, OutputRoot: outputRoot,
				InitializationTimeout: 5 * time.Second, ShutdownTimeout: 2 * time.Second, Stderr: io.Discard,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = backend.Close() })
			directory := privateExecutionStateDirectory(t)
			f := newExecutionDrainFixture(t, directory, backend)
			f.clock.Advance(time.Since(f.clock.Now()))
			var authorities []*velav1.StageAuthority
			retained := make(map[string][]byte)
			for sequence := int64(12); sequence <= 13; sequence++ {
				authority := f.authority(t, 0, sequence)
				spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"` + scenario.port + `":{"required":true}}`)}
				if scenario.component == "CPU_MEDIA" {
					payload := []byte("CPU mock input media")
					digest := sha256.Sum256(payload)
					input := runtimeExecutionSpec().Inputs[0]
					input.Sha256, input.SizeBytes = digest[:], int64(len(payload))
					path := filepath.Join(inputRoot, "stage-runs", authority.StageRunId, "inputs", input.StageArtifactId, hex.EncodeToString(digest[:])+".bin")
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, payload, 0o600); err != nil {
						t.Fatal(err)
					}
					spec.Inputs = []*velav1.StageInputArtifact{input}
				}
				digest, err := stageauthority.ExecutionSpecDigest(spec)
				if err != nil {
					t.Fatal(err)
				}
				authority.ExecutionSpecDigest = digest[:]
				authority, err = f.signer.Sign(authority)
				if err != nil {
					t.Fatal(err)
				}
				authorities = append(authorities, authority)
				prepared, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: spec})
				if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("prepare: %v %v", prepared, err)
				}
				assertExecutionDrainCheckpoint(t, f.supervisor, authority, false)
				started, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
				if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("start: %v %v", started, err)
				}
				if scenario.mode == "hang" {
					canceled, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
					if err != nil || !canceled.GetCancellationAcknowledged() {
						t.Fatalf("cancel: %v %v", canceled, err)
					}
				}
				status, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
				if err != nil || status.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("status: %v %v", status, err)
				}
				if scenario.mode == "success" {
					sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
					if err != nil || sealed.GetReceipt() == nil {
						t.Fatalf("durable Seal: %v %v", sealed, err)
					}
					manifest, err := stageartifact.ParseLocalOutputManifestV1(sealed.GetReceipt().GetOutputManifestJson())
					if err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(outputRoot, manifest.LocalLocator)
					payload, err := os.ReadFile(path)
					if err != nil || sha256.Sum256(payload) != manifest.PayloadSHA256 {
						t.Fatalf("sealed payload: %v", err)
					}
					retained[path] = payload
				}
				assertExecutionDrainCheckpoint(t, f.supervisor, authority, true)
				if err := backend.Err(); err != nil {
					t.Fatalf("drain lost residency: %v", err)
				}
			}
			f.supervisor.Close()
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			for path, expected := range retained {
				if payload, err := os.ReadFile(path); err != nil || !bytes.Equal(payload, expected) {
					t.Fatalf("drain/reuse/shutdown modified sealed output: %v", err)
				}
			}
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
			for _, authority := range authorities {
				assertExecutionDrainCheckpoint(t, recovered.supervisor, authority, true)
			}
			prepareFloorRuntime(t, recovered.supervisor, recovered.authority(t, 0, 14))
		})
	}
}

func nativeDrainCommands(t *testing.T) string {
	t.Helper()
	if directory := os.Getenv("VELA_TEST_DRAIN_COMMAND_DIRECTORY"); directory != "" {
		return directory
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source unavailable")
	}
	directory := t.TempDir()
	for name, pkg := range map[string]string{"h3-encoder": "./cmd/vela-h3-stage-mock", "vela-lab-cpu-thumbnail-mock": "./cmd/vela-lab-cpu-thumbnail-mock"} {
		command := exec.CommandContext(t.Context(), "go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", filepath.Join(directory, name), pkg)
		command.Dir = filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build native driver: %v\n%s", err, output)
		}
	}
	return directory
}
