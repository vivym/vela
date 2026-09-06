package modelruntime_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestModelRuntimeRenewalRecoveryWithCompiledH3Process(t *testing.T) {
	commands := nativeDrainCommands(t)
	for _, outcome := range []string{"not-applied", "applied-response-lost"} {
		t.Run(outcome, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"inputs", "outputs"} {
				if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			binding := runtimeBinding()
			process, err := modelruntime.NewProcessBackend(t.Context(), binding, modelruntime.ProcessBackendConfig{
				Component: "ENCODER", ModelComponentRevision: "renewal-recovery-cpu-test-v1",
				Command: []string{filepath.Join(commands, "h3-encoder")}, Environment: []string{"VELA_H3_STAGE_MOCK_MODE=hang"},
				LocalDevices: []modelruntime.DriverDevice{{DeviceID: binding.Devices[0].ID, DeviceEpoch: binding.Devices[0].Epoch,
					GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0"}},
				ScratchRoot: root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
				InitializationTimeout: 5 * time.Second, ShutdownTimeout: 2 * time.Second, Stderr: io.Discard,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = process.Close() })
			backend := &renewalRecoveryProcess{ProcessBackend: process, applyRenewal: outcome == "applied-response-lost"}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			f.clock.Advance(time.Since(f.clock.Now()))
			first := f.authority(t, 0, 12)
			spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"seed":17}`), ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`)}
			digest, err := stageauthority.ExecutionSpecDigest(spec)
			if err != nil {
				t.Fatal(err)
			}
			first.ExecutionSpecDigest = digest[:]
			first, err = f.signer.Sign(first)
			if err != nil {
				t.Fatal(err)
			}
			if response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: first, ExecutionSpec: spec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("prepare CPU process: %v %v", response, err)
			}
			if response, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: first}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("start CPU process: %v %v", response, err)
			}
			f.clock.Advance(time.Second)
			renewed := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal response was not lost: %v %v", response, err)
			}
			if response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: renewed, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP}); err != nil || !response.GetCancellationAcknowledged() {
				t.Fatalf("CPU process renewal could not recover cancellation: %v %v", response, err)
			}
			actual := first
			if backend.applyRenewal {
				actual = renewed
			}
			discovered, err := f.supervisor.InspectAllocationExecution(t.Context(), allocationInspectionRequest(renewed))
			if err != nil || !proto.Equal(discovered.GetObservedAuthority(), actual) {
				t.Fatalf("CPU process backend identity discovery: %v %v", discovered, err)
			}
			actual = discovered.GetObservedAuthority()
			read, err := f.supervisor.InspectExecution(t.Context(), inspectionRequest(actual))
			if err != nil || !read.GetKnown() || read.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
				t.Fatalf("CPU process exact identity was lost: %v %v", read, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, actual, false)
			checkpoint, err := f.supervisor.DrainExecution(t.Context(), actual)
			if err != nil || checkpoint == nil || !proto.Equal(checkpoint.Authority, actual) {
				t.Fatalf("CPU process could not checkpoint its actual drain: %+v %v", checkpoint, err)
			}
			if readiness, err := process.Probe(t.Context(), velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP); err != nil || !readiness.Ready {
				t.Fatalf("identity reconciliation unloaded the resident CPU process: %+v %v", readiness, err)
			}
			next := f.authority(t, 0, 13)
			next.ExecutionSpecDigest = digest[:]
			next, err = f.signer.Sign(next)
			if err != nil {
				t.Fatal(err)
			}
			if response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: spec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("drained CPU process could not prepare its next allocation: %v %v", response, err)
			}
		})
	}
}

type renewalRecoveryProcess struct {
	*modelruntime.ProcessBackend
	applyRenewal bool
}

func (backend *renewalRecoveryProcess) Status(ctx context.Context, authority stageauthority.Verified) (modelruntime.BackendStatus, error) {
	if backend.applyRenewal {
		if _, err := backend.ProcessBackend.Status(ctx, authority); err != nil {
			return modelruntime.BackendStatus{}, err
		}
	}
	return modelruntime.BackendStatus{}, errors.New("injected native process renewal response loss")
}
