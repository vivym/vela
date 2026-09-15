package modelruntime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// Exercises the actual Go clients and Python subprocess, including cancellation
// while the resident component is busy. This is protocol evidence, not GPU proof.
func TestFastH3DriverInspectionAndWriterDrain(t *testing.T) {
	source, python := os.Getenv("VELA_FAST_H3_SOURCE_ROOT"), os.Getenv("VELA_FAST_H3_PYTHON")
	if source == "" || python == "" {
		t.Skip("set VELA_FAST_H3_SOURCE_ROOT and VELA_FAST_H3_PYTHON")
	}
	source = canonicalConformancePath(t, source)
	root := canonicalConformancePath(t, t.TempDir())
	for _, dir := range []string{"inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	stderr := &bytes.Buffer{}
	backend, err := NewProcessBackend(t.Context(), processBackendBinding(), ProcessBackendConfig{
		Component: "ENCODER", ModelComponentRevision: "minimax-h3-encoder-r1",
		Command:      []string{python, "-B", "-m", "fast_h3.vela.driver", "--component", "ENCODER", "--runtime-factory", "tests.fake_component_runtime:create_runtime"},
		Environment:  []string{"PYTHONPATH=" + filepath.Join(source, "src") + string(os.PathListSeparator) + source},
		LocalDevices: []DriverDevice{{DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7, GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0"}},
		ScratchRoot:  root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
		InitializationTimeout: 10 * time.Second, ShutdownTimeout: 5 * time.Second, Stderr: stderr,
	})
	if err != nil {
		t.Fatalf("start driver: %v %s", err, stderr.String())
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	authority := processBackendAuthority(1)
	authority.Authority.ExecutionSequence = 1
	if observation, err := backend.InspectExecution(ctx, authority); err != nil || observation.Known {
		t.Fatalf("unknown: %+v %v", observation, err)
	}
	if _, err := backend.DrainExecution(ctx, authority); err == nil {
		t.Fatal("unknown execution drained")
	}
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"seed":17,"block_until_cancel":true}`), ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`)}
	if err := backend.Prepare(ctx, authority, spec); err != nil {
		t.Fatal(err)
	}
	if err := backend.Start(ctx, authority); err != nil {
		t.Fatal(err)
	}
	if observation, err := backend.InspectExecution(ctx, authority); err != nil || !observation.Known || observation.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING {
		t.Fatalf("running: %+v %v", observation, err)
	}
	if _, err := backend.DrainExecution(ctx, authority); err == nil {
		t.Fatal("running writer drained")
	}
	if err := backend.Cancel(ctx, authority, velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_AGENT_SHUTDOWN); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := backend.Status(ctx, authority)
		if err != nil {
			t.Fatal(err)
		}
		if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancel did not join writer")
		}
		time.Sleep(10 * time.Millisecond)
	}
	wrong := processBackendAuthority(1)
	wrong.Authority.ExecutionSequence = 2
	if _, err := backend.DrainExecution(ctx, wrong); err == nil {
		t.Fatal("wrong sequence drained")
	}
	if _, err := backend.DrainExecution(ctx, authority); err != nil {
		t.Fatal(err)
	}
	if err := backend.Start(ctx, authority); err == nil {
		t.Fatal("drained writer restarted")
	}
	next := processBackendAuthority(2)
	next.Authority.ExecutionSequence = 2
	spec.ParametersJson = []byte(`{"seed":18}`)
	if err := backend.Prepare(ctx, next, spec); err != nil {
		t.Fatal(err)
	}
	if err := backend.Start(ctx, next); err != nil {
		t.Fatal(err)
	}
	waitForFastH3Output(t, backend, next, stderr)
	if _, err := backend.DrainExecution(ctx, next); err == nil {
		t.Fatal("unsealed output drained")
	}
	if _, err := backend.Seal(ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.DrainExecution(ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.DrainExecution(ctx, authority); err != nil {
		t.Fatalf("historical exact drain: %v", err)
	}
	if err := backend.Prepare(ctx, authority, spec); err == nil {
		t.Fatal("historical execution resurrected")
	}
	if probe, err := backend.Probe(ctx, velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP); err != nil || !probe.Ready {
		t.Fatalf("drain unloaded residency: %+v %v", probe, err)
	}
	fatal := processBackendAuthority(3)
	fatal.Authority.ExecutionSequence = 3
	spec.ParametersJson = []byte(`{"seed":19,"fail_after_cancel":true}`)
	if err := backend.Prepare(ctx, fatal, spec); err != nil {
		t.Fatal(err)
	}
	if err := backend.Start(ctx, fatal); err != nil {
		t.Fatal(err)
	}
	if err := backend.Cancel(ctx, fatal, velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_AGENT_SHUTDOWN); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		status, err := backend.Status(ctx, fatal)
		if err != nil {
			t.Fatal(err)
		}
		if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
			break
		}
		if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
			t.Fatal("cancel hid writer failure")
		}
		if time.Now().After(deadline) {
			t.Fatal("writer failure did not become terminal")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := backend.DrainExecution(ctx, fatal); err == nil {
		t.Fatal("unproven writers drained after cancel")
	}
}
