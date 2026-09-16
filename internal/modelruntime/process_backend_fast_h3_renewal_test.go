package modelruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// Exercise the production Go/Python wire contract without loading model weights.
func TestProcessBackendFastH3AuthorityRenewal(t *testing.T) {
	source, python := os.Getenv("VELA_FAST_H3_SOURCE_ROOT"), os.Getenv("VELA_FAST_H3_PYTHON")
	if source == "" || python == "" {
		t.Skip("set fast-h3 source and Python for driver contract test")
	}
	source = canonicalConformancePath(t, source)
	root := canonicalConformancePath(t, t.TempDir())
	for _, name := range []string{"inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	stderr := &bytes.Buffer{}
	b, err := NewProcessBackend(t.Context(), processBackendBinding(), ProcessBackendConfig{
		Component: "ENCODER", ModelComponentRevision: "renewal-contract-v1",
		Command:      []string{python, "-m", "fast_h3.vela.driver", "--component", "ENCODER", "--runtime-factory", "tests.fake_component_runtime:create_runtime"},
		Environment:  []string{"PYTHONPATH=" + filepath.Join(source, "src") + string(os.PathListSeparator) + source},
		LocalDevices: []DriverDevice{{DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7, GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0"}},
		ScratchRoot:  root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
		InitializationTimeout: 5 * time.Second, ShutdownTimeout: 3 * time.Second, Stderr: stderr,
	})
	if err != nil {
		t.Fatalf("initialize: %v; %s", err, stderr)
	}
	t.Cleanup(func() { _ = b.Close() })
	original := processBackendAuthority(1)
	original.Authority.ExecutionSequence = 1
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"seed":7}`), ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`)}
	if err := b.Prepare(t.Context(), original, spec); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context(), original); err != nil {
		t.Fatal(err)
	}
	renewed := original
	renewed.Authority = proto.Clone(original.Authority).(*velav1.StageAuthority)
	renewed.Authority.StageVersion++
	renewed.Digest[0] ^= 0xff
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, err := b.Status(t.Context(), renewed)
		if err != nil {
			t.Fatalf("Control start renewal rejected: %v; %s", err, stderr)
		}
		if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("output never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := b.Status(t.Context(), original); err == nil {
		t.Fatal("retired authority accepted")
	}
	if _, err := b.Seal(t.Context(), renewed); err != nil {
		t.Fatal(err)
	}
	inspection, err := b.InspectExecution(t.Context(), renewed)
	if err != nil || !inspection.Known {
		t.Fatalf("renewed inspection: %+v %v", inspection, err)
	}
	inspection, err = b.InspectExecution(t.Context(), original)
	if err != nil || inspection.Known {
		t.Fatalf("retired inspection: %+v %v", inspection, err)
	}
	if _, err := b.DrainExecution(t.Context(), renewed); err != nil {
		t.Fatal(err)
	}
}
