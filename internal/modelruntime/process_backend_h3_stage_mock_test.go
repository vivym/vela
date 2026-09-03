package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageartifact"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestProcessBackendExecutesH3StageMockCommand(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repository := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize process backend root: %v", err)
	}
	commandPath := filepath.Join(root, "h3-encoder")
	command := exec.Command("go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", commandPath, "./cmd/vela-h3-stage-mock")
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build native H3 Stage mock command: %v\n%s", err, output)
	}
	inputRoot := filepath.Join(root, "inputs")
	outputRoot := filepath.Join(root, "outputs")
	for _, directory := range []string{inputRoot, outputRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatalf("create process backend directory: %v", err)
		}
	}
	var stderr bytes.Buffer
	backend, err := NewProcessBackend(
		context.Background(), processBackendBinding(),
		ProcessBackendConfig{
			Component: "ENCODER", ModelComponentRevision: "h3-stage-mock-v1",
			Command: []string{commandPath},
			LocalDevices: []DriverDevice{{
				DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
				GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
			}},
			ScratchRoot: root, InputRoot: inputRoot, OutputRoot: outputRoot,
			InitializationTimeout: 10 * time.Second, ShutdownTimeout: 5 * time.Second,
			Stderr: &stderr,
		},
	)
	if err != nil {
		t.Fatalf("start H3 Stage mock ProcessBackend: %v; stderr=%s", err, stderr.String())
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = backend.Close()
		}
	})
	probe, err := backend.Probe(
		context.Background(),
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
	)
	if err != nil || !probe.Ready || !bytes.Contains(probe.Evidence, []byte("ENCODER")) {
		t.Fatalf("probe H3 Stage mock = %#v error=%v; stderr=%s", probe, err, stderr.String())
	}
	authority := processBackendAuthority(1)
	if err := backend.Prepare(context.Background(), authority, &velav1.StageExecutionSpec{
		ParametersJson:             []byte(`{"seed":17}`),
		ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
	}); err != nil {
		t.Fatalf("prepare H3 Stage mock: %v; stderr=%s", err, stderr.String())
	}
	renewed := authority
	renewed.Authority = proto.Clone(authority.Authority).(*velav1.StageAuthority)
	renewed.Authority.StageVersion++
	renewed.Digest = sha256.Sum256([]byte("renewed H3 Stage mock authority"))
	if err := backend.Start(context.Background(), renewed); err != nil {
		t.Fatalf("start H3 Stage mock: %v; stderr=%s", err, stderr.String())
	}
	status, err := backend.Status(context.Background(), renewed)
	if err != nil || status.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY {
		t.Fatalf("H3 Stage mock status=%#v error=%v; stderr=%s", status, err, stderr.String())
	}
	sealed, err := backend.Seal(context.Background(), renewed)
	if err != nil || sealed.TotalSizeBytes <= 0 {
		t.Fatalf("seal H3 Stage mock=%#v error=%v; stderr=%s", sealed, err, stderr.String())
	}
	var manifest struct {
		OutputPort   string `json:"output_port"`
		ContentType  string `json:"content_type"`
		SizeBytes    int64  `json:"size_bytes"`
		LocalLocator string `json:"local_locator"`
	}
	if err := json.Unmarshal(sealed.OutputManifestJSON, &manifest); err != nil ||
		manifest.OutputPort != "conditioning" || manifest.ContentType != "application/x-minimax-h3-encoder" ||
		manifest.SizeBytes != sealed.TotalSizeBytes || manifest.LocalLocator == "" {
		t.Fatalf("H3 Stage mock manifest=%s error=%v", sealed.OutputManifestJSON, err)
	}
	parsed, err := stageartifact.ParseLocalOutputManifestV1(sealed.OutputManifestJSON)
	if err != nil {
		t.Fatalf("parse H3 Stage mock LocalOutputManifestV1: %v", err)
	}
	payload, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(manifest.LocalLocator)))
	if err != nil {
		t.Fatalf("read H3 Stage mock output: %v", err)
	}
	if digest := sha256.Sum256(payload); digest != parsed.PayloadSHA256 || int64(len(payload)) != parsed.SizeBytes {
		t.Fatal("H3 Stage mock output does not match LocalOutputManifestV1")
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("close H3 Stage mock ProcessBackend: %v; stderr=%s", err, stderr.String())
	}
	closed = true
}
