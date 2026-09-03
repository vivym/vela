package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestComponentFromExecutable(t *testing.T) {
	for name, want := range map[string]string{
		"h3-encoder": "ENCODER", "h3-dit": "DIT", "h3-vae-decoder": "VAE_DECODER",
	} {
		got, err := componentFromExecutable(name)
		if err != nil || got != want {
			t.Fatalf("componentFromExecutable(%q)=%q error=%v want=%q", name, got, err, want)
		}
	}
	if _, err := componentFromExecutable("vela-h3-stage-mock"); err == nil {
		t.Fatal("componentFromExecutable accepted an unbound executable")
	}
}

func TestCommandHandlesSIGTERMAfterPublishingUnsealedOutput(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repository := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	buildRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve build root: %v", err)
	}
	commandPath := filepath.Join(buildRoot, "h3-encoder")
	command := exec.Command(
		"go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false",
		"-o", commandPath, "./cmd/vela-h3-stage-mock",
	)
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build H3 Stage mock command: %v\n%s", err, output)
	}

	for _, testCase := range []struct {
		name          string
		blockCleanup  bool
		wantExitError bool
	}{
		{name: "clean cancellation"},
		{name: "cleanup failure", blockCleanup: true, wantExitError: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatalf("resolve runtime root: %v", err)
			}
			inputRoot := filepath.Join(root, "inputs")
			outputRoot := filepath.Join(root, "outputs")
			for _, directory := range []string{inputRoot, outputRoot} {
				if err := os.Mkdir(directory, 0o700); err != nil {
					t.Fatalf("create runtime directory: %v", err)
				}
			}
			process := exec.Command(commandPath)
			process.Env = append(os.Environ(),
				"VELA_MODEL_DRIVER_PROTOCOL=stdio-json-v1",
				"VELA_H3_STAGE_MOCK_MODE=success",
			)
			stdin, err := process.StdinPipe()
			if err != nil {
				t.Fatalf("open command stdin: %v", err)
			}
			stdout, err := process.StdoutPipe()
			if err != nil {
				t.Fatalf("open command stdout: %v", err)
			}
			var stderr bytes.Buffer
			process.Stderr = &stderr
			if err := process.Start(); err != nil {
				t.Fatalf("start H3 Stage mock command: %v", err)
			}
			processDone := false
			t.Cleanup(func() {
				if !processDone {
					_ = process.Process.Kill()
					_ = process.Wait()
				}
			})
			identity := commandStageIdentity()
			specification, err := proto.MarshalOptions{Deterministic: true}.Marshal(
				&velav1.StageExecutionSpec{
					ParametersJson:             []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				},
			)
			if err != nil {
				t.Fatalf("marshal command execution spec: %v", err)
			}
			encoder := json.NewEncoder(stdin)
			decoder := json.NewDecoder(stdout)
			for _, request := range []any{
				commandInitializeRequest(root, inputRoot, outputRoot),
				map[string]any{
					"schema_version": 1, "request_id": 2, "operation": "prepare",
					"prepare": map[string]any{"identity": identity, "execution_spec": specification},
				},
				map[string]any{
					"schema_version": 1, "request_id": 3, "operation": "start",
					"stage": map[string]any{"identity": identity},
				},
			} {
				if err := encoder.Encode(request); err != nil {
					t.Fatalf("write command request: %v", err)
				}
				var response map[string]any
				if err := decoder.Decode(&response); err != nil {
					t.Fatalf("read command response: %v; stderr=%s", err, stderr.String())
				}
				if acknowledged, _ := response["acknowledged"].(bool); !acknowledged {
					t.Fatalf("command response=%#v; stderr=%s", response, stderr.String())
				}
			}
			outputPath := filepath.Join(
				outputRoot, identity["stage_attempt_id"].(string), "conditioning.bin",
			)
			if _, err := os.Stat(outputPath); err != nil {
				t.Fatalf("stat published output: %v", err)
			}
			if testCase.blockCleanup {
				if err := os.Remove(outputPath); err != nil {
					t.Fatalf("replace published output: %v", err)
				}
				if err := os.Mkdir(outputPath, 0o700); err != nil {
					t.Fatalf("create cleanup blocker: %v", err)
				}
				if err := os.WriteFile(filepath.Join(outputPath, "blocker"), []byte("block cleanup"), 0o600); err != nil {
					t.Fatalf("write cleanup blocker: %v", err)
				}
			}
			if err := process.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatalf("signal H3 Stage mock command: %v", err)
			}
			waited := make(chan error, 1)
			go func() { waited <- process.Wait() }()
			select {
			case waitErr := <-waited:
				processDone = true
				if testCase.wantExitError != (waitErr != nil) {
					t.Fatalf("command wait error=%v stderr=%s", waitErr, stderr.String())
				}
			case <-time.After(3 * time.Second):
				_ = process.Process.Kill()
				<-waited
				processDone = true
				t.Fatal("H3 Stage mock command did not stop after SIGTERM")
			}
			if testCase.blockCleanup {
				if !strings.Contains(stderr.String(), "remove unsealed H3 Stage mock output") {
					t.Fatalf("cleanup failure stderr=%q", stderr.String())
				}
			} else if _, err := os.Lstat(filepath.Dir(outputPath)); !os.IsNotExist(err) {
				t.Fatalf("unsealed output remains after SIGTERM: %v", err)
			}
		})
	}
}

func commandInitializeRequest(root, inputRoot, outputRoot string) map[string]any {
	return map[string]any{
		"schema_version": 1, "request_id": 1, "operation": "initialize",
		"initialize": map[string]any{
			"worker_instance_id": "49000000-0000-0000-0000-000000000001", "worker_instance_epoch": 1,
			"worker_member_id": "49000000-0000-0000-0000-000000000002", "worker_member_epoch": 1,
			"device_set_digest":  "11" + string(bytes.Repeat([]byte("0"), 62)),
			"devices":            []any{map[string]any{"id": "49000000-0000-0000-0000-000000000003", "epoch": 1}},
			"membership_digest":  "22" + string(bytes.Repeat([]byte("0"), 62)),
			"members":            []any{map[string]any{"id": "49000000-0000-0000-0000-000000000002", "epoch": 1}},
			"model_residency_id": "49000000-0000-0000-0000-000000000004",
			"runtime_identity":   "ENCODER-mock-v1", "model_runtime_epoch": 1,
			"stage_profile_revision_id": "49000000-0000-0000-0000-000000000005",
			"component":                 "ENCODER", "model_component_revision": "h3-mock-v1",
			"local_devices": []any{map[string]any{
				"device_id": "49000000-0000-0000-0000-000000000003", "device_epoch": 1,
				"gpu_uuid": "GPU-00000000-0000-0000-0000-000000000001", "pci_bdf": "0000:41:00.0",
			}},
			"scratch_root": root, "input_root": inputRoot, "output_root": outputRoot,
		},
	}
}

func commandStageIdentity() map[string]any {
	return map[string]any{
		"authority_digest": "33" + string(bytes.Repeat([]byte("0"), sha256.Size*2-2)),
		"job_id":           "49100000-0000-0000-0000-000000000001",
		"attempt_id":       "49200000-0000-0000-0000-000000000001",
		"stage_run_id":     "49300000-0000-0000-0000-000000000001",
		"stage_attempt_id": "49400000-0000-0000-0000-000000000001",
		"stage_lease_id":   "49500000-0000-0000-0000-000000000001",
		"attempt_fence":    2, "stage_fence": 3, "stage_version": 4,
		"stage_profile_revision_id": "49000000-0000-0000-0000-000000000005",
	}
}
