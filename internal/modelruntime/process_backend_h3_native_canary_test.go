package modelruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/h3request"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// Opt-in Linux GPU integration. This uses real ProcessBackends and weights but
// does not constitute a scheduled Vela Job or a production Launch Receipt.
func TestFastH3NativeGPUCanary(t *testing.T) {
	root := os.Getenv("VELA_H3_NATIVE_CANARY_ROOT")
	if root == "" {
		t.Skip("set VELA_H3_NATIVE_CANARY_ROOT for the real GPU canary")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("canary root must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	source := canonicalConformancePath(t, os.Getenv("VELA_FAST_H3_SOURCE_ROOT"))
	python := absoluteConformancePath(t, os.Getenv("VELA_FAST_H3_PYTHON"))
	gpu, bdf := os.Getenv("VELA_H3_CANARY_GPU_UUID"), os.Getenv("VELA_H3_CANARY_GPU_BDF")
	if !driverGPUUUIDPattern.MatchString(gpu) || !driverPCIBDFPattern.MatchString(bdf) {
		t.Fatal("explicit GPU UUID and BDF are required")
	}
	seed, duration := int64(17), 5.0
	frozen, err := h3request.Freeze("A red sailboat glides across a calm lake at sunrise, soft water sounds.", "balanced", "h3-native-canary", h3request.Request{
		Task: "t2va", Seed: &seed, Target: h3request.Target{ShortEdge: 768, AspectRatio: "16:9", DurationSeconds: &duration}, Sampling: h3request.Sampling{NumInferenceSteps: 20, Quality: "lossless"},
	})
	if err != nil {
		t.Fatal(err)
	}
	parameters, err := json.Marshal(frozen.Parameters)
	if err != nil {
		t.Fatal(err)
	}
	var outputs []fastH3ConformanceOutput
	for index, component := range []string{"ENCODER", "DIT", "VAE_DECODER"} {
		t.Run(component, func(t *testing.T) {
			if len(outputs) != index {
				t.Fatal("upstream canary stage failed")
			}
			componentRoot := filepath.Join(root, component)
			inputRoot, outputRoot := filepath.Join(componentRoot, "inputs"), filepath.Join(componentRoot, "outputs")
			for _, dir := range []string{componentRoot, inputRoot, outputRoot} {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			warmupInputs := []map[string]any{}
			if index > 0 {
				previous := outputs[index-1]
				warmupInputs = append(warmupInputs, map[string]any{"path": previous.path, "sha256": previous.PayloadSHA256, "size_bytes": previous.SizeBytes})
			}
			warmup, err := json.Marshal(map[string]any{"schema": "fast_h3.warmup/v1", "component": component, "parameters": json.RawMessage(parameters), "inputs": warmupInputs})
			if err != nil {
				t.Fatal(err)
			}
			warmupPath := filepath.Join(componentRoot, "warmup.json")
			if err := os.WriteFile(warmupPath, warmup, 0600); err != nil {
				t.Fatal(err)
			}
			log, err := os.OpenFile(filepath.Join(componentRoot, "driver.log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := log.Close(); err != nil {
					t.Errorf("close driver log: %v", err)
				}
			}()
			backend, err := NewProcessBackend(context.Background(), processBackendBinding(), ProcessBackendConfig{
				Component: component, ModelComponentRevision: "h3-native-0de6ff6-6dbcc182-v1",
				Command: []string{python, "-B", "-m", "fast_h3.vela.driver", "--component", component, "--runtime-factory", "fast_h3.vela.h3_runtime:create_runtime"},
				Environment: []string{
					"PYTHONPATH=" + filepath.Join(source, "src") + ":" + filepath.Join(source, "sglang/python"),
					"PATH=" + filepath.Dir(python) + ":/usr/local/cuda/bin:/usr/local/bin:/usr/bin:/bin",
					"XDG_CACHE_HOME=" + filepath.Join(componentRoot, "cache"),
					"TRITON_CACHE_DIR=" + filepath.Join(componentRoot, "triton-cache"),
					"TORCHINDUCTOR_CACHE_DIR=" + filepath.Join(componentRoot, "torch-cache"),
					"CUDA_CACHE_PATH=" + filepath.Join(componentRoot, "cuda-cache"),
					"CUDA_VISIBLE_DEVICES=" + gpu, "NVIDIA_VISIBLE_DEVICES=" + gpu,
					"FAST_H3_MODEL_PATH=/srv/models/MiniMaxAI/MiniMax-H3", "FAST_H3_MODEL_VARIANT=fl2va",
					"FAST_H3_DIT_PATH=/srv/models/MiniMaxAI/MiniMax-H3-int8-v5-convrot-hardened/transformer",
					"FAST_H3_ENCODER_PATH=/srv/models/MiniMax-H3-encoder-int8",
					"FAST_H3_ADALN_PATH=/srv/models/MiniMax-H3-adaln-table-hardened/steps20.safetensors",
					"FAST_H3_ARTIFACT_ROOT=/srv/models",
					"FAST_H3_MODEL_MANIFEST_PATH=" + filepath.Join(source, "deploy/k8s/h3-disaggregated/node19-model-manifest-v5.json"),
					"FAST_H3_MODEL_MANIFEST_SHA256=6eac1f04b601ebfd5d9cf54a130d9c3b7e5a13549fa1a8517bea2730657a28c0",
					"FAST_H3_MASTER_PORT=29683", "FAST_H3_WARMUP_SPEC_PATH=" + warmupPath,
					"TMPDIR=" + componentRoot, "OMP_NUM_THREADS=8", "TOKENIZERS_PARALLELISM=false",
				},
				LocalDevices: []DriverDevice{{DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7, GPUUUID: gpu, PCIBDF: bdf}},
				ScratchRoot:  componentRoot, InputRoot: inputRoot, OutputRoot: outputRoot, InitializationTimeout: 20 * time.Minute, ShutdownTimeout: 30 * time.Second, Stderr: log,
			})
			if err != nil {
				t.Fatalf("initialize real %s: %v; see %s", component, err, log.Name())
			}
			defer func() { _ = backend.Close() }()
			authority := processBackendAuthority(byte(index + 1))
			authority.Authority.ExecutionSequence = 1
			port := []string{"conditioning", "latent", "video"}[index]
			expected, _ := json.Marshal(map[string]any{port: map[string]bool{"required": true}})
			spec := &velav1.StageExecutionSpec{ParametersJson: parameters, ExpectedOutputManifestJson: expected}
			if index > 0 {
				spec.Inputs = []*velav1.StageInputArtifact{materializeFastH3StageInput(t, inputRoot, authority, "73000000-0000-0000-0000-000000000001", "native-v1", "74000000-0000-0000-0000-000000000001", outputs[index-1])}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			defer cancel()
			if err := backend.Prepare(ctx, authority, spec); err != nil {
				t.Fatal(err)
			}
			if err := backend.Start(ctx, authority); err != nil {
				t.Fatal(err)
			}
			for {
				status, err := backend.Status(ctx, authority)
				if err != nil {
					t.Fatal(err)
				}
				if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY {
					break
				}
				if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
					t.Fatalf("real execution failed: %+v", status)
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Second):
				}
			}
			sealed, err := backend.Seal(ctx, authority)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := backend.DrainExecution(ctx, authority); err != nil {
				t.Fatal(err)
			}
			var output fastH3ConformanceOutput
			if err := json.Unmarshal(sealed.OutputManifestJSON, &output); err != nil {
				t.Fatal(err)
			}
			output.path = filepath.Join(outputRoot, filepath.FromSlash(output.LocalLocator))
			if index == 2 && output.ContentType != "video/mp4" {
				t.Fatalf("expected encoded MP4, got %s", output.ContentType)
			}
			if err := os.WriteFile(filepath.Join(componentRoot, "sealed.json"), sealed.OutputManifestJSON, 0600); err != nil {
				t.Fatal(err)
			}
			outputs = append(outputs, output)
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			t.Logf("sealed %s bytes=%d sha256=%s", component, output.SizeBytes, output.PayloadSHA256)
		})
	}
}
