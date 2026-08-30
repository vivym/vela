package h3mockbackend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testWorkerID     = "20000000-0000-0000-0000-000000000001"
	testProfileID    = "30000000-0000-0000-0000-000000000001"
	testOutputSpecID = "80000000-0000-0000-0000-000000000001"
	testRevision     = "vela-h3-mock-v1"
)

var testGPUs = []string{
	"GPU-00000000-0000-0000-0000-000000000001",
	"GPU-00000000-0000-0000-0000-000000000002",
	"GPU-00000000-0000-0000-0000-000000000003",
	"GPU-00000000-0000-0000-0000-000000000004",
	"GPU-00000000-0000-0000-0000-000000000005",
	"GPU-00000000-0000-0000-0000-000000000006",
	"GPU-00000000-0000-0000-0000-000000000007",
	"GPU-00000000-0000-0000-0000-000000000008",
}

func TestRunReportsExactDeviceReadiness(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	resultPath := filepath.Join(root, "result.json")
	writeJSON(t, requestPath, map[string]any{
		"schema_version":                1,
		"cycle_id":                      "10000000-0000-0000-0000-000000000001",
		"worker_id":                     testWorkerID,
		"worker_epoch":                  7,
		"node_identity":                 "h3-mock-node-01",
		"execution_profile_revision_id": testProfileID,
		"inference_backend_revision":    testRevision,
		"deadline":                      "2030-01-01T00:00:00Z",
		"check":                         "DEVICE",
	})
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)

	err := Run(context.Background(), []string{
		"--vela-readiness-check", "DEVICE",
		"--vela-readiness-request", requestPath,
		"--vela-readiness-result", resultPath,
	})
	if err != nil {
		t.Fatalf("Run readiness: %v", err)
	}

	var result struct {
		SchemaVersion int      `json:"schema_version"`
		Check         string   `json:"check"`
		Passed        bool     `json:"passed"`
		EncoderVAEGPU string   `json:"encoder_vae_gpu_uuid"`
		DiTGPUUUIDs   []string `json:"dit_gpu_uuids"`
	}
	content, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if err := json.Unmarshal(content, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.SchemaVersion != 1 || result.Check != "DEVICE" || !result.Passed ||
		result.EncoderVAEGPU != testGPUs[0] || !reflect.DeepEqual(result.DiTGPUUUIDs, testGPUs[1:]) {
		t.Fatalf("device readiness = %#v", result)
	}
	info, err := os.Stat(resultPath)
	if err != nil {
		t.Fatalf("stat result: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("result mode = %04o, want 0600", info.Mode().Perm())
	}
}

func TestRunReportsBackendWarmupAndCanaryReadiness(t *testing.T) {
	tests := []struct {
		check string
		want  map[string]any
	}{
		{
			check: "INFERENCE_BACKEND",
			want: map[string]any{
				"schema_version":             float64(1),
				"check":                      "INFERENCE_BACKEND",
				"passed":                     true,
				"inference_backend_revision": testRevision,
				"loaded":                     true,
			},
		},
		{
			check: "MODEL_WARMUP",
			want: map[string]any{
				"schema_version":                float64(1),
				"check":                         "MODEL_WARMUP",
				"passed":                        true,
				"execution_profile_revision_id": testProfileID,
				"warmed":                        true,
			},
		},
		{
			check: "CANARY",
			want: map[string]any{
				"schema_version": float64(1),
				"check":          "CANARY",
				"passed":         true,
				"output_sha256":  "159936091c96631fa42e0802a4f47a7236770faacfda2d961e51de7d7a85f2ef",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.check, func(t *testing.T) {
			root := t.TempDir()
			requestPath := filepath.Join(root, "request.json")
			resultPath := filepath.Join(root, "result.json")
			writeJSON(t, requestPath, readinessPayload(test.check))
			t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
			t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)

			err := Run(context.Background(), []string{
				"--vela-readiness-check", test.check,
				"--vela-readiness-request", requestPath,
				"--vela-readiness-result", resultPath,
			})
			if err != nil {
				t.Fatalf("Run readiness: %v", err)
			}
			content, err := os.ReadFile(resultPath)
			if err != nil {
				t.Fatalf("read result: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(content, &got); err != nil {
				t.Fatalf("decode result: %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("readiness result = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestRunProducesCompleteMockArtifactSet(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	statusPath := filepath.Join(root, "status.json")
	manifestPath := filepath.Join(root, "manifest.json")
	failurePath := filepath.Join(root, "failure.json")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	requestContent := []byte(`{"client_metadata":{"source":"mock-test"},"generation_count":1,"generation_preset":"balanced","model":"minimax-h3","output_spec":"video-1080p-5s-24fps","prompt":"mock prompt","service_class":"standard"}`)
	writeJSON(t, requestPath, executionRequestPayload(requestContent))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath, outputDir, statusPath, manifestPath, failurePath,
		testOutputSpecID, false,
	))
	if err != nil {
		t.Fatalf("Run execution: %v", err)
	}
	if _, err := os.Stat(failurePath); !os.IsNotExist(err) {
		t.Fatalf("successful execution failure receipt stat error = %v", err)
	}

	var manifest struct {
		SchemaVersion int `json:"schema_version"`
		Outputs       []struct {
			Kind        string `json:"kind"`
			Ordinal     int    `json:"ordinal"`
			Path        string `json:"path"`
			ContentType string `json:"content_type"`
		} `json:"outputs"`
	}
	readJSON(t, manifestPath, &manifest)
	if manifest.SchemaVersion != 1 || len(manifest.Outputs) != 2 {
		t.Fatalf("output manifest = %#v", manifest)
	}
	want := []struct {
		kind        string
		name        string
		contentType string
	}{
		{kind: "VIDEO", name: "video.mp4", contentType: "video/mp4"},
		{kind: "THUMBNAIL", name: "thumbnail.webp", contentType: "image/webp"},
	}
	for index, expected := range want {
		output := manifest.Outputs[index]
		if output.Kind != expected.kind || output.Ordinal != 0 ||
			output.Path != filepath.Join(outputDir, expected.name) ||
			output.ContentType != expected.contentType {
			t.Fatalf("output %d = %#v", index, output)
		}
		info, err := os.Stat(output.Path)
		if err != nil {
			t.Fatalf("stat output %d: %v", index, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 {
			t.Fatalf("output %d info = %#v", index, info)
		}
	}

	var status struct {
		SchemaVersion             int     `json:"schema_version"`
		BackendStage              string  `json:"backend_stage"`
		Sequence                  int64   `json:"sequence"`
		BackendStageProgress      float64 `json:"backend_stage_progress"`
		EstimatedRemainingSeconds int64   `json:"estimated_remaining_seconds"`
	}
	readJSON(t, statusPath, &status)
	if status.SchemaVersion != 1 || status.BackendStage != "mock/finalize" ||
		status.Sequence != 4 || status.BackendStageProgress != 0.95 ||
		status.EstimatedRemainingSeconds != 0 {
		t.Fatalf("final backend status = %#v", status)
	}
}

func TestRunRejectsOutputSpecOutsideConfiguredMockContract(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	writeJSON(t, requestPath, executionRequestPayload([]byte(`{"prompt":"mock prompt"}`)))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath,
		outputDir,
		filepath.Join(root, "status.json"),
		filepath.Join(root, "manifest.json"),
		filepath.Join(root, "failure.json"),
		"80000000-0000-0000-0000-000000000099",
		false,
	))
	if err == nil || err.Error() != "mock execution request does not match configured OutputSpec" {
		t.Fatalf("output spec mismatch error = %v", err)
	}
	entries, readErr := os.ReadDir(outputDir)
	if readErr != nil {
		t.Fatalf("read output directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("output spec mismatch wrote outputs: %#v", entries)
	}
}

func TestRunWritesBoundedInjectedFailureReceipt(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	writeJSON(t, requestPath, executionRequestPayload([]byte(`{"prompt":"private prompt"}`)))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)
	failurePath := filepath.Join(root, "failure.json")
	manifestPath := filepath.Join(root, "manifest.json")

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath,
		outputDir,
		filepath.Join(root, "status.json"),
		manifestPath,
		failurePath,
		testOutputSpecID,
		false,
		"--mock-mode", "failure",
		"--mock-failure-class", "CUDA_OOM",
		"--mock-failure-fingerprint", "mock/cuda-oom/dit",
		"--mock-failure-stage", "mock/encode",
		"--mock-failure-gpu-index", "1",
		"--mock-retry-recommended", "true",
		"--mock-worker-reusable", "false",
	))
	if !errors.Is(err, ErrInjectedFailure) {
		t.Fatalf("injected failure error = %v", err)
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("failure mode manifest stat error = %v", err)
	}
	var failure struct {
		SchemaVersion      int      `json:"schema_version"`
		FailureClass       string   `json:"failure_class"`
		FailureFingerprint string   `json:"failure_fingerprint"`
		ErrorSummary       string   `json:"error_summary"`
		BackendStage       string   `json:"backend_stage"`
		GPUUUIDs           []string `json:"gpu_uuids"`
		RetryRecommended   bool     `json:"retry_recommended"`
		WorkerReusable     bool     `json:"worker_reusable"`
	}
	readJSON(t, failurePath, &failure)
	if failure.SchemaVersion != 1 || failure.FailureClass != "CUDA_OOM" ||
		failure.FailureFingerprint != "mock/cuda-oom/dit" ||
		failure.ErrorSummary != "mock backend injected a configured failure" ||
		failure.BackendStage != "mock/encode" ||
		!reflect.DeepEqual(failure.GPUUUIDs, []string{testGPUs[1]}) ||
		!failure.RetryRecommended || failure.WorkerReusable {
		t.Fatalf("failure receipt = %#v", failure)
	}
	content, readErr := os.ReadFile(failurePath)
	if readErr != nil {
		t.Fatalf("read failure receipt: %v", readErr)
	}
	if bytes.Contains(content, []byte("private prompt")) {
		t.Fatal("failure receipt leaked request content")
	}
}

func TestRunWritesEmptyGPUArrayWhenFailureHasNoImplicatedDevice(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	writeJSON(t, requestPath, executionRequestPayload([]byte(`{"prompt":"private prompt"}`)))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)
	failurePath := filepath.Join(root, "failure.json")

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath,
		outputDir,
		filepath.Join(root, "status.json"),
		filepath.Join(root, "manifest.json"),
		failurePath,
		testOutputSpecID,
		false,
		"--mock-mode", "failure",
	))
	if !errors.Is(err, ErrInjectedFailure) {
		t.Fatalf("injected failure error = %v", err)
	}
	var failure struct {
		GPUUUIDs []string `json:"gpu_uuids"`
	}
	readJSON(t, failurePath, &failure)
	if failure.GPUUUIDs == nil || len(failure.GPUUUIDs) != 0 {
		t.Fatalf("default failure GPU UUIDs = %#v, want an empty JSON array", failure.GPUUUIDs)
	}
}

func TestRunResumeAtomicallyReplacesPartialMockOutput(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	videoPath := filepath.Join(outputDir, "video.mp4")
	if err := os.WriteFile(videoPath, []byte("partial-mock-video"), 0o600); err != nil {
		t.Fatalf("write partial output: %v", err)
	}
	writeJSON(t, requestPath, executionRequestPayload([]byte(`{"prompt":"mock prompt"}`)))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath,
		outputDir,
		filepath.Join(root, "status.json"),
		filepath.Join(root, "manifest.json"),
		filepath.Join(root, "failure.json"),
		testOutputSpecID,
		true,
	))
	if err != nil {
		t.Fatalf("Run resumed execution: %v", err)
	}
	content, err := os.ReadFile(videoPath)
	if err != nil {
		t.Fatalf("read resumed video: %v", err)
	}
	digest := sha256.Sum256(content)
	if got := hex.EncodeToString(digest[:]); got != "d69f5e36470074ff2e23fe1538eba01da9abc41c4a2ad71885102e4f79116658" {
		t.Fatalf("resumed video digest = %s", got)
	}
}

func TestRunHangStopsOnCancellationWithoutPublishingArtifacts(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	writeJSON(t, requestPath, executionRequestPayload([]byte(`{"prompt":"mock prompt"}`)))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)
	statusPath := filepath.Join(root, "status.json")
	manifestPath := filepath.Join(root, "manifest.json")
	failurePath := filepath.Join(root, "failure.json")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	arguments := executionArgumentsForTest(
		requestPath, outputDir, statusPath, manifestPath, failurePath,
		testOutputSpecID, false, "--mock-mode", "hang",
	)
	go func() {
		done <- Run(ctx, arguments)
	}()
	waitForFile(t, statusPath)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("hang cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("hang mode did not stop after cancellation")
	}
	for _, path := range []string{manifestPath, failurePath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("hang mode published %s: %v", path, err)
		}
	}
}

func TestRunRejectsDuplicateReadinessAuthority(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	resultPath := filepath.Join(root, "result.json")
	content := []byte(`{"schema_version":1,"schema_version":1,"cycle_id":"10000000-0000-0000-0000-000000000001","worker_id":"20000000-0000-0000-0000-000000000001","worker_epoch":7,"node_identity":"h3-mock-node-01","execution_profile_revision_id":"30000000-0000-0000-0000-000000000001","inference_backend_revision":"vela-h3-mock-v1","deadline":"2030-01-01T00:00:00Z","check":"DEVICE"}`)
	if err := os.WriteFile(requestPath, content, 0o600); err != nil {
		t.Fatalf("write duplicate request: %v", err)
	}
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	t.Setenv("VELA_RUNNER_BACKEND_REVISION", testRevision)

	err := Run(context.Background(), []string{
		"--vela-readiness-check", "DEVICE",
		"--vela-readiness-request", requestPath,
		"--vela-readiness-result", resultPath,
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate JSON key") {
		t.Fatalf("duplicate readiness authority error = %v", err)
	}
	if _, err := os.Stat(resultPath); !os.IsNotExist(err) {
		t.Fatalf("duplicate request result stat error = %v", err)
	}
}

func TestRunRejectsUnknownNestedDebugDumpAuthorizationField(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	payload := executionRequestPayload([]byte(`{"prompt":"mock prompt"}`))
	spec := payload["execution_spec"].(map[string]any)
	spec["debug_dump_authorization"] = map[string]any{
		"authorization_id": "90000000-0000-0000-0000-000000000001",
		"expires_at": map[string]any{
			"seconds": 1_900_000_000,
			"nanos":   0,
			"unknown": true,
		},
	}
	writeJSON(t, requestPath, payload)
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath,
		outputDir,
		filepath.Join(root, "status.json"),
		filepath.Join(root, "manifest.json"),
		filepath.Join(root, "failure.json"),
		testOutputSpecID,
		false,
	))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("nested unknown field error = %v", err)
	}
}

func TestRunAcceptsStrictDebugDumpAuthorization(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	payload := executionRequestPayload([]byte(`{"prompt":"mock prompt"}`))
	spec := payload["execution_spec"].(map[string]any)
	spec["debug_dump_authorization"] = map[string]any{
		"authorization_id": "90000000-0000-0000-0000-000000000001",
		"expires_at": map[string]any{
			"seconds": 1_900_000_000,
			"nanos":   0,
		},
	}
	writeJSON(t, requestPath, payload)
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath,
		outputDir,
		filepath.Join(root, "status.json"),
		filepath.Join(root, "manifest.json"),
		filepath.Join(root, "failure.json"),
		testOutputSpecID,
		false,
	))
	if err != nil {
		t.Fatalf("Run with debug dump authorization: %v", err)
	}
}

func TestRunRejectsResultOutsideRunnerOwnedRequestDirectory(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	writeJSON(t, requestPath, executionRequestPayload([]byte(`{"prompt":"mock prompt"}`)))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	outsideStatusPath := filepath.Join(t.TempDir(), "status.json")

	err := Run(context.Background(), executionArgumentsForTest(
		requestPath,
		outputDir,
		outsideStatusPath,
		filepath.Join(root, "manifest.json"),
		filepath.Join(root, "failure.json"),
		testOutputSpecID,
		false,
	))
	if err == nil || !strings.Contains(err.Error(), "direct children") {
		t.Fatalf("outside result path error = %v", err)
	}
	if _, err := os.Stat(outsideStatusPath); !os.IsNotExist(err) {
		t.Fatalf("outside result path was written: %v", err)
	}
}

func TestRunResumeStartsWithDocumentedPrepareStage(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	statusPath := filepath.Join(root, "status.json")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	writeJSON(t, requestPath, executionRequestPayload([]byte(`{"prompt":"mock prompt"}`)))
	t.Setenv("CUDA_VISIBLE_DEVICES", strings.Join(testGPUs, ","))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	arguments := executionArgumentsForTest(
		requestPath,
		outputDir,
		statusPath,
		filepath.Join(root, "manifest.json"),
		filepath.Join(root, "failure.json"),
		testOutputSpecID,
		true,
	)
	arguments[1] = "1s"
	go func() { done <- Run(ctx, arguments) }()
	waitForFile(t, statusPath)
	var status backendStatus
	readJSON(t, statusPath, &status)
	if status.BackendStage != "mock/prepare" {
		t.Fatalf("first resumed stage = %q, want mock/prepare", status.BackendStage)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("resumed execution cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("resumed execution did not stop after cancellation")
	}
}

func TestFilesystemInputsRequirePrivateModeAndCurrentOwner(t *testing.T) {
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	outputDir := filepath.Join(root, "outputs")
	writeJSON(t, requestPath, readinessPayload("DEVICE"))
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}

	requestInfo, err := os.Lstat(requestPath)
	if err != nil {
		t.Fatalf("stat request: %v", err)
	}
	outputInfo, err := os.Lstat(outputDir)
	if err != nil {
		t.Fatalf("stat output directory: %v", err)
	}
	ownerUID, err := currentProcessUID()
	if err != nil {
		t.Fatalf("resolve current owner: %v", err)
	}
	wrongOwnerUID := ownerUID ^ 1
	if err := validateJSONInputInfo(requestInfo, wrongOwnerUID); err == nil {
		t.Fatal("request owned by another user was accepted")
	}
	if err := validateOutputDirectoryInfo(outputInfo, wrongOwnerUID); err == nil {
		t.Fatal("output directory owned by another user was accepted")
	}

	if err := os.Chmod(requestPath, 0o640); err != nil {
		t.Fatalf("make request group-readable: %v", err)
	}
	requestInfo, err = os.Lstat(requestPath)
	if err != nil {
		t.Fatalf("restat request: %v", err)
	}
	if err := validateJSONInputInfo(requestInfo, ownerUID); err == nil {
		t.Fatal("group-readable request was accepted")
	}
}

func TestMockMediaFixturesMatchDocumentedOutputSpec(t *testing.T) {
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe is not installed")
	}
	tests := []struct {
		name       string
		path       string
		codec      string
		width      int
		height     int
		duration   string
		frameRate  string
		frameCount string
		format     string
	}{
		{
			name: "video", path: "testdata/video-1080p-5s-24fps.mp4", codec: "h264",
			width: 1920, height: 1080, duration: "5.000000", frameRate: "24/1",
			frameCount: "120", format: "mov,mp4,m4a,3gp,3g2,mj2",
		},
		{
			name: "thumbnail", path: "testdata/thumbnail-320x180.webp", codec: "webp",
			width: 320, height: 180, frameRate: "25/1", format: "webp_pipe",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(
				ffprobe, "-v", "error", "-show_streams", "-show_format", "-of", "json", test.path,
			)
			encoded, err := command.Output()
			if err != nil {
				t.Fatalf("probe mock media: %v", err)
			}
			var document struct {
				Streams []struct {
					CodecName  string `json:"codec_name"`
					Width      int    `json:"width"`
					Height     int    `json:"height"`
					Duration   string `json:"duration"`
					FrameRate  string `json:"avg_frame_rate"`
					FrameCount string `json:"nb_frames"`
				} `json:"streams"`
				Format struct {
					Name string `json:"format_name"`
				} `json:"format"`
			}
			if err := json.Unmarshal(encoded, &document); err != nil {
				t.Fatalf("decode mock media facts: %v", err)
			}
			if len(document.Streams) != 1 {
				t.Fatalf("mock media streams = %#v", document.Streams)
			}
			stream := document.Streams[0]
			if stream.CodecName != test.codec || stream.Width != test.width ||
				stream.Height != test.height || stream.Duration != test.duration ||
				stream.FrameRate != test.frameRate || stream.FrameCount != test.frameCount ||
				document.Format.Name != test.format {
				t.Fatalf("mock media facts = stream %#v format %#v", stream, document.Format)
			}
		})
	}
}

func writeJSON(t *testing.T, path string, payload any) {
	t.Helper()
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("protect fixture directory: %v", err)
	}
	content, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}

func readJSON(t *testing.T, path string, destination any) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON fixture: %v", err)
	}
	if err := json.Unmarshal(content, destination); err != nil {
		t.Fatalf("decode JSON fixture: %v", err)
	}
}

func executionRequestPayload(requestContent []byte) map[string]any {
	return map[string]any{
		"schema_version": 1,
		"identity": map[string]any{
			"attempt_id":   "40000000-0000-0000-0000-000000000001",
			"job_id":       "50000000-0000-0000-0000-000000000001",
			"worker_id":    testWorkerID,
			"worker_epoch": 7,
			"lease_fence":  11,
		},
		"execution_spec": map[string]any{
			"model_revision_id":             "60000000-0000-0000-0000-000000000001",
			"generation_preset_revision_id": "70000000-0000-0000-0000-000000000001",
			"execution_profile_revision_id": testProfileID,
			"output_spec_id":                "80000000-0000-0000-0000-000000000001",
			"request_content_base64":        base64.StdEncoding.EncodeToString(requestContent),
		},
	}
}

func readinessPayload(check string) map[string]any {
	return map[string]any{
		"schema_version":                1,
		"cycle_id":                      "10000000-0000-0000-0000-000000000001",
		"worker_id":                     testWorkerID,
		"worker_epoch":                  7,
		"node_identity":                 "h3-mock-node-01",
		"execution_profile_revision_id": testProfileID,
		"inference_backend_revision":    testRevision,
		"deadline":                      "2030-01-01T00:00:00Z",
		"check":                         check,
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat awaited file: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func executionArgumentsForTest(
	requestPath string,
	outputDir string,
	statusPath string,
	manifestPath string,
	failurePath string,
	outputSpecID string,
	resume bool,
	extra ...string,
) []string {
	return append(extra, []string{
		"--mock-stage-delay", "0s",
		"--mock-output-spec-id", outputSpecID,
		"--vela-request", requestPath,
		"--vela-output-dir", outputDir,
		"--vela-status", statusPath,
		"--vela-output-manifest", manifestPath,
		"--vela-failure", failurePath,
		"--vela-resume", fmt.Sprintf("%t", resume),
	}...)
}
