package h3stagemock_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/h3stagemock"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeExecutesAndSealsDeterministicStageOutput(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	stageRunID := "49300000-0000-0000-0000-000000000001"
	artifactID := "49600000-0000-0000-0000-000000000001"
	inputPayload := []byte("sealed encoder tensor")
	inputDigest := sha256.Sum256(inputPayload)
	inputPath := filepath.Join(
		inputRoot, "stage-runs", stageRunID, "inputs", artifactID,
		hex.EncodeToString(inputDigest[:])+".bin",
	)
	if err := os.MkdirAll(filepath.Dir(inputPath), 0o700); err != nil {
		t.Fatalf("create input directory: %v", err)
	}
	if err := os.WriteFile(inputPath, inputPayload, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	identity := stageIdentity(stageRunID)
	specification := executionSpec(t, &velav1.StageExecutionSpec{
		Inputs: []*velav1.StageInputArtifact{{
			StageArtifactId: artifactID, ObjectVersion: "encoder-v1",
			Sha256: inputDigest[:], SizeBytes: int64(len(inputPayload)),
			StageInterfaceRevisionId: "49700000-0000-0000-0000-000000000001",
		}},
		ParametersJson:             []byte(`{"seed":17}`),
		ExpectedOutputManifestJson: []byte(`{"latent":"49800000-0000-0000-0000-000000000001"}`),
	})
	requests := []any{
		initializeRequest(1, root, inputRoot, outputRoot, "DIT"),
		map[string]any{"schema_version": 1, "request_id": 2, "operation": "probe", "probe": map[string]any{"check": "MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP"}},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "prepare", "prepare": map[string]any{"identity": identity, "execution_spec": specification}},
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "start", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "status", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "seal", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 7, "operation": "shutdown"},
	}

	responses := runRuntime(t, h3stagemock.Config{
		Component: "DIT", Mode: h3stagemock.ModeSuccess,
	}, requests)
	if len(responses) != len(requests) {
		t.Fatalf("response count=%d want=%d", len(responses), len(requests))
	}
	if !boolField(t, responses[0], "initialized") || !boolField(t, responses[0], "acknowledged") {
		t.Fatalf("initialize response=%#v", responses[0])
	}
	probe := objectField(t, responses[1], "probe")
	if !boolField(t, probe, "ready") || string(bytesField(t, probe, "evidence")) != "vela-h3-stage-mock/v1:DIT:ready" {
		t.Fatalf("probe response=%#v", responses[1])
	}
	status := objectField(t, responses[4], "status")
	if stringField(t, status, "state") != "OUTPUT_READY" ||
		stringField(t, status, "backend_stage") != "mock/dit" {
		t.Fatalf("status response=%#v", responses[4])
	}
	output := objectField(t, responses[5], "output")
	manifestJSON := bytesField(t, output, "output_manifest_json")
	var manifest struct {
		SchemaVersion int    `json:"schema_version"`
		OutputPort    string `json:"output_port"`
		LocalLocator  string `json:"local_locator"`
		ContentType   string `json:"content_type"`
		PayloadSHA256 string `json:"payload_sha256"`
		SizeBytes     int64  `json:"size_bytes"`
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		t.Fatalf("decode output manifest: %v", err)
	}
	if manifest.SchemaVersion != 1 || manifest.OutputPort != "latent" ||
		manifest.ContentType != "application/x-minimax-h3-latent" ||
		manifest.LocalLocator != identity["stage_attempt_id"].(string)+"/latent.bin" {
		t.Fatalf("output manifest=%#v", manifest)
	}
	payload, err := os.ReadFile(filepath.Join(outputRoot, manifest.LocalLocator))
	if err != nil {
		t.Fatalf("read sealed output: %v", err)
	}
	if string(payload) != "{\"component\":\"DIT\",\"input_sha256\":[\"c10d6c2c8cec75f39efd2321ef98cf85381c9c0ab3c0f46dc91c7e749a8ffe4d\"],\"parameters_sha256\":\"286eb51c30a5753571a71d0a19be45487e3ba3d692d815a093ae11fb279c46b6\",\"root_input_sha256\":[],\"schema_version\":1}\n" {
		t.Fatalf("sealed payload=%q", payload)
	}
	wantDigest := sha256.Sum256(payload)
	if manifest.PayloadSHA256 != hex.EncodeToString(wantDigest[:]) ||
		manifest.SizeBytes != int64(len(payload)) || numberField(t, output, "total_size_bytes") != int64(len(payload)) {
		t.Fatalf("output receipt=%#v manifest=%#v", output, manifest)
	}
}

func TestRuntimeSupportsFailureAndHangModes(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		mode       h3stagemock.Mode
		operations []string
		wantStates []string
	}{
		{name: "failure", mode: h3stagemock.ModeFailure, operations: []string{"status"}, wantStates: []string{"FAILED"}},
		{name: "hang", mode: h3stagemock.ModeHang, operations: []string{"status", "cancel", "status"}, wantStates: []string{"RUNNING", "STOPPED"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root, inputRoot, outputRoot := runtimeRoots(t)
			identity := stageIdentity("49300000-0000-0000-0000-000000000001")
			specification := executionSpec(t, &velav1.StageExecutionSpec{
				ParametersJson:             []byte(`{"seed":17}`),
				ExpectedOutputManifestJson: []byte(`{"conditioning":"49800000-0000-0000-0000-000000000001"}`),
			})
			requests := []any{
				initializeRequest(1, root, inputRoot, outputRoot, "ENCODER"),
				map[string]any{"schema_version": 1, "request_id": 2, "operation": "prepare", "prepare": map[string]any{"identity": identity, "execution_spec": specification}},
				map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": identity}},
			}
			requestID := 4
			for _, operation := range testCase.operations {
				payload := map[string]any{"schema_version": 1, "request_id": requestID, "operation": operation}
				if operation == "cancel" {
					payload["cancel"] = map[string]any{"identity": identity, "reason": "MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP"}
				} else {
					payload["stage"] = map[string]any{"identity": identity}
				}
				requests = append(requests, payload)
				requestID++
			}
			requests = append(requests, map[string]any{"schema_version": 1, "request_id": requestID, "operation": "shutdown"})
			responses := runRuntime(t, h3stagemock.Config{Component: "ENCODER", Mode: testCase.mode}, requests)
			var states []string
			for _, response := range responses {
				if status, ok := response["status"].(map[string]any); ok {
					states = append(states, stringField(t, status, "state"))
				}
			}
			if len(states) != len(testCase.wantStates) {
				t.Fatalf("states=%v want=%v", states, testCase.wantStates)
			}
			for index := range states {
				if states[index] != testCase.wantStates[index] {
					t.Fatalf("states=%v want=%v", states, testCase.wantStates)
				}
			}
			if testCase.mode == h3stagemock.ModeFailure {
				failure := objectField(t, objectField(t, responses[3], "status"), "failure")
				if stringField(t, failure, "failure_class") != "MOCK_INJECTED_FAILURE" ||
					!boolField(t, failure, "worker_reusable") {
					t.Fatalf("failure=%#v", failure)
				}
			}
		})
	}
}

func TestRuntimeDiscardsUnsealedOutputOnCancel(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	identity := stageIdentity("49300000-0000-0000-0000-000000000001")
	requests := []any{
		initializeRequest(1, root, inputRoot, outputRoot, "ENCODER"),
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "prepare",
			"prepare": map[string]any{
				"identity": identity,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					ParametersJson:             []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": identity}},
		map[string]any{
			"schema_version": 1, "request_id": 4, "operation": "cancel",
			"cancel": map[string]any{
				"identity": identity, "reason": "MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP",
			},
		},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "status", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "shutdown"},
	}
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, requests)
	if state := stringField(t, objectField(t, responses[4], "status"), "state"); state != "STOPPED" {
		t.Fatalf("state=%q want STOPPED", state)
	}
	outputDirectory := filepath.Join(outputRoot, identity["stage_attempt_id"].(string))
	if _, err := os.Lstat(outputDirectory); !os.IsNotExist(err) {
		t.Fatalf("unsealed output remains after cancel: %v", err)
	}
}

func TestVAEMockOutputPassesPinnedFFprobeContract(t *testing.T) {
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe is not installed")
	}
	version, err := exec.Command(ffprobe, "-version").Output()
	if err != nil || !strings.HasPrefix(string(version), "ffprobe version 8.0.1 ") {
		t.Skip("pinned ffprobe 8.0.1 is not installed")
	}

	root, inputRoot, outputRoot := runtimeRoots(t)
	stageRunID := "49300000-0000-0000-0000-000000000001"
	artifactID := "49600000-0000-0000-0000-000000000001"
	inputPayload := []byte("sealed dit latent")
	inputDigest := sha256.Sum256(inputPayload)
	inputPath := filepath.Join(
		inputRoot, "stage-runs", stageRunID, "inputs", artifactID,
		hex.EncodeToString(inputDigest[:])+".bin",
	)
	if err := os.MkdirAll(filepath.Dir(inputPath), 0o700); err != nil {
		t.Fatalf("create input directory: %v", err)
	}
	if err := os.WriteFile(inputPath, inputPayload, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	identity := stageIdentity(stageRunID)
	requests := []any{
		initializeRequest(1, root, inputRoot, outputRoot, "VAE_DECODER"),
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "prepare",
			"prepare": map[string]any{
				"identity": identity,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					Inputs: []*velav1.StageInputArtifact{{
						StageArtifactId: artifactID, ObjectVersion: "dit-v1", Sha256: inputDigest[:],
						SizeBytes:                int64(len(inputPayload)),
						StageInterfaceRevisionId: "49700000-0000-0000-0000-000000000001",
					}},
					ParametersJson:             []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"video":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "seal", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "shutdown"},
	}
	responses := runRuntime(t, h3stagemock.Config{
		Component: "VAE_DECODER", Mode: h3stagemock.ModeSuccess,
	}, requests)
	manifestJSON := bytesField(t, objectField(t, responses[3], "output"), "output_manifest_json")
	var manifest struct {
		LocalLocator string `json:"local_locator"`
		ContentType  string `json:"content_type"`
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil || manifest.ContentType != "video/mp4" {
		t.Fatalf("decode VAE output manifest: %v; manifest=%s", err, manifestJSON)
	}
	outputPath := filepath.Join(outputRoot, filepath.FromSlash(manifest.LocalLocator))
	probe := exec.Command(
		ffprobe, "-v", "error", "-hide_banner", "-protocol_whitelist", "file",
		"-probesize", "67108864", "-analyzeduration", "10000000", "-show_entries",
		"program_version=version:stream=codec_name,codec_type,width,height,avg_frame_rate,nb_frames,duration:format=format_name,duration,size",
		"-of", "json", outputPath,
	)
	encoded, err := probe.CombinedOutput()
	if err != nil || !bytes.Contains(encoded, []byte(`"codec_name": "h264"`)) ||
		!bytes.Contains(encoded, []byte(`"format_name": "mov,mp4,m4a,3gp,3g2,mj2"`)) {
		t.Fatalf("probe VAE mock output: %v; output=%s", err, encoded)
	}
}

func TestRuntimeRejectsDuplicateJSONKeys(t *testing.T) {
	var output bytes.Buffer
	err := h3stagemock.Run(context.Background(), h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
		Stdin:  bytes.NewBufferString("{\"schema_version\":1,\"request_id\":1,\"request_id\":2,\"operation\":\"shutdown\"}\n"),
		Stdout: &output,
	})
	if err == nil || output.Len() != 0 {
		t.Fatalf("Run duplicate keys error=%v output=%q", err, output.String())
	}
}

func runRuntime(t *testing.T, config h3stagemock.Config, requests []any) []map[string]any {
	t.Helper()
	var input bytes.Buffer
	encoder := json.NewEncoder(&input)
	for _, request := range requests {
		if err := encoder.Encode(request); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	var output bytes.Buffer
	config.Stdin = &input
	config.Stdout = &output
	if err := h3stagemock.Run(context.Background(), config); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var responses []map[string]any
	scanner := bufio.NewScanner(&output)
	for scanner.Scan() {
		var response map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		responses = append(responses, response)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan responses: %v", err)
	}
	return responses
}

func runtimeRoots(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("resolve runtime root: %v", err)
	}
	inputRoot := filepath.Join(root, "inputs")
	outputRoot := filepath.Join(root, "outputs")
	for _, path := range []string{inputRoot, outputRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create runtime root: %v", err)
		}
	}
	return root, inputRoot, outputRoot
}

func initializeRequest(requestID int, root, inputRoot, outputRoot, component string) map[string]any {
	return map[string]any{
		"schema_version": 1, "request_id": requestID, "operation": "initialize",
		"initialize": map[string]any{
			"worker_instance_id": "49000000-0000-0000-0000-000000000001", "worker_instance_epoch": 1,
			"worker_member_id": "49000000-0000-0000-0000-000000000002", "worker_member_epoch": 1,
			"device_set_digest":  "11" + string(bytes.Repeat([]byte("0"), 62)),
			"devices":            []any{map[string]any{"id": "49000000-0000-0000-0000-000000000003", "epoch": 1}},
			"membership_digest":  "22" + string(bytes.Repeat([]byte("0"), 62)),
			"members":            []any{map[string]any{"id": "49000000-0000-0000-0000-000000000002", "epoch": 1}},
			"model_residency_id": "49000000-0000-0000-0000-000000000004",
			"runtime_identity":   component + "-mock-v1", "model_runtime_epoch": 1,
			"stage_profile_revision_id": "49000000-0000-0000-0000-000000000005",
			"component":                 component, "model_component_revision": "h3-mock-v1",
			"local_devices": []any{map[string]any{
				"device_id": "49000000-0000-0000-0000-000000000003", "device_epoch": 1,
				"gpu_uuid": "GPU-00000000-0000-0000-0000-000000000001", "pci_bdf": "0000:41:00.0",
			}},
			"scratch_root": root, "input_root": inputRoot, "output_root": outputRoot,
		},
	}
}

func stageIdentity(stageRunID string) map[string]any {
	return map[string]any{
		"authority_digest": "33" + string(bytes.Repeat([]byte("0"), 62)),
		"job_id":           "49100000-0000-0000-0000-000000000001",
		"attempt_id":       "49200000-0000-0000-0000-000000000001",
		"stage_run_id":     stageRunID,
		"stage_attempt_id": "49400000-0000-0000-0000-000000000001",
		"stage_lease_id":   "49500000-0000-0000-0000-000000000001",
		"attempt_fence":    2, "stage_fence": 3, "stage_version": 4,
		"stage_profile_revision_id": "49000000-0000-0000-0000-000000000005",
	}
}

func executionSpec(t *testing.T, specification *velav1.StageExecutionSpec) []byte {
	t.Helper()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(specification)
	if err != nil {
		t.Fatalf("encode execution spec: %v", err)
	}
	return encoded
}

func objectField(t *testing.T, object map[string]any, name string) map[string]any {
	t.Helper()
	value, ok := object[name].(map[string]any)
	if !ok {
		t.Fatalf("field %q=%#v is not an object", name, object[name])
	}
	return value
}

func stringField(t *testing.T, object map[string]any, name string) string {
	t.Helper()
	value, ok := object[name].(string)
	if !ok {
		t.Fatalf("field %q=%#v is not a string", name, object[name])
	}
	return value
}

func boolField(t *testing.T, object map[string]any, name string) bool {
	t.Helper()
	value, ok := object[name].(bool)
	if !ok {
		t.Fatalf("field %q=%#v is not a bool", name, object[name])
	}
	return value
}

func bytesField(t *testing.T, object map[string]any, name string) []byte {
	t.Helper()
	encoded := stringField(t, object, name)
	value, err := bytesFromBase64(encoded)
	if err != nil {
		t.Fatalf("decode field %q: %v", name, err)
	}
	return value
}

func bytesFromBase64(encoded string) ([]byte, error) {
	var value []byte
	err := json.Unmarshal([]byte(`"`+encoded+`"`), &value)
	return value, err
}

func numberField(t *testing.T, object map[string]any, name string) int64 {
	t.Helper()
	value, ok := object[name].(float64)
	if !ok {
		t.Fatalf("field %q=%#v is not a number", name, object[name])
	}
	return int64(value)
}
