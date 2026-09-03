package h3stagemock_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/h3mockbackend"
	"github.com/vivym/vela/internal/h3stagemock"
	"github.com/vivym/vela/internal/stageartifact"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/encoding/protowire"
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
		Lineage       struct {
			AttemptID              string `json:"attempt_id"`
			StageRunID             string `json:"stage_run_id"`
			StageAttemptID         string `json:"stage_attempt_id"`
			StageLeaseID           string `json:"stage_lease_id"`
			AttemptFence           int64  `json:"attempt_fence"`
			StageFence             int64  `json:"stage_fence"`
			StageProfileRevisionID string `json:"stage_profile_revision_id"`
		} `json:"lineage"`
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		t.Fatalf("decode output manifest: %v", err)
	}
	if manifest.SchemaVersion != 1 || manifest.OutputPort != "latent" ||
		manifest.ContentType != "application/x-minimax-h3-latent" ||
		manifest.LocalLocator != identity["stage_attempt_id"].(string)+"/latent.bin" ||
		manifest.Lineage.AttemptID != identity["attempt_id"].(string) ||
		manifest.Lineage.StageRunID != identity["stage_run_id"].(string) ||
		manifest.Lineage.StageAttemptID != identity["stage_attempt_id"].(string) ||
		manifest.Lineage.StageLeaseID != identity["stage_lease_id"].(string) ||
		manifest.Lineage.AttemptFence != int64(identity["attempt_fence"].(int)) ||
		manifest.Lineage.StageFence != int64(identity["stage_fence"].(int)) ||
		manifest.Lineage.StageProfileRevisionID != identity["stage_profile_revision_id"].(string) {
		t.Fatalf("output manifest=%#v", manifest)
	}
	outputPath := filepath.Join(outputRoot, manifest.LocalLocator)
	payload, err := os.ReadFile(outputPath)
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
	information, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("stat sealed output: %v", err)
	}
	if information.Mode().Perm() != 0o600 {
		t.Fatalf("sealed output mode=%v, want 0600", information.Mode().Perm())
	}
}

func TestRuntimePublishesCPUThumbnailFixture(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	stageRunID := "49300000-0000-0000-0000-000000000001"
	artifactID := "49600000-0000-0000-0000-000000000001"
	inputPayload := []byte("sealed mock video")
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
	initialization := initializeRequest(1, root, inputRoot, outputRoot, "CPU_MEDIA")
	initialize := initialization["initialize"].(map[string]any)
	initialize["model_component_revision"] = "lab-thumbnail-mock-v1"
	initialize["local_devices"] = []any{map[string]any{
		"device_id": "49000000-0000-0000-0000-000000000003", "device_epoch": 1,
		"resource_class": "CPU",
	}}
	identity := stageIdentity(stageRunID)
	responses := runRuntime(t, h3stagemock.Config{
		Component: "CPU_MEDIA", Mode: h3stagemock.ModeSuccess,
	}, []any{
		initialization,
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "prepare",
			"prepare": map[string]any{
				"identity": identity,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					Inputs: []*velav1.StageInputArtifact{{
						StageArtifactId: artifactID, ObjectVersion: "video-v1", Sha256: inputDigest[:],
						SizeBytes:                int64(len(inputPayload)),
						StageInterfaceRevisionId: "49700000-0000-0000-0000-000000000001",
					}},
					ParametersJson:             []byte(`{"mock":true}`),
					ExpectedOutputManifestJson: []byte(`{"thumbnail":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "seal", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "shutdown"},
	})
	manifestJSON := bytesField(t, objectField(t, responses[3], "output"), "output_manifest_json")
	manifest, err := stageartifact.ParseLocalOutputManifestV1(manifestJSON)
	if err != nil {
		t.Fatalf("parse CPU thumbnail output manifest: %v", err)
	}
	if manifest.OutputPort != "thumbnail" || manifest.ContentType != "image/webp" {
		t.Fatalf("CPU thumbnail manifest = %#v", manifest)
	}
	payload, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(manifest.LocalLocator)))
	if err != nil {
		t.Fatalf("read CPU thumbnail output: %v", err)
	}
	want, err := h3mockbackend.ReadThumbnailFixture()
	if err != nil {
		t.Fatalf("read expected thumbnail fixture: %v", err)
	}
	if !bytes.Equal(payload, want) || manifest.SizeBytes != int64(len(want)) ||
		manifest.PayloadSHA256 != sha256.Sum256(want) {
		t.Fatal("CPU thumbnail payload does not match the bounded WebP fixture")
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
			if testCase.mode == h3stagemock.ModeFailure {
				replacement := cloneObject(identity)
				replacement["authority_digest"] = "44" + string(bytes.Repeat([]byte("0"), 62))
				replacement["stage_run_id"] = "49300000-0000-0000-0000-000000000002"
				replacement["stage_attempt_id"] = "49400000-0000-0000-0000-000000000002"
				replacement["stage_lease_id"] = "49500000-0000-0000-0000-000000000002"
				requests = append(requests, map[string]any{
					"schema_version": 1, "request_id": requestID, "operation": "prepare",
					"prepare": map[string]any{"identity": replacement, "execution_spec": specification},
				})
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
				if !boolField(t, responses[len(responses)-2], "acknowledged") {
					t.Fatalf("replacement after injected failure=%#v", responses[len(responses)-2])
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

func TestRuntimeDiscardsUnsealedOutputOnShutdown(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	identity := stageIdentity("49300000-0000-0000-0000-000000000001")
	runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, []any{
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
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "shutdown"},
	})
	outputDirectory := filepath.Join(outputRoot, identity["stage_attempt_id"].(string))
	if _, err := os.Lstat(outputDirectory); !os.IsNotExist(err) {
		t.Fatalf("unsealed output remains after shutdown: %v", err)
	}
}

func TestRuntimeRejectsCancelAfterSealAndRetainsOutput(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	identity := stageIdentity("49300000-0000-0000-0000-000000000001")
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, []any{
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
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "seal", "stage": map[string]any{"identity": identity}},
		map[string]any{
			"schema_version": 1, "request_id": 5, "operation": "cancel",
			"cancel": map[string]any{
				"identity": identity, "reason": "MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP",
			},
		},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "shutdown"},
	})
	if _, ok := responses[4]["error"].(string); !ok {
		t.Fatalf("sealed cancel response=%#v", responses[4])
	}
	outputPath := filepath.Join(
		outputRoot, identity["stage_attempt_id"].(string), "conditioning.bin",
	)
	information, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("stat retained sealed output: %v", err)
	}
	if information.Mode().Perm() != 0o600 {
		t.Fatalf("sealed output mode=%v, want retained 0600 file", information.Mode().Perm())
	}
}

func TestVAEMockOutputPassesPinnedFFprobeContract(t *testing.T) {
	requirePinned := os.Getenv("VELA_REQUIRE_PINNED_FFPROBE") == "1"
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		if requirePinned {
			t.Fatal("pinned ffprobe 8.0.1 is required")
		}
		t.Skip("ffprobe is not installed")
	}
	version, err := exec.Command(ffprobe, "-version").Output()
	if err != nil || !strings.HasPrefix(string(version), "ffprobe version 8.0.1 ") {
		if requirePinned {
			t.Fatalf("pinned ffprobe 8.0.1 is required: %v; version=%s", err, version)
		}
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
	if err != nil {
		t.Fatalf("probe VAE mock output: %v; output=%s", err, encoded)
	}
	var result struct {
		ProgramVersion struct {
			Version string `json:"version"`
		} `json:"program_version"`
		Streams []struct {
			CodecName   string `json:"codec_name"`
			CodecType   string `json:"codec_type"`
			Width       int    `json:"width"`
			Height      int    `json:"height"`
			AverageRate string `json:"avg_frame_rate"`
			FrameCount  string `json:"nb_frames"`
			Duration    string `json:"duration"`
		} `json:"streams"`
		Format struct {
			Name     string `json:"format_name"`
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatalf("decode VAE ffprobe output: %v; output=%s", err, encoded)
	}
	if result.ProgramVersion.Version != "8.0.1" || len(result.Streams) != 1 ||
		result.Streams[0].CodecName != "h264" || result.Streams[0].CodecType != "video" ||
		result.Streams[0].Width != 1920 || result.Streams[0].Height != 1080 ||
		result.Streams[0].AverageRate != "24/1" || result.Streams[0].FrameCount != "120" ||
		result.Streams[0].Duration != "5.000000" ||
		result.Format.Name != "mov,mp4,m4a,3gp,3g2,mj2" || result.Format.Duration != "5.000000" {
		t.Fatalf("VAE ffprobe contract=%#v", result)
	}
}

func TestEncoderConsumesOrderedRootInputsAndRejectsInvalidOrdering(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	stageRunID := "49300000-0000-0000-0000-000000000001"
	payloads := [][]byte{[]byte("first root input"), []byte("second root input")}
	rootInputs := make([]*velav1.StageRootInputMaterial, 0, len(payloads))
	wantDigests := make([]string, 0, len(payloads))
	for index, payload := range payloads {
		digest := sha256.Sum256(payload)
		encodedDigest := hex.EncodeToString(digest[:])
		path := filepath.Join(
			inputRoot, "stage-runs", stageRunID, "root-inputs", fmt.Sprint(index), encodedDigest+".bin",
		)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create root input directory: %v", err)
		}
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatalf("write root input: %v", err)
		}
		rootInputs = append(rootInputs, &velav1.StageRootInputMaterial{
			ConditionIndex: int32(index), Uri: fmt.Sprintf("mock://condition/%d", index),
			Sha256: digest[:], SizeBytes: int64(len(payload)),
		})
		wantDigests = append(wantDigests, encodedDigest)
	}
	identity := stageIdentity(stageRunID)
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, []any{
		initializeRequest(1, root, inputRoot, outputRoot, "ENCODER"),
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "prepare",
			"prepare": map[string]any{
				"identity": identity,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					RootInputs: rootInputs, ParametersJson: []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "seal", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "shutdown"},
	})
	manifestJSON := bytesField(t, objectField(t, responses[3], "output"), "output_manifest_json")
	var manifest struct {
		LocalLocator string `json:"local_locator"`
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		t.Fatalf("decode Encoder output manifest: %v", err)
	}
	encoded, err := os.ReadFile(filepath.Join(outputRoot, manifest.LocalLocator))
	if err != nil {
		t.Fatalf("read Encoder output: %v", err)
	}
	var payload struct {
		RootInputSHA256 []string `json:"root_input_sha256"`
	}
	if err := json.Unmarshal(encoded, &payload); err != nil || !slices.Equal(payload.RootInputSHA256, wantDigests) {
		t.Fatalf("Encoder root input payload=%#v error=%v", payload, err)
	}

	invalidRoot, invalidInputRoot, invalidOutputRoot := runtimeRoots(t)
	invalidIdentity := stageIdentity(stageRunID)
	rootInputs[0].ConditionIndex = 1
	invalidResponses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, []any{
		initializeRequest(1, invalidRoot, invalidInputRoot, invalidOutputRoot, "ENCODER"),
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "prepare",
			"prepare": map[string]any{
				"identity": invalidIdentity,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					RootInputs: rootInputs, ParametersJson: []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "shutdown"},
	})
	if _, ok := invalidResponses[1]["error"].(string); !ok {
		t.Fatalf("invalid root input response=%#v", invalidResponses[1])
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

func TestRuntimeAcceptsMonotonicAuthorityRenewalAndRetiresPreviousDigest(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	original := stageIdentity("49300000-0000-0000-0000-000000000001")
	stale := cloneObject(original)
	stale["authority_digest"] = "66" + string(bytes.Repeat([]byte("0"), 62))
	stale["stage_version"] = 3
	renewed := cloneObject(original)
	renewed["authority_digest"] = "55" + string(bytes.Repeat([]byte("0"), 62))
	renewed["stage_version"] = 5
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, []any{
		initializeRequest(1, root, inputRoot, outputRoot, "ENCODER"),
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "prepare",
			"prepare": map[string]any{
				"identity": original,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					ParametersJson:             []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": stale}},
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "start", "stage": map[string]any{"identity": renewed}},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "status", "stage": map[string]any{"identity": renewed}},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "status", "stage": map[string]any{"identity": original}},
		map[string]any{"schema_version": 1, "request_id": 7, "operation": "seal", "stage": map[string]any{"identity": renewed}},
		map[string]any{"schema_version": 1, "request_id": 8, "operation": "shutdown"},
	})
	if _, ok := responses[2]["error"].(string); !ok {
		t.Fatalf("non-monotonic renewal response=%#v", responses[2])
	}
	if !boolField(t, responses[3], "acknowledged") ||
		stringField(t, objectField(t, responses[4], "status"), "state") != "OUTPUT_READY" {
		t.Fatalf("renewed execution responses=%#v", responses[3:5])
	}
	if _, ok := responses[5]["error"].(string); !ok {
		t.Fatalf("retired authority status response=%#v", responses[5])
	}
}

func TestRuntimeCleansUnsealedOutputOnFatalProtocolExit(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		trailing string
	}{
		{name: "EOF"},
		{name: "malformed request", trailing: "not-json\n"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root, inputRoot, outputRoot := runtimeRoots(t)
			identity := stageIdentity("49300000-0000-0000-0000-000000000001")
			var input bytes.Buffer
			encoder := json.NewEncoder(&input)
			for _, request := range []any{
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
			} {
				if err := encoder.Encode(request); err != nil {
					t.Fatalf("encode request: %v", err)
				}
			}
			input.WriteString(testCase.trailing)
			var output bytes.Buffer
			err := h3stagemock.Run(context.Background(), h3stagemock.Config{
				Component: "ENCODER", Mode: h3stagemock.ModeSuccess, Stdin: &input, Stdout: &output,
			})
			if err == nil {
				t.Fatal("fatal protocol exit unexpectedly succeeded")
			}
			outputDirectory := filepath.Join(outputRoot, identity["stage_attempt_id"].(string))
			if _, statErr := os.Lstat(outputDirectory); !os.IsNotExist(statErr) {
				t.Fatalf("unsealed output remains after fatal exit: %v", statErr)
			}
		})
	}
}

func TestRuntimeCancellationInterruptsIdleDeadlineInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create cancellable input pipe: %v", err)
	}
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})
	result := make(chan error, 1)
	go func() {
		result <- h3stagemock.Run(ctx, h3stagemock.Config{
			Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
			Stdin: reader, Stdout: io.Discard,
		})
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run cancellation error=%v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after idle input cancellation")
	}
}

func TestRuntimeCancellationSurfacesUnsealedOutputCleanupFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inputReader, inputWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("create cancellable input pipe: %v", err)
	}
	outputReader, outputWriter := io.Pipe()
	t.Cleanup(func() {
		_ = inputReader.Close()
		_ = inputWriter.Close()
		_ = outputReader.Close()
		_ = outputWriter.Close()
	})
	root, inputRoot, outputRoot := runtimeRoots(t)
	identity := stageIdentity("49300000-0000-0000-0000-000000000001")
	result := make(chan error, 1)
	go func() {
		result <- h3stagemock.Run(ctx, h3stagemock.Config{
			Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
			Stdin: inputReader, Stdout: outputWriter,
		})
	}()
	encoder := json.NewEncoder(inputWriter)
	responses := bufio.NewReader(outputReader)
	for _, request := range []any{
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
	} {
		if err := encoder.Encode(request); err != nil {
			t.Fatalf("encode request: %v", err)
		}
		if _, err := responses.ReadBytes('\n'); err != nil {
			t.Fatalf("read response: %v", err)
		}
	}
	outputPath := filepath.Join(
		outputRoot, identity["stage_attempt_id"].(string), "conditioning.bin",
	)
	if err := os.Remove(outputPath); err != nil {
		t.Fatalf("replace unsealed output: %v", err)
	}
	if err := os.Mkdir(outputPath, 0o700); err != nil {
		t.Fatalf("create blocking output directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputPath, "blocker"), []byte("block cleanup"), 0o600); err != nil {
		t.Fatalf("write cleanup blocker: %v", err)
	}
	cancel()
	select {
	case err := <-result:
		if err == context.Canceled || !errors.Is(err, context.Canceled) ||
			!strings.Contains(err.Error(), "remove unsealed H3 Stage mock output") {
			t.Fatalf("Run cancellation cleanup error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return cancellation cleanup failure")
	}
}

func TestRuntimeRejectsNestedProtobufUnknownFields(t *testing.T) {
	unknown := protowire.AppendTag(nil, 1000, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	for _, testCase := range []struct {
		name          string
		component     string
		specification *velav1.StageExecutionSpec
	}{
		{
			name: "StageInputArtifact", component: "DIT",
			specification: &velav1.StageExecutionSpec{
				Inputs: []*velav1.StageInputArtifact{{
					StageArtifactId: "49600000-0000-0000-0000-000000000001",
					ObjectVersion:   "encoder-v1", Sha256: bytes.Repeat([]byte{1}, sha256.Size), SizeBytes: 1,
					StageInterfaceRevisionId: "49700000-0000-0000-0000-000000000001",
				}},
				ParametersJson: []byte(`{"seed":17}`), ExpectedOutputManifestJson: []byte(`{"latent":true}`),
			},
		},
		{
			name: "StageRootInputMaterial", component: "ENCODER",
			specification: &velav1.StageExecutionSpec{
				RootInputs: []*velav1.StageRootInputMaterial{{
					ConditionIndex: 0, Uri: "mock://condition/0", Sha256: bytes.Repeat([]byte{2}, sha256.Size), SizeBytes: 1,
				}},
				ParametersJson: []byte(`{"seed":17}`), ExpectedOutputManifestJson: []byte(`{"conditioning":true}`),
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.specification.GetInputs() != nil {
				testCase.specification.GetInputs()[0].ProtoReflect().SetUnknown(unknown)
			} else {
				testCase.specification.GetRootInputs()[0].ProtoReflect().SetUnknown(unknown)
			}
			root, inputRoot, outputRoot := runtimeRoots(t)
			responses := runRuntime(t, h3stagemock.Config{
				Component: testCase.component, Mode: h3stagemock.ModeSuccess,
			}, []any{
				initializeRequest(1, root, inputRoot, outputRoot, testCase.component),
				map[string]any{
					"schema_version": 1, "request_id": 2, "operation": "prepare",
					"prepare": map[string]any{
						"identity":       stageIdentity("49300000-0000-0000-0000-000000000001"),
						"execution_spec": executionSpec(t, testCase.specification),
					},
				},
				map[string]any{"schema_version": 1, "request_id": 3, "operation": "shutdown"},
			})
			if _, ok := responses[1]["error"].(string); !ok {
				t.Fatalf("nested unknown field response=%#v", responses[1])
			}
		})
	}
}

func TestRuntimeRejectsAuthoritySpecMutationAfterSeal(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	identity := stageIdentity("49300000-0000-0000-0000-000000000001")
	original := executionSpec(t, &velav1.StageExecutionSpec{
		ParametersJson:             []byte(`{"seed":17}`),
		ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
	})
	changed := executionSpec(t, &velav1.StageExecutionSpec{
		ParametersJson:             []byte(`{"seed":18}`),
		ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
	})
	requests := []any{
		initializeRequest(1, root, inputRoot, outputRoot, "ENCODER"),
		map[string]any{"schema_version": 1, "request_id": 2, "operation": "prepare", "prepare": map[string]any{"identity": identity, "execution_spec": original}},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "seal", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "prepare", "prepare": map[string]any{"identity": identity, "execution_spec": original}},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "prepare", "prepare": map[string]any{"identity": identity, "execution_spec": changed}},
		map[string]any{"schema_version": 1, "request_id": 7, "operation": "status", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 8, "operation": "shutdown"},
	}
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, requests)
	if !boolField(t, responses[4], "acknowledged") {
		t.Fatalf("exact replay response=%#v", responses[4])
	}
	if _, ok := responses[5]["error"].(string); !ok {
		t.Fatalf("mutated replay response=%#v", responses[5])
	}
	if state := stringField(t, objectField(t, responses[6], "status"), "state"); state != "OUTPUT_SEALED" {
		t.Fatalf("state after rejected mutation=%q want OUTPUT_SEALED", state)
	}
}

func TestRuntimeRejectsRetiredAuthorityWithoutRetainingExecutionHistory(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	first := stageIdentity("49300000-0000-0000-0000-000000000001")
	second := stageIdentity("49300000-0000-0000-0000-000000000002")
	second["authority_digest"] = "44" + string(bytes.Repeat([]byte("0"), 62))
	second["stage_attempt_id"] = "49400000-0000-0000-0000-000000000002"
	second["stage_lease_id"] = "49500000-0000-0000-0000-000000000002"
	specification := executionSpec(t, &velav1.StageExecutionSpec{
		ParametersJson:             []byte(`{"seed":17}`),
		ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
	})
	requests := []any{
		initializeRequest(1, root, inputRoot, outputRoot, "ENCODER"),
		map[string]any{"schema_version": 1, "request_id": 2, "operation": "prepare", "prepare": map[string]any{"identity": first, "execution_spec": specification}},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "start", "stage": map[string]any{"identity": first}},
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "seal", "stage": map[string]any{"identity": first}},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "prepare", "prepare": map[string]any{"identity": second, "execution_spec": specification}},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "status", "stage": map[string]any{"identity": first}},
		map[string]any{
			"schema_version": 1, "request_id": 7, "operation": "prepare",
			"prepare": map[string]any{
				"identity": first,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					ParametersJson:             []byte(`{"seed":18}`),
					ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 8, "operation": "shutdown"},
	}
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, requests)
	if !boolField(t, responses[4], "acknowledged") {
		t.Fatalf("next assignment response=%#v", responses[4])
	}
	if _, ok := responses[5]["error"].(string); !ok {
		t.Fatalf("completed assignment remained queryable: %#v", responses[5])
	}
	if _, ok := responses[6]["error"].(string); !ok {
		t.Fatalf("retired authority was rebound: %#v", responses[6])
	}
}

func TestRuntimeReportsPublicationFailureAsReusableFailure(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	identity := stageIdentity("49300000-0000-0000-0000-000000000001")
	replacement := cloneObject(identity)
	replacement["authority_digest"] = "44" + string(bytes.Repeat([]byte("0"), 62))
	replacement["stage_run_id"] = "49300000-0000-0000-0000-000000000002"
	replacement["stage_attempt_id"] = "49400000-0000-0000-0000-000000000002"
	replacement["stage_lease_id"] = "49500000-0000-0000-0000-000000000002"
	if err := os.Mkdir(filepath.Join(outputRoot, identity["stage_attempt_id"].(string)), 0o700); err != nil {
		t.Fatalf("create conflicting output directory: %v", err)
	}
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
		map[string]any{"schema_version": 1, "request_id": 4, "operation": "status", "stage": map[string]any{"identity": identity}},
		map[string]any{
			"schema_version": 1, "request_id": 5, "operation": "prepare",
			"prepare": map[string]any{
				"identity": replacement,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					ParametersJson:             []byte(`{"seed":18}`),
					ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "shutdown"},
	}
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, requests)
	if _, ok := responses[2]["error"].(string); !ok {
		t.Fatalf("publication failure response=%#v", responses[2])
	}
	status := objectField(t, responses[3], "status")
	if stringField(t, status, "state") != "FAILED" {
		t.Fatalf("publication failure status=%#v", status)
	}
	failure := objectField(t, status, "failure")
	if stringField(t, failure, "failure_class") != "MOCK_OUTPUT_PUBLICATION_FAILED" ||
		!boolField(t, failure, "worker_reusable") {
		t.Fatalf("publication failure evidence=%#v", failure)
	}
	if !boolField(t, responses[4], "acknowledged") {
		t.Fatalf("replacement after publication failure=%#v", responses[4])
	}
}

func TestRuntimeRejectsMalformedDeviceAndUnknownReadinessCheck(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	invalidInitialization := initializeRequest(1, root, inputRoot, outputRoot, "ENCODER")
	initialize := invalidInitialization["initialize"].(map[string]any)
	initialize["local_devices"].([]any)[0].(map[string]any)["gpu_uuid"] = "GPU-zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"
	initialize["local_devices"].([]any)[0].(map[string]any)["pci_bdf"] = "xxxx:yy:zz.z"
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, []any{
		invalidInitialization,
		map[string]any{"schema_version": 1, "request_id": 2, "operation": "shutdown"},
	})
	if _, ok := responses[0]["error"].(string); !ok {
		t.Fatalf("malformed device response=%#v", responses[0])
	}

	responses = runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
	}, []any{
		initializeRequest(1, root, inputRoot, outputRoot, "ENCODER"),
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "probe",
			"probe": map[string]any{"check": "MODEL_RUNTIME_READINESS_CHECK_NOT_IN_PROTO"},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "shutdown"},
	})
	if _, ok := responses[1]["error"].(string); !ok {
		t.Fatalf("unknown readiness response=%#v", responses[1])
	}
}

func TestRuntimeRejectsUnknownCancelReason(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	identity := stageIdentity("49300000-0000-0000-0000-000000000001")
	responses := runRuntime(t, h3stagemock.Config{
		Component: "ENCODER", Mode: h3stagemock.ModeHang,
	}, []any{
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
				"identity": identity, "reason": "MODEL_RUNTIME_CANCEL_REASON_NOT_IN_PROTO",
			},
		},
		map[string]any{"schema_version": 1, "request_id": 5, "operation": "status", "stage": map[string]any{"identity": identity}},
		map[string]any{"schema_version": 1, "request_id": 6, "operation": "shutdown"},
	})
	if _, ok := responses[3]["error"].(string); !ok {
		t.Fatalf("unknown cancel response=%#v", responses[3])
	}
	if state := stringField(t, objectField(t, responses[4], "status"), "state"); state != "RUNNING" {
		t.Fatalf("state after rejected cancel=%q want RUNNING", state)
	}
}

func TestRuntimeRejectsSymlinkStageInput(t *testing.T) {
	root, inputRoot, outputRoot := runtimeRoots(t)
	stageRunID := "49300000-0000-0000-0000-000000000001"
	artifactID := "49600000-0000-0000-0000-000000000001"
	payload := []byte("symlinked encoder tensor")
	digest := sha256.Sum256(payload)
	realPath := filepath.Join(root, "source.bin")
	if err := os.WriteFile(realPath, payload, 0o600); err != nil {
		t.Fatalf("write source input: %v", err)
	}
	inputPath := filepath.Join(
		inputRoot, "stage-runs", stageRunID, "inputs", artifactID,
		hex.EncodeToString(digest[:])+".bin",
	)
	if err := os.MkdirAll(filepath.Dir(inputPath), 0o700); err != nil {
		t.Fatalf("create input directory: %v", err)
	}
	if err := os.Symlink(realPath, inputPath); err != nil {
		t.Fatalf("create symlink input: %v", err)
	}
	identity := stageIdentity(stageRunID)
	responses := runRuntime(t, h3stagemock.Config{
		Component: "DIT", Mode: h3stagemock.ModeSuccess,
	}, []any{
		initializeRequest(1, root, inputRoot, outputRoot, "DIT"),
		map[string]any{
			"schema_version": 1, "request_id": 2, "operation": "prepare",
			"prepare": map[string]any{
				"identity": identity,
				"execution_spec": executionSpec(t, &velav1.StageExecutionSpec{
					Inputs: []*velav1.StageInputArtifact{{
						StageArtifactId: artifactID, ObjectVersion: "encoder-v1", Sha256: digest[:],
						SizeBytes:                int64(len(payload)),
						StageInterfaceRevisionId: "49700000-0000-0000-0000-000000000001",
					}},
					ParametersJson:             []byte(`{"seed":17}`),
					ExpectedOutputManifestJson: []byte(`{"latent":{"required":true}}`),
				}),
			},
		},
		map[string]any{"schema_version": 1, "request_id": 3, "operation": "shutdown"},
	})
	if _, ok := responses[1]["error"].(string); !ok {
		t.Fatalf("symlink input response=%#v", responses[1])
	}
}

func TestRuntimeRejectsUnknownFieldsTrailingDataAndOversizedMessages(t *testing.T) {
	for name, input := range map[string]string{
		"unknown field": `{"schema_version":1,"request_id":1,"operation":"shutdown","extra":true}` + "\n",
		"trailing data": `{"schema_version":1,"request_id":1,"operation":"shutdown"} {}` + "\n",
		"oversized":     strings.Repeat("x", (1<<20)+1) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			err := h3stagemock.Run(context.Background(), h3stagemock.Config{
				Component: "ENCODER", Mode: h3stagemock.ModeSuccess,
				Stdin: bytes.NewBufferString(input), Stdout: &output,
			})
			if err == nil || output.Len() != 0 {
				t.Fatalf("Run error=%v output=%q", err, output.String())
			}
		})
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

func cloneObject(source map[string]any) map[string]any {
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
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
