package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/h3request"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type fastH3ConformanceBackend struct {
	component  string
	backend    *ProcessBackend
	inputRoot  string
	outputRoot string
	stderr     *bytes.Buffer
}

type fastH3ConformanceOutput struct {
	OutputPort    string `json:"output_port"`
	LocalLocator  string `json:"local_locator"`
	ContentType   string `json:"content_type"`
	PayloadSHA256 string `json:"payload_sha256"`
	SizeBytes     int64  `json:"size_bytes"`
	path          string
}

func TestProcessBackendConformsToFastH3PythonDriver(t *testing.T) {
	fastH3Root := os.Getenv("VELA_FAST_H3_SOURCE_ROOT")
	python := os.Getenv("VELA_FAST_H3_PYTHON")
	if fastH3Root == "" || python == "" {
		t.Skip("set VELA_FAST_H3_SOURCE_ROOT and VELA_FAST_H3_PYTHON for external driver conformance")
	}
	fastH3Root = canonicalConformancePath(t, fastH3Root)
	python = absoluteConformancePath(t, python)
	driverPath := filepath.Join(fastH3Root, "src", "fast_h3", "vela", "driver.py")
	if information, err := os.Stat(driverPath); err != nil || !information.Mode().IsRegular() {
		t.Fatalf("fast-h3 driver is unavailable at %s: %v", driverPath, err)
	}
	fixtureRoot := canonicalConformancePath(t, "testdata")
	root := canonicalConformancePath(t, t.TempDir())
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("make conformance root private: %v", err)
	}
	eventLog := filepath.Join(root, "events.log")

	components := []string{"ENCODER", "DIT", "VAE_DECODER"}
	backends := make(map[string]*fastH3ConformanceBackend, len(components))
	for _, component := range components {
		backends[component] = newFastH3ConformanceBackend(
			t, root, eventLog, fastH3Root, fixtureRoot, python, component,
		)
	}

	rootMaterial := []byte("vela-fast-h3-root-material-v1")
	rootDigest := sha256.Sum256(rootMaterial)
	seed := int64(17)
	duration := 5.0
	frozen, err := h3request.Freeze(
		"a precise artifact contract",
		"balanced",
		"fast-h3-process-conformance",
		h3request.Request{
			Task: "ref2va",
			Seed: &seed,
			Conditions: []h3request.Condition{{
				Role: "reference", Type: "image", URI: "vela://uploads/reference-frame",
				DownloadURL: "https://objects.example.test/reference-frame",
				SHA256:      hex.EncodeToString(rootDigest[:]), SizeBytes: int64(len(rootMaterial)),
			}},
			Target: h3request.Target{
				ShortEdge: 768, AspectRatio: "16:9", DurationSeconds: &duration,
			},
			Sampling: h3request.Sampling{NumInferenceSteps: 30, Quality: "lossless"},
		},
	)
	if err != nil {
		t.Fatalf("freeze exact H3 conformance request: %v", err)
	}
	parameters, err := json.Marshal(frozen.Parameters)
	if err != nil {
		t.Fatalf("encode exact H3 conformance parameters: %v", err)
	}

	var finalDigest string
	for assignment := byte(0); assignment < 2; assignment++ {
		encoderAuthority := processBackendAuthority(assignment*3 + 1)
		materializeFastH3RootInput(
			t, backends["ENCODER"].inputRoot, encoderAuthority,
			rootDigest, rootMaterial,
		)
		encoder := runFastH3ConformanceStage(
			t, backends["ENCODER"], encoderAuthority,
			&velav1.StageExecutionSpec{
				ParametersJson:             parameters,
				ExpectedOutputManifestJson: []byte(`{"conditioning":{"required":true}}`),
				RootInputs: []*velav1.StageRootInputMaterial{{
					ConditionIndex: 0,
					Uri:            frozen.RootInputs[0].URI,
					Sha256:         rootDigest[:],
					SizeBytes:      int64(len(rootMaterial)),
				}},
			},
		)

		ditAuthority := processBackendAuthority(assignment*3 + 2)
		encoderInput := materializeFastH3StageInput(
			t, backends["DIT"].inputRoot, ditAuthority,
			fmt.Sprintf("73000000-0000-0000-0000-00000000000%d", assignment+1),
			"encoder-v1", "74000000-0000-0000-0000-000000000001", encoder,
		)
		dit := runFastH3ConformanceStage(
			t, backends["DIT"], ditAuthority,
			&velav1.StageExecutionSpec{
				Inputs:                     []*velav1.StageInputArtifact{encoderInput},
				ParametersJson:             parameters,
				ExpectedOutputManifestJson: []byte(`{"latent":{"required":true}}`),
			},
		)

		vaeAuthority := processBackendAuthority(assignment*3 + 3)
		ditInput := materializeFastH3StageInput(
			t, backends["VAE_DECODER"].inputRoot, vaeAuthority,
			fmt.Sprintf("75000000-0000-0000-0000-00000000000%d", assignment+1),
			"dit-v1", "74000000-0000-0000-0000-000000000002", dit,
		)
		vae := runFastH3ConformanceStage(
			t, backends["VAE_DECODER"], vaeAuthority,
			&velav1.StageExecutionSpec{
				Inputs:                     []*velav1.StageInputArtifact{ditInput},
				ParametersJson:             parameters,
				ExpectedOutputManifestJson: []byte(`{"video":{"required":true}}`),
			},
		)
		if !strings.HasSuffix(encoder.ContentType, "encoder-conditioning.v1") ||
			!strings.HasSuffix(dit.ContentType, "dit-latents.v1") ||
			!strings.HasSuffix(vae.ContentType, "decoded-media.v1") {
			t.Fatalf("H3 artifact content types = %q/%q/%q", encoder.ContentType, dit.ContentType, vae.ContentType)
		}
		if finalDigest == "" {
			finalDigest = vae.PayloadSHA256
		} else if vae.PayloadSHA256 != finalDigest {
			t.Fatalf("resident retry VAE digest = %s, want %s", vae.PayloadSHA256, finalDigest)
		}
	}

	for _, component := range components {
		backend := backends[component]
		if err := backend.backend.Close(); err != nil {
			t.Fatalf("close fast-h3 %s ProcessBackend: %v; stderr=%s", component, err, backend.stderr.String())
		}
	}
	events, err := os.ReadFile(eventLog)
	if err != nil {
		t.Fatalf("read conformance events: %v", err)
	}
	for _, component := range components {
		for event, count := range map[string]int{
			"initialize:" + component: 1,
			"prepare:" + component:    2,
			"execute:" + component:    2,
			"shutdown:" + component:   1,
		} {
			if got := strings.Count(string(events), event+"\n"); got != count {
				t.Fatalf("event %q count=%d want=%d; events=%s", event, got, count, events)
			}
		}
	}
}

func newFastH3ConformanceBackend(
	t *testing.T,
	root,
	eventLog,
	fastH3Root,
	fixtureRoot,
	python,
	component string,
) *fastH3ConformanceBackend {
	t.Helper()
	componentRoot := filepath.Join(root, strings.ToLower(component))
	inputRoot := filepath.Join(componentRoot, "inputs")
	outputRoot := filepath.Join(componentRoot, "outputs")
	for _, directory := range []string{componentRoot, inputRoot, outputRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatalf("create %s conformance directory: %v", component, err)
		}
	}
	stderr := &bytes.Buffer{}
	backend, err := NewProcessBackend(
		context.Background(),
		processBackendBinding(),
		ProcessBackendConfig{
			Component: component, ModelComponentRevision: "minimax-h3-" + strings.ToLower(component) + "-r1",
			Command: []string{
				python, "-m", "fast_h3.vela.driver", "--component", component,
				"--runtime-factory", "fast_h3_conformance_runtime:create_runtime",
			},
			Environment: []string{
				"PYTHONPATH=" + filepath.Join(fastH3Root, "src") + string(os.PathListSeparator) + fixtureRoot,
				"FAST_H3_CONFORMANCE_EVENT_LOG=" + eventLog,
			},
			LocalDevices: []DriverDevice{{
				DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
				GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
			}},
			ScratchRoot: componentRoot, InputRoot: inputRoot, OutputRoot: outputRoot,
			InitializationTimeout: 30 * time.Second,
			ShutdownTimeout:       5 * time.Second,
			Stderr:                stderr,
		},
	)
	if err != nil {
		t.Fatalf("start fast-h3 %s ProcessBackend: %v; stderr=%s", component, err, stderr.String())
	}
	t.Cleanup(func() { _ = backend.Close() })
	probe, err := backend.Probe(
		context.Background(),
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
	)
	if err != nil || !probe.Ready || !bytes.Contains(probe.Evidence, []byte(component)) {
		t.Fatalf("%s warmup probe = %#v error=%v; stderr=%s", component, probe, err, stderr.String())
	}
	return &fastH3ConformanceBackend{
		component: component, backend: backend, inputRoot: inputRoot,
		outputRoot: outputRoot, stderr: stderr,
	}
}

func runFastH3ConformanceStage(
	t *testing.T,
	backend *fastH3ConformanceBackend,
	authority stageauthority.Verified,
	spec *velav1.StageExecutionSpec,
) fastH3ConformanceOutput {
	t.Helper()
	if err := backend.backend.Prepare(context.Background(), authority, spec); err != nil {
		t.Fatalf("prepare %s: %v; stderr=%s", backend.component, err, backend.stderr.String())
	}
	if err := backend.backend.Start(context.Background(), authority); err != nil {
		t.Fatalf("start %s: %v; stderr=%s", backend.component, err, backend.stderr.String())
	}
	waitForFastH3Output(t, backend.backend, authority, backend.stderr)
	sealed, err := backend.backend.Seal(context.Background(), authority)
	if err != nil || sealed.TotalSizeBytes <= 0 {
		t.Fatalf("seal %s = %#v error=%v; stderr=%s", backend.component, sealed, err, backend.stderr.String())
	}
	var output fastH3ConformanceOutput
	if err := json.Unmarshal(sealed.OutputManifestJSON, &output); err != nil ||
		output.OutputPort == "" || output.LocalLocator == "" || output.PayloadSHA256 == "" ||
		output.SizeBytes != sealed.TotalSizeBytes {
		t.Fatalf("sealed %s manifest = %s error=%v", backend.component, sealed.OutputManifestJSON, err)
	}
	output.path = filepath.Join(backend.outputRoot, filepath.FromSlash(output.LocalLocator))
	payload, err := os.ReadFile(output.path)
	if err != nil {
		t.Fatalf("read %s output: %v", backend.component, err)
	}
	digest := sha256.Sum256(payload)
	if int64(len(payload)) != output.SizeBytes || hex.EncodeToString(digest[:]) != output.PayloadSHA256 {
		t.Fatalf("%s output does not match sealed manifest", backend.component)
	}
	return output
}

func materializeFastH3RootInput(
	t *testing.T,
	inputRoot string,
	authority stageauthority.Verified,
	digest [sha256.Size]byte,
	payload []byte,
) {
	t.Helper()
	path := filepath.Join(
		inputRoot, "stage-runs", authority.Authority.GetStageRunId(), "root-inputs", "0",
		hex.EncodeToString(digest[:])+".bin",
	)
	materializeFastH3File(t, path, payload)
}

func materializeFastH3StageInput(
	t *testing.T,
	inputRoot string,
	authority stageauthority.Verified,
	artifactID,
	objectVersion,
	interfaceRevisionID string,
	output fastH3ConformanceOutput,
) *velav1.StageInputArtifact {
	t.Helper()
	payload, err := os.ReadFile(output.path)
	if err != nil {
		t.Fatalf("read upstream H3 artifact: %v", err)
	}
	digest, err := hex.DecodeString(output.PayloadSHA256)
	if err != nil || len(digest) != sha256.Size {
		t.Fatalf("decode upstream H3 artifact digest %q: %v", output.PayloadSHA256, err)
	}
	path := filepath.Join(
		inputRoot, "stage-runs", authority.Authority.GetStageRunId(), "inputs", artifactID,
		output.PayloadSHA256+".bin",
	)
	materializeFastH3File(t, path, payload)
	return &velav1.StageInputArtifact{
		StageArtifactId: artifactID, ObjectVersion: objectVersion, Sha256: digest,
		SizeBytes: output.SizeBytes, StageInterfaceRevisionId: interfaceRevisionID,
	}
}

func materializeFastH3File(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create H3 material directory: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write H3 material: %v", err)
	}
}

func canonicalConformancePath(t *testing.T, path string) string {
	t.Helper()
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("make conformance path absolute: %v", err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		t.Fatalf("canonicalize conformance path %s: %v", absolute, err)
	}
	return canonical
}

func absoluteConformancePath(t *testing.T, path string) string {
	t.Helper()
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("make conformance path absolute: %v", err)
	}
	if filepath.Clean(absolute) != absolute {
		t.Fatalf("conformance path is not clean: %s", absolute)
	}
	if _, err := os.Stat(absolute); err != nil {
		t.Fatalf("stat conformance path %s: %v", absolute, err)
	}
	return absolute
}

func waitForFastH3Output(
	t *testing.T,
	backend *ProcessBackend,
	authority stageauthority.Verified,
	stderr *bytes.Buffer,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := backend.Status(context.Background(), authority)
		if err != nil {
			t.Fatalf("read fast-h3 status: %v; stderr=%s", err, stderr.String())
		}
		if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY {
			return
		}
		if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED ||
			status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
			t.Fatalf("fast-h3 execution stopped in state %s detail=%s; stderr=%s", status.State, status.Detail, stderr.String())
		}
		if time.Now().After(deadline) {
			t.Fatalf("fast-h3 output did not become ready; last status=%#v; stderr=%s", status, stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
