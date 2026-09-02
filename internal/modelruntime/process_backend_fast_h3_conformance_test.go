package modelruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

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

	components := map[string]string{
		"ENCODER":     "conditioning",
		"DIT":         "latent",
		"VAE_DECODER": "video",
	}
	for component, outputPort := range components {
		t.Run(component, func(t *testing.T) {
			root := canonicalConformancePath(t, t.TempDir())
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatalf("make scratch root private: %v", err)
			}
			outputRoot := filepath.Join(root, "outputs")
			if err := os.Mkdir(outputRoot, 0o700); err != nil {
				t.Fatalf("create output root: %v", err)
			}
			eventLog := filepath.Join(root, "events.log")
			var stderr bytes.Buffer
			backend, err := NewProcessBackend(
				context.Background(),
				processBackendBinding(),
				ProcessBackendConfig{
					Component:              component,
					ModelComponentRevision: "minimax-h3-" + strings.ToLower(component) + "-r1",
					Command: []string{
						python,
						"-m",
						"fast_h3.vela.driver",
						"--component",
						component,
						"--runtime-factory",
						"fast_h3_conformance_runtime:create_runtime",
					},
					Environment: []string{
						"PYTHONPATH=" + filepath.Join(fastH3Root, "src") + string(os.PathListSeparator) + fixtureRoot,
						"FAST_H3_CONFORMANCE_EVENT_LOG=" + eventLog,
					},
					LocalDevices: []DriverDevice{{
						DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
						GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
					}},
					ScratchRoot: root, OutputRoot: outputRoot,
					InitializationTimeout: 10 * time.Second,
					ShutdownTimeout:       2 * time.Second,
					Stderr:                &stderr,
				},
			)
			if err != nil {
				t.Fatalf("start fast-h3 ProcessBackend: %v; stderr=%s", err, stderr.String())
			}
			t.Cleanup(func() { _ = backend.Close() })

			probe, err := backend.Probe(
				context.Background(),
				velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
			)
			if err != nil || !probe.Ready || !bytes.Contains(probe.Evidence, []byte(component)) {
				t.Fatalf("warmup probe = %#v error=%v; stderr=%s", probe, err, stderr.String())
			}

			for assignment := byte(1); assignment <= 2; assignment++ {
				authority := processBackendAuthority(assignment)
				manifest := []byte(`{"` + outputPort + `":{"required":true}}`)
				if err := backend.Prepare(
					context.Background(),
					authority,
					&velav1.StageExecutionSpec{
						ParametersJson:             []byte(`{}`),
						ExpectedOutputManifestJson: manifest,
					},
				); err != nil {
					t.Fatalf("prepare assignment %d: %v; stderr=%s", assignment, err, stderr.String())
				}
				if err := backend.Start(context.Background(), authority); err != nil {
					t.Fatalf("start assignment %d: %v; stderr=%s", assignment, err, stderr.String())
				}
				waitForFastH3Output(t, backend, authority, &stderr)
				sealed, err := backend.Seal(context.Background(), authority)
				if err != nil || sealed.TotalSizeBytes <= 0 {
					t.Fatalf("seal assignment %d = %#v error=%v; stderr=%s", assignment, sealed, err, stderr.String())
				}
				var decoded map[string]any
				if err := json.Unmarshal(sealed.OutputManifestJSON, &decoded); err != nil || decoded["output_port"] != outputPort {
					t.Fatalf("sealed manifest assignment %d = %s error=%v", assignment, sealed.OutputManifestJSON, err)
				}
			}

			if err := backend.Close(); err != nil {
				t.Fatalf("close fast-h3 ProcessBackend: %v; stderr=%s", err, stderr.String())
			}
			events, err := os.ReadFile(eventLog)
			if err != nil {
				t.Fatalf("read conformance events: %v", err)
			}
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
		})
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
	deadline := time.Now().Add(5 * time.Second)
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
			t.Fatalf("fast-h3 execution stopped in state %s; stderr=%s", status.State, stderr.String())
		}
		if time.Now().After(deadline) {
			t.Fatalf("fast-h3 output did not become ready; last status=%#v; stderr=%s", status, stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
