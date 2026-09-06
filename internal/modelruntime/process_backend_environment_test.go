package modelruntime

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/driverdrain"
	"github.com/vivym/vela/internal/driverinspection"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const environmentHelperArgument = "vela-driver-environment-helper"

type driverEnvironmentSample struct {
	Names    []string          `json:"names"`
	Selected map[string]string `json:"selected"`
}

func TestProcessBackendUsesOnlyDeclaredEnvironment(t *testing.T) {
	t.Setenv("VELA_TEST_UNDECLARED_RUNTIME_CONFIG", "parent-only")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "synthetic-parent-secret")
	t.Setenv("HTTP_PROXY", "http://unapproved.invalid:9999")
	t.Setenv("CUDA_VISIBLE_DEVICES", "parent-device-selection")
	t.Setenv("LD_LIBRARY_PATH", "/unapproved/parent-libraries")
	t.Setenv("PATH", "/unapproved/parent-bin")
	for _, scenario := range []string{"omitted", "explicit", "explicit-empty", "encoded-value"} {
		t.Run(scenario, func(t *testing.T) {
			config := driverEnvironmentConfig(t)
			if scenario != "omitted" {
				for _, name := range []string{"PATH", "LD_LIBRARY_PATH", "CUDA_VISIBLE_DEVICES", "HTTP_PROXY"} {
					value := ""
					if scenario == "explicit" {
						value = "approved-" + name
					}
					config.Environment = append(config.Environment, name+"="+value)
				}
			}
			if scenario == "encoded-value" {
				config.Environment = append(config.Environment, "VELA_TEST_VALUE=\u4e2d\u6587=a=b\n")
			}
			backend, err := NewProcessBackend(t.Context(), processBackendBinding(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = backend.Close() })
			probe, err := backend.Probe(t.Context(), velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND)
			if err != nil || !probe.Ready {
				t.Fatalf("environment helper did not initialize/probe: %v", err)
			}
			var sample driverEnvironmentSample
			if err := json.Unmarshal(probe.Evidence, &sample); err != nil {
				t.Fatal(err)
			}
			expected := map[string]string{"VELA_MODEL_DRIVER_PROTOCOL": "stdio-json-v1", driverinspection.Environment: "3", driverdrain.Environment: "4"}
			for _, entry := range config.Environment {
				name, value, _ := strings.Cut(entry, "=")
				expected[name] = value
			}
			for _, name := range sample.Names {
				if _, declared := expected[name]; !declared {
					t.Errorf("driver inherited undeclared environment key %q", name)
				}
			}
			if len(sample.Names) != len(expected) {
				t.Errorf("driver environment key count = %d, expected %d", len(sample.Names), len(expected))
			}
			for name, value := range expected {
				if !slices.Contains(sample.Names, name) || sample.Selected[name] != value {
					t.Errorf("declared/protocol environment key %q was omitted or changed", name)
				}
			}
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProcessBackendRejectsInvalidEnvironmentBeforeSpawn(t *testing.T) {
	tooMany := make([]string, 129)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("ENTRY_%d=value", index)
	}
	for _, test := range []struct {
		name        string
		environment []string
	}{
		{"protocol", []string{"VELA_MODEL_DRIVER_PROTOCOL=override"}},
		{"inspection-channel", []string{driverinspection.Environment + "=99"}},
		{"drain-channel", []string{driverdrain.Environment + "=99"}},
		{"duplicate", []string{"KEY=one", "KEY=two"}},
		{"missing-equals", []string{"KEY"}},
		{"empty-name", []string{"=value"}},
		{"nul-name", []string{"KE\x00Y=value"}},
		{"nul-value", []string{"KEY=val\x00ue"}},
		{"invalid-utf8", []string{"KEY=\xff"}},
		{"oversized-entry", []string{"K=" + strings.Repeat("x", 4095)}},
		{"too-many-entries", tooMany},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := driverEnvironmentConfig(t)
			config.Environment = test.environment
			// A missing executable distinguishes validation from a spawn failure.
			config.Command = []string{filepath.Join(config.ScratchRoot, "not-an-executable")}
			backend, err := NewProcessBackend(t.Context(), processBackendBinding(), config)
			if backend != nil {
				_ = backend.Close()
				t.Fatal("invalid environment produced a backend")
			}
			if err == nil || !strings.Contains(err.Error(), "driver environment") {
				t.Fatalf("expected environment validation before spawn, got %v", err)
			}
		})
	}
}

func driverEnvironmentConfig(t *testing.T) ProcessBackendConfig {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	input, output := filepath.Join(root, "input"), filepath.Join(root, "output")
	for _, path := range []string{input, output} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return ProcessBackendConfig{Component: "ENCODER", ModelComponentRevision: "environment-fixture-r1",
		Command:      []string{binary, "-test.run=^TestDriverEnvironmentHelper$", "--", environmentHelperArgument},
		LocalDevices: []DriverDevice{{DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7, ResourceClass: "CPU"}},
		ScratchRoot:  root, InputRoot: input, OutputRoot: output, InitializationTimeout: 5 * time.Second, ShutdownTimeout: 3 * time.Second,
		Stderr: io.Discard}
}

func TestDriverEnvironmentHelper(t *testing.T) {
	if !slices.Contains(os.Args, environmentHelperArgument) {
		return
	}
	sample := driverEnvironmentSample{Selected: make(map[string]string)}
	// Never copy inherited secret values into test evidence. Only explicitly
	// synthetic configuration and fixed protocol values are sampled below.
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		sample.Names = append(sample.Names, name)
	}
	for _, name := range []string{"VELA_TEST_VALUE", "PATH", "LD_LIBRARY_PATH", "CUDA_VISIBLE_DEVICES", "HTTP_PROXY",
		"VELA_MODEL_DRIVER_PROTOCOL", driverinspection.Environment, driverdrain.Environment} {
		if value, found := os.LookupEnv(name); found {
			sample.Selected[name] = value
		}
	}
	evidence, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	scanner, writer := bufio.NewScanner(os.Stdin), json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request driverRequestV1
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			t.Fatal(err)
		}
		response := driverResponseV1{SchemaVersion: driverProtocolVersion, RequestID: request.RequestID, Acknowledged: true}
		switch request.Operation {
		case "initialize":
			response.Initialized = true
		case "probe":
			response.Probe = &driverProbeResultV1{Ready: true, Evidence: evidence}
		case "shutdown":
			response.Acknowledged = true
		default:
			response.Error = "unsupported environment test operation"
		}
		if err := writer.Encode(response); err != nil {
			t.Fatal(err)
		}
		if request.Operation == "shutdown" {
			return
		}
	}
}
