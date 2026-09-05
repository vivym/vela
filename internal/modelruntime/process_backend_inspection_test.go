//go:build darwin || linux

package modelruntime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/driverdrain"
	"github.com/vivym/vela/internal/driverinspection"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestProcessBackendInspectionFailuresPreserveResidentCancellation(t *testing.T) {
	for _, mode := range []string{"silent", "malformed", "late"} {
		t.Run(mode, func(t *testing.T) {
			backend, logPath := startInspectionProcess(t, mode)
			authority := processBackendAuthority(1)
			if err := backend.Prepare(context.Background(), authority, &velav1.StageExecutionSpec{}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Start(context.Background(), authority); err != nil {
				t.Fatal(err)
			}
			queryCtx, queryCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer queryCancel()
			completed := make(chan error, 1)
			go func() {
				observation, err := backend.InspectExecution(queryCtx, authority)
				if observation.Known {
					err = errors.New("failed query returned a state observation")
				}
				completed <- err
			}()
			waitForDriverEvent(t, logPath, "inspect")
			if mode == "late" {
				// Reply to this query arrives only after the later Cancel command.
				if err := <-completed; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("initial query did not expire: %v", err)
				}
			}
			cancelCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if err := backend.Cancel(cancelCtx, authority, velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP); err != nil {
				t.Fatalf("inspection blocked resident cancellation: %v", err)
			}
			if mode != "late" {
				if err := <-completed; err == nil {
					t.Fatal("failed inspection returned success")
				}
			}
			probe, err := backend.Probe(context.Background(), velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP)
			if err != nil || !probe.Ready || backend.Err() != nil {
				t.Fatalf("inspection failure unloaded driver: %+v %v", probe, err)
			}
			if mode == "late" {
				observed, err := backend.InspectExecution(context.Background(), authority)
				if err != nil || !observed.Known || observed.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED || observed.Sequence != 2 {
					t.Fatalf("late response contaminated retry: %+v %v", observed, err)
				}
			}
			if err := backend.Close(); err != nil {
				t.Fatalf("resident driver could not shut down normally: %v", err)
			}
		})
	}
}

func startInspectionProcess(t *testing.T, mode string) (*ProcessBackend, string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	inputRoot, outputRoot := filepath.Join(root, "inputs"), filepath.Join(root, "outputs")
	for _, path := range []string{inputRoot, outputRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	logPath := filepath.Join(root, "events")
	backend, err := NewProcessBackend(context.Background(), processBackendBinding(), ProcessBackendConfig{
		Component: "ENCODER", ModelComponentRevision: "inspection-test-v1",
		Command:     []string{executable, "-test.run=^TestProcessBackendInspectionDriverHelper$"},
		Environment: []string{"VELA_TEST_INSPECTION_DRIVER=" + mode, "VELA_TEST_INSPECTION_LOG=" + logPath},
		LocalDevices: []DriverDevice{{DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
			GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0"}},
		ScratchRoot: root, InputRoot: inputRoot, OutputRoot: outputRoot,
		InitializationTimeout: 5 * time.Second, ShutdownTimeout: 2 * time.Second, Stderr: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	return backend, logPath
}

func TestProcessBackendInspectionDriverHelper(t *testing.T) {
	mode := os.Getenv("VELA_TEST_INSPECTION_DRIVER")
	if mode == "" {
		return
	}
	conn, err := driverinspection.OpenInherited(os.Getenv(driverinspection.Environment))
	if err != nil || conn == nil {
		t.Fatal("helper inspection channel is missing")
	}
	defer func() { _ = conn.Close() }()
	released := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(released) })
	if strings.HasPrefix(mode, "drain-") {
		go serveFaultyDrain(strings.TrimPrefix(mode, "drain-"), released)
	}
	go func() {
		packet := make([]byte, 1025)
		for {
			n, err := conn.Read(packet)
			if err != nil {
				return
			}
			appendDriverEvent(os.Getenv("VELA_TEST_INSPECTION_LOG"), "inspect")
			if mode == "silent" {
				continue
			}
			if mode == "malformed" {
				_, _ = conn.Write([]byte("{"))
				continue
			}
			var query struct {
				SchemaVersion   int    `json:"schema_version"`
				RequestID       uint64 `json:"request_id"`
				AuthorityDigest string `json:"authority_digest"`
			}
			if json.Unmarshal(packet[:n], &query) != nil {
				return
			}
			<-released
			encoded, _ := json.Marshal(map[string]any{
				"schema_version": 1, "request_id": query.RequestID, "authority_digest": query.AuthorityDigest,
				"observation": driverinspection.Observation{Known: true, State: "STOPPED", Sequence: int64(query.RequestID)},
			})
			_, _ = conn.Write(encoded)
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64<<10), maxDriverMessageBytes)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request driverRequestV1
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			return
		}
		response := driverResponseV1{SchemaVersion: 1, RequestID: request.RequestID, Acknowledged: true}
		switch request.Operation {
		case "initialize":
			response.Initialized, response.InspectionProtocol = true, driverinspection.Protocol
			if strings.HasPrefix(mode, "drain-") {
				response.DrainProtocol = driverdrain.Protocol
			}
		case "cancel":
			release.Do(func() { close(released) })
		case "probe":
			response.Probe = &driverProbeResultV1{Ready: true}
		case "shutdown":
			_ = encoder.Encode(response)
			return
		}
		if encoder.Encode(response) != nil {
			return
		}
	}
}
