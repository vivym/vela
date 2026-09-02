package modelruntime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestProcessBackendLoadsOnceAndStaysResidentAcrossAssignments(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	for _, component := range []string{"ENCODER", "DIT", "VAE_DECODER"} {
		t.Run(component, func(t *testing.T) {
			root := t.TempDir()
			inputRoot := filepath.Join(root, "inputs")
			outputRoot := filepath.Join(root, "outputs")
			if err := os.Mkdir(inputRoot, 0o700); err != nil {
				t.Fatalf("create input root: %v", err)
			}
			if err := os.Mkdir(outputRoot, 0o700); err != nil {
				t.Fatalf("create output root: %v", err)
			}
			logPath := filepath.Join(root, "driver-events.log")
			gatePath := filepath.Join(root, "allow-initialize")
			result := make(chan struct {
				backend *ProcessBackend
				err     error
			}, 1)
			go func() {
				backend, startErr := NewProcessBackend(
					context.Background(), processBackendBinding(), ProcessBackendConfig{
						Component: component, ModelComponentRevision: "minimax-h3-" + strings.ToLower(component) + "-r1",
						Command: []string{executable, "-test.run=^TestProcessBackendDriverHelper$"},
						Environment: []string{
							"GO_WANT_MODEL_RUNTIME_DRIVER_HELPER=1",
							"TEST_MODEL_RUNTIME_DRIVER_LOG=" + logPath,
							"TEST_MODEL_RUNTIME_DRIVER_GATE=" + gatePath,
						},
						LocalDevices: []DriverDevice{{
							DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
							GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
						}},
						ScratchRoot: root, InputRoot: inputRoot, OutputRoot: outputRoot,
						InitializationTimeout: 10 * time.Second, ShutdownTimeout: 2 * time.Second,
					},
				)
				result <- struct {
					backend *ProcessBackend
					err     error
				}{backend: backend, err: startErr}
			}()
			waitForDriverEvent(t, logPath, "initialize")
			select {
			case started := <-result:
				if started.backend != nil {
					_ = started.backend.Close()
				}
				t.Fatalf("backend became ready before driver load/warmup gate: %v", started.err)
			default:
			}
			if err := os.WriteFile(gatePath, []byte("ready\n"), 0o600); err != nil {
				t.Fatalf("release driver initialization: %v", err)
			}
			started := <-result
			if started.err != nil {
				t.Fatalf("NewProcessBackend: %v", started.err)
			}
			backend := started.backend
			t.Cleanup(func() { _ = backend.Close() })

			probe, err := backend.Probe(
				context.Background(),
				velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
			)
			if err != nil || !probe.Ready || string(probe.Evidence) != component+":loaded-once" {
				t.Fatalf("Probe = %#v error=%v", probe, err)
			}
			for assignment := byte(1); assignment <= 2; assignment++ {
				verified := processBackendAuthority(assignment)
				if err := backend.Prepare(
					context.Background(), verified,
					&velav1.StageExecutionSpec{ParametersJson: []byte(`{"steps":30}`)},
				); err != nil {
					t.Fatalf("Prepare assignment %d: %v", assignment, err)
				}
				if err := backend.Start(context.Background(), verified); err != nil {
					t.Fatalf("Start assignment %d: %v", assignment, err)
				}
				status, err := backend.Status(context.Background(), verified)
				if err != nil || status.State !=
					velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING {
					t.Fatalf("Status assignment %d = %#v error=%v", assignment, status, err)
				}
				if err := backend.Cancel(
					context.Background(), verified,
					velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				); err != nil {
					t.Fatalf("Cancel assignment %d: %v", assignment, err)
				}
			}
			if err := backend.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			events, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatalf("read driver events: %v", err)
			}
			lines := strings.Fields(string(events))
			for event, want := range map[string]int{
				"initialize": 1, "prepare": 2, "start": 2, "cancel": 2, "shutdown": 1,
			} {
				got := 0
				for _, line := range lines {
					if line == event {
						got++
					}
				}
				if got != want {
					t.Fatalf("driver event %q count=%d want=%d; events=%q", event, got, want, events)
				}
			}
		})
	}
}

func TestProcessBackendDriverHelper(t *testing.T) {
	if os.Getenv("GO_WANT_MODEL_RUNTIME_DRIVER_HELPER") != "1" {
		return
	}
	logPath := os.Getenv("TEST_MODEL_RUNTIME_DRIVER_LOG")
	gatePath := os.Getenv("TEST_MODEL_RUNTIME_DRIVER_GATE")
	state := "PREPARING"
	component := ""
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64<<10), maxDriverMessageBytes)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request driverRequestV1
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			return
		}
		appendDriverEvent(logPath, request.Operation)
		response := driverResponseV1{
			SchemaVersion: driverProtocolVersion, RequestID: request.RequestID,
		}
		switch request.Operation {
		case "initialize":
			component = request.Initialize.Component
			if request.Initialize.InputRoot == "" ||
				request.Initialize.InputRoot == request.Initialize.ScratchRoot ||
				request.Initialize.InputRoot == request.Initialize.OutputRoot {
				response.Error = "driver input root is invalid"
				break
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(gatePath); err == nil {
					break
				}
				if time.Now().After(deadline) {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			response.Initialized = true
			response.Acknowledged = true
		case "probe":
			response.Probe = &driverProbeResultV1{
				Ready: true, Evidence: []byte(component + ":loaded-once"), Detail: "ready",
			}
		case "prepare":
			if !completeDriverLineageIdentity(request.Prepare.Identity) {
				response.Error = "stage lineage identity is incomplete"
				break
			}
			state = "PREPARED"
			response.Acknowledged = true
		case "start":
			state = "RUNNING"
			response.Acknowledged = true
		case "status":
			progress := 0.5
			response.Status = &driverStatusV1{
				State: state, Sequence: 3, BackendStage: strings.ToLower(component),
				Progress: &progress, BoundedStatusJSON: []byte(`{"driver":"ready"}`),
			}
		case "cancel":
			state = "STOPPED"
			response.Acknowledged = true
		case "seal":
			response.Output = &driverOutputV1{
				OutputManifestJSON: []byte(`{"schema_version":1}`), TotalSizeBytes: 128,
			}
		case "shutdown":
			response.Acknowledged = true
			_ = encoder.Encode(response)
			return
		default:
			response.Error = "unsupported helper operation"
		}
		if err := encoder.Encode(response); err != nil {
			return
		}
	}
}

func processBackendBinding() stageauthority.RuntimeBinding {
	return stageauthority.RuntimeBinding{
		WorkerInstanceID: "23000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 5,
		WorkerMemberID: "43000000-0000-0000-0000-000000000001", WorkerMemberEpoch: 8,
		DeviceSetDigest:      []byte(strings.Repeat("a", sha256.Size)),
		Devices:              []stageauthority.DeviceEpoch{{ID: "33000000-0000-0000-0000-000000000001", Epoch: 7}},
		MembershipDigest:     []byte(strings.Repeat("b", sha256.Size)),
		Members:              []stageauthority.MemberEpoch{{ID: "43000000-0000-0000-0000-000000000001", Epoch: 8}},
		ModelResidencyID:     "53000000-0000-0000-0000-000000000001",
		ModelRuntimeIdentity: "h3-process-runtime-v1", ModelRuntimeEpoch: 9,
		StageProfileRevisionID: "63000000-0000-0000-0000-000000000001",
	}
}

func processBackendAuthority(seed byte) stageauthority.Verified {
	digest := sha256.Sum256([]byte{seed})
	return stageauthority.Verified{
		Authority: &velav1.StageAuthority{
			JobId:                  "13000000-0000-0000-0000-000000000001",
			AttemptId:              "13000000-0000-0000-0000-000000000002",
			StageRunId:             "13000000-0000-0000-0000-000000000003",
			StageAttemptId:         fmt.Sprintf("13000000-0000-0000-0000-00000000000%d", seed+3),
			StageLeaseId:           fmt.Sprintf("13000000-0000-0000-0000-00000000001%d", seed),
			AttemptFence:           2,
			StageFence:             int64(seed) + 2,
			StageVersion:           int64(seed) + 3,
			StageProfileRevisionId: "63000000-0000-0000-0000-000000000001",
		},
		Digest: digest,
	}
}

func completeDriverLineageIdentity(identity driverStageIdentityV1) bool {
	return identity.AttemptFence > 0 && identity.StageFence > 0 && identity.StageVersion > 0 &&
		identity.StageProfileRevisionID == "63000000-0000-0000-0000-000000000001"
}

func waitForDriverEvent(t *testing.T, path, event string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		content, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(content), event+"\n") {
			return
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read driver event log: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("driver event %q did not arrive", event)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func appendDriverEvent(path, event string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintln(file, event)
	_ = file.Close()
}
