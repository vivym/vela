//go:build darwin || linux

package modelruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeServerRecoveryDoesNotLaunchBesideSurvivingProcessWriter(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	stopWriter := func() {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, "stop-writer"), nil, 0o600); err != nil {
			t.Error(err)
		}
		if _, err := os.Stat(filepath.Join(root, "writer-ready")); err == nil {
			waitRecoveryProcessFile(t, filepath.Join(root, "writer-stopped"))
		}
	}
	t.Cleanup(stopWriter)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryStartupProcessHelper$")
	command.Env = append(os.Environ(), "VELA_TEST_RECOVERY_PROCESS=owner", "VELA_TEST_RECOVERY_PROCESS_ROOT="+root,
		"VELA_TEST_RECOVERY_PROCESS_JOURNAL="+directory, "GORACE=atexit_sleep_ms=0")
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 72 {
		t.Fatalf("Runtime owner did not exit after durable admission: %s %v", output, err)
	}
	waitRecoveryProcessFile(t, filepath.Join(root, "writer-ready"))
	f := newExecutionFloorFixture(t, "")
	state := readDurableExecutionState(t, directory)
	var records []retainedExecutionDocument
	if err := json.Unmarshal(state.Executions, &records); err != nil || len(records) != 1 {
		t.Fatalf("owner exit lost pending execution: %v", err)
	}
	original := &velav1.StageAuthority{}
	if err := proto.Unmarshal(records[0].Authority, original); err != nil {
		t.Fatal(err)
	}
	f.authorities[0] = original
	config := recoveredRuntimeServerConfig(t, f, directory)
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || journal.PendingExecutions != 1 {
		t.Fatalf("owner exit did not retain pending state: %+v %v", journal, err)
	}
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
	config.BackendFactory = nil
	for index := range config.Manifest.Runtimes {
		runtime := &config.Manifest.Runtimes[index]
		runtime.Command = []string{os.Args[0], "-test.run=^TestRecoveryStartupProcessHelper$"}
		runtime.Environment = []string{"VELA_TEST_RECOVERY_PROCESS=replacement", "VELA_TEST_RECOVERY_PROCESS_ROOT=" + root, "GORACE=atexit_sleep_ms=0"}
		runtime.ScratchRoot, runtime.InputRoot, runtime.OutputRoot = root, filepath.Join(root, "inputs"), filepath.Join(root, "outputs")
		runtime.InitializationTimeout, runtime.ShutdownTimeout = "2s", "1s"
	}
	before, err := os.Stat(filepath.Join(root, "writer-data"))
	if err != nil {
		t.Fatal(err)
	}
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: config.SocketPath, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	current := f.bindings[0]
	current.ModelRuntimeEpoch++
	identity := discoverExecutionFloorIdentity(t, client, current)
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: identity, Authority: original}
	read, err := client.InspectStageAllocationAuthorities(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope})
	if err != nil || !proto.Equal(read.GetAuthorities().GetOriginal(), original) {
		t.Fatalf("surviving writer prevented historical query: %v %v", read, err)
	}
	installed, err := client.InstallStageExecutionFloor(t.Context(), &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
		SchemaVersion: 2, Identity: identity, Disposition: f.disposition(t),
	})
	if err != nil || !installed.GetDurable() || installed.GetInstalledCutoff() != 11 {
		t.Fatalf("recovery server could not install durable restriction: %v %v", installed, err)
	}
	assertRecoveryServerDeniesExecution(t, f, client, identity)
	deadline := time.Now().Add(2 * time.Second)
	for {
		after, err := os.Stat(filepath.Join(root, "writer-data"))
		if err != nil {
			t.Fatal(err)
		}
		if after.Size() > before.Size() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("test did not retain a live writer after Runtime owner exit")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(root, "replacement-started")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending writer allowed the replacement driver to start: %v", err)
	}
	stopWriter()
	// The test's stop marker is not a trusted Runtime drain checkpoint.
	assertRecoveryServerDeniesExecution(t, f, client, identity)
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "replacement-started")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("another restart inferred drain from process disappearance: %v", err)
	}
	// Positive control: the same default process factory launches these commands
	// from an independently initialized empty journal after the writer has stopped.
	control := config
	control.RegistryBinding, control.RegistryVerifier = nil, nil
	control.ExecutionFloor = &modelruntime.ExecutionFloorConfig{State: &modelruntime.ExecutionFloorStateConfig{
		Directory: privateExecutionStateDirectory(t), Initialize: true,
	}}
	started, err := modelruntime.StartRuntimeServer(t.Context(), control)
	if err != nil {
		t.Fatalf("default driver positive control failed: %v", err)
	}
	if err := started.Close(); err != nil {
		t.Fatal(err)
	}
	waitRecoveryProcessFile(t, filepath.Join(root, "replacement-started"))
}

func waitRecoveryProcessFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process marker missing: %s", filepath.Base(path))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRecoveryStartupProcessHelper(t *testing.T) {
	mode := os.Getenv("VELA_TEST_RECOVERY_PROCESS")
	if mode == "" {
		return
	}
	root := os.Getenv("VELA_TEST_RECOVERY_PROCESS_ROOT")
	if mode == "owner" {
		backend, err := modelruntime.NewProcessBackend(t.Context(), runtimeBinding(), modelruntime.ProcessBackendConfig{
			Component: "ENCODER", ModelComponentRevision: "cpu-recovery-startup-test",
			Command:      []string{os.Args[0], "-test.run=^TestRecoveryStartupProcessHelper$"},
			Environment:  []string{"VELA_TEST_RECOVERY_PROCESS=driver", "GORACE=atexit_sleep_ms=0"},
			LocalDevices: []modelruntime.DriverDevice{{DeviceID: runtimeBinding().Devices[0].ID, DeviceEpoch: runtimeBinding().Devices[0].Epoch, ResourceClass: "CPU"}},
			ScratchRoot:  root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
			InitializationTimeout: 3 * time.Second, ShutdownTimeout: time.Second, Stderr: io.Discard,
		})
		if err != nil {
			t.Fatal(err)
		}
		f := newExecutionDrainFixture(t, os.Getenv("VELA_TEST_RECOVERY_PROCESS_JOURNAL"), backend)
		prepareFloorRuntime(t, f.supervisor, f.authorities[0])
		os.Exit(72)
	}
	if mode == "writer" {
		file, err := os.OpenFile(filepath.Join(root, "writer-data"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(root, "stop-writer")); err == nil {
				break
			}
			if _, err := file.Write([]byte("write\n")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "writer-ready"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "writer-stopped"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	if mode != "driver" && mode != "replacement" {
		t.Fatalf("unexpected recovery helper mode %q", mode)
	}
	if mode == "replacement" {
		if err := os.WriteFile(filepath.Join(root, "replacement-started"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	for {
		var request struct {
			RequestID uint64 `json:"request_id"`
			Operation string `json:"operation"`
		}
		if err := decoder.Decode(&request); errors.Is(err, io.EOF) {
			os.Exit(0)
		} else if err != nil {
			t.Fatal(err)
		}
		if mode == "driver" && request.Operation == "prepare" {
			writer := exec.Command(os.Args[0], "-test.run=^TestRecoveryStartupProcessHelper$")
			writer.Env = append(os.Environ(), "VELA_TEST_RECOVERY_PROCESS=writer")
			writer.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := writer.Start(); err != nil {
				t.Fatal(err)
			}
			if err := writer.Process.Release(); err != nil {
				t.Fatal(err)
			}
			waitRecoveryProcessFile(t, filepath.Join(root, "writer-ready"))
		}
		if err := encoder.Encode(map[string]any{"schema_version": 1, "request_id": request.RequestID,
			"acknowledged": true, "initialized": request.Operation == "initialize"}); err != nil {
			t.Fatal(err)
		}
		if request.Operation == "shutdown" {
			os.Exit(0)
		}
	}
}
