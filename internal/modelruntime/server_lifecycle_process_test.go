//go:build darwin || linux

package modelruntime_test

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const lifecycleProcessMode = "VELA_TEST_LIFECYCLE_PROCESS"
const lifecycleProcessRoot = "/evidence"

type lifecycleProcessIdentity struct {
	PID        int    `json:"pid"`
	ParentPID  int    `json:"parent_pid"`
	GroupID    int    `json:"group_id"`
	StartTicks string `json:"start_ticks"`
	Namespace  string `json:"pid_namespace"`
	BootID     string `json:"boot_id"`
}

// This helper runs only inside the explicitly enabled CPU container experiment.
// Its process observations are test evidence, never Runtime drain authority.
func TestRuntimeLifecycleProcessHelper(t *testing.T) {
	mode := os.Getenv(lifecycleProcessMode)
	if mode == "" {
		return
	}
	switch mode {
	case "init-volume":
		if err := os.Chmod(lifecycleProcessRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(lifecycleProcessRoot, 65534, 65534); err != nil {
			t.Fatal(err)
		}
	case "owner":
		runLifecycleOwner(t)
	case "replacement", "fresh-replacement":
		writeLifecycleJSON(t, "replacement-owner.json", readLifecycleIdentity(t, os.Getpid()))
		if mode == "fresh-replacement" {
			for _, name := range []string{"journal", "inputs", "outputs"} {
				if err := os.Mkdir(filepath.Join(lifecycleProcessRoot, name), 0o700); err != nil {
					t.Fatal(err)
				}
			}
		}
		_, config := lifecycleRuntimeConfig(t, "replacement-driver")
		config.ExecutionFloor.State.Initialize = mode == "fresh-replacement"
		journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
		if err != nil {
			t.Fatal(err)
		}
		writeLifecycleJSON(t, "replacement-journal.json", journal)
		config.ExecutionFloor.State.Initialize = false
		config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
		server, err := modelruntime.StartRuntimeServer(t.Context(), config)
		if err != nil {
			t.Fatal(err)
		}
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
		writeLifecycleJSON(t, "replacement-closed", true)
	case "wrapper":
		writeLifecycleJSON(t, "wrapper.json", readLifecycleIdentity(t, os.Getpid()))
		owner := lifecycleHelperCommand("owner")
		log, err := os.OpenFile(filepath.Join(lifecycleProcessRoot, "owner-log"), os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = log.Close() }()
		// Descendants may retain stderr after owner exit. A regular log file
		// keeps their lifetime independent of exec.Cmd's pipe-copy goroutines.
		owner.Stdout, owner.Stderr = log, log
		err = owner.Run()
		var exited *exec.ExitError
		if !errors.As(err, &exited) || exited.ExitCode() != 72 {
			output, _ := os.ReadFile(log.Name())
			t.Fatalf("owner did not exit at the selected crash boundary: %s %v", output, err)
		}
		writeLifecycleJSON(t, "owner-exited.json", map[string]int{"exit_code": exited.ExitCode()})
		waitLifecycleFile(t, "stop-wrapper")
	case "driver", "replacement-driver":
		runLifecycleDriver(t)
	case "writer":
		runLifecycleWriter(t)
	case "exit-owner", "stop-writer", "stop-wrapper":
		writeLifecycleJSON(t, mode, true)
	case "inspect-writer":
		var recorded lifecycleProcessIdentity
		readLifecycleJSON(t, "writer.json", &recorded)
		current := readLifecycleIdentity(t, recorded.PID)
		if current.PID != recorded.PID || current.StartTicks != recorded.StartTicks ||
			current.Namespace != recorded.Namespace || current.BootID != recorded.BootID || current.GroupID != recorded.GroupID {
			t.Fatalf("recorded writer identity changed: recorded=%+v current=%+v", recorded, current)
		}
		if err := json.NewEncoder(os.Stdout).Encode(current); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unexpected lifecycle helper mode %q", mode)
	}
	os.Exit(0)
}

func runLifecycleOwner(t *testing.T) {
	t.Helper()
	writeLifecycleJSON(t, "owner.json", readLifecycleIdentity(t, os.Getpid()))
	for _, name := range []string{"journal", "inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(lifecycleProcessRoot, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	f, config := lifecycleRuntimeConfig(t, "driver")
	initialize := *config.ExecutionFloor.State
	initialize.Initialize = true
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, initialize)
	if err != nil {
		t.Fatal(err)
	}
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
	go func() {
		waitLifecycleFile(t, "exit-owner")
		os.Exit(72)
	}()
	phase := os.Getenv("VELA_TEST_LIFECYCLE_PHASE")
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if phase == "initializing" {
		t.Fatal("initialization returned before the crash gate")
	}
	writeLifecycleJSON(t, "server-ready", true)
	if phase == "admitted" {
		client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{
			SocketPath: config.SocketPath, ExpectedUID: uint32(os.Geteuid()),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		authority := f.authority(t, 0, 10)
		authority.Members[0].ModelRuntimeEpoch++
		authority, err = f.signer.Sign(authority)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{
			Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
		})
		if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("durable Prepare was not admitted: %v %v", prepared, err)
		}
	} else if phase != "idle" && phase != "closed-idle" {
		t.Fatalf("unexpected lifecycle phase %q", phase)
	}
	if phase == "closed-idle" {
		if err := server.Close(); err != nil {
			t.Fatalf("graceful idle Close failed: %v", err)
		}
		writeLifecycleJSON(t, "owner-server-closed", true)
	}
	writeLifecycleJSON(t, "owner-ready", true)
	waitLifecycleFile(t, "never-release-owner")
}

func runLifecycleDriver(t *testing.T) {
	t.Helper()
	component := os.Getenv("VELA_TEST_LIFECYCLE_COMPONENT")
	replacement := os.Getenv(lifecycleProcessMode) == "replacement-driver"
	if replacement {
		writeLifecycleJSON(t, "replacement-"+component+".json", readLifecycleIdentity(t, os.Getpid()))
	} else if component == "ENCODER" {
		writeLifecycleJSON(t, "driver.json", readLifecycleIdentity(t, os.Getpid()))
	}
	phase := os.Getenv("VELA_TEST_LIFECYCLE_PHASE")
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	for {
		var request struct {
			RequestID uint64 `json:"request_id"`
			Operation string `json:"operation"`
		}
		if err := decoder.Decode(&request); errors.Is(err, io.EOF) {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		spawn := request.Operation == "initialize" && phase != "admitted" || request.Operation == "prepare" && phase == "admitted"
		if !replacement && component == "ENCODER" && spawn {
			writer := lifecycleHelperCommand("writer")
			writer.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := writer.Start(); err != nil {
				t.Fatal(err)
			}
			if err := writer.Process.Release(); err != nil {
				t.Fatal(err)
			}
			waitLifecycleFile(t, "writer-ready")
			if phase == "initializing" {
				writeLifecycleJSON(t, "owner-ready", true)
				waitLifecycleFile(t, "never-release-initialize")
			}
		}
		if err := encoder.Encode(map[string]any{"schema_version": 1, "request_id": request.RequestID,
			"acknowledged": true, "initialized": request.Operation == "initialize"}); err != nil {
			t.Fatal(err)
		}
		if request.Operation == "shutdown" {
			return
		}
	}
}

func lifecycleRuntimeConfig(t *testing.T, driverMode string) (*executionFloorFixture, modelruntime.RuntimeServerConfig) {
	t.Helper()
	f, err := executionFloorFixtureWithState(t, "", nil, 9, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	config := recoveredRuntimeServerConfig(t, f, filepath.Join(lifecycleProcessRoot, "journal"))
	config.EpochStore, err = modelruntime.NewFileEpochStore(filepath.Join(lifecycleProcessRoot, "epochs"))
	if err != nil {
		t.Fatal(err)
	}
	config.BackendFactory = nil
	config.Manifest.LocalDevices[0].GPUUUID, config.Manifest.LocalDevices[0].PCIBDF = "", ""
	config.Manifest.LocalDevices[0].ResourceClass = "CPU"
	for index := range config.Manifest.Runtimes {
		runtime := &config.Manifest.Runtimes[index]
		runtime.Command = []string{os.Args[0], "-test.run=^TestRuntimeLifecycleProcessHelper$"}
		runtime.Environment = []string{lifecycleProcessMode + "=" + driverMode,
			"VELA_TEST_LIFECYCLE_COMPONENT=" + runtime.Component,
			"VELA_TEST_LIFECYCLE_PHASE=" + os.Getenv("VELA_TEST_LIFECYCLE_PHASE")}
		runtime.ScratchRoot, runtime.InputRoot, runtime.OutputRoot = lifecycleProcessRoot,
			filepath.Join(lifecycleProcessRoot, "inputs"), filepath.Join(lifecycleProcessRoot, "outputs")
		runtime.InitializationTimeout, runtime.ShutdownTimeout = "30s", "1s"
	}
	return f, config
}

func runLifecycleWriter(t *testing.T) {
	t.Helper()
	writeLifecycleJSON(t, "writer.json", readLifecycleIdentity(t, os.Getpid()))
	file, err := os.OpenFile(filepath.Join(lifecycleProcessRoot, "writer-data"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	deadline := time.Now().Add(time.Minute)
	ready := false
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(lifecycleProcessRoot, "stop-writer")); err == nil {
			writeLifecycleJSON(t, "writer-stopped", true)
			return
		}
		if _, err := file.WriteString("write\n"); err != nil {
			t.Fatal(err)
		}
		if !ready {
			writeLifecycleJSON(t, "writer-ready", true)
			ready = true
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("writer exceeded the bounded experiment lifetime")
}

func lifecycleHelperCommand(mode string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestRuntimeLifecycleProcessHelper$")
	command.Env = append(os.Environ(), lifecycleProcessMode+"="+mode)
	return command
}

func readLifecycleIdentity(t *testing.T, pid int) lifecycleProcessIdentity {
	t.Helper()
	root := filepath.Join("/proc", strconv.Itoa(pid))
	stat, err := os.ReadFile(filepath.Join(root, "stat"))
	if err != nil {
		t.Fatal(err)
	}
	// The comm field is parenthesized and may contain spaces or parentheses.
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		t.Fatal("process stat has no command boundary")
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 20 {
		t.Fatal("process stat is truncated")
	}
	parent, parentErr := strconv.Atoi(fields[1])
	group, groupErr := strconv.Atoi(fields[2])
	start, startErr := strconv.ParseUint(fields[19], 10, 64)
	if parentErr != nil || groupErr != nil || startErr != nil || start == 0 {
		t.Fatalf("invalid process identity: %v %v %v", parentErr, groupErr, startErr)
	}
	namespace, err := os.Readlink(filepath.Join(root, "ns/pid"))
	if err != nil {
		t.Fatal(err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	return lifecycleProcessIdentity{PID: pid, ParentPID: parent, GroupID: group,
		StartTicks: fields[19], Namespace: namespace, BootID: strings.TrimSpace(string(boot))}
}

func writeLifecycleJSON(t *testing.T, name string, value any) {
	t.Helper()
	document, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lifecycleProcessRoot, name), document, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readLifecycleJSON(t *testing.T, name string, value any) {
	t.Helper()
	document, err := os.ReadFile(filepath.Join(lifecycleProcessRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(document, value); err != nil {
		t.Fatal(err)
	}
}

func waitLifecycleFile(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(lifecycleProcessRoot, name)); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lifecycle experiment gate timed out: %s", name)
}
