package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestRunServesResidentRuntimeUntilShutdown(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	root := t.TempDir()
	inputRoot := filepath.Join(root, "inputs")
	if err := os.Mkdir(inputRoot, 0o700); err != nil {
		t.Fatalf("create input root: %v", err)
	}
	outputRoot := filepath.Join(root, "outputs")
	if err := os.Mkdir(outputRoot, 0o700); err != nil {
		t.Fatalf("create output root: %v", err)
	}
	temporaryRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary root: %v", err)
	}
	socketRoot, err := os.MkdirTemp(temporaryRoot, "vela-mr-")
	if err != nil {
		t.Fatalf("create socket root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	socketPath := filepath.Join(socketRoot, "runtime.sock")
	eventPath := filepath.Join(root, "events.log")
	manifest := commandLaunchManifest(root, outputRoot, executable, eventPath)
	manifestPath := filepath.Join(root, "launch.json")
	writeCommandJSON(t, manifestPath, manifest)
	keyringPath := filepath.Join(root, "verifier-keyring.json")
	verifierKeyring, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{
		"authority-v1": make([]byte, 32),
	})
	if err != nil {
		t.Fatalf("derive verifier keyring: %v", err)
	}
	defer stageauthority.ClearKeyring(verifierKeyring)
	writeCommandJSON(t, keyringPath, map[string]string{
		"authority-v1": base64.StdEncoding.EncodeToString(verifierKeyring["authority-v1"]),
	})

	t.Setenv("VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_FILE", manifestPath)
	t.Setenv("VELA_MODEL_RUNTIME_AUTHORITY_VERIFIER_KEYRING_FILE", keyringPath)
	t.Setenv("VELA_MODEL_RUNTIME_EPOCH_DIRECTORY", filepath.Join(root, "epochs"))
	t.Setenv("VELA_MODEL_RUNTIME_SOCKET", socketPath)
	t.Setenv("VELA_MODEL_RUNTIME_CANCEL_TIMEOUT", "5s")
	t.Setenv("VELA_MODEL_RUNTIME_SHUTDOWN_TIMEOUT", "5s")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()

	waitForCommandEvent(t, eventPath, "initialize")
	waitForPrivateCommandSocket(t, socketPath)
	dialContext, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()
	client, err := modelruntimetransport.Dial(dialContext, modelruntimetransport.Config{
		SocketPath: socketPath, ExpectedUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	discovered, err := client.DiscoverRuntimeIdentities(
		context.Background(),
		&velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
			WorkerInstanceId: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
			WorkerMemberId: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch,
		},
	)
	_ = client.Close()
	if err != nil || len(discovered.GetIdentities()) != 1 ||
		discovered.GetIdentities()[0].GetModelRuntimeEpoch() != 1 {
		t.Fatalf("DiscoverRuntimeIdentities = %#v error=%v", discovered, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("vela-model-runtime did not stop")
	}
	waitForCommandEvent(t, eventPath, "shutdown")
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime socket remains after shutdown: %v", err)
	}
}

func TestRunPropagatesProductionAuthorityClockSkew(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	root := t.TempDir()
	inputRoot := filepath.Join(root, "inputs")
	outputRoot := filepath.Join(root, "outputs")
	for _, directory := range []string{inputRoot, outputRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatalf("create runtime directory: %v", err)
		}
	}
	manifestPath := filepath.Join(root, "launch.json")
	writeCommandJSON(
		t,
		manifestPath,
		commandLaunchManifest(root, outputRoot, executable, filepath.Join(root, "events.log")),
	)
	keyringPath := filepath.Join(root, "verifier-keyring.json")
	verifierKeyring, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{
		"authority-v1": make([]byte, 32),
	})
	if err != nil {
		t.Fatalf("derive verifier keyring: %v", err)
	}
	defer stageauthority.ClearKeyring(verifierKeyring)
	writeCommandJSON(t, keyringPath, map[string]string{
		"authority-v1": base64.StdEncoding.EncodeToString(verifierKeyring["authority-v1"]),
	})
	t.Setenv("VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_FILE", manifestPath)
	t.Setenv("VELA_MODEL_RUNTIME_AUTHORITY_VERIFIER_KEYRING_FILE", keyringPath)
	t.Setenv("VELA_MODEL_RUNTIME_EPOCH_DIRECTORY", filepath.Join(root, "epochs"))
	t.Setenv("VELA_MODEL_RUNTIME_SOCKET", filepath.Join(root, "runtime.sock"))
	t.Setenv("VELA_MODEL_RUNTIME_CANCEL_TIMEOUT", "5s")
	t.Setenv("VELA_MODEL_RUNTIME_SHUTDOWN_TIMEOUT", "5s")
	ctx, cancel := context.WithCancel(context.Background())
	var observed time.Duration
	err = runUsing(ctx, func(
		ctx context.Context,
		config modelruntime.RuntimeServerConfig,
	) (modelRuntimeServer, error) {
		observed = config.MaxClockSkew
		cancel()
		return canceledModelRuntimeServer{ctx: ctx}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runUsing error = %v, want context cancellation", err)
	}
	if observed != authoritypolicy.ProductionMaxClockSkew {
		t.Fatalf("runtime server clock skew = %s, want %s", observed, authoritypolicy.ProductionMaxClockSkew)
	}
}

type canceledModelRuntimeServer struct {
	ctx context.Context
}

func (server canceledModelRuntimeServer) Wait() error {
	<-server.ctx.Done()
	return server.ctx.Err()
}

func (server canceledModelRuntimeServer) Close() error {
	return server.ctx.Err()
}

func TestLoadCommandConfigRequiresCanonicalPathsAndBoundedDurations(t *testing.T) {
	root := t.TempDir()
	for name, value := range map[string]string{
		"VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_FILE":            filepath.Join(root, "launch.json"),
		"VELA_MODEL_RUNTIME_AUTHORITY_VERIFIER_KEYRING_FILE": filepath.Join(root, "verifier-keyring.json"),
		"VELA_MODEL_RUNTIME_EPOCH_DIRECTORY":                 filepath.Join(root, "epochs"),
		"VELA_MODEL_RUNTIME_SOCKET":                          filepath.Join(root, "runtime.sock"),
		"VELA_MODEL_RUNTIME_CANCEL_TIMEOUT":                  "5s",
		"VELA_MODEL_RUNTIME_SHUTDOWN_TIMEOUT":                "30s",
	} {
		t.Setenv(name, value)
	}
	configuration, err := loadCommandConfig()
	if err != nil {
		t.Fatalf("loadCommandConfig: %v", err)
	}
	if configuration.cancelTimeout != 5*time.Second || configuration.shutdownTimeout != 30*time.Second ||
		configuration.epochDirectory != filepath.Join(root, "epochs") {
		t.Fatalf("configuration = %#v", configuration)
	}

	t.Setenv("VELA_MODEL_RUNTIME_SOCKET", "relative.sock")
	if _, err := loadCommandConfig(); err == nil || !strings.Contains(err.Error(), "VELA_MODEL_RUNTIME_SOCKET") {
		t.Fatalf("relative socket error = %v", err)
	}
	t.Setenv("VELA_MODEL_RUNTIME_SOCKET", filepath.Join(root, "runtime.sock"))
	t.Setenv("VELA_MODEL_RUNTIME_CANCEL_TIMEOUT", "0s")
	if _, err := loadCommandConfig(); err == nil || !strings.Contains(err.Error(), "CANCEL_TIMEOUT") {
		t.Fatalf("zero cancellation timeout error = %v", err)
	}
}

func TestModelRuntimeCommandDriverHelper(t *testing.T) {
	if os.Getenv("GO_WANT_VELA_MODEL_RUNTIME_DRIVER") != "1" {
		return
	}
	eventPath := os.Getenv("TEST_VELA_MODEL_RUNTIME_DRIVER_EVENTS")
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			return
		}
		operation, _ := request["operation"].(string)
		requestID, _ := request["request_id"].(float64)
		appendCommandEvent(eventPath, operation)
		response := map[string]any{
			"schema_version": 1, "request_id": uint64(requestID),
		}
		switch operation {
		case "initialize":
			response["initialized"] = true
			response["acknowledged"] = true
		case "shutdown":
			response["acknowledged"] = true
			_ = encoder.Encode(response)
			return
		default:
			response["error"] = "unexpected helper operation"
		}
		if err := encoder.Encode(response); err != nil {
			return
		}
	}
}

func commandLaunchManifest(root, outputRoot, executable, eventPath string) modelruntime.LaunchManifest {
	return modelruntime.LaunchManifest{
		SchemaVersion:           1,
		WorkerProfileRevisionID: "72000000-0000-0000-0000-000000000001",
		WorkerRole:              "dit", CapacitySlots: 1,
		WorkerInstanceID: "22000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 3,
		WorkerMemberID: "42000000-0000-0000-0000-000000000001", WorkerMemberEpoch: 4,
		DeviceSetDigest: strings.Repeat("a", 64), MembershipDigest: strings.Repeat("b", 64),
		Devices: []modelruntime.LaunchDeviceEpoch{{ID: "32000000-0000-0000-0000-000000000001", Epoch: 5}},
		Members: []modelruntime.LaunchMemberEpoch{{ID: "42000000-0000-0000-0000-000000000001", Epoch: 4}},
		LocalDevices: []modelruntime.DriverDevice{{
			DeviceID: "32000000-0000-0000-0000-000000000001", DeviceEpoch: 5,
			GPUUUID: "GPU-00000000-0000-0000-0000-000000000002", PCIBDF: "0000:42:00.0",
		}},
		Runtimes: []modelruntime.LaunchRuntime{{
			ModelResidencyID:       "52000000-0000-0000-0000-000000000001",
			RuntimeIdentity:        "minimax-h3-dit-runtime-v1",
			StageProfileRevisionID: "62000000-0000-0000-0000-000000000001",
			Component:              "DIT", ModelComponentRevision: "minimax-h3-dit-r1",
			RuntimeImageDigest: strings.Repeat("c", 64),
			Command:            []string{executable, "-test.run=^TestModelRuntimeCommandDriverHelper$"},
			Environment: []string{
				"GO_WANT_VELA_MODEL_RUNTIME_DRIVER=1",
				"TEST_VELA_MODEL_RUNTIME_DRIVER_EVENTS=" + eventPath,
			},
			ScratchRoot: root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: outputRoot,
			InitializationTimeout: "5s", ShutdownTimeout: "5s",
		}},
	}
}

func writeCommandJSON(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode %s: %v", filepath.Base(path), err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write %s: %v", filepath.Base(path), err)
	}
}

func waitForCommandEvent(t *testing.T, path, event string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		content, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(content), event+"\n") {
			return
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read helper events: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper event %q did not arrive", event)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForPrivateCommandSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := os.Lstat(path)
		if err == nil && info.Mode()&os.ModeSocket != 0 && info.Mode().Perm() == 0o600 {
			return
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect ModelRuntime socket: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("private ModelRuntime socket did not become ready: info=%v error=%v", info, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func appendCommandEvent(path, event string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintln(file, event)
	_ = file.Close()
}
