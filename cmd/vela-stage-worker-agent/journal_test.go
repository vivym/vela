package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestJournalCommandRequiresExplicitActionAndNeverInfersBootstrap(t *testing.T) {
	arguments, directory := journalCommandFixture(t)
	for _, prefix := range [][]string{
		{"unknown"}, {"journal"}, {"journal", "--action", "recover"},
		{"journal", "--action", "upgrade-v2"}, {"journal", "--action", "upgrade-v3"},
		{"journal", "--action", "upgrade-v4"}, {"journal", "--action", "invalid"},
		{"journal", "--action", "initialize", "--unknown"},
	} {
		var output bytes.Buffer
		args := append(append([]string(nil), prefix...), arguments...)
		if err := runCommand(t.Context(), args, &output, io.Discard); err == nil || output.Len() != 0 {
			t.Fatalf("invalid/empty recovery acquired success: args=%v err=%v output=%s", args, err, output.String())
		}
		if entries, err := os.ReadDir(directory); err != nil || len(entries) != 0 {
			t.Fatalf("failed preparation initialized state: %v %v", entries, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runCommand(ctx, append([]string{"journal", "--action", "initialize"}, arguments...), io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled initialization: %v", err)
	}
}

func TestJournalCommandInitializesOfflineAndPreservesCompleteMemberScope(t *testing.T) {
	arguments, directory := journalCommandFixture(t)
	// A serving process could not start with this environment or missing backend.
	t.Setenv("VELA_WORKER_INSTANCE_ID", "")
	prepare := func(action string) (stageworkeragent.AssignmentJournalStatus, error) {
		t.Helper()
		var output bytes.Buffer
		err := runCommand(t.Context(), append([]string{"journal", "--action", action}, arguments...), &output, io.Discard)
		if err != nil {
			if output.Len() != 0 {
				t.Fatal("failed preparation wrote a success response")
			}
			return stageworkeragent.AssignmentJournalStatus{}, err
		}
		var result struct {
			Action  string                                   `json:"action"`
			Journal stageworkeragent.AssignmentJournalStatus `json:"journal"`
		}
		if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Action != action {
			t.Fatalf("journal output: %s %v", output.String(), err)
		}
		return result.Journal, nil
	}
	first, err := prepare("initialize")
	if err != nil || first.JournalID == uuid.Nil || first.SchemaVersion != 5 || first.Watermark != 0 {
		t.Fatalf("initialize: %+v %v", first, err)
	}
	if _, err := prepare("initialize"); err == nil {
		t.Fatal("reinitialization accepted")
	}
	second, err := prepare("recover")
	if err != nil || second != first {
		t.Fatalf("offline recovery: %+v %v", second, err)
	}
	path := filepath.Join(directory, "assignment-admission.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := modelruntime.LoadLaunchManifest(arguments[1])
	if err != nil {
		t.Fatal(err)
	}
	manifest.Members[1].DeviceSubsetDigest = strings.Repeat("f", 64)
	writeJournalJSON(t, arguments[1], manifest)
	if _, err := prepare("recover"); err == nil {
		t.Fatal("remote member subset change recovered under unchanged Worker epoch")
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("rejected recovery rewrote state: %v", err)
	}
}

func TestJournalCommandOutputFailureCanRecoverWithoutReinitialization(t *testing.T) {
	arguments, _ := journalCommandFixture(t)
	if err := runCommand(t.Context(), append([]string{"journal", "--action", "initialize"}, arguments...), failedJournalWriter{}, io.Discard); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("lost initialization output: %v", err)
	}
	if err := runCommand(t.Context(), append([]string{"journal", "--action", "recover"}, arguments...), io.Discard, io.Discard); err != nil {
		t.Fatalf("recovery after output loss: %v", err)
	}
}

func TestJournalCommandEntrypointRunsOfflineInSeparateProcess(t *testing.T) {
	arguments, _ := journalCommandFixture(t)
	encoded, err := json.Marshal(append([]string{"journal", "--action", "initialize"}, arguments...))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestJournalCommandProcessHelper$")
	command.Env = append(os.Environ(), "VELA_TEST_JOURNAL_ARGUMENTS="+string(encoded))
	output, err := command.CombinedOutput()
	if err != nil || !json.Valid(output) || !bytes.Contains(output, []byte(`"schema_version":5`)) {
		t.Fatalf("offline main: %s %v", output, err)
	}
}

func TestJournalCommandProcessHelper(t *testing.T) {
	arguments := os.Getenv("VELA_TEST_JOURNAL_ARGUMENTS")
	if arguments == "" {
		return
	}
	var decoded []string
	if err := json.Unmarshal([]byte(arguments), &decoded); err != nil {
		t.Fatal(err)
	}
	os.Args = append([]string{os.Args[0]}, decoded...)
	main()
	os.Exit(0)
}

func TestJournalCommandLaunchBindingLeavesRuntimeEpochUnobserved(t *testing.T) {
	arguments, _ := journalCommandFixture(t)
	manifest, err := modelruntime.LoadLaunchManifest(arguments[1])
	if err != nil {
		t.Fatal(err)
	}
	config, err := offlineAdmissionConfig(manifest, stageworkeragent.AssignmentAdmissionConfig{})
	if err != nil || len(config.Bindings) != 2 {
		t.Fatalf("complete offline topology: %+v %v", config.Bindings, err)
	}
	for _, binding := range config.Bindings {
		if binding.Runtime.ModelRuntimeEpoch != 0 || binding.Runtime.ModelRuntimeIdentity != "" || binding.Runtime.ModelResidencyID != "" || binding.Runtime.StageProfileRevisionID != "" {
			t.Fatal("offline topology invented an observed local/remote Runtime route")
		}
	}
	manifest.Devices[0].Epoch++
	members, err := modelruntime.LoadLaunchManifest(arguments[1])
	if err != nil || config.Bindings[0].Runtime.Devices[0].Epoch != members.Devices[0].Epoch {
		t.Fatal("caller mutation changed prepared topology")
	}
}

func TestJournalCommandRequiresSharedAUXRootsBeforeWriting(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "different-roots", true: "shared-roots"}[shared], func(t *testing.T) {
			arguments, directory := journalCommandFixture(t)
			manifest, err := modelruntime.LoadLaunchManifest(arguments[1])
			if err != nil {
				t.Fatal(err)
			}
			manifest.WorkerRole, manifest.SharedSlotException = "aux", "H3_AUX_ENCODER_VAE"
			manifest.Devices, manifest.Members = manifest.Devices[:1], manifest.Members[:1]
			manifest.Runtimes[0].Component = "ENCODER"
			second := manifest.Runtimes[0]
			second.ModelResidencyID, second.StageProfileRevisionID = uuid.NewString(), uuid.NewString()
			second.Component = "VAE_DECODER"
			if !shared {
				second.InputRoot = filepath.Join(second.ScratchRoot, "other-inputs")
			}
			manifest.Runtimes = append(manifest.Runtimes, second)
			writeJournalJSON(t, arguments[1], manifest)
			var output bytes.Buffer
			err = runCommand(t.Context(), append([]string{"journal", "--action", "initialize"}, arguments...), &output, io.Discard)
			if shared {
				if err != nil || output.Len() == 0 {
					t.Fatalf("shared AUX roots rejected: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), "one shared local input/output root pair") || output.Len() != 0 {
					t.Fatalf("different AUX roots accepted: %v", err)
				}
				if entries, err := os.ReadDir(directory); err != nil || len(entries) != 0 {
					t.Fatalf("ambiguous roots initialized state: %v %v", entries, err)
				}
			}
		})
	}
}

type failedJournalWriter struct{}

func (failedJournalWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func journalCommandFixture(t *testing.T) ([]string, string) {
	t.Helper()
	root := t.TempDir()
	directory := filepath.Join(root, "admission")
	for _, path := range []string{directory, filepath.Join(root, "inputs"), filepath.Join(root, "outputs")} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	manifest := modelruntime.LaunchManifest{
		SchemaVersion: 2, WorkerProfileRevisionID: "72000000-0000-0000-0000-000000000001",
		WorkerRole: "llm", CapacitySlots: 1,
		WorkerInstanceID: "22000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 3,
		WorkerMemberID: "42000000-0000-0000-0000-000000000001", WorkerMemberEpoch: 4,
		DeviceSetDigest: strings.Repeat("a", 64), MembershipDigest: strings.Repeat("b", 64),
		Devices: []modelruntime.LaunchDeviceEpoch{
			{ID: "32000000-0000-0000-0000-000000000001", Epoch: 5},
			{ID: "32000000-0000-0000-0000-000000000002", Epoch: 5},
		},
		Members: []modelruntime.LaunchMemberEpoch{
			{ID: "42000000-0000-0000-0000-000000000001", Epoch: 4, IdentityDigest: strings.Repeat("d", 64), DeviceSubsetDigest: strings.Repeat("e", 64)},
			{ID: "42000000-0000-0000-0000-000000000002", Epoch: 4, IdentityDigest: strings.Repeat("1", 64), DeviceSubsetDigest: strings.Repeat("2", 64)},
		},
		LocalDevices: []modelruntime.DriverDevice{{DeviceID: "32000000-0000-0000-0000-000000000001", DeviceEpoch: 5,
			GPUUUID: "GPU-00000000-0000-0000-0000-000000000002", PCIBDF: "0000:42:00.0"}},
		Runtimes: []modelruntime.LaunchRuntime{{
			ModelResidencyID: "52000000-0000-0000-0000-000000000001", RuntimeIdentity: "offline-llm",
			StageProfileRevisionID: "62000000-0000-0000-0000-000000000001", Component: "LLM",
			ModelComponentRevision: "test-llm-v1", ModelRuntimeEpochFloor: 999, RuntimeImageDigest: strings.Repeat("c", 64),
			Command: []string{filepath.Join(root, "nonexistent-backend")}, ScratchRoot: root,
			InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
			InitializationTimeout: "5s", ShutdownTimeout: "5s",
		}},
	}
	manifestPath := filepath.Join(root, "launch.json")
	writeJournalJSON(t, manifestPath, manifest)
	keyring, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"authority-v1": make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer stageauthority.ClearKeyring(keyring)
	verifierPath := filepath.Join(root, "verifier.json")
	writeJournalJSON(t, verifierPath, map[string]string{"authority-v1": base64.StdEncoding.EncodeToString(keyring["authority-v1"])})
	return []string{"--launch-manifest-file", manifestPath, "--verifier-keyring-file", verifierPath, "--directory", directory, "--max-records", "4"}, directory
}

func writeJournalJSON(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}
