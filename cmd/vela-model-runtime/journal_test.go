package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
)

func TestJournalCommandRequiresExplicitActionAndNeverInfersBootstrap(t *testing.T) {
	arguments, directory := journalCommandFixture(t)
	for _, args := range [][]string{
		{"unknown"}, append([]string{"journal"}, arguments...),
		append([]string{"journal", "--action", "recover"}, arguments...),
		append([]string{"journal", "--action", "invalid"}, arguments...),
		append([]string{"journal", "--action", "upgrade-v2"}, arguments...),
		append([]string{"journal", "--action", "upgrade-v3"}, arguments...),
		append([]string{"journal", "--action", "upgrade-v4"}, arguments...),
		append([]string{"journal", "--action", "upgrade-v5"}, arguments...),
		append([]string{"journal", "--action", "upgrade-v6"}, arguments...),
		append([]string{"journal", "--action", "upgrade-v7"}, arguments...),
		append([]string{"journal", "--action", "initialize", "--unknown"}, arguments...),
		append(append([]string{"journal", "--action", "initialize"}, arguments...), "unexpected"),
	} {
		var output bytes.Buffer
		if err := runCommand(t.Context(), args, &output, io.Discard); err == nil || output.Len() != 0 {
			t.Fatalf("invalid/empty recovery acquired success: args=%v err=%v output=%s", args, err, output.String())
		}
		entries, err := os.ReadDir(directory)
		if err != nil || len(entries) != 0 {
			t.Fatalf("failed preparation initialized state: %v %v", entries, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runCommand(ctx, append([]string{"journal", "--action", "initialize"}, arguments...), io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bootstrap: %v", err)
	}
}

func TestJournalCommandInitializesOfflineAndPreservesIdentityOnRecovery(t *testing.T) {
	arguments, directory := journalCommandFixture(t)
	prepare := func(action string) (modelruntime.ExecutionJournalStatus, error) {
		t.Helper()
		var output bytes.Buffer
		err := runCommand(t.Context(), append([]string{"journal", "--action", action}, arguments...), &output, io.Discard)
		if err != nil {
			if output.Len() != 0 {
				t.Fatal("failed preparation wrote a success result")
			}
			return modelruntime.ExecutionJournalStatus{}, err
		}
		var result struct {
			Action  string                              `json:"action"`
			Journal modelruntime.ExecutionJournalStatus `json:"journal"`
		}
		if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Action != action {
			t.Fatalf("journal result: %s %v", output.String(), err)
		}
		return result.Journal, nil
	}
	first, err := prepare("initialize")
	if err != nil || first.SchemaVersion != 8 || first.JournalID == uuid.Nil || first.Highest != 0 || first.Floor != 0 {
		t.Fatalf("offline bootstrap: %+v %v", first, err)
	}
	if _, err := prepare("initialize"); err == nil {
		t.Fatal("repeated initialization reused an existing journal")
	}
	second, err := prepare("recover")
	if err != nil || first != second {
		t.Fatalf("recovery changed journal identity/restrictions: %+v %v", second, err)
	}
	statePath := filepath.Join(directory, "execution-admission.json")
	wire, err := os.ReadFile(statePath)
	if err != nil || !bytes.Contains(wire, []byte(`"schema_version":8`)) {
		t.Fatal("missing current journal for legacy fixture", err)
	}
	legacy := bytes.Replace(wire, []byte(`"schema_version":8`), []byte(`"schema_version":6`), 1)
	if err := os.WriteFile(statePath, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepare("recover"); err == nil {
		t.Fatal("ordinary command silently upgraded schema 6")
	}
	if upgraded, err := prepare("upgrade-v6"); err != nil || upgraded != first {
		t.Fatalf("explicit command lost schema-6 identity/lifecycle: %+v %v", upgraded, err)
	}
	legacy = bytes.Replace(wire, []byte(`"schema_version":8`), []byte(`"schema_version":7`), 1)
	if err := os.WriteFile(statePath, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepare("recover"); err == nil {
		t.Fatal("ordinary command silently upgraded schema 7")
	}
	if upgraded, err := prepare("upgrade-v7"); err != nil || upgraded != first {
		t.Fatalf("explicit command lost schema-7 identity/lifecycle: %+v %v", upgraded, err)
	}
	manifestPath := arguments[1]
	manifest, err := modelruntime.LoadLaunchManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.WorkerInstanceEpoch++
	writeCommandJSON(t, manifestPath, manifest)
	if _, err := prepare("recover"); err == nil {
		t.Fatal("recovery accepted a different Worker epoch")
	}
	manifest.WorkerInstanceEpoch--
	writeCommandJSON(t, manifestPath, manifest)
	if err := os.WriteFile(filepath.Join(directory, "execution-admission.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepare("recover"); err == nil {
		t.Fatal("recovery accepted a corrupt journal")
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

type failedJournalWriter struct{}

func (failedJournalWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func journalCommandFixture(t *testing.T) ([]string, string) {
	t.Helper()
	root := t.TempDir()
	directory := filepath.Join(root, "execution-journal")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	// This executable and its model roots deliberately do not exist. Offline
	// preparation must need neither model startup nor ordinary server settings.
	manifestPath := filepath.Join(root, "launch.json")
	writeCommandJSON(t, manifestPath, commandLaunchManifest(root, filepath.Join(root, "outputs"), filepath.Join(root, "missing-backend"), filepath.Join(root, "events")))
	keyring, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"authority-v1": make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer stageauthority.ClearKeyring(keyring)
	verifierPath := filepath.Join(root, "verifier.json")
	writeCommandJSON(t, verifierPath, map[string]string{"authority-v1": base64.StdEncoding.EncodeToString(keyring["authority-v1"])})
	return []string{"--launch-manifest-file", manifestPath, "--verifier-keyring-file", verifierPath, "--directory", directory}, directory
}
