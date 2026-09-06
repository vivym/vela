package modelruntime_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestExecutionJournalPreparationRequiresExclusiveOwnership(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	state := *config.ExecutionFloor.State
	first, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state)
	if err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	state.Initialize = false
	if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err == nil {
		t.Fatal("offline preparation acquired a running server's journal")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state)
	if err != nil || recovered != first {
		t.Fatalf("offline recovery changed journal after runtime shutdown: %+v %v", recovered, err)
	}
}

func TestExecutionJournalPreparationUpgradesOnlyExplicitValidatedSchema(t *testing.T) {
	for _, version := range []int{2, 3} {
		config := journalRuntimeServerConfig(t)
		state := *config.ExecutionFloor.State
		initialized, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state)
		if err != nil {
			t.Fatal(err)
		}
		document := readDurableExecutionState(t, state.Directory)
		document.SchemaVersion = version
		path := filepath.Join(state.Directory, durableStateFileName)
		old := encodeDurableExecutionState(t, document)
		if err := os.WriteFile(path, old, 0o600); err != nil {
			t.Fatal(err)
		}
		state.Initialize = false
		if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err == nil {
			t.Fatal("ordinary recovery implicitly upgraded legacy schema")
		}
		state.UpgradeV2, state.UpgradeV3 = version == 3, version == 2
		if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err == nil {
			t.Fatal("upgrade accepted a different source schema")
		}
		unchanged, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(old, unchanged) {
			t.Fatalf("rejected recovery changed legacy state: %v", err)
		}
		state.UpgradeV2, state.UpgradeV3 = version == 2, version == 3
		upgraded, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state)
		if err != nil || upgraded != initialized {
			t.Fatalf("explicit upgrade lost journal identity/restrictions: %+v %v", upgraded, err)
		}
		state.UpgradeV2, state.UpgradeV3 = false, false
		if recovered, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err != nil || recovered != upgraded {
			t.Fatalf("ordinary recovery after upgrade: %+v %v", recovered, err)
		}
	}
}
