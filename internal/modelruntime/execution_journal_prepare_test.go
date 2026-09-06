package modelruntime_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestExecutionJournalInspectionRevalidatesAndClosesAfterCallback(t *testing.T) {
	for _, failure := range []string{"callback", "replacement"} {
		t.Run(failure, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			state := *config.ExecutionFloor.State
			first, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state)
			if err != nil {
				t.Fatal(err)
			}
			state.Initialize = false
			injected := errors.New("inspection interrupted")
			err = modelruntime.WithPreparedExecutionJournal(t.Context(), config.Manifest, config.Validator, state, func(status modelruntime.ExecutionJournalStatus) error {
				if status != first {
					t.Fatal("inspection changed status")
				}
				if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err == nil {
					t.Fatal("callback released journal ownership")
				}
				if failure == "replacement" {
					path := filepath.Join(state.Directory, durableStateFileName)
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(path, path+".old"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, data, 0o600); err != nil {
						t.Fatal(err)
					}
					return nil
				}
				return injected
			})
			if err == nil || failure == "callback" && !errors.Is(err, injected) {
				t.Fatalf("inspection lost callback/binding failure: %v", err)
			}
			if failure == "callback" {
				if recovered, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err != nil || recovered != first {
					t.Fatalf("callback failure leaked lock or changed journal: %+v %v", recovered, err)
				}
			}
		})
	}
}

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
	for _, version := range []int{2, 3, 4} {
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
		state.UpgradeV2, state.UpgradeV3 = version != 2, version == 2
		if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err == nil {
			t.Fatal("upgrade accepted a different source schema")
		}
		unchanged, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(old, unchanged) {
			t.Fatalf("rejected recovery changed legacy state: %v", err)
		}
		state.UpgradeV2, state.UpgradeV3 = version == 2, version == 3
		state.UpgradeV4 = version == 4
		upgraded, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state)
		if err != nil || upgraded != initialized {
			t.Fatalf("explicit upgrade lost journal identity/restrictions: %+v %v", upgraded, err)
		}
		state.UpgradeV2, state.UpgradeV3 = false, false
		state.UpgradeV4 = false
		if recovered, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err != nil || recovered != upgraded {
			t.Fatalf("ordinary recovery after upgrade: %+v %v", recovered, err)
		}
	}
}
