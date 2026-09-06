package stageworkeragent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestAssignmentJournalPreparationRequiresExclusiveOwnership(t *testing.T) {
	fixture := newAdmissionFixture(t)
	first, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
	if err != nil || first.SchemaVersion != 5 || first.JournalID == uuid.Nil || first.Watermark != 0 || first.RetainedExecutions != 0 {
		t.Fatalf("prepare: %+v %v", first, err)
	}
	fixture.config.Initialize = false
	gate := fixture.open(t)
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config); err == nil {
		t.Fatal("offline preparation acquired a live admission journal")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
	if err != nil || second != first {
		t.Fatalf("recovery changed journal: %+v %v", second, err)
	}
}

func TestAssignmentJournalPreparationPreservesPendingHistoryWithoutRuntimeRoutes(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			fixture := newAssignmentFloorFixture(t)
			gate := fixture.open(t)
			handle := beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
			if complete {
				if err := handle.CompleteInputs(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			handle.Release()
			if _, err := gate.InstallExecutionFloor(t.Context(), fixture.disposition); err != nil {
				t.Fatal(err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			config := fixture.admissionFixture.config
			path := filepath.Join(config.Directory, admissionTestState)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for i := range config.Bindings {
				binding := &config.Bindings[i].Runtime
				binding.ModelRuntimeEpoch = 0
				binding.ModelRuntimeIdentity, binding.ModelResidencyID, binding.StageProfileRevisionID = "", "", ""
			}
			result, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), config)
			wantUnproven := 1
			if complete {
				wantUnproven = 0
			}
			if err != nil || result.Watermark != 1 || result.Floor != 7 || result.RetainedExecutions != 1 || result.UnprovenInputs != wantUnproven || result.RetirementsRetired != 0 {
				t.Fatalf("offline history inspection: %+v %v", result, err)
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
				t.Fatalf("preparation changed retained history: %v", err)
			}
		})
	}
}

func TestAssignmentJournalPreparationUpgradesOnlyWithOriginalTopologyWitness(t *testing.T) {
	for _, schema := range []int{2, 3, 4} {
		for _, witness := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/witness=%t", schema, witness), func(t *testing.T) {
				fixture := newAssignmentFloorFixture(t)
				gate := fixture.open(t)
				if witness {
					if _, err := gate.InstallExecutionFloor(t.Context(), fixture.disposition); err != nil {
						t.Fatal(err)
					}
				}
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
				config := fixture.admissionFixture.config
				path := filepath.Join(config.Directory, admissionTestState)
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				legacy := legacyAdmissionDocument(t, original, schema)
				if err := os.WriteFile(path, legacy, 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), config); err == nil {
					t.Fatal("ordinary preparation migrated legacy state")
				}
				config.UpgradeV2, config.UpgradeV3, config.UpgradeV4 = schema == 2, schema == 3, schema == 4
				result, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), config)
				want := legacy
				if witness {
					if err != nil || result.SchemaVersion != 5 || result.Floor != 7 {
						t.Fatalf("explicit preparation upgrade: %+v %v", result, err)
					}
					want = original
				} else if err == nil || result != (stageworkeragent.AssignmentJournalStatus{}) {
					t.Fatalf("upgrade inferred missing topology: %+v %v", result, err)
				}
				if after, err := os.ReadFile(path); err != nil || !bytes.Equal(want, after) {
					t.Fatalf("preparation changed evidence: %v", err)
				}
			})
		}
	}
}

func TestAssignmentJournalPreparationRejectsCanceledBootstrapBeforeWrites(t *testing.T) {
	fixture := newAdmissionFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := stageworkeragent.PrepareAssignmentJournal(ctx, fixture.config); !errors.Is(err, context.Canceled) || result != (stageworkeragent.AssignmentJournalStatus{}) {
		t.Fatalf("canceled bootstrap: %+v %v", result, err)
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(nil, fixture.config); err == nil { //nolint:staticcheck // Exercise invalid context rejection before initialization.
		t.Fatal("nil preparation context accepted")
	}
	for _, root := range []string{fixture.config.Directory, fixture.config.InputRoot, fixture.config.OutputRoot} {
		if entries, err := os.ReadDir(root); err != nil || len(entries) != 0 {
			t.Fatalf("canceled preparation wrote state: %v %v", entries, err)
		}
	}
}

func TestAssignmentJournalPreparationDoesNotAdvanceRetirementOrDeleteScratch(t *testing.T) {
	for _, phase := range []string{"INTENT", "READY", "RETIRED"} {
		t.Run(phase, func(t *testing.T) {
			fixture := newAssignmentFloorFixture(t)
			gate := fixture.open(t)
			group := startFloorCollectorRuntimes(t, fixture, t.TempDir(), true, false)
			paths := retirementScratch(t, fixture)
			if phase == "INTENT" {
				beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
				err := sync()
				if phase == "READY" {
					// The hook follows persistence; inspect the published phase only
					// to inject process interruption before its first deletion.
					document, readErr := os.ReadFile(filepath.Join(fixture.admissionFixture.config.Directory, admissionTestState))
					if readErr != nil {
						return readErr
					}
					var published struct {
						Retirements []struct {
							Phase string `json:"phase"`
						} `json:"retirements"`
					}
					if err := json.Unmarshal(document, &published); err != nil {
						return err
					}
					for _, retirement := range published.Retirements {
						if retirement.Phase == "READY" {
							cancel()
						}
					}
				}
				return err
			})
			result, err := terminalRetirer(t, gate, fixture).Retire(ctx, fixture.disposition, nil, drainCollectorTargets(fixture))
			restore()
			if phase == "READY" && !errors.Is(err, context.Canceled) ||
				phase != "READY" && string(result.Phase) != phase ||
				phase == "RETIRED" && err != nil || phase == "INTENT" && err == nil {
				t.Fatalf("retirement fixture: %+v %v", result, err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			group.close()
			config := fixture.admissionFixture.config
			path := filepath.Join(config.Directory, admissionTestState)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			status, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{"INTENT": status.RetirementIntents, "READY": status.RetirementsReady, "RETIRED": status.RetirementsRetired}
			if counts[phase] != 1 || status.RetirementIntents+status.RetirementsReady+status.RetirementsRetired != 1 {
				t.Fatalf("offline status changed retirement phase: %+v", status)
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
				t.Fatalf("offline preparation advanced retirement: %v", err)
			}
			assertRetirementScratch(t, paths, phase != "RETIRED")
		})
	}
}
