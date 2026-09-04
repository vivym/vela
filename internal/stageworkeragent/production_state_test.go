package stageworkeragent_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestFileProductionStatePersistsMonotonicControlAndCapacitySequences(t *testing.T) {
	directory := t.TempDir()
	configuration := stageworkeragent.FileProductionStateConfig{
		Directory:           directory,
		WorkerInstanceID:    uuid.MustParse("48000000-0000-0000-0000-000000000001"),
		WorkerInstanceEpoch: 5,
		WorkerMemberID:      uuid.MustParse("48000000-0000-0000-0000-000000000004"),
	}
	state, err := stageworkeragent.NewFileProductionState(configuration)
	if err != nil {
		t.Fatalf("NewFileProductionState: %v", err)
	}
	controlEpoch, err := state.NextControlSessionEpoch(context.Background())
	if err != nil || controlEpoch != 2 {
		t.Fatalf("first control epoch = %d, error=%v", controlEpoch, err)
	}
	capacityBase := int64(5) << 32
	for want := capacityBase + 1; want <= capacityBase+2; want++ {
		sequence, sequenceErr := state.NextCapacityObservationSequence(context.Background())
		if sequenceErr != nil || sequence != want {
			t.Fatalf("capacity sequence = %d, want %d, error=%v", sequence, want, sequenceErr)
		}
	}
	if err := state.Close(); err != nil {
		t.Fatalf("close production state: %v", err)
	}

	reopened, err := stageworkeragent.NewFileProductionState(configuration)
	if err != nil {
		t.Fatalf("reopen FileProductionState: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	controlEpoch, err = reopened.NextControlSessionEpoch(context.Background())
	if err != nil || controlEpoch != 3 {
		t.Fatalf("reopened control epoch = %d, error=%v", controlEpoch, err)
	}
	sequence, err := reopened.NextCapacityObservationSequence(context.Background())
	if err != nil || sequence != capacityBase+3 {
		t.Fatalf("reopened capacity sequence = %d, error=%v", sequence, err)
	}
}

func TestFileProductionStateUpgradesLegacyCapacitySequence(t *testing.T) {
	directory := t.TempDir()
	statePath := filepath.Join(directory, "stage-worker-production-state.json")
	if err := os.WriteFile(statePath, []byte(`{
		"schema_version":1,
		"worker_instance_id":"48000000-0000-0000-0000-000000000001",
		"worker_instance_epoch":5,
		"worker_member_id":"48000000-0000-0000-0000-000000000004",
		"control_session_epoch":17,
		"capacity_observation_sequence":123
	}`), 0o600); err != nil {
		t.Fatalf("write legacy production state: %v", err)
	}
	configuration := stageworkeragent.FileProductionStateConfig{
		Directory:           directory,
		WorkerInstanceID:    uuid.MustParse("48000000-0000-0000-0000-000000000001"),
		WorkerInstanceEpoch: 5,
		WorkerMemberID:      uuid.MustParse("48000000-0000-0000-0000-000000000004"),
	}
	state, err := stageworkeragent.NewFileProductionState(configuration)
	if err != nil {
		t.Fatalf("upgrade legacy FileProductionState: %v", err)
	}
	sequence, err := state.NextCapacityObservationSequence(context.Background())
	want := (int64(5) << 32) + 1
	if err != nil || sequence != want {
		t.Fatalf("upgraded capacity sequence = %d, want %d, error=%v", sequence, want, err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("close upgraded production state: %v", err)
	}

	reopened, err := stageworkeragent.NewFileProductionState(configuration)
	if err != nil {
		t.Fatalf("reopen upgraded FileProductionState: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	sequence, err = reopened.NextCapacityObservationSequence(context.Background())
	if err != nil || sequence != want+1 {
		t.Fatalf("persisted upgraded capacity sequence = %d, want %d, error=%v", sequence, want+1, err)
	}
}

func TestFileProductionStateReconcilesDatabaseHighWater(t *testing.T) {
	directory := t.TempDir()
	configuration := stageworkeragent.FileProductionStateConfig{
		Directory:           directory,
		WorkerInstanceID:    uuid.MustParse("48000000-0000-0000-0000-000000000001"),
		WorkerInstanceEpoch: 5,
		WorkerMemberID:      uuid.MustParse("48000000-0000-0000-0000-000000000004"),
	}
	state, err := stageworkeragent.NewFileProductionState(configuration)
	if err != nil {
		t.Fatalf("NewFileProductionState: %v", err)
	}
	if _, err := state.NextControlSessionEpoch(context.Background()); err != nil {
		t.Fatalf("allocate local control epoch: %v", err)
	}
	if err := state.ObserveControlSessionEpoch(context.Background(), 900); err != nil {
		t.Fatalf("observe durable control epoch: %v", err)
	}
	capacityHighWater := (int64(5) << 32) + 900
	if err := state.ObserveCapacityObservationSequence(
		context.Background(),
		capacityHighWater,
	); err != nil {
		t.Fatalf("observe durable capacity sequence: %v", err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("close synchronized production state: %v", err)
	}

	reopened, err := stageworkeragent.NewFileProductionState(configuration)
	if err != nil {
		t.Fatalf("reopen synchronized FileProductionState: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	controlEpoch, err := reopened.NextControlSessionEpoch(context.Background())
	if err != nil || controlEpoch != 901 {
		t.Fatalf("reconciled control epoch = %d, want 901, error=%v", controlEpoch, err)
	}
	sequence, err := reopened.NextCapacityObservationSequence(context.Background())
	if err != nil || sequence != capacityHighWater+1 {
		t.Fatalf("reconciled capacity sequence = %d, want %d, error=%v", sequence, capacityHighWater+1, err)
	}
}

func TestFileProductionStateExcludesAnotherProcessAndBindsWorkerIdentity(t *testing.T) {
	directory := t.TempDir()
	configuration := stageworkeragent.FileProductionStateConfig{
		Directory:           directory,
		WorkerInstanceID:    uuid.MustParse("48000000-0000-0000-0000-000000000001"),
		WorkerInstanceEpoch: 5,
		WorkerMemberID:      uuid.MustParse("48000000-0000-0000-0000-000000000004"),
	}
	state, err := stageworkeragent.NewFileProductionState(configuration)
	if err != nil {
		t.Fatalf("NewFileProductionState: %v", err)
	}
	if _, err := stageworkeragent.NewFileProductionState(configuration); err == nil ||
		!strings.Contains(err.Error(), "lock") {
		t.Fatalf("concurrent production state error = %v", err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("close production state: %v", err)
	}

	configuration.WorkerInstanceEpoch++
	if _, err := stageworkeragent.NewFileProductionState(configuration); err == nil ||
		!strings.Contains(err.Error(), "another WorkerInstance") {
		t.Fatalf("mismatched production state error = %v", err)
	}
}
