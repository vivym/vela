package stageworkeragent_test

import (
	"context"
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
	for want := int64(1); want <= 2; want++ {
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
	if err != nil || sequence != 3 {
		t.Fatalf("reopened capacity sequence = %d, error=%v", sequence, err)
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
