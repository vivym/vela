package stageworkeragent_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestAssignmentHistoryReclaimPersistsBaseAndRecovers(t *testing.T) {
	f := newAdmissionFixture(t)
	gate := f.open(t)
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	completeAdmissionInputs(t, handle)
	if err := gate.CloseExecution(t.Context(), f.assignment.Authority); err != nil {
		t.Fatal(err)
	}
	second := f.next(t, 2)
	handle = beginAdmission(t, gate, second, uuid.New())
	completeAdmissionInputs(t, handle)
	if err := gate.CloseExecution(t.Context(), second.Authority); err != nil {
		t.Fatal(err)
	}

	var persisted struct {
		Scope []byte `json:"scope"`
	}
	document, err := os.ReadFile(filepath.Join(f.config.Directory, admissionTestState))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(document, &persisted); err != nil {
		t.Fatal(err)
	}
	cutoff := stageworkeragent.AssignmentHistoryCutoff{
		ScopeDigest: persistedScope(persisted.Scope), WorkerInstanceID: f.config.WorkerInstanceID,
		WorkerInstanceEpoch: f.config.WorkerInstanceEpoch, WorkerMemberID: f.config.WorkerMemberID,
		FromSequence: 1, ThroughSequence: 1,
		CumulativeDigest: sha256.Sum256([]byte("history-1")), TerminalProofDigest: sha256.Sum256([]byte("terminal-1")),
		InputProofDigest: sha256.Sum256([]byte("input-1")), MaterializationProofDigest: sha256.Sum256([]byte("materialization-1")),
	}
	if err := gate.RecordAssignmentHistoryCutoff(t.Context(), cutoff); err != nil {
		t.Fatal(err)
	}
	tampered := cutoff
	tampered.CumulativeDigest = sha256.Sum256([]byte("tampered"))
	if err := gate.ReclaimAssignmentHistory(t.Context(), tampered); err == nil {
		t.Fatal("reclamation accepted a cutoff different from the persisted proof")
	}
	failed := true
	restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
		if failed {
			failed = false
			return os.ErrInvalid
		}
		return sync()
	})
	if err := gate.ReclaimAssignmentHistory(t.Context(), cutoff); err == nil {
		t.Fatal("reclamation unexpectedly succeeded across directory sync failure")
	}
	restore()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	snapshot, err := gate.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Watermark != 2 || len(snapshot.Pending) != 0 || snapshot.Latest == nil {
		t.Fatalf("unexpected post-reclaim snapshot: %+v", snapshot)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(f.config.Directory, admissionTestState)
	original, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(original, &decoded); err != nil {
		t.Fatal(err)
	}
	cutoffs, ok := decoded["history_cutoffs"].([]any)
	if !ok || len(cutoffs) != 1 {
		t.Fatalf("decode persisted cutoff: %#v", decoded["history_cutoffs"])
	}
	cutoffDocument, ok := cutoffs[0].(map[string]any)
	if !ok {
		t.Fatalf("decode persisted cutoff entry: %#v", cutoffs[0])
	}
	tamperedDigest := sha256.Sum256([]byte("disk-tampered"))
	digestValues := make([]any, len(tamperedDigest))
	for index, value := range tamperedDigest {
		digestValues[index] = float64(value)
	}
	cutoffDocument["cumulative_digest"] = digestValues
	tamperedDocument, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(original, tamperedDocument) {
		t.Fatal("tamper did not change journal")
	}
	if err := os.WriteFile(statePath, tamperedDocument, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := stageworkeragent.NewFileAssignmentAdmission(f.config); err == nil {
		_ = reopened.Close()
		t.Fatal("recovery accepted a tampered persisted cutoff")
	}
	if err := os.WriteFile(statePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	third := f.next(t, 3)
	thirdHandle, err := gate.Begin(t.Context(), third, uuid.New())
	if err != nil {
		t.Fatalf("recovered journal rejected next execution: %v", err)
	}
	thirdHandle.Release()
}

func persistedScope(scope []byte) [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], scope)
	return result
}
