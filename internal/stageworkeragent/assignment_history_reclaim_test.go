package stageworkeragent_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
	tamperedIdentity := cutoff
	tamperedIdentity.WorkerMemberID = uuid.New()
	if err := gate.ReclaimAssignmentHistory(t.Context(), tamperedIdentity); err == nil {
		t.Fatal("reclamation accepted a cutoff from another journal identity")
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
	firstDigest, err := cutoff.Digest()
	if err != nil {
		t.Fatal(err)
	}
	secondCutoff := cutoff
	secondCutoff.FromSequence, secondCutoff.ThroughSequence = 2, 2
	secondCutoff.PreviousCutoffDigest = firstDigest
	secondCutoff.CumulativeDigest = sha256.Sum256([]byte("history-2"))
	secondCutoff.TerminalProofDigest = sha256.Sum256([]byte("terminal-2"))
	secondCutoff.InputProofDigest = sha256.Sum256([]byte("input-2"))
	secondCutoff.MaterializationProofDigest = sha256.Sum256([]byte("materialization-2"))
	if err := gate.RecordAssignmentHistoryCutoff(t.Context(), secondCutoff); err != nil {
		t.Fatal(err)
	}
	failed = true
	restore = stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
		if failed {
			failed = false
			return os.ErrInvalid
		}
		return sync()
	})
	if err := gate.ReclaimAssignmentHistory(t.Context(), secondCutoff); err == nil {
		t.Fatal("second reclamation unexpectedly succeeded across directory sync failure")
	}
	restore()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	snapshot, err = gate.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Watermark != 2 || len(snapshot.Pending) != 0 {
		t.Fatalf("failed second reclamation corrupted retained history: %+v", snapshot)
	}
	if snapshot.Latest != nil {
		if err := gate.ReclaimAssignmentHistory(t.Context(), secondCutoff); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := gate.Close(); err != nil {
			t.Fatal(err)
		}
		prepared, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), f.config)
		if err != nil || prepared.HistoryBase != 2 {
			t.Fatalf("failed second reclamation recovered without a valid committed base: %+v %v", prepared, err)
		}
		gate = f.open(t)
	}
	snapshot, err = gate.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Watermark != 2 || len(snapshot.Pending) != 0 || snapshot.Latest != nil {
		t.Fatalf("unexpected post-second-reclaim snapshot: %+v", snapshot)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	prepared, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), f.config)
	if err != nil || prepared.HistoryBase != 2 || prepared.Watermark != 2 || prepared.HistoryCutoffs != 2 || prepared.RetainedExecutions != 0 {
		t.Fatalf("prepared status lost reclaimed prefix: %+v %v", prepared, err)
	}
	statePath := filepath.Join(f.config.Directory, admissionTestState)
	original, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var reconciled struct {
		HistoryBase    int64                                      `json:"history_base"`
		HistoryCutoffs []stageworkeragent.AssignmentHistoryCutoff `json:"history_cutoffs"`
	}
	if err := json.Unmarshal(original, &reconciled); err != nil {
		t.Fatal(err)
	}
	if reconciled.HistoryBase != 2 || len(reconciled.HistoryCutoffs) != 2 {
		t.Fatalf("persisted reclamation proof summary mismatch: %+v", reconciled)
	}
	firstDigest, err = cutoff.Digest()
	if err != nil || reconciled.HistoryCutoffs[0] != cutoff || reconciled.HistoryCutoffs[1] != secondCutoff ||
		reconciled.HistoryCutoffs[1].PreviousCutoffDigest != firstDigest {
		t.Fatalf("persisted cutoff proof chain mismatch: %+v", reconciled.HistoryCutoffs)
	}
	var decoded map[string]any
	if err := json.Unmarshal(original, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded["history_base"] = float64(1)
	tamperedBase, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, tamperedBase, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := stageworkeragent.NewFileAssignmentAdmission(f.config); err == nil {
		_ = reopened.Close()
		t.Fatal("recovery accepted a history base inconsistent with persisted cutoffs")
	}
	if err := os.WriteFile(statePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(original, &decoded); err != nil {
		t.Fatal(err)
	}
	cutoffs, ok := decoded["history_cutoffs"].([]any)
	if !ok || len(cutoffs) != 2 {
		t.Fatalf("decode persisted cutoff: %#v", decoded["history_cutoffs"])
	}
	cutoffDocument, ok := cutoffs[0].(map[string]any)
	if !ok {
		t.Fatalf("decode persisted cutoff entry: %#v", cutoffs[0])
	}
	cutoffDocument["worker_member_id"] = uuid.NewString()
	tamperedIdentityDocument, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, tamperedIdentityDocument, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := stageworkeragent.NewFileAssignmentAdmission(f.config); err == nil {
		_ = reopened.Close()
		t.Fatal("recovery accepted a persisted cutoff from another journal identity")
	}
	if err := os.WriteFile(statePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(original, &decoded); err != nil {
		t.Fatal(err)
	}
	cutoffs, ok = decoded["history_cutoffs"].([]any)
	if !ok || len(cutoffs) != 2 {
		t.Fatalf("decode restored persisted cutoffs: %#v", decoded["history_cutoffs"])
	}
	cutoffDocument, ok = cutoffs[0].(map[string]any)
	if !ok {
		t.Fatalf("decode restored cutoff entry: %#v", cutoffs[0])
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

func TestAssignmentHistoryReclaimBoundedArrivalCampaign(t *testing.T) {
	f := newAdmissionFixture(t)
	gate := f.open(t)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutinesBefore := runtime.NumGoroutine()
	statePath := filepath.Join(f.config.Directory, admissionTestState)
	document, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	initialStateBytes := len(document)
	var persisted struct {
		Scope []byte `json:"scope"`
	}
	if err := json.Unmarshal(document, &persisted); err != nil {
		t.Fatal(err)
	}
	var previous [sha256.Size]byte
	var lastCutoff stageworkeragent.AssignmentHistoryCutoff
	for sequence := int64(1); sequence <= 40; sequence++ {
		assignment := f.assignment
		acquireID := f.acquireID
		if sequence > 1 {
			assignment = f.next(t, sequence)
			acquireID = uuid.New()
		}
		handle := beginAdmission(t, gate, assignment, acquireID)
		completeAdmissionInputs(t, handle)
		if err := gate.CloseExecution(t.Context(), assignment.Authority); err != nil {
			t.Fatal(err)
		}
		cutoff := stageworkeragent.AssignmentHistoryCutoff{
			ScopeDigest: persistedScope(persisted.Scope), WorkerInstanceID: f.config.WorkerInstanceID,
			WorkerInstanceEpoch: f.config.WorkerInstanceEpoch, WorkerMemberID: f.config.WorkerMemberID,
			FromSequence: sequence, ThroughSequence: sequence, PreviousCutoffDigest: previous,
			CumulativeDigest:           sha256.Sum256([]byte(fmt.Sprintf("history-%d", sequence))),
			TerminalProofDigest:        sha256.Sum256([]byte(fmt.Sprintf("terminal-%d", sequence))),
			InputProofDigest:           sha256.Sum256([]byte(fmt.Sprintf("input-%d", sequence))),
			MaterializationProofDigest: sha256.Sum256([]byte(fmt.Sprintf("materialization-%d", sequence))),
		}
		if err := gate.RecordAssignmentHistoryCutoff(t.Context(), cutoff); err != nil {
			t.Fatal(err)
		}
		if err := gate.ReclaimAssignmentHistory(t.Context(), cutoff); err != nil {
			t.Fatal(err)
		}
		lastCutoff = cutoff
		previous, err = cutoff.Digest()
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := gate.Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Watermark != sequence || len(snapshot.Pending) != 0 || snapshot.Latest != nil {
			t.Fatalf("sequence %d retained unexpected history: %+v", sequence, snapshot)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	goroutinesAfter := runtime.NumGoroutine()
	heapDelta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("bounded arrival resources: goroutines %d -> %d, heap_alloc %d -> %d (delta=%d)", goroutinesBefore, goroutinesAfter, before.HeapAlloc, after.HeapAlloc, heapDelta)
	if goroutinesAfter > goroutinesBefore+8 {
		t.Fatalf("goroutine count grew across bounded arrival campaign: %d -> %d", goroutinesBefore, goroutinesAfter)
	}
	if heapDelta > 8<<20 {
		t.Fatalf("heap allocation grew across bounded arrival campaign: %d bytes", heapDelta)
	}
	finalState, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bounded arrival journal state: %d -> %d bytes (delta=%d), retained cutoffs=%d", initialStateBytes, finalState.Size(), finalState.Size()-int64(initialStateBytes), 40)
	lastCutoffDigest, err := lastCutoff.Digest()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := stageworkeragent.AssignmentHistoryCheckpoint{
		ScopeDigest: persistedScope(persisted.Scope), WorkerInstanceID: f.config.WorkerInstanceID,
		WorkerInstanceEpoch: f.config.WorkerInstanceEpoch, WorkerMemberID: f.config.WorkerMemberID,
		FromSequence: 1, ThroughSequence: 40, CumulativeDigest: lastCutoff.CumulativeDigest,
		TerminalProofDigest: lastCutoff.TerminalProofDigest, InputProofDigest: lastCutoff.InputProofDigest,
		MaterializationProofDigest: lastCutoff.MaterializationProofDigest, LastCutoffDigest: lastCutoffDigest,
		CompactedCutoffCount: 40, Revision: 1,
	}
	if err := gate.CompactAssignmentHistory(t.Context(), checkpoint); err != nil {
		t.Fatal(err)
	}
	compactedState, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if compactedState.Size() >= finalState.Size() {
		t.Fatalf("checkpoint compaction did not reduce state: before=%d after=%d", finalState.Size(), compactedState.Size())
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	prepared, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), f.config)
	if err != nil || prepared.HistoryBase != 40 || prepared.HistoryCutoffs != 0 || prepared.RetainedExecutions != 0 {
		t.Fatalf("checkpoint compaction recovery mismatch: %+v %v", prepared, err)
	}
}
