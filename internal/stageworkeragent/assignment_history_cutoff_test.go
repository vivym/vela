package stageworkeragent

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
)

func cutoffFixture() AssignmentHistoryCutoff {
	return AssignmentHistoryCutoff{ScopeDigest: sha256.Sum256([]byte("scope")), WorkerInstanceID: uuid.New(), WorkerInstanceEpoch: 7, WorkerMemberID: uuid.New(), FromSequence: 1, ThroughSequence: 2, CumulativeDigest: sha256.Sum256([]byte("history")), TerminalProofDigest: sha256.Sum256([]byte("terminal")), InputProofDigest: sha256.Sum256([]byte("input")), MaterializationProofDigest: sha256.Sum256([]byte("materialization"))}
}

func TestAssignmentHistoryCutoffCanonicalAndChain(t *testing.T) {
	first := cutoffFixture()
	wire, err := first.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, mustMarshal(first)) {
		t.Fatal("canonical encoding changed")
	}
	next := first
	next.FromSequence, next.ThroughSequence = 3, 4
	digest, err := first.Digest()
	if err != nil {
		t.Fatal(err)
	}
	next.PreviousCutoffDigest = digest
	if err := next.ValidateSuccessor(&first); err != nil {
		t.Fatal(err)
	}
}

func TestAssignmentHistoryCutoffRejectsGapsAndIncompleteProof(t *testing.T) {
	first := cutoffFixture()
	first.InputProofDigest = [sha256.Size]byte{}
	if err := first.Validate(); err == nil {
		t.Fatal("accepted incomplete proof")
	}
	first = cutoffFixture()
	next := first
	next.FromSequence, next.ThroughSequence = 4, 5
	digest, _ := first.Digest()
	next.PreviousCutoffDigest = digest
	if err := next.ValidateSuccessor(&first); err == nil {
		t.Fatal("accepted sequence gap")
	}
}

func TestAssignmentHistoryCutoffRejectsNonzeroInitialSequence(t *testing.T) {
	cutoff := cutoffFixture()
	cutoff.FromSequence, cutoff.ThroughSequence = 2, 2
	if err := cutoff.ValidateSuccessor(nil); err == nil {
		t.Fatal("accepted an initial cutoff that skipped sequence one")
	}
}

func checkpointFixture() AssignmentHistoryCheckpoint {
	return AssignmentHistoryCheckpoint{
		ScopeDigest: sha256.Sum256([]byte("scope")), WorkerInstanceID: uuid.New(), WorkerInstanceEpoch: 7,
		WorkerMemberID: uuid.New(), FromSequence: 1, ThroughSequence: 128,
		CumulativeDigest: sha256.Sum256([]byte("history")), TerminalProofDigest: sha256.Sum256([]byte("terminal")),
		InputProofDigest: sha256.Sum256([]byte("input")), MaterializationProofDigest: sha256.Sum256([]byte("materialization")),
		LastCutoffDigest: sha256.Sum256([]byte("last-cutoff")), CompactedCutoffCount: 64, Revision: 1,
	}
}

func TestAssignmentHistoryCheckpointCanonicalAndRejectsIncompleteProof(t *testing.T) {
	checkpoint := checkpointFixture()
	wire, err := checkpoint.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, mustMarshal(checkpoint)) {
		t.Fatal("checkpoint canonical encoding changed")
	}
	if digest, err := checkpoint.Digest(); err != nil || digest == ([sha256.Size]byte{}) {
		t.Fatalf("checkpoint digest invalid: %x %v", digest, err)
	}
	for name, mutate := range map[string]func(*AssignmentHistoryCheckpoint){
		"identity": func(value *AssignmentHistoryCheckpoint) { value.WorkerMemberID = uuid.Nil },
		"range":    func(value *AssignmentHistoryCheckpoint) { value.ThroughSequence = 0 },
		"proof":    func(value *AssignmentHistoryCheckpoint) { value.CumulativeDigest = [sha256.Size]byte{} },
		"count":    func(value *AssignmentHistoryCheckpoint) { value.CompactedCutoffCount = 0 },
		"revision": func(value *AssignmentHistoryCheckpoint) { value.Revision = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			value := checkpoint
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("accepted invalid checkpoint")
			}
		})
	}
}
