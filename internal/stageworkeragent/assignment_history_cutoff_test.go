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
