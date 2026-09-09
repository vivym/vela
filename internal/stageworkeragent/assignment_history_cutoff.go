package stageworkeragent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// AssignmentHistoryCutoff is a durable, append-only proof that a contiguous
// range of assignment history may be reclaimed. It contains no deletion
// authority by itself; callers must persist and verify it atomically with the
// corresponding reclamation.
type AssignmentHistoryCutoff struct {
	ScopeDigest                [sha256.Size]byte `json:"scope_digest"`
	WorkerInstanceID           uuid.UUID         `json:"worker_instance_id"`
	WorkerInstanceEpoch        int64             `json:"worker_instance_epoch"`
	WorkerMemberID             uuid.UUID         `json:"worker_member_id"`
	FromSequence               int64             `json:"from_sequence"`
	ThroughSequence            int64             `json:"through_sequence"`
	CumulativeDigest           [sha256.Size]byte `json:"cumulative_digest"`
	PreviousCutoffDigest       [sha256.Size]byte `json:"previous_cutoff_digest"`
	TerminalProofDigest        [sha256.Size]byte `json:"terminal_proof_digest"`
	InputProofDigest           [sha256.Size]byte `json:"input_proof_digest"`
	MaterializationProofDigest [sha256.Size]byte `json:"materialization_proof_digest"`
}

// AssignmentHistoryCheckpoint is a compact, immutable anchor for a contiguous
// prefix of cutoff proofs. It is intentionally independent of journal state;
// persistence/recovery must bind it to the journal before using it to remove
// older cutoff records.
type AssignmentHistoryCheckpoint struct {
	ScopeDigest                [sha256.Size]byte `json:"scope_digest"`
	WorkerInstanceID           uuid.UUID         `json:"worker_instance_id"`
	WorkerInstanceEpoch        int64             `json:"worker_instance_epoch"`
	WorkerMemberID             uuid.UUID         `json:"worker_member_id"`
	FromSequence               int64             `json:"from_sequence"`
	ThroughSequence            int64             `json:"through_sequence"`
	CumulativeDigest           [sha256.Size]byte `json:"cumulative_digest"`
	TerminalProofDigest        [sha256.Size]byte `json:"terminal_proof_digest"`
	InputProofDigest           [sha256.Size]byte `json:"input_proof_digest"`
	MaterializationProofDigest [sha256.Size]byte `json:"materialization_proof_digest"`
	LastCutoffDigest           [sha256.Size]byte `json:"last_cutoff_digest"`
	CompactedCutoffCount       int64             `json:"compacted_cutoff_count"`
	Revision                   int64             `json:"revision"`
}

func (c AssignmentHistoryCheckpoint) Validate() error {
	if c.ScopeDigest == ([sha256.Size]byte{}) || c.WorkerInstanceID == uuid.Nil || c.WorkerInstanceEpoch <= 0 || c.WorkerMemberID == uuid.Nil {
		return errors.New("assignment history checkpoint identity is invalid")
	}
	if c.FromSequence <= 0 || c.ThroughSequence < c.FromSequence || c.CompactedCutoffCount <= 0 || c.Revision <= 0 {
		return errors.New("assignment history checkpoint range is invalid")
	}
	if c.CumulativeDigest == ([sha256.Size]byte{}) || c.TerminalProofDigest == ([sha256.Size]byte{}) || c.InputProofDigest == ([sha256.Size]byte{}) || c.MaterializationProofDigest == ([sha256.Size]byte{}) || c.LastCutoffDigest == ([sha256.Size]byte{}) {
		return errors.New("assignment history checkpoint proof is incomplete")
	}
	return nil
}

func (c AssignmentHistoryCheckpoint) Digest() ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if err := c.Validate(); err != nil {
		return zero, err
	}
	wire, err := json.Marshal(c)
	if err != nil {
		return zero, err
	}
	return sha256.Sum256(wire), nil
}

func (c AssignmentHistoryCheckpoint) CanonicalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	wire, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var canonical AssignmentHistoryCheckpoint
	if err := json.Unmarshal(wire, &canonical); err != nil || !bytes.Equal(wire, mustMarshal(canonical)) {
		return nil, errors.New("assignment history checkpoint is not canonical")
	}
	return wire, nil
}

func (c AssignmentHistoryCutoff) Validate() error {
	if c.ScopeDigest == ([sha256.Size]byte{}) || c.WorkerInstanceID == uuid.Nil || c.WorkerInstanceEpoch <= 0 || c.WorkerMemberID == uuid.Nil {
		return errors.New("assignment history cutoff identity is invalid")
	}
	if c.FromSequence <= 0 || c.ThroughSequence < c.FromSequence {
		return errors.New("assignment history cutoff sequence range is invalid")
	}
	if c.CumulativeDigest == ([sha256.Size]byte{}) || c.TerminalProofDigest == ([sha256.Size]byte{}) || c.InputProofDigest == ([sha256.Size]byte{}) || c.MaterializationProofDigest == ([sha256.Size]byte{}) {
		return errors.New("assignment history cutoff proof is incomplete")
	}
	return nil
}

// ValidateSuccessor checks that next extends prev without a gap or identity
// change. The first cutoff must use a zero previous digest; subsequent cutoffs
// must chain to the canonical digest of the preceding record.
func (c AssignmentHistoryCutoff) ValidateSuccessor(prev *AssignmentHistoryCutoff) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if prev == nil {
		if c.PreviousCutoffDigest != ([sha256.Size]byte{}) {
			return errors.New("first assignment history cutoff has a previous digest")
		}
		if c.FromSequence != 1 {
			return errors.New("first assignment history cutoff must start at sequence one")
		}
		return nil
	}
	if err := prev.Validate(); err != nil {
		return fmt.Errorf("previous assignment history cutoff: %w", err)
	}
	if c.ScopeDigest != prev.ScopeDigest || c.WorkerInstanceID != prev.WorkerInstanceID || c.WorkerInstanceEpoch != prev.WorkerInstanceEpoch || c.WorkerMemberID != prev.WorkerMemberID {
		return errors.New("assignment history cutoff identity changed")
	}
	if c.FromSequence != prev.ThroughSequence+1 || c.ThroughSequence <= prev.ThroughSequence {
		return errors.New("assignment history cutoff is not contiguous and increasing")
	}
	digest, err := prev.Digest()
	if err != nil {
		return err
	}
	if c.PreviousCutoffDigest != digest {
		return errors.New("assignment history cutoff chain digest mismatch")
	}
	return nil
}

func (c AssignmentHistoryCutoff) Digest() ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if err := c.Validate(); err != nil {
		return zero, err
	}
	wire, err := json.Marshal(c)
	if err != nil {
		return zero, err
	}
	return sha256.Sum256(wire), nil
}

func (c AssignmentHistoryCutoff) CanonicalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	wire, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var canonical AssignmentHistoryCutoff
	if err := json.Unmarshal(wire, &canonical); err != nil || !bytes.Equal(wire, mustMarshal(canonical)) {
		return nil, errors.New("assignment history cutoff is not canonical")
	}
	return wire, nil
}

func mustMarshal(value any) []byte { wire, _ := json.Marshal(value); return wire }
