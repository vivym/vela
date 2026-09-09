package stageworkeragent

import (
	"context"
	"errors"
	"slices"
)

// RecordAssignmentHistoryCutoff durably records a validated reclamation proof.
// It deliberately does not delete history yet: deletion must be a later
// transaction whose preconditions include this exact persisted proof.
func (gate *FileAssignmentAdmission) RecordAssignmentHistoryCutoff(ctx context.Context, cutoff AssignmentHistoryCutoff) error {
	if gate == nil || ctx == nil {
		return errors.New("assignment history cutoff requires a gate and context")
	}
	if err := cutoff.Validate(); err != nil {
		return err
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	if err := validateAssignmentHistoryCutoffsForAppend(gate, cutoff); err != nil {
		return err
	}
	next := cloneAdmissionState(gate.state)
	next.HistoryCutoffs = append(next.HistoryCutoffs, cutoff)
	if err := gate.commit(ctx, next); err != nil {
		return err
	}
	return ctx.Err()
}

func validateAssignmentHistoryCutoffs(state assignmentAdmissionState) error {
	if len(state.HistoryCutoffs) == 0 {
		if state.HistoryCheckpoint != nil {
			if state.HistoryBase != state.HistoryCheckpoint.ThroughSequence {
				return errors.New("assignment history base does not match checkpoint")
			}
			return nil
		}
		if state.HistoryBase != 0 {
			return errors.New("assignment history base has no persisted cutoff")
		}
		return nil
	}
	var previous *AssignmentHistoryCutoff
	baseMatched := state.HistoryBase == 0
	for index := range state.HistoryCutoffs {
		cutoff := state.HistoryCutoffs[index]
		if index == 0 && state.HistoryCheckpoint != nil {
			checkpoint := state.HistoryCheckpoint
			if err := cutoff.Validate(); err != nil {
				return err
			}
			checkpointDigest, err := checkpoint.Digest()
			if err != nil || cutoff.FromSequence != checkpoint.ThroughSequence+1 || cutoff.PreviousCutoffDigest != checkpointDigest ||
				cutoff.ScopeDigest != checkpoint.ScopeDigest || cutoff.WorkerInstanceID != checkpoint.WorkerInstanceID ||
				cutoff.WorkerInstanceEpoch != checkpoint.WorkerInstanceEpoch || cutoff.WorkerMemberID != checkpoint.WorkerMemberID {
				return errors.New("assignment history cutoff does not extend checkpoint")
			}
		} else if err := cutoff.ValidateSuccessor(previous); err != nil {
			return err
		}
		baseMatched = baseMatched || cutoff.ThroughSequence == state.HistoryBase
		previous = &cutoff
	}
	if previous == nil || previous.ThroughSequence < state.HistoryBase || !baseMatched {
		return errors.New("assignment history base is not a persisted cutoff boundary")
	}
	return nil
}

func validateAssignmentHistoryCheckpoint(state assignmentAdmissionState) error {
	checkpoint := state.HistoryCheckpoint
	if checkpoint == nil {
		return nil
	}
	if err := checkpoint.Validate(); err != nil {
		return err
	}
	if checkpoint.ThroughSequence > state.HistoryBase {
		return errors.New("assignment history checkpoint exceeds history base")
	}
	if len(state.HistoryCutoffs) != 0 {
		first := state.HistoryCutoffs[0]
		if first.FromSequence != checkpoint.ThroughSequence+1 || first.PreviousCutoffDigest != checkpointDigestOrZero(*checkpoint) {
			return errors.New("assignment history checkpoint does not anchor retained cutoff chain")
		}
	}
	return nil
}

func checkpointDigestOrZero(checkpoint AssignmentHistoryCheckpoint) [32]byte {
	digest, _ := checkpoint.Digest()
	return digest
}

func validateAssignmentHistoryCutoffsForAppend(gate *FileAssignmentAdmission, cutoff AssignmentHistoryCutoff) error {
	state := gate.state
	if err := validateAssignmentHistoryCutoffIdentity(state, gate.scopeDigest, cutoff); err != nil {
		return err
	}
	if err := validateAssignmentHistoryCutoffs(state); err != nil {
		return err
	}
	var previous *AssignmentHistoryCutoff
	if len(state.HistoryCutoffs) != 0 {
		previous = &state.HistoryCutoffs[len(state.HistoryCutoffs)-1]
	}
	if err := cutoff.ValidateSuccessor(previous); err != nil {
		return err
	}
	if cutoff.ThroughSequence > state.Watermark {
		return errors.New("assignment history cutoff exceeds admission watermark")
	}
	if cutoff.FromSequence > cutoff.ThroughSequence {
		return errors.New("assignment history cutoff sequence range is invalid")
	}
	sequences := make(map[int64]bool)
	for _, entry := range admissionEntries(state) {
		record, err := gate.record(entry)
		if err != nil {
			return err
		}
		sequence := record.Original.GetExecutionSequence()
		if sequence < cutoff.FromSequence || sequence > cutoff.ThroughSequence {
			continue
		}
		if entry.Phase != AssignmentClosed || entry.InputDrain == nil {
			return errors.New("assignment history cutoff includes execution without terminal input proof")
		}
		sequences[sequence] = true
	}
	for sequence := cutoff.FromSequence; sequence <= cutoff.ThroughSequence; sequence++ {
		if !sequences[sequence] {
			return errors.New("assignment history cutoff range is not fully retained")
		}
	}
	return nil
}

// ReclaimAssignmentHistory removes only the contiguous, terminal prefix named
// by an already persisted cutoff. The proof remains in the journal so recovery
// can establish the new HistoryBase without trusting deleted records.
func (gate *FileAssignmentAdmission) ReclaimAssignmentHistory(ctx context.Context, cutoff AssignmentHistoryCutoff) error {
	if gate == nil || ctx == nil {
		return errors.New("assignment history reclamation requires a gate and context")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	if err := cutoff.Validate(); err != nil {
		return err
	}
	if err := validateAssignmentHistoryCutoffIdentity(gate.state, gate.scopeDigest, cutoff); err != nil {
		return err
	}
	if len(gate.state.HistoryCutoffs) == 0 || gate.state.HistoryBase+1 != cutoff.FromSequence {
		return errors.New("assignment history reclamation cutoff is not the next persisted prefix")
	}
	var persisted *AssignmentHistoryCutoff
	for index := range gate.state.HistoryCutoffs {
		candidate := &gate.state.HistoryCutoffs[index]
		if candidate.FromSequence == cutoff.FromSequence {
			persisted = candidate
			break
		}
	}
	if persisted == nil {
		return errors.New("assignment history reclamation cutoff is not persisted")
	}
	lastDigest, err := persisted.Digest()
	if err != nil || lastDigest != cutoffDigestOrZero(cutoff) {
		return errors.New("assignment history reclamation cutoff is not the persisted proof")
	}
	if err := validateAssignmentHistoryReclaimRange(gate, cutoff); err != nil {
		return err
	}
	next := cloneAdmissionState(gate.state)
	next.HistoryBase = cutoff.ThroughSequence
	filtered := next.Pending[:0]
	for _, entry := range next.Pending {
		record, err := gate.record(entry)
		if err != nil {
			return err
		}
		if record.Original.GetExecutionSequence() > cutoff.ThroughSequence {
			filtered = append(filtered, entry)
		}
	}
	next.Pending = filtered
	if next.Latest != nil {
		record, err := gate.record(*next.Latest)
		if err != nil {
			return err
		}
		if record.Original.GetExecutionSequence() <= cutoff.ThroughSequence {
			next.Latest = nil
		}
	}
	if err := gate.commit(ctx, next); err != nil {
		return err
	}
	return ctx.Err()
}

// CompactAssignmentHistory replaces a contiguous, already-reclaimed cutoff
// prefix with one durable checkpoint. The checkpoint is committed together
// with the retained suffix; old proofs are never removed before this state is
// durably written.
func (gate *FileAssignmentAdmission) CompactAssignmentHistory(ctx context.Context, checkpoint AssignmentHistoryCheckpoint) error {
	if gate == nil || ctx == nil {
		return errors.New("assignment history compaction requires a gate and context")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	if gate.state.HistoryCheckpoint != nil {
		return errors.New("assignment history checkpoint already exists")
	}
	if err := checkpoint.Validate(); err != nil {
		return err
	}
	if err := validateAssignmentHistoryCutoffIdentity(gate.state, gate.scopeDigest, AssignmentHistoryCutoff{
		ScopeDigest: checkpoint.ScopeDigest, WorkerInstanceID: checkpoint.WorkerInstanceID,
		WorkerInstanceEpoch: checkpoint.WorkerInstanceEpoch, WorkerMemberID: checkpoint.WorkerMemberID,
		FromSequence: 1, ThroughSequence: 1, CumulativeDigest: checkpoint.CumulativeDigest,
		TerminalProofDigest: checkpoint.TerminalProofDigest, InputProofDigest: checkpoint.InputProofDigest,
		MaterializationProofDigest: checkpoint.MaterializationProofDigest,
	}); err != nil {
		return err
	}
	if checkpoint.FromSequence != 1 || checkpoint.ThroughSequence > gate.state.HistoryBase || checkpoint.CompactedCutoffCount > int64(len(gate.state.HistoryCutoffs)) {
		return errors.New("assignment history checkpoint does not cover a reclaimed cutoff prefix")
	}
	count := int(checkpoint.CompactedCutoffCount)
	if count == 0 || count > len(gate.state.HistoryCutoffs) {
		return errors.New("assignment history checkpoint cutoff count is invalid")
	}
	covered := gate.state.HistoryCutoffs[count-1]
	if covered.ThroughSequence != checkpoint.ThroughSequence {
		return errors.New("assignment history checkpoint range does not match cutoff prefix")
	}
	coveredDigest, err := covered.Digest()
	if err != nil || coveredDigest != checkpoint.LastCutoffDigest {
		return errors.New("assignment history checkpoint does not match cutoff prefix digest")
	}
	next := cloneAdmissionState(gate.state)
	next.HistoryCheckpoint = &checkpoint
	next.HistoryCutoffs = slices.Clone(next.HistoryCutoffs[count:])
	if len(next.HistoryCutoffs) > 0 {
		checkpointDigest, err := checkpoint.Digest()
		if err != nil {
			return err
		}
		previousDigest := checkpointDigest
		for index := range next.HistoryCutoffs {
			next.HistoryCutoffs[index].PreviousCutoffDigest = previousDigest
			previousDigest, err = next.HistoryCutoffs[index].Digest()
			if err != nil {
				return err
			}
		}
	}
	if err := gate.commit(ctx, next); err != nil {
		return err
	}
	return ctx.Err()
}

func cutoffDigestOrZero(cutoff AssignmentHistoryCutoff) [32]byte {
	digest, _ := cutoff.Digest()
	return digest
}

func validateAssignmentHistoryReclaimRange(gate *FileAssignmentAdmission, cutoff AssignmentHistoryCutoff) error {
	if cutoff.ThroughSequence > gate.state.Watermark {
		return errors.New("assignment history reclamation exceeds admission watermark")
	}
	seen := make(map[int64]bool)
	for _, entry := range admissionEntries(gate.state) {
		record, err := gate.record(entry)
		if err != nil {
			return err
		}
		sequence := record.Original.GetExecutionSequence()
		if sequence < cutoff.FromSequence || sequence > cutoff.ThroughSequence {
			continue
		}
		if entry.Phase != AssignmentClosed || entry.InputDrain == nil {
			return errors.New("assignment history reclamation includes unproven execution")
		}
		seen[sequence] = true
	}
	for sequence := cutoff.FromSequence; sequence <= cutoff.ThroughSequence; sequence++ {
		if !seen[sequence] {
			return errors.New("assignment history reclamation range is incomplete")
		}
	}
	return nil
}

func validateAssignmentHistoryCutoffIdentity(state assignmentAdmissionState, scope [32]byte, cutoff AssignmentHistoryCutoff) error {
	if cutoff.ScopeDigest != scope || cutoff.WorkerInstanceID != state.WorkerInstanceID || cutoff.WorkerInstanceEpoch != state.WorkerInstanceEpoch || cutoff.WorkerMemberID != state.WorkerMemberID {
		return errors.New("assignment history cutoff identity does not match journal")
	}
	return nil
}
