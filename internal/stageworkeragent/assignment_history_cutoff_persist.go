package stageworkeragent

import (
	"context"
	"errors"
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
	var previous *AssignmentHistoryCutoff
	for index := range state.HistoryCutoffs {
		cutoff := state.HistoryCutoffs[index]
		if err := cutoff.ValidateSuccessor(previous); err != nil {
			return err
		}
		previous = &cutoff
	}
	return nil
}

func validateAssignmentHistoryCutoffsForAppend(gate *FileAssignmentAdmission, cutoff AssignmentHistoryCutoff) error {
	state := gate.state
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
