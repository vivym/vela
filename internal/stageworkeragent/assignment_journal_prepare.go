package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
)

// AssignmentJournalStatus reports validated local history, not readiness or
// writer completion. Preparation releases the lifetime lock before returning.
type AssignmentJournalStatus struct {
	JournalID          uuid.UUID         `json:"journal_id"`
	SchemaVersion      int               `json:"schema_version"`
	Scope              [sha256.Size]byte `json:"scope"`
	Watermark          int64             `json:"watermark"`
	Floor              int64             `json:"floor"`
	RetainedExecutions int               `json:"retained_executions"`
	UnprovenInputs     int               `json:"unproven_inputs"`
	RetirementIntents  int               `json:"retirement_intents"`
	RetirementsReady   int               `json:"retirements_ready"`
	RetirementsRetired int               `json:"retirements_retired"`
}

// PrepareAssignmentJournal uses the same exclusive ownership and durability
// checks as live admission but exposes no admission handle. Initialization and
// legacy upgrades retain their explicit configuration/evidence requirements.
func PrepareAssignmentJournal(ctx context.Context, config AssignmentAdmissionConfig) (AssignmentJournalStatus, error) {
	if ctx == nil {
		return AssignmentJournalStatus{}, errors.New("assignment journal preparation requires context")
	}
	if err := context.Cause(ctx); err != nil {
		return AssignmentJournalStatus{}, err
	}
	gate, err := NewFileAssignmentAdmission(config)
	if err != nil {
		return AssignmentJournalStatus{}, err
	}
	result := AssignmentJournalStatus{
		JournalID: gate.state.ID, SchemaVersion: gate.state.SchemaVersion, Scope: gate.scopeDigest,
		Watermark: gate.state.Watermark, Floor: gate.state.Floor,
	}
	for _, entry := range admissionEntries(gate.state) {
		result.RetainedExecutions++
		if entry.InputDrain == nil {
			result.UnprovenInputs++
		}
	}
	for _, entry := range gate.state.Retirements {
		switch entry.Phase {
		case TerminalRetirementIntent:
			result.RetirementIntents++
		case TerminalRetirementReady:
			result.RetirementsReady++
		case TerminalRetirementRetired:
			result.RetirementsRetired++
		}
	}
	if err := errors.Join(gate.Close(), context.Cause(ctx)); err != nil {
		return AssignmentJournalStatus{}, err
	}
	return result, nil
}
