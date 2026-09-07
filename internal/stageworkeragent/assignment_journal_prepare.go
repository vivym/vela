package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
)

// AssignmentJournalStatus reports validated local history, not readiness or
// writer completion. Preparation releases the lifetime lock before returning.
type AssignmentJournalStatus struct {
	Storage            journalbinding.StorageIdentity `json:"storage"`
	JournalID          uuid.UUID                      `json:"journal_id"`
	SchemaVersion      int                            `json:"schema_version"`
	Scope              [sha256.Size]byte              `json:"scope"`
	Watermark          int64                          `json:"watermark"`
	Floor              int64                          `json:"floor"`
	RetainedExecutions int                            `json:"retained_executions"`
	UnprovenInputs     int                            `json:"unproven_inputs"`
	RetirementIntents  int                            `json:"retirement_intents"`
	RetirementsReady   int                            `json:"retirements_ready"`
	RetirementsRetired int                            `json:"retirements_retired"`
}

// PrepareAssignmentJournal uses the same exclusive ownership and durability
// checks as live admission but exposes no admission handle. Initialization and
// legacy upgrades retain their explicit configuration/evidence requirements.
func PrepareAssignmentJournal(ctx context.Context, config AssignmentAdmissionConfig) (AssignmentJournalStatus, error) {
	var result AssignmentJournalStatus
	err := WithPreparedAssignmentJournal(ctx, config, func(status AssignmentJournalStatus) error {
		result = status
		return nil
	})
	if err != nil {
		return AssignmentJournalStatus{}, err
	}
	return result, nil
}

// WithPreparedAssignmentJournal holds exclusive journal ownership throughout
// inspect, without exposing input admission. It revalidates before releasing
// the lock, including on callback failure. The callback grants no writer drain.
func WithPreparedAssignmentJournal(ctx context.Context, config AssignmentAdmissionConfig, inspect func(AssignmentJournalStatus) error) (err error) {
	if ctx == nil || inspect == nil {
		return errors.New("assignment journal preparation requires context and inspection")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	gate, err := NewFileAssignmentAdmission(config)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, gate.files.validateBinding(), gate.Close(), context.Cause(ctx)) }()
	result := AssignmentJournalStatus{
		Storage: journalbinding.StorageIdentity{Root: journalbinding.FileIdentity(admissionFileIdentity(gate.files.infos[0])),
			Lock: journalbinding.FileIdentity(admissionFileIdentity(gate.files.lockInfo))},
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
	return inspect(result)
}
