package modelruntime

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
)

// ExecutionJournalStatus describes one completed offline preparation. It grants
// no execution or retirement authority and does not retain the lifetime lock.
type ExecutionJournalStatus struct {
	JournalID          uuid.UUID         `json:"journal_id"`
	SchemaVersion      int               `json:"schema_version"`
	Scope              [sha256.Size]byte `json:"scope"`
	Highest            int64             `json:"highest"`
	Floor              int64             `json:"floor"`
	RetainedExecutions int               `json:"retained_executions"`
	PendingExecutions  int               `json:"pending_executions"`
}

// PrepareExecutionJournal validates trusted launch ownership and opens the
// journal without allocating Runtime epochs or starting a backend. Initialize
// requires independent first-use authorization; recovery never infers it.
// Recovery may sync state and remove unpublished temporary files. Upgrade flags
// retain their explicit validated-migration semantics.
func PrepareExecutionJournal(ctx context.Context, manifest LaunchManifest, validator *stageauthority.Validator, config ExecutionFloorStateConfig) (ExecutionJournalStatus, error) {
	if ctx == nil || validator == nil {
		return ExecutionJournalStatus{}, errors.New("ModelRuntime journal preparation requires context and verifier")
	}
	if err := context.Cause(ctx); err != nil {
		return ExecutionJournalStatus{}, err
	}
	bindings, err := manifest.RuntimeBindings()
	if err != nil {
		return ExecutionJournalStatus{}, err
	}
	floor, err := manifest.bindExecutionFloorConfig(ExecutionFloorConfig{}, validator)
	if err != nil {
		return ExecutionJournalStatus{}, err
	}
	verifier, err := newExecutionFloorVerifier(*floor, bindings[0])
	if err != nil {
		return ExecutionJournalStatus{}, err
	}
	store, err := openExecutionState(config, executionJournalScope{binding: cloneBinding(bindings[0]), floor: verifier})
	if err != nil {
		return ExecutionJournalStatus{}, err
	}
	result := ExecutionJournalStatus{
		JournalID: store.state.ID, SchemaVersion: store.state.SchemaVersion, Scope: store.state.Scope,
		Highest: store.state.Highest, Floor: store.state.Floor, RetainedExecutions: len(store.state.Executions),
	}
	for _, record := range store.state.Executions {
		if record.Drain == nil {
			result.PendingExecutions++
		}
	}
	if err := errors.Join(store.close(), context.Cause(ctx)); err != nil {
		return ExecutionJournalStatus{}, err
	}
	return result, nil
}
