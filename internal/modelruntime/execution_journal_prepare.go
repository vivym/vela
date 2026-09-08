package modelruntime

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageauthority"
)

// ExecutionJournalStatus describes one completed offline preparation. It grants
// no execution or retirement authority and does not retain the lifetime lock.
type ExecutionJournalStatus struct {
	Storage            journalbinding.StorageIdentity `json:"storage"`
	JournalID          uuid.UUID                      `json:"journal_id"`
	SchemaVersion      int                            `json:"schema_version"`
	Scope              [sha256.Size]byte              `json:"scope"`
	Highest            int64                          `json:"highest"`
	Floor              int64                          `json:"floor"`
	RetainedExecutions int                            `json:"retained_executions"`
	PendingExecutions  int                            `json:"pending_executions"`
	BackendLifecycle   BackendLifecycleStatus         `json:"backend_lifecycle"`
}

// PrepareExecutionJournal validates trusted launch ownership and opens the
// journal without allocating Runtime epochs or starting a backend. Initialize
// requires independent first-use authorization; recovery never infers it.
// Recovery may sync state and remove unpublished temporary files. Upgrade flags
// retain their explicit validated-migration semantics.
func PrepareExecutionJournal(ctx context.Context, manifest LaunchManifest, validator *stageauthority.Validator, config ExecutionFloorStateConfig) (ExecutionJournalStatus, error) {
	var result ExecutionJournalStatus
	err := WithPreparedExecutionJournal(ctx, manifest, validator, config, func(status ExecutionJournalStatus) error {
		result = status
		return nil
	})
	if err != nil {
		return ExecutionJournalStatus{}, err
	}
	return result, nil
}

// WithPreparedExecutionJournal retains the journal lifetime lock throughout
// inspect and revalidates before release. It allocates no Runtime epoch, starts
// no backend and exposes no execution or drain authority.
func WithPreparedExecutionJournal(ctx context.Context, manifest LaunchManifest, validator *stageauthority.Validator, config ExecutionFloorStateConfig, inspect func(ExecutionJournalStatus) error) (err error) {
	if ctx == nil || validator == nil || inspect == nil {
		return errors.New("ModelRuntime journal preparation requires context, verifier and inspection")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	scope, err := executionScopeForManifest(manifest, validator)
	if err != nil {
		return err
	}
	store, err := openExecutionState(config, scope)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.check(), store.close(), context.Cause(ctx)) }()
	return inspect(store.status())
}
