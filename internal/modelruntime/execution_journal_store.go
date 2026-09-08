package modelruntime

import (
	"context"
	"errors"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// The admission mutex owns this access. view is a verified read view, not a
// mutation handle. All durable changes use the typed methods below.
type executionJournalStore interface {
	view() *executionJournal
	checkContext(context.Context) error
	close() error
	recoveryError() error
	saveHighestContext(context.Context, *velav1.StageAuthority, time.Duration) error
	saveFloorContext(context.Context, *velav1.StageTerminalDisposition) error
	saveCandidatesContext(context.Context, stageauthority.Verified, *stageauthority.Verified) error
	saveSealContext(context.Context, stageauthority.Verified, *velav1.LocalMaterializationReceipt) error
	saveHealthContext(context.Context, stageauthority.Verified, *FailureEvidence) error
	saveDrainContext(context.Context, stageauthority.Verified, BackendDrain, time.Time) error
	saveNonAdmissionContext(context.Context, stageauthority.Verified, executionJournalRoute, time.Time) error
	saveTerminalNonAdmissionContext(context.Context, *velav1.StageTerminalDisposition, string, executionJournalRoute, time.Time) error
	retainedAuthority([]byte) (stageauthority.Verified, error)
	retainedExecutionIndex(stageauthority.Verified) (int, error)
	nonAdmissionCheckpoint(stageauthority.Verified) (*ExecutionNonAdmissionCheckpoint, error)
	terminalNonAdmissionCheckpoint(*velav1.StageTerminalDisposition, *velav1.StageTerminalAllocation) (*TerminalNonAdmissionCheckpoint, error)
}

func (store *executionStateFile) view() *executionJournal { return &store.executionJournal }

// Local filesystem operations are synchronous: context bounds entry, not an
// in-progress fsync. Preserve the owner's non-contextual transition API.
func (store *executionStateFile) checkContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(errJournalReadInterrupted, err)
	}
	return store.check()
}
func (store *executionStateFile) saveHighestContext(ctx context.Context, authority *velav1.StageAuthority, skew time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveHighest(authority, skew)
}
func (store *executionStateFile) saveFloorContext(ctx context.Context, disposition *velav1.StageTerminalDisposition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveFloor(disposition)
}
func (store *executionStateFile) saveCandidatesContext(ctx context.Context, accepted stageauthority.Verified, confirmed *stageauthority.Verified) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveCandidates(accepted, confirmed)
}
func (store *executionStateFile) saveSealContext(ctx context.Context, authority stageauthority.Verified, receipt *velav1.LocalMaterializationReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveSeal(authority, receipt)
}
func (store *executionStateFile) saveHealthContext(ctx context.Context, authority stageauthority.Verified, evidence *FailureEvidence) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveHealth(authority, evidence)
}
func (store *executionStateFile) saveDrainContext(ctx context.Context, authority stageauthority.Verified, drain BackendDrain, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveDrain(authority, drain, at)
}
func (store *executionStateFile) saveNonAdmissionContext(ctx context.Context, authority stageauthority.Verified, route executionJournalRoute, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveNonAdmission(authority, route, at)
}
func (store *executionStateFile) saveTerminalNonAdmissionContext(ctx context.Context, disposition *velav1.StageTerminalDisposition, id string, route executionJournalRoute, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.saveTerminalNonAdmission(disposition, id, route, at)
}
