package modelruntime

import (
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// The admission mutex owns this access. view is a verified read view, not a
// mutation handle. All durable changes use the typed methods below.
type executionJournalStore interface {
	view() *executionJournal
	check() error
	close() error
	recoveryError() error
	saveHighest(*velav1.StageAuthority, time.Duration) error
	saveFloor(*velav1.StageTerminalDisposition) error
	saveCandidates(stageauthority.Verified, *stageauthority.Verified) error
	saveSeal(stageauthority.Verified, *velav1.LocalMaterializationReceipt) error
	saveHealth(stageauthority.Verified, *FailureEvidence) error
	saveDrain(stageauthority.Verified, BackendDrain, time.Time) error
	saveNonAdmission(stageauthority.Verified, executionJournalRoute, time.Time) error
	saveTerminalNonAdmission(*velav1.StageTerminalDisposition, string, executionJournalRoute, time.Time) error
	retainedAuthority([]byte) (stageauthority.Verified, error)
	retainedExecutionIndex(stageauthority.Verified) (int, error)
	nonAdmissionCheckpoint(stageauthority.Verified) (*ExecutionNonAdmissionCheckpoint, error)
	terminalNonAdmissionCheckpoint(*velav1.StageTerminalDisposition, *velav1.StageTerminalAllocation) (*TerminalNonAdmissionCheckpoint, error)
}

func (store *executionStateFile) view() *executionJournal { return &store.executionJournal }
