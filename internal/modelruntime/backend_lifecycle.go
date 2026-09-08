package modelruntime

import (
	"crypto/sha256"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrBackendIncarnationUnproven = errors.New("ModelRuntime backend incarnation requires independently verified retirement")

type BackendLifecycleState string

const (
	BackendLifecycleUnstarted     BackendLifecycleState = "UNSTARTED"
	BackendLifecycleUnresolved    BackendLifecycleState = "UNRESOLVED"
	BackendLifecycleLegacyUnknown BackendLifecycleState = "LEGACY_UNKNOWN"
)

// BackendLifecycleStatus records startup intent, not process containment or
// physical quiescence. Its incarnation is scoped by the containing journal.
// UNSTARTED is created only by authorized first-use journal initialization.
type BackendLifecycleStatus struct {
	State         BackendLifecycleState `json:"state"`
	IncarnationID uuid.UUID             `json:"incarnation_id"`
	LaunchDigest  [sha256.Size]byte     `json:"launch_digest"`
	RecordedAt    time.Time             `json:"recorded_at"`
}

func (store *executionJournal) validateBackendLifecycle() error {
	lifecycle := store.state.BackendLifecycle
	if store.state.SchemaVersion < 6 {
		if lifecycle != nil {
			return errors.New("legacy Runtime journal cannot contain backend lifecycle evidence")
		}
		return nil
	}
	if lifecycle == nil {
		return errors.New("runtime journal is missing its backend lifecycle state")
	}
	switch lifecycle.State {
	case BackendLifecycleUnstarted, BackendLifecycleLegacyUnknown:
		if *lifecycle != (BackendLifecycleStatus{State: lifecycle.State}) {
			return errors.New("runtime backend lifecycle has unexpected incarnation evidence")
		}
	case BackendLifecycleUnresolved:
		if lifecycle.IncarnationID == uuid.Nil || lifecycle.IncarnationID.Version() != 4 || lifecycle.IncarnationID.Variant() != uuid.RFC4122 ||
			lifecycle.LaunchDigest == ([sha256.Size]byte{}) || lifecycle.RecordedAt.IsZero() || lifecycle.RecordedAt.Location() != time.UTC {
			return errors.New("runtime backend incarnation evidence is invalid")
		}
	default:
		return errors.New("runtime backend lifecycle state is invalid")
	}
	return nil
}

// The caller holds the original startup journal lock. This single member-wide
// intent precedes the first factory, covering partial AUX startup and crashes.
// Failure or normal Close never clears it; this process cannot attest its exit.
func (store *executionStateFile) recordBackendStartup(manifest LaunchManifest) error {
	if store.recoveryDrain {
		return ErrBackendIncarnationUnproven
	}
	return store.transition(func(draft *executionJournalDraft) error {
		return draft.recordBackendStartup(manifest, uuid.New(), time.Now().UTC())
	})
}

// Manifest, incarnation and time are owner-selected inputs. This records intent
// only; it cannot approve launch configuration, prove process exit, or replace
// the Registry/Node startup permission exchange.
func (draft *executionJournalDraft) recordBackendStartup(manifest LaunchManifest, incarnation uuid.UUID, observed time.Time) error {
	if err := draft.workerHealthError(); err != nil {
		return err
	}
	if draft.state.BackendLifecycle == nil || draft.state.BackendLifecycle.State != BackendLifecycleUnstarted {
		return ErrBackendIncarnationUnproven
	}
	document, err := EncodeLaunchManifest(manifest)
	if err != nil {
		return err
	}
	scope, err := executionScopeForManifest(manifest, draft.scope.floor.validator)
	if err != nil {
		return err
	}
	digest, err := scope.digest()
	if err != nil || digest != draft.state.Scope {
		return ErrBackendIncarnationUnproven
	}
	if incarnation == uuid.Nil || incarnation.Version() != 4 || incarnation.Variant() != uuid.RFC4122 || observed.IsZero() || observed.Location() != time.UTC {
		return ErrBackendIncarnationUnproven
	}
	next := draft.state
	next.BackendLifecycle = &BackendLifecycleStatus{State: BackendLifecycleUnresolved,
		IncarnationID: incarnation, LaunchDigest: sha256.Sum256(document), RecordedAt: observed}
	return draft.replace(next)
}

func (store *executionStateFile) recoveryError() error {
	if store.recoveryDrain {
		return ErrExecutionDrainUnproven
	}
	if store.recoveryBackend {
		return ErrBackendIncarnationUnproven
	}
	return store.workerHealthError()
}
