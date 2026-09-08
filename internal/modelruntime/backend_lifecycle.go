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
	if err := store.workerHealthError(); err != nil {
		return err
	}
	if store.state.BackendLifecycle == nil || store.state.BackendLifecycle.State != BackendLifecycleUnstarted || store.recoveryDrain {
		return ErrBackendIncarnationUnproven
	}
	document, err := EncodeLaunchManifest(manifest)
	if err != nil {
		return err
	}
	next := store.state
	next.BackendLifecycle = &BackendLifecycleStatus{State: BackendLifecycleUnresolved,
		IncarnationID: uuid.New(), LaunchDigest: sha256.Sum256(document), RecordedAt: time.Now().UTC()}
	return store.persist(next)
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
