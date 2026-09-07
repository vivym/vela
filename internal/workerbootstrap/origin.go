package workerbootstrap

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
)

const originName = "journal-origin.json"

// This record preserves the preparer's original observations separately from
// mutable journal state. Its owner and storage remain trusted; it is not an
// independently authenticated Node record or a production startup grant.
type journalOrigin struct {
	SchemaVersion int                            `json:"schema_version"`
	Pair          journalPair                    `json:"pair"`
	Worker        journalbinding.StorageIdentity `json:"worker"`
	Runtime       journalbinding.StorageIdentity `json:"runtime"`
}

func (state *operationState) writeOrigin(origin journalOrigin) error {
	if err := state.validate(); err != nil {
		return err
	}
	if err := state.validateOrigin(origin); err != nil {
		return err
	}
	wire, err := json.Marshal(origin)
	if err != nil {
		return err
	}
	file, err := state.roots[operationRoot].OpenFile(originName, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	info, statErr := file.Stat()
	_, writeErr := file.Write(wire)
	if err := errors.Join(statErr, writeErr, file.Sync(), file.Close(), syncRoot(state.roots[operationRoot])); err != nil {
		return err
	}
	state.originInfo, state.originBytes = info, wire
	return state.validate()
}

func (state *operationState) readOrigin() (journalOrigin, error) {
	file, err := state.roots[operationRoot].OpenFile(originName, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return journalOrigin{}, fmt.Errorf("%w: original journal storage record unavailable: %w", ErrIncomplete, err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	current, currentErr := state.roots[operationRoot].Lstat(originName)
	if err != nil || currentErr != nil || !privateFile(info) || !os.SameFile(info, current) ||
		state.originInfo != nil && !os.SameFile(info, state.originInfo) {
		return journalOrigin{}, errors.New("original journal storage record is untrusted or replaced")
	}
	var origin journalOrigin
	wire, err := readDocument(file, &origin)
	if err != nil {
		return journalOrigin{}, err
	}
	if err := state.validateOrigin(origin); err != nil {
		return journalOrigin{}, err
	}
	if state.originInfo == nil {
		// A previous process may have died before file or directory sync.
		if err := errors.Join(file.Sync(), syncRoot(state.roots[operationRoot])); err != nil {
			return journalOrigin{}, err
		}
		state.originInfo, state.originBytes = info, wire
	}
	return origin, nil
}

func (state *operationState) validateOrigin(origin journalOrigin) error {
	pair := origin.Pair
	if origin.SchemaVersion != 1 || pair.RequestID != state.operation.RequestID || pair.WorkerID == uuid.Nil || pair.RuntimeID == uuid.Nil ||
		pair.WorkerID == pair.RuntimeID || pair.WorkerScope == ([sha256.Size]byte{}) || pair.RuntimeScope == ([sha256.Size]byte{}) ||
		!origin.Worker.Valid() || !origin.Runtime.Valid() || origin.Worker.Lock == origin.Runtime.Lock ||
		origin.Worker.Root != journalbinding.FileIdentity(state.operation.Binding.Roots[workerRoot]) ||
		origin.Runtime.Root != journalbinding.FileIdentity(state.operation.Binding.Roots[runtimeRoot]) {
		return errors.New("original journal storage record differs from the bootstrap operation")
	}
	return nil
}
