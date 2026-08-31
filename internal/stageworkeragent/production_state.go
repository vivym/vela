package stageworkeragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	productionStateFileName = "stage-worker-production-state.json"
	productionLockFileName  = "stage-worker-production-state.lock"
	maxProductionStateBytes = 16 << 10
)

type FileProductionStateConfig struct {
	Directory           string
	WorkerInstanceID    uuid.UUID
	WorkerInstanceEpoch int64
	WorkerMemberID      uuid.UUID
}

type fileProductionStateV1 struct {
	SchemaVersion               int       `json:"schema_version"`
	WorkerInstanceID            uuid.UUID `json:"worker_instance_id"`
	WorkerInstanceEpoch         int64     `json:"worker_instance_epoch"`
	WorkerMemberID              uuid.UUID `json:"worker_member_id"`
	ControlSessionEpoch         int64     `json:"control_session_epoch"`
	CapacityObservationSequence int64     `json:"capacity_observation_sequence"`
}

type FileProductionState struct {
	directory string
	statePath string
	lock      *os.File
	state     fileProductionStateV1
	mu        sync.Mutex
}

func NewFileProductionState(
	config FileProductionStateConfig,
) (*FileProductionState, error) {
	directory := filepath.Clean(config.Directory)
	if !filepath.IsAbs(directory) || directory != config.Directory ||
		config.WorkerInstanceID == uuid.Nil || config.WorkerInstanceEpoch <= 0 ||
		config.WorkerMemberID == uuid.Nil {
		return nil, errors.New("Stage Worker production state configuration is invalid")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create Stage Worker production state directory: %w", err)
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("Stage Worker production state directory is not private")
	}
	lock, err := os.OpenFile(
		filepath.Join(directory, productionLockFileName),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open Stage Worker production state lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("lock Stage Worker production state: %w", err)
	}
	state := &FileProductionState{
		directory: directory,
		statePath: filepath.Join(directory, productionStateFileName),
		lock:      lock,
	}
	if err := state.load(config); err != nil {
		_ = state.Close()
		return nil, err
	}
	return state, nil
}

func (state *FileProductionState) NextControlSessionEpoch(ctx context.Context) (int64, error) {
	return state.next(ctx, true)
}

func (state *FileProductionState) NextCapacityObservationSequence(
	ctx context.Context,
) (int64, error) {
	return state.next(ctx, false)
}

func (state *FileProductionState) Close() error {
	if state == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.lock == nil {
		return nil
	}
	lock := state.lock
	state.lock = nil
	unlockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return errors.Join(unlockErr, lock.Close())
}

func (state *FileProductionState) load(config FileProductionStateConfig) error {
	document, err := os.ReadFile(state.statePath)
	if errors.Is(err, os.ErrNotExist) {
		state.state = fileProductionStateV1{
			SchemaVersion: 1, WorkerInstanceID: config.WorkerInstanceID,
			WorkerInstanceEpoch: config.WorkerInstanceEpoch,
			WorkerMemberID:      config.WorkerMemberID,
			// Fleet creates a WorkerInstance with control_session_epoch=1.
			ControlSessionEpoch: 1,
		}
		return state.persist(state.state)
	}
	if err != nil {
		return fmt.Errorf("read Stage Worker production state: %w", err)
	}
	if len(document) == 0 || len(document) > maxProductionStateBytes ||
		strictjson.RejectDuplicateKeys(document) != nil {
		return errors.New("Stage Worker production state is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var decoded fileProductionStateV1
	if err := decoder.Decode(&decoded); err != nil {
		return errors.New("Stage Worker production state is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) ||
		!validFileProductionState(decoded) {
		return errors.New("Stage Worker production state is invalid")
	}
	if decoded.WorkerInstanceID != config.WorkerInstanceID ||
		decoded.WorkerInstanceEpoch != config.WorkerInstanceEpoch ||
		decoded.WorkerMemberID != config.WorkerMemberID {
		return errors.New("Stage Worker production state belongs to another WorkerInstance")
	}
	state.state = decoded
	return nil
}

func (state *FileProductionState) next(ctx context.Context, control bool) (int64, error) {
	if state == nil || ctx == nil {
		return 0, errors.New("Stage Worker production state is not configured")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.lock == nil {
		return 0, errors.New("Stage Worker production state is closed")
	}
	next := state.state
	var value *int64
	if control {
		value = &next.ControlSessionEpoch
	} else {
		value = &next.CapacityObservationSequence
	}
	if *value == math.MaxInt64 {
		return 0, errors.New("Stage Worker production sequence is exhausted")
	}
	*value++
	if !validFileProductionState(next) {
		return 0, errors.New("Stage Worker production state transition is invalid")
	}
	if err := state.persist(next); err != nil {
		return 0, err
	}
	state.state = next
	return *value, nil
}

func (state *FileProductionState) persist(next fileProductionStateV1) error {
	document, err := json.Marshal(next)
	if err != nil || len(document) > maxProductionStateBytes {
		return errors.New("encode Stage Worker production state")
	}
	temporary, err := os.CreateTemp(state.directory, ".stage-worker-production-*.tmp")
	if err != nil {
		return fmt.Errorf("create Stage Worker production state: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("restrict Stage Worker production state: %w", err)
	}
	if _, err := temporary.Write(document); err != nil {
		return fmt.Errorf("write Stage Worker production state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync Stage Worker production state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Stage Worker production state: %w", err)
	}
	if err := os.Rename(temporaryPath, state.statePath); err != nil {
		return fmt.Errorf("publish Stage Worker production state: %w", err)
	}
	directory, err := os.Open(state.directory)
	if err != nil {
		return fmt.Errorf("open Stage Worker production state directory: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync Stage Worker production state directory: %w", err)
	}
	committed = true
	return nil
}

func validFileProductionState(state fileProductionStateV1) bool {
	return state.SchemaVersion == 1 && state.WorkerInstanceID != uuid.Nil &&
		state.WorkerInstanceEpoch > 0 && state.WorkerMemberID != uuid.Nil &&
		state.ControlSessionEpoch > 0 && state.CapacityObservationSequence >= 0
}
