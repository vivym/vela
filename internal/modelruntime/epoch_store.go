package modelruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/vivym/vela/internal/stageauthority"
)

type EpochStore interface {
	Next(stageauthority.RuntimeBinding) (int64, error)
}

type EpochFloorStore interface {
	NextAfter(stageauthority.RuntimeBinding, int64) (int64, error)
}

type EpochStoreFunc func(stageauthority.RuntimeBinding) (int64, error)

func (allocate EpochStoreFunc) Next(binding stageauthority.RuntimeBinding) (int64, error) {
	return allocate(binding)
}

type FileEpochStore struct {
	directory string
}

func NewFileEpochStore(directory string) (*FileEpochStore, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, errors.New("ModelRuntime epoch directory must be an absolute clean path")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create ModelRuntime epoch directory: %w", err)
	}
	return &FileEpochStore{directory: directory}, nil
}

func (store *FileEpochStore) Next(binding stageauthority.RuntimeBinding) (int64, error) {
	return store.NextAfter(binding, 0)
}

func (store *FileEpochStore) NextAfter(
	binding stageauthority.RuntimeBinding,
	minimumExclusive int64,
) (int64, error) {
	if store == nil || store.directory == "" {
		return 0, errors.New("ModelRuntime epoch store is not configured")
	}
	if minimumExclusive < 0 {
		return 0, errors.New("ModelRuntime epoch floor is invalid")
	}
	key := strings.Join([]string{
		binding.WorkerInstanceID,
		binding.WorkerMemberID,
		binding.ModelResidencyID,
		binding.ModelRuntimeIdentity,
	}, "\x00")
	digest := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(digest[:])
	lock, err := os.OpenFile(filepath.Join(store.directory, name+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, fmt.Errorf("open ModelRuntime epoch lock: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("lock ModelRuntime epoch: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	epochPath := filepath.Join(store.directory, name+".epoch")
	current, err := readEpoch(epochPath)
	if err != nil {
		return 0, err
	}
	if current < minimumExclusive {
		current = minimumExclusive
	}
	if current == int64(^uint64(0)>>1) {
		return 0, errors.New("ModelRuntime epoch is exhausted")
	}
	next := current + 1
	if err := writeEpoch(store.directory, epochPath, next); err != nil {
		return 0, err
	}
	return next, nil
}

func readEpoch(path string) (int64, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read ModelRuntime epoch: %w", err)
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(payload)), 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New("persisted ModelRuntime epoch is invalid")
	}
	return value, nil
}

func writeEpoch(directory, path string, epoch int64) error {
	temporary, err := os.CreateTemp(directory, ".epoch-*")
	if err != nil {
		return fmt.Errorf("create ModelRuntime epoch temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure ModelRuntime epoch temporary file: %w", err)
	}
	if _, err := fmt.Fprintf(temporary, "%d\n", epoch); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write ModelRuntime epoch: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync ModelRuntime epoch: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close ModelRuntime epoch: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish ModelRuntime epoch: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open ModelRuntime epoch directory: %w", err)
	}
	defer func() { _ = directoryHandle.Close() }()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync ModelRuntime epoch directory: %w", err)
	}
	return nil
}
