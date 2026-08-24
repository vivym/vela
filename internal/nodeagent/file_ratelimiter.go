package nodeagent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/securefile"
)

const maxRateLimitStateBytes = 64 * 1024

type FileRateLimiter struct {
	directory string
	config    RateLimit
}

type fileRateLimitState struct {
	History []time.Time `json:"history"`
}

func NewFileRateLimiter(directory string, config RateLimit) (*FileRateLimiter, error) {
	if !validRateLimit(config) {
		return nil, errors.New("node Agent rate limit is invalid")
	}
	cleaned := filepath.Clean(directory)
	if !filepath.IsAbs(cleaned) || cleaned != directory {
		return nil, errors.New("node Agent rate-limit directory must be an absolute clean path")
	}
	if err := securefile.ValidateDirectory(cleaned); err != nil {
		return nil, errors.New("node Agent rate-limit directory does not satisfy the security contract")
	}
	return &FileRateLimiter{directory: cleaned, config: config}, nil
}

func (limiter *FileRateLimiter) Allow(now time.Time) error {
	if limiter == nil || limiter.directory == "" || !validRateLimit(limiter.config) || now.IsZero() {
		return errors.New("node Agent file rate limiter is not configured")
	}
	lockPath := filepath.Join(limiter.directory, ".remediation-rate-limit.lock")
	lock, err := securefile.OpenPrivateState(lockPath)
	if err != nil {
		return fmt.Errorf("open node Agent rate-limit lock: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock node Agent rate-limit state: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	state, err := limiter.load()
	if err != nil {
		return err
	}
	history, err := allowedHistory(limiter.config, state.History, now.UTC())
	if err != nil {
		return err
	}
	state.History = history
	return limiter.save(state)
}

func (limiter *FileRateLimiter) load() (fileRateLimitState, error) {
	path := filepath.Join(limiter.directory, ".remediation-rate-limit.json")
	content, err := securefile.Read(path, maxRateLimitStateBytes, true)
	if errors.Is(err, os.ErrNotExist) {
		return fileRateLimitState{}, nil
	}
	if err != nil {
		return fileRateLimitState{}, fmt.Errorf("read node Agent rate-limit state: %w", err)
	}
	var state fileRateLimitState
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return fileRateLimitState{}, fmt.Errorf("decode node Agent rate-limit state: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fileRateLimitState{}, errors.New("node Agent rate-limit state must contain exactly one JSON document")
	}
	for index, timestamp := range state.History {
		if timestamp.IsZero() || index > 0 && timestamp.Before(state.History[index-1]) {
			return fileRateLimitState{}, errors.New("node Agent rate-limit state contains an invalid timestamp")
		}
	}
	return state, nil
}

func (limiter *FileRateLimiter) save(state fileRateLimitState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode node Agent rate-limit state: %w", err)
	}
	temporary, err := os.CreateTemp(limiter.directory, ".remediation-rate-limit-*.tmp")
	if err != nil {
		return fmt.Errorf("create node Agent rate-limit state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("restrict node Agent rate-limit state: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write node Agent rate-limit state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync node Agent rate-limit state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close node Agent rate-limit state: %w", err)
	}
	path := filepath.Join(limiter.directory, ".remediation-rate-limit.json")
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish node Agent rate-limit state: %w", err)
	}
	directory, err := os.Open(limiter.directory)
	if err != nil {
		return fmt.Errorf("open node Agent rate-limit directory for sync: %w", err)
	}
	defer func() { _ = directory.Close() }()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync node Agent rate-limit directory: %w", err)
	}
	return nil
}

var _ ExecutionRateLimiter = (*FileRateLimiter)(nil)
