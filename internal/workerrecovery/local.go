package workerrecovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const (
	defaultMetadataLimit = 16 * 1024
	maxRootPathLength    = 4096
	maxStateNameLength   = 128
	maxStageLength       = 32
	maxEntriesLimit      = 100000
)

var (
	ErrInvalidIdentity       = errors.New("invalid Worker local recovery identity")
	ErrStateNotFound         = errors.New("worker local recovery state was not found")
	ErrStateIdentityMismatch = errors.New("worker local recovery state identity does not match")
	ErrStateTerminal         = errors.New("worker local recovery state is terminal")
	ErrUnsafeName            = errors.New("worker local recovery state name is unsafe")
	ErrQuotaExceeded         = errors.New("worker local recovery state quota exceeded")
	ErrCriticalWatermark     = errors.New("worker local recovery storage is at the critical watermark")
	ErrInvalidStateDirectory = errors.New("worker local recovery state directory is unsafe")
)

// Stage identifies the owning stage of a local recovery file. It is not a
// customer-controlled path component.
type Stage string

const (
	StageEncoder Stage = "encoder"
	StageDiT     Stage = "dit"
	StageVAE     Stage = "vae"
	StageUpload  Stage = "upload"
)

func (stage Stage) valid() bool {
	switch stage {
	case StageEncoder, StageDiT, StageVAE, StageUpload:
		return true
	default:
		return false
	}
}

// Identity binds local state to the Worker process authority that created it.
// A changed Worker epoch or Lease fence must start from a new state directory;
// local files are never a cross-Worker or cross-fence checkpoint.
type Identity struct {
	AttemptID   uuid.UUID
	WorkerID    uuid.UUID
	WorkerEpoch int64
	Fence       int64
}

func (identity Identity) valid() bool {
	return identity.AttemptID != uuid.Nil && identity.WorkerID != uuid.Nil &&
		identity.WorkerEpoch > 0 && identity.Fence > 0
}

type Config struct {
	Root               string
	WorkerID           uuid.UUID
	WorkerEpoch        int64
	AttemptQuotaBytes  int64
	MaxEntryBytes      int64
	MaxEntries         int
	HighWatermarkBytes int64
	LowWatermarkBytes  int64
	CriticalFreeBytes  int64
	TerminalRetention  time.Duration
	SpaceProbe         func(string) (Space, error)
	Clock              func() time.Time
}

// Space is the filesystem-level capacity observation used for watermark
// decisions. Deployments with an XFS project quota should provide a probe that
// reports the quota's effective total and free values, rather than the host
// filesystem totals.
type Space struct {
	TotalBytes int64
	FreeBytes  int64
}

type WatermarkState string

const (
	WatermarkNormal    WatermarkState = "NORMAL"
	WatermarkPressured WatermarkState = "PRESSURED"
	WatermarkCritical  WatermarkState = "CRITICAL"
)

type Watermark struct {
	State             WatermarkState
	TotalBytes        int64
	FreeBytes         int64
	UsedBytes         int64
	AssignmentAllowed bool
	ResumeEligible    bool
}

type Entry struct {
	Name       string
	Stage      Stage
	SizeBytes  int64
	ModifiedAt time.Time
}

type ReconcileResult struct {
	Scanned  int
	Removed  int
	Deferred int
	Unsafe   int
}

type Manager struct {
	root              string
	attemptsRoot      string
	terminalRoot      string
	workerID          uuid.UUID
	workerEpoch       int64
	attemptQuotaBytes int64
	maxEntryBytes     int64
	maxEntries        int
	highWatermark     int64
	lowWatermark      int64
	criticalFree      int64
	terminalRetention time.Duration
	spaceProbe        func(string) (Space, error)
	clock             func() time.Time
	mu                sync.Mutex
}

type rootMetadata struct {
	WorkerID    uuid.UUID `json:"worker_id"`
	WorkerEpoch int64     `json:"worker_epoch"`
}

type attemptMetadata struct {
	AttemptID   uuid.UUID  `json:"attempt_id"`
	WorkerID    uuid.UUID  `json:"worker_id"`
	WorkerEpoch int64      `json:"worker_epoch"`
	Fence       int64      `json:"fence"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	TerminalAt  *time.Time `json:"terminal_at,omitempty"`
}

// New creates or opens the Worker-local state root. The root is identity-bound
// on first use, so accidentally mounting the same NVMe path into another
// Worker or epoch fails closed.
func New(config Config) (*Manager, error) {
	cleanRoot := filepath.Clean(config.Root)
	if !filepath.IsAbs(cleanRoot) || cleanRoot == string(filepath.Separator) ||
		len(cleanRoot) > maxRootPathLength || strings.ContainsRune(cleanRoot, '\x00') {
		return nil, errors.New("worker local recovery root must be a bounded absolute non-root path")
	}
	if config.WorkerID == uuid.Nil || config.WorkerEpoch <= 0 {
		return nil, ErrInvalidIdentity
	}
	if config.AttemptQuotaBytes <= 0 || config.MaxEntryBytes <= 0 ||
		config.MaxEntryBytes > config.AttemptQuotaBytes {
		return nil, errors.New("worker local recovery quotas are invalid")
	}
	if config.MaxEntries <= 0 || config.MaxEntries > maxEntriesLimit {
		return nil, errors.New("worker local recovery entry limit is invalid")
	}
	if config.LowWatermarkBytes < 0 || config.HighWatermarkBytes <= config.LowWatermarkBytes ||
		config.CriticalFreeBytes < 0 {
		return nil, errors.New("worker local recovery watermarks are invalid")
	}
	if config.TerminalRetention < time.Second || config.TerminalRetention > 24*time.Hour {
		return nil, errors.New("worker local recovery terminal retention must be between one second and 24 hours")
	}
	spaceProbe := config.SpaceProbe
	if spaceProbe == nil {
		spaceProbe = statSpace
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	if err := ensureSecureDirectory(cleanRoot); err != nil {
		return nil, fmt.Errorf("prepare Worker local recovery root: %w", err)
	}
	manager := &Manager{
		root:              cleanRoot,
		workerID:          config.WorkerID,
		workerEpoch:       config.WorkerEpoch,
		attemptQuotaBytes: config.AttemptQuotaBytes,
		maxEntryBytes:     config.MaxEntryBytes,
		maxEntries:        config.MaxEntries,
		highWatermark:     config.HighWatermarkBytes,
		lowWatermark:      config.LowWatermarkBytes,
		criticalFree:      config.CriticalFreeBytes,
		terminalRetention: config.TerminalRetention,
		spaceProbe:        spaceProbe,
		clock:             clock,
	}
	if err := manager.bindRoot(); err != nil {
		return nil, err
	}
	attemptsRoot := filepath.Join(cleanRoot, "attempts")
	if err := ensureSecureDirectory(attemptsRoot); err != nil {
		return nil, fmt.Errorf("prepare Worker local recovery attempts root: %w", err)
	}
	terminalRoot := filepath.Join(cleanRoot, "terminal")
	if err := ensureSecureDirectory(terminalRoot); err != nil {
		return nil, fmt.Errorf("prepare Worker local recovery terminal root: %w", err)
	}
	manager.attemptsRoot = attemptsRoot
	manager.terminalRoot = terminalRoot
	return manager, nil
}

func (manager *Manager) bindRoot() error {
	path := filepath.Join(manager.root, ".worker.json")
	manager.mu.Lock()
	defer manager.mu.Unlock()
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return writeJSONAtomic(manager.root, path, rootMetadata{
			WorkerID: manager.workerID, WorkerEpoch: manager.workerEpoch,
		})
	}
	if err != nil {
		return fmt.Errorf("inspect Worker local recovery identity: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidStateDirectory
	}
	var metadata rootMetadata
	if err := readJSONBounded(path, defaultMetadataLimit, &metadata); err != nil {
		return fmt.Errorf("read Worker local recovery identity: %w", err)
	}
	if metadata.WorkerID != manager.workerID || metadata.WorkerEpoch != manager.workerEpoch {
		return ErrStateIdentityMismatch
	}
	return nil
}

// Open returns a handle for same-Worker, same-epoch, same-fence process
// recovery. A new fence is intentionally rejected to prevent stale local
// output from becoming a checkpoint for a replacement Attempt authority.
func (manager *Manager) Open(ctx context.Context, identity Identity) (*Handle, error) {
	if manager == nil {
		return nil, errors.New("worker local recovery manager is not configured")
	}
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	if !identity.valid() || identity.WorkerID != manager.workerID ||
		identity.WorkerEpoch != manager.workerEpoch {
		return nil, ErrInvalidIdentity
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	terminalPath := manager.terminalPath(identity.AttemptID)
	if _, err := os.Lstat(terminalPath); err == nil {
		return nil, ErrStateTerminal
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect terminal Worker local recovery state: %w", err)
	}
	path := manager.attemptPath(identity.AttemptID)
	if err := ensureSecureDirectory(path); err != nil {
		return nil, fmt.Errorf("prepare Attempt local recovery directory: %w", err)
	}
	metadata, err := manager.readAttemptMetadata(path)
	if errors.Is(err, os.ErrNotExist) {
		now := manager.clock().UTC()
		metadata = attemptMetadata{
			AttemptID: identity.AttemptID, WorkerID: identity.WorkerID,
			WorkerEpoch: identity.WorkerEpoch, Fence: identity.Fence,
			CreatedAt: now, UpdatedAt: now,
		}
		if err := manager.writeAttemptMetadata(path, metadata); err != nil {
			return nil, fmt.Errorf("create Attempt local recovery metadata: %w", err)
		}
	} else if err != nil {
		return nil, err
	} else if metadata.AttemptID != identity.AttemptID || metadata.WorkerID != identity.WorkerID ||
		metadata.WorkerEpoch != identity.WorkerEpoch || metadata.Fence != identity.Fence {
		return nil, ErrStateIdentityMismatch
	} else if metadata.TerminalAt != nil {
		return nil, ErrStateTerminal
	}
	return &Handle{manager: manager, identity: identity}, nil
}

type Handle struct {
	manager  *Manager
	identity Identity
}

func (handle *Handle) Write(
	ctx context.Context,
	stage Stage,
	name string,
	source io.Reader,
) (Entry, error) {
	if handle == nil || handle.manager == nil {
		return Entry{}, errors.New("worker local recovery handle is not configured")
	}
	if err := requireContext(ctx); err != nil {
		return Entry{}, err
	}
	if !stage.valid() || !validStateName(name) || source == nil {
		return Entry{}, ErrUnsafeName
	}
	manager := handle.manager
	manager.mu.Lock()
	defer manager.mu.Unlock()
	path := manager.attemptPath(handle.identity.AttemptID)
	metadata, err := manager.requireMetadata(path, handle.identity)
	if err != nil {
		return Entry{}, err
	}
	if metadata.TerminalAt != nil {
		return Entry{}, ErrStateTerminal
	}
	if err := manager.requireWritableSpace(0); err != nil {
		return Entry{}, err
	}
	temporary, err := os.CreateTemp(path, ".tmp-")
	if err != nil {
		return Entry{}, fmt.Errorf("create local recovery temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0600); err != nil {
		return Entry{}, fmt.Errorf("protect local recovery temporary file: %w", err)
	}
	written, err := io.CopyN(temporary, source, manager.maxEntryBytes+1)
	if err != nil && !errors.Is(err, io.EOF) {
		return Entry{}, fmt.Errorf("write local recovery state: %w", err)
	}
	if written > manager.maxEntryBytes {
		return Entry{}, ErrQuotaExceeded
	}
	if err := temporary.Sync(); err != nil {
		return Entry{}, fmt.Errorf("sync local recovery state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return Entry{}, fmt.Errorf("close local recovery state: %w", err)
	}
	entryPath := filepath.Join(path, stateFileName(stage, name))
	oldSize, oldExists, err := regularFileSize(entryPath)
	if err != nil {
		return Entry{}, err
	}
	entries, err := manager.listEntries(path)
	if err != nil {
		return Entry{}, err
	}
	var currentBytes int64
	for _, entry := range entries {
		if entry.SizeBytes > manager.attemptQuotaBytes-currentBytes {
			return Entry{}, ErrQuotaExceeded
		}
		currentBytes += entry.SizeBytes
	}
	if written > manager.attemptQuotaBytes-(currentBytes-oldSize) {
		return Entry{}, ErrQuotaExceeded
	}
	if err := manager.requireWritableSpace(written - oldSize); err != nil {
		return Entry{}, err
	}
	if !oldExists {
		if len(entries) >= manager.maxEntries {
			return Entry{}, ErrQuotaExceeded
		}
	}
	if err := os.Rename(temporaryPath, entryPath); err != nil {
		return Entry{}, fmt.Errorf("publish local recovery state: %w", err)
	}
	if info, err := os.Lstat(entryPath); err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return Entry{}, ErrInvalidStateDirectory
	}
	now := manager.clock().UTC()
	metadata.UpdatedAt = now
	if err := manager.writeAttemptMetadata(path, metadata); err != nil {
		return Entry{}, fmt.Errorf("update local recovery metadata: %w", err)
	}
	if err := syncDirectory(path); err != nil {
		return Entry{}, fmt.Errorf("sync local recovery directory: %w", err)
	}
	return Entry{Name: name, Stage: stage, SizeBytes: written, ModifiedAt: now}, nil
}

func (handle *Handle) Read(ctx context.Context, stage Stage, name string) ([]byte, error) {
	if handle == nil || handle.manager == nil {
		return nil, errors.New("worker local recovery handle is not configured")
	}
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	if !stage.valid() || !validStateName(name) {
		return nil, ErrUnsafeName
	}
	manager := handle.manager
	manager.mu.Lock()
	defer manager.mu.Unlock()
	path := manager.attemptPath(handle.identity.AttemptID)
	metadata, err := manager.requireMetadata(path, handle.identity)
	if err != nil {
		return nil, err
	}
	if metadata.TerminalAt != nil {
		return nil, ErrStateTerminal
	}
	entryPath := filepath.Join(path, stateFileName(stage, name))
	info, err := os.Lstat(entryPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrStateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("inspect local recovery state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > manager.maxEntryBytes {
		return nil, ErrInvalidStateDirectory
	}
	return os.ReadFile(entryPath)
}

func (handle *Handle) List(ctx context.Context) ([]Entry, error) {
	if handle == nil || handle.manager == nil {
		return nil, errors.New("worker local recovery handle is not configured")
	}
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	manager := handle.manager
	manager.mu.Lock()
	defer manager.mu.Unlock()
	path := manager.attemptPath(handle.identity.AttemptID)
	metadata, err := manager.requireMetadata(path, handle.identity)
	if err != nil {
		return nil, err
	}
	if metadata.TerminalAt != nil {
		return nil, ErrStateTerminal
	}
	return manager.listEntries(path)
}

// MarkTerminal records the terminal marker before attempting immediate
// deletion. If a process dies during removal, Reconcile can safely finish it.
func (handle *Handle) MarkTerminal(ctx context.Context) error {
	if handle == nil || handle.manager == nil {
		return errors.New("worker local recovery handle is not configured")
	}
	if err := requireContext(ctx); err != nil {
		return err
	}
	manager := handle.manager
	manager.mu.Lock()
	defer manager.mu.Unlock()
	path := manager.attemptPath(handle.identity.AttemptID)
	metadata, err := manager.requireMetadata(path, handle.identity)
	if err != nil {
		return err
	}
	if metadata.TerminalAt == nil {
		now := manager.clock().UTC()
		metadata.TerminalAt = &now
		metadata.UpdatedAt = now
		if err := manager.writeAttemptMetadata(path, metadata); err != nil {
			return fmt.Errorf("mark local recovery state terminal: %w", err)
		}
	}
	if err := writeJSONAtomic(manager.terminalRoot, manager.terminalPath(handle.identity.AttemptID), metadata); err != nil {
		return fmt.Errorf("record terminal Worker local recovery state: %w", err)
	}
	return removeAttemptDirectory(path)
}

func (manager *Manager) Watermark(ctx context.Context) (Watermark, error) {
	if manager == nil {
		return Watermark{}, errors.New("worker local recovery manager is not configured")
	}
	if err := requireContext(ctx); err != nil {
		return Watermark{}, err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	space, err := manager.spaceProbe(manager.root)
	if err != nil {
		return Watermark{}, fmt.Errorf("observe Worker local recovery capacity: %w", err)
	}
	if space.TotalBytes <= 0 || space.FreeBytes < 0 || space.FreeBytes > space.TotalBytes {
		return Watermark{}, errors.New("worker local recovery capacity observation is invalid")
	}
	used := space.TotalBytes - space.FreeBytes
	state := WatermarkNormal
	if used >= manager.highWatermark {
		state = WatermarkPressured
	}
	if space.FreeBytes <= manager.criticalFree {
		state = WatermarkCritical
	}
	return Watermark{
		State: state, TotalBytes: space.TotalBytes, FreeBytes: space.FreeBytes,
		UsedBytes: used, AssignmentAllowed: state == WatermarkNormal,
		ResumeEligible: used <= manager.lowWatermark,
	}, nil
}

// Reconcile only removes directories that carry a durable terminal marker.
// Unknown or malformed directories are counted and left untouched so a bad
// mount or an unexpected file cannot cause broad deletion of customer data.
func (manager *Manager) Reconcile(ctx context.Context) (ReconcileResult, error) {
	if manager == nil {
		return ReconcileResult{}, errors.New("worker local recovery manager is not configured")
	}
	if err := requireContext(ctx); err != nil {
		return ReconcileResult{}, err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entries, err := os.ReadDir(manager.attemptsRoot)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("list Worker local recovery Attempts: %w", err)
	}
	now := manager.clock().UTC()
	result := ReconcileResult{}
	for _, entry := range entries {
		result.Scanned++
		path := filepath.Join(manager.attemptsRoot, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return result, fmt.Errorf("inspect Worker local recovery Attempt: %w", err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			result.Unsafe++
			continue
		}
		metadata, err := manager.readAttemptMetadata(path)
		if err != nil || metadata.WorkerID != manager.workerID || metadata.WorkerEpoch != manager.workerEpoch ||
			metadata.AttemptID != parseUUID(entry.Name()) {
			result.Unsafe++
			continue
		}
		if metadata.TerminalAt == nil || now.Before(metadata.TerminalAt.Add(manager.terminalRetention)) {
			result.Deferred++
			continue
		}
		if err := removeAttemptDirectory(path); err != nil {
			return result, fmt.Errorf("remove terminal Worker local recovery state: %w", err)
		}
		result.Removed++
	}
	terminalEntries, err := os.ReadDir(manager.terminalRoot)
	if err != nil {
		return result, fmt.Errorf("list terminal Worker local recovery state: %w", err)
	}
	for _, entry := range terminalEntries {
		result.Scanned++
		path := filepath.Join(manager.terminalRoot, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return result, fmt.Errorf("inspect terminal Worker local recovery state: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			result.Unsafe++
			continue
		}
		var metadata attemptMetadata
		if err := readJSONBounded(path, defaultMetadataLimit, &metadata); err != nil ||
			metadata.AttemptID == uuid.Nil || metadata.WorkerID != manager.workerID ||
			metadata.WorkerEpoch != manager.workerEpoch || metadata.TerminalAt == nil {
			result.Unsafe++
			continue
		}
		if now.Before(metadata.TerminalAt.Add(manager.terminalRetention)) {
			result.Deferred++
			continue
		}
		if err := os.Remove(path); err != nil {
			return result, fmt.Errorf("remove terminal Worker local recovery marker: %w", err)
		}
		result.Removed++
	}
	if err := syncDirectory(manager.terminalRoot); err != nil {
		return result, fmt.Errorf("sync terminal Worker local recovery root: %w", err)
	}
	return result, nil
}

func (manager *Manager) requireWritableSpace(additionalBytes int64) error {
	if additionalBytes < 0 {
		additionalBytes = 0
	}
	space, err := manager.spaceProbe(manager.root)
	if err != nil {
		return fmt.Errorf("observe Worker local recovery capacity: %w", err)
	}
	if space.TotalBytes <= 0 || space.FreeBytes < 0 || space.FreeBytes > space.TotalBytes {
		return errors.New("worker local recovery capacity observation is invalid")
	}
	if space.FreeBytes <= manager.criticalFree || additionalBytes > space.FreeBytes-manager.criticalFree {
		return ErrCriticalWatermark
	}
	return nil
}

func (manager *Manager) requireMetadata(path string, identity Identity) (attemptMetadata, error) {
	metadata, err := manager.readAttemptMetadata(path)
	if errors.Is(err, os.ErrNotExist) {
		return attemptMetadata{}, ErrStateNotFound
	}
	if err != nil {
		return attemptMetadata{}, err
	}
	if metadata.AttemptID != identity.AttemptID || metadata.WorkerID != identity.WorkerID ||
		metadata.WorkerEpoch != identity.WorkerEpoch || metadata.Fence != identity.Fence {
		return attemptMetadata{}, ErrStateIdentityMismatch
	}
	return metadata, nil
}

func (manager *Manager) readAttemptMetadata(path string) (attemptMetadata, error) {
	var metadata attemptMetadata
	if err := readJSONBounded(filepath.Join(path, ".identity.json"), defaultMetadataLimit, &metadata); err != nil {
		return attemptMetadata{}, fmt.Errorf("read Attempt local recovery metadata: %w", err)
	}
	if metadata.AttemptID == uuid.Nil || metadata.WorkerID == uuid.Nil || metadata.WorkerEpoch <= 0 ||
		metadata.Fence <= 0 || metadata.CreatedAt.IsZero() || metadata.UpdatedAt.IsZero() {
		return attemptMetadata{}, ErrInvalidIdentity
	}
	return metadata, nil
}

func (manager *Manager) writeAttemptMetadata(path string, metadata attemptMetadata) error {
	return writeJSONAtomic(path, filepath.Join(path, ".identity.json"), metadata)
}

func (manager *Manager) attemptPath(attemptID uuid.UUID) string {
	return filepath.Join(manager.attemptsRoot, attemptID.String())
}

func (manager *Manager) terminalPath(attemptID uuid.UUID) string {
	return filepath.Join(manager.terminalRoot, attemptID.String()+".json")
}

func (manager *Manager) listEntries(path string) ([]Entry, error) {
	directoryEntries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("list Attempt local recovery state: %w", err)
	}
	result := make([]Entry, 0, len(directoryEntries))
	for _, directoryEntry := range directoryEntries {
		if strings.HasPrefix(directoryEntry.Name(), ".") {
			continue
		}
		stage, name, ok := parseStateFileName(directoryEntry.Name())
		if !ok {
			return nil, ErrInvalidStateDirectory
		}
		path := filepath.Join(path, directoryEntry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("inspect Attempt local recovery state: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > manager.maxEntryBytes {
			return nil, ErrInvalidStateDirectory
		}
		result = append(result, Entry{Name: name, Stage: stage, SizeBytes: info.Size(), ModifiedAt: info.ModTime().UTC()})
	}
	return result, nil
}

func validStateName(name string) bool {
	if name == "" || len(name) > maxStateNameLength || name == "." || name == ".." ||
		filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) {
		return false
	}
	for _, char := range name {
		if char > unicode.MaxASCII ||
			(!unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '.' && char != '_' && char != '-') {
			return false
		}
	}
	return true
}

func stateFileName(stage Stage, name string) string {
	return string(stage) + "--" + name
}

func parseStateFileName(name string) (Stage, string, bool) {
	parts := strings.SplitN(name, "--", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	stage, stateName := Stage(parts[0]), parts[1]
	return stage, stateName, stage.valid() && validStateName(stateName)
}

func parseUUID(name string) uuid.UUID {
	parsed, err := uuid.Parse(name)
	if err != nil {
		return uuid.Nil
	}
	return parsed
}

func regularFileSize(path string) (int64, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("inspect existing local recovery state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return 0, false, ErrInvalidStateDirectory
	}
	return info.Size(), true, nil
}

func ensureSecureDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidStateDirectory
	}
	if err := os.Chmod(path, 0700); err != nil {
		return err
	}
	return nil
}

func writeJSONAtomic(directory, path string, value any) error {
	if err := ensureSecureDirectory(directory); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".json-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0600); err != nil {
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetEscapeHTML(true)
	if err := encoder.Encode(value); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func readJSONBounded(path string, limit int64, target any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > limit {
		return ErrInvalidStateDirectory
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(io.LimitReader(file, limit+1))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func removeAttemptDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidStateDirectory
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		entryInfo, err := os.Lstat(entryPath)
		if err != nil {
			return err
		}
		if entryInfo.IsDir() || entryInfo.Mode()&os.ModeSymlink != 0 || !entryInfo.Mode().IsRegular() {
			return ErrInvalidStateDirectory
		}
		if err := os.Remove(entryPath); err != nil {
			return err
		}
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func statSpace(path string) (Space, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return Space{}, err
	}
	blockSize := uint64(stat.Bsize)
	if blockSize == 0 {
		return Space{}, errors.New("filesystem block size is zero")
	}
	total, ok := multiplyUint64(uint64(stat.Blocks), blockSize)
	if !ok {
		return Space{}, errors.New("filesystem total capacity overflowed")
	}
	available, ok := multiplyUint64(uint64(stat.Bavail), blockSize)
	if !ok || available > total || total > uint64(^uint64(0)>>1) || available > uint64(^uint64(0)>>1) {
		return Space{}, errors.New("filesystem capacity observation is invalid")
	}
	return Space{TotalBytes: int64(total), FreeBytes: int64(available)}, nil
}

func multiplyUint64(left, right uint64) (uint64, bool) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, false
	}
	return left * right, true
}

func requireContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker local recovery context is required")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
