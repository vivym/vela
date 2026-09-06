package modelruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/strictjson"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	executionStateName     = "execution-admission.json"
	executionStateLockName = "execution-admission.lock"
	maxExecutionStateBytes = 12 << 20
	maxExecutionWireBytes  = 64 << 10
	maxRetainedExecutions  = 32
)

type executionFileIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type executionDiskState struct {
	SchemaVersion         int                         `json:"schema_version"`
	ID                    uuid.UUID                   `json:"journal_id"`
	Scope                 [sha256.Size]byte           `json:"scope"`
	Root                  executionFileIdentity       `json:"root"`
	Lock                  executionFileIdentity       `json:"lock"`
	Highest               int64                       `json:"highest"`
	Authority             []byte                      `json:"highest_authority"`
	Floor                 int64                       `json:"floor"`
	Disposition           []byte                      `json:"floor_disposition"`
	Executions            []retainedExecution         `json:"executions"`
	NonAdmissions         []executionDiskNonAdmission `json:"non_admissions,omitempty"`
	TerminalNonAdmissions []terminalDiskNonAdmission  `json:"terminal_non_admissions,omitempty"`
}

type retainedExecution struct {
	Authority  []byte                   `json:"authority"`
	Drain      *executionDiskDrain      `json:"drain"`
	Candidates *executionDiskCandidates `json:"candidates,omitempty"`
}

type executionDiskCandidates struct {
	Accepted  []byte `json:"accepted"`
	Confirmed []byte `json:"confirmed,omitempty"`
}

type executionDiskDrain struct {
	Authority []byte       `json:"authority"`
	Result    BackendDrain `json:"result"`
	DrainedAt time.Time    `json:"drained_at"`
}

// The enclosing admission mutex owns this store and its lifetime lock.
type executionStateFile struct {
	scope         executionJournalScope
	path          string
	root          *os.Root
	rootInfo      os.FileInfo
	lock          *os.File
	lockInfo      os.FileInfo
	lockID        uuid.UUID
	stateInfo     os.FileInfo
	stateDigest   [sha256.Size]byte
	state         executionDiskState
	recoveryDrain bool
	syncDirectory func(*os.Root) error
}

func openExecutionState(config ExecutionFloorStateConfig, journalScope executionJournalScope) (*executionStateFile, error) {
	selected := 0
	for _, enabled := range []bool{config.Initialize, config.UpgradeV2, config.UpgradeV3, config.UpgradeV4} {
		if enabled {
			selected++
		}
	}
	if selected > 1 {
		return nil, errors.New("ModelRuntime execution state bootstrap and upgrades are mutually exclusive")
	}
	if !filepath.IsAbs(config.Directory) || filepath.Clean(config.Directory) != config.Directory {
		return nil, errors.New("ModelRuntime execution state requires an absolute clean directory")
	}
	info, err := os.Lstat(config.Directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("ModelRuntime execution state directory is missing or not private")
	}
	root, err := securefile.OpenTrustedRoot(config.Directory)
	if err != nil {
		return nil, err
	}
	store := &executionStateFile{scope: journalScope, path: config.Directory, root: root, rootInfo: info, syncDirectory: syncExecutionStateDirectory}
	success := false
	defer func() {
		if !success {
			_ = store.close()
		}
	}()
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		return nil, errors.New("ModelRuntime execution state directory changed while opening")
	}
	flags := os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	if config.Initialize {
		flags |= os.O_CREATE | os.O_EXCL
	}
	store.lock, err = root.OpenFile(executionStateLockName, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ModelRuntime execution state lock: %w", err)
	}
	store.lockInfo, err = store.lock.Stat()
	if err != nil || !executionPrivateFile(store.lockInfo, 36, config.Initialize) {
		return nil, errors.New("ModelRuntime execution state lock is not private regular data")
	}
	if err := syscall.Flock(int(store.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock ModelRuntime execution state: %w", err)
	}
	scope, err := journalScope.digest()
	if err != nil {
		return nil, err
	}
	if config.Initialize {
		if err := executionStateDirectoryEmpty(root); err != nil {
			return nil, err
		}
		state := executionDiskState{SchemaVersion: 5, ID: uuid.New(), Scope: scope, Root: executionIdentity(info), Lock: executionIdentity(store.lockInfo)}
		store.lockID = state.ID
		if _, err := store.lock.WriteString(state.ID.String()); err != nil {
			return nil, err
		}
		if err := store.lock.Sync(); err != nil {
			return nil, err
		}
		if err := store.persist(state); err != nil {
			return nil, err
		}
	} else {
		store.lockID, err = readExecutionLockID(store.lock)
		if err != nil {
			return nil, err
		}
		document, stateInfo, err := readExecutionStateFile(root)
		if err != nil {
			return nil, fmt.Errorf("ModelRuntime execution state requires recovery: %w", err)
		}
		if err := strictjson.RejectDuplicateKeys(document); err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(document))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&store.state); err != nil {
			return nil, err
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return nil, errors.New("ModelRuntime execution state has trailing data")
		}
		canonical, err := json.Marshal(store.state)
		if err != nil || !bytes.Equal(canonical, document) {
			return nil, errors.New("ModelRuntime execution state is not canonical")
		}
		store.stateInfo, store.stateDigest = stateInfo, sha256.Sum256(document)
	}
	upgrade := !config.Initialize && (config.UpgradeV4 && store.state.SchemaVersion == 4 || len(store.state.TerminalNonAdmissions) == 0 &&
		(config.UpgradeV2 && store.state.SchemaVersion == 2 && len(store.state.NonAdmissions) == 0 || config.UpgradeV3 && store.state.SchemaVersion == 3))
	if (store.state.SchemaVersion != 5 && !upgrade) || store.state.ID == uuid.Nil || store.state.ID != store.lockID || store.state.Scope != scope ||
		store.state.Root != executionIdentity(info) || store.state.Lock != executionIdentity(store.lockInfo) {
		return nil, errors.New("ModelRuntime execution state ownership or schema changed")
	}
	if err := store.validateProofs(); err != nil {
		return nil, err
	}
	if !config.Initialize {
		for _, record := range store.state.Executions {
			store.recoveryDrain = store.recoveryDrain || record.Drain == nil
		}
		if err := store.removeUnpublished(); err != nil {
			return nil, err
		}
		if err := store.recoverDurability(); err != nil {
			return nil, err
		}
	}
	if err := store.check(); err != nil {
		return nil, err
	}
	if upgrade {
		next := store.state
		next.SchemaVersion = 5
		if err := store.persist(next); err != nil {
			return nil, fmt.Errorf("upgrade ModelRuntime execution state: %w", err)
		}
	}
	success = true
	return store, nil
}

func (store *executionStateFile) validateProofs() error {
	state := store.state
	if state.Highest < 0 || state.Floor < 0 || (state.Highest == 0) != (len(state.Authority) == 0) ||
		(state.Floor == 0) != (len(state.Disposition) == 0) || len(state.Authority) > maxExecutionWireBytes {
		return errors.New("ModelRuntime execution state has invalid restriction witnesses")
	}
	if state.Highest > 0 {
		authority := &velav1.StageAuthority{}
		if err := proto.Unmarshal(state.Authority, authority); err != nil {
			return err
		}
		verified, err := store.scope.floor.validator.ValidateEnvelopeSignature(authority)
		if err != nil {
			return err
		}
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Authority)
		if err != nil || !bytes.Equal(wire, state.Authority) || verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 ||
			verified.Authority.GetExecutionSequence() != state.Highest {
			return errors.New("ModelRuntime execution watermark witness does not match")
		}
		if err := store.scope.matchRetainedExecutionScope(verified.Authority); err != nil {
			return err
		}
	}
	if state.Floor > 0 {
		disposition := &velav1.StageTerminalDisposition{}
		if err := proto.Unmarshal(state.Disposition, disposition); err != nil {
			return err
		}
		verified, err := store.scope.floor.validator.ValidateTerminalDispositionSignature(disposition)
		if err != nil {
			return err
		}
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Disposition)
		if err != nil || !bytes.Equal(wire, state.Disposition) || verified.Disposition.GetCutoff() != state.Floor {
			return errors.New("ModelRuntime execution floor witness does not match")
		}
		// Restoring an old restriction grants no authority to its historical runtimes.
		if err := store.scope.matchExecutionFloorScope(verified.Disposition); err != nil {
			return err
		}
	}
	if err := store.validateRetainedExecutions(); err != nil {
		return err
	}
	if err := store.validateNonAdmissions(); err != nil {
		return err
	}
	return store.validateTerminalNonAdmissions()
}

func (store *executionStateFile) saveHighest(authority *velav1.StageAuthority) error {
	state := store.state
	if len(state.Executions) >= maxRetainedExecutions {
		return ErrExecutionHistoryFull
	}
	if authority.GetExecutionSequence() <= state.Highest {
		return errors.New("ModelRuntime execution watermark cannot regress")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(authority)
	if err != nil || len(wire) > maxExecutionWireBytes {
		return errors.New("ModelRuntime execution authority exceeds its persistence bound")
	}
	state.Highest, state.Authority = authority.GetExecutionSequence(), wire
	state.Executions = append(slices.Clone(state.Executions), retainedExecution{Authority: bytes.Clone(wire),
		Candidates: &executionDiskCandidates{Accepted: bytes.Clone(wire)}})
	return store.persist(state)
}

func (store *executionStateFile) saveFloor(disposition *velav1.StageTerminalDisposition) error {
	state := store.state
	if disposition.GetCutoff() <= state.Floor {
		return errors.New("ModelRuntime execution floor cannot regress")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(disposition)
	if err != nil {
		return err
	}
	state.Floor, state.Disposition = disposition.GetCutoff(), wire
	return store.persist(state)
}

func (store *executionStateFile) check() error {
	if store.root == nil || store.lock == nil {
		return errors.New("ModelRuntime execution state is closed")
	}
	info, err := os.Lstat(store.path)
	if err != nil || !os.SameFile(info, store.rootInfo) || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("ModelRuntime execution state directory binding changed")
	}
	lockInfo, err := store.root.Lstat(executionStateLockName)
	if err != nil || !os.SameFile(lockInfo, store.lockInfo) || !executionPrivateFile(lockInfo, 36, false) {
		return errors.New("ModelRuntime execution state lock binding changed")
	}
	id, err := readExecutionLockID(store.lock)
	if err != nil || id != store.lockID {
		return errors.New("ModelRuntime execution state lock identity changed")
	}
	if store.stateInfo != nil {
		document, info, err := readExecutionStateFile(store.root)
		if err != nil || !os.SameFile(info, store.stateInfo) || sha256.Sum256(document) != store.stateDigest {
			return errors.New("ModelRuntime execution state content or binding changed")
		}
	}
	return nil
}

func (store *executionStateFile) persist(state executionDiskState) error {
	if err := store.check(); err != nil {
		return err
	}
	document, err := json.Marshal(state)
	if err != nil || len(document) > maxExecutionStateBytes {
		return errors.New("ModelRuntime execution state exceeds its bound")
	}
	name := ".execution-admission-" + state.ID.String() + "-" + uuid.NewString() + ".tmp"
	file, err := store.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = store.root.Remove(name) }()
	if _, err := file.Write(document); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	publishedInfo, err := file.Stat()
	if err != nil || !executionPrivateFile(publishedInfo, maxExecutionStateBytes, false) {
		return errors.New("ModelRuntime execution temporary state binding changed")
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := store.root.Rename(name, executionStateName); err != nil {
		return err
	}
	if err := store.syncDirectory(store.root); err != nil {
		return err
	}
	info, err := store.root.Lstat(executionStateName)
	if err != nil || !os.SameFile(info, publishedInfo) {
		return errors.New("ModelRuntime execution state was replaced after rename")
	}
	store.stateInfo, store.stateDigest = info, sha256.Sum256(document)
	if err := store.check(); err != nil {
		return err
	}
	store.state = state
	return nil
}

// Under the lifetime lock, files tagged with this journal's UUID can only be
// unpublished writes from an interrupted transaction. They granted no admission.
func (store *executionStateFile) removeUnpublished() error {
	directory, err := store.root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	prefix := ".execution-admission-" + store.state.ID.String() + "-"
	removed := false
	for {
		names, err := directory.Readdirnames(64)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for _, name := range names {
			if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".tmp") {
				continue
			}
			suffix := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".tmp")
			id, parseErr := uuid.Parse(suffix)
			if parseErr != nil || id == uuid.Nil || id.String() != suffix {
				continue
			}
			info, statErr := store.root.Lstat(name)
			if statErr != nil || !executionPrivateFile(info, maxExecutionStateBytes, true) {
				return errors.New("ModelRuntime unpublished execution state is untrusted")
			}
			if err := store.root.Remove(name); err != nil {
				return err
			}
			removed = true
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	if removed {
		return store.syncDirectory(store.root)
	}
	return nil
}

// A previous process may have observed Rename without completing directory
// fsync. Reestablish durability before restored state can back a checkpoint.
func (store *executionStateFile) recoverDurability() error {
	file, err := store.root.OpenFile(executionStateName, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !os.SameFile(info, store.stateInfo) || !executionPrivateFile(info, maxExecutionStateBytes, false) {
		return errors.New("ModelRuntime execution state changed before recovery sync")
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := store.lock.Sync(); err != nil {
		return err
	}
	return store.syncDirectory(store.root)
}

func readExecutionLockID(lock *os.File) (uuid.UUID, error) {
	var value [36]byte
	if _, err := lock.ReadAt(value[:], 0); err != nil {
		return uuid.Nil, err
	}
	id, err := uuid.Parse(string(value[:]))
	if err != nil || id == uuid.Nil || id.String() != string(value[:]) {
		return uuid.Nil, errors.New("ModelRuntime execution lock identity is invalid")
	}
	return id, nil
}

func syncExecutionStateDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (store *executionStateFile) close() error {
	var err error
	if store.lock != nil {
		err = errors.Join(syscall.Flock(int(store.lock.Fd()), syscall.LOCK_UN), store.lock.Close())
		store.lock = nil
	}
	if store.root != nil {
		err = errors.Join(err, store.root.Close())
		store.root = nil
	}
	return err
}

func readExecutionStateFile(root *os.Root) ([]byte, os.FileInfo, error) {
	file, err := root.OpenFile(executionStateName, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !executionPrivateFile(info, maxExecutionStateBytes, false) {
		return nil, nil, errors.New("ModelRuntime execution state is not private bounded regular data")
	}
	document, err := io.ReadAll(io.LimitReader(file, maxExecutionStateBytes+1))
	if err != nil || len(document) == 0 || len(document) > maxExecutionStateBytes {
		return nil, nil, errors.New("ModelRuntime execution state exceeds its bound")
	}
	current, err := root.Lstat(executionStateName)
	if err != nil || !os.SameFile(current, info) {
		return nil, nil, errors.New("ModelRuntime execution state changed while reading")
	}
	return document, info, nil
}

func executionPrivateFile(info os.FileInfo, limit int64, empty bool) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > limit || (!empty && info.Size() <= 0) {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1 && int(stat.Uid) == os.Geteuid()
}

func executionIdentity(info os.FileInfo) executionFileIdentity {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return executionFileIdentity{}
	}
	return executionFileIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}
}

func executionStateDirectoryEmpty(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	names, err := directory.Readdirnames(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	for _, name := range names {
		if name != executionStateLockName {
			return errors.New("ModelRuntime execution state cannot initialize a reused directory")
		}
	}
	return nil
}
