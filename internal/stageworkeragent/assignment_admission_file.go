package stageworkeragent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	maxAdmissionAuthorityBytes = 64 << 10
	maxAdmissionStateBytes     = 16 << 20
	admissionStateFile         = "assignment-admission.json"
	admissionLockFile          = "assignment-admission.lock"
	admissionRootMarker        = ".vela-assignment-admission"
)

type assignmentAdmissionEntry struct {
	// Schema 6 adds optional diagnostics; execution identity remains signed.
	OriginTraceParent string                          `json:"origin_trace_parent,omitempty"`
	AcquireCommandID  uuid.UUID                       `json:"acquire_command_id"`
	Identity          [sha256.Size]byte               `json:"execution_identity"`
	Phase             AssignmentAdmissionPhase        `json:"phase"`
	OriginalWire      []byte                          `json:"original_authority"`
	LatestWire        []byte                          `json:"latest_authority"`
	InputDrain        *AssignmentInputDrainCheckpoint `json:"input_drain,omitempty"`
}

type admissionDirectoryIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type assignmentAdmissionState struct {
	SchemaVersion       int                           `json:"schema_version"`
	Scope               []byte                        `json:"scope,omitempty"`
	ID                  uuid.UUID                     `json:"journal_id"`
	WorkerInstanceID    uuid.UUID                     `json:"worker_instance_id"`
	WorkerInstanceEpoch int64                         `json:"worker_instance_epoch"`
	WorkerMemberID      uuid.UUID                     `json:"worker_member_id"`
	Directories         [3]admissionDirectoryIdentity `json:"directories"`
	Lock                admissionDirectoryIdentity    `json:"lock"`
	MaxRecords          int                           `json:"max_records"`
	HistoryBase         int64                         `json:"history_base,omitempty"`
	Watermark           int64                         `json:"watermark"`
	Floor               int64                         `json:"floor"`
	FloorWire           []byte                        `json:"floor_disposition"`
	Latest              *assignmentAdmissionEntry     `json:"latest"`
	Pending             []assignmentAdmissionEntry    `json:"pending"`
	Retirements         []terminalRetirementEntry     `json:"retirements,omitempty"`
	HistoryCheckpoint   *AssignmentHistoryCheckpoint  `json:"history_checkpoint,omitempty"`
	HistoryCutoffs      []AssignmentHistoryCutoff     `json:"history_cutoffs,omitempty"`
}

type assignmentAdmissionFiles struct {
	roots         [3]*os.Root
	paths         [3]string
	infos         [3]os.FileInfo
	lock          *os.File
	lockInfo      os.FileInfo
	stateInfo     os.FileInfo
	id            uuid.UUID
	syncDirectory func(*os.Root) error
}

func openAssignmentAdmissionFiles(config AssignmentAdmissionConfig, scope [sha256.Size]byte) (*assignmentAdmissionFiles, assignmentAdmissionState, error) {
	files := &assignmentAdmissionFiles{paths: [3]string{config.Directory, config.InputRoot, config.OutputRoot}, syncDirectory: syncAdmissionDirectory}
	success := false
	defer func() {
		if !success {
			_ = files.close()
		}
	}()
	for i, directory := range files.paths {
		if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
			return nil, assignmentAdmissionState{}, errors.New("assignment admission requires canonical absolute directories")
		}
		info, err := os.Lstat(directory)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
			return nil, assignmentAdmissionState{}, errors.New("assignment admission directory is missing or untrusted")
		}
		root, err := securefile.OpenTrustedRoot(directory)
		if err != nil {
			return nil, assignmentAdmissionState{}, err
		}
		files.roots[i] = root
		opened, err := root.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			return nil, assignmentAdmissionState{}, errors.New("assignment admission directory changed while opening")
		}
		files.infos[i] = opened
		for j := 0; j < i; j++ {
			if os.SameFile(files.infos[j], opened) || admissionPathsOverlap(files.paths[j], directory) {
				return nil, assignmentAdmissionState{}, errors.New("assignment admission directories must not overlap")
			}
		}
	}
	flags := os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	if config.Initialize {
		flags |= os.O_CREATE | os.O_EXCL
	}
	lock, err := files.roots[0].OpenFile(admissionLockFile, flags, 0o600)
	if err != nil {
		return nil, assignmentAdmissionState{}, err
	}
	files.lock = lock
	files.lockInfo, err = lock.Stat()
	if err != nil || !admissionPrivateFile(files.lockInfo, 1, true) {
		return nil, assignmentAdmissionState{}, errors.New("assignment admission lock is untrusted")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, assignmentAdmissionState{}, fmt.Errorf("lock assignment admission: %w", err)
	}
	var state assignmentAdmissionState
	if config.Initialize {
		// Explicit bootstrap still cannot reuse existing state or content roots.
		for i, root := range files.roots {
			exception := ""
			if i == 0 {
				exception = admissionLockFile
			}
			if err := admissionDirectoryEmpty(root, exception); err != nil {
				return nil, state, err
			}
		}
		state = assignmentAdmissionState{SchemaVersion: 5, Scope: bytes.Clone(scope[:]), ID: uuid.New(), WorkerInstanceID: config.WorkerInstanceID, WorkerInstanceEpoch: config.WorkerInstanceEpoch, WorkerMemberID: config.WorkerMemberID, MaxRecords: config.MaxRecords}
		for i, info := range files.infos {
			state.Directories[i] = admissionFileIdentity(info)
		}
		state.Lock = admissionFileIdentity(files.lockInfo)
		files.id = state.ID
		for _, root := range files.roots[1:] {
			marker, err := root.OpenFile(admissionRootMarker, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
			if err != nil {
				return nil, state, err
			}
			_, writeErr := marker.WriteString(state.ID.String())
			syncErr := marker.Sync()
			closeErr := marker.Close()
			if err := errors.Join(writeErr, syncErr, closeErr, syncAdmissionDirectory(root)); err != nil {
				return nil, state, err
			}
		}
		if err := files.persist(state); err != nil {
			return nil, state, err
		}
	} else {
		document, info, err := readAdmissionFile(files.roots[0], admissionStateFile, maxAdmissionStateBytes)
		if err != nil {
			return nil, state, fmt.Errorf("assignment admission state requires recovery: %w", err)
		}
		if strictjson.RejectDuplicateKeys(document) != nil {
			return nil, state, errors.New("assignment admission state contains duplicate fields")
		}
		decoder := json.NewDecoder(bytes.NewReader(document))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&state); err != nil {
			return nil, state, errors.New("assignment admission state is invalid")
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return nil, state, errors.New("assignment admission state has trailing data")
		}
		canonical, err := json.Marshal(state)
		if err != nil || !bytes.Equal(canonical, document) {
			return nil, state, errors.New("assignment admission state is not canonical")
		}
		files.stateInfo, files.id = info, state.ID
	}
	if state.WorkerInstanceID != config.WorkerInstanceID || state.WorkerInstanceEpoch != config.WorkerInstanceEpoch || state.WorkerMemberID != config.WorkerMemberID ||
		state.MaxRecords != config.MaxRecords || state.ID == uuid.Nil || state.Lock != admissionFileIdentity(files.lockInfo) {
		return nil, state, errors.New("assignment admission state belongs to another Worker or configuration")
	}
	for i, info := range files.infos {
		if state.Directories[i] != admissionFileIdentity(info) {
			return nil, state, errors.New("assignment admission root ownership changed")
		}
	}
	if err := files.validateBinding(); err != nil {
		return nil, state, err
	}
	success = true
	return files, state, nil
}

func (files *assignmentAdmissionFiles) validateBinding() error {
	for i, root := range files.roots {
		if root == nil {
			return ErrAdmissionClosed
		}
		current, err := os.Lstat(files.paths[i])
		if err != nil || !os.SameFile(current, files.infos[i]) || !current.IsDir() || current.Mode().Perm()&0o022 != 0 {
			return errors.New("assignment admission directory binding changed")
		}
		if i > 0 {
			marker, _, err := readAdmissionFile(root, admissionRootMarker, 36)
			if err != nil || string(marker) != files.id.String() {
				return errors.New("assignment admission root marker changed")
			}
		}
	}
	lockInfo, err := files.roots[0].Lstat(admissionLockFile)
	if err != nil || !os.SameFile(lockInfo, files.lockInfo) || !admissionPrivateFile(lockInfo, 1, true) {
		return errors.New("assignment admission lock binding changed")
	}
	if files.stateInfo != nil {
		info, err := files.roots[0].Lstat(admissionStateFile)
		if err != nil || !os.SameFile(info, files.stateInfo) || !admissionPrivateFile(info, maxAdmissionStateBytes, false) ||
			info.Size() != files.stateInfo.Size() || !info.ModTime().Equal(files.stateInfo.ModTime()) {
			return errors.New("assignment admission state binding changed")
		}
	}
	return nil
}

func (files *assignmentAdmissionFiles) persist(state assignmentAdmissionState) error {
	if err := files.validateBinding(); err != nil {
		return err
	}
	document, err := json.Marshal(state)
	if err != nil || len(document) > maxAdmissionStateBytes {
		return errors.New("assignment admission state exceeds its bound")
	}
	name := ".assignment-admission-" + uuid.NewString() + ".tmp"
	temporary, err := files.roots[0].OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = temporary.Close(); _ = files.roots[0].Remove(name) }()
	if _, err := temporary.Write(document); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := files.roots[0].Rename(name, admissionStateFile); err != nil {
		return err
	}
	if err := files.syncDirectory(files.roots[0]); err != nil {
		return err
	}
	info, err := files.roots[0].Lstat(admissionStateFile)
	if err != nil {
		return err
	}
	files.stateInfo = info
	return files.validateBinding()
}

func (files *assignmentAdmissionFiles) recoverDurability() error {
	if err := files.validateBinding(); err != nil {
		return err
	}
	file, err := files.roots[0].OpenFile(admissionStateFile, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	info, statErr := file.Stat()
	if statErr != nil || !os.SameFile(info, files.stateInfo) {
		_ = file.Close()
		return errors.New("assignment admission state changed during recovery")
	}
	if err := errors.Join(file.Sync(), file.Close(), files.lock.Sync(), files.syncDirectory(files.roots[0])); err != nil {
		return err
	}
	return files.validateBinding()
}

func (files *assignmentAdmissionFiles) close() error {
	if files == nil {
		return nil
	}
	var result error
	if files.lock != nil {
		result = errors.Join(result, syscall.Flock(int(files.lock.Fd()), syscall.LOCK_UN), files.lock.Close())
		files.lock = nil
	}
	for i, root := range files.roots {
		if root != nil {
			result = errors.Join(result, root.Close())
			files.roots[i] = nil
		}
	}
	return result
}

func readAdmissionFile(root *os.Root, name string, limit int64) ([]byte, os.FileInfo, error) {
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !admissionPrivateFile(info, limit, false) {
		return nil, nil, errors.New("assignment admission file is not private bounded regular data")
	}
	document, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(document) == 0 || int64(len(document)) > limit {
		return nil, nil, errors.New("assignment admission file exceeds its bound")
	}
	current, err := root.Lstat(name)
	if err != nil || !os.SameFile(info, current) {
		return nil, nil, errors.New("assignment admission file changed while reading")
	}
	return document, info, nil
}

func admissionPrivateFile(info os.FileInfo, limit int64, empty bool) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > limit || (!empty && info.Size() <= 0) {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1 && int(stat.Uid) == os.Geteuid()
}

func admissionDirectoryEmpty(root *os.Root, exception string) error {
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
		if name != exception {
			return errors.New("assignment admission cannot initialize reused directories")
		}
	}
	return nil
}

func syncAdmissionDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func admissionFileIdentity(info fs.FileInfo) admissionDirectoryIdentity {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return admissionDirectoryIdentity{}
	}
	return admissionDirectoryIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}
}

func admissionPathsOverlap(a, b string) bool {
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err != nil || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
			return true
		}
	}
	return false
}

func cloneAdmissionState(state assignmentAdmissionState) assignmentAdmissionState {
	state.Scope = bytes.Clone(state.Scope)
	state.Pending = slices.Clone(state.Pending)
	state.Retirements = slices.Clone(state.Retirements)
	state.HistoryCutoffs = slices.Clone(state.HistoryCutoffs)
	if state.Latest != nil {
		latest := *state.Latest
		state.Latest = &latest
	}
	return state
}
