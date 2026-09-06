package workerbootstrap

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
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	scratchRoot = iota
	operationRoot
	workerRoot
	runtimeRoot
	inputRoot
	outputRoot
	rootCount
)

const (
	operationName = "operation.json"
	pairName      = "pair.json"
	maxStateBytes = 16 << 10
)

var rootNames = [rootCount]string{".", "bootstrap", "worker-admission", "runtime-admission", "inputs", "outputs"}

type fileIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type operationBinding struct {
	BundleDigest [sha256.Size]byte       `json:"bundle_digest"`
	LaunchDigest [sha256.Size]byte       `json:"launch_digest"`
	Node         string                  `json:"node"`
	Actor        string                  `json:"actor"`
	MaxRecords   int                     `json:"max_records"`
	Scratch      string                  `json:"scratch"`
	Roots        [rootCount]fileIdentity `json:"roots"`
	Lock         fileIdentity            `json:"lock"`
}

type operationRecord struct {
	SchemaVersion int              `json:"schema_version"`
	RequestID     uuid.UUID        `json:"request_id"`
	Binding       operationBinding `json:"binding"`
}

type journalPair struct {
	RequestID    uuid.UUID         `json:"request_id"`
	WorkerID     uuid.UUID         `json:"worker_journal_id"`
	WorkerScope  [sha256.Size]byte `json:"worker_scope"`
	RuntimeID    uuid.UUID         `json:"runtime_journal_id"`
	RuntimeScope [sha256.Size]byte `json:"runtime_scope"`
}

type operationState struct {
	roots          [rootCount]*os.Root
	paths          [rootCount]string
	infos          [rootCount]os.FileInfo
	lock           *os.File
	lockInfo       os.FileInfo
	operation      operationRecord
	operationBytes []byte
	pairInfo       os.FileInfo
	pairBytes      []byte
	fresh          bool
}

func openOperation(p preparation, allowCreate bool) (*operationState, error) {
	state := &operationState{}
	success := false
	defer func() {
		if !success {
			_ = state.close()
		}
	}()
	for i, name := range rootNames {
		path := filepath.Join(p.config.ScratchDirectory, name)
		info, err := os.Lstat(path)
		if err != nil || !privateDirectory(info) {
			return nil, errors.New("worker bootstrap requires existing owner-only directories")
		}
		state.paths[i], state.infos[i] = path, info
		root, err := securefile.OpenTrustedRoot(path)
		if err != nil {
			return nil, err
		}
		state.roots[i] = root
		opened, err := root.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			return nil, errors.New("worker bootstrap directory changed while opening")
		}
		for j := 0; j < i; j++ {
			if os.SameFile(state.infos[j], opened) {
				return nil, errors.New("worker bootstrap directories must be distinct")
			}
		}
	}
	if _, err := state.roots[operationRoot].Lstat(operationName); errors.Is(err, os.ErrNotExist) {
		if !allowCreate {
			return nil, fmt.Errorf("%w: retained operation is missing", ErrIncomplete)
		}
		if err := state.requireUnusedRoots(false); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	flags := os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	var lock *os.File
	var err error
	if allowCreate {
		lock, err = state.roots[operationRoot].OpenFile(operationName, flags|os.O_CREATE|os.O_EXCL, 0o600)
		state.fresh = err == nil
	}
	if !allowCreate || errors.Is(err, os.ErrExist) {
		lock, err = state.roots[operationRoot].OpenFile(operationName, flags, 0)
	}
	if err != nil {
		return nil, err
	}
	state.lock = lock
	state.lockInfo, err = lock.Stat()
	if err != nil || !privateFile(state.lockInfo) {
		return nil, errors.New("worker bootstrap operation is not private regular data")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock Worker bootstrap operation: %w", err)
	}
	binding := operationBinding{BundleDigest: p.digest, LaunchDigest: p.launchID,
		Node: p.config.NodeIdentity, Actor: p.config.ActorIdentity, MaxRecords: p.config.MaxRecords,
		Scratch: p.config.ScratchDirectory, Lock: identity(state.lockInfo)}
	for i, info := range state.infos {
		binding.Roots[i] = identity(info)
	}
	if state.fresh {
		if err := state.requireUnusedRoots(true); err != nil {
			return nil, err
		}
		state.operation = operationRecord{SchemaVersion: 1, RequestID: uuid.New(), Binding: binding}
		state.operationBytes, err = json.Marshal(state.operation)
		if err != nil {
			return nil, err
		}
		if _, err := lock.Write(state.operationBytes); err != nil {
			return nil, err
		}
	} else {
		state.operationBytes, err = readDocument(lock, &state.operation)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid local operation: %w", ErrIncomplete, err)
		}
		if state.operation.SchemaVersion != 1 || state.operation.RequestID == uuid.Nil || state.operation.Binding != binding {
			return nil, errors.New("worker bootstrap operation belongs to different roots or configuration")
		}
	}
	// Reconfirm an uncertain previous fsync before using retained identity.
	if err := errors.Join(lock.Sync(), syncRoot(state.roots[operationRoot])); err != nil {
		return nil, err
	}
	if err := state.validate(); err != nil {
		return nil, err
	}
	success = true
	return state, nil
}

func (state *operationState) requireUnusedRoots(operationExists bool) error {
	for i, root := range state.roots {
		directory, err := root.Open(".")
		if err != nil {
			return err
		}
		names, readErr := directory.Readdirnames(rootCount + 1)
		closeErr := directory.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		allowed := []string(nil)
		switch i {
		case scratchRoot:
			allowed = rootNames[1:]
		case operationRoot:
			if operationExists {
				allowed = []string{operationName}
			}
		}
		if len(names) != len(allowed) {
			return errors.New("worker bootstrap first use requires unused local roots")
		}
		for _, name := range names {
			if !slices.Contains(allowed, name) {
				return errors.New("worker bootstrap first use found unrecognized history")
			}
		}
	}
	return nil
}

func (state *operationState) validate() error {
	for i, root := range state.roots {
		current, err := os.Lstat(state.paths[i])
		if err != nil || !privateDirectory(current) || !os.SameFile(current, state.infos[i]) {
			return errors.New("worker bootstrap directory binding changed")
		}
		opened, err := root.Stat(".")
		if err != nil || !privateDirectory(opened) || !os.SameFile(opened, current) {
			return errors.New("worker bootstrap held directory binding changed")
		}
	}
	current, err := state.roots[operationRoot].Lstat(operationName)
	if err != nil || !privateFile(current) || !os.SameFile(current, state.lockInfo) {
		return errors.New("worker bootstrap operation binding changed")
	}
	var operation operationRecord
	encoded, err := readDocument(state.lock, &operation)
	if err != nil || !bytes.Equal(encoded, state.operationBytes) {
		return errors.New("worker bootstrap operation changed")
	}
	if state.pairInfo != nil {
		file, err := state.openPair()
		if err != nil {
			return err
		}
		var pair journalPair
		encoded, readErr := readDocument(file, &pair)
		if err := errors.Join(readErr, file.Close()); err != nil {
			return err
		}
		if !bytes.Equal(encoded, state.pairBytes) {
			return errors.New("worker bootstrap recorded pair changed")
		}
	}
	return nil
}

func (state *operationState) writePair(pair journalPair) error {
	if err := state.validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(pair)
	if err != nil {
		return err
	}
	file, err := state.roots[operationRoot].OpenFile(pairName, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	info, infoErr := file.Stat()
	_, writeErr := file.Write(encoded)
	if err := errors.Join(infoErr, writeErr, file.Sync(), file.Close(), syncRoot(state.roots[operationRoot])); err != nil {
		return err
	}
	state.pairInfo, state.pairBytes = info, encoded
	return state.validate()
}

func (state *operationState) openPair() (*os.File, error) {
	file, err := state.roots[operationRoot].OpenFile(pairName, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	pathInfo, pathErr := state.roots[operationRoot].Lstat(pairName)
	if err != nil || pathErr != nil || !privateFile(info) || !os.SameFile(info, pathInfo) ||
		state.pairInfo != nil && !os.SameFile(info, state.pairInfo) {
		_ = file.Close()
		return nil, errors.New("worker bootstrap pair is untrusted or replaced")
	}
	state.pairInfo = info
	return file, nil
}

func (state *operationState) readPair() (journalPair, error) {
	file, err := state.openPair()
	if err != nil {
		return journalPair{}, err
	}
	var pair journalPair
	encoded, readErr := readDocument(file, &pair)
	if err := errors.Join(readErr, file.Sync(), file.Close(), syncRoot(state.roots[operationRoot])); err != nil {
		return journalPair{}, err
	}
	if pair.RequestID != state.operation.RequestID || pair.WorkerID == uuid.Nil || pair.RuntimeID == uuid.Nil ||
		pair.WorkerID == pair.RuntimeID || pair.WorkerScope == ([sha256.Size]byte{}) || pair.RuntimeScope == ([sha256.Size]byte{}) ||
		state.pairBytes != nil && !bytes.Equal(encoded, state.pairBytes) {
		return journalPair{}, errors.New("worker bootstrap pair identity is invalid")
	}
	state.pairBytes = encoded
	return pair, nil
}

func readDocument(file *os.File, value any) ([]byte, error) {
	info, err := file.Stat()
	if err != nil || !privateFile(info) || info.Size() == 0 {
		return nil, errors.New("worker bootstrap state is empty or untrusted")
	}
	encoded, err := io.ReadAll(io.NewSectionReader(file, 0, maxStateBytes+1))
	if err != nil || len(encoded) > maxStateBytes {
		return nil, errors.New("worker bootstrap state exceeds its bound")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("worker bootstrap state has trailing data")
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(encoded, canonical) {
		return nil, errors.New("worker bootstrap state is not canonical")
	}
	return encoded, nil
}

func (state *operationState) close() error {
	var err error
	if state.lock != nil {
		err = errors.Join(syscall.Flock(int(state.lock.Fd()), syscall.LOCK_UN), state.lock.Close())
	}
	for _, root := range state.roots {
		if root != nil {
			err = errors.Join(err, root.Close())
		}
	}
	return err
}

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func privateDirectory(info os.FileInfo) bool {
	if info == nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func privateFile(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > maxStateBytes {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid()) && stat.Nlink == 1
}

func identity(info os.FileInfo) fileIdentity {
	stat := info.Sys().(*syscall.Stat_t)
	return fileIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}
}
