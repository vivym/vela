package nodeagent

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
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	workerInstanceEpochStateName  = "worker-instance-epochs.json"
	workerInstanceEpochLockName   = "worker-instance-epochs.lock"
	maxWorkerInstanceEpochBytes   = 1 << 20
	maxWorkerInstanceEpochDevices = 1024
)

type FileWorkerInstanceEpochStoreConfig struct {
	Directory    string
	NodeIdentity string
	BootIDPath   string
}

type WorkerInstanceDeviceEpochBinding struct {
	GPUUUID           string
	PCIBDF            string
	AttestationDigest string
}

type WorkerInstanceEpochSnapshot struct {
	NodeEpoch         int64
	AgentSessionEpoch int64
	DeviceEpochs      map[string]int64
}

type WorkerInstanceEpochStore interface {
	BindWorkerInstanceDevices(
		context.Context,
		[]WorkerInstanceDeviceEpochBinding,
	) (WorkerInstanceEpochSnapshot, error)
}

type WorkerInstanceObservationSequencer interface {
	NextWorkerInstanceObservationSequence(context.Context, uuid.UUID) (int64, error)
}

type fileWorkerInstanceDeviceEpoch struct {
	PCIBDF            string `json:"pci_bdf"`
	AttestationDigest string `json:"attestation_digest"`
	Epoch             int64  `json:"epoch"`
}

type fileWorkerInstanceEpochState struct {
	SchemaVersion        int                                      `json:"schema_version"`
	NodeIdentity         string                                   `json:"node_identity"`
	BootID               string                                   `json:"boot_id"`
	NodeEpoch            int64                                    `json:"node_epoch"`
	AgentSessionEpoch    int64                                    `json:"agent_session_epoch"`
	Devices              map[string]fileWorkerInstanceDeviceEpoch `json:"devices"`
	ObservationSequences map[string]int64                         `json:"observation_sequences"`
}

type FileWorkerInstanceEpochStore struct {
	directory string
	statePath string
	lock      *os.File
	state     fileWorkerInstanceEpochState
	mu        sync.Mutex
}

func NewFileWorkerInstanceEpochStore(
	config FileWorkerInstanceEpochStoreConfig,
) (*FileWorkerInstanceEpochStore, error) {
	directory := filepath.Clean(config.Directory)
	bootIDPath := filepath.Clean(config.BootIDPath)
	if !filepath.IsAbs(directory) || directory != config.Directory ||
		!filepath.IsAbs(bootIDPath) || bootIDPath != config.BootIDPath ||
		!validText(config.NodeIdentity, maxIdentityText) {
		return nil, errors.New("WorkerInstance epoch store configuration is invalid")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create WorkerInstance epoch directory: %w", err)
	}
	if err := securefile.ValidateDirectory(directory); err != nil {
		return nil, errors.New("WorkerInstance epoch directory does not satisfy the security contract")
	}
	bootID, err := readBootID(bootIDPath)
	if err != nil {
		return nil, err
	}
	lock, err := securefile.OpenPrivateState(filepath.Join(directory, workerInstanceEpochLockName))
	if err != nil {
		return nil, fmt.Errorf("open WorkerInstance epoch lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("lock WorkerInstance epoch state: %w", err)
	}
	store := &FileWorkerInstanceEpochStore{
		directory: directory,
		statePath: filepath.Join(directory, workerInstanceEpochStateName),
		lock:      lock,
	}
	if err := store.load(config.NodeIdentity, bootID); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (store *FileWorkerInstanceEpochStore) load(nodeIdentity string, bootID string) error {
	content, err := securefile.Read(store.statePath, maxWorkerInstanceEpochBytes, true)
	switch {
	case errors.Is(err, os.ErrNotExist):
		store.state = fileWorkerInstanceEpochState{
			SchemaVersion: 1, NodeIdentity: nodeIdentity, BootID: bootID,
			NodeEpoch: 1, AgentSessionEpoch: 1,
			Devices:              make(map[string]fileWorkerInstanceDeviceEpoch),
			ObservationSequences: make(map[string]int64),
		}
		return store.persist(store.state)
	case err != nil:
		return fmt.Errorf("read WorkerInstance epoch state: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(content); err != nil {
		return fmt.Errorf("decode WorkerInstance epoch state: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var state fileWorkerInstanceEpochState
	if err := decoder.Decode(&state); err != nil {
		return errors.New("WorkerInstance epoch state is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) ||
		!validFileWorkerInstanceEpochState(state) {
		return errors.New("WorkerInstance epoch state is invalid")
	}
	if state.NodeIdentity != nodeIdentity {
		return errors.New("WorkerInstance epoch state belongs to another Node")
	}
	if state.AgentSessionEpoch == math.MaxInt64 {
		return errors.New("WorkerInstance Agent session epoch is exhausted")
	}
	state.AgentSessionEpoch++
	if state.BootID != bootID {
		if state.NodeEpoch == math.MaxInt64 {
			return errors.New("WorkerInstance Node epoch is exhausted")
		}
		state.NodeEpoch++
		state.BootID = bootID
		for gpuUUID, device := range state.Devices {
			if device.Epoch == math.MaxInt64 {
				return errors.New("WorkerInstance Device epoch is exhausted")
			}
			device.Epoch++
			state.Devices[gpuUUID] = device
		}
	}
	if err := store.persist(state); err != nil {
		return err
	}
	store.state = state
	return nil
}

func (store *FileWorkerInstanceEpochStore) NextWorkerInstanceObservationSequence(
	ctx context.Context,
	workerInstanceID uuid.UUID,
) (int64, error) {
	if store == nil || workerInstanceID == uuid.Nil {
		return 0, errors.New("WorkerInstance observation sequencer is not configured")
	}
	if err := contextError(ctx); err != nil {
		return 0, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.lock == nil {
		return 0, errors.New("WorkerInstance observation sequencer is not configured")
	}
	state := cloneFileWorkerInstanceEpochState(store.state)
	key := workerInstanceID.String()
	current := state.ObservationSequences[key]
	if current == math.MaxInt64 {
		return 0, errors.New("WorkerInstance observation sequence is exhausted")
	}
	next := current + 1
	state.ObservationSequences[key] = next
	if !validFileWorkerInstanceEpochState(state) {
		return 0, errors.New("WorkerInstance observation sequence transition is invalid")
	}
	if err := store.persist(state); err != nil {
		return 0, err
	}
	store.state = state
	return next, nil
}

func (store *FileWorkerInstanceEpochStore) BindWorkerInstanceDevices(
	ctx context.Context,
	bindings []WorkerInstanceDeviceEpochBinding,
) (WorkerInstanceEpochSnapshot, error) {
	if store == nil {
		return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance epoch store is not configured")
	}
	if err := contextError(ctx); err != nil {
		return WorkerInstanceEpochSnapshot{}, err
	}
	if len(bindings) == 0 || len(bindings) > maxWorkerInstanceEpochDevices {
		return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance Device epoch bindings are invalid")
	}
	seenGPU := make(map[string]struct{}, len(bindings))
	seenBDF := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if !validGPUUUID(binding.GPUUUID) || !validPCIBDF(binding.PCIBDF) ||
			!validDigestHex(binding.AttestationDigest) {
			return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance Device epoch binding is invalid")
		}
		if _, duplicate := seenGPU[binding.GPUUUID]; duplicate {
			return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance GPU epoch binding is duplicated")
		}
		if _, duplicate := seenBDF[binding.PCIBDF]; duplicate {
			return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance PCI epoch binding is duplicated")
		}
		seenGPU[binding.GPUUUID] = struct{}{}
		seenBDF[binding.PCIBDF] = struct{}{}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.lock == nil {
		return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance epoch store is not configured")
	}
	state := cloneFileWorkerInstanceEpochState(store.state)
	changed := false
	deviceEpochs := make(map[string]int64, len(bindings))
	originalByBDF := make(map[string]string, len(state.Devices))
	for gpuUUID, device := range state.Devices {
		originalByBDF[device.PCIBDF] = gpuUUID
	}
	updates := make(map[string]fileWorkerInstanceDeviceEpoch, len(bindings))
	displaced := make(map[string]struct{})
	for _, binding := range bindings {
		device, exists := state.Devices[binding.GPUUUID]
		switch {
		case exists && (device.PCIBDF != binding.PCIBDF ||
			device.AttestationDigest != binding.AttestationDigest):
			if device.Epoch == math.MaxInt64 {
				return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance Device epoch is exhausted")
			}
			device.PCIBDF = binding.PCIBDF
			device.AttestationDigest = binding.AttestationDigest
			device.Epoch++
			changed = true
		case !exists:
			predecessorUUID, replacesSlot := originalByBDF[binding.PCIBDF]
			if replacesSlot {
				predecessor := state.Devices[predecessorUUID]
				if predecessor.Epoch == math.MaxInt64 {
					return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance Device epoch is exhausted")
				}
				device = fileWorkerInstanceDeviceEpoch{
					PCIBDF: binding.PCIBDF, AttestationDigest: binding.AttestationDigest,
					Epoch: predecessor.Epoch + 1,
				}
			} else {
				device = fileWorkerInstanceDeviceEpoch{
					PCIBDF: binding.PCIBDF, AttestationDigest: binding.AttestationDigest, Epoch: 1,
				}
			}
			changed = true
		}
		if predecessorUUID, occupied := originalByBDF[binding.PCIBDF]; occupied && predecessorUUID != binding.GPUUUID {
			if _, remainsInSet := seenGPU[predecessorUUID]; !remainsInSet {
				displaced[predecessorUUID] = struct{}{}
			}
		}
		updates[binding.GPUUUID] = device
		deviceEpochs[binding.GPUUUID] = device.Epoch
	}
	for gpuUUID := range displaced {
		delete(state.Devices, gpuUUID)
		changed = true
	}
	for gpuUUID, device := range updates {
		state.Devices[gpuUUID] = device
	}
	if !validFileWorkerInstanceEpochState(state) {
		return WorkerInstanceEpochSnapshot{}, errors.New("WorkerInstance Device epoch state transition is invalid")
	}
	if changed {
		if err := store.persist(state); err != nil {
			return WorkerInstanceEpochSnapshot{}, err
		}
		store.state = state
	}
	return WorkerInstanceEpochSnapshot{
		NodeEpoch: store.state.NodeEpoch, AgentSessionEpoch: store.state.AgentSessionEpoch,
		DeviceEpochs: deviceEpochs,
	}, nil
}

func (store *FileWorkerInstanceEpochStore) persist(state fileWorkerInstanceEpochState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode WorkerInstance epoch state: %w", err)
	}
	temporary, err := os.CreateTemp(store.directory, ".worker-instance-epochs-*.tmp")
	if err != nil {
		return fmt.Errorf("create WorkerInstance epoch state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("restrict WorkerInstance epoch state: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write WorkerInstance epoch state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync WorkerInstance epoch state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close WorkerInstance epoch state: %w", err)
	}
	if err := os.Rename(temporaryPath, store.statePath); err != nil {
		return fmt.Errorf("publish WorkerInstance epoch state: %w", err)
	}
	if err := syncDirectory(store.directory); err != nil {
		return fmt.Errorf("sync WorkerInstance epoch directory: %w", err)
	}
	return nil
}

func (store *FileWorkerInstanceEpochStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	lock := store.lock
	store.lock = nil
	store.mu.Unlock()
	if lock == nil {
		return nil
	}
	return errors.Join(syscall.Flock(int(lock.Fd()), syscall.LOCK_UN), lock.Close())
}

func readBootID(path string) (string, error) {
	bootID, err := readBoundedSystemText(path, 128)
	if err != nil {
		return "", fmt.Errorf("read Node boot ID: %w", err)
	}
	bootID = strings.TrimSpace(bootID)
	parsed, err := uuid.Parse(bootID)
	if err != nil || parsed == uuid.Nil || parsed.String() != bootID {
		return "", errors.New("Node boot ID is invalid")
	}
	return bootID, nil
}

func validFileWorkerInstanceEpochState(state fileWorkerInstanceEpochState) bool {
	if state.SchemaVersion != 1 || !validText(state.NodeIdentity, maxIdentityText) ||
		state.NodeEpoch <= 0 || state.AgentSessionEpoch <= 0 ||
		len(state.Devices) > maxWorkerInstanceEpochDevices || state.Devices == nil ||
		state.ObservationSequences == nil {
		return false
	}
	bootID, err := uuid.Parse(state.BootID)
	if err != nil || bootID == uuid.Nil || bootID.String() != state.BootID {
		return false
	}
	seenBDFs := make(map[string]struct{}, len(state.Devices))
	for gpuUUID, device := range state.Devices {
		if !validGPUUUID(gpuUUID) || !validPCIBDF(device.PCIBDF) ||
			!validDigestHex(device.AttestationDigest) || device.Epoch <= 0 {
			return false
		}
		if _, duplicate := seenBDFs[device.PCIBDF]; duplicate {
			return false
		}
		seenBDFs[device.PCIBDF] = struct{}{}
	}
	for workerInstanceID, sequence := range state.ObservationSequences {
		parsed, err := uuid.Parse(workerInstanceID)
		if err != nil || parsed == uuid.Nil || parsed.String() != workerInstanceID || sequence <= 0 {
			return false
		}
	}
	return true
}

func cloneFileWorkerInstanceEpochState(
	state fileWorkerInstanceEpochState,
) fileWorkerInstanceEpochState {
	cloned := state
	cloned.Devices = make(map[string]fileWorkerInstanceDeviceEpoch, len(state.Devices))
	for gpuUUID, device := range state.Devices {
		cloned.Devices[gpuUUID] = device
	}
	cloned.ObservationSequences = make(map[string]int64, len(state.ObservationSequences))
	for workerInstanceID, sequence := range state.ObservationSequences {
		cloned.ObservationSequences[workerInstanceID] = sequence
	}
	return cloned
}

var _ WorkerInstanceEpochStore = (*FileWorkerInstanceEpochStore)(nil)
var _ WorkerInstanceObservationSequencer = (*FileWorkerInstanceEpochStore)(nil)
