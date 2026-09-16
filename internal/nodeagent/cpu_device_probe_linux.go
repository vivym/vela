package nodeagent

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// CPUDeviceProbe measures the host's online CPU set. CPU Device IDs identify
// approved logical slots; exclusive CPU/memory capacity remains enforced by the
// signed Pod resource contract and kubelet, not by this inventory observation.
type CPUDeviceProbe struct {
	NodeIdentity   string
	OnlineCPUsPath string
	Epochs         *FileWorkerInstanceEpochStore
}

func (probe *CPUDeviceProbe) AttestWorkerInstanceDevices(ctx context.Context, expected []ExpectedWorkerDevice) ([]AttestedWorkerDevice, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if probe == nil || probe.Epochs == nil || !validText(probe.NodeIdentity, maxIdentityText) || !absoluteCleanPath(probe.OnlineCPUsPath) || len(expected) == 0 || len(expected) > maxWorkerInstanceEpochDevices {
		return nil, errors.New("CPU device probe is not configured")
	}
	online, err := readBoundedSystemText(probe.OnlineCPUsPath, 65536)
	if err != nil {
		return nil, err
	}
	online = strings.TrimSpace(online)
	if !validOnlineCPUSet(online) {
		return nil, errors.New("online CPU evidence is invalid")
	}
	store := probe.Epochs
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.lock == nil || store.state.NodeIdentity != probe.NodeIdentity {
		return nil, errors.New("CPU epoch store belongs to another node or is closed")
	}
	state := cloneFileWorkerInstanceEpochState(store.state)
	seen := make(map[uuid.UUID]bool, len(expected))
	result := make([]AttestedWorkerDevice, 0, len(expected))
	for _, device := range expected {
		if device.Kind != "CPU" || device.DeviceID == uuid.Nil || device.ComputeNodeID == uuid.Nil || device.NodeIdentity != probe.NodeIdentity || device.GPUUUID != "" || device.PCIBDF != "" || seen[device.DeviceID] {
			return nil, errors.New("expected CPU slot identity is invalid")
		}
		seen[device.DeviceID] = true
		digest := digestCanonical(struct {
			NodeIdentity string    `json:"node_identity"`
			DeviceID     uuid.UUID `json:"device_id"`
			OnlineCPUs   string    `json:"online_cpus"`
		}{probe.NodeIdentity, device.DeviceID, online})
		id := device.DeviceID.String()
		slot := state.CPUSlots[id]
		if slot.AttestationDigest != digest {
			if slot.Epoch == math.MaxInt64 {
				return nil, errors.New("CPU slot epoch is exhausted")
			}
			slot.Epoch++
			slot.AttestationDigest = digest
			state.CPUSlots[id] = slot
		}
		nodeDigest := digestCanonical(struct {
			NodeIdentity string `json:"node_identity"`
			NodeEpoch    int64  `json:"node_epoch"`
		}{probe.NodeIdentity, state.NodeEpoch})
		result = append(result, AttestedWorkerDevice{DeviceID: device.DeviceID, ComputeNodeID: device.ComputeNodeID, NodeIdentity: probe.NodeIdentity,
			NodeEpoch: state.NodeEpoch, AgentSessionEpoch: state.AgentSessionEpoch, DeviceEpoch: slot.Epoch,
			NodeAttestationDigest: nodeDigest, DeviceAttestationDigest: digest, Health: "HEALTHY"})
	}
	if !validFileWorkerInstanceEpochState(state) {
		return nil, errors.New("CPU epoch transition is invalid")
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := store.persist(state); err != nil {
		return nil, err
	}
	store.state = state
	return result, nil
}

func validOnlineCPUSet(value string) bool {
	if value == "" {
		return false
	}
	previous := -1
	for _, item := range strings.Split(value, ",") {
		bounds := strings.Split(item, "-")
		if len(bounds) > 2 {
			return false
		}
		first, err := strconv.Atoi(bounds[0])
		if err != nil || first < 0 || first <= previous || strconv.Itoa(first) != bounds[0] {
			return false
		}
		last := first
		if len(bounds) == 2 {
			last, err = strconv.Atoi(bounds[1])
			if err != nil || last <= first || strconv.Itoa(last) != bounds[1] {
				return false
			}
		}
		previous = last
	}
	return true
}
