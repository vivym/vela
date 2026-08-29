package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

type ExpectedWorkerDevice struct {
	DeviceID      uuid.UUID
	ComputeNodeID uuid.UUID
	NodeIdentity  string
	GPUUUID       string
	PCIBDF        string
}

type AttestedWorkerDevice struct {
	DeviceID                uuid.UUID
	ComputeNodeID           uuid.UUID
	NodeIdentity            string
	GPUUUID                 string
	PCIBDF                  string
	NodeEpoch               int64
	AgentSessionEpoch       int64
	DeviceEpoch             int64
	NodeAttestationDigest   string
	DeviceAttestationDigest string
	Health                  string
}

type WorkerInstanceDeviceProbe interface {
	AttestWorkerInstanceDevices(
		context.Context,
		[]ExpectedWorkerDevice,
	) ([]AttestedWorkerDevice, error)
}

type WorkerInstanceObserver interface {
	Observe(context.Context, fleet.WorkerInstanceEvidence) (fleet.WorkerInstanceDecision, error)
}

type WorkerInstanceEvidenceTemplate struct {
	Evidence   fleet.WorkerInstanceEvidence
	ObservedBy string
}

type WorkerInstanceEvidenceReporter struct {
	probe    WorkerInstanceDeviceProbe
	observer WorkerInstanceObserver
	ttl      time.Duration
	clock    func() time.Time
}

func NewWorkerInstanceEvidenceReporter(
	probe WorkerInstanceDeviceProbe,
	observer WorkerInstanceObserver,
	ttl time.Duration,
	clock func() time.Time,
) (*WorkerInstanceEvidenceReporter, error) {
	if probe == nil || observer == nil || ttl < 10*time.Second || ttl > 10*time.Minute ||
		ttl%time.Second != 0 || clock == nil {
		return nil, errors.New("WorkerInstance evidence reporter configuration is invalid")
	}
	return &WorkerInstanceEvidenceReporter{
		probe: probe, observer: observer, ttl: ttl, clock: clock,
	}, nil
}

func (reporter *WorkerInstanceEvidenceReporter) Report(
	ctx context.Context,
	template WorkerInstanceEvidenceTemplate,
) (fleet.WorkerInstanceDecision, error) {
	if ctx == nil || reporter == nil || reporter.probe == nil || reporter.observer == nil ||
		reporter.clock == nil {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance evidence reporter is not configured")
	}
	evidence := cloneWorkerInstanceEvidence(template.Evidence)
	if evidence.SchemaVersion != 1 || evidence.WorkerInstanceID == uuid.Nil ||
		evidence.InstanceEpoch <= 0 || evidence.ControlSessionEpoch <= 0 ||
		evidence.DeviceSet.ID == uuid.Nil || len(evidence.DeviceSet.Devices) == 0 ||
		len(evidence.Members) == 0 || len(evidence.Residencies) == 0 ||
		evidence.Capacity.Sequence <= 0 || len(evidence.Capacity.Vector) == 0 ||
		!validText(template.ObservedBy, maxIdentityText) {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance evidence template is invalid")
	}
	expected := make([]ExpectedWorkerDevice, 0, len(evidence.DeviceSet.Devices))
	expectedByID := make(map[uuid.UUID]fleet.WorkerDeviceEvidence, len(evidence.DeviceSet.Devices))
	for _, device := range evidence.DeviceSet.Devices {
		if device.ID == uuid.Nil || device.ComputeNodeID == uuid.Nil ||
			!validText(device.NodeIdentity, maxIdentityText) || !gpuUUIDPattern.MatchString(device.GPUUUID) ||
			!pciBDFPattern.MatchString(device.PCIBDF) || device.Kind != "GPU" {
			return fleet.WorkerInstanceDecision{}, errors.New("expected WorkerInstance device identity is invalid")
		}
		if _, exists := expectedByID[device.ID]; exists {
			return fleet.WorkerInstanceDecision{}, errors.New("expected WorkerInstance device identity is duplicated")
		}
		expectedByID[device.ID] = device
		expected = append(expected, ExpectedWorkerDevice{
			DeviceID: device.ID, ComputeNodeID: device.ComputeNodeID,
			NodeIdentity: device.NodeIdentity, GPUUUID: device.GPUUUID, PCIBDF: device.PCIBDF,
		})
	}
	observed, err := reporter.probe.AttestWorkerInstanceDevices(ctx, expected)
	if err != nil {
		return fleet.WorkerInstanceDecision{}, fmt.Errorf("attest WorkerInstance devices: %w", err)
	}
	if len(observed) != len(expected) {
		return fleet.WorkerInstanceDecision{}, errors.New("attested WorkerInstance device set is incomplete")
	}
	attestedByID := make(map[uuid.UUID]AttestedWorkerDevice, len(observed))
	physicalDevices := make(map[string]struct{}, len(observed))
	for _, device := range observed {
		expectedDevice, exists := expectedByID[device.DeviceID]
		physicalKey := device.GPUUUID + "\x00" + device.PCIBDF
		if !exists || device.ComputeNodeID != expectedDevice.ComputeNodeID ||
			device.NodeIdentity != expectedDevice.NodeIdentity || device.GPUUUID != expectedDevice.GPUUUID ||
			device.PCIBDF != expectedDevice.PCIBDF || device.NodeEpoch <= 0 ||
			device.AgentSessionEpoch <= 0 || device.DeviceEpoch <= 0 || device.Health != "HEALTHY" ||
			!validDigestHex(device.NodeAttestationDigest) ||
			!validDigestHex(device.DeviceAttestationDigest) {
			return fleet.WorkerInstanceDecision{}, errors.New("attested WorkerInstance device does not match approved physical identity")
		}
		if _, exists := attestedByID[device.DeviceID]; exists {
			return fleet.WorkerInstanceDecision{}, errors.New("attested WorkerInstance device identity is duplicated")
		}
		if _, exists := physicalDevices[physicalKey]; exists {
			return fleet.WorkerInstanceDecision{}, errors.New("attested WorkerInstance physical device is duplicated")
		}
		attestedByID[device.DeviceID] = device
		physicalDevices[physicalKey] = struct{}{}
	}
	for index := range evidence.DeviceSet.Devices {
		device := &evidence.DeviceSet.Devices[index]
		attested, exists := attestedByID[device.ID]
		if !exists {
			return fleet.WorkerInstanceDecision{}, errors.New("attested WorkerInstance device set is incomplete")
		}
		device.NodeEpoch = attested.NodeEpoch
		device.AgentSessionEpoch = attested.AgentSessionEpoch
		device.DeviceEpoch = attested.DeviceEpoch
		device.NodeAttestationDigest = attested.NodeAttestationDigest
		device.AttestationDigest = attested.DeviceAttestationDigest
		device.Health = attested.Health
	}
	if err := bindWorkerMemberEvidence(&evidence); err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	membershipDigest, topologyDigest, err := workerDeviceSetDigests(evidence.DeviceSet.Devices)
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	evidence.DeviceSet.MembershipDigest = membershipDigest
	evidence.DeviceSet.TopologyDigest = topologyDigest
	now := reporter.clock().UTC()
	if now.IsZero() {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance evidence clock is invalid")
	}
	evidence.ObservedAt = now
	evidence.ObservedBy = template.ObservedBy
	evidence.Capacity.ObservedAt = now
	evidence.Capacity.ExpiresAt = now.Add(reporter.ttl)
	decision, err := reporter.observer.Observe(ctx, evidence)
	if err != nil {
		return fleet.WorkerInstanceDecision{}, fmt.Errorf("observe WorkerInstance authority: %w", err)
	}
	if decision.WorkerInstanceID != evidence.WorkerInstanceID ||
		decision.InstanceEpoch != evidence.InstanceEpoch || decision.ControlSessionEpoch <= 0 ||
		decision.ModelRuntimeEpoch <= 0 {
		return fleet.WorkerInstanceDecision{}, errors.New("WorkerInstance observation decision is invalid")
	}
	return decision, nil
}

func bindWorkerMemberEvidence(evidence *fleet.WorkerInstanceEvidence) error {
	deviceIDs := make(map[uuid.UUID]struct{}, len(evidence.DeviceSet.Devices))
	for _, device := range evidence.DeviceSet.Devices {
		deviceIDs[device.ID] = struct{}{}
	}
	covered := make(map[uuid.UUID]struct{}, len(deviceIDs))
	for index := range evidence.Members {
		member := &evidence.Members[index]
		if member.ID == uuid.Nil || member.ComputeNodeID == uuid.Nil || member.MemberEpoch <= 0 ||
			!validText(member.MemberKey, 100) || len(member.DeviceIDs) == 0 || member.Readiness != "READY" {
			return errors.New("WorkerInstance member evidence template is invalid")
		}
		identities := make([]string, 0, len(member.DeviceIDs))
		for _, deviceID := range member.DeviceIDs {
			if _, exists := deviceIDs[deviceID]; !exists {
				return errors.New("WorkerInstance member references an unknown device")
			}
			if _, exists := covered[deviceID]; exists {
				return errors.New("WorkerInstance member device ownership is duplicated")
			}
			covered[deviceID] = struct{}{}
			identities = append(identities, deviceID.String())
		}
		sort.Strings(identities)
		member.DeviceSubsetDigest = digestCanonical(identities)
		member.IdentityDigest = digestCanonical(struct {
			ID           string `json:"id"`
			Key          string `json:"key"`
			NodeID       string `json:"node_id"`
			Epoch        int64  `json:"epoch"`
			DeviceDigest string `json:"device_digest"`
		}{
			ID: member.ID.String(), Key: member.MemberKey,
			NodeID: member.ComputeNodeID.String(), Epoch: member.MemberEpoch,
			DeviceDigest: member.DeviceSubsetDigest,
		})
	}
	if len(covered) != len(deviceIDs) {
		return errors.New("WorkerInstance member evidence does not cover every device")
	}
	return nil
}

func workerDeviceSetDigests(devices []fleet.WorkerDeviceEvidence) (string, string, error) {
	type membership struct {
		DeviceID    string `json:"device_id"`
		NodeID      string `json:"node_id"`
		GPUUUID     string `json:"gpu_uuid"`
		PCIBDF      string `json:"pci_bdf"`
		NodeEpoch   int64  `json:"node_epoch"`
		DeviceEpoch int64  `json:"device_epoch"`
	}
	type topology struct {
		DeviceID     string `json:"device_id"`
		NodeIdentity string `json:"node_identity"`
		Region       string `json:"region"`
		Network      string `json:"network"`
		FaultDomain  string `json:"fault_domain"`
		Ordinal      int    `json:"ordinal"`
	}
	memberships := make([]membership, 0, len(devices))
	topologies := make([]topology, 0, len(devices))
	for _, device := range devices {
		memberships = append(memberships, membership{
			DeviceID: device.ID.String(), NodeID: device.ComputeNodeID.String(),
			GPUUUID: device.GPUUUID, PCIBDF: device.PCIBDF,
			NodeEpoch: device.NodeEpoch, DeviceEpoch: device.DeviceEpoch,
		})
		topologies = append(topologies, topology{
			DeviceID: device.ID.String(), NodeIdentity: device.NodeIdentity,
			Region: device.Region, Network: device.NetworkDomain,
			FaultDomain: device.FaultDomain, Ordinal: device.Ordinal,
		})
	}
	sort.Slice(memberships, func(left, right int) bool {
		return memberships[left].DeviceID < memberships[right].DeviceID
	})
	sort.Slice(topologies, func(left, right int) bool {
		return topologies[left].DeviceID < topologies[right].DeviceID
	})
	return digestCanonical(memberships), digestCanonical(topologies), nil
}

func digestCanonical(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func validDigestHex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func cloneWorkerInstanceEvidence(evidence fleet.WorkerInstanceEvidence) fleet.WorkerInstanceEvidence {
	cloned := evidence
	cloned.DeviceSet.Devices = append([]fleet.WorkerDeviceEvidence(nil), evidence.DeviceSet.Devices...)
	cloned.Members = make([]fleet.WorkerMemberEvidence, len(evidence.Members))
	for index, member := range evidence.Members {
		cloned.Members[index] = member
		cloned.Members[index].DeviceIDs = append([]uuid.UUID(nil), member.DeviceIDs...)
	}
	cloned.Residencies = append([]fleet.ModelResidencyEvidence(nil), evidence.Residencies...)
	cloned.Capacity.Vector = make(map[string]int64, len(evidence.Capacity.Vector))
	for key, value := range evidence.Capacity.Vector {
		cloned.Capacity.Vector[key] = value
	}
	return cloned
}
