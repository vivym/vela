package nodeagent

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

const (
	maxNVIDIAInventoryBytes = 64 * 1024
	maxNVIDIAInventoryGPUs  = 1024
)

var (
	pciBDFEightDomainPattern = regexp.MustCompile(`^[0-9a-f]{8}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]$`)
	sysfsHexValuePattern     = regexp.MustCompile(`^0x[0-9a-f]{2,8}$`)
	sysfsNUMANodePattern     = regexp.MustCompile(`^-?[0-9]+$`)
)

type NVIDIAInventoryRunner interface {
	Run(context.Context, string, []string) ([]byte, error)
}

// ExecNVIDIAInventoryRunner executes only Vela's fixed NVIDIA inventory query
// through the same held-executable boundary as other Node Agent host commands.
type ExecNVIDIAInventoryRunner struct{}

func (ExecNVIDIAInventoryRunner) Run(ctx context.Context, path string, args []string) ([]byte, error) {
	if len(args) != 2 || args[0] != "--query-gpu=uuid,pci.bus_id" ||
		args[1] != "--format=csv,noheader,nounits" {
		return nil, errors.New("NVIDIA inventory query is not approved")
	}
	return runHeldExecutable(ctx, path, args, maxNVIDIAInventoryBytes)
}

type NVIDIAGPUProbeConfig struct {
	NodeIdentity      string
	NVIDIASMIPath     string
	PCIBusDevicesRoot string
	SysDevicesRoot    string
	DriverVersionPath string
}

type NVIDIAGPUProbe struct {
	config NVIDIAGPUProbeConfig
	runner NVIDIAInventoryRunner
	epochs WorkerInstanceEpochStore
}

type verifiedPCIDevice struct {
	GPUUUID         string
	PCIBDF          string
	Vendor          string
	Device          string
	SubsystemVendor string
	SubsystemDevice string
	Revision        string
	NUMANode        string
	Driver          string
	DriverVersion   string
}

func NewNVIDIAGPUProbe(
	config NVIDIAGPUProbeConfig,
	runner NVIDIAInventoryRunner,
	epochs WorkerInstanceEpochStore,
) (*NVIDIAGPUProbe, error) {
	if !validText(config.NodeIdentity, maxIdentityText) || runner == nil || epochs == nil ||
		!absoluteCleanPath(config.NVIDIASMIPath) ||
		!absoluteCleanPath(config.PCIBusDevicesRoot) ||
		!absoluteCleanPath(config.SysDevicesRoot) ||
		!absoluteCleanPath(config.DriverVersionPath) {
		return nil, errors.New("NVIDIA GPU probe configuration is invalid")
	}
	return &NVIDIAGPUProbe{config: config, runner: runner, epochs: epochs}, nil
}

func (probe *NVIDIAGPUProbe) AttestWorkerInstanceDevices(
	ctx context.Context,
	expected []ExpectedWorkerDevice,
) ([]AttestedWorkerDevice, error) {
	if probe == nil || probe.runner == nil || probe.epochs == nil {
		return nil, errors.New("NVIDIA GPU probe is not configured")
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if len(expected) == 0 || len(expected) > maxNVIDIAInventoryGPUs {
		return nil, errors.New("expected WorkerInstance GPU set is invalid")
	}
	inventoryOutput, err := probe.runner.Run(ctx, probe.config.NVIDIASMIPath, []string{
		"--query-gpu=uuid,pci.bus_id",
		"--format=csv,noheader,nounits",
	})
	if err != nil {
		return nil, fmt.Errorf("query NVIDIA GPU inventory: %w", err)
	}
	inventory, err := parseNVIDIAInventory(inventoryOutput)
	if err != nil {
		return nil, err
	}
	driverVersion, err := readBoundedSystemText(probe.config.DriverVersionPath, maxNVIDIAInventoryBytes)
	if err != nil || !validText(driverVersion, 4096) {
		return nil, errors.New("NVIDIA driver version evidence is invalid")
	}

	seenDeviceIDs := make(map[uuid.UUID]struct{}, len(expected))
	seenPhysical := make(map[string]struct{}, len(expected))
	bindings := make([]WorkerInstanceDeviceEpochBinding, 0, len(expected))
	for _, device := range expected {
		if device.DeviceID == uuid.Nil || device.ComputeNodeID == uuid.Nil ||
			device.NodeIdentity != probe.config.NodeIdentity || !validGPUUUID(device.GPUUUID) ||
			!validPCIBDF(device.PCIBDF) {
			return nil, errors.New("expected WorkerInstance GPU identity is invalid")
		}
		if _, duplicate := seenDeviceIDs[device.DeviceID]; duplicate {
			return nil, errors.New("expected WorkerInstance Device identity is duplicated")
		}
		physicalKey := device.GPUUUID + "\x00" + device.PCIBDF
		if _, duplicate := seenPhysical[physicalKey]; duplicate {
			return nil, errors.New("expected WorkerInstance physical GPU is duplicated")
		}
		seenDeviceIDs[device.DeviceID] = struct{}{}
		seenPhysical[physicalKey] = struct{}{}
		observedBDF, exists := inventory[device.GPUUUID]
		if !exists || observedBDF != device.PCIBDF {
			return nil, fmt.Errorf("expected GPU %s at %s is not present", device.GPUUUID, device.PCIBDF)
		}
		attested, err := probe.verifyPCIDevice(device.GPUUUID, device.PCIBDF, driverVersion)
		if err != nil {
			return nil, err
		}
		digest := digestCanonical(attested)
		if !validDigestHex(digest) {
			return nil, errors.New("NVIDIA GPU attestation digest is invalid")
		}
		bindings = append(bindings, WorkerInstanceDeviceEpochBinding{
			GPUUUID: device.GPUUUID, PCIBDF: device.PCIBDF, AttestationDigest: digest,
		})
	}
	snapshot, err := probe.epochs.BindWorkerInstanceDevices(ctx, bindings)
	if err != nil {
		return nil, fmt.Errorf("bind WorkerInstance GPU epochs: %w", err)
	}
	if snapshot.NodeEpoch <= 0 || snapshot.AgentSessionEpoch <= 0 ||
		len(snapshot.DeviceEpochs) != len(expected) {
		return nil, errors.New("WorkerInstance GPU epoch snapshot is invalid")
	}
	nodeDigest := digestCanonical(struct {
		NodeIdentity string `json:"node_identity"`
		NodeEpoch    int64  `json:"node_epoch"`
	}{
		NodeIdentity: probe.config.NodeIdentity, NodeEpoch: snapshot.NodeEpoch,
	})
	if !validDigestHex(nodeDigest) {
		return nil, errors.New("node attestation digest is invalid")
	}
	result := make([]AttestedWorkerDevice, 0, len(expected))
	for index, device := range expected {
		deviceEpoch := snapshot.DeviceEpochs[device.GPUUUID]
		if deviceEpoch <= 0 {
			return nil, errors.New("WorkerInstance GPU Device epoch is invalid")
		}
		result = append(result, AttestedWorkerDevice{
			DeviceID: device.DeviceID, ComputeNodeID: device.ComputeNodeID,
			NodeIdentity: device.NodeIdentity, GPUUUID: device.GPUUUID, PCIBDF: device.PCIBDF,
			NodeEpoch: snapshot.NodeEpoch, AgentSessionEpoch: snapshot.AgentSessionEpoch,
			DeviceEpoch: deviceEpoch, NodeAttestationDigest: nodeDigest,
			DeviceAttestationDigest: bindings[index].AttestationDigest, Health: "HEALTHY",
		})
	}
	return result, nil
}

func parseNVIDIAInventory(output []byte) (map[string]string, error) {
	if len(output) == 0 || len(output) > maxNVIDIAInventoryBytes {
		return nil, errors.New("NVIDIA GPU inventory is empty or exceeds its bound")
	}
	reader := csv.NewReader(bytes.NewReader(output))
	reader.FieldsPerRecord = 2
	reader.TrimLeadingSpace = true
	inventory := make(map[string]string)
	bdfs := make(map[string]struct{})
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || len(record) != 2 || len(inventory) == maxNVIDIAInventoryGPUs {
			return nil, errors.New("NVIDIA GPU inventory is invalid")
		}
		gpuUUID := strings.TrimSpace(record[0])
		bdf, ok := canonicalPCIBDF(strings.TrimSpace(record[1]))
		if !validGPUUUID(gpuUUID) || !ok {
			return nil, errors.New("NVIDIA GPU inventory identity is invalid")
		}
		if _, duplicate := inventory[gpuUUID]; duplicate {
			return nil, errors.New("NVIDIA GPU inventory UUID is duplicated")
		}
		if _, duplicate := bdfs[bdf]; duplicate {
			return nil, errors.New("NVIDIA GPU inventory PCI BDF is duplicated")
		}
		inventory[gpuUUID] = bdf
		bdfs[bdf] = struct{}{}
	}
	if len(inventory) == 0 {
		return nil, errors.New("NVIDIA GPU inventory contains no devices")
	}
	return inventory, nil
}

func canonicalPCIBDF(value string) (string, bool) {
	value = strings.ToLower(value)
	if validPCIBDF(value) {
		return value, true
	}
	if !pciBDFEightDomainPattern.MatchString(value) || value[:4] != "0000" {
		return "", false
	}
	canonical := value[4:]
	return canonical, validPCIBDF(canonical)
}

func (probe *NVIDIAGPUProbe) verifyPCIDevice(
	gpuUUID string,
	bdf string,
	driverVersion string,
) (verifiedPCIDevice, error) {
	busEntry := filepath.Join(probe.config.PCIBusDevicesRoot, bdf)
	resolvedEntry, err := filepath.EvalSymlinks(busEntry)
	if err != nil {
		return verifiedPCIDevice{}, fmt.Errorf("resolve PCI device %s: %w", bdf, err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(probe.config.SysDevicesRoot)
	if err != nil {
		return verifiedPCIDevice{}, fmt.Errorf("resolve sysfs devices root: %w", err)
	}
	relative, err := filepath.Rel(resolvedRoot, resolvedEntry)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return verifiedPCIDevice{}, errors.New("PCI device resolves outside the trusted sysfs devices root")
	}
	info, err := os.Stat(resolvedEntry)
	if err != nil || !info.IsDir() {
		return verifiedPCIDevice{}, errors.New("PCI device sysfs entry is invalid")
	}
	values := make(map[string]string, 6)
	for _, name := range []string{
		"vendor", "device", "subsystem_vendor", "subsystem_device", "revision", "numa_node",
	} {
		value, err := readBoundedSystemText(filepath.Join(resolvedEntry, name), 128)
		if err != nil {
			return verifiedPCIDevice{}, fmt.Errorf("read PCI device %s attribute %s: %w", bdf, name, err)
		}
		values[name] = value
	}
	if values["vendor"] != "0x10de" || !sysfsHexValuePattern.MatchString(values["device"]) ||
		!sysfsHexValuePattern.MatchString(values["subsystem_vendor"]) ||
		!sysfsHexValuePattern.MatchString(values["subsystem_device"]) ||
		!sysfsHexValuePattern.MatchString(values["revision"]) ||
		!sysfsNUMANodePattern.MatchString(values["numa_node"]) {
		return verifiedPCIDevice{}, errors.New("PCI device sysfs identity is not a valid NVIDIA GPU")
	}
	driver, err := filepath.EvalSymlinks(filepath.Join(resolvedEntry, "driver"))
	if err != nil || filepath.Base(driver) != "nvidia" {
		return verifiedPCIDevice{}, errors.New("PCI device is not bound to the NVIDIA driver")
	}
	return verifiedPCIDevice{
		GPUUUID: gpuUUID, PCIBDF: bdf, Vendor: values["vendor"], Device: values["device"],
		SubsystemVendor: values["subsystem_vendor"], SubsystemDevice: values["subsystem_device"],
		Revision: values["revision"], NUMANode: values["numa_node"], Driver: filepath.Base(driver),
		DriverVersion: driverVersion,
	}, nil
}

func readBoundedSystemText(path string, maximum int64) (string, error) {
	if !absoluteCleanPath(path) || maximum <= 0 {
		return "", errors.New("system evidence path or size bound is invalid")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || !pathInfo.Mode().IsRegular() {
		return "", errors.New("system evidence file is invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(pathInfo, openedInfo) || !openedInfo.Mode().IsRegular() {
		return "", errors.New("system evidence changed while it was opened")
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || len(content) == 0 || int64(len(content)) > maximum {
		return "", errors.New("system evidence is empty or exceeds its bound")
	}
	return strings.TrimSpace(string(content)), nil
}

func absoluteCleanPath(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}

var _ WorkerInstanceDeviceProbe = (*NVIDIAGPUProbe)(nil)
var _ NVIDIAInventoryRunner = ExecNVIDIAInventoryRunner{}
