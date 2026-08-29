package nodeagent_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
)

func TestNVIDIAGPUProbePersistsNodeSessionAndDeviceEpochs(t *testing.T) {
	root := t.TempDir()
	busRoot := filepath.Join(root, "sys", "bus", "pci", "devices")
	sysDevicesRoot := filepath.Join(root, "sys", "devices")
	driverRoot := filepath.Join(root, "sys", "drivers", "nvidia")
	for _, path := range []string{busRoot, sysDevicesRoot, driverRoot} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create GPU fixture directory %s: %v", path, err)
		}
	}
	firstUUID := "GPU-00000000-0000-0000-0000-000000000001"
	secondUUID := "GPU-00000000-0000-0000-0000-000000000002"
	firstBDF := "0000:41:00.0"
	secondBDF := "0000:42:00.0"
	firstRevision := createPCIDeviceFixture(t, busRoot, sysDevicesRoot, driverRoot, firstBDF, "0xa1")
	createPCIDeviceFixture(t, busRoot, sysDevicesRoot, driverRoot, secondBDF, "0xb2")
	bootIDPath := filepath.Join(root, "boot_id")
	driverVersionPath := filepath.Join(root, "nvidia-version")
	writePrivateFixture(t, bootIDPath, "49420000-0000-0000-0000-000000000001\n")
	writePrivateFixture(t, driverVersionPath, "NVRM version: 580.65.06\n")
	stateDirectory := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
		t.Fatalf("create epoch state directory: %v", err)
	}
	runner := &recordingNVIDIAInventoryRunner{output: []byte(
		firstUUID + ", 00000000:41:00.0\n" +
			secondUUID + ", 00000000:42:00.0\n",
	)}
	expected := []nodeagent.ExpectedWorkerDevice{
		{
			DeviceID:      uuid.MustParse("49420000-0000-0000-0000-000000000011"),
			ComputeNodeID: uuid.MustParse("49420000-0000-0000-0000-000000000021"),
			NodeIdentity:  "h3-node-01", GPUUUID: firstUUID, PCIBDF: firstBDF,
		},
		{
			DeviceID:      uuid.MustParse("49420000-0000-0000-0000-000000000012"),
			ComputeNodeID: uuid.MustParse("49420000-0000-0000-0000-000000000021"),
			NodeIdentity:  "h3-node-01", GPUUUID: secondUUID, PCIBDF: secondBDF,
		},
	}

	store := openEpochStore(t, stateDirectory, bootIDPath)
	probe := newGPUProbe(t, runner, store, busRoot, sysDevicesRoot, driverVersionPath)
	first := attestDevices(t, probe, expected)
	assertDeviceEpochs(t, first, 1, 1, map[string]int64{firstUUID: 1, secondUUID: 1})
	firstNodeDigest := first[0].NodeAttestationDigest
	firstDeviceDigests := map[string]string{
		firstUUID:  first[0].DeviceAttestationDigest,
		secondUUID: first[1].DeviceAttestationDigest,
	}
	repeated := attestDevices(t, probe, expected)
	assertDeviceEpochs(t, repeated, 1, 1, map[string]int64{firstUUID: 1, secondUUID: 1})
	assertAttestationDigests(t, repeated, firstNodeDigest, firstDeviceDigests)
	if err := store.Close(); err != nil {
		t.Fatalf("close first epoch store: %v", err)
	}

	store = openEpochStore(t, stateDirectory, bootIDPath)
	probe = newGPUProbe(t, runner, store, busRoot, sysDevicesRoot, driverVersionPath)
	restarted := attestDevices(t, probe, expected)
	assertDeviceEpochs(t, restarted, 1, 2, map[string]int64{firstUUID: 1, secondUUID: 1})
	assertAttestationDigests(t, restarted, firstNodeDigest, firstDeviceDigests)
	writePrivateFixture(t, firstRevision, "0xa2\n")
	deviceChanged := attestDevices(t, probe, expected)
	assertDeviceEpochs(t, deviceChanged, 1, 2, map[string]int64{firstUUID: 2, secondUUID: 1})
	if deviceChanged[0].NodeAttestationDigest != firstNodeDigest ||
		deviceChanged[0].DeviceAttestationDigest == firstDeviceDigests[firstUUID] ||
		deviceChanged[1].DeviceAttestationDigest != firstDeviceDigests[secondUUID] {
		t.Fatalf("device-only change produced inconsistent attestation digests: %#v", deviceChanged)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close restarted epoch store: %v", err)
	}

	writePrivateFixture(t, bootIDPath, "49420000-0000-0000-0000-000000000002\n")
	store = openEpochStore(t, stateDirectory, bootIDPath)
	t.Cleanup(func() { _ = store.Close() })
	probe = newGPUProbe(t, runner, store, busRoot, sysDevicesRoot, driverVersionPath)
	rebooted := attestDevices(t, probe, expected)
	assertDeviceEpochs(t, rebooted, 2, 3, map[string]int64{firstUUID: 3, secondUUID: 2})
	if rebooted[0].NodeAttestationDigest == firstNodeDigest {
		t.Fatal("Node reboot did not change the Node attestation digest")
	}

	if !reflect.DeepEqual(runner.args, []string{
		"--query-gpu=uuid,pci.bus_id", "--format=csv,noheader,nounits",
	}) || runner.path != "/usr/bin/nvidia-smi" || runner.calls != 5 {
		t.Fatalf("NVIDIA inventory calls=%d path=%q args=%#v", runner.calls, runner.path, runner.args)
	}
	stateInfo, err := os.Stat(filepath.Join(stateDirectory, "worker-instance-epochs.json"))
	if err != nil || stateInfo.Mode().Perm() != 0o600 {
		t.Fatalf("epoch state info=%#v error=%v", stateInfo, err)
	}
	for _, device := range rebooted {
		if len(device.NodeAttestationDigest) != 64 || len(device.DeviceAttestationDigest) != 64 ||
			device.Health != "HEALTHY" {
			t.Fatalf("GPU attestation evidence=%#v", device)
		}
	}
	if rebooted[0].DeviceAttestationDigest == rebooted[1].DeviceAttestationDigest {
		t.Fatal("distinct GPUs produced the same device attestation digest")
	}
}

func assertAttestationDigests(
	t *testing.T,
	devices []nodeagent.AttestedWorkerDevice,
	nodeDigest string,
	deviceDigests map[string]string,
) {
	t.Helper()
	for _, device := range devices {
		if device.NodeAttestationDigest != nodeDigest ||
			device.DeviceAttestationDigest != deviceDigests[device.GPUUUID] {
			t.Fatalf("attestation digests for %s changed without a lifecycle change", device.GPUUUID)
		}
	}
}

func openEpochStore(
	t *testing.T,
	stateDirectory string,
	bootIDPath string,
) *nodeagent.FileWorkerInstanceEpochStore {
	t.Helper()
	store, err := nodeagent.NewFileWorkerInstanceEpochStore(nodeagent.FileWorkerInstanceEpochStoreConfig{
		Directory: stateDirectory, NodeIdentity: "h3-node-01", BootIDPath: bootIDPath,
	})
	if err != nil {
		t.Fatalf("open WorkerInstance epoch store: %v", err)
	}
	return store
}

func newGPUProbe(
	t *testing.T,
	runner nodeagent.NVIDIAInventoryRunner,
	store nodeagent.WorkerInstanceEpochStore,
	busRoot string,
	sysDevicesRoot string,
	driverVersionPath string,
) *nodeagent.NVIDIAGPUProbe {
	t.Helper()
	probe, err := nodeagent.NewNVIDIAGPUProbe(nodeagent.NVIDIAGPUProbeConfig{
		NodeIdentity: "h3-node-01", NVIDIASMIPath: "/usr/bin/nvidia-smi",
		PCIBusDevicesRoot: busRoot, SysDevicesRoot: sysDevicesRoot,
		DriverVersionPath: driverVersionPath,
	}, runner, store)
	if err != nil {
		t.Fatalf("create NVIDIA GPU probe: %v", err)
	}
	return probe
}

func attestDevices(
	t *testing.T,
	probe *nodeagent.NVIDIAGPUProbe,
	expected []nodeagent.ExpectedWorkerDevice,
) []nodeagent.AttestedWorkerDevice {
	t.Helper()
	devices, err := probe.AttestWorkerInstanceDevices(context.Background(), expected)
	if err != nil {
		t.Fatalf("attest WorkerInstance devices: %v", err)
	}
	return devices
}

func assertDeviceEpochs(
	t *testing.T,
	devices []nodeagent.AttestedWorkerDevice,
	nodeEpoch int64,
	sessionEpoch int64,
	deviceEpochs map[string]int64,
) {
	t.Helper()
	if len(devices) != len(deviceEpochs) {
		t.Fatalf("attested device count=%d want=%d", len(devices), len(deviceEpochs))
	}
	for _, device := range devices {
		if device.NodeEpoch != nodeEpoch || device.AgentSessionEpoch != sessionEpoch ||
			device.DeviceEpoch != deviceEpochs[device.GPUUUID] {
			t.Fatalf("attested epochs for %s = node %d session %d device %d", device.GPUUUID, device.NodeEpoch, device.AgentSessionEpoch, device.DeviceEpoch)
		}
	}
}

func createPCIDeviceFixture(
	t *testing.T,
	busRoot string,
	sysDevicesRoot string,
	driverRoot string,
	bdf string,
	revision string,
) string {
	t.Helper()
	deviceRoot := filepath.Join(sysDevicesRoot, "pci0000:40", bdf)
	if err := os.MkdirAll(deviceRoot, 0o755); err != nil {
		t.Fatalf("create sysfs device fixture: %v", err)
	}
	attributes := map[string]string{
		"vendor": "0x10de\n", "device": "0x2b85\n",
		"subsystem_vendor": "0x10de\n", "subsystem_device": "0x1976\n",
		"revision": revision + "\n", "numa_node": "0\n",
	}
	for name, value := range attributes {
		writePrivateFixture(t, filepath.Join(deviceRoot, name), value)
	}
	if err := os.Symlink(driverRoot, filepath.Join(deviceRoot, "driver")); err != nil {
		t.Fatalf("link NVIDIA driver fixture: %v", err)
	}
	relative, err := filepath.Rel(busRoot, deviceRoot)
	if err != nil {
		t.Fatalf("relativize sysfs GPU fixture: %v", err)
	}
	if err := os.Symlink(relative, filepath.Join(busRoot, bdf)); err != nil {
		t.Fatalf("link PCI bus fixture: %v", err)
	}
	return filepath.Join(deviceRoot, "revision")
}

func writePrivateFixture(t *testing.T, path string, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

type recordingNVIDIAInventoryRunner struct {
	output []byte
	calls  int
	path   string
	args   []string
}

func (runner *recordingNVIDIAInventoryRunner) Run(
	_ context.Context,
	path string,
	args []string,
) ([]byte, error) {
	runner.calls++
	runner.path = path
	runner.args = append([]string(nil), args...)
	return append([]byte(nil), runner.output...), nil
}

var _ nodeagent.NVIDIAInventoryRunner = (*recordingNVIDIAInventoryRunner)(nil)

func TestNVIDIAGPUProbeRejectsMismatchedOrUnverifiedDevice(t *testing.T) {
	root := t.TempDir()
	stateDirectory := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
		t.Fatalf("create state directory: %v", err)
	}
	bootIDPath := filepath.Join(root, "boot_id")
	writePrivateFixture(t, bootIDPath, "49420000-0000-0000-0000-000000000001\n")
	store := openEpochStore(t, stateDirectory, bootIDPath)
	t.Cleanup(func() { _ = store.Close() })
	busRoot := filepath.Join(root, "sys", "bus", "pci", "devices")
	sysDevicesRoot := filepath.Join(root, "sys", "devices")
	driverRoot := filepath.Join(root, "sys", "drivers", "nouveau")
	for _, path := range []string{busRoot, sysDevicesRoot, driverRoot} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create GPU mismatch fixture: %v", err)
		}
	}
	createPCIDeviceFixture(t, busRoot, sysDevicesRoot, driverRoot, "0000:41:00.0", "0xa1")
	driverVersionPath := filepath.Join(root, "nvidia-version")
	writePrivateFixture(t, driverVersionPath, "NVRM version: 580.65.06\n")
	runner := &recordingNVIDIAInventoryRunner{output: []byte(
		"GPU-00000000-0000-0000-0000-000000000099, 00000000:41:00.0\n",
	)}
	probe := newGPUProbe(t, runner, store, busRoot, sysDevicesRoot, driverVersionPath)
	_, err := probe.AttestWorkerInstanceDevices(context.Background(), []nodeagent.ExpectedWorkerDevice{{
		DeviceID:      uuid.MustParse("49420000-0000-0000-0000-000000000011"),
		ComputeNodeID: uuid.MustParse("49420000-0000-0000-0000-000000000021"),
		NodeIdentity:  "h3-node-01",
		GPUUUID:       "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
	}})
	if err == nil || !strings.Contains(err.Error(), "expected GPU") {
		t.Fatalf("mismatched NVIDIA inventory error=%v", err)
	}
	runner.output = []byte(
		"GPU-00000000-0000-0000-0000-000000000001, 00000000:41:00.0\n",
	)
	_, err = probe.AttestWorkerInstanceDevices(context.Background(), []nodeagent.ExpectedWorkerDevice{{
		DeviceID:      uuid.MustParse("49420000-0000-0000-0000-000000000011"),
		ComputeNodeID: uuid.MustParse("49420000-0000-0000-0000-000000000021"),
		NodeIdentity:  "h3-node-01",
		GPUUUID:       "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
	}})
	if err == nil || !strings.Contains(err.Error(), "NVIDIA driver") {
		t.Fatalf("unverified NVIDIA driver error=%v", err)
	}
}

func TestExecNVIDIAInventoryRunnerUsesExactHeldExecutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nvidia-smi")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf 'GPU-00000000-0000-0000-0000-000000000001, 00000000:41:00.0\\n'\n"), 0o700); err != nil {
		t.Fatalf("write nvidia-smi fixture: %v", err)
	}
	runner := nodeagent.ExecNVIDIAInventoryRunner{}
	output, err := runner.Run(context.Background(), path, []string{
		"--query-gpu=uuid,pci.bus_id", "--format=csv,noheader,nounits",
	})
	if err != nil {
		t.Fatalf("run NVIDIA inventory: %v", err)
	}
	if string(output) != "GPU-00000000-0000-0000-0000-000000000001, 00000000:41:00.0\n" {
		t.Fatalf("NVIDIA inventory output=%q", output)
	}
	if _, err := runner.Run(context.Background(), path, []string{"--query-gpu=name"}); err == nil {
		t.Fatal("NVIDIA inventory runner accepted an unapproved query")
	}
}

func TestFileWorkerInstanceEpochStoreRejectsInvalidStateAndLockConflict(t *testing.T) {
	const validPrefix = `{"schema_version":1,"node_identity":"h3-node-01","boot_id":"49420000-0000-0000-0000-000000000001","node_epoch":1,"agent_session_epoch":1,`
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cases := map[string]string{
		"malformed":         `{"schema_version":`,
		"duplicate key":     `{"schema_version":1,"schema_version":1}`,
		"unknown field":     validPrefix + `"devices":{},"unexpected":true}`,
		"another Node":      `{"schema_version":1,"node_identity":"h3-node-02","boot_id":"49420000-0000-0000-0000-000000000001","node_epoch":1,"agent_session_epoch":1,"devices":{}}`,
		"duplicate PCI BDF": validPrefix + `"devices":{"GPU-00000000-0000-0000-0000-000000000001":{"pci_bdf":"0000:41:00.0","attestation_digest":"` + digest + `","epoch":1},"GPU-00000000-0000-0000-0000-000000000002":{"pci_bdf":"0000:41:00.0","attestation_digest":"` + digest + `","epoch":1}}}`,
	}
	for name, state := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			bootIDPath := filepath.Join(root, "boot_id")
			writePrivateFixture(t, bootIDPath, "49420000-0000-0000-0000-000000000001\n")
			stateDirectory := filepath.Join(root, "state")
			if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
				t.Fatalf("create state directory: %v", err)
			}
			writePrivateFixture(t, filepath.Join(stateDirectory, "worker-instance-epochs.json"), state)
			store, err := nodeagent.NewFileWorkerInstanceEpochStore(nodeagent.FileWorkerInstanceEpochStoreConfig{
				Directory: stateDirectory, NodeIdentity: "h3-node-01", BootIDPath: bootIDPath,
			})
			if store != nil {
				_ = store.Close()
			}
			if err == nil {
				t.Fatal("invalid WorkerInstance epoch state was accepted")
			}
		})
	}

	root := t.TempDir()
	bootIDPath := filepath.Join(root, "boot_id")
	writePrivateFixture(t, bootIDPath, "49420000-0000-0000-0000-000000000001\n")
	stateDirectory := filepath.Join(root, "state")
	first := openEpochStore(t, stateDirectory, bootIDPath)
	t.Cleanup(func() { _ = first.Close() })
	second, err := nodeagent.NewFileWorkerInstanceEpochStore(nodeagent.FileWorkerInstanceEpochStoreConfig{
		Directory: stateDirectory, NodeIdentity: "h3-node-01", BootIDPath: bootIDPath,
	})
	if second != nil {
		_ = second.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "lock WorkerInstance epoch state") {
		t.Fatalf("concurrent epoch store lock error=%v", err)
	}
}

func TestFileWorkerInstanceEpochStoreRejectsInvalidBootID(t *testing.T) {
	root := t.TempDir()
	bootIDPath := filepath.Join(root, "boot_id")
	writePrivateFixture(t, bootIDPath, "not-a-boot-id\n")
	store, err := nodeagent.NewFileWorkerInstanceEpochStore(nodeagent.FileWorkerInstanceEpochStoreConfig{
		Directory: filepath.Join(root, "state"), NodeIdentity: "h3-node-01", BootIDPath: bootIDPath,
	})
	if store != nil {
		_ = store.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "boot ID is invalid") {
		t.Fatalf("invalid boot ID error=%v", err)
	}
}

func TestFileWorkerInstanceEpochStoreReadsLinuxProcBootID(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux exposes the production boot ID through procfs")
	}
	store, err := nodeagent.NewFileWorkerInstanceEpochStore(nodeagent.FileWorkerInstanceEpochStoreConfig{
		Directory: filepath.Join(t.TempDir(), "state"), NodeIdentity: "h3-node-01",
		BootIDPath: "/proc/sys/kernel/random/boot_id",
	})
	if err != nil {
		t.Fatalf("open epoch store with procfs boot ID: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close epoch store: %v", err)
	}
}
