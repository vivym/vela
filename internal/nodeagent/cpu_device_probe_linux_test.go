package nodeagent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestCPUDeviceProbePersistsTopologyAndBootEpochs(t *testing.T) {
	directory := t.TempDir()
	online, boot := filepath.Join(directory, "online"), filepath.Join(directory, "boot")
	write := func(path, value string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(online, "0-15\n")
	write(boot, uuid.NewString())
	config := FileWorkerInstanceEpochStoreConfig{Directory: filepath.Join(directory, "state"), NodeIdentity: "node", BootIDPath: boot}
	store, err := NewFileWorkerInstanceEpochStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	probe := &CPUDeviceProbe{NodeIdentity: "node", OnlineCPUsPath: online, Epochs: store}
	device := ExpectedWorkerDevice{Kind: "CPU", DeviceID: uuid.New(), ComputeNodeID: uuid.New(), NodeIdentity: "node"}
	check := func(want int64) AttestedWorkerDevice {
		t.Helper()
		got, err := probe.AttestWorkerInstanceDevices(t.Context(), []ExpectedWorkerDevice{device})
		if err != nil || len(got) != 1 || got[0].DeviceEpoch != want || got[0].GPUUUID != "" || got[0].PCIBDF != "" {
			t.Fatalf("CPU evidence: %v %v", got, err)
		}
		return got[0]
	}
	first := check(1)
	check(1)
	write(online, "0-7")
	check(2)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewFileWorkerInstanceEpochStore(config)
	if err != nil {
		t.Fatal(err)
	}
	probe.Epochs = store
	if got := check(2); got.NodeEpoch != first.NodeEpoch || got.AgentSessionEpoch != first.AgentSessionEpoch+1 {
		t.Fatal("restart did not retain device and advance agent")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	write(boot, uuid.NewString())
	store, err = NewFileWorkerInstanceEpochStore(config)
	if err != nil {
		t.Fatal(err)
	}
	probe.Epochs = store
	if got := check(3); got.NodeEpoch != first.NodeEpoch+1 {
		t.Fatal("boot did not advance node and slot")
	}
	for _, fault := range []string{"gpu", "node", "duplicate", "kind", "invalid-online"} {
		t.Run(fault, func(t *testing.T) {
			bad := device
			items := []ExpectedWorkerDevice{bad}
			switch fault {
			case "gpu":
				items[0].GPUUUID = "GPU-00000000-0000-0000-0000-000000000001"
			case "node":
				items[0].NodeIdentity = "other"
			case "duplicate":
				items = append(items, bad)
			case "kind":
				items[0].Kind = "GPU"
			case "invalid-online":
				write(online, "0-7,3")
			}
			if got, err := probe.AttestWorkerInstanceDevices(t.Context(), items); err == nil || len(got) != 0 {
				t.Fatal("accepted invalid CPU evidence")
			}
		})
	}
}
