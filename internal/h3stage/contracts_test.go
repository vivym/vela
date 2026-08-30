package h3stage_test

import (
	"fmt"
	"testing"

	"github.com/vivym/vela/internal/h3stage"
)

func TestProductionProfilesKeepH3DiTAndAUXOwnershipExplicit(t *testing.T) {
	profiles, err := h3stage.ProductionProfiles()
	if err != nil {
		t.Fatalf("ProductionProfiles: %v", err)
	}

	dit := requireProfile(t, profiles, h3stage.DiTSingleGPUProfile)
	if dit.Stage != h3stage.StageDiT || dit.DeviceCount != 1 || dit.ActiveStageSlots != 1 ||
		len(dit.ResidentStages) != 1 || dit.ResidentStages[0] != h3stage.StageDiT {
		t.Fatalf("single-GPU DiT profile = %#v", dit)
	}

	encoder := requireProfile(t, profiles, h3stage.EncoderSingleGPUProfile)
	vae := requireProfile(t, profiles, h3stage.VAESingleGPUProfile)
	if encoder.WorkerProfileStableID == vae.WorkerProfileStableID ||
		encoder.CapacityPoolStableID == dit.CapacityPoolStableID ||
		encoder.CapacityPoolStableID == vae.CapacityPoolStableID ||
		dit.CapacityPoolStableID == vae.CapacityPoolStableID {
		t.Fatalf("standard component profiles do not have independent ownership: %#v %#v %#v", encoder, dit, vae)
	}
	if encoder.PublicPhase != "PREPARING" || dit.PublicPhase != "GENERATING" ||
		vae.PublicPhase != "GENERATING" || encoder.OutputInterfaceStableID != "h3-conditioning" ||
		dit.OutputInterfaceStableID != "h3-latent" || vae.OutputInterfaceStableID != "h3-video" {
		t.Fatalf("H3 phase/interface mapping = %#v %#v %#v", encoder, dit, vae)
	}

	encoderAUX := requireProfile(t, profiles, h3stage.EncoderAUXProfile)
	vaeAUX := requireProfile(t, profiles, h3stage.VAEAUXProfile)
	for _, profile := range []h3stage.StageProfile{encoderAUX, vaeAUX} {
		if profile.WorkerProfileStableID != h3stage.AUXWorkerProfile ||
			profile.DeviceCount != 1 || profile.ActiveStageSlots != 1 ||
			len(profile.ResidentStages) != 2 ||
			profile.ResidentStages[0] != h3stage.StageEncoder ||
			profile.ResidentStages[1] != h3stage.StageVAEDecoder {
			t.Fatalf("AUX StageProfile = %#v", profile)
		}
	}
}

func TestCurrentSameNodeLayoutIsOneAUXPlusSevenIndependentDiTWorkers(t *testing.T) {
	devices := make([]h3stage.DevicePlacement, 8)
	for index := range devices {
		devices[index] = h3stage.DevicePlacement{
			ID:      fmt.Sprintf("device-%d", index),
			GPUUUID: fmt.Sprintf("GPU-00000000-0000-0000-0000-%012d", index),
			PCIBDF:  fmt.Sprintf("0000:%02x:00.0", index+1),
		}
	}

	layout, err := h3stage.CurrentSameNodeLayout("h3-node-01", devices)
	if err != nil {
		t.Fatalf("CurrentSameNodeLayout: %v", err)
	}
	if len(layout.Workers) != 8 {
		t.Fatalf("WorkerInstance count = %d, want 8", len(layout.Workers))
	}

	auxWorkers := 0
	ditWorkers := 0
	seenDevices := make(map[string]struct{}, len(devices))
	for _, worker := range layout.Workers {
		if worker.NodeIdentity != "h3-node-01" || len(worker.Devices) != 1 {
			t.Fatalf("Worker placement = %#v", worker)
		}
		deviceID := worker.Devices[0].ID
		if _, duplicated := seenDevices[deviceID]; duplicated {
			t.Fatalf("device %q is shared by multiple WorkerInstances", deviceID)
		}
		seenDevices[deviceID] = struct{}{}
		switch worker.WorkerProfileStableID {
		case h3stage.AUXWorkerProfile:
			auxWorkers++
			if worker.ActiveStageSlots != 1 {
				t.Fatalf("AUX active slots = %d, want 1", worker.ActiveStageSlots)
			}
		case h3stage.DiTWorkerProfile:
			ditWorkers++
			if len(worker.AllowedStageProfiles) != 1 ||
				worker.AllowedStageProfiles[0] != h3stage.DiTSingleGPUProfile {
				t.Fatalf("DiT Worker profile allowlist = %v", worker.AllowedStageProfiles)
			}
		default:
			t.Fatalf("unexpected WorkerProfile %q", worker.WorkerProfileStableID)
		}
	}
	if auxWorkers != 1 || ditWorkers != 7 {
		t.Fatalf("layout AUX/DiT workers = %d/%d, want 1/7", auxWorkers, ditWorkers)
	}
}

func TestCrossNodeLayoutScalesEncoderDiTAndVAEIndependently(t *testing.T) {
	encoderDevices := []h3stage.NodeDevicePlacement{
		{NodeIdentity: "encoder-node-01", Device: testDevice(0)},
		{NodeIdentity: "encoder-node-02", Device: testDevice(1)},
	}
	ditDevices := []h3stage.NodeDevicePlacement{
		{NodeIdentity: "dit-node-01", Device: testDevice(2)},
		{NodeIdentity: "dit-node-02", Device: testDevice(3)},
		{NodeIdentity: "dit-node-03", Device: testDevice(4)},
	}
	vaeDevices := []h3stage.NodeDevicePlacement{
		{NodeIdentity: "vae-node-01", Device: testDevice(5)},
	}

	layout, err := h3stage.CrossNodeLayout(encoderDevices, ditDevices, vaeDevices)
	if err != nil {
		t.Fatalf("CrossNodeLayout: %v", err)
	}
	if len(layout.Workers) != 6 {
		t.Fatalf("WorkerInstance count = %d, want 6", len(layout.Workers))
	}

	counts := map[string]int{}
	seenDevices := map[string]struct{}{}
	for _, worker := range layout.Workers {
		counts[worker.WorkerProfileStableID]++
		if len(worker.Devices) != 1 || worker.ActiveStageSlots != 1 {
			t.Fatalf("cross-node WorkerInstance = %#v", worker)
		}
		if _, duplicated := seenDevices[worker.Devices[0].GPUUUID]; duplicated {
			t.Fatalf("GPU %q is shared", worker.Devices[0].GPUUUID)
		}
		seenDevices[worker.Devices[0].GPUUUID] = struct{}{}
		if worker.WorkerProfileStableID == h3stage.DiTWorkerProfile &&
			(len(worker.AllowedStageProfiles) != 1 ||
				worker.AllowedStageProfiles[0] != h3stage.DiTSingleGPUProfile) {
			t.Fatalf("DiT WorkerInstance profile allowlist = %v", worker.AllowedStageProfiles)
		}
	}
	if counts[h3stage.EncoderWorkerProfile] != 2 ||
		counts[h3stage.DiTWorkerProfile] != 3 ||
		counts[h3stage.VAEWorkerProfile] != 1 {
		t.Fatalf("independent Encoder/DiT/VAE capacity = %#v", counts)
	}
}

func testDevice(index int) h3stage.DevicePlacement {
	return h3stage.DevicePlacement{
		ID:      fmt.Sprintf("cross-device-%d", index),
		GPUUUID: fmt.Sprintf("GPU-10000000-0000-0000-0000-%012d", index),
		PCIBDF:  fmt.Sprintf("0000:%02x:00.0", index+32),
	}
}

func requireProfile(
	t *testing.T,
	profiles []h3stage.StageProfile,
	stableID string,
) h3stage.StageProfile {
	t.Helper()
	for _, profile := range profiles {
		if profile.StableID == stableID {
			return profile
		}
	}
	t.Fatalf("missing StageProfile %q", stableID)
	return h3stage.StageProfile{}
}
