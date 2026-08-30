package h3stage

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type Stage string

const (
	StageEncoder    Stage = "ENCODER"
	StageDiT        Stage = "DIT"
	StageVAEDecoder Stage = "VAE_DECODER"
)

const (
	EncoderWorkerProfile = "h3-encoder-worker-single-gpu"
	DiTWorkerProfile     = "h3-dit-worker-single-gpu"
	VAEWorkerProfile     = "h3-vae-worker-single-gpu"
	AUXWorkerProfile     = "h3-encoder-vae-aux-single-gpu"

	EncoderSingleGPUProfile = "h3-encoder-single-gpu"
	DiTSingleGPUProfile     = "h3-dit-single-gpu"
	VAESingleGPUProfile     = "h3-vae-single-gpu"
	EncoderAUXProfile       = "h3-encoder-aux-single-gpu"
	VAEAUXProfile           = "h3-vae-aux-single-gpu"

	EncoderCapacityPool = "h3-encoder-capacity"
	DiTCapacityPool     = "h3-dit-capacity"
	VAECapacityPool     = "h3-vae-capacity"
	AUXCapacityPool     = "h3-encoder-vae-aux-capacity"
)

type StageProfile struct {
	StableID                  string
	Stage                     Stage
	WorkerProfileStableID     string
	CapacityPoolStableID      string
	ModelComponentRevision    string
	RuntimeAdapterRevision    string
	InputInterfaceStableIDs   []string
	OutputInterfaceStableID   string
	ResultEquivalenceStableID string
	PublicPhase               string
	DeviceCount               int
	ActiveStageSlots          int
	ResidentStages            []Stage
}

type DevicePlacement struct {
	ID      string
	GPUUUID string
	PCIBDF  string
}

type NodeDevicePlacement struct {
	NodeIdentity string
	Device       DevicePlacement
}

type WorkerPlacement struct {
	StableID              string
	NodeIdentity          string
	WorkerProfileStableID string
	Devices               []DevicePlacement
	AllowedStageProfiles  []string
	ActiveStageSlots      int
}

type Layout struct {
	Workers []WorkerPlacement
}

func ProductionProfiles() ([]StageProfile, error) {
	profiles := []StageProfile{
		standardProfile(
			EncoderSingleGPUProfile, StageEncoder, EncoderWorkerProfile, EncoderCapacityPool,
			"h3-encoder-v1", nil, "h3-conditioning", "h3-encoder-exact", "PREPARING",
		),
		standardProfile(
			DiTSingleGPUProfile, StageDiT, DiTWorkerProfile, DiTCapacityPool,
			"h3-dit-v1", []string{"h3-conditioning"}, "h3-latent", "h3-dit-exact",
			"GENERATING",
		),
		standardProfile(
			VAESingleGPUProfile, StageVAEDecoder, VAEWorkerProfile, VAECapacityPool,
			"h3-vae-v1", []string{"h3-latent"}, "h3-video", "h3-vae-exact", "GENERATING",
		),
		auxProfile(EncoderAUXProfile, StageEncoder),
		auxProfile(VAEAUXProfile, StageVAEDecoder),
	}
	if err := validateProfiles(profiles); err != nil {
		return nil, err
	}
	return cloneProfiles(profiles), nil
}

func CurrentSameNodeLayout(nodeIdentity string, devices []DevicePlacement) (Layout, error) {
	if strings.TrimSpace(nodeIdentity) == "" {
		return Layout{}, errors.New("H3 layout node identity is required")
	}
	if len(devices) != 8 {
		return Layout{}, fmt.Errorf("current H3 same-node layout requires exactly 8 GPUs, got %d", len(devices))
	}
	seenIDs := make(map[string]struct{}, len(devices))
	seenGPUUUIDs := make(map[string]struct{}, len(devices))
	seenBDFs := make(map[string]struct{}, len(devices))
	for _, device := range devices {
		if strings.TrimSpace(device.ID) == "" || strings.TrimSpace(device.GPUUUID) == "" ||
			strings.TrimSpace(device.PCIBDF) == "" {
			return Layout{}, errors.New("H3 layout device identity is incomplete")
		}
		if duplicate(seenIDs, device.ID) || duplicate(seenGPUUUIDs, device.GPUUUID) ||
			duplicate(seenBDFs, device.PCIBDF) {
			return Layout{}, errors.New("H3 layout cannot share a device between WorkerInstances")
		}
	}

	workers := make([]WorkerPlacement, 0, len(devices))
	workers = append(workers, WorkerPlacement{
		StableID: nodeIdentity + "/aux-0", NodeIdentity: nodeIdentity,
		WorkerProfileStableID: AUXWorkerProfile,
		Devices:               []DevicePlacement{devices[0]},
		AllowedStageProfiles:  []string{EncoderAUXProfile, VAEAUXProfile},
		ActiveStageSlots:      1,
	})
	for index, device := range devices[1:] {
		workers = append(workers, WorkerPlacement{
			StableID: nodeIdentity + fmt.Sprintf("/dit-%d", index), NodeIdentity: nodeIdentity,
			WorkerProfileStableID: DiTWorkerProfile,
			Devices:               []DevicePlacement{device},
			AllowedStageProfiles:  []string{DiTSingleGPUProfile},
			ActiveStageSlots:      1,
		})
	}
	return Layout{Workers: cloneWorkers(workers)}, nil
}

func CrossNodeLayout(
	encoderDevices []NodeDevicePlacement,
	ditDevices []NodeDevicePlacement,
	vaeDevices []NodeDevicePlacement,
) (Layout, error) {
	if len(encoderDevices) == 0 || len(ditDevices) == 0 || len(vaeDevices) == 0 {
		return Layout{}, errors.New("cross-node H3 layout requires Encoder, DiT, and VAE capacity")
	}
	seenIDs := make(map[string]struct{})
	seenGPUUUIDs := make(map[string]struct{})
	seenBDFs := make(map[string]struct{})
	workers := make([]WorkerPlacement, 0, len(encoderDevices)+len(ditDevices)+len(vaeDevices))
	appendWorkers := func(
		placements []NodeDevicePlacement,
		stageLabel string,
		workerProfile string,
		stageProfile string,
	) error {
		for index, placement := range placements {
			device := placement.Device
			if strings.TrimSpace(placement.NodeIdentity) == "" ||
				strings.TrimSpace(device.ID) == "" || strings.TrimSpace(device.GPUUUID) == "" ||
				strings.TrimSpace(device.PCIBDF) == "" {
				return errors.New("cross-node H3 layout device identity is incomplete")
			}
			if duplicate(seenIDs, device.ID) || duplicate(seenGPUUUIDs, device.GPUUUID) ||
				duplicate(seenBDFs, device.PCIBDF) {
				return errors.New("cross-node H3 layout cannot share a device between WorkerInstances")
			}
			workers = append(workers, WorkerPlacement{
				StableID:              placement.NodeIdentity + fmt.Sprintf("/%s-%d", stageLabel, index),
				NodeIdentity:          placement.NodeIdentity,
				WorkerProfileStableID: workerProfile,
				Devices:               []DevicePlacement{device},
				AllowedStageProfiles:  []string{stageProfile},
				ActiveStageSlots:      1,
			})
		}
		return nil
	}
	if err := appendWorkers(
		encoderDevices, "encoder", EncoderWorkerProfile, EncoderSingleGPUProfile,
	); err != nil {
		return Layout{}, err
	}
	if err := appendWorkers(ditDevices, "dit", DiTWorkerProfile, DiTSingleGPUProfile); err != nil {
		return Layout{}, err
	}
	if err := appendWorkers(vaeDevices, "vae", VAEWorkerProfile, VAESingleGPUProfile); err != nil {
		return Layout{}, err
	}
	return Layout{Workers: cloneWorkers(workers)}, nil
}

func standardProfile(
	stableID string,
	stage Stage,
	workerProfile string,
	capacityPool string,
	modelComponent string,
	inputs []string,
	output string,
	equivalence string,
	publicPhase string,
) StageProfile {
	return StageProfile{
		StableID: stableID, Stage: stage, WorkerProfileStableID: workerProfile,
		CapacityPoolStableID: capacityPool, ModelComponentRevision: modelComponent,
		RuntimeAdapterRevision:  "h3-stage-runtime-adapter-v1",
		InputInterfaceStableIDs: slices.Clone(inputs), OutputInterfaceStableID: output,
		ResultEquivalenceStableID: equivalence, PublicPhase: publicPhase,
		DeviceCount: 1, ActiveStageSlots: 1, ResidentStages: []Stage{stage},
	}
}

func auxProfile(stableID string, stage Stage) StageProfile {
	var profile StageProfile
	if stage == StageEncoder {
		profile = standardProfile(
			stableID, stage, AUXWorkerProfile, AUXCapacityPool, "h3-encoder-v1", nil,
			"h3-conditioning", "h3-encoder-exact", "PREPARING",
		)
	} else {
		profile = standardProfile(
			stableID, stage, AUXWorkerProfile, AUXCapacityPool, "h3-vae-v1",
			[]string{"h3-latent"}, "h3-video", "h3-vae-exact", "GENERATING",
		)
	}
	profile.ResidentStages = []Stage{StageEncoder, StageVAEDecoder}
	return profile
}

func validateProfiles(profiles []StageProfile) error {
	seen := make(map[string]struct{}, len(profiles))
	for _, profile := range profiles {
		if strings.TrimSpace(profile.StableID) == "" ||
			strings.TrimSpace(profile.WorkerProfileStableID) == "" ||
			strings.TrimSpace(profile.CapacityPoolStableID) == "" ||
			strings.TrimSpace(profile.ModelComponentRevision) == "" ||
			strings.TrimSpace(profile.RuntimeAdapterRevision) == "" ||
			strings.TrimSpace(profile.OutputInterfaceStableID) == "" ||
			strings.TrimSpace(profile.ResultEquivalenceStableID) == "" ||
			(profile.PublicPhase != "PREPARING" && profile.PublicPhase != "GENERATING") ||
			profile.DeviceCount != 1 || profile.ActiveStageSlots != 1 ||
			len(profile.ResidentStages) == 0 {
			return errors.New("H3 StageProfile contract is invalid")
		}
		if duplicate(seen, profile.StableID) {
			return errors.New("H3 StageProfile stable identity is duplicated")
		}
		if !slices.Contains(profile.ResidentStages, profile.Stage) {
			return errors.New("H3 StageProfile does not contain its resident stage")
		}
		if profile.Stage == StageDiT && (profile.WorkerProfileStableID != DiTWorkerProfile ||
			len(profile.ResidentStages) != 1) {
			return errors.New("H3 DiT must remain an independent single-GPU resident stage")
		}
	}
	return nil
}

func duplicate(seen map[string]struct{}, value string) bool {
	if _, ok := seen[value]; ok {
		return true
	}
	seen[value] = struct{}{}
	return false
}

func cloneProfiles(profiles []StageProfile) []StageProfile {
	cloned := slices.Clone(profiles)
	for index := range cloned {
		cloned[index].ResidentStages = slices.Clone(cloned[index].ResidentStages)
		cloned[index].InputInterfaceStableIDs = slices.Clone(cloned[index].InputInterfaceStableIDs)
	}
	return cloned
}

func cloneWorkers(workers []WorkerPlacement) []WorkerPlacement {
	cloned := slices.Clone(workers)
	for index := range cloned {
		cloned[index].Devices = slices.Clone(cloned[index].Devices)
		cloned[index].AllowedStageProfiles = slices.Clone(cloned[index].AllowedStageProfiles)
	}
	return cloned
}
