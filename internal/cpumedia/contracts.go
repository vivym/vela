package cpumedia

import (
	"errors"
	"fmt"
	"slices"
)

type Stage string

const (
	StageEncode    Stage = "ENCODE"
	StageMux       Stage = "MUX"
	StageThumbnail Stage = "THUMBNAIL"
)

const (
	EncodeProfile    = "h3-cpu-encode"
	MuxProfile       = "h3-cpu-mux"
	ThumbnailProfile = "h3-cpu-thumbnail"

	EncodeWorkerProfile    = "h3-cpu-encode-worker"
	MuxWorkerProfile       = "h3-cpu-mux-worker"
	ThumbnailWorkerProfile = "h3-cpu-thumbnail-worker"

	EncodeCapacityPool    = "h3-cpu-encode-capacity"
	MuxCapacityPool       = "h3-cpu-mux-capacity"
	ThumbnailCapacityPool = "h3-cpu-thumbnail-capacity"
)

type ResourceVector struct {
	CPUMilli     int64
	MemoryBytes  int64
	ScratchBytes int64
	Concurrency  int64
}

type Profile struct {
	StableID                  string
	Stage                     Stage
	WorkerProfileStableID     string
	CapacityPoolStableID      string
	RuntimeAdapterRevision    string
	InputInterfaceStableID    string
	OutputInterfaceStableID   string
	ResultEquivalenceStableID string
	ResourceClass             string
	DeviceCount               int
	MemberCount               int
	CapacityLimits            ResourceVector
}

func ProductionProfiles() ([]Profile, error) {
	profiles := []Profile{
		{
			StableID: EncodeProfile, Stage: StageEncode,
			WorkerProfileStableID:  EncodeWorkerProfile,
			CapacityPoolStableID:   EncodeCapacityPool,
			RuntimeAdapterRevision: "ffmpeg-encode-v1",
			InputInterfaceStableID: "h3-frame-bundle", OutputInterfaceStableID: "h3-encoded-video",
			ResultEquivalenceStableID: "h3-encode-exact", ResourceClass: "CPU",
			DeviceCount: 1, MemberCount: 1,
			CapacityLimits: ResourceVector{
				CPUMilli: 4_000, MemoryBytes: 8 << 30, ScratchBytes: 128 << 30, Concurrency: 1,
			},
		},
		{
			StableID: MuxProfile, Stage: StageMux,
			WorkerProfileStableID:  MuxWorkerProfile,
			CapacityPoolStableID:   MuxCapacityPool,
			RuntimeAdapterRevision: "ffmpeg-mux-v1",
			InputInterfaceStableID: "h3-encoded-video", OutputInterfaceStableID: "h3-video",
			ResultEquivalenceStableID: "h3-mux-exact", ResourceClass: "CPU",
			DeviceCount: 1, MemberCount: 1,
			CapacityLimits: ResourceVector{
				CPUMilli: 2_000, MemoryBytes: 4 << 30, ScratchBytes: 128 << 30, Concurrency: 1,
			},
		},
		{
			StableID: ThumbnailProfile, Stage: StageThumbnail,
			WorkerProfileStableID:  ThumbnailWorkerProfile,
			CapacityPoolStableID:   ThumbnailCapacityPool,
			RuntimeAdapterRevision: "ffmpeg-thumbnail-v1",
			InputInterfaceStableID: "h3-frame-bundle", OutputInterfaceStableID: "h3-thumbnail",
			ResultEquivalenceStableID: "h3-thumbnail-exact", ResourceClass: "CPU",
			DeviceCount: 1, MemberCount: 1,
			CapacityLimits: ResourceVector{
				CPUMilli: 2_000, MemoryBytes: 4 << 30, ScratchBytes: 32 << 30, Concurrency: 1,
			},
		},
	}
	if err := validateProfiles(profiles); err != nil {
		return nil, err
	}
	return slices.Clone(profiles), nil
}

func productionProfile(stableID string) (Profile, error) {
	profiles, err := ProductionProfiles()
	if err != nil {
		return Profile{}, err
	}
	for _, profile := range profiles {
		if profile.StableID == stableID {
			return profile, nil
		}
	}
	return Profile{}, fmt.Errorf("unsupported CPU media StageProfile %q", stableID)
}

func validateProfiles(profiles []Profile) error {
	if len(profiles) == 0 {
		return errors.New("CPU media profiles are empty")
	}
	stages := make(map[Stage]struct{}, len(profiles))
	workers := make(map[string]struct{}, len(profiles))
	pools := make(map[string]struct{}, len(profiles))
	for _, profile := range profiles {
		limits := profile.CapacityLimits
		if profile.StableID == "" || profile.Stage == "" ||
			profile.WorkerProfileStableID == "" || profile.CapacityPoolStableID == "" ||
			profile.RuntimeAdapterRevision == "" || profile.InputInterfaceStableID == "" ||
			profile.OutputInterfaceStableID == "" || profile.ResultEquivalenceStableID == "" ||
			profile.ResourceClass != "CPU" || profile.DeviceCount != 1 || profile.MemberCount != 1 ||
			limits.CPUMilli <= 0 || limits.MemoryBytes <= 0 || limits.ScratchBytes <= 0 ||
			limits.Concurrency <= 0 {
			return fmt.Errorf("CPU media profile %q is incomplete or unbounded", profile.StableID)
		}
		if _, exists := stages[profile.Stage]; exists {
			return fmt.Errorf("CPU media stage %q has multiple profiles", profile.Stage)
		}
		if _, exists := workers[profile.WorkerProfileStableID]; exists {
			return fmt.Errorf("CPU media WorkerProfile %q is shared", profile.WorkerProfileStableID)
		}
		if _, exists := pools[profile.CapacityPoolStableID]; exists {
			return fmt.Errorf("CPU media CapacityPool %q is shared", profile.CapacityPoolStableID)
		}
		stages[profile.Stage] = struct{}{}
		workers[profile.WorkerProfileStableID] = struct{}{}
		pools[profile.CapacityPoolStableID] = struct{}{}
	}
	return nil
}
