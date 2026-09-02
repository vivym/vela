package cpumedia_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/vivym/vela/internal/cpumedia"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const encodeProfileRevisionID = "49700000-0000-0000-0000-000000000041"

func TestProfilesBoundEveryCPUWorkerResourceIndependently(t *testing.T) {
	profiles, err := cpumedia.ProductionProfiles()
	if err != nil {
		t.Fatalf("ProductionProfiles: %v", err)
	}
	if len(profiles) != 3 {
		t.Fatalf("CPU media profile count = %d, want 3", len(profiles))
	}

	seenStages := map[cpumedia.Stage]bool{}
	seenWorkers := map[string]bool{}
	seenPools := map[string]bool{}
	for _, profile := range profiles {
		limits := profile.CapacityLimits
		if limits.CPUMilli <= 0 || limits.MemoryBytes <= 0 ||
			limits.ScratchBytes <= 0 || limits.Concurrency <= 0 {
			t.Fatalf("profile %s has unbounded resources: %#v", profile.StableID, limits)
		}
		if profile.ResourceClass != "CPU" || profile.DeviceCount != 1 ||
			profile.MemberCount != 1 || profile.InputInterfaceStableID == "" ||
			profile.OutputInterfaceStableID == "" {
			t.Fatalf("profile %s is not a complete CPU StageProfile: %#v", profile.StableID, profile)
		}
		if seenStages[profile.Stage] || seenWorkers[profile.WorkerProfileStableID] ||
			seenPools[profile.CapacityPoolStableID] {
			t.Fatalf("CPU stage ownership is shared: %#v", profile)
		}
		seenStages[profile.Stage] = true
		seenWorkers[profile.WorkerProfileStableID] = true
		seenPools[profile.CapacityPoolStableID] = true
	}
	for _, stage := range []cpumedia.Stage{
		cpumedia.StageEncode, cpumedia.StageMux, cpumedia.StageThumbnail,
	} {
		if !seenStages[stage] {
			t.Fatalf("missing CPU media stage %s", stage)
		}
	}
}

func TestAdapterRejectsCapacityDriftBeforeCallingCPUDriver(t *testing.T) {
	profile := requireCPUProfile(t, cpumedia.EncodeProfile)
	driver := &recordingDriver{resources: profile.CapacityLimits}
	adapter, err := cpumedia.NewAdapter(cpumedia.AdapterConfig{
		ProfileStableID: profile.StableID, StageProfileRevisionID: encodeProfileRevisionID,
		Driver: driver,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	driver.resources.ScratchBytes++

	err = adapter.Prepare(
		context.Background(), verifiedCPUAuthority(encodeProfileRevisionID),
		&velav1.StageExecutionSpec{ParametersJson: []byte(`{"preset":"balanced"}`)},
	)
	if err == nil || driver.prepareCalls != 0 {
		t.Fatalf("capacity drift Prepare error=%v driver_calls=%d", err, driver.prepareCalls)
	}
}

func TestCPUAdapterExecutesTrackedStageWithoutModelResidencyCommands(t *testing.T) {
	profile := requireCPUProfile(t, cpumedia.EncodeProfile)
	driver := &recordingDriver{
		resources: profile.CapacityLimits,
		manifest:  []byte(`{"kind":"encoded-video","version":1}`),
	}
	adapter, err := cpumedia.NewAdapter(cpumedia.AdapterConfig{
		ProfileStableID: profile.StableID, StageProfileRevisionID: encodeProfileRevisionID,
		Driver: driver,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	verified := verifiedCPUAuthority(encodeProfileRevisionID)
	spec := &velav1.StageExecutionSpec{
		Inputs: []*velav1.StageInputArtifact{{
			StageArtifactId: "49700000-0000-0000-0000-000000000001",
			ObjectVersion:   "frames-v1", Sha256: make([]byte, sha256.Size), SizeBytes: 4096,
			StageInterfaceRevisionId: "49700000-0000-0000-0000-000000000011",
		}},
		ParametersJson:             []byte(`{"codec":"h264"}`),
		ExpectedOutputManifestJson: []byte(`{"kind":"encoded-video"}`),
	}
	if err := adapter.Prepare(context.Background(), verified, spec); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := adapter.Start(context.Background(), verified); err != nil {
		t.Fatalf("Start: %v", err)
	}
	sealed, err := adapter.Seal(context.Background(), verified)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if !json.Valid(sealed.OutputManifestJSON) || sealed.TotalSizeBytes != 8192 ||
		driver.prepareCalls != 1 || driver.startCalls != 1 || driver.sealCalls != 1 {
		t.Fatalf("CPU stage result=%#v driver=%#v", sealed, driver)
	}
}

func requireCPUProfile(t *testing.T, stableID string) cpumedia.Profile {
	t.Helper()
	profiles, err := cpumedia.ProductionProfiles()
	if err != nil {
		t.Fatalf("ProductionProfiles: %v", err)
	}
	for _, profile := range profiles {
		if profile.StableID == stableID {
			return profile
		}
	}
	t.Fatalf("missing CPU media profile %s", stableID)
	return cpumedia.Profile{}
}

func verifiedCPUAuthority(profileRevisionID string) stageauthority.Verified {
	authority := &velav1.StageAuthority{
		StageProfileRevisionId: profileRevisionID,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49700000-0000-0000-0000-000000000002", DeviceEpoch: 1,
		}},
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "49700000-0000-0000-0000-000000000003",
			MemberEpoch:    1, ModelRuntimeEpoch: 1,
			IdentityDigest: bytes.Repeat([]byte{0x73}, 32),
		}},
		StageLeaseId:   "49700000-0000-0000-0000-000000000004",
		StageRunId:     "49700000-0000-0000-0000-000000000005",
		StageAttemptId: "49700000-0000-0000-0000-000000000006",
		AttemptFence:   1, StageFence: 1, StageVersion: 1,
	}
	return stageauthority.Verified{
		Authority: authority, Digest: sha256.Sum256([]byte("cpu-stage-authority")),
	}
}

type recordingDriver struct {
	resources    cpumedia.ResourceVector
	manifest     []byte
	prepareCalls int
	startCalls   int
	sealCalls    int
	prepared     bool
	started      bool
}

func (driver *recordingDriver) Resources() cpumedia.ResourceVector { return driver.resources }

func (driver *recordingDriver) Probe(
	context.Context,
	velav1.ModelRuntimeReadinessCheck,
) (modelruntime.ProbeResult, error) {
	return modelruntime.ProbeResult{Ready: true}, nil
}

func (driver *recordingDriver) Prepare(
	_ context.Context,
	_ cpumedia.ExecutionIdentity,
	spec *velav1.StageExecutionSpec,
) error {
	driver.prepareCalls++
	if spec == nil || !json.Valid(spec.GetParametersJson()) {
		return errors.New("invalid CPU execution spec")
	}
	driver.prepared = true
	return nil
}

func (driver *recordingDriver) Start(context.Context, cpumedia.ExecutionIdentity) error {
	driver.startCalls++
	if !driver.prepared {
		return errors.New("CPU stage is not prepared")
	}
	driver.started = true
	return nil
}

func (driver *recordingDriver) Cancel(
	context.Context,
	cpumedia.ExecutionIdentity,
	velav1.ModelRuntimeCancelReason,
) error {
	driver.started = false
	return nil
}

func (driver *recordingDriver) Status(
	context.Context,
	cpumedia.ExecutionIdentity,
) (modelruntime.BackendStatus, error) {
	return modelruntime.BackendStatus{
		State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
	}, nil
}

func (driver *recordingDriver) Seal(
	context.Context,
	cpumedia.ExecutionIdentity,
) (modelruntime.SealedOutput, error) {
	driver.sealCalls++
	if !driver.started {
		return modelruntime.SealedOutput{}, errors.New("CPU stage is not started")
	}
	return modelruntime.SealedOutput{
		OutputManifestJSON: append([]byte(nil), driver.manifest...), TotalSizeBytes: 8192,
	}, nil
}
