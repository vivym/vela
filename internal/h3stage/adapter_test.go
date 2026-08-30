package h3stage_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/vivym/vela/internal/h3stage"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const ditProfileRevisionID = "49000000-0000-0000-0000-000000000041"

func TestAdapterRequiresAnAlreadyWarmExactResidentComponent(t *testing.T) {
	for _, test := range []struct {
		name      string
		residency h3stage.Residency
	}{
		{
			name: "cold",
			residency: h3stage.Residency{
				Stage: h3stage.StageDiT, StageProfileRevisionID: ditProfileRevisionID,
				ModelRevision: "minimax-h3@sha256:model", BackendRevision: "sglang@sha256:backend",
			},
		},
		{
			name: "wrong component",
			residency: h3stage.Residency{
				Stage: h3stage.StageEncoder, StageProfileRevisionID: ditProfileRevisionID, Warm: true,
				ModelRevision: "minimax-h3@sha256:model", BackendRevision: "sglang@sha256:backend",
			},
		},
		{
			name: "wrong profile",
			residency: h3stage.Residency{
				Stage:                  h3stage.StageDiT,
				StageProfileRevisionID: "49000000-0000-0000-0000-000000000099", Warm: true,
				ModelRevision: "minimax-h3@sha256:model", BackendRevision: "sglang@sha256:backend",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := h3stage.NewAdapter(h3stage.AdapterConfig{
				ProfileStableID:        h3stage.DiTSingleGPUProfile,
				StageProfileRevisionID: ditProfileRevisionID,
				Driver:                 &deterministicDriver{residency: test.residency},
			})
			if err == nil {
				t.Fatal("NewAdapter accepted mismatched or cold residency")
			}
		})
	}
}

func TestDiTAdapterRejectsMultiGPUAssignmentBeforeCallingTheDriver(t *testing.T) {
	driver := newWarmDiTDriver("node-a")
	adapter := newDiTAdapter(t, driver)
	authority := ditAuthority(2)

	err := adapter.Prepare(
		context.Background(),
		stageauthority.Verified{Authority: authority, Digest: sha256.Sum256([]byte("authority"))},
		&velav1.StageExecutionSpec{ParametersJson: []byte(`{"seed":7}`)},
	)
	if err == nil || driver.prepareCalls != 0 {
		t.Fatalf("multi-GPU DiT Prepare error=%v driver_calls=%d", err, driver.prepareCalls)
	}
}

func TestAdapterRejectsResidencyDriftBeforeAssignment(t *testing.T) {
	driver := newWarmDiTDriver("node-a")
	adapter := newDiTAdapter(t, driver)
	driver.residency.Warm = false

	err := adapter.Prepare(
		context.Background(),
		stageauthority.Verified{
			Authority: ditAuthority(1), Digest: sha256.Sum256([]byte("authority")),
		},
		&velav1.StageExecutionSpec{ParametersJson: []byte(`{"seed":7}`)},
	)
	if err == nil || driver.prepareCalls != 0 {
		t.Fatalf("drifted residency Prepare error=%v driver_calls=%d", err, driver.prepareCalls)
	}
}

func TestSameNodeAndCrossNodeAdaptersProduceIdenticalCertifiedBytes(t *testing.T) {
	placements := []string{"h3-node-01", "h3-node-09"}
	var reference []byte
	for _, placement := range placements {
		driver := newWarmDiTDriver(placement)
		adapter := newDiTAdapter(t, driver)
		authority := ditAuthority(1)
		verified := stageauthority.Verified{
			Authority: authority, Digest: sha256.Sum256([]byte("certified-authority")),
		}
		spec := &velav1.StageExecutionSpec{
			Inputs: []*velav1.StageInputArtifact{{
				StageArtifactId: "49600000-0000-0000-0000-000000000001",
				ObjectVersion:   "conditioning-v1", Sha256: bytes.Repeat([]byte{0x51}, 32),
				SizeBytes:                4096,
				StageInterfaceRevisionId: "49000000-0000-0000-0000-000000000011",
			}},
			ParametersJson:             []byte(`{"prompt_digest":"sha256:certified","seed":7}`),
			ExpectedOutputManifestJson: []byte(`{"kind":"h3-latent","version":1}`),
		}
		if err := adapter.Prepare(context.Background(), verified, spec); err != nil {
			t.Fatalf("%s Prepare: %v", placement, err)
		}
		if err := adapter.Start(context.Background(), verified); err != nil {
			t.Fatalf("%s Start: %v", placement, err)
		}
		sealed, err := adapter.Seal(context.Background(), verified)
		if err != nil {
			t.Fatalf("%s Seal: %v", placement, err)
		}
		if reference == nil {
			reference = sealed.OutputManifestJSON
			continue
		}
		if !bytes.Equal(reference, sealed.OutputManifestJSON) {
			t.Fatalf("same/cross-node certified output differs:\n%s\n%s", reference, sealed.OutputManifestJSON)
		}
	}
}

func newDiTAdapter(t *testing.T, driver *deterministicDriver) *h3stage.Adapter {
	t.Helper()
	adapter, err := h3stage.NewAdapter(h3stage.AdapterConfig{
		ProfileStableID:        h3stage.DiTSingleGPUProfile,
		StageProfileRevisionID: ditProfileRevisionID,
		Driver:                 driver,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	return adapter
}

func newWarmDiTDriver(node string) *deterministicDriver {
	return &deterministicDriver{residency: h3stage.Residency{
		Stage: h3stage.StageDiT, StageProfileRevisionID: ditProfileRevisionID, Warm: true,
		ModelRevision: "minimax-h3@sha256:model", BackendRevision: "sglang@sha256:backend",
		PlacementEvidence: node,
	}}
}

func ditAuthority(deviceCount int) *velav1.StageAuthority {
	devices := make([]*velav1.StageAuthorityDeviceEpoch, deviceCount)
	for index := range devices {
		devices[index] = &velav1.StageAuthorityDeviceEpoch{
			DeviceId: "device-" + string(rune('a'+index)), DeviceEpoch: 1,
		}
	}
	return &velav1.StageAuthority{
		StageProfileRevisionId: ditProfileRevisionID,
		Devices:                devices,
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "43000000-0000-0000-0000-000000000001",
			MemberEpoch:    1, ModelRuntimeEpoch: 1,
		}},
		StageLeaseId:   "49500000-0000-0000-0000-000000000001",
		StageRunId:     "49300000-0000-0000-0000-000000000001",
		StageAttemptId: "49400000-0000-0000-0000-000000000001",
		AttemptFence:   1, StageFence: 1, StageVersion: 1,
	}
}

type deterministicDriver struct {
	residency    h3stage.Residency
	prepareCalls int
	prepared     []byte
	started      bool
}

func (driver *deterministicDriver) Residency() h3stage.Residency { return driver.residency }

func (driver *deterministicDriver) Probe(
	context.Context,
	velav1.ModelRuntimeReadinessCheck,
) (modelruntime.ProbeResult, error) {
	return modelruntime.ProbeResult{Ready: driver.residency.Warm}, nil
}

func (driver *deterministicDriver) Prepare(
	_ context.Context,
	_ h3stage.ExecutionIdentity,
	spec *velav1.StageExecutionSpec,
) error {
	driver.prepareCalls++
	if spec == nil {
		return errors.New("missing execution spec")
	}
	digest := sha256.Sum256(append(append([]byte(nil), spec.GetParametersJson()...), spec.GetInputs()[0].GetSha256()...))
	driver.prepared = digest[:]
	return nil
}

func (driver *deterministicDriver) Start(context.Context, h3stage.ExecutionIdentity) error {
	if len(driver.prepared) == 0 {
		return errors.New("not prepared")
	}
	driver.started = true
	return nil
}

func (driver *deterministicDriver) Cancel(
	context.Context,
	h3stage.ExecutionIdentity,
	velav1.ModelRuntimeCancelReason,
) error {
	driver.started = false
	return nil
}

func (driver *deterministicDriver) Status(
	context.Context,
	h3stage.ExecutionIdentity,
) (modelruntime.BackendStatus, error) {
	return modelruntime.BackendStatus{
		State:        velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
		BackendStage: "dit",
	}, nil
}

func (driver *deterministicDriver) Seal(
	context.Context,
	h3stage.ExecutionIdentity,
) (modelruntime.SealedOutput, error) {
	if !driver.started {
		return modelruntime.SealedOutput{}, errors.New("not started")
	}
	manifest := append([]byte(`{"kind":"h3-latent","sha256":"`), []byte(fmtHex(driver.prepared))...)
	manifest = append(manifest, []byte(`"}`)...)
	return modelruntime.SealedOutput{OutputManifestJSON: manifest, TotalSizeBytes: 4096}, nil
}

func fmtHex(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, item := range value {
		encoded[index*2] = digits[item>>4]
		encoded[index*2+1] = digits[item&0x0f]
	}
	return string(encoded)
}
