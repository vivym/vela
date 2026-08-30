package cpumedia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type ExecutionIdentity struct {
	StageLeaseID   string
	StageRunID     string
	StageAttemptID string
	AttemptFence   int64
	StageFence     int64
	StageVersion   int64
}

type Driver interface {
	Resources() ResourceVector
	Probe(context.Context, velav1.ModelRuntimeReadinessCheck) (modelruntime.ProbeResult, error)
	Prepare(context.Context, ExecutionIdentity, *velav1.StageExecutionSpec) error
	Start(context.Context, ExecutionIdentity) error
	Cancel(context.Context, ExecutionIdentity, velav1.ModelRuntimeCancelReason) error
	Status(context.Context, ExecutionIdentity) (modelruntime.BackendStatus, error)
	Seal(context.Context, ExecutionIdentity) (modelruntime.SealedOutput, error)
}

type AdapterConfig struct {
	ProfileStableID        string
	StageProfileRevisionID string
	Driver                 Driver
}

type Adapter struct {
	profile                Profile
	stageProfileRevisionID string
	driver                 Driver
}

func NewAdapter(config AdapterConfig) (*Adapter, error) {
	if config.Driver == nil {
		return nil, errors.New("CPU media Driver is required")
	}
	if _, err := uuid.Parse(config.StageProfileRevisionID); err != nil {
		return nil, errors.New("CPU media StageProfile revision identity is invalid")
	}
	profile, err := productionProfile(config.ProfileStableID)
	if err != nil {
		return nil, err
	}
	adapter := &Adapter{
		profile: profile, stageProfileRevisionID: config.StageProfileRevisionID,
		driver: config.Driver,
	}
	if err := adapter.validateCapacity(); err != nil {
		return nil, err
	}
	return adapter, nil
}

func (adapter *Adapter) Probe(
	ctx context.Context,
	check velav1.ModelRuntimeReadinessCheck,
) (modelruntime.ProbeResult, error) {
	if err := adapter.validateCapacity(); err != nil {
		return modelruntime.ProbeResult{}, err
	}
	return adapter.driver.Probe(ctx, check)
}

func (adapter *Adapter) Prepare(
	ctx context.Context,
	authority stageauthority.Verified,
	spec *velav1.StageExecutionSpec,
) error {
	identity, err := adapter.validateExecution(authority)
	if err != nil {
		return err
	}
	if spec == nil {
		return errors.New("CPU media stage execution spec is required")
	}
	if len(spec.GetParametersJson()) > 0 && !json.Valid(spec.GetParametersJson()) {
		return errors.New("CPU media stage parameters are not valid JSON")
	}
	if len(spec.GetExpectedOutputManifestJson()) > 0 &&
		!json.Valid(spec.GetExpectedOutputManifestJson()) {
		return errors.New("CPU media expected output manifest is not valid JSON")
	}
	return adapter.driver.Prepare(ctx, identity, spec)
}

func (adapter *Adapter) Start(
	ctx context.Context,
	authority stageauthority.Verified,
) error {
	identity, err := adapter.validateExecution(authority)
	if err != nil {
		return err
	}
	return adapter.driver.Start(ctx, identity)
}

func (adapter *Adapter) Cancel(
	ctx context.Context,
	authority stageauthority.Verified,
	reason velav1.ModelRuntimeCancelReason,
) error {
	identity, err := adapter.validateExecution(authority)
	if err != nil {
		return err
	}
	return adapter.driver.Cancel(ctx, identity, reason)
}

func (adapter *Adapter) Status(
	ctx context.Context,
	authority stageauthority.Verified,
) (modelruntime.BackendStatus, error) {
	identity, err := adapter.validateExecution(authority)
	if err != nil {
		return modelruntime.BackendStatus{}, err
	}
	status, err := adapter.driver.Status(ctx, identity)
	if err != nil {
		return modelruntime.BackendStatus{}, err
	}
	if strings.TrimSpace(status.BackendStage) == "" {
		status.BackendStage = string(adapter.profile.Stage)
	}
	return status, nil
}

func (adapter *Adapter) Seal(
	ctx context.Context,
	authority stageauthority.Verified,
) (modelruntime.SealedOutput, error) {
	identity, err := adapter.validateExecution(authority)
	if err != nil {
		return modelruntime.SealedOutput{}, err
	}
	sealed, err := adapter.driver.Seal(ctx, identity)
	if err != nil {
		return modelruntime.SealedOutput{}, err
	}
	if sealed.TotalSizeBytes <= 0 || len(sealed.OutputManifestJSON) == 0 ||
		!json.Valid(sealed.OutputManifestJSON) {
		return modelruntime.SealedOutput{}, errors.New("CPU media stage returned an invalid sealed output")
	}
	return modelruntime.SealedOutput{
		OutputManifestJSON: append([]byte(nil), sealed.OutputManifestJSON...),
		TotalSizeBytes:     sealed.TotalSizeBytes,
	}, nil
}

func (adapter *Adapter) validateExecution(
	authority stageauthority.Verified,
) (ExecutionIdentity, error) {
	if err := adapter.validateCapacity(); err != nil {
		return ExecutionIdentity{}, err
	}
	envelope := authority.Authority
	if envelope == nil || envelope.GetStageProfileRevisionId() != adapter.stageProfileRevisionID {
		return ExecutionIdentity{}, errors.New("CPU media StageAuthority does not match the configured StageProfile")
	}
	if len(envelope.GetDevices()) != adapter.profile.DeviceCount ||
		len(envelope.GetMembers()) != adapter.profile.MemberCount {
		return ExecutionIdentity{}, fmt.Errorf(
			"CPU media %s Assignment requires exactly %d device and %d WorkerMember",
			adapter.profile.Stage, adapter.profile.DeviceCount, adapter.profile.MemberCount,
		)
	}
	identity := ExecutionIdentity{
		StageLeaseID: envelope.GetStageLeaseId(), StageRunID: envelope.GetStageRunId(),
		StageAttemptID: envelope.GetStageAttemptId(), AttemptFence: envelope.GetAttemptFence(),
		StageFence: envelope.GetStageFence(), StageVersion: envelope.GetStageVersion(),
	}
	if strings.TrimSpace(identity.StageLeaseID) == "" || strings.TrimSpace(identity.StageRunID) == "" ||
		strings.TrimSpace(identity.StageAttemptID) == "" || identity.AttemptFence <= 0 ||
		identity.StageFence <= 0 || identity.StageVersion <= 0 {
		return ExecutionIdentity{}, errors.New("CPU media StageAuthority execution identity is incomplete")
	}
	return identity, nil
}

func (adapter *Adapter) validateCapacity() error {
	if adapter == nil || adapter.driver == nil || adapter.stageProfileRevisionID == "" {
		return errors.New("CPU media Adapter is not configured")
	}
	if adapter.driver.Resources() != adapter.profile.CapacityLimits {
		return errors.New("CPU media Driver capacity does not match the certified WorkerProfile")
	}
	return nil
}

var _ modelruntime.Backend = (*Adapter)(nil)
