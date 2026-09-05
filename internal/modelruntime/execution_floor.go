package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// ExecutionFloorMember is trusted topology supplied at construction, never
// learned from the disposition being verified. Runtime epochs come from Services.
type ExecutionFloorMember struct {
	WorkerMemberID     string
	MemberEpoch        int64
	IdentityDigest     []byte
	DeviceSubsetDigest []byte
}

type ExecutionFloorConfig struct {
	Validator *stageauthority.Validator
	Members   []ExecutionFloorMember
	State     *ExecutionFloorStateConfig
}

// ExecutionFloorStateConfig defaults to recovery of existing state. Initialize
// is an explicit, one-time bootstrap into an already trusted empty directory.
// Empty storage alone does not prove a new Worker; ordinary restarts must never
// infer Initialize from missing files.
type ExecutionFloorStateConfig struct {
	Directory  string
	Initialize bool
}

type executionFloorVerifier struct {
	validator *stageauthority.Validator
	members   map[string]ExecutionFloorMember
}

// ExecutionFloorInstallation reports an admission checkpoint. Durable means its
// floor is persisted; neither form proves backend drain or authorizes deletion.
type ExecutionFloorInstallation struct {
	Cutoff            int64
	Durable           bool
	DispositionDigest [sha256.Size]byte
	pending           []<-chan struct{}
}

// WaitAcceptedOperations waits only for calls admitted before this checkpoint.
// Backend tasks, child writers, later cancellation calls, and Worker input
// resolvers need separate drain evidence, even when this returns successfully.
func (installation *ExecutionFloorInstallation) WaitAcceptedOperations(ctx context.Context) error {
	if installation == nil {
		return errors.New("ModelRuntime execution floor installation is missing")
	}
	for _, done := range installation.pending {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

// NewSupervisorWithExecutionFloor enables signed cutoff installation. A State
// configuration additionally persists restrictions across process restarts.
func NewSupervisorWithExecutionFloor(config ExecutionFloorConfig, services ...*Service) (*Supervisor, error) {
	return newSupervisor(&config, services...)
}

func newExecutionFloorVerifier(config ExecutionFloorConfig, supervisor *Supervisor) (*executionFloorVerifier, error) {
	baseline := supervisor.services[0].binding
	if config.Validator == nil || len(config.Members) != len(baseline.Members) {
		return nil, errors.New("ModelRuntime execution floor requires a verifier and complete trusted membership")
	}
	verifier := &executionFloorVerifier{validator: config.Validator, members: make(map[string]ExecutionFloorMember)}
	for _, member := range config.Members {
		if _, duplicate := verifier.members[member.WorkerMemberID]; duplicate ||
			len(member.IdentityDigest) != sha256.Size || len(member.DeviceSubsetDigest) != sha256.Size {
			return nil, errors.New("ModelRuntime execution floor trusted member is invalid")
		}
		member.IdentityDigest = bytes.Clone(member.IdentityDigest)
		member.DeviceSubsetDigest = bytes.Clone(member.DeviceSubsetDigest)
		verifier.members[member.WorkerMemberID] = member
	}
	localFound := false
	for _, member := range baseline.Members {
		configured, found := verifier.members[member.ID]
		if !found || configured.MemberEpoch != member.Epoch {
			return nil, errors.New("ModelRuntime execution floor trusted membership does not match resident topology")
		}
		localFound = localFound || member.ID == baseline.WorkerMemberID && member.Epoch == baseline.WorkerMemberEpoch
	}
	if !localFound {
		return nil, errors.New("ModelRuntime execution floor topology omits the local member")
	}
	return verifier, nil
}

func (supervisor *Supervisor) InstallExecutionFloor(ctx context.Context, value *velav1.StageTerminalDisposition) (*ExecutionFloorInstallation, error) {
	if supervisor == nil || supervisor.floor == nil || supervisor.admission == nil {
		return nil, errors.New("ModelRuntime execution floor verifier is not configured")
	}
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := admission.checkStateLocked(); err != nil {
		return nil, err
	}
	// Validate freshness at the same admission boundary that installs the cutoff.
	verified, err := supervisor.floor.validator.ValidateTerminalDispositionEnvelope(value)
	if err != nil {
		return nil, err
	}
	if err := supervisor.matchExecutionFloorScope(verified.Disposition, true); err != nil {
		return nil, err
	}
	if verified.Disposition.GetCutoff() > admission.floor && admission.store != nil {
		if err := admission.store.saveFloor(verified.Disposition); err != nil {
			return nil, admission.failStateLocked(err)
		}
	}
	admission.floor = max(admission.floor, verified.Disposition.GetCutoff())
	installation := &ExecutionFloorInstallation{Cutoff: admission.floor, Durable: admission.store != nil, DispositionDigest: verified.Digest}
	for _, operation := range admission.active {
		if operation.sequence <= admission.floor {
			installation.pending = append(installation.pending, operation.done)
		}
	}
	return installation, nil
}

func (supervisor *Supervisor) matchExecutionFloorScope(value *velav1.StageTerminalDisposition, currentRuntimes bool) error {
	baseline := supervisor.services[0].binding
	if value.GetWorkerInstanceId() != baseline.WorkerInstanceID || value.GetWorkerInstanceEpoch() != baseline.WorkerInstanceEpoch ||
		!bytes.Equal(value.GetDeviceSetDigest(), baseline.DeviceSetDigest) || !bytes.Equal(value.GetMembershipDigest(), baseline.MembershipDigest) ||
		len(value.GetDevices()) != len(baseline.Devices) {
		return stageauthority.ErrRuntimeMismatch
	}
	for _, device := range baseline.Devices {
		found := false
		for _, signed := range value.GetDevices() {
			found = found || signed.GetDeviceId() == device.ID && signed.GetDeviceEpoch() == device.Epoch
		}
		if !found {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	for _, allocation := range value.GetAllocations() {
		if len(allocation.GetMembers()) != len(supervisor.floor.members) {
			return stageauthority.ErrRuntimeMismatch
		}
		localFound := false
		for _, member := range allocation.GetMembers() {
			trusted, found := supervisor.floor.members[member.GetWorkerMemberId()]
			if !found || member.GetMemberEpoch() != trusted.MemberEpoch ||
				!bytes.Equal(member.GetIdentityDigest(), trusted.IdentityDigest) || !bytes.Equal(member.GetDeviceSubsetDigest(), trusted.DeviceSubsetDigest) {
				return stageauthority.ErrRuntimeMismatch
			}
			if member.GetWorkerMemberId() == baseline.WorkerMemberID {
				localFound = true
				// Barrier generation is not a member-local ModelRuntime epoch.
				route := runtimeRoute{
					modelResidencyID: allocation.GetModelResidencyId(), runtimeIdentity: allocation.GetModelRuntimeIdentity(),
					modelRuntimeEpoch: member.GetModelRuntimeEpoch(), stageProfileRevisionID: allocation.GetStageProfileRevisionId(),
				}
				if currentRuntimes && supervisor.routes[route] == nil {
					return stageauthority.ErrRuntimeMismatch
				}
			}
		}
		if !localFound {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	return nil
}
