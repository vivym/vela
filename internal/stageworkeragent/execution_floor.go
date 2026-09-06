package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// ExecutionFloorBinding comes from trusted configuration/discovery, never the
// terminal response being checked. Every historical resident route is required.
type ExecutionFloorBinding struct {
	Runtime            stageauthority.RuntimeBinding
	IdentityDigest     [sha256.Size]byte
	DeviceSubsetDigest [sha256.Size]byte
}

type ExecutionFloorConfig struct {
	Validator *stageauthority.Validator
	Bindings  []ExecutionFloorBinding
	Timeout   time.Duration
	// CurrentReaders explicitly selects current journal owners for automatic
	// recovery. Every reader must also match exactly one trusted Binding.
	CurrentReaders map[string]*velav1.ModelRuntimeIdentity
}

// ExecutionFloorResult describes this call's member acknowledgements only. It
// proves neither writer drain nor Worker input exclusion and cannot permit GC.
type ExecutionFloorResult struct {
	DispositionDigest [sha256.Size]byte
	Cutoff            int64
	RequiredMembers   int
	Acknowledgements  map[string]*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse
	AllInstalled      bool
}

type executionFloorCollector struct {
	validator *stageauthority.Validator
	bindings  []ExecutionFloorBinding
	timeout   time.Duration
	readers   map[string]*velav1.ModelRuntimeIdentity
}

func newExecutionFloorCollector(config *ExecutionFloorConfig, memberIDs []string) (*executionFloorCollector, error) {
	if config == nil {
		return nil, nil
	}
	if config.Validator == nil || len(config.Bindings) == 0 || len(config.Bindings) > 16*64 {
		return nil, errors.New("execution floor collection requires trusted Runtime bindings")
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	if timeout < time.Millisecond || timeout > time.Minute {
		return nil, errors.New("execution floor collection timeout is invalid")
	}
	collector := &executionFloorCollector{validator: config.Validator, timeout: timeout}
	seen := make(map[string]bool)
	for _, binding := range config.Bindings {
		if !slices.Contains(memberIDs, binding.Runtime.WorkerMemberID) ||
			binding.IdentityDigest == ([sha256.Size]byte{}) || binding.DeviceSubsetDigest == ([sha256.Size]byte{}) {
			return nil, errors.New("execution floor collection member binding is invalid")
		}
		binding.Runtime.Devices = slices.Clone(binding.Runtime.Devices)
		binding.Runtime.Members = slices.Clone(binding.Runtime.Members)
		binding.Runtime.DeviceSetDigest = bytes.Clone(binding.Runtime.DeviceSetDigest)
		binding.Runtime.MembershipDigest = bytes.Clone(binding.Runtime.MembershipDigest)
		collector.bindings = append(collector.bindings, binding)
		seen[binding.Runtime.WorkerMemberID] = true
	}
	if len(seen) != len(memberIDs) {
		return nil, errors.New("execution floor collection membership is incomplete")
	}
	if config.CurrentReaders != nil {
		if len(config.CurrentReaders) != len(memberIDs) {
			return nil, errors.New("execution floor recovery readers are incomplete")
		}
		collector.readers = make(map[string]*velav1.ModelRuntimeIdentity, len(memberIDs))
		for id, identity := range config.CurrentReaders {
			matches := 0
			for _, binding := range collector.bindings {
				if id == binding.Runtime.WorkerMemberID && identity != nil && proto.Equal(identity, executionDrainIdentity(binding.Runtime)) {
					matches++
				}
			}
			if matches != 1 {
				return nil, errors.New("execution floor recovery reader is not a unique trusted binding")
			}
			collector.readers[id] = proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
		}
	}
	return collector, nil
}

// InstallExecutionFloor sends the complete signed history to every configured
// member. A partial installation is restrictive and may be retried in full.
func (agent *Agent) InstallExecutionFloor(ctx context.Context, disposition *velav1.StageTerminalDisposition) (ExecutionFloorResult, error) {
	return agent.installExecutionFloor(ctx, disposition, nil, false)
}

// InstallRecoveryExecutionFloor sends the restriction to independently trusted
// current journal owners. It neither recreates nor certifies historical writers.
func (agent *Agent) InstallRecoveryExecutionFloor(ctx context.Context, disposition *velav1.StageTerminalDisposition, readers map[string]*velav1.ModelRuntimeIdentity) (ExecutionFloorResult, error) {
	return agent.installExecutionFloor(ctx, disposition, readers, true)
}

func (agent *Agent) installExecutionFloor(ctx context.Context, disposition *velav1.StageTerminalDisposition, readers map[string]*velav1.ModelRuntimeIdentity, recovery bool) (ExecutionFloorResult, error) {
	result := ExecutionFloorResult{}
	if agent == nil || agent.floor == nil || ctx == nil {
		return result, errors.New("execution floor collection is not configured")
	}
	callContext, cancel := context.WithTimeout(ctx, agent.floor.timeout)
	defer cancel()
	if err := callContext.Err(); err != nil {
		return result, err
	}
	verified, err := agent.floor.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return result, err
	}
	var targets map[string]*velav1.ModelRuntimeIdentity
	version := uint32(1)
	if recovery {
		version = 2
		targets, err = agent.recoveryExecutionFloorTargets(verified.Disposition, readers)
	} else {
		targets, err = agent.executionFloorTargets(verified.Disposition)
	}
	if err != nil {
		return result, err
	}
	if err := callContext.Err(); err != nil {
		return result, err
	}
	result.DispositionDigest, result.Cutoff = verified.Digest, verified.Disposition.GetCutoff()
	result.RequiredMembers = len(agent.ids)
	result.Acknowledgements = make(map[string]*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse)
	type memberResult struct {
		id       string
		response *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse
		err      error
	}
	results := make(chan memberResult, len(agent.ids))
	for _, id := range agent.ids {
		client, identity := agent.members[id], targets[id]
		// Each hop receives its own copy; it cannot alter another member's request
		// or the independently bound response identity and disposition digest.
		request := &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
			SchemaVersion: version, Identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
			Disposition: proto.Clone(verified.Disposition).(*velav1.StageTerminalDisposition),
		}
		go func() {
			if err := callContext.Err(); err != nil {
				results <- memberResult{id: id, err: err}
				return
			}
			response, callErr := client.InstallStageExecutionFloor(callContext, request, grpc.MaxCallSendMsgSize(4<<20))
			if callErr == nil {
				callErr = callContext.Err()
			}
			if callErr == nil {
				callErr = modelruntimetransport.ValidateExecutionFloorAcknowledgementForVersion(version, identity, verified.Digest, verified.Disposition.GetCutoff(), response)
			}
			if callErr == nil {
				response = proto.Clone(response).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse)
			}
			results <- memberResult{id: id, response: response, err: callErr}
		}()
	}
	failures := make(map[string]error)
	for range agent.ids {
		select {
		case <-callContext.Done():
			return result, callContext.Err()
		case member := <-results:
			if err := callContext.Err(); err != nil {
				return result, err
			}
			if member.err != nil {
				failures[member.id] = member.err
			} else {
				result.Acknowledgements[member.id] = member.response
			}
		}
	}
	var joined error
	for _, id := range agent.ids {
		if err := failures[id]; err != nil {
			joined = errors.Join(joined, fmt.Errorf("install execution floor on member %s: %w", id, err))
		}
	}
	if err := callContext.Err(); err != nil {
		return result, errors.Join(joined, err)
	}
	result.AllInstalled = joined == nil && len(result.Acknowledgements) == result.RequiredMembers
	return result, joined
}

func (agent *Agent) recoveryExecutionFloorTargets(value *velav1.StageTerminalDisposition, readers map[string]*velav1.ModelRuntimeIdentity) (map[string]*velav1.ModelRuntimeIdentity, error) {
	if len(readers) != len(agent.ids) {
		return nil, errors.New("execution floor recovery readers are incomplete")
	}
	targets := make(map[string]*velav1.ModelRuntimeIdentity, len(readers))
	for _, allocation := range value.GetAllocations() {
		if len(allocation.GetMembers()) != len(agent.ids) {
			return nil, errors.New("execution floor signed membership is incomplete")
		}
		for _, member := range allocation.GetMembers() {
			id := member.GetWorkerMemberId()
			identity := readers[id]
			if agent.members[id] == nil || identity == nil {
				return nil, errors.New("execution floor recovery member is not configured")
			}
			matches := 0
			for _, binding := range agent.floor.bindings {
				if binding.matchesTopology(value, allocation, member) && proto.Equal(executionDrainIdentity(binding.Runtime), identity) {
					matches++
				}
			}
			if matches != 1 {
				return nil, errors.New("execution floor recovery reader does not match trusted member topology")
			}
			targets[id] = proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
		}
	}
	return targets, nil
}

func (agent *Agent) executionFloorTargets(value *velav1.StageTerminalDisposition) (map[string]*velav1.ModelRuntimeIdentity, error) {
	targets := make(map[string]*velav1.ModelRuntimeIdentity)
	for _, allocation := range value.GetAllocations() {
		if len(allocation.GetMembers()) != len(agent.ids) {
			return nil, errors.New("execution floor signed membership is incomplete")
		}
		for _, member := range allocation.GetMembers() {
			if agent.members[member.GetWorkerMemberId()] == nil {
				return nil, errors.New("execution floor signed member is not configured")
			}
			matches := 0
			for _, binding := range agent.floor.bindings {
				if !binding.matches(value, allocation, member) {
					continue
				}
				matches++
				if targets[member.GetWorkerMemberId()] == nil {
					runtime := binding.Runtime
					targets[member.GetWorkerMemberId()] = &velav1.ModelRuntimeIdentity{
						WorkerInstanceId: runtime.WorkerInstanceID, WorkerInstanceEpoch: runtime.WorkerInstanceEpoch,
						WorkerMemberId: runtime.WorkerMemberID, WorkerMemberEpoch: runtime.WorkerMemberEpoch,
						DeviceSetDigest: bytes.Clone(runtime.DeviceSetDigest), MembershipDigest: bytes.Clone(runtime.MembershipDigest),
						ModelResidencyId: runtime.ModelResidencyID, RuntimeIdentity: runtime.ModelRuntimeIdentity,
						ModelRuntimeEpoch: runtime.ModelRuntimeEpoch, StageProfileRevisionId: runtime.StageProfileRevisionID,
					}
				}
			}
			if matches != 1 {
				return nil, fmt.Errorf("execution floor member %s historical Runtime binding is missing or ambiguous", member.GetWorkerMemberId())
			}
		}
	}
	return targets, nil
}

func (binding ExecutionFloorBinding) matches(value *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation, member *velav1.StageTerminalMember) bool {
	runtime := binding.Runtime
	return runtime.ModelResidencyID == allocation.GetModelResidencyId() && runtime.ModelRuntimeIdentity == allocation.GetModelRuntimeIdentity() &&
		runtime.ModelRuntimeEpoch == member.GetModelRuntimeEpoch() && runtime.StageProfileRevisionID == allocation.GetStageProfileRevisionId() &&
		binding.matchesTopology(value, allocation, member)
}

func (binding ExecutionFloorBinding) matchesTopology(value *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation, member *velav1.StageTerminalMember) bool {
	runtime := binding.Runtime
	if runtime.WorkerInstanceID != value.GetWorkerInstanceId() || runtime.WorkerInstanceEpoch != value.GetWorkerInstanceEpoch() ||
		runtime.WorkerMemberID != member.GetWorkerMemberId() || runtime.WorkerMemberEpoch != member.GetMemberEpoch() ||
		!bytes.Equal(runtime.DeviceSetDigest, value.GetDeviceSetDigest()) || !bytes.Equal(runtime.MembershipDigest, value.GetMembershipDigest()) ||
		!bytes.Equal(binding.IdentityDigest[:], member.GetIdentityDigest()) || !bytes.Equal(binding.DeviceSubsetDigest[:], member.GetDeviceSubsetDigest()) ||
		len(runtime.Devices) != len(value.GetDevices()) || len(runtime.Members) != len(allocation.GetMembers()) {
		return false
	}
	devices := make(map[stageauthority.DeviceEpoch]bool)
	for _, device := range runtime.Devices {
		devices[device] = true
	}
	for _, device := range value.GetDevices() {
		if !devices[stageauthority.DeviceEpoch{ID: device.GetDeviceId(), Epoch: device.GetDeviceEpoch()}] {
			return false
		}
	}
	members := make(map[stageauthority.MemberEpoch]bool)
	for _, configured := range runtime.Members {
		members[configured] = true
	}
	for _, signed := range allocation.GetMembers() {
		if !members[stageauthority.MemberEpoch{ID: signed.GetWorkerMemberId(), Epoch: signed.GetMemberEpoch()}] {
			return false
		}
	}
	return true
}
