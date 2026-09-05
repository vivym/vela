package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// ExecutionDrainResult covers one queried execution across its complete membership.
// Member checkpoints retain the exact envelopes actually drained.
// It does not establish a signed floor, input exclusion or permission to delete.
type ExecutionDrainResult struct {
	AuthorityDigest   [sha256.Size]byte
	ExecutionSequence int64
	RequiredMembers   int
	Members           map[string]*velav1.ModelRuntimeExecutionDrainResult
	AllDrained        bool
}

// DrainExecution uses the same independently trusted member bindings as floor
// installation. It never substitutes cancellation or Status for a checkpoint.
func (agent *Agent) DrainExecution(ctx context.Context, authority *velav1.StageAuthority) (ExecutionDrainResult, error) {
	return agent.collectExecutionDrain(ctx, authority, nil, false, false)
}

// InspectExecutionDrain reads existing checkpoints from explicitly named current
// journal owners, including after epoch/profile changes. Every owner must match
// independent configuration; targets cannot add or omit execution members.
func (agent *Agent) InspectExecutionDrain(ctx context.Context, authority *velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity) (ExecutionDrainResult, error) {
	return agent.collectExecutionDrain(ctx, authority, targets, true, false)
}

func (agent *Agent) collectExecutionDrain(ctx context.Context, authority *velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity, historical, allocation bool) (ExecutionDrainResult, error) {
	result := ExecutionDrainResult{}
	if agent == nil || agent.floor == nil || ctx == nil {
		return result, errors.New("execution drain collection is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, agent.floor.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	verified, err := agent.floor.validator.ValidateEnvelopeForReplay(authority, 0)
	if err != nil {
		return result, err
	}
	scopes, err := agent.executionDrainScopes(verified.Authority, targets, historical)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.AuthorityDigest, result.ExecutionSequence = verified.Digest, verified.Authority.GetExecutionSequence()
	result.RequiredMembers, result.Members = len(agent.ids), make(map[string]*velav1.ModelRuntimeExecutionDrainResult)
	type memberResult struct {
		id       string
		response *velav1.ModelRuntimeExecutionDrainResult
		err      error
	}
	completed := make(chan memberResult, len(agent.ids))
	for _, id := range agent.ids {
		client, scope := agent.members[id], scopes[id]
		go func() {
			if err := ctx.Err(); err != nil {
				completed <- memberResult{id: id, err: err}
				return
			}
			sent := proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
			var response *velav1.ModelRuntimeExecutionDrainResult
			var err error
			if allocation {
				var wrapper *velav1.ModelRuntimeServiceInspectStageAllocationDrainResponse
				wrapper, err = client.InspectStageAllocationDrain(ctx, &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: sent})
				if err == nil {
					if wrapper == nil || len(wrapper.ProtoReflect().GetUnknown()) != 0 {
						err = errors.New("invalid allocation drain inspection response")
					} else {
						response = wrapper.GetResult()
					}
				}
			} else if historical {
				var wrapper *velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse
				wrapper, err = client.InspectStageExecutionDrain(ctx, &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: sent})
				if err == nil {
					if wrapper == nil || len(wrapper.ProtoReflect().GetUnknown()) != 0 {
						err = errors.New("invalid drain inspection response")
					} else {
						response = wrapper.GetResult()
					}
				}
			} else {
				var wrapper *velav1.ModelRuntimeServiceDrainStageExecutionResponse
				wrapper, err = client.DrainStageExecution(ctx, &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: sent})
				if err == nil {
					if wrapper == nil || len(wrapper.ProtoReflect().GetUnknown()) != 0 {
						err = errors.New("invalid drain response")
					} else {
						response = wrapper.GetResult()
					}
				}
			}
			if err == nil {
				err = ctx.Err()
			}
			if err == nil {
				if allocation {
					err = modelruntimetransport.ValidateAllocationDrainResult(agent.floor.validator, scope, verified.Digest, response)
				} else {
					err = modelruntimetransport.ValidateExecutionDrainResult(scope, verified.Digest, response)
				}
			}
			if err == nil && (response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || response.GetCheckpoint() == nil) {
				err = errors.New("member execution writer drain is unproven")
			}
			if err == nil {
				response = proto.Clone(response).(*velav1.ModelRuntimeExecutionDrainResult)
			}
			completed <- memberResult{id: id, response: response, err: err}
		}()
	}
	failures := make(map[string]error)
	for range agent.ids {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case member := <-completed:
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			if member.err != nil {
				failures[member.id] = member.err
			} else {
				result.Members[member.id] = member.response
			}
		}
	}
	var joined error
	for _, id := range agent.ids {
		if err := failures[id]; err != nil {
			joined = errors.Join(joined, fmt.Errorf("drain member %s: %w", id, err))
		}
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(joined, err)
	}
	result.AllDrained = joined == nil && len(result.Members) == result.RequiredMembers
	return result, joined
}

func (agent *Agent) executionDrainScopes(authority *velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity, historical bool) (map[string]*velav1.ModelRuntimeExecutionDrainScope, error) {
	if len(authority.GetMembers()) != len(agent.ids) || (historical && len(targets) != len(agent.ids)) {
		return nil, errors.New("execution drain membership is incomplete")
	}
	scopes := make(map[string]*velav1.ModelRuntimeExecutionDrainScope)
	for _, member := range authority.GetMembers() {
		id := member.GetWorkerMemberId()
		scope, err := agent.executionDrainMemberScope(authority, id, targets[id], historical)
		if err != nil {
			return nil, err
		}
		scopes[id] = scope
	}
	return scopes, nil
}

func (agent *Agent) executionDrainMemberScope(authority *velav1.StageAuthority, id string, target *velav1.ModelRuntimeIdentity, historical bool) (*velav1.ModelRuntimeExecutionDrainScope, error) {
	if agent.members[id] == nil {
		return nil, errors.New("execution drain member is not configured")
	}
	var member *velav1.StageAuthorityMemberEpoch
	for _, candidate := range authority.GetMembers() {
		if candidate.GetWorkerMemberId() == id {
			if member != nil {
				return nil, errors.New("execution drain member is ambiguous")
			}
			member = candidate
		}
	}
	if member == nil {
		return nil, errors.New("execution drain member is absent from authority")
	}
	var selected *velav1.ModelRuntimeExecutionDrainScope
	matches := 0
	for _, binding := range agent.floor.bindings {
		if binding.Runtime.WorkerMemberID != id || !bytes.Equal(binding.IdentityDigest[:], member.GetIdentityDigest()) {
			continue
		}
		identity := executionDrainIdentity(binding.Runtime)
		if historical && !proto.Equal(target, identity) {
			continue
		}
		original := binding.Runtime
		if historical {
			original.ModelResidencyID, original.ModelRuntimeIdentity, original.StageProfileRevisionID = authority.GetModelResidencyId(), authority.GetModelRuntimeIdentity(), authority.GetStageProfileRevisionId()
			original.ModelRuntimeEpoch = member.GetModelRuntimeEpoch()
		}
		if _, err := agent.floor.validator.ValidateSignature(authority, original); err != nil {
			continue
		}
		scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: identity, Authority: proto.Clone(authority).(*velav1.StageAuthority)}
		if _, err := modelruntimetransport.ValidateExecutionDrainScope(agent.floor.validator, scope, 0, historical); err != nil {
			return nil, err
		}
		selected, matches = scope, matches+1
	}
	if matches != 1 {
		return nil, fmt.Errorf("execution drain member %s binding is missing or ambiguous", id)
	}
	return selected, nil
}

func executionDrainIdentity(binding stageauthority.RuntimeBinding) *velav1.ModelRuntimeIdentity {
	return &velav1.ModelRuntimeIdentity{WorkerInstanceId: binding.WorkerInstanceID, WorkerInstanceEpoch: binding.WorkerInstanceEpoch,
		WorkerMemberId: binding.WorkerMemberID, WorkerMemberEpoch: binding.WorkerMemberEpoch,
		DeviceSetDigest: bytes.Clone(binding.DeviceSetDigest), MembershipDigest: bytes.Clone(binding.MembershipDigest),
		ModelResidencyId: binding.ModelResidencyID, RuntimeIdentity: binding.ModelRuntimeIdentity,
		ModelRuntimeEpoch: binding.ModelRuntimeEpoch, StageProfileRevisionId: binding.StageProfileRevisionID}
}
