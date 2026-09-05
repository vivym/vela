package stageworkeragent

import (
	"context"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func (agent *Agent) terminalAllocationMemberScope(disposition *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation, id string, target *velav1.ModelRuntimeIdentity, historical bool) (*velav1.ModelRuntimeTerminalAllocationScope, error) {
	if agent.members[id] == nil || len(allocation.GetMembers()) != len(agent.ids) {
		return nil, errors.New("terminal non-admission membership is incomplete")
	}
	var member *velav1.StageTerminalMember
	for _, candidate := range allocation.GetMembers() {
		if candidate.GetWorkerMemberId() == id {
			member = candidate
		}
	}
	if member == nil {
		return nil, errors.New("terminal non-admission member is missing")
	}
	var selected *velav1.ModelRuntimeTerminalAllocationScope
	matches := 0
	for _, binding := range agent.floor.bindings {
		if !binding.matchesTopology(disposition, allocation, member) {
			continue
		}
		identity := executionDrainIdentity(binding.Runtime)
		if historical && !proto.Equal(target, identity) || !historical && !binding.matches(disposition, allocation, member) {
			continue
		}
		scope := &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: identity,
			Disposition: proto.Clone(disposition).(*velav1.StageTerminalDisposition), StageAllocationId: allocation.GetStageAllocationId()}
		if _, err := modelruntimetransport.ValidateTerminalAllocationScope(agent.floor.validator, scope, historical); err != nil {
			return nil, err
		}
		selected, matches = scope, matches+1
	}
	if matches != 1 {
		return nil, fmt.Errorf("terminal non-admission member %s binding is missing or ambiguous", id)
	}
	return selected, nil
}

func (agent *Agent) collectMemberTerminalNonAdmission(ctx context.Context, id string, disposition *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation, target *velav1.ModelRuntimeIdentity, checkpoint bool) (ExecutionExclusionProof, error) {
	proof := ExecutionExclusionProof{}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	scope, err := agent.terminalAllocationMemberScope(disposition, allocation, id, target, true)
	if err != nil {
		return proof, err
	}
	verified, err := modelruntimetransport.ValidateTerminalAllocationScope(agent.floor.validator, scope, true)
	if err != nil {
		return proof, err
	}
	client := agent.members[id]
	response, err := client.InspectStageTerminalNonAdmission(ctx, &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)})
	if err != nil {
		return proof, err
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
		return proof, errors.New("terminal non-admission inspection wrapper is invalid")
	}
	result := response.GetResult()
	if err := modelruntimetransport.ValidateTerminalNonAdmissionResult(agent.floor.validator, scope, verified.Digest, result); err != nil {
		return proof, err
	}
	if result.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		return proof, errors.New("terminal non-admission inspection rejected")
	}
	if result.GetCheckpoint() == nil && checkpoint {
		scope, err = agent.terminalAllocationMemberScope(disposition, allocation, id, nil, false)
		if err != nil {
			return proof, err
		}
		response, err := client.CheckpointStageTerminalNonAdmission(ctx, &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)})
		if err != nil {
			return proof, err
		}
		if err := ctx.Err(); err != nil {
			return proof, err
		}
		if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
			return proof, errors.New("terminal non-admission checkpoint wrapper is invalid")
		}
		result = response.GetResult()
		if err := modelruntimetransport.ValidateTerminalNonAdmissionResult(agent.floor.validator, scope, verified.Digest, result); err != nil {
			return proof, err
		}
	}
	if result.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || result.GetCheckpoint() == nil {
		return proof, errors.New("member terminal execution exclusion is unproven")
	}
	proof.TerminalNeverAdmitted = proto.Clone(result).(*velav1.ModelRuntimeTerminalNonAdmissionResult)
	return proof, nil
}
