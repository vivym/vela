package stageworkeragent

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// Only the terminal retirement coordinator selects live recovery, after input
// exclusion and all Runtime floors. Historical replacement owners may replay
// prior proof, but cannot send an old execution to a replacement backend.
func (agent *Agent) recoverMemberExecutionDrain(ctx context.Context, memberID string, query *velav1.ModelRuntimeExecutionDrainScope) (ExecutionExclusionProof, error) {
	proof := ExecutionExclusionProof{}
	scope, err := agent.executionDrainMemberScope(query.GetAuthority(), memberID, nil, false)
	if err != nil {
		return proof, err
	}
	if !proto.Equal(scope.GetIdentity(), query.GetIdentity()) {
		return proof, errors.New("live recovery requires the original resident Runtime")
	}
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(agent.floor.validator, scope, 0, false)
	if err != nil {
		return proof, err
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	client := agent.members[memberID]
	discovered, err := client.InspectAllocationExecution(ctx, &velav1.ModelRuntimeServiceInspectAllocationExecutionRequest{
		SchemaVersion: 1, Authority: proto.Clone(verified.Authority).(*velav1.StageAuthority),
	})
	if err != nil {
		return proof, err
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	if err := modelruntimetransport.ValidateAllocationExecutionResponse(agent.floor.validator, verified.Authority, verified.Digest, scope.GetIdentity(), discovered); err != nil {
		return proof, err
	}
	if discovered.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || discovered.GetObservedAuthority() == nil {
		return proof, errors.New("live backend execution identity is unproven")
	}
	exactScope := proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	exactScope.Authority = proto.Clone(discovered.GetObservedAuthority()).(*velav1.StageAuthority)
	exact, err := modelruntimetransport.ValidateExecutionDrainScope(agent.floor.validator, exactScope, 0, false)
	if err != nil {
		return proof, err
	}
	state := discovered.GetInspection().GetState()
	if state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED && state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED {
		response, err := client.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{
			Authority: proto.Clone(exact.Authority).(*velav1.StageAuthority),
			Reason:    velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
		})
		if err != nil {
			return proof, err
		}
		if err := ctx.Err(); err != nil {
			return proof, err
		}
		valid, _ := validCancelResponse(response, exact.Digest, exact.Authority, memberID)
		if !valid || len(response.ProtoReflect().GetUnknown()) != 0 || !proto.Equal(response.GetRuntimeIdentity(), exactScope.GetIdentity()) {
			return proof, errors.New("live backend cancellation response is invalid or rejected")
		}
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	checkpoint, err := agent.drainMemberExactExecution(ctx, memberID, exactScope)
	if err != nil {
		return proof, err
	}
	// Re-read the persisted checkpoint through the original query. Never relabel
	// an exact response's correlation digest to make it an allocation response.
	read, err := client.InspectStageAllocationDrain(ctx, &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{
		Scope: proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope),
	})
	if err != nil {
		return proof, err
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	if read == nil || len(read.ProtoReflect().GetUnknown()) != 0 ||
		modelruntimetransport.ValidateAllocationDrainResult(agent.floor.validator, scope, verified.Digest, read.GetResult()) != nil ||
		read.GetResult().GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || !proto.Equal(read.GetResult().GetCheckpoint(), checkpoint) {
		return proof, errors.New("live backend drain checkpoint was not recovered exactly")
	}
	proof.Drain = proto.Clone(read.GetResult()).(*velav1.ModelRuntimeExecutionDrainResult)
	return proof, nil
}

// A lost drain/stop-observation reply may leave a durable checkpoint and an
// unreconciled active slot. Retry exact drain only at the original current owner.
func (agent *Agent) finishMemberDrainedExecution(ctx context.Context, memberID string, query *velav1.ModelRuntimeExecutionDrainScope, saved *velav1.ModelRuntimeExecutionDrainCheckpoint) error {
	scope, err := agent.executionDrainMemberScope(saved.GetAuthority(), memberID, nil, false)
	if err != nil || !proto.Equal(scope.GetIdentity(), query.GetIdentity()) {
		return nil
	}
	expected := proto.Clone(saved).(*velav1.ModelRuntimeExecutionDrainCheckpoint)
	checkpoint, err := agent.drainMemberExactExecution(ctx, memberID, scope)
	if err != nil {
		return err
	}
	if !proto.Equal(checkpoint, expected) {
		return errors.New("replayed exact drain changed its durable checkpoint")
	}
	return nil
}

func (agent *Agent) drainMemberExactExecution(ctx context.Context, memberID string, scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainCheckpoint, error) {
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(agent.floor.validator, scope, 0, false)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	drained, err := agent.members[memberID].DrainStageExecution(ctx, &velav1.ModelRuntimeServiceDrainStageExecutionRequest{
		Scope: proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope),
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if drained == nil || len(drained.ProtoReflect().GetUnknown()) != 0 ||
		modelruntimetransport.ValidateExecutionDrainResult(scope, verified.Digest, drained.GetResult()) != nil ||
		drained.GetResult().GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || drained.GetResult().GetCheckpoint() == nil {
		return nil, errors.New("live backend writer drain is unproven")
	}
	return proto.Clone(drained.GetResult().GetCheckpoint()).(*velav1.ModelRuntimeExecutionDrainCheckpoint), nil
}
