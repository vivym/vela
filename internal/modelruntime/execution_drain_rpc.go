package modelruntime

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (supervisor *Supervisor) DrainStageExecution(ctx context.Context, request *velav1.ModelRuntimeServiceDrainStageExecutionRequest) (*velav1.ModelRuntimeServiceDrainStageExecutionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "execution drain request is invalid")
	}
	result, err := supervisor.executionDrainRPC(ctx, request.GetScope(), false, false)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceDrainStageExecutionResponse{Result: result}, nil
}

func (supervisor *Supervisor) InspectStageExecutionDrain(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest) (*velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "execution drain inspection request is invalid")
	}
	result, err := supervisor.executionDrainRPC(ctx, request.GetScope(), true, false)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse{Result: result}, nil
}

func (supervisor *Supervisor) InspectStageAllocationDrain(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest) (*velav1.ModelRuntimeServiceInspectStageAllocationDrainResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "allocation drain inspection request is invalid")
	}
	result, err := supervisor.executionDrainRPC(ctx, request.GetScope(), true, true)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInspectStageAllocationDrainResponse{Result: result}, nil
}

func (supervisor *Supervisor) executionDrainRPC(ctx context.Context, scope *velav1.ModelRuntimeExecutionDrainScope, historical, allocation bool) (*velav1.ModelRuntimeExecutionDrainResult, error) {
	if ctx == nil || supervisor == nil || supervisor.floor == nil || supervisor.admission == nil ||
		supervisor.admission.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "durable execution drain is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(supervisor.floor.validator, scope, supervisor.services[0].maxClockSkew, historical)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "execution drain scope is invalid")
	}
	if supervisor.routeIdentity(scope.GetIdentity()) == nil || supervisor.matchRetainedExecutionScope(verified.Authority) != nil {
		return nil, status.Error(codes.FailedPrecondition, "execution drain target or topology is stale")
	}
	result := &velav1.ModelRuntimeExecutionDrainResult{SchemaVersion: 1,
		Identity: proto.Clone(scope.GetIdentity()).(*velav1.ModelRuntimeIdentity), AuthorityDigest: append([]byte(nil), verified.Digest[:]...),
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED}
	var checkpoint *ExecutionDrainCheckpoint
	if allocation {
		checkpoint, err = supervisor.InspectAllocationDrain(ctx, verified.Authority)
	} else if historical {
		checkpoint, err = supervisor.InspectExecutionDrain(ctx, verified.Authority)
	} else {
		checkpoint, err = supervisor.DrainExecution(ctx, verified.Authority)
	}
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		if errors.Is(err, ErrExecutionStateRecovery) {
			return nil, status.Error(codes.FailedPrecondition, "execution drain journal requires recovery")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		result.Detail = "execution writer drain is unproven"
		return result, nil
	}
	result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	result.Detail = "exact durable execution drain is unknown"
	if checkpoint != nil {
		result.Checkpoint = &velav1.ModelRuntimeExecutionDrainCheckpoint{SchemaVersion: 1,
			Authority: proto.Clone(checkpoint.Authority).(*velav1.StageAuthority), AuthorityDigest: append([]byte(nil), checkpoint.Result.AuthorityDigest[:]...),
			WorkerMemberId: checkpoint.WorkerMemberID, ExecutionSequence: checkpoint.Result.ExecutionSequence,
			Contract: checkpoint.Result.Contract, DrainedAt: timestamppb.New(checkpoint.DrainedAt)}
		result.Detail = "exact execution writer drain is durably checkpointed"
	}
	err = modelruntimetransport.ValidateExecutionDrainResult(scope, verified.Digest, result)
	if allocation {
		err = modelruntimetransport.ValidateAllocationDrainResult(supervisor.floor.validator, scope, verified.Digest, result)
	}
	if err != nil {
		return nil, status.Error(codes.DataLoss, "persisted execution drain checkpoint is invalid")
	}
	return result, nil
}
