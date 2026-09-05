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

func (supervisor *Supervisor) CheckpointStageTerminalNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest) (*velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "terminal non-admission checkpoint request is invalid")
	}
	result, err := supervisor.terminalNonAdmissionRPC(ctx, request.GetScope(), false)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse{Result: result}, nil
}

func (supervisor *Supervisor) InspectStageTerminalNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest) (*velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "terminal non-admission inspection request is invalid")
	}
	result, err := supervisor.terminalNonAdmissionRPC(ctx, request.GetScope(), true)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse{Result: result}, nil
}

func (supervisor *Supervisor) terminalNonAdmissionRPC(ctx context.Context, scope *velav1.ModelRuntimeTerminalAllocationScope, historical bool) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error) {
	if ctx == nil || supervisor == nil || supervisor.floor == nil || supervisor.admission == nil || supervisor.admission.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "durable terminal non-admission is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := modelruntimetransport.ValidateTerminalAllocationScope(supervisor.floor.validator, scope, historical)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "terminal non-admission scope is invalid")
	}
	if supervisor.routeIdentity(scope.GetIdentity()) == nil || supervisor.matchExecutionFloorScope(verified.Disposition, false) != nil {
		return nil, status.Error(codes.FailedPrecondition, "terminal non-admission reader or topology is stale")
	}
	result := &velav1.ModelRuntimeTerminalNonAdmissionResult{SchemaVersion: 1, Identity: proto.Clone(scope.GetIdentity()).(*velav1.ModelRuntimeIdentity),
		DispositionDigest: append([]byte(nil), verified.Digest[:]...), StageAllocationId: scope.GetStageAllocationId(),
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED}
	var checkpoint *TerminalNonAdmissionCheckpoint
	if historical {
		checkpoint, err = supervisor.InspectTerminalNonAdmission(ctx, verified.Disposition, scope.GetStageAllocationId())
	} else {
		checkpoint, err = supervisor.CheckpointTerminalNonAdmission(ctx, verified.Disposition, scope.GetStageAllocationId())
	}
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		if errors.Is(err, ErrExecutionStateRecovery) {
			return nil, status.Error(codes.FailedPrecondition, "terminal non-admission journal requires recovery")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return result, nil
	}
	result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	if checkpoint != nil {
		result.Checkpoint = &velav1.ModelRuntimeTerminalNonAdmissionCheckpoint{SchemaVersion: 1,
			Disposition: proto.Clone(checkpoint.Disposition).(*velav1.StageTerminalDisposition), DispositionDigest: append([]byte(nil), checkpoint.DispositionDigest[:]...),
			WorkerMemberId: checkpoint.WorkerMemberID, StageAllocationId: checkpoint.StageAllocationID, ExecutionSequence: checkpoint.ExecutionSequence,
			InstalledCutoff: checkpoint.InstalledCutoff, Contract: checkpoint.Contract, ObservedAt: timestamppb.New(checkpoint.ObservedAt)}
	}
	if modelruntimetransport.ValidateTerminalNonAdmissionResult(supervisor.floor.validator, scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "persisted terminal non-admission checkpoint is invalid")
	}
	return result, nil
}
