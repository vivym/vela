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

func (supervisor *Supervisor) CheckpointStageNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest) (*velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "non-admission checkpoint request is invalid")
	}
	result, err := supervisor.nonAdmissionRPC(ctx, request.GetScope(), false)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse{Result: result}, nil
}

func (supervisor *Supervisor) InspectStageNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest) (*velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "non-admission inspection request is invalid")
	}
	result, err := supervisor.nonAdmissionRPC(ctx, request.GetScope(), true)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse{Result: result}, nil
}

func (supervisor *Supervisor) nonAdmissionRPC(ctx context.Context, scope *velav1.ModelRuntimeExecutionDrainScope, historical bool) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
	if ctx == nil || supervisor == nil || supervisor.floor == nil || supervisor.admission == nil || supervisor.admission.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "durable non-admission is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(supervisor.floor.validator, scope, supervisor.services[0].maxClockSkew, historical)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "non-admission scope is invalid")
	}
	if supervisor.routeIdentity(scope.GetIdentity()) == nil || supervisor.matchRetainedExecutionScope(verified.Authority) != nil {
		return nil, status.Error(codes.FailedPrecondition, "non-admission reader or topology is stale")
	}
	result := &velav1.ModelRuntimeExecutionNonAdmissionResult{SchemaVersion: 1, Identity: proto.Clone(scope.GetIdentity()).(*velav1.ModelRuntimeIdentity),
		AuthorityDigest: append([]byte(nil), verified.Digest[:]...), Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED}
	var checkpoint *ExecutionNonAdmissionCheckpoint
	if historical {
		checkpoint, err = supervisor.InspectNonAdmission(ctx, verified.Authority)
	} else {
		checkpoint, err = supervisor.CheckpointNonAdmission(ctx, verified.Authority)
	}
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		if errors.Is(err, ErrExecutionStateRecovery) {
			return nil, status.Error(codes.FailedPrecondition, "non-admission journal requires recovery")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return result, nil
	}
	result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	if checkpoint != nil {
		result.Checkpoint = &velav1.ModelRuntimeExecutionNonAdmissionCheckpoint{SchemaVersion: 1,
			Authority: proto.Clone(checkpoint.Authority).(*velav1.StageAuthority), AuthorityDigest: append([]byte(nil), checkpoint.AuthorityDigest[:]...),
			WorkerMemberId: checkpoint.WorkerMemberID, ExecutionSequence: checkpoint.ExecutionSequence, InstalledCutoff: checkpoint.InstalledCutoff,
			Contract: checkpoint.Contract, ObservedAt: timestamppb.New(checkpoint.ObservedAt)}
	}
	if modelruntimetransport.ValidateExecutionNonAdmissionResult(supervisor.floor.validator, scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "persisted non-admission checkpoint is invalid")
	}
	return result, nil
}
