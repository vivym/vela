package stageworkermembertransport

import (
	"context"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (client *Client) CheckpointStageNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member non-admission request is invalid")
	}
	result, err := client.nonAdmission(ctx, request.GetScope(), false, options...)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse{Result: result}, nil
}

func (client *Client) InspectStageNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member non-admission inspection request is invalid")
	}
	result, err := client.nonAdmission(ctx, request.GetScope(), true, options...)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse{Result: result}, nil
}

func (client *Client) nonAdmission(ctx context.Context, scope *velav1.ModelRuntimeExecutionDrainScope, historical bool, options ...grpc.CallOption) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
	if client == nil || client.service == nil || client.floorValidator == nil || ctx == nil {
		return nil, status.Error(codes.FailedPrecondition, "member non-admission client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(client.floorValidator, scope, 0, historical)
	if err != nil || scope.GetIdentity().GetWorkerMemberId() != client.targetID || !client.matchesTargetAuthority(verified.Authority) {
		return nil, status.Error(codes.FailedPrecondition, "member non-admission scope is invalid")
	}
	scope = proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	sent := proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	var result *velav1.ModelRuntimeExecutionNonAdmissionResult
	if historical {
		var response *velav1.StageWorkerMemberServiceInspectStageNonAdmissionResponse
		response, err = client.service.InspectStageNonAdmission(ctx, &velav1.StageWorkerMemberServiceInspectStageNonAdmissionRequest{TargetWorkerMemberId: client.targetID,
			Command: &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: sent}}, options...)
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || response.GetResult() == nil || len(response.GetResult().ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "member non-admission inspection wrapper is invalid")
			}
			result = response.GetResult().GetResult()
		}
	} else {
		var response *velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionResponse
		response, err = client.service.CheckpointStageNonAdmission(ctx, &velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionRequest{TargetWorkerMemberId: client.targetID,
			Command: &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: sent}}, options...)
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || response.GetResult() == nil || len(response.GetResult().ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "member non-admission checkpoint wrapper is invalid")
			}
			result = response.GetResult().GetResult()
		}
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if modelruntimetransport.ValidateExecutionNonAdmissionResult(client.floorValidator, scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "member non-admission checkpoint is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeExecutionNonAdmissionResult), nil
}

func (server *Server) CheckpointStageNonAdmission(ctx context.Context, request *velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionRequest) (*velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || request.GetCommand() == nil || len(request.GetCommand().ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member non-admission request is invalid")
	}
	result, err := server.nonAdmission(ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetScope(), false)
	if err != nil {
		return nil, err
	}
	return &velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse{Result: result}}, nil
}

func (server *Server) InspectStageNonAdmission(ctx context.Context, request *velav1.StageWorkerMemberServiceInspectStageNonAdmissionRequest) (*velav1.StageWorkerMemberServiceInspectStageNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || request.GetCommand() == nil || len(request.GetCommand().ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member non-admission inspection request is invalid")
	}
	result, err := server.nonAdmission(ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetScope(), true)
	if err != nil {
		return nil, err
	}
	return &velav1.StageWorkerMemberServiceInspectStageNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse{Result: result}}, nil
}

func (server *Server) nonAdmission(ctx context.Context, targetID string, scope *velav1.ModelRuntimeExecutionDrainScope, historical bool) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
	if ctx == nil || scope == nil {
		return nil, status.Error(codes.InvalidArgument, "member non-admission scope is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := server.authorizePeer(ctx, targetID, scope.GetAuthority(), true)
	if err != nil {
		return nil, err
	}
	if _, err := modelruntimetransport.ValidateExecutionDrainScope(server.validator, scope, server.maxClockSkew, historical); err != nil || scope.GetIdentity().GetWorkerMemberId() != targetID {
		return nil, status.Error(codes.FailedPrecondition, "member non-admission scope is invalid")
	}
	known := false
	for _, identity := range server.localIdentities {
		known = known || proto.Equal(identity, scope.GetIdentity())
	}
	if !known {
		return nil, status.Error(codes.FailedPrecondition, "member non-admission journal owner is not resident")
	}
	scope = proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	sent := proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	var result *velav1.ModelRuntimeExecutionNonAdmissionResult
	if historical {
		var response *velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse
		response, err = server.runtime.InspectStageNonAdmission(ctx, &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: sent})
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "local non-admission inspection wrapper is invalid")
			}
			result = response.GetResult()
		}
	} else {
		var response *velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse
		response, err = server.runtime.CheckpointStageNonAdmission(ctx, &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: sent})
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "local non-admission checkpoint wrapper is invalid")
			}
			result = response.GetResult()
		}
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if modelruntimetransport.ValidateExecutionNonAdmissionResult(server.validator, scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "local non-admission checkpoint is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeExecutionNonAdmissionResult), nil
}
