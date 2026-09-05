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

func (client *Client) DrainStageExecution(ctx context.Context, request *velav1.ModelRuntimeServiceDrainStageExecutionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceDrainStageExecutionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member drain request is invalid")
	}
	result, err := client.executionDrain(ctx, request.GetScope(), false, options...)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceDrainStageExecutionResponse{Result: result}, nil
}

func (client *Client) InspectStageExecutionDrain(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member drain inspection request is invalid")
	}
	result, err := client.executionDrain(ctx, request.GetScope(), true, options...)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse{Result: result}, nil
}

func (client *Client) executionDrain(ctx context.Context, scope *velav1.ModelRuntimeExecutionDrainScope, historical bool, options ...grpc.CallOption) (*velav1.ModelRuntimeExecutionDrainResult, error) {
	if client == nil || client.service == nil || client.floorValidator == nil || ctx == nil {
		return nil, status.Error(codes.FailedPrecondition, "member drain client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(client.floorValidator, scope, 0, historical)
	if err != nil || scope.GetIdentity().GetWorkerMemberId() != client.targetID || !client.matchesTargetAuthority(verified.Authority) {
		return nil, status.Error(codes.FailedPrecondition, "member drain scope is invalid")
	}
	scope = proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	// Keep the expected scope separate from any in-process client mutation.
	sent := proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	var result *velav1.ModelRuntimeExecutionDrainResult
	if historical {
		var response *velav1.StageWorkerMemberServiceInspectStageExecutionDrainResponse
		response, err = client.service.InspectStageExecutionDrain(ctx, &velav1.StageWorkerMemberServiceInspectStageExecutionDrainRequest{
			TargetWorkerMemberId: client.targetID, Command: &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: sent}}, options...)
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || response.GetResult() == nil || len(response.GetResult().ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "member drain inspection wrapper is invalid")
			}
			result = response.GetResult().GetResult()
		}
	} else {
		var response *velav1.StageWorkerMemberServiceDrainStageExecutionResponse
		response, err = client.service.DrainStageExecution(ctx, &velav1.StageWorkerMemberServiceDrainStageExecutionRequest{
			TargetWorkerMemberId: client.targetID, Command: &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: sent}}, options...)
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || response.GetResult() == nil || len(response.GetResult().ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "member drain wrapper is invalid")
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
	if modelruntimetransport.ValidateExecutionDrainResult(scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "member drain checkpoint is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeExecutionDrainResult), nil
}

func (server *Server) DrainStageExecution(ctx context.Context, request *velav1.StageWorkerMemberServiceDrainStageExecutionRequest) (*velav1.StageWorkerMemberServiceDrainStageExecutionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || request.GetCommand() == nil || len(request.GetCommand().ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member drain request is invalid")
	}
	result, err := server.executionDrain(ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetScope(), false)
	if err != nil {
		return nil, err
	}
	return &velav1.StageWorkerMemberServiceDrainStageExecutionResponse{Result: &velav1.ModelRuntimeServiceDrainStageExecutionResponse{Result: result}}, nil
}

func (server *Server) InspectStageExecutionDrain(ctx context.Context, request *velav1.StageWorkerMemberServiceInspectStageExecutionDrainRequest) (*velav1.StageWorkerMemberServiceInspectStageExecutionDrainResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || request.GetCommand() == nil || len(request.GetCommand().ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member drain inspection request is invalid")
	}
	result, err := server.executionDrain(ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetScope(), true)
	if err != nil {
		return nil, err
	}
	return &velav1.StageWorkerMemberServiceInspectStageExecutionDrainResponse{Result: &velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse{Result: result}}, nil
}

func (server *Server) executionDrain(ctx context.Context, targetID string, scope *velav1.ModelRuntimeExecutionDrainScope, historical bool) (*velav1.ModelRuntimeExecutionDrainResult, error) {
	if ctx == nil || scope == nil {
		return nil, status.Error(codes.InvalidArgument, "member drain scope is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := server.authorizePeer(ctx, targetID, scope.GetAuthority(), true)
	if err != nil {
		return nil, err
	}
	if _, err := modelruntimetransport.ValidateExecutionDrainScope(server.validator, scope, server.maxClockSkew, historical); err != nil || scope.GetIdentity().GetWorkerMemberId() != targetID {
		return nil, status.Error(codes.FailedPrecondition, "member drain scope is invalid")
	}
	known := false
	for _, identity := range server.localIdentities {
		known = known || proto.Equal(identity, scope.GetIdentity())
	}
	if !known {
		return nil, status.Error(codes.FailedPrecondition, "member drain journal owner is not resident")
	}
	scope = proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	sent := proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	var result *velav1.ModelRuntimeExecutionDrainResult
	if historical {
		var response *velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse
		response, err = server.runtime.InspectStageExecutionDrain(ctx, &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: sent})
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "local drain inspection wrapper is invalid")
			}
			result = response.GetResult()
		}
	} else {
		var response *velav1.ModelRuntimeServiceDrainStageExecutionResponse
		response, err = server.runtime.DrainStageExecution(ctx, &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: sent})
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "local drain wrapper is invalid")
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
	if modelruntimetransport.ValidateExecutionDrainResult(scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "local execution drain checkpoint is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeExecutionDrainResult), nil
}
