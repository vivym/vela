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

func (client *Client) InspectStageAllocationAuthorities(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse, error) {
	if !validAllocationAuthoritiesRequest(request) {
		return nil, status.Error(codes.InvalidArgument, "member allocation authorities request is invalid")
	}
	if client == nil || client.service == nil || client.floorValidator == nil || ctx == nil {
		return nil, status.Error(codes.FailedPrecondition, "member allocation authorities client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	scope := proto.Clone(request.GetScope()).(*velav1.ModelRuntimeExecutionDrainScope)
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(client.floorValidator, scope, 0, true)
	if err != nil || scope.GetIdentity().GetWorkerMemberId() != client.targetID || !client.matchesTargetAuthority(verified.Authority) {
		return nil, status.Error(codes.FailedPrecondition, "member allocation authorities scope is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response, err := client.service.InspectStageAllocationAuthorities(ctx, &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest{
		TargetWorkerMemberId: client.targetID, Command: &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{
			Scope: proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope),
		},
	}, options...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 ||
		modelruntimetransport.ValidateAllocationAuthoritiesResponse(client.floorValidator, scope, verified.Digest, response.GetResult()) != nil {
		return nil, status.Error(codes.DataLoss, "member allocation authorities response is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return proto.Clone(response.GetResult()).(*velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse), nil
}

func (server *Server) InspectStageAllocationAuthorities(ctx context.Context, request *velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest) (*velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesResponse, error) {
	if ctx == nil || request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || !validAllocationAuthoritiesRequest(request.GetCommand()) {
		return nil, status.Error(codes.InvalidArgument, "member allocation authorities request is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	scope := proto.Clone(request.GetCommand().GetScope()).(*velav1.ModelRuntimeExecutionDrainScope)
	verified, err := server.authorizePeer(ctx, request.GetTargetWorkerMemberId(), scope.GetAuthority(), true)
	if err != nil {
		return nil, err
	}
	if _, err := modelruntimetransport.ValidateExecutionDrainScope(server.validator, scope, server.maxClockSkew, true); err != nil || scope.GetIdentity().GetWorkerMemberId() != request.GetTargetWorkerMemberId() {
		return nil, status.Error(codes.FailedPrecondition, "member allocation authorities scope is invalid")
	}
	known := false
	for _, identity := range server.localIdentities {
		known = known || proto.Equal(identity, scope.GetIdentity())
	}
	if !known {
		return nil, status.Error(codes.FailedPrecondition, "member allocation history reader is not resident")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response, err := server.runtime.InspectStageAllocationAuthorities(ctx, &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{
		Scope: proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope),
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if modelruntimetransport.ValidateAllocationAuthoritiesResponse(server.validator, scope, verified.Digest, response) != nil {
		return nil, status.Error(codes.DataLoss, "local allocation authorities response is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesResponse{
		Result: proto.Clone(response).(*velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse),
	}, nil
}

func validAllocationAuthoritiesRequest(request *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest) bool {
	return request != nil && len(request.ProtoReflect().GetUnknown()) == 0 && request.GetScope() != nil && proto.Size(request.GetScope()) <= 65<<10
}
