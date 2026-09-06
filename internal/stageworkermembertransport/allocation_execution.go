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

func (client *Client) InspectAllocationExecution(ctx context.Context, request *velav1.ModelRuntimeServiceInspectAllocationExecutionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectAllocationExecutionResponse, error) {
	if client == nil || client.service == nil || client.floorValidator == nil || ctx == nil || !validAllocationInspectionRequest(request) ||
		!client.matchesTargetAuthority(request.GetAuthority()) {
		return nil, status.Error(codes.FailedPrecondition, "member allocation inspection client or request is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := client.floorValidator.ValidateEnvelopeForReplay(request.GetAuthority(), 0)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "member allocation inspection authority is invalid")
	}
	expected := verified.Authority
	response, err := client.service.InspectAllocationExecution(ctx, &velav1.StageWorkerMemberServiceInspectAllocationExecutionRequest{
		TargetWorkerMemberId: client.targetID, Command: &velav1.ModelRuntimeServiceInspectAllocationExecutionRequest{
			SchemaVersion: 1, Authority: proto.Clone(expected).(*velav1.StageAuthority),
		},
	}, options...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	result := response.GetResult()
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || result == nil ||
		!runtimeIdentityMatchesAuthority(result.GetRuntimeIdentity(), expected, MemberBinding{
			ID: client.targetID, Epoch: result.GetRuntimeIdentity().GetWorkerMemberEpoch(),
		}) || modelruntimetransport.ValidateAllocationExecutionResponse(client.floorValidator, expected, verified.Digest, result.GetRuntimeIdentity(), result) != nil {
		return nil, status.Error(codes.DataLoss, "member allocation execution observation is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeServiceInspectAllocationExecutionResponse), nil
}

func (server *Server) InspectAllocationExecution(ctx context.Context, request *velav1.StageWorkerMemberServiceInspectAllocationExecutionRequest) (*velav1.StageWorkerMemberServiceInspectAllocationExecutionResponse, error) {
	if ctx == nil || request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || !validAllocationInspectionRequest(request.GetCommand()) {
		return nil, status.Error(codes.InvalidArgument, "member allocation inspection request is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	command := proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServiceInspectAllocationExecutionRequest)
	identity, digest, err := server.authorize(ctx, request.GetTargetWorkerMemberId(), command.GetAuthority(), true)
	if err != nil {
		return nil, err
	}
	result, err := server.runtime.InspectAllocationExecution(ctx, proto.Clone(command).(*velav1.ModelRuntimeServiceInspectAllocationExecutionRequest))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if modelruntimetransport.ValidateAllocationExecutionResponse(server.validator, command.GetAuthority(), digest, identity, result) != nil {
		return nil, status.Error(codes.DataLoss, "local allocation execution observation is invalid")
	}
	return &velav1.StageWorkerMemberServiceInspectAllocationExecutionResponse{
		Result: proto.Clone(result).(*velav1.ModelRuntimeServiceInspectAllocationExecutionResponse),
	}, nil
}

func validAllocationInspectionRequest(request *velav1.ModelRuntimeServiceInspectAllocationExecutionRequest) bool {
	return request != nil && request.GetSchemaVersion() == 1 && len(request.ProtoReflect().GetUnknown()) == 0 &&
		request.GetAuthority() != nil && proto.Size(request.GetAuthority()) <= 64<<10
}
