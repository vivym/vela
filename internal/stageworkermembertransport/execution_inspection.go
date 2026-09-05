package stageworkermembertransport

import (
	"context"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (client *Client) InspectExecution(
	ctx context.Context, request *velav1.ModelRuntimeServiceInspectExecutionRequest, options ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceInspectExecutionResponse, error) {
	if client == nil || client.service == nil || ctx == nil || !validInspectionRequest(request) ||
		!client.matchesTargetAuthority(request.GetAuthority()) {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member inspection client or request is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	command := proto.Clone(request).(*velav1.ModelRuntimeServiceInspectExecutionRequest)
	digest, err := stageauthority.Digest(command.GetAuthority())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member inspection authority is invalid")
	}
	response, err := client.service.InspectExecution(ctx, &velav1.StageWorkerMemberServiceInspectExecutionRequest{
		TargetWorkerMemberId: client.targetID, Command: command,
	}, options...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	result := response.GetResult()
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || result == nil ||
		!runtimeIdentityMatchesAuthority(result.GetRuntimeIdentity(), command.GetAuthority(), MemberBinding{
			ID: client.targetID, Epoch: result.GetRuntimeIdentity().GetWorkerMemberEpoch(),
		}) || modelruntimetransport.ValidateExecutionInspectionResponse(digest, result.GetRuntimeIdentity(), result) != nil {
		return nil, status.Error(codes.DataLoss, "Stage Worker member execution inspection is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeServiceInspectExecutionResponse), nil
}

func (server *Server) InspectExecution(
	ctx context.Context, request *velav1.StageWorkerMemberServiceInspectExecutionRequest,
) (*velav1.StageWorkerMemberServiceInspectExecutionResponse, error) {
	if ctx == nil || request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || !validInspectionRequest(request.GetCommand()) {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member inspection request is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	command := proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServiceInspectExecutionRequest)
	identity, digest, err := server.authorize(ctx, request.GetTargetWorkerMemberId(), command.GetAuthority(), true)
	if err != nil {
		return nil, err
	}
	result, err := server.runtime.InspectExecution(ctx, command)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if modelruntimetransport.ValidateExecutionInspectionResponse(digest, identity, result) != nil {
		return nil, status.Error(codes.DataLoss, "local ModelRuntime execution inspection is invalid")
	}
	return &velav1.StageWorkerMemberServiceInspectExecutionResponse{
		Result: proto.Clone(result).(*velav1.ModelRuntimeServiceInspectExecutionResponse),
	}, nil
}

func validInspectionRequest(request *velav1.ModelRuntimeServiceInspectExecutionRequest) bool {
	return request != nil && request.GetSchemaVersion() == 1 && len(request.ProtoReflect().GetUnknown()) == 0 && request.GetAuthority() != nil
}
