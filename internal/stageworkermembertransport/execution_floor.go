package stageworkermembertransport

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (client *Client) InstallStageExecutionFloor(ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	if client == nil || client.service == nil || client.floorValidator == nil || ctx == nil {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := modelruntimetransport.ValidateExecutionFloorRequest(client.floorValidator, request)
	if err != nil || request.GetIdentity().GetWorkerMemberId() != client.targetID {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor request is invalid or stale")
	}
	for _, allocation := range verified.Disposition.GetAllocations() {
		for _, member := range allocation.GetMembers() {
			if member.GetWorkerMemberId() == client.targetID && !bytes.Equal(member.GetIdentityDigest(), client.targetIdentityDigest[:]) {
				return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor target identity is stale")
			}
		}
	}
	command := proto.Clone(request).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest)
	command.Disposition = verified.Disposition
	response, err := client.service.InstallStageExecutionFloor(ctx, &velav1.StageWorkerMemberServiceInstallStageExecutionFloorRequest{
		TargetWorkerMemberId: client.targetID, Command: command,
	}, options...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 ||
		modelruntimetransport.ValidateExecutionFloorAcknowledgementForVersion(command.GetSchemaVersion(), command.GetIdentity(), verified.Digest, verified.Disposition.GetCutoff(), response.GetResult()) != nil {
		return nil, status.Error(codes.DataLoss, "Stage Worker member floor acknowledgement is invalid")
	}
	return proto.Clone(response.GetResult()).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse), nil
}

func (server *Server) InstallStageExecutionFloor(ctx context.Context, request *velav1.StageWorkerMemberServiceInstallStageExecutionFloorRequest) (*velav1.StageWorkerMemberServiceInstallStageExecutionFloorResponse, error) {
	if server == nil || server.authenticator == nil || server.validator == nil || server.runtime == nil || ctx == nil {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor server is not configured")
	}
	if request == nil || request.GetCommand() == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		request.GetTargetWorkerMemberId() != server.localMember.ID {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member floor target is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	peer, err := server.authenticator.Authenticate(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "authenticate Stage Worker member floor peer")
	}
	verified, err := modelruntimetransport.ValidateExecutionFloorRequest(server.validator, request.GetCommand())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor request is invalid or stale")
	}
	command := proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest)
	command.Disposition = verified.Disposition
	knownTarget := false
	for _, identity := range server.localIdentities {
		knownTarget = knownTarget || proto.Equal(identity, command.GetIdentity())
	}
	if !knownTarget {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor target is not resident")
	}
	peerDigest := sha256.Sum256([]byte(peer.SPIFFEID))
	for _, allocation := range verified.Disposition.GetAllocations() {
		if len(allocation.GetMembers()) != len(server.membersByID) {
			return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor membership is incomplete")
		}
		for _, member := range allocation.GetMembers() {
			configured, found := server.membersByID[member.GetWorkerMemberId()]
			if !found || configured.Epoch != member.GetMemberEpoch() {
				return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor membership is stale")
			}
		}
		// Verified terminal members are canonical and sorted by UUID.
		leader := allocation.GetMembers()[0]
		if !bytes.Equal(peerDigest[:], leader.GetIdentityDigest()) {
			return nil, status.Error(codes.PermissionDenied, "only the deterministic WorkerMember leader may install remote floors")
		}
		if command.GetSchemaVersion() == 1 && !server.matchesFloorAllocation(command, allocation) {
			return nil, status.Error(codes.FailedPrecondition, "Stage Worker member floor historical runtime is not resident")
		}
	}
	result, err := server.runtime.InstallStageExecutionFloor(ctx, command, grpc.MaxCallSendMsgSize(4<<20))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := modelruntimetransport.ValidateExecutionFloorAcknowledgementForVersion(command.GetSchemaVersion(), command.GetIdentity(), verified.Digest, verified.Disposition.GetCutoff(), result); err != nil {
		return nil, status.Error(codes.DataLoss, "local ModelRuntime floor acknowledgement is invalid")
	}
	return &velav1.StageWorkerMemberServiceInstallStageExecutionFloorResponse{
		Result: proto.Clone(result).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse),
	}, nil
}

func (server *Server) matchesFloorAllocation(command *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest, allocation *velav1.StageTerminalAllocation) bool {
	for _, member := range allocation.GetMembers() {
		if member.GetWorkerMemberId() != server.localMember.ID {
			continue
		}
		for _, identity := range server.localIdentities {
			if identity.GetWorkerInstanceId() == command.GetIdentity().GetWorkerInstanceId() &&
				identity.GetWorkerInstanceEpoch() == command.GetIdentity().GetWorkerInstanceEpoch() &&
				bytes.Equal(identity.GetDeviceSetDigest(), command.GetIdentity().GetDeviceSetDigest()) &&
				bytes.Equal(identity.GetMembershipDigest(), command.GetIdentity().GetMembershipDigest()) &&
				identity.GetModelResidencyId() == allocation.GetModelResidencyId() && identity.GetRuntimeIdentity() == allocation.GetModelRuntimeIdentity() &&
				identity.GetModelRuntimeEpoch() == member.GetModelRuntimeEpoch() && identity.GetStageProfileRevisionId() == allocation.GetStageProfileRevisionId() {
				return true
			}
		}
	}
	return false
}
