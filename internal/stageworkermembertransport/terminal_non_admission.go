package stageworkermembertransport

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (client *Client) CheckpointStageTerminalNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member terminal non-admission request is invalid")
	}
	result, err := client.terminalNonAdmission(ctx, request.GetScope(), false, options...)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse{Result: result}, nil
}

func (client *Client) InspectStageTerminalNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member terminal non-admission inspection request is invalid")
	}
	result, err := client.terminalNonAdmission(ctx, request.GetScope(), true, options...)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse{Result: result}, nil
}

func (client *Client) terminalNonAdmission(ctx context.Context, scope *velav1.ModelRuntimeTerminalAllocationScope, historical bool, options ...grpc.CallOption) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error) {
	if client == nil || client.service == nil || client.floorValidator == nil || ctx == nil {
		return nil, status.Error(codes.FailedPrecondition, "member terminal non-admission client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	verified, err := modelruntimetransport.ValidateTerminalAllocationScope(client.floorValidator, scope, historical)
	if err != nil || scope.GetIdentity().GetWorkerMemberId() != client.targetID {
		return nil, status.Error(codes.FailedPrecondition, "member terminal non-admission scope is invalid")
	}
	for _, allocation := range verified.Disposition.GetAllocations() {
		for _, member := range allocation.GetMembers() {
			if member.GetWorkerMemberId() == client.targetID && !bytes.Equal(member.GetIdentityDigest(), client.targetIdentityDigest[:]) {
				return nil, status.Error(codes.FailedPrecondition, "member terminal non-admission target identity is stale")
			}
		}
	}
	scope = proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)
	sent := proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)
	var result *velav1.ModelRuntimeTerminalNonAdmissionResult
	if historical {
		var response *velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionResponse
		response, err = client.service.InspectStageTerminalNonAdmission(ctx, &velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionRequest{TargetWorkerMemberId: client.targetID,
			Command: &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: sent}}, options...)
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || response.GetResult() == nil || len(response.GetResult().ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "member terminal non-admission inspection wrapper is invalid")
			}
			result = response.GetResult().GetResult()
		}
	} else {
		var response *velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionResponse
		response, err = client.service.CheckpointStageTerminalNonAdmission(ctx, &velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionRequest{TargetWorkerMemberId: client.targetID,
			Command: &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: sent}}, options...)
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || response.GetResult() == nil || len(response.GetResult().ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "member terminal non-admission checkpoint wrapper is invalid")
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
	if modelruntimetransport.ValidateTerminalNonAdmissionResult(client.floorValidator, scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "member terminal non-admission checkpoint is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeTerminalNonAdmissionResult), nil
}

func (server *Server) CheckpointStageTerminalNonAdmission(ctx context.Context, request *velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionRequest) (*velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || request.GetCommand() == nil || len(request.GetCommand().ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member terminal non-admission request is invalid")
	}
	result, err := server.terminalNonAdmission(ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetScope(), false)
	if err != nil {
		return nil, err
	}
	return &velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse{Result: result}}, nil
}

func (server *Server) InspectStageTerminalNonAdmission(ctx context.Context, request *velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionRequest) (*velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || request.GetCommand() == nil || len(request.GetCommand().ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "member terminal non-admission inspection request is invalid")
	}
	result, err := server.terminalNonAdmission(ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetScope(), true)
	if err != nil {
		return nil, err
	}
	return &velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse{Result: result}}, nil
}

func (server *Server) terminalNonAdmission(ctx context.Context, targetID string, scope *velav1.ModelRuntimeTerminalAllocationScope, historical bool) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error) {
	verified, err := server.authorizeTerminalNonAdmission(ctx, targetID, scope, historical)
	if err != nil {
		return nil, err
	}
	scope = proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)
	sent := proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)
	var result *velav1.ModelRuntimeTerminalNonAdmissionResult
	if historical {
		var response *velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse
		response, err = server.runtime.InspectStageTerminalNonAdmission(ctx, &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: sent})
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "local terminal non-admission inspection wrapper is invalid")
			}
			result = response.GetResult()
		}
	} else {
		var response *velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse
		response, err = server.runtime.CheckpointStageTerminalNonAdmission(ctx, &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: sent})
		if err == nil {
			if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
				return nil, status.Error(codes.DataLoss, "local terminal non-admission checkpoint wrapper is invalid")
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
	if modelruntimetransport.ValidateTerminalNonAdmissionResult(server.validator, scope, verified.Digest, result) != nil {
		return nil, status.Error(codes.DataLoss, "local terminal non-admission checkpoint is invalid")
	}
	return proto.Clone(result).(*velav1.ModelRuntimeTerminalNonAdmissionResult), nil
}

func (server *Server) authorizeTerminalNonAdmission(ctx context.Context, targetID string, scope *velav1.ModelRuntimeTerminalAllocationScope, historical bool) (stageauthority.VerifiedTerminalDisposition, error) {
	empty := stageauthority.VerifiedTerminalDisposition{}
	if server == nil || server.authenticator == nil || server.validator == nil || server.runtime == nil || ctx == nil {
		return empty, status.Error(codes.FailedPrecondition, "member terminal non-admission server is not configured")
	}
	if err := ctx.Err(); err != nil {
		return empty, status.FromContextError(err).Err()
	}
	if targetID != server.localMember.ID {
		return empty, status.Error(codes.InvalidArgument, "terminal non-admission target is not the local member")
	}
	peer, err := server.authenticator.Authenticate(ctx)
	if err != nil {
		return empty, status.Error(codes.Unauthenticated, "authenticate terminal non-admission peer")
	}
	verified, err := modelruntimetransport.ValidateTerminalAllocationScope(server.validator, scope, historical)
	if err != nil || scope.GetIdentity().GetWorkerMemberId() != targetID {
		return empty, status.Error(codes.FailedPrecondition, "member terminal non-admission scope is invalid")
	}
	known := false
	for _, identity := range server.localIdentities {
		known = known || proto.Equal(identity, scope.GetIdentity())
	}
	if !known {
		return empty, status.Error(codes.FailedPrecondition, "terminal non-admission journal owner is not resident")
	}
	peerDigest := sha256.Sum256([]byte(peer.SPIFFEID))
	for _, allocation := range verified.Disposition.GetAllocations() {
		if len(allocation.GetMembers()) != len(server.membersByID) {
			return empty, status.Error(codes.FailedPrecondition, "terminal non-admission membership is incomplete")
		}
		for _, member := range allocation.GetMembers() {
			configured, ok := server.membersByID[member.GetWorkerMemberId()]
			if !ok || configured.Epoch != member.GetMemberEpoch() {
				return empty, status.Error(codes.FailedPrecondition, "terminal non-admission membership is stale")
			}
		}
		// Signed terminal membership is canonical and sorted by UUID.
		if !bytes.Equal(peerDigest[:], allocation.GetMembers()[0].GetIdentityDigest()) {
			return empty, status.Error(codes.PermissionDenied, "only the deterministic WorkerMember leader may query terminal non-admission")
		}
	}
	return verified, nil
}
