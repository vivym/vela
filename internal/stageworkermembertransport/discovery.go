package stageworkermembertransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DiscoverRuntimeIdentities observes the pinned member before assignment. The
// caller must bind the result to its trusted launch topology and approved routes;
// these identities alone do not establish readiness or durable journal ownership.
// Optional Registry binding evidence is forwarded intact for the caller to verify.
func (client *Client) DiscoverRuntimeIdentities(ctx context.Context, request *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	if client == nil || client.service == nil || ctx == nil {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member discovery client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if !validDiscoveryRequest(request) || request.GetWorkerMemberId() != client.targetID {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member discovery request is invalid")
	}
	command := proto.Clone(request).(*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest)
	response, err := client.service.DiscoverRuntimeIdentities(ctx, &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{
		TargetWorkerMemberId: client.targetID, Command: proto.Clone(command).(*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest),
	}, options...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || !validDiscoveryResult(command, response.GetResult()) {
		return nil, status.Error(codes.DataLoss, "Stage Worker member discovery result is invalid")
	}
	return proto.Clone(response.GetResult()).(*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse), nil
}

func (server *Server) DiscoverRuntimeIdentities(ctx context.Context, request *velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest) (*velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesResponse, error) {
	if server == nil || server.authenticator == nil || server.runtime == nil || ctx == nil || server.discoveryLeaderDigest == ([sha256.Size]byte{}) {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member discovery requires trusted member identities")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || request.GetTargetWorkerMemberId() != server.localMember.ID || !validDiscoveryRequest(request.GetCommand()) {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member discovery request is invalid")
	}
	peer, err := server.authenticator.Authenticate(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "authenticate Stage Worker member discovery peer")
	}
	if sha256.Sum256([]byte(peer.SPIFFEID)) != server.discoveryLeaderDigest {
		return nil, status.Error(codes.PermissionDenied, "only the configured WorkerMember leader may discover remote runtimes")
	}
	command := proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest)
	if !proto.Equal(command, discoveryRequestFor(server.localIdentities[0])) {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member discovery scope is stale")
	}
	result, err := server.runtime.DiscoverRuntimeIdentities(ctx, proto.Clone(command).(*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if !validDiscoveryResult(command, result) || len(result.GetIdentities()) != len(server.localIdentities) {
		return nil, status.Error(codes.DataLoss, "local ModelRuntime discovery result is invalid")
	}
	for _, identity := range result.GetIdentities() {
		found := false
		for _, trusted := range server.localIdentities {
			found = found || proto.Equal(identity, trusted)
		}
		if !found {
			return nil, status.Error(codes.FailedPrecondition, "local ModelRuntime discovery changed its configured identity set")
		}
	}
	return &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesResponse{
		Result: proto.Clone(result).(*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse),
	}, nil
}

func validDiscoveryRequest(request *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest) bool {
	return request != nil && len(request.ProtoReflect().GetUnknown()) == 0 &&
		validDiscoveryUUID(request.GetWorkerInstanceId()) && request.GetWorkerInstanceEpoch() > 0 &&
		validDiscoveryUUID(request.GetWorkerMemberId()) && request.GetWorkerMemberEpoch() > 0
}

func validDiscoveryUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func validDiscoveryDigest(value []byte) bool {
	return len(value) == sha256.Size && [sha256.Size]byte(value) != ([sha256.Size]byte{})
}

func discoveryRequestFor(identity *velav1.ModelRuntimeIdentity) *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest {
	return &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: identity.GetWorkerInstanceId(), WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
		WorkerMemberId: identity.GetWorkerMemberId(), WorkerMemberEpoch: identity.GetWorkerMemberEpoch(),
	}
}

func validDiscoveryResult(request *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest, response *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse) bool {
	if !validDiscoveryRequest(request) || response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || proto.Size(response) > 64<<10 || len(response.GetIdentities()) == 0 || len(response.GetIdentities()) > 16 {
		return false
	}
	seen := make(map[string]bool, len(response.GetIdentities()))
	first := response.GetIdentities()[0]
	for _, identity := range response.GetIdentities() {
		if identity == nil || len(identity.ProtoReflect().GetUnknown()) != 0 || !proto.Equal(discoveryRequestFor(identity), request) ||
			!validDiscoveryUUID(identity.GetModelResidencyId()) || !validDiscoveryUUID(identity.GetStageProfileRevisionId()) ||
			identity.GetModelRuntimeEpoch() <= 0 || strings.TrimSpace(identity.GetRuntimeIdentity()) == "" || len(identity.GetRuntimeIdentity()) > 256 ||
			!validDiscoveryDigest(identity.GetDeviceSetDigest()) || !validDiscoveryDigest(identity.GetMembershipDigest()) ||
			!bytes.Equal(identity.GetDeviceSetDigest(), first.GetDeviceSetDigest()) || !bytes.Equal(identity.GetMembershipDigest(), first.GetMembershipDigest()) ||
			seen[identity.GetModelResidencyId()] {
			return false
		}
		seen[identity.GetModelResidencyId()] = true
	}
	return true
}
