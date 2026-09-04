package stageworkermembertransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type MemberBinding struct {
	ID    string
	Epoch int64
}

type ServerConfig struct {
	Authenticator   stageworkertransport.Authenticator
	Validator       *stageauthority.Validator
	Runtime         velav1.ModelRuntimeServiceClient
	LocalIdentities []*velav1.ModelRuntimeIdentity
	Members         []MemberBinding
	MaxClockSkew    time.Duration
}

type Server struct {
	velav1.UnimplementedStageWorkerMemberServiceServer
	authenticator   stageworkertransport.Authenticator
	validator       *stageauthority.Validator
	runtime         velav1.ModelRuntimeServiceClient
	localMember     MemberBinding
	localIdentities []*velav1.ModelRuntimeIdentity
	membersByID     map[string]MemberBinding
	maxClockSkew    time.Duration
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.Authenticator == nil || config.Validator == nil || config.Runtime == nil ||
		len(config.LocalIdentities) == 0 || len(config.LocalIdentities) > 16 ||
		len(config.Members) < 2 || len(config.Members) > 64 {
		return nil, errors.New("stage worker member server configuration is incomplete")
	}
	if config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, errors.New("stage worker member server clock skew is invalid")
	}
	membersByID := make(map[string]MemberBinding, len(config.Members))
	for _, member := range config.Members {
		if uuid.Validate(member.ID) != nil || member.Epoch <= 0 {
			return nil, errors.New("stage worker member binding is invalid")
		}
		if _, duplicate := membersByID[member.ID]; duplicate {
			return nil, errors.New("stage worker member identity is duplicated")
		}
		membersByID[member.ID] = member
	}
	identities := make([]*velav1.ModelRuntimeIdentity, 0, len(config.LocalIdentities))
	var local MemberBinding
	for _, identity := range config.LocalIdentities {
		member, ok := membersByID[identity.GetWorkerMemberId()]
		if !ok || identity.GetWorkerMemberEpoch() != member.Epoch ||
			identity.GetWorkerInstanceId() == "" || identity.GetWorkerInstanceEpoch() <= 0 ||
			identity.GetModelRuntimeEpoch() <= 0 || len(identity.GetDeviceSetDigest()) != 32 ||
			len(identity.GetMembershipDigest()) != 32 {
			return nil, errors.New("stage worker local runtime identity is invalid")
		}
		if local.ID == "" {
			local = member
		} else if local.ID != member.ID {
			return nil, errors.New("stage worker member server spans multiple local members")
		}
		identities = append(identities, proto.Clone(identity).(*velav1.ModelRuntimeIdentity))
	}
	return &Server{
		authenticator: config.Authenticator, validator: config.Validator, runtime: config.Runtime,
		localMember: local, localIdentities: identities,
		membersByID: membersByID, maxClockSkew: config.MaxClockSkew,
	}, nil
}

func (server *Server) PrepareStage(
	ctx context.Context,
	request *velav1.StageWorkerMemberServicePrepareStageRequest,
) (*velav1.StageWorkerMemberServicePrepareStageResponse, error) {
	if request == nil || request.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member prepare request is incomplete")
	}
	authority := request.GetCommand().GetAuthority()
	identity, digest, err := server.authorize(ctx, request.GetTargetWorkerMemberId(), authority)
	if err != nil {
		return nil, err
	}
	specDigest, err := stageauthority.ExecutionSpecDigest(request.GetCommand().GetExecutionSpec())
	if err != nil || !bytes.Equal(specDigest[:], authority.GetExecutionSpecDigest()) {
		return nil, status.Error(codes.FailedPrecondition, "Stage Worker member execution spec is stale")
	}
	result, err := server.runtime.PrepareStage(
		ctx, proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServicePrepareStageRequest),
	)
	if err != nil {
		return nil, err
	}
	if !validRuntimeResponse(result, digest, identity) {
		return nil, status.Error(codes.Internal, "local ModelRuntime returned malformed prepare result")
	}
	return &velav1.StageWorkerMemberServicePrepareStageResponse{Result: result}, nil
}

func (server *Server) StartStage(
	ctx context.Context,
	request *velav1.StageWorkerMemberServiceStartStageRequest,
) (*velav1.StageWorkerMemberServiceStartStageResponse, error) {
	if request == nil || request.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member start request is incomplete")
	}
	identity, digest, err := server.authorize(
		ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetAuthority(),
	)
	if err != nil {
		return nil, err
	}
	result, err := server.runtime.StartStage(
		ctx, proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServiceStartStageRequest),
	)
	if err != nil {
		return nil, err
	}
	if !validRuntimeResponse(result, digest, identity) {
		return nil, status.Error(codes.Internal, "local ModelRuntime returned malformed start result")
	}
	return &velav1.StageWorkerMemberServiceStartStageResponse{Result: result}, nil
}

func (server *Server) CancelStage(
	ctx context.Context,
	request *velav1.StageWorkerMemberServiceCancelStageRequest,
) (*velav1.StageWorkerMemberServiceCancelStageResponse, error) {
	if request == nil || request.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member cancel request is incomplete")
	}
	identity, digest, err := server.authorize(
		ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetAuthority(),
	)
	if err != nil {
		return nil, err
	}
	result, err := server.runtime.CancelStage(
		ctx, proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServiceCancelStageRequest),
	)
	if err != nil {
		return nil, err
	}
	if !validRuntimeResponse(result, digest, identity) {
		return nil, status.Error(codes.Internal, "local ModelRuntime returned malformed cancel result")
	}
	return &velav1.StageWorkerMemberServiceCancelStageResponse{Result: result}, nil
}

func (server *Server) Status(
	ctx context.Context,
	request *velav1.StageWorkerMemberServiceStatusRequest,
) (*velav1.StageWorkerMemberServiceStatusResponse, error) {
	if request == nil || request.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "Stage Worker member status request is incomplete")
	}
	identity, digest, err := server.authorize(
		ctx, request.GetTargetWorkerMemberId(), request.GetCommand().GetAuthority(),
	)
	if err != nil {
		return nil, err
	}
	result, err := server.runtime.Status(
		ctx, proto.Clone(request.GetCommand()).(*velav1.ModelRuntimeServiceStatusRequest),
	)
	if err != nil {
		return nil, err
	}
	if !validRuntimeResponse(result, digest, identity) {
		return nil, status.Error(codes.Internal, "local ModelRuntime returned malformed status result")
	}
	return &velav1.StageWorkerMemberServiceStatusResponse{Result: result}, nil
}

func (server *Server) authorize(
	ctx context.Context,
	targetMemberID string,
	authority *velav1.StageAuthority,
) (*velav1.ModelRuntimeIdentity, [32]byte, error) {
	if server == nil || server.authenticator == nil || server.validator == nil || server.runtime == nil {
		return nil, [32]byte{}, status.Error(codes.FailedPrecondition, "Stage Worker member server is not configured")
	}
	if targetMemberID != server.localMember.ID {
		return nil, [32]byte{}, status.Error(codes.InvalidArgument, "Stage Worker member target does not match local member")
	}
	peer, err := server.authenticator.Authenticate(ctx)
	if err != nil {
		return nil, [32]byte{}, status.Error(codes.Unauthenticated, "authenticate Stage Worker member peer")
	}
	verified, err := server.validator.ValidateEnvelopeWithClockSkew(
		authority,
		server.maxClockSkew,
	)
	if err != nil {
		return nil, [32]byte{}, status.Error(codes.FailedPrecondition, "Stage Worker member authority is invalid or stale")
	}
	if len(verified.Authority.GetMembers()) != len(server.membersByID) {
		return nil, [32]byte{}, status.Error(codes.FailedPrecondition, "Stage Worker member authority membership is incomplete")
	}
	leaderID := ""
	var leaderIdentityDigest []byte
	for _, member := range verified.Authority.GetMembers() {
		configured, exists := server.membersByID[member.GetWorkerMemberId()]
		if !exists || member.GetMemberEpoch() != configured.Epoch || member.GetModelRuntimeEpoch() <= 0 {
			return nil, [32]byte{}, status.Error(codes.FailedPrecondition, "Stage Worker member authority membership is stale")
		}
		if leaderID == "" || member.GetWorkerMemberId() < leaderID {
			leaderID = member.GetWorkerMemberId()
			leaderIdentityDigest = member.GetIdentityDigest()
		}
	}
	peerDigest := sha256.Sum256([]byte(peer.SPIFFEID))
	if len(leaderIdentityDigest) != sha256.Size ||
		!bytes.Equal(peerDigest[:], leaderIdentityDigest) {
		return nil, [32]byte{}, status.Error(codes.PermissionDenied, "only the deterministic WorkerMember leader may invoke remote members")
	}
	for _, identity := range server.localIdentities {
		if runtimeIdentityMatchesAuthority(identity, verified.Authority, server.localMember) {
			return proto.Clone(identity).(*velav1.ModelRuntimeIdentity), verified.Digest, nil
		}
	}
	return nil, [32]byte{}, status.Error(codes.FailedPrecondition, "Stage Worker member authority does not match local resident runtime")
}

type runtimeResponse interface {
	GetAuthorityDigest() []byte
	GetRuntimeIdentity() *velav1.ModelRuntimeIdentity
}

func validRuntimeResponse(
	response runtimeResponse,
	digest [32]byte,
	identity *velav1.ModelRuntimeIdentity,
) bool {
	return response != nil && bytes.Equal(response.GetAuthorityDigest(), digest[:]) &&
		proto.Equal(response.GetRuntimeIdentity(), identity)
}

func runtimeIdentityMatchesAuthority(
	identity *velav1.ModelRuntimeIdentity,
	authority *velav1.StageAuthority,
	local MemberBinding,
) bool {
	if identity == nil || authority == nil || identity.GetWorkerMemberId() != local.ID ||
		identity.GetWorkerMemberEpoch() != local.Epoch ||
		identity.GetWorkerInstanceId() != authority.GetWorkerInstanceId() ||
		identity.GetWorkerInstanceEpoch() != authority.GetWorkerInstanceEpoch() ||
		!bytes.Equal(identity.GetDeviceSetDigest(), authority.GetDeviceSetDigest()) ||
		!bytes.Equal(identity.GetMembershipDigest(), authority.GetMembershipDigest()) ||
		identity.GetModelResidencyId() != authority.GetModelResidencyId() ||
		identity.GetRuntimeIdentity() != authority.GetModelRuntimeIdentity() ||
		identity.GetStageProfileRevisionId() != authority.GetStageProfileRevisionId() {
		return false
	}
	for _, member := range authority.GetMembers() {
		if member.GetWorkerMemberId() == local.ID {
			return member.GetMemberEpoch() == local.Epoch &&
				member.GetModelRuntimeEpoch() == identity.GetModelRuntimeEpoch()
		}
	}
	return false
}
