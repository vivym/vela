package stageworkertransport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"strings"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Identity struct {
	SPIFFEID string
}

type Authenticator interface {
	Authenticate(context.Context) (Identity, error)
}

type AuthenticatorFunc func(context.Context) (Identity, error)

func (authenticate AuthenticatorFunc) Authenticate(ctx context.Context) (Identity, error) {
	return authenticate(ctx)
}

type PeerAuthenticator struct{}

func (PeerAuthenticator) Authenticate(ctx context.Context) (Identity, error) {
	spiffeID, ok := verifiedPeerSPIFFEID(ctx)
	if !ok {
		return Identity{}, errors.New("verified Stage Worker SPIFFE identity is required")
	}
	return Identity{SPIFFEID: spiffeID}, nil
}

func verifiedPeerSPIFFEID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	connectionPeer, ok := peer.FromContext(ctx)
	if !ok || connectionPeer.AuthInfo == nil {
		return "", false
	}
	var tlsInfo credentials.TLSInfo
	switch typed := connectionPeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = typed
	case *credentials.TLSInfo:
		if typed == nil {
			return "", false
		}
		tlsInfo = *typed
	default:
		return "", false
	}
	state := tlsInfo.State
	if !state.HandshakeComplete || len(state.PeerCertificates) == 0 ||
		len(state.VerifiedChains) == 0 {
		return "", false
	}
	leaf := state.PeerCertificates[0]
	verified := false
	for _, chain := range state.VerifiedChains {
		if len(chain) != 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verified = true
			break
		}
	}
	if !verified || len(leaf.URIs) != 1 || leaf.URIs[0] == nil {
		return "", false
	}
	identity := leaf.URIs[0].String()
	parsed, err := url.Parse(identity)
	if err != nil || parsed == nil || parsed.Scheme != "spiffe" || parsed.Host == "" ||
		parsed.User != nil || parsed.Path == "" || parsed.Opaque != "" || parsed.RawQuery != "" ||
		parsed.Fragment != "" || len(identity) > 500 || strings.TrimSpace(identity) != identity ||
		strings.ContainsRune(identity, '\x00') || parsed.String() != identity {
		return "", false
	}
	return identity, true
}

type Handler interface {
	Handle(
		context.Context,
		Identity,
		int64,
		*velav1.StageWorkerControlServiceConnectRequest,
	) (*velav1.StageWorkerControlServiceConnectResponse, error)
}

type StopSource interface {
	Stops(context.Context, Identity, int64) <-chan *velav1.StopStage
}

type AuthorityObserver interface {
	ObserveStageAuthority(Identity, int64, *velav1.StageAuthority) error
}

type StopSourceFunc func(context.Context, Identity, int64) <-chan *velav1.StopStage

func (source StopSourceFunc) Stops(
	ctx context.Context,
	identity Identity,
	sessionEpoch int64,
) <-chan *velav1.StopStage {
	return source(ctx, identity, sessionEpoch)
}

type ServerConfig struct {
	Authenticator Authenticator
	Handler       Handler
	StopSource    StopSource
}

type Server struct {
	velav1.UnimplementedStageWorkerControlServiceServer
	authenticator Authenticator
	handler       Handler
	stopSource    StopSource
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.Authenticator == nil {
		return nil, errors.New("missing Stage Worker control authenticator")
	}
	if config.Handler == nil {
		return nil, errors.New("missing Stage Worker control handler")
	}
	if config.StopSource == nil {
		return nil, errors.New("missing Stage Worker control StopStage source")
	}
	return &Server{
		authenticator: config.Authenticator,
		handler:       config.Handler,
		stopSource:    config.StopSource,
	}, nil
}

func (server *Server) Connect(
	stream grpc.BidiStreamingServer[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
) error {
	if server == nil || server.authenticator == nil || server.handler == nil ||
		server.stopSource == nil {
		return status.Error(codes.FailedPrecondition, "Stage Worker control server is not configured")
	}
	identity, err := server.authenticator.Authenticate(stream.Context())
	if err != nil {
		return status.Error(codes.Unauthenticated, "authenticate Stage Worker stream")
	}
	if identity.SPIFFEID == "" {
		return status.Error(codes.Unauthenticated, "Stage Worker identity is missing")
	}
	var sessionEpoch int64
	var stopCommands <-chan *velav1.StopStage
	seen := make(map[string]struct{})
	requests := receiveRequests(stream)
	for {
		select {
		case received := <-requests:
			if errors.Is(received.err, io.EOF) {
				return nil
			}
			if received.err != nil {
				return received.err
			}
			request := received.request
			if request == nil || request.GetOperation() == nil || request.GetControlSessionEpoch() <= 0 {
				return status.Error(codes.InvalidArgument, "Stage Worker request is incomplete")
			}
			if _, parseErr := uuid.Parse(request.GetRequestId()); parseErr != nil {
				return status.Error(codes.InvalidArgument, "Stage Worker request identity is invalid")
			}
			if sessionEpoch == 0 {
				sessionEpoch = request.GetControlSessionEpoch()
				stopCommands = server.stopSource.Stops(stream.Context(), identity, sessionEpoch)
			} else if request.GetControlSessionEpoch() != sessionEpoch {
				return status.Error(codes.FailedPrecondition, "control session epoch changed within one stream")
			}
			if _, duplicated := seen[request.GetRequestId()]; duplicated {
				return status.Error(codes.AlreadyExists, "Stage Worker request identity is duplicated")
			}
			seen[request.GetRequestId()] = struct{}{}
			if len(seen) > 100_000 {
				return status.Error(codes.ResourceExhausted, "Stage Worker stream request bound exceeded")
			}
			response, handleErr := server.handler.Handle(
				stream.Context(), identity, sessionEpoch, request,
			)
			if handleErr != nil {
				return handleErr
			}
			if response == nil || response.GetRequestId() != request.GetRequestId() ||
				response.GetResult() == nil {
				return status.Error(codes.Internal, "Stage Worker handler returned malformed response")
			}
			if observer, ok := server.stopSource.(AuthorityObserver); ok {
				if authority := acceptedStageAuthority(request, response); authority != nil {
					if observeErr := observer.ObserveStageAuthority(
						identity, sessionEpoch, authority,
					); observeErr != nil {
						return status.Error(codes.Internal, "track Stage Worker execution authority")
					}
				}
			}
			if sendErr := stream.Send(response); sendErr != nil {
				return sendErr
			}
		case stop, open := <-stopCommands:
			if !open {
				stopCommands = nil
				continue
			}
			if !validStopCommand(stop) {
				return status.Error(codes.Internal, "Stage Worker StopStage source returned malformed command")
			}
			if sendErr := stream.Send(&velav1.StageWorkerControlServiceConnectResponse{
				Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{
					StopStage: stop,
				},
			}); sendErr != nil {
				return sendErr
			}
		}
	}
}

func acceptedStageAuthority(
	request *velav1.StageWorkerControlServiceConnectRequest,
	response *velav1.StageWorkerControlServiceConnectResponse,
) *velav1.StageAuthority {
	if assignment := response.GetStageAssignment(); assignment != nil {
		return assignment.GetAuthority()
	}
	result := response.GetStageCommandResult()
	if result == nil ||
		(result.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			result.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) {
		return nil
	}
	if result.GetRenewedAuthority() != nil {
		return result.GetRenewedAuthority()
	}
	if reattach := request.GetReattachStage(); reattach != nil {
		return reattach.GetAuthority()
	}
	return nil
}

type receivedRequest struct {
	request *velav1.StageWorkerControlServiceConnectRequest
	err     error
}

func receiveRequests(
	stream grpc.BidiStreamingServer[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
) <-chan receivedRequest {
	received := make(chan receivedRequest, 1)
	go func() {
		for {
			request, err := stream.Recv()
			select {
			case received <- receivedRequest{request: request, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received
}

func validStopCommand(stop *velav1.StopStage) bool {
	if stop == nil || stop.GetAuthority() == nil || stop.GetIssuedAt() == nil ||
		stop.GetReason() < velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED ||
		stop.GetReason() > velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_MEMBER_BARRIER_FAILED {
		return false
	}
	return stop.GetIssuedAt().CheckValid() == nil
}
