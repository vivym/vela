package stageworkermembertransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Dialer func(context.Context, string) (net.Conn, error)

type ClientConfig struct {
	Address              string
	TargetWorkerMemberID string
	TargetIdentityDigest []byte
	TransportCredentials credentials.TransportCredentials
	Dialer               Dialer
}

type Client struct {
	connection           *grpc.ClientConn
	service              velav1.StageWorkerMemberServiceClient
	targetID             string
	targetIdentityDigest [sha256.Size]byte
}

func Dial(ctx context.Context, config ClientConfig) (*Client, error) {
	if ctx == nil || uuid.Validate(config.TargetWorkerMemberID) != nil ||
		len(config.TargetIdentityDigest) != sha256.Size ||
		config.TransportCredentials == nil {
		return nil, errors.New("stage worker member client configuration is incomplete")
	}
	host, port, err := net.SplitHostPort(config.Address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return nil, errors.New("stage worker member address must contain a host and port")
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(&identityPinnedCredentials{
			TransportCredentials: config.TransportCredentials,
			identityDigest:       [sha256.Size]byte(config.TargetIdentityDigest),
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20),
		),
	}
	if config.Dialer != nil {
		options = append(options, grpc.WithContextDialer(config.Dialer))
	}
	connection, err := grpc.NewClient("passthrough:///"+config.Address, options...)
	if err != nil {
		return nil, fmt.Errorf("configure Stage Worker member client: %w", err)
	}
	connection.Connect()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("stage worker member connection shut down during startup")
		}
		if !connection.WaitForStateChange(ctx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("connect to Stage Worker member: %w", ctx.Err())
		}
	}
	return &Client{
		connection:           connection,
		service:              velav1.NewStageWorkerMemberServiceClient(connection),
		targetID:             config.TargetWorkerMemberID,
		targetIdentityDigest: [sha256.Size]byte(config.TargetIdentityDigest),
	}, nil
}

func (client *Client) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func (client *Client) PrepareStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServicePrepareStageRequest,
	options ...grpc.CallOption,
) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	if client == nil || client.service == nil || request == nil ||
		!client.matchesTargetAuthority(request.GetAuthority()) {
		return nil, errors.New("stage worker member prepare client is not configured")
	}
	response, err := client.service.PrepareStage(ctx, &velav1.StageWorkerMemberServicePrepareStageRequest{
		TargetWorkerMemberId: client.targetID, Command: request,
	}, options...)
	if err != nil {
		return nil, err
	}
	if response.GetResult() == nil {
		return nil, status.Error(codes.DataLoss, "Stage Worker member prepare response is incomplete")
	}
	return response.GetResult(), nil
}

func (client *Client) StartStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStartStageRequest,
	options ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	if client == nil || client.service == nil || request == nil ||
		!client.matchesTargetAuthority(request.GetAuthority()) {
		return nil, errors.New("stage worker member start client is not configured")
	}
	response, err := client.service.StartStage(ctx, &velav1.StageWorkerMemberServiceStartStageRequest{
		TargetWorkerMemberId: client.targetID, Command: request,
	}, options...)
	if err != nil {
		return nil, err
	}
	if response.GetResult() == nil {
		return nil, status.Error(codes.DataLoss, "Stage Worker member start response is incomplete")
	}
	return response.GetResult(), nil
}

func (client *Client) CancelStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceCancelStageRequest,
	options ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceCancelStageResponse, error) {
	if client == nil || client.service == nil || request == nil ||
		!client.matchesTargetAuthority(request.GetAuthority()) {
		return nil, errors.New("stage worker member cancel client is not configured")
	}
	response, err := client.service.CancelStage(ctx, &velav1.StageWorkerMemberServiceCancelStageRequest{
		TargetWorkerMemberId: client.targetID, Command: request,
	}, options...)
	if err != nil {
		return nil, err
	}
	if response.GetResult() == nil {
		return nil, status.Error(codes.DataLoss, "Stage Worker member cancel response is incomplete")
	}
	return response.GetResult(), nil
}

func (client *Client) Status(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStatusRequest,
	options ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	if client == nil || client.service == nil || request == nil ||
		!client.matchesTargetAuthority(request.GetAuthority()) {
		return nil, errors.New("stage worker member status client is not configured")
	}
	response, err := client.service.Status(ctx, &velav1.StageWorkerMemberServiceStatusRequest{
		TargetWorkerMemberId: client.targetID, Command: request,
	}, options...)
	if err != nil {
		return nil, err
	}
	if response.GetResult() == nil {
		return nil, status.Error(codes.DataLoss, "Stage Worker member status response is incomplete")
	}
	return response.GetResult(), nil
}

func (*Client) DiscoverRuntimeIdentities(
	context.Context,
	*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest,
	...grpc.CallOption,
) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "remote ModelRuntime identity discovery is not exposed")
}

func (*Client) ProbeReadiness(
	context.Context,
	*velav1.ModelRuntimeServiceProbeReadinessRequest,
	...grpc.CallOption,
) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	return nil, status.Error(codes.Unimplemented, "remote ModelRuntime readiness is not exposed")
}

func (*Client) SealOutput(
	context.Context,
	*velav1.ModelRuntimeServiceSealOutputRequest,
	...grpc.CallOption,
) (*velav1.ModelRuntimeServiceSealOutputResponse, error) {
	return nil, status.Error(codes.Unimplemented, "remote ModelRuntime output sealing is not exposed")
}

func (client *Client) matchesTargetAuthority(authority *velav1.StageAuthority) bool {
	if client == nil || authority == nil {
		return false
	}
	matched := false
	for _, member := range authority.GetMembers() {
		if member.GetWorkerMemberId() != client.targetID {
			continue
		}
		if matched || !bytes.Equal(member.GetIdentityDigest(), client.targetIdentityDigest[:]) {
			return false
		}
		matched = true
	}
	return matched
}

type identityPinnedCredentials struct {
	credentials.TransportCredentials
	identityDigest [sha256.Size]byte
}

func (pinned *identityPinnedCredentials) ClientHandshake(
	ctx context.Context,
	authority string,
	rawConnection net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	connection, authInfo, err := pinned.TransportCredentials.ClientHandshake(
		ctx, authority, rawConnection,
	)
	if err != nil {
		return nil, nil, err
	}
	if !matchesPeerIdentityDigest(authInfo, pinned.identityDigest) {
		_ = connection.Close()
		return nil, nil, errors.New("stage worker member TLS peer identity does not match target")
	}
	return connection, authInfo, nil
}

func (pinned *identityPinnedCredentials) Clone() credentials.TransportCredentials {
	return &identityPinnedCredentials{
		TransportCredentials: pinned.TransportCredentials.Clone(),
		identityDigest:       pinned.identityDigest,
	}
}

func matchesPeerIdentityDigest(
	authInfo credentials.AuthInfo,
	want [sha256.Size]byte,
) bool {
	var state tls.ConnectionState
	switch typed := authInfo.(type) {
	case credentials.TLSInfo:
		state = typed.State
	case *credentials.TLSInfo:
		if typed == nil {
			return false
		}
		state = typed.State
	default:
		return false
	}
	if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return false
	}
	leaf := state.PeerCertificates[0]
	verifiedLeaf := false
	for _, chain := range state.VerifiedChains {
		if len(chain) > 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verifiedLeaf = true
			break
		}
	}
	if !verifiedLeaf || len(leaf.URIs) != 1 || !validSPIFFEURI(leaf.URIs[0]) {
		return false
	}
	digest := sha256.Sum256([]byte(leaf.URIs[0].String()))
	return bytes.Equal(digest[:], want[:])
}

func validSPIFFEURI(identity *url.URL) bool {
	return identity != nil && identity.Scheme == "spiffe" && identity.Host != "" &&
		identity.User == nil && strings.HasPrefix(identity.Path, "/") && identity.RawQuery == "" &&
		identity.Fragment == ""
}

var _ velav1.ModelRuntimeServiceClient = (*Client)(nil)
