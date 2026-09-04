package stageworkertransport_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPeerAuthenticatorExtractsSingleVerifiedSPIFFEURI(t *testing.T) {
	leaf := &x509.Certificate{
		Raw:  []byte("verified-stage-worker-leaf"),
		URIs: []*url.URL{{Scheme: "spiffe", Host: "vela.internal", Path: "/stage-worker/member-1"}},
	}
	ctx := verifiedStageWorkerPeerContext(leaf)

	identity, err := (stageworkertransport.PeerAuthenticator{}).Authenticate(ctx)
	if err != nil {
		t.Fatalf("authenticate verified Stage Worker certificate: %v", err)
	}
	if identity.SPIFFEID != "spiffe://vela.internal/stage-worker/member-1" {
		t.Fatalf("Stage Worker SPIFFE identity = %q", identity.SPIFFEID)
	}
}

func TestPeerAuthenticatorRejectsUnverifiedOrAmbiguousIdentity(t *testing.T) {
	validURI := &url.URL{Scheme: "spiffe", Host: "vela.internal", Path: "/stage-worker/member-1"}
	for _, test := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing peer", ctx: context.Background()},
		{
			name: "unverified certificate",
			ctx: peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{
				State: tls.ConnectionState{
					HandshakeComplete: true,
					PeerCertificates:  []*x509.Certificate{{Raw: []byte("leaf"), URIs: []*url.URL{validURI}}},
				},
			}}),
		},
		{
			name: "multiple URI SANs",
			ctx: verifiedStageWorkerPeerContext(&x509.Certificate{
				Raw: []byte("leaf"),
				URIs: []*url.URL{
					validURI,
					{Scheme: "spiffe", Host: "vela.internal", Path: "/stage-worker/member-2"},
				},
			}),
		},
		{
			name: "non SPIFFE URI",
			ctx: verifiedStageWorkerPeerContext(&x509.Certificate{
				Raw:  []byte("leaf"),
				URIs: []*url.URL{{Scheme: "https", Host: "vela.internal", Path: "/stage-worker/member-1"}},
			}),
		},
		{
			name: "SPIFFE URI with query",
			ctx: verifiedStageWorkerPeerContext(&x509.Certificate{
				Raw: []byte("leaf"),
				URIs: []*url.URL{{
					Scheme: "spiffe", Host: "vela.internal", Path: "/stage-worker/member-1",
					RawQuery: "role=admin",
				}},
			}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := (stageworkertransport.PeerAuthenticator{}).Authenticate(test.ctx); err == nil {
				t.Fatal("invalid Stage Worker peer identity was accepted")
			}
		})
	}
}

func verifiedStageWorkerPeerContext(leaf *x509.Certificate) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{
			HandshakeComplete: true,
			PeerCertificates:  []*x509.Certificate{leaf},
			VerifiedChains:    [][]*x509.Certificate{{leaf}},
		},
	}})
}

func TestClientUsesOnePersistentCorrelatedStageWorkerStream(t *testing.T) {
	handler := &recordingStageHandler{}
	server, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
		Authenticator: stageworkertransport.AuthenticatorFunc(func(context.Context) (stageworkertransport.Identity, error) {
			return stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}, nil
		}),
		Handler:    handler,
		StopSource: noStops,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	counted := &countingStageServer{Server: server}
	address := serveStageControl(t, counted)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		InitialControlSessionEpoch: 41,
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	requests := []*velav1.StageWorkerControlServiceConnectRequest{
		{Operation: &velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence{
			RegisterWorkerEvidence: &velav1.RegisterWorkerEvidenceRequest{},
		}},
		{Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
			AcquireStage: &velav1.AcquireStageRequest{
				WorkerInstanceId:    "23000000-0000-0000-0000-000000000001",
				WorkerInstanceEpoch: 5, CapacityObservationSequence: 7,
			},
		}},
		{Operation: &velav1.StageWorkerControlServiceConnectRequest_StartStage{
			StartStage: &velav1.StartStageRequest{Authority: &velav1.StageAuthority{}},
		}},
	}
	for _, request := range requests {
		response, exchangeErr := client.Exchange(ctx, request)
		if exchangeErr != nil || response.GetRequestId() == "" || response.GetResult() == nil {
			t.Fatalf("Exchange = %#v error=%v", response, exchangeErr)
		}
	}
	if counted.ConnectCount() != 1 {
		t.Fatalf("Connect calls = %d, want one persistent stream", counted.ConnectCount())
	}
	if handler.RequestCount() != len(requests) || handler.Epochs() != "41,41,41" {
		t.Fatalf("handler requests=%d epochs=%s", handler.RequestCount(), handler.Epochs())
	}
}

func TestClientPreservesCanonicalCallerRequestIDForDurableReplay(t *testing.T) {
	handler := &recordingStageHandler{}
	server, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
		Authenticator: stageworkertransport.AuthenticatorFunc(func(context.Context) (stageworkertransport.Identity, error) {
			return stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}, nil
		}),
		Handler: handler, StopSource: noStops,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	address := serveStageControl(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		InitialControlSessionEpoch: 42,
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	requestID := "24000000-0000-0000-0000-000000000001"
	response, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: requestID,
		Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
			AcquireStage: &velav1.AcquireStageRequest{},
		},
	})
	if err != nil || response.GetRequestId() != requestID || handler.LastRequestID() != requestID {
		t.Fatalf("Exchange response=%#v handler_request_id=%q error=%v", response, handler.LastRequestID(), err)
	}
	if _, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: "not-a-canonical-uuid",
		Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
			AcquireStage: &velav1.AcquireStageRequest{},
		},
	}); err == nil {
		t.Fatal("Exchange accepted a non-canonical caller request ID")
	}
}

func TestClientReconnectIncrementsSessionEpochAndCarriesReattach(t *testing.T) {
	server := &reconnectingStageServer{}
	address := serveStageControl(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		InitialControlSessionEpoch: 50,
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
			AcquireStage: &velav1.AcquireStageRequest{},
		},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("first Exchange error = %v, want Unavailable", err)
	}
	response, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
			ReattachStage: &velav1.ReattachStageRequest{Authority: &velav1.StageAuthority{}},
		},
	})
	if err != nil || response.GetStageCommandResult().GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE {
		t.Fatalf("reattach Exchange = %#v error=%v", response, err)
	}
	if got := server.Epochs(); got != "50,51" {
		t.Fatalf("control session epochs = %s, want 50,51", got)
	}
}

func TestClientObtainsEveryStreamEpochFromDurableSource(t *testing.T) {
	server := &reconnectingStageServer{}
	address := serveStageControl(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	epochs := &sequenceEpochSource{values: []int64{80, 94}}
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		ControlSessionEpochSource: epochs,
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
			AcquireStage: &velav1.AcquireStageRequest{},
		},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("first Exchange error = %v, want Unavailable", err)
	}
	if _, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
			ReattachStage: &velav1.ReattachStageRequest{Authority: &velav1.StageAuthority{}},
		},
	}); err != nil {
		t.Fatalf("reattach Exchange: %v", err)
	}
	if got := server.Epochs(); got != "80,94" {
		t.Fatalf("control session epochs = %s, want durable values 80,94", got)
	}
}

func TestClientReconcilesDurableControlSessionEpochAndReconnects(t *testing.T) {
	server := &synchronizingStageServer{durableEpoch: 95}
	address := serveStageControl(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	epochs := &reconcilingEpochSource{current: 1}
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		ControlSessionEpochSource: epochs,
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence{
			RegisterWorkerEvidence: &velav1.RegisterWorkerEvidenceRequest{},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "synchronized") {
		t.Fatalf("synchronization Exchange error = %v", err)
	}
	response, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence{
			RegisterWorkerEvidence: &velav1.RegisterWorkerEvidenceRequest{},
		},
	})
	if err != nil || !response.GetWorkerReadinessDecision().GetReady() {
		t.Fatalf("post-synchronization Exchange = %#v error=%v", response, err)
	}
	if got := server.Epochs(); got != "2,96" || epochs.observed != 95 {
		t.Fatalf("synchronized epochs = %s observed=%d, want 2,96 observed=95", got, epochs.observed)
	}
}

type sequenceEpochSource struct {
	values []int64
	index  int
}

type reconcilingEpochSource struct {
	current  int64
	observed int64
}

func (source *reconcilingEpochSource) NextControlSessionEpoch(context.Context) (int64, error) {
	source.current++
	return source.current, nil
}

func (source *reconcilingEpochSource) ObserveControlSessionEpoch(
	_ context.Context,
	epoch int64,
) error {
	source.current = epoch
	source.observed = epoch
	return nil
}

func (source *sequenceEpochSource) NextControlSessionEpoch(context.Context) (int64, error) {
	if source.index >= len(source.values) {
		return 0, errors.New("test control session epochs exhausted")
	}
	value := source.values[source.index]
	source.index++
	return value, nil
}

func TestServerPushesStopStageWithoutWaitingForAnotherWorkerRequest(t *testing.T) {
	stops := make(chan *velav1.StopStage, 1)
	server, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
		Authenticator: stageworkertransport.AuthenticatorFunc(func(context.Context) (stageworkertransport.Identity, error) {
			return stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}, nil
		}),
		Handler: &recordingStageHandler{},
		StopSource: stageworkertransport.StopSourceFunc(func(
			context.Context,
			stageworkertransport.Identity,
			int64,
		) <-chan *velav1.StopStage {
			return stops
		}),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	address := serveStageControl(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		InitialControlSessionEpoch: 60,
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
			AcquireStage: &velav1.AcquireStageRequest{},
		},
	}); err != nil {
		t.Fatalf("initial Exchange: %v", err)
	}
	stops <- &velav1.StopStage{
		Authority: &velav1.StageAuthority{},
		Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED,
		IssuedAt:  timestamppb.Now(),
	}
	select {
	case command := <-client.Commands():
		if command.GetRequestId() != "" || command.GetStopStage() == nil {
			t.Fatalf("unsolicited command = %#v", command)
		}
	case <-ctx.Done():
		t.Fatal("StopStage was not pushed without another Worker request")
	}
}

func TestServerTracksOnlyAcceptedStageAuthorities(t *testing.T) {
	assignmentAuthority := &velav1.StageAuthority{StageLeaseId: "assignment-authority"}
	renewedAuthority := &velav1.StageAuthority{StageLeaseId: "renewed-authority"}
	reattachedAuthority := &velav1.StageAuthority{StageLeaseId: "reattached-authority"}
	rejectedAuthority := &velav1.StageAuthority{StageLeaseId: "rejected-authority"}
	stopSource := &recordingAuthorityStopSource{}
	server, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
		Authenticator: stageworkertransport.AuthenticatorFunc(func(context.Context) (stageworkertransport.Identity, error) {
			return stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}, nil
		}),
		Handler: &authoritySequenceHandler{
			assignment: assignmentAuthority,
			renewed:    renewedAuthority,
		},
		StopSource: stopSource,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	address := serveStageControl(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		InitialControlSessionEpoch: 71,
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	requests := []*velav1.StageWorkerControlServiceConnectRequest{
		{Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
			AcquireStage: &velav1.AcquireStageRequest{},
		}},
		{Operation: &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
			HeartbeatStage: &velav1.HeartbeatStageRequest{Authority: assignmentAuthority},
		}},
		{Operation: &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
			ReattachStage: &velav1.ReattachStageRequest{Authority: reattachedAuthority},
		}},
		{Operation: &velav1.StageWorkerControlServiceConnectRequest_StartStage{
			StartStage: &velav1.StartStageRequest{Authority: rejectedAuthority},
		}},
	}
	for _, request := range requests {
		if _, err := client.Exchange(ctx, request); err != nil {
			t.Fatalf("exchange authority command: %v", err)
		}
	}
	if got := stopSource.ObservedLeaseIDs(); got !=
		"assignment-authority,renewed-authority,reattached-authority" {
		t.Fatalf("observed StageAuthority leases = %q", got)
	}
}

type authoritySequenceHandler struct {
	assignment *velav1.StageAuthority
	renewed    *velav1.StageAuthority
}

func (handler *authoritySequenceHandler) Handle(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	response := &velav1.StageWorkerControlServiceConnectResponse{RequestId: request.GetRequestId()}
	switch {
	case request.GetAcquireStage() != nil:
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_StageAssignment{
			StageAssignment: &velav1.StageAssignment{Authority: handler.assignment},
		}
	case request.GetHeartbeatStage() != nil:
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision:         velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
				Operation:        velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE,
				RenewedAuthority: handler.renewed,
			},
		}
	case request.GetReattachStage() != nil:
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision:  velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED,
				Operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE,
			},
		}
	default:
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision:  velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED,
				Operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE,
			},
		}
	}
	return response, nil
}

type recordingAuthorityStopSource struct {
	mu          sync.Mutex
	authorities []*velav1.StageAuthority
}

func (source *recordingAuthorityStopSource) Stops(
	context.Context,
	stageworkertransport.Identity,
	int64,
) <-chan *velav1.StopStage {
	return nil
}

func (source *recordingAuthorityStopSource) ObserveStageAuthority(
	_ stageworkertransport.Identity,
	_ int64,
	authority *velav1.StageAuthority,
) error {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.authorities = append(source.authorities, authority)
	return nil
}

func (source *recordingAuthorityStopSource) ObservedLeaseIDs() string {
	source.mu.Lock()
	defer source.mu.Unlock()
	result := ""
	for index, authority := range source.authorities {
		if index != 0 {
			result += ","
		}
		result += authority.GetStageLeaseId()
	}
	return result
}

type recordingStageHandler struct {
	mu         sync.Mutex
	requests   int
	epochs     []int64
	requestIDs []string
}

func (handler *recordingStageHandler) Handle(
	_ context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if identity.SPIFFEID == "" {
		return nil, status.Error(codes.Unauthenticated, "missing WorkerMember identity")
	}
	handler.mu.Lock()
	handler.requests++
	handler.epochs = append(handler.epochs, sessionEpoch)
	handler.requestIDs = append(handler.requestIDs, request.GetRequestId())
	handler.mu.Unlock()
	response := &velav1.StageWorkerControlServiceConnectResponse{RequestId: request.GetRequestId()}
	switch request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{
			WorkerReadinessDecision: &velav1.WorkerReadinessDecision{Ready: true},
		}
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_NoWork{
			NoWork: &velav1.NoStageWork{},
		}
	default:
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision:  velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
				Operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE,
			},
		}
	}
	return response, nil
}

func (handler *recordingStageHandler) LastRequestID() string {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if len(handler.requestIDs) == 0 {
		return ""
	}
	return handler.requestIDs[len(handler.requestIDs)-1]
}

func (handler *recordingStageHandler) RequestCount() int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return handler.requests
}

func (handler *recordingStageHandler) Epochs() string {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	result := ""
	for index, epoch := range handler.epochs {
		if index > 0 {
			result += ","
		}
		result += string(rune('0'+epoch/10)) + string(rune('0'+epoch%10))
	}
	return result
}

type countingStageServer struct {
	*stageworkertransport.Server
	mu       sync.Mutex
	connects int
}

func (server *countingStageServer) Connect(
	stream grpc.BidiStreamingServer[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
) error {
	server.mu.Lock()
	server.connects++
	server.mu.Unlock()
	return server.Server.Connect(stream)
}

func (server *countingStageServer) ConnectCount() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.connects
}

type reconnectingStageServer struct {
	velav1.UnimplementedStageWorkerControlServiceServer
	mu       sync.Mutex
	connects int
	epochs   []int64
}

type synchronizingStageServer struct {
	velav1.UnimplementedStageWorkerControlServiceServer
	mu           sync.Mutex
	epochs       []int64
	durableEpoch int64
}

func (server *synchronizingStageServer) Connect(
	stream grpc.BidiStreamingServer[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	server.mu.Lock()
	server.epochs = append(server.epochs, request.GetControlSessionEpoch())
	connection := len(server.epochs)
	server.mu.Unlock()
	acceptedEpoch := request.GetControlSessionEpoch()
	if connection == 1 {
		acceptedEpoch = server.durableEpoch
	}
	return stream.Send(&velav1.StageWorkerControlServiceConnectResponse{
		RequestId: request.GetRequestId(),
		Result: &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{
			WorkerReadinessDecision: &velav1.WorkerReadinessDecision{
				Ready: true, ControlSessionEpoch: acceptedEpoch,
			},
		},
	})
}

func (server *synchronizingStageServer) Epochs() string {
	server.mu.Lock()
	defer server.mu.Unlock()
	return fmt.Sprintf("%d,%d", server.epochs[0], server.epochs[1])
}

func (server *reconnectingStageServer) Connect(
	stream grpc.BidiStreamingServer[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	server.mu.Lock()
	server.connects++
	connection := server.connects
	server.epochs = append(server.epochs, request.GetControlSessionEpoch())
	server.mu.Unlock()
	if connection == 1 {
		return status.Error(codes.Unavailable, "connection lost")
	}
	if request.GetReattachStage() == nil {
		return status.Error(codes.InvalidArgument, "ReattachStage required after reconnect")
	}
	return stream.Send(&velav1.StageWorkerControlServiceConnectResponse{
		RequestId: request.GetRequestId(),
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision:  velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
				Operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE,
			},
		},
	})
}

var noStops = stageworkertransport.StopSourceFunc(func(
	context.Context,
	stageworkertransport.Identity,
	int64,
) <-chan *velav1.StopStage {
	return nil
})

func (server *reconnectingStageServer) Epochs() string {
	server.mu.Lock()
	defer server.mu.Unlock()
	result := ""
	for index, epoch := range server.epochs {
		if index > 0 {
			result += ","
		}
		result += string(rune('0'+epoch/10)) + string(rune('0'+epoch%10))
	}
	return result
}

func serveStageControl(
	t *testing.T,
	server velav1.StageWorkerControlServiceServer,
) string {
	t.Helper()
	grpcServer := grpc.NewServer()
	velav1.RegisterStageWorkerControlServiceServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-done; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve StageWorker control: %v", serveErr)
		}
	})
	return listener.Addr().String()
}
