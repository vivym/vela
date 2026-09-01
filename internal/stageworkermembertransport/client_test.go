package stageworkermembertransport

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientWrapsExactTargetAndDoesNotExposeDiscovery(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	recorder := &memberServiceRecorder{}
	velav1.RegisterStageWorkerMemberServiceServer(server, recorder)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	defer func() {
		server.Stop()
		_ = listener.Close()
		<-serveErrors
	}()
	target := "49360000-0000-0000-0000-000000000020"
	targetSPIFFE := "spiffe://vela.test/stage-worker/member-1"
	targetIdentityDigest := sha256.Sum256([]byte(targetSPIFFE))
	client, err := Dial(context.Background(), ClientConfig{
		Address: "bufconn.test:9444", TargetWorkerMemberID: target,
		TargetIdentityDigest: targetIdentityDigest[:],
		TransportCredentials: testTransportCredentialsForIdentity(t, targetSPIFFE),
		Dialer:               func(context.Context, string) (net.Conn, error) { return listener.Dial() },
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	authority := &velav1.StageAuthority{
		JobId: "job-marker",
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: target, IdentityDigest: targetIdentityDigest[:],
		}},
	}
	response, err := client.Status(context.Background(), &velav1.ModelRuntimeServiceStatusRequest{
		Authority: authority,
	})
	if err != nil || response.GetDetail() != "proxied" || recorder.target != target ||
		recorder.authority.GetJobId() != "job-marker" {
		t.Fatalf("Status = %#v recorder=%#v error=%v", response, recorder, err)
	}
	if _, err := client.DiscoverRuntimeIdentities(
		context.Background(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{},
	); err == nil {
		t.Fatal("remote runtime identity discovery was exposed")
	}
	mismatched := &velav1.StageAuthority{Members: []*velav1.StageAuthorityMemberEpoch{{
		WorkerMemberId: target, IdentityDigest: bytesOf('x'),
	}}}
	if _, err := client.Status(context.Background(), &velav1.ModelRuntimeServiceStatusRequest{
		Authority: mismatched,
	}); err == nil {
		t.Fatal("client sent authority for a different target member identity")
	}
}

func TestDialRejectsWrongMemberCertificate(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	velav1.RegisterStageWorkerMemberServiceServer(server, &memberServiceRecorder{})
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	defer func() {
		server.Stop()
		_ = listener.Close()
		<-serveErrors
	}()
	targetSPIFFE := "spiffe://vela.test/stage-worker/member-1"
	targetIdentityDigest := sha256.Sum256([]byte(targetSPIFFE))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	client, err := Dial(ctx, ClientConfig{
		Address: "bufconn.test:9444", TargetWorkerMemberID: "49360000-0000-0000-0000-000000000020",
		TargetIdentityDigest: targetIdentityDigest[:],
		TransportCredentials: testTransportCredentialsForIdentity(
			t, "spiffe://vela.test/stage-worker/member-2",
		),
		Dialer: func(context.Context, string) (net.Conn, error) { return listener.Dial() },
	})
	if client != nil || err == nil {
		t.Fatalf("Dial with wrong member certificate = %#v error=%v", client, err)
	}
}

type memberServiceRecorder struct {
	velav1.UnimplementedStageWorkerMemberServiceServer
	target    string
	authority *velav1.StageAuthority
}

type testTransportCredentials struct {
	authInfo credentials.AuthInfo
}

func testTransportCredentialsForIdentity(
	t *testing.T,
	identity string,
) credentials.TransportCredentials {
	t.Helper()
	spiffeID, err := url.Parse(identity)
	if err != nil {
		t.Fatalf("parse test SPIFFE identity: %v", err)
	}
	leaf := &x509.Certificate{Raw: []byte(identity), URIs: []*url.URL{spiffeID}}
	return &testTransportCredentials{authInfo: credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf}},
	}}}
}

func (transport *testTransportCredentials) ClientHandshake(
	_ context.Context,
	_ string,
	connection net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	return connection, transport.authInfo, nil
}

func (transport *testTransportCredentials) ServerHandshake(
	connection net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	return connection, transport.authInfo, nil
}

func (transport *testTransportCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (transport *testTransportCredentials) Clone() credentials.TransportCredentials {
	return &testTransportCredentials{authInfo: transport.authInfo}
}

func (*testTransportCredentials) OverrideServerName(string) error { return nil }

func (recorder *memberServiceRecorder) Status(
	_ context.Context,
	request *velav1.StageWorkerMemberServiceStatusRequest,
) (*velav1.StageWorkerMemberServiceStatusResponse, error) {
	recorder.target = request.GetTargetWorkerMemberId()
	recorder.authority = request.GetCommand().GetAuthority()
	return &velav1.StageWorkerMemberServiceStatusResponse{
		Result: &velav1.ModelRuntimeServiceStatusResponse{Detail: "proxied"},
	}, nil
}
