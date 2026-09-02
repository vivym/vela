package stageworkertransport_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkertransport"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

func TestServerTLSCredentialsRequireVerifiedStageWorkerCertificate(t *testing.T) {
	ca, caKey, caPEM := issueStageWorkerTestCA(t)
	serverPEM, serverKeyPEM := issueStageWorkerTestCertificate(
		t, ca, caKey, "stage-worker-control.internal",
		[]string{"stage-worker-control.internal"}, nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	spiffeID, err := url.Parse("spiffe://vela.internal/stage-worker/member-1")
	if err != nil {
		t.Fatalf("parse Stage Worker SPIFFE ID: %v", err)
	}
	clientPEM, clientKeyPEM := issueStageWorkerTestCertificate(
		t, ca, caKey, "member-1", nil, []*url.URL{spiffeID},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	serverCertPath := filepath.Join(directory, "server.crt")
	serverKeyPath := filepath.Join(directory, "server.key")
	clientCAPath := filepath.Join(directory, "client-ca.crt")
	clientCertPath := filepath.Join(directory, "client.crt")
	clientKeyPath := filepath.Join(directory, "client.key")
	serverCAPath := filepath.Join(directory, "server-ca.crt")
	for path, content := range map[string][]byte{
		serverCertPath: serverPEM,
		serverKeyPath:  serverKeyPEM,
		clientCAPath:   caPEM,
		clientCertPath: clientPEM,
		clientKeyPath:  clientKeyPEM,
		serverCAPath:   caPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Stage Worker TLS fixture %s: %v", path, err)
		}
	}
	serverCredentials, err := stageworkertransport.NewServerTLSCredentials(
		serverCertPath, serverKeyPath, clientCAPath,
	)
	if err != nil {
		t.Fatalf("NewServerTLSCredentials: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append Stage Worker test CA")
	}
	clientCredentials, err := stageworkertransport.NewClientTLSCredentials(
		clientCertPath, clientKeyPath, serverCAPath, "stage-worker-control.internal",
	)
	if err != nil {
		t.Fatalf("NewClientTLSCredentials: %v", err)
	}
	authInfo, clientErr, serverErr := stageWorkerTLSHandshake(
		t, serverCredentials, clientCredentials,
	)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("verified Stage Worker mTLS errors = client %v server %v", clientErr, serverErr)
	}
	tlsInfo, ok := authInfo.(credentials.TLSInfo)
	if !ok || tlsInfo.State.Version != tls.VersionTLS13 ||
		len(tlsInfo.State.VerifiedChains) == 0 ||
		len(tlsInfo.State.PeerCertificates) != 1 ||
		len(tlsInfo.State.PeerCertificates[0].URIs) != 1 ||
		tlsInfo.State.PeerCertificates[0].URIs[0].String() != spiffeID.String() {
		t.Fatalf("Stage Worker server mTLS auth info = %#v", authInfo)
	}

	unauthenticated := credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: "stage-worker-control.internal",
		RootCAs:    roots,
	})
	_, clientErr, serverErr = stageWorkerTLSHandshake(t, serverCredentials, unauthenticated)
	if clientErr == nil && serverErr == nil {
		t.Fatal("Stage Worker gRPC accepted a client without a certificate")
	}
}

func stageWorkerTLSHandshake(
	t *testing.T,
	serverCredentials credentials.TransportCredentials,
	clientCredentials credentials.TransportCredentials,
) (credentials.AuthInfo, error, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	defer func() { _ = listener.Close() }()
	type result struct {
		authInfo credentials.AuthInfo
		err      error
	}
	serverResult := make(chan result, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverResult <- result{err: acceptErr}
			return
		}
		secured, authInfo, handshakeErr := serverCredentials.ServerHandshake(connection)
		if secured != nil {
			_ = secured.Close()
		}
		serverResult <- result{authInfo: authInfo, err: handshakeErr}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := listener.DialContext(ctx)
	if err != nil {
		t.Fatalf("dial Stage Worker TLS listener: %v", err)
	}
	secured, _, clientErr := clientCredentials.ClientHandshake(
		ctx, "stage-worker-control.internal", connection,
	)
	if secured != nil {
		_ = secured.Close()
	}
	server := <-serverResult
	return server.authInfo, clientErr, server.err
}

func issueStageWorkerTestCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Stage Worker CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Vela Stage Worker Test CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatalf("issue Stage Worker test CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse Stage Worker test CA: %v", err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueStageWorkerTestCertificate(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	commonName string,
	dnsNames []string,
	identities []*url.URL,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Stage Worker certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate Stage Worker certificate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usage,
		DNSNames: dnsNames, URIs: identities,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		t.Fatalf("issue Stage Worker test certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal Stage Worker test key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}
