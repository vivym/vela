package nodeagent

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

func TestNodeAgentTLSBindsServerCertificateToNodeAndAgentIdentity(t *testing.T) {
	caCertificate, caKey, caPEM := issueNodeAgentTestCA(t)
	identity := NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 1}
	nodeSPIFFE, err := url.Parse(NodeAgentSPIFFEIdentity(identity))
	if err != nil {
		t.Fatalf("parse Node Agent SPIFFE ID: %v", err)
	}
	serverCertificate, serverKey := issueNodeAgentTestCertificate(
		t, caCertificate, caKey, "node-agent.internal", []string{"node-agent.internal"},
		[]*url.URL{nodeSPIFFE}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	controllerSPIFFE, err := url.Parse("spiffe://vela.internal/controller/control-1")
	if err != nil {
		t.Fatalf("parse controller SPIFFE ID: %v", err)
	}
	clientCertificate, clientKey := issueNodeAgentTestCertificate(
		t, caCertificate, caKey, "control-1", nil, []*url.URL{controllerSPIFFE},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	paths := map[string][]byte{
		"server.crt": serverCertificate, "server.key": serverKey,
		"client.crt": clientCertificate, "client.key": clientKey, "ca.crt": caPEM,
	}
	for name, content := range paths {
		if err := os.WriteFile(filepath.Join(directory, name), content, 0o600); err != nil {
			t.Fatalf("write TLS fixture %s: %v", name, err)
		}
	}
	serverCredentials, err := NewServerTLSCredentials(
		filepath.Join(directory, "server.crt"), filepath.Join(directory, "server.key"),
		filepath.Join(directory, "ca.crt"), identity,
	)
	if err != nil {
		t.Fatalf("NewServerTLSCredentials: %v", err)
	}
	clientCredentials, err := NewClientTLSCredentials(
		filepath.Join(directory, "client.crt"), filepath.Join(directory, "client.key"),
		filepath.Join(directory, "ca.crt"), "node-agent.internal", NodeAgentSPIFFEIdentity(identity),
		"controller/control-1",
	)
	if err != nil {
		t.Fatalf("NewClientTLSCredentials: %v", err)
	}
	if _, err := NewClientTLSCredentials(
		filepath.Join(directory, "client.crt"), filepath.Join(directory, "client.key"),
		filepath.Join(directory, "ca.crt"), "node-agent.internal", NodeAgentSPIFFEIdentity(identity),
		"controller/control-other",
	); err == nil {
		t.Fatal("controller certificate was accepted for a different actor identity")
	}
	clientErr, serverErr := performNodeAgentTLSHandshake(t, serverCredentials, clientCredentials)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("bound Node Agent TLS handshake errors = client %v server %v", clientErr, serverErr)
	}
	if err := os.Chmod(filepath.Join(directory, "client.key"), 0o644); err != nil {
		t.Fatalf("relax client key permissions: %v", err)
	}
	if _, err := NewClientTLSCredentials(
		filepath.Join(directory, "client.crt"), filepath.Join(directory, "client.key"),
		filepath.Join(directory, "ca.crt"), "node-agent.internal", NodeAgentSPIFFEIdentity(identity),
		"controller/control-1",
	); err == nil {
		t.Fatal("group/world-accessible controller private key was accepted")
	}
	if err := os.Chmod(filepath.Join(directory, "client.key"), 0o600); err != nil {
		t.Fatalf("restore client key permissions: %v", err)
	}

	wrongIdentity := NodeAgentIdentity{NodeIdentity: identity.NodeIdentity, AgentID: uuid.New(), AgentEpoch: 1}
	if _, err := NewServerTLSCredentials(
		filepath.Join(directory, "server.crt"), filepath.Join(directory, "server.key"),
		filepath.Join(directory, "ca.crt"), wrongIdentity,
	); err == nil {
		t.Fatal("server certificate was accepted for a different Agent identity")
	}
	wrongClientCredentials, err := NewClientTLSCredentials(
		filepath.Join(directory, "client.crt"), filepath.Join(directory, "client.key"),
		filepath.Join(directory, "ca.crt"), "node-agent.internal", NodeAgentSPIFFEIdentity(wrongIdentity),
		"controller/control-1",
	)
	if err != nil {
		t.Fatalf("configure wrong expected identity: %v", err)
	}
	clientErr, serverErr = performNodeAgentTLSHandshake(t, serverCredentials, wrongClientCredentials)
	if clientErr == nil && serverErr == nil {
		t.Fatal("client accepted a Node Agent certificate for a different Agent identity")
	}
}

func performNodeAgentTLSHandshake(t *testing.T, serverCredentials, clientCredentials credentials.TransportCredentials) (error, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	defer func() { _ = listener.Close() }()
	serverResult := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverResult <- err
			return
		}
		secured, _, err := serverCredentials.ServerHandshake(connection)
		if secured != nil {
			_ = secured.Close()
		}
		serverResult <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := listener.DialContext(ctx)
	if err != nil {
		t.Fatalf("dial Node Agent TLS fixture: %v", err)
	}
	secured, _, clientErr := clientCredentials.ClientHandshake(ctx, "node-agent.internal", connection)
	if secured != nil {
		_ = secured.Close()
	}
	return clientErr, <-serverResult
}

func issueNodeAgentTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate Node Agent test CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Vela Node Agent Test CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("issue Node Agent test CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse Node Agent test CA: %v", err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueNodeAgentTestCertificate(
	t *testing.T,
	caCertificate *x509.Certificate,
	caKey *rsa.PrivateKey,
	commonName string,
	dnsNames []string,
	identities []*url.URL,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate Node Agent test certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate Node Agent test certificate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usage, DNSNames: dnsNames, URIs: identities,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCertificate, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("issue Node Agent test certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
