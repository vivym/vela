package fleettransport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
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

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

func TestServerTLSCredentialsRequireVerifiedFleetControllerCertificate(t *testing.T) {
	caCertificate, caKey, caPEM := issueFleetTestCA(t)
	serverCertificate, serverKey := issueFleetTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "fleet-maintenance.internal"},
		[]string{"fleet-maintenance.internal"},
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	spiffeID, err := url.Parse("spiffe://vela.internal/fleet-controller/primary")
	if err != nil {
		t.Fatalf("parse Fleet Controller SPIFFE ID: %v", err)
	}
	clientCertificate, clientKey := issueFleetTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "vela-fleet-controller"},
		nil,
		[]*url.URL{spiffeID},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)

	directory := t.TempDir()
	serverCertificatePath := filepath.Join(directory, "server.crt")
	serverKeyPath := filepath.Join(directory, "server.key")
	clientCAPath := filepath.Join(directory, "client-ca.crt")
	clientCertificatePath := filepath.Join(directory, "client.crt")
	clientKeyPath := filepath.Join(directory, "client.key")
	serverCAPath := filepath.Join(directory, "server-ca.crt")
	for path, content := range map[string][]byte{
		serverCertificatePath: serverCertificate,
		serverKeyPath:         serverKey,
		clientCAPath:          caPEM,
		clientCertificatePath: clientCertificate,
		clientKeyPath:         clientKey,
		serverCAPath:          caPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Fleet TLS fixture %s: %v", path, err)
		}
	}

	serverCredentials, err := NewServerTLSCredentials(
		serverCertificatePath,
		serverKeyPath,
		clientCAPath,
	)
	if err != nil {
		t.Fatalf("create Fleet server TLS credentials: %v", err)
	}
	clientCredentials, err := NewClientTLSCredentials(
		clientCertificatePath,
		clientKeyPath,
		serverCAPath,
		"fleet-maintenance.internal",
	)
	if err != nil {
		t.Fatalf("create Fleet client TLS credentials: %v", err)
	}
	authInfo, clientErr, serverErr := performFleetTLSHandshake(
		t,
		serverCredentials,
		clientCredentials,
	)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("verified Fleet mTLS handshake errors = client %v server %v", clientErr, serverErr)
	}
	tlsInfo, ok := authInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 ||
		len(tlsInfo.State.PeerCertificates) != 1 ||
		len(tlsInfo.State.PeerCertificates[0].URIs) != 1 ||
		tlsInfo.State.PeerCertificates[0].URIs[0].String() != spiffeID.String() {
		t.Fatalf("Fleet server mTLS auth info = %#v", authInfo)
	}

	unauthenticatedClient := credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: "fleet-maintenance.internal",
		RootCAs:    mustFleetCertPool(t, caPEM),
		NextProtos: []string{"h2"},
	})
	_, clientErr, serverErr = performFleetTLSHandshake(
		t,
		serverCredentials,
		unauthenticatedClient,
	)
	if clientErr == nil && serverErr == nil {
		t.Fatal("Fleet maintenance TLS accepted a client without a certificate")
	}
}

func mustFleetCertPool(t *testing.T, caPEM []byte) *x509.CertPool {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append Fleet test root CA")
	}
	return roots
}

func performFleetTLSHandshake(
	t *testing.T,
	serverCredentials credentials.TransportCredentials,
	clientCredentials credentials.TransportCredentials,
) (credentials.AuthInfo, error, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	defer func() { _ = listener.Close() }()
	type handshakeResult struct {
		authInfo credentials.AuthInfo
		err      error
	}
	serverResult := make(chan handshakeResult, 1)
	go func() {
		serverConnection, err := listener.Accept()
		if err != nil {
			serverResult <- handshakeResult{err: err}
			return
		}
		connection, authInfo, err := serverCredentials.ServerHandshake(serverConnection)
		if connection != nil {
			_ = connection.Close()
		}
		serverResult <- handshakeResult{authInfo: authInfo, err: err}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientConnection, err := listener.DialContext(ctx)
	if err != nil {
		t.Fatalf("dial Fleet TLS listener: %v", err)
	}
	connection, _, clientErr := clientCredentials.ClientHandshake(
		ctx,
		"fleet-maintenance.internal",
		clientConnection,
	)
	if connection != nil {
		_ = connection.Close()
	}
	server := <-serverResult
	return server.authInfo, clientErr, server.err
}

func issueFleetTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate Fleet test CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Vela Fleet Test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("issue Fleet test CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse Fleet test CA: %v", err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueFleetTestCertificate(
	t *testing.T,
	issuer *x509.Certificate,
	issuerKey *rsa.PrivateKey,
	subject pkix.Name,
	dnsNames []string,
	uris []*url.URL,
	usages []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate Fleet test certificate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  usages,
		DNSNames:     dnsNames,
		URIs:         uris,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, issuer, &key.PublicKey, issuerKey)
	if err != nil {
		t.Fatalf("issue Fleet test certificate: %v", err)
	}
	keyDER := x509.MarshalPKCS1PrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER})
}
