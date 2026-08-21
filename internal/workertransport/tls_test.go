package workertransport

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

func TestServerTLSCredentialsRequireVerifiedSPIFFEClientCertificate(t *testing.T) {
	caCertificate, caKey, caPEM := issueTestCA(t)
	serverCertificate, serverKeyPEM := issueTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "worker-control.internal"},
		[]string{"worker-control.internal"},
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	spiffeID, err := url.Parse("spiffe://vela.internal/worker/h3-primary-01")
	if err != nil {
		t.Fatalf("parse SPIFFE ID: %v", err)
	}
	clientCertificate, clientKeyPEM := issueTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "h3-primary-01"},
		nil,
		[]*url.URL{spiffeID},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	serverCertPath := filepath.Join(directory, "server.crt")
	serverKeyPath := filepath.Join(directory, "server.key")
	clientCAPath := filepath.Join(directory, "client-ca.crt")
	for path, content := range map[string][]byte{
		serverCertPath: serverCertificate,
		serverKeyPath:  serverKeyPEM,
		clientCAPath:   caPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write TLS fixture %s: %v", path, err)
		}
	}
	serverCredentials, err := NewServerTLSCredentials(
		serverCertPath,
		serverKeyPath,
		clientCAPath,
	)
	if err != nil {
		t.Fatalf("NewServerTLSCredentials: %v", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		t.Fatal("append test root CA")
	}
	clientPair, err := tls.X509KeyPair(clientCertificate, clientKeyPEM)
	if err != nil {
		t.Fatalf("parse client certificate: %v", err)
	}
	clientCredentials := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		ServerName:   "worker-control.internal",
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{clientPair},
	})
	serverAuthInfo, clientErr, serverErr := performTLSHandshake(
		t,
		serverCredentials,
		clientCredentials,
	)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("verified mTLS handshake errors = client %v server %v", clientErr, serverErr)
	}
	tlsInfo, ok := serverAuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 ||
		len(tlsInfo.State.PeerCertificates) != 1 ||
		len(tlsInfo.State.PeerCertificates[0].URIs) != 1 ||
		tlsInfo.State.PeerCertificates[0].URIs[0].String() != spiffeID.String() {
		t.Fatalf("server mTLS auth info = %#v", serverAuthInfo)
	}

	unauthenticatedClient := credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: "worker-control.internal",
		RootCAs:    rootCAs,
	})
	_, clientErr, serverErr = performTLSHandshake(t, serverCredentials, unauthenticatedClient)
	if clientErr == nil && serverErr == nil {
		t.Fatal("Worker gRPC TLS accepted a client without a certificate")
	}
}

func performTLSHandshake(
	t *testing.T,
	serverCredentials credentials.TransportCredentials,
	clientCredentials credentials.TransportCredentials,
) (credentials.AuthInfo, error, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	defer func() { _ = listener.Close() }()
	serverResult := make(chan struct {
		authInfo credentials.AuthInfo
		err      error
	}, 1)
	go func() {
		serverConnection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverResult <- struct {
				authInfo credentials.AuthInfo
				err      error
			}{err: acceptErr}
			return
		}
		connection, authInfo, err := serverCredentials.ServerHandshake(serverConnection)
		if connection != nil {
			_ = connection.Close()
		}
		serverResult <- struct {
			authInfo credentials.AuthInfo
			err      error
		}{authInfo: authInfo, err: err}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientConnection, err := listener.DialContext(ctx)
	if err != nil {
		t.Fatalf("dial buffered TLS listener: %v", err)
	}
	connection, _, clientErr := clientCredentials.ClientHandshake(
		ctx,
		"worker-control.internal",
		clientConnection,
	)
	if connection != nil {
		_ = connection.Close()
	}
	server := <-serverResult
	return server.authInfo, clientErr, server.err
}

func issueTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Vela Worker Test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("issue test CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse test CA: %v", err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueTestCertificate(
	t *testing.T,
	caCertificate *x509.Certificate,
	caKey *rsa.PrivateKey,
	subject pkix.Name,
	dnsNames []string,
	identities []*url.URL,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate test certificate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
		DNSNames:     dnsNames,
		URIs:         identities,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCertificate, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("issue test certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
