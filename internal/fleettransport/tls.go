package fleettransport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"

	"github.com/vivym/vela/internal/securefile"
	"google.golang.org/grpc/credentials"
)

func NewClientTLSCredentials(
	certificatePath string,
	privateKeyPath string,
	serverCAPath string,
	serverName string,
) (credentials.TransportCredentials, error) {
	if serverName == "" || len(serverName) > 253 ||
		strings.TrimSpace(serverName) != serverName || strings.ContainsRune(serverName, '\x00') {
		return nil, errors.New("fleet maintenance server name is invalid")
	}
	certificatePEM, err := securefile.Read(
		certificatePath,
		maximumTLSCertificateBytes,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("read Fleet maintenance client certificate: %w", err)
	}
	privateKeyPEM, err := securefile.Read(privateKeyPath, maximumTLSPrivateKeyBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read Fleet maintenance client private key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("fleet maintenance client certificate and key are invalid")
	}
	serverCAPEM, err := securefile.Read(serverCAPath, maximumTLSCABytes, false)
	if err != nil {
		return nil, fmt.Errorf("read Fleet maintenance server CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverCAPEM) {
		return nil, errors.New("fleet maintenance server CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, ServerName: serverName,
		RootCAs: roots, Certificates: []tls.Certificate{certificate},
		NextProtos: []string{"h2"},
	}), nil
}

const (
	maximumTLSCertificateBytes = 1 << 20
	maximumTLSPrivateKeyBytes  = 1 << 20
	maximumTLSCABytes          = 4 << 20
)

func NewServerTLSCredentials(
	certificatePath string,
	privateKeyPath string,
	clientCAPath string,
) (credentials.TransportCredentials, error) {
	certificatePEM, err := securefile.Read(
		certificatePath,
		maximumTLSCertificateBytes,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("read Fleet maintenance server certificate: %w", err)
	}
	privateKeyPEM, err := securefile.Read(privateKeyPath, maximumTLSPrivateKeyBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read Fleet maintenance server private key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("fleet maintenance server certificate and key are invalid")
	}
	clientCAPEM, err := securefile.Read(clientCAPath, maximumTLSCABytes, false)
	if err != nil {
		return nil, fmt.Errorf("read Fleet maintenance client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("fleet maintenance client CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs,
		NextProtos: []string{"h2"},
	}), nil
}
