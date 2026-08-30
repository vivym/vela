package stageworkertransport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/securefile"
	"google.golang.org/grpc/credentials"
)

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
		return nil, fmt.Errorf("read Stage Worker control server certificate: %w", err)
	}
	privateKeyPEM, err := securefile.Read(
		privateKeyPath,
		maximumTLSPrivateKeyBytes,
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("read Stage Worker control server private key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("Stage Worker control server certificate and key are invalid")
	}
	clientCAPEM, err := securefile.Read(clientCAPath, maximumTLSCABytes, false)
	if err != nil {
		return nil, fmt.Errorf("read Stage Worker control client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("Stage Worker control client CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{
			certificate,
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  clientCAs,
		NextProtos: []string{"h2"},
	}), nil
}
