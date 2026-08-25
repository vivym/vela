package workertransport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/credentials"
)

const (
	maxTLSCertificateBytes = 1 << 20
	maxTLSPrivateKeyBytes  = 1 << 20
	maxTLSClientCABytes    = 4 << 20
)

func NewServerTLSCredentials(
	certificatePath string,
	privateKeyPath string,
	clientCAPath string,
) (credentials.TransportCredentials, error) {
	certificatePEM, err := readTLSFile(certificatePath, maxTLSCertificateBytes)
	if err != nil {
		return nil, fmt.Errorf("read Worker gRPC server certificate: %w", err)
	}
	privateKeyPEM, err := readTLSFile(privateKeyPath, maxTLSPrivateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("read Worker gRPC server private key: %w", err)
	}
	serverCertificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("worker gRPC server certificate and key are invalid")
	}
	clientCAPEM, err := readTLSFile(clientCAPath, maxTLSClientCABytes)
	if err != nil {
		return nil, fmt.Errorf("read Worker gRPC client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("worker gRPC client CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		NextProtos:   []string{"h2"},
	}), nil
}

func NewClientTLSCredentials(
	certificatePath string,
	privateKeyPath string,
	serverCAPath string,
	serverName string,
) (credentials.TransportCredentials, error) {
	if strings.TrimSpace(serverName) != serverName || serverName == "" ||
		len(serverName) > 253 || strings.ContainsRune(serverName, '\x00') {
		return nil, errors.New("worker gRPC server name is invalid")
	}
	certificatePEM, err := readTLSFile(certificatePath, maxTLSCertificateBytes)
	if err != nil {
		return nil, fmt.Errorf("read Worker gRPC client certificate: %w", err)
	}
	privateKeyPEM, err := readTLSFile(privateKeyPath, maxTLSPrivateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("read Worker gRPC client private key: %w", err)
	}
	clientCertificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("worker gRPC client certificate and key are invalid")
	}
	serverCAPEM, err := readTLSFile(serverCAPath, maxTLSClientCABytes)
	if err != nil {
		return nil, fmt.Errorf("read Worker gRPC server CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(serverCAPEM) {
		return nil, errors.New("worker gRPC server CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, ServerName: serverName,
		RootCAs: rootCAs, Certificates: []tls.Certificate{clientCertificate},
		NextProtos: []string{"h2"},
	}), nil
}

func readTLSFile(path string, maxBytes int64) ([]byte, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || maxBytes <= 0 {
		return nil, errors.New("TLS file path must be absolute")
	}
	file, err := os.Open(cleaned)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxBytes {
		return nil, errors.New("TLS file must be a non-empty bounded regular file")
	}
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 || int64(len(content)) > maxBytes {
		return nil, errors.New("TLS file exceeds configured bounds")
	}
	return content, nil
}
