package nodeagent

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/grpc/credentials"
)

const (
	maxTLSCertificateBytes = 1 << 20
	maxTLSPrivateKeyBytes  = 1 << 20
	maxTLSCABytes          = 4 << 20
)

func NewServerTLSCredentials(certificatePath, privateKeyPath, clientCAPath string) (credentials.TransportCredentials, error) {
	certificatePEM, err := readTLSFile(certificatePath, maxTLSCertificateBytes)
	if err != nil {
		return nil, fmt.Errorf("read node Agent server certificate: %w", err)
	}
	privateKeyPEM, err := readTLSFile(privateKeyPath, maxTLSPrivateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("read node Agent server private key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("node Agent server certificate and key are invalid")
	}
	clientCAPEM, err := readTLSFile(clientCAPath, maxTLSCABytes)
	if err != nil {
		return nil, fmt.Errorf("read node Agent controller CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("node Agent controller CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs,
		NextProtos: []string{"h2"},
	}), nil
}

func NewClientTLSCredentials(certificatePath, privateKeyPath, rootCAPath, serverName string) (credentials.TransportCredentials, error) {
	if serverName == "" {
		return nil, errors.New("node Agent TLS server name is required")
	}
	certificatePEM, err := readTLSFile(certificatePath, maxTLSCertificateBytes)
	if err != nil {
		return nil, fmt.Errorf("read control-plane client certificate: %w", err)
	}
	privateKeyPEM, err := readTLSFile(privateKeyPath, maxTLSPrivateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("read control-plane client private key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("control-plane client certificate and key are invalid")
	}
	rootPEM, err := readTLSFile(rootCAPath, maxTLSCABytes)
	if err != nil {
		return nil, fmt.Errorf("read node Agent root CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		return nil, errors.New("node Agent root CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate},
		RootCAs: roots, ServerName: serverName, NextProtos: []string{"h2"},
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
