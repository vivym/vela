package financereconciliation

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	maxTLSCertificateBytes = 1 << 20
	maxTLSPrivateKeyBytes  = 1 << 20
	maxTLSClientCABytes    = 4 << 20
)

func NewServerTLSConfig(
	certificatePath string,
	privateKeyPath string,
	clientCAPath string,
) (*tls.Config, error) {
	certificatePEM, err := readTLSFile(certificatePath, maxTLSCertificateBytes, false)
	if err != nil {
		return nil, fmt.Errorf("read Finance Reconciliation server certificate: %w", err)
	}
	privateKeyPEM, err := readTLSFile(privateKeyPath, maxTLSPrivateKeyBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read Finance Reconciliation server private key: %w", err)
	}
	defer clear(privateKeyPEM)
	serverCertificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("finance reconciliation server certificate and key are invalid")
	}
	clientCAPEM, err := readTLSFile(clientCAPath, maxTLSClientCABytes, false)
	if err != nil {
		return nil, fmt.Errorf("read Finance Reconciliation client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("finance reconciliation client CA contains no certificates")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		NextProtos:   []string{"h2", "http/1.1"},
	}, nil
}

func readTLSFile(path string, maximum int64, private bool) ([]byte, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || maximum <= 0 {
		return nil, errors.New("TLS file path must be absolute")
	}
	file, err := os.Open(cleaned)
	if err != nil {
		return nil, errors.New("TLS file cannot be opened")
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("TLS file must be a non-empty bounded regular file")
	}
	if private && info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("TLS private key file must not be group or world accessible")
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || len(content) == 0 || int64(len(content)) > maximum {
		clear(content)
		return nil, errors.New("TLS file cannot be read within configured bounds")
	}
	return content, nil
}
