package nodeagent

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"

	"github.com/vivym/vela/internal/securefile"
	"google.golang.org/grpc/credentials"
)

const (
	maxTLSCertificateBytes = 1 << 20
	maxTLSPrivateKeyBytes  = 1 << 20
	maxTLSCABytes          = 4 << 20
)

func NewServerTLSCredentials(certificatePath, privateKeyPath, clientCAPath string, localIdentity NodeAgentIdentity) (credentials.TransportCredentials, error) {
	certificatePEM, err := readTLSFile(certificatePath, maxTLSCertificateBytes, false)
	if err != nil {
		return nil, fmt.Errorf("read node Agent server certificate: %w", err)
	}
	privateKeyPEM, err := readTLSFile(privateKeyPath, maxTLSPrivateKeyBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read node Agent server private key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("node Agent server certificate and key are invalid")
	}
	expectedSPIFFEIdentity := NodeAgentSPIFFEIdentity(localIdentity)
	if expectedSPIFFEIdentity == "" {
		return nil, errors.New("node Agent local TLS identity is invalid")
	}
	leaf, err := certificateLeaf(certificate)
	if err != nil || !certificateHasNodeAgentIdentity(leaf, expectedSPIFFEIdentity) {
		return nil, errors.New("node Agent server certificate does not match its Node and Worker identity")
	}
	clientCAPEM, err := readTLSFile(clientCAPath, maxTLSCABytes, false)
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

func NewClientTLSCredentials(
	certificatePath, privateKeyPath, rootCAPath, serverName, expectedSPIFFEIdentity, actorIdentity string,
) (credentials.TransportCredentials, error) {
	if serverName == "" {
		return nil, errors.New("node Agent TLS server name is required")
	}
	parsedSPIFFEIdentity, err := url.Parse(expectedSPIFFEIdentity)
	if err != nil || !isNodeAgentSPIFFEID(parsedSPIFFEIdentity) {
		return nil, errors.New("node Agent expected SPIFFE identity is invalid")
	}
	certificatePEM, err := readTLSFile(certificatePath, maxTLSCertificateBytes, false)
	if err != nil {
		return nil, fmt.Errorf("read control-plane client certificate: %w", err)
	}
	privateKeyPEM, err := readTLSFile(privateKeyPath, maxTLSPrivateKeyBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read control-plane client private key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return nil, errors.New("control-plane client certificate and key are invalid")
	}
	controllerLeaf, err := certificateLeaf(certificate)
	if err != nil || !certificateHasControllerIdentity(controllerLeaf, ControllerSPIFFEIdentity(actorIdentity)) {
		return nil, errors.New("control-plane client certificate does not match its actor identity")
	}
	rootPEM, err := readTLSFile(rootCAPath, maxTLSCABytes, false)
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
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 ||
				!certificateHasNodeAgentIdentity(state.PeerCertificates[0], expectedSPIFFEIdentity) {
				return errors.New("node Agent server certificate SPIFFE identity does not match endpoint registration")
			}
			return nil
		},
	}), nil
}

func certificateLeaf(certificate tls.Certificate) (*x509.Certificate, error) {
	if certificate.Leaf != nil {
		return certificate.Leaf, nil
	}
	if len(certificate.Certificate) == 0 {
		return nil, errors.New("TLS certificate chain is empty")
	}
	return x509.ParseCertificate(certificate.Certificate[0])
}

func certificateHasNodeAgentIdentity(certificate *x509.Certificate, expected string) bool {
	return certificate != nil && len(certificate.URIs) == 1 &&
		certificate.URIs[0] != nil && certificate.URIs[0].String() == expected
}

func certificateHasControllerIdentity(certificate *x509.Certificate, expected string) bool {
	if certificate == nil || expected == "" || len(certificate.URIs) != 1 || certificate.URIs[0] == nil {
		return false
	}
	identity := certificate.URIs[0]
	return identity.String() == expected && isControllerSPIFFEID(identity)
}

func readTLSFile(path string, maxBytes int64, private bool) ([]byte, error) {
	return securefile.Read(path, maxBytes, private)
}
