package legalhold

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestServerTLSConfigAndHTTPHandlerRequireExactComplianceIdentity(t *testing.T) {
	caCertificate, caKey, caPEM := issueLegalHoldTestCA(t, "Vela Compliance Test CA")
	serverPEM, serverKeyPEM := issueLegalHoldTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "legal-hold-listener.internal"},
		[]string{"legal-hold-listener.internal"},
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	expectedURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/primary")
	validClientPEM, validClientKeyPEM := issueLegalHoldTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "primary-compliance"},
		nil,
		[]*url.URL{expectedURI},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	wrongURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/other")
	wrongURIClientPEM, wrongURIClientKeyPEM := issueLegalHoldTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "other-compliance"},
		nil,
		[]*url.URL{wrongURI},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	additionalURIClientPEM, additionalURIClientKeyPEM := issueLegalHoldTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "ambiguous-compliance"},
		nil,
		[]*url.URL{expectedURI, wrongURI},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	otherCA, otherCAKey, _ := issueLegalHoldTestCA(t, "Other Compliance Test CA")
	wrongCAClientPEM, wrongCAClientKeyPEM := issueLegalHoldTestCertificate(
		t,
		otherCA,
		otherCAKey,
		pkix.Name{CommonName: "wrong-ca-compliance"},
		nil,
		[]*url.URL{expectedURI},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)

	directory := t.TempDir()
	serverCertificatePath := filepath.Join(directory, "server.crt")
	serverKeyPath := filepath.Join(directory, "server.key")
	clientCAPath := filepath.Join(directory, "client-ca.crt")
	for path, content := range map[string][]byte{
		serverCertificatePath: serverPEM,
		serverKeyPath:         serverKeyPEM,
		clientCAPath:          caPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Compliance TLS fixture: %v", err)
		}
	}
	tlsConfig, err := NewServerTLSConfig(serverCertificatePath, serverKeyPath, clientCAPath)
	if err != nil {
		t.Fatalf("NewServerTLSConfig: %v", err)
	}
	applier := &recordingApplier{
		identity: Identity{
			PrincipalID:    uuid.MustParse("00000000-0000-0000-0000-000000003305"),
			StableID:       "primary-compliance",
			TLSURIIdentity: expectedURI.String(),
		},
		result: Result{
			EventID:       uuid.MustParse("00000000-0000-0000-0000-000000003306"),
			HoldID:        uuid.MustParse("00000000-0000-0000-0000-000000003301"),
			State:         StateActive,
			Scope:         ScopeOrganization,
			RecordClasses: []RecordClass{RecordClassMetadata},
			RecordedAt:    time.Date(2026, 8, 27, 14, 0, 0, 0, time.UTC),
		},
	}
	handler, err := NewHTTPHandler(applier)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = tlsConfig
	server.StartTLS()
	defer server.Close()

	serverRoots := x509.NewCertPool()
	if !serverRoots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append Compliance test server root")
	}
	validClient := parseLegalHoldTestKeyPair(t, validClientPEM, validClientKeyPEM)
	response, err := sendLegalHoldTestRequest(
		server.URL,
		serverRoots,
		&validClient,
		tls.VersionTLS13,
		tls.VersionTLS13,
	)
	if err != nil {
		t.Fatalf("send valid Compliance mTLS request: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusCreated || len(applier.requests) != 1 {
		t.Fatalf("valid Compliance mTLS response = %d calls=%d", response.StatusCode, len(applier.requests))
	}

	for _, test := range []struct {
		name        string
		certificate *tls.Certificate
		minVersion  uint16
		maxVersion  uint16
		wantStatus  int
		wantError   bool
	}{
		{name: "anonymous", minVersion: tls.VersionTLS13, maxVersion: tls.VersionTLS13, wantError: true},
		{
			name:        "wrong CA",
			certificate: legalHoldTestKeyPairPointer(t, wrongCAClientPEM, wrongCAClientKeyPEM),
			minVersion:  tls.VersionTLS13, maxVersion: tls.VersionTLS13, wantError: true,
		},
		{
			name:        "TLS 1.2",
			certificate: &validClient,
			minVersion:  tls.VersionTLS12, maxVersion: tls.VersionTLS12, wantError: true,
		},
		{
			name:        "wrong URI",
			certificate: legalHoldTestKeyPairPointer(t, wrongURIClientPEM, wrongURIClientKeyPEM),
			minVersion:  tls.VersionTLS13, maxVersion: tls.VersionTLS13,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:        "additional URI",
			certificate: legalHoldTestKeyPairPointer(t, additionalURIClientPEM, additionalURIClientKeyPEM),
			minVersion:  tls.VersionTLS13, maxVersion: tls.VersionTLS13,
			wantStatus: http.StatusUnauthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			beforeCalls := len(applier.requests)
			response, err := sendLegalHoldTestRequest(
				server.URL,
				serverRoots,
				test.certificate,
				test.minVersion,
				test.maxVersion,
			)
			if test.wantError {
				if err == nil {
					_ = response.Body.Close()
					t.Fatal("Compliance TLS request unexpectedly completed")
				}
			} else {
				if err != nil {
					t.Fatalf("Compliance TLS request: %v", err)
				}
				_ = response.Body.Close()
				if response.StatusCode != test.wantStatus {
					t.Fatalf("Compliance TLS response = %d, want %d", response.StatusCode, test.wantStatus)
				}
			}
			if len(applier.requests) != beforeCalls {
				t.Fatalf("rejected Compliance TLS request reached Apply: calls %d -> %d", beforeCalls, len(applier.requests))
			}
		})
	}
}

func sendLegalHoldTestRequest(
	serverURL string,
	roots *x509.CertPool,
	certificate *tls.Certificate,
	minVersion, maxVersion uint16,
) (*http.Response, error) {
	tlsConfig := &tls.Config{
		MinVersion: minVersion,
		MaxVersion: maxVersion,
		ServerName: "legal-hold-listener.internal",
		RootCAs:    roots,
	}
	if certificate != nil {
		tlsConfig.Certificates = []tls.Certificate{*certificate}
	}
	client := &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	body := []byte(`{
		"idempotency_key":"legal-hold-tls-3301",
		"source_sequence":1,
		"hold_id":"00000000-0000-0000-0000-000000003301",
		"kind":"HOLD_PLACED",
		"scope":"ORGANIZATION",
		"organization_id":"00000000-0000-0000-0000-000000003302",
		"record_classes":["METADATA"],
		"reason_code":"LITIGATION",
		"external_reference":"matter-tls-3301/place",
		"effective_at":"2026-08-27T12:00:00Z"
	}`)
	request, err := http.NewRequest(http.MethodPost, serverURL+EventPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	return client.Do(request)
}

func issueLegalHoldTestCA(
	t *testing.T,
	commonName string,
) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Compliance test CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatalf("issue Compliance test CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse Compliance test CA: %v", err)
	}
	return certificate, privateKey, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueLegalHoldTestCertificate(
	t *testing.T,
	caCertificate *x509.Certificate,
	caKey ed25519.PrivateKey,
	subject pkix.Name,
	dnsNames []string,
	identities []*url.URL,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Compliance test certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate Compliance test certificate serial: %v", err)
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
	der, err := x509.CreateCertificate(rand.Reader, template, caCertificate, publicKey, caKey)
	if err != nil {
		t.Fatalf("issue Compliance test certificate: %v", err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal Compliance test private key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
}

func parseLegalHoldTestKeyPair(t *testing.T, certificatePEM, keyPEM []byte) tls.Certificate {
	t.Helper()
	pair, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		t.Fatalf("parse Compliance test key pair: %v", err)
	}
	return pair
}

func legalHoldTestKeyPairPointer(t *testing.T, certificatePEM, keyPEM []byte) *tls.Certificate {
	t.Helper()
	pair := parseLegalHoldTestKeyPair(t, certificatePEM, keyPEM)
	return &pair
}
