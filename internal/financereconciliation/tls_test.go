package financereconciliation

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
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

func TestServerTLSConfigAndHTTPHandlerRequireExactFinanceIdentity(t *testing.T) {
	caCertificate, caKey, caPEM := issueFinanceTestCA(t, "Vela Finance Test CA")
	serverPEM, serverKeyPEM := issueFinanceTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "finance-listener.internal"},
		[]string{"finance-listener.internal"},
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	expectedURI := mustParseTestURL(t, "spiffe://finance.internal/reconciliation/primary")
	validClientPEM, validClientKeyPEM := issueFinanceTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "primary-finance-reconciliation"},
		nil,
		[]*url.URL{expectedURI},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	wrongURI := mustParseTestURL(t, "spiffe://finance.internal/reconciliation/other")
	wrongURIClientPEM, wrongURIClientKeyPEM := issueFinanceTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "other-finance-reconciliation"},
		nil,
		[]*url.URL{wrongURI},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	additionalURIClientPEM, additionalURIClientKeyPEM := issueFinanceTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "ambiguous-finance-reconciliation"},
		nil,
		[]*url.URL{expectedURI, wrongURI},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	otherCA, otherCAKey, _ := issueFinanceTestCA(t, "Other Finance Test CA")
	wrongCAClientPEM, wrongCAClientKeyPEM := issueFinanceTestCertificate(
		t,
		otherCA,
		otherCAKey,
		pkix.Name{CommonName: "wrong-ca-finance-reconciliation"},
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
			t.Fatalf("write Finance TLS fixture: %v", err)
		}
	}
	tlsConfig, err := NewServerTLSConfig(serverCertificatePath, serverKeyPath, clientCAPath)
	if err != nil {
		t.Fatalf("NewServerTLSConfig: %v", err)
	}
	applier := &recordingApplier{
		identity: Identity{
			PrincipalID:    uuid.MustParse("00000000-0000-0000-0000-000000001901"),
			StableID:       "primary-finance-reconciliation",
			TLSURIIdentity: expectedURI.String(),
		},
		result: Result{
			RecordID:                 uuid.MustParse("00000000-0000-0000-0000-000000001904"),
			OrganizationID:           uuid.MustParse("00000000-0000-0000-0000-000000001903"),
			Kind:                     KindSettlementPosted,
			Currency:                 "CNY",
			ContractCreditLimitMinor: 100000,
			UnsettledPostedMinor:     750,
			AccountVersion:           9,
			PostedAt:                 time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC),
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
		t.Fatal("append Finance test server root")
	}
	validClient := parseFinanceTestKeyPair(t, validClientPEM, validClientKeyPEM)
	response, err := sendFinanceTestRequest(server.URL, serverRoots, &validClient, tls.VersionTLS13, tls.VersionTLS13)
	if err != nil {
		t.Fatalf("send valid Finance mTLS request: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusCreated || len(applier.requests) != 1 {
		t.Fatalf("valid Finance mTLS response = %d calls=%d", response.StatusCode, len(applier.requests))
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
			certificate: financeTestKeyPairPointer(t, wrongCAClientPEM, wrongCAClientKeyPEM),
			minVersion:  tls.VersionTLS13, maxVersion: tls.VersionTLS13, wantError: true,
		},
		{
			name:        "TLS 1.2",
			certificate: &validClient,
			minVersion:  tls.VersionTLS12, maxVersion: tls.VersionTLS12, wantError: true,
		},
		{
			name:        "wrong URI",
			certificate: financeTestKeyPairPointer(t, wrongURIClientPEM, wrongURIClientKeyPEM),
			minVersion:  tls.VersionTLS13, maxVersion: tls.VersionTLS13,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:        "additional URI",
			certificate: financeTestKeyPairPointer(t, additionalURIClientPEM, additionalURIClientKeyPEM),
			minVersion:  tls.VersionTLS13, maxVersion: tls.VersionTLS13,
			wantStatus: http.StatusUnauthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			beforeCalls := len(applier.requests)
			response, err := sendFinanceTestRequest(
				server.URL,
				serverRoots,
				test.certificate,
				test.minVersion,
				test.maxVersion,
			)
			if test.wantError {
				if err == nil {
					_ = response.Body.Close()
					t.Fatal("Finance TLS request unexpectedly completed")
				}
			} else {
				if err != nil {
					t.Fatalf("Finance TLS request: %v", err)
				}
				_ = response.Body.Close()
				if response.StatusCode != test.wantStatus {
					t.Fatalf("Finance TLS response = %d, want %d", response.StatusCode, test.wantStatus)
				}
			}
			if len(applier.requests) != beforeCalls {
				t.Fatalf("rejected Finance TLS request reached Apply: calls %d -> %d", beforeCalls, len(applier.requests))
			}
		})
	}
}

func sendFinanceTestRequest(
	serverURL string,
	roots *x509.CertPool,
	certificate *tls.Certificate,
	minVersion, maxVersion uint16,
) (*http.Response, error) {
	tlsConfig := &tls.Config{
		MinVersion: minVersion,
		MaxVersion: maxVersion,
		ServerName: "finance-listener.internal",
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
		"idempotency_key":"settlement-tls-1901",
		"source_sequence":1,
		"organization_id":"00000000-0000-0000-0000-000000001903",
		"kind":"SETTLEMENT_POSTED",
		"currency":"CNY",
		"settlement_minor":500,
		"external_reference":"payment-tls-1901",
		"effective_at":"2026-08-24T12:00:00Z"
	}`)
	request, err := http.NewRequest(
		http.MethodPost,
		serverURL+ReconciliationPath,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	return client.Do(request)
}

func issueFinanceTestCA(
	t *testing.T,
	commonName string,
) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate Finance test CA key: %v", err)
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
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("issue Finance test CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse Finance test CA: %v", err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueFinanceTestCertificate(
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
		t.Fatalf("generate Finance test certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate Finance test certificate serial: %v", err)
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
		t.Fatalf("issue Finance test certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func parseFinanceTestKeyPair(t *testing.T, certificatePEM, keyPEM []byte) tls.Certificate {
	t.Helper()
	pair, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		t.Fatalf("parse Finance test key pair: %v", err)
	}
	return pair
}

func financeTestKeyPairPointer(t *testing.T, certificatePEM, keyPEM []byte) *tls.Certificate {
	t.Helper()
	pair := parseFinanceTestKeyPair(t, certificatePEM, keyPEM)
	return &pair
}
