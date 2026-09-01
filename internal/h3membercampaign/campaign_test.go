package h3membercampaign

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRunProvesAuthenticatedMultiMemberBarrierAndCancellation(t *testing.T) {
	topology := FixedTopology()
	files := issueCampaignTLS(t, topology)
	address := unusedAddress(t)
	key := []byte("0123456789abcdef0123456789abcdef")
	serverContext, stopServer := context.WithCancel(context.Background())
	ready := make(chan struct{})
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- Serve(serverContext, ServerConfig{
			ListenAddress: address,
			TLS: ServerTLSFiles{
				Certificate: files.serverCertificate,
				PrivateKey:  files.serverPrivateKey,
				ClientCA:    files.ca,
			},
			AuthorityKeyID: "campaign-authority-v1",
			AuthorityKey:   key,
			Ready:          ready,
		})
	}()
	select {
	case <-ready:
	case err := <-serverErrors:
		t.Fatalf("serve follower before ready: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("follower did not become ready")
	}

	runContext, cancelRun := context.WithTimeout(context.Background(), 5*time.Second)
	receipt, err := Run(runContext, ClientConfig{
		Address:    address,
		ServerName: topology.Follower.ServerName,
		TLS: ClientTLSFiles{
			Certificate: files.clientCertificate,
			PrivateKey:  files.clientPrivateKey,
			ServerCA:    files.ca,
		},
		AuthorityKeyID: "campaign-authority-v1",
		AuthorityKey:   key,
		DialTimeout:    2 * time.Second,
		CommandTimeout: 2 * time.Second,
	})
	cancelRun()
	if err != nil {
		t.Fatalf("run authenticated member campaign: %v", err)
	}
	if receipt.SchemaVersion != SchemaVersion || receipt.Outcome != OutcomePass ||
		!receipt.BarrierPassed || receipt.PreparedMembers != 2 || receipt.StartedMembers != 2 ||
		receipt.ReportingMembers != 2 || receipt.CancellationAcknowledgedMembers != 2 ||
		!receipt.AllStopped || receipt.LeaderMemberID != topology.Leader.ID ||
		receipt.FollowerMemberID != topology.Follower.ID ||
		!validCampaignReceiptBinding(receipt, topology) {
		t.Fatalf("campaign receipt = %#v", receipt)
	}

	stopServer()
	select {
	case err := <-serverErrors:
		if err != nil {
			t.Fatalf("stop follower: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("follower did not stop")
	}
}

func TestRunRejectsFollowerLossBetweenPrepareAndStart(t *testing.T) {
	topology := FixedTopology()
	files := issueCampaignTLS(t, topology)
	address := unusedAddress(t)
	key := []byte("0123456789abcdef0123456789abcdef")
	serverContext, stopServer := context.WithCancel(context.Background())
	ready := make(chan struct{})
	prepared := make(chan struct{}, 1)
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- Serve(serverContext, ServerConfig{
			ListenAddress: address,
			TLS: ServerTLSFiles{
				Certificate: files.serverCertificate,
				PrivateKey:  files.serverPrivateKey,
				ClientCA:    files.ca,
			},
			AuthorityKeyID: "campaign-authority-v1",
			AuthorityKey:   key,
			Ready:          ready,
			Logf: func(format string, _ ...any) {
				if format == "prepared member %s" {
					prepared <- struct{}{}
				}
			},
		})
	}()
	select {
	case <-ready:
	case err := <-serverErrors:
		t.Fatalf("serve fault follower before ready: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("fault follower did not become ready")
	}

	runContext, cancelRun := context.WithTimeout(context.Background(), 5*time.Second)
	type result struct {
		receipt Receipt
		err     error
	}
	results := make(chan result, 1)
	go func() {
		receipt, err := Run(runContext, ClientConfig{
			Address:    address,
			ServerName: topology.Follower.ServerName,
			TLS: ClientTLSFiles{
				Certificate: files.clientCertificate,
				PrivateKey:  files.clientPrivateKey,
				ServerCA:    files.ca,
			},
			AuthorityKeyID:       "campaign-authority-v1",
			AuthorityKey:         key,
			DialTimeout:          2 * time.Second,
			CommandTimeout:       2 * time.Second,
			RemoteStartDelay:     250 * time.Millisecond,
			ExpectedStartFailure: true,
		})
		results <- result{receipt: receipt, err: err}
	}()
	select {
	case <-prepared:
		stopServer()
	case err := <-serverErrors:
		t.Fatalf("fault follower stopped before prepare: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("fault follower was not prepared")
	}
	select {
	case err := <-serverErrors:
		if err != nil {
			t.Fatalf("stop fault follower: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fault follower did not stop")
	}
	select {
	case result := <-results:
		cancelRun()
		if result.err != nil {
			t.Fatalf("run expected follower-loss campaign: %v", result.err)
		}
		receipt := result.receipt
		if receipt.Outcome != OutcomeFaultRejected || receipt.BarrierPassed ||
			receipt.PreparedMembers != 2 || receipt.StartedMembers != 1 ||
			receipt.CancellationAcknowledgedMembers != 1 || !receipt.LocalMemberStopped ||
			!receipt.RemoteMemberUnavailable || receipt.AllStopped || receipt.FaultPhase != "START" ||
			!validCampaignReceiptBinding(receipt, topology) {
			t.Fatalf("fault campaign receipt = %#v", receipt)
		}
	case <-time.After(3 * time.Second):
		cancelRun()
		t.Fatal("fault campaign did not finish")
	}
}

func validCampaignReceiptBinding(receipt Receipt, topology Topology) bool {
	if uuid.Validate(receipt.ExerciseID) != nil || receipt.StartedAt.IsZero() ||
		receipt.CompletedAt.Before(receipt.StartedAt) {
		return false
	}
	for _, value := range []string{
		receipt.AuthorityDigest,
		receipt.LeaderIdentityDigest,
		receipt.FollowerIdentityDigest,
	} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != 32 {
			return false
		}
	}
	return receipt.LeaderIdentityDigest == hex.EncodeToString(memberIdentityDigest(topology.Leader)) &&
		receipt.FollowerIdentityDigest == hex.EncodeToString(memberIdentityDigest(topology.Follower))
}

type campaignTLSFiles struct {
	ca                string
	serverCertificate string
	serverPrivateKey  string
	clientCertificate string
	clientPrivateKey  string
}

func issueCampaignTLS(t *testing.T, topology Topology) campaignTLSFiles {
	t.Helper()
	_, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate campaign CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Vela disposable member campaign CA"},
		NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(
		rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey,
	)
	if err != nil {
		t.Fatalf("issue campaign CA: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse campaign CA: %v", err)
	}
	serverCertificate, serverPrivateKey := issueCampaignCertificate(
		t, ca, caKey, topology.Follower.ServerName, []string{topology.Follower.ServerName},
		topology.Follower.SPIFFEID, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	clientCertificate, clientPrivateKey := issueCampaignCertificate(
		t, ca, caKey, "campaign-leader", nil,
		topology.Leader.SPIFFEID, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	return campaignTLSFiles{
		ca: writeCampaignFile(t, directory, "ca.crt", pem.EncodeToMemory(&pem.Block{
			Type: "CERTIFICATE", Bytes: caDER,
		})),
		serverCertificate: writeCampaignFile(t, directory, "server.crt", serverCertificate),
		serverPrivateKey:  writeCampaignFile(t, directory, "server.key", serverPrivateKey),
		clientCertificate: writeCampaignFile(t, directory, "client.crt", clientCertificate),
		clientPrivateKey:  writeCampaignFile(t, directory, "client.key", clientPrivateKey),
	}
}

func issueCampaignCertificate(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	commonName string,
	dnsNames []string,
	identity string,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate campaign certificate key: %v", err)
	}
	spiffeID, err := url.Parse(identity)
	if err != nil {
		t.Fatalf("parse campaign SPIFFE identity: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate campaign certificate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usage,
		DNSNames: dnsNames, URIs: []*url.URL{spiffeID},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		t.Fatalf("issue campaign certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal campaign certificate key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func writeCampaignFile(t *testing.T, directory, name string, contents []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write campaign TLS file: %v", err)
	}
	return path
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve campaign address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release campaign address: %v", err)
	}
	return address
}
