package main

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
)

func TestGenerateCreatesProtectedCompleteAssets(t *testing.T) {
	output := filepath.Join(t.TempDir(), "assets")
	if err := generate(options{
		output: output, postgresHost: defaultPostgresHost,
		natsHost: defaultNATSHost, minioHost: defaultMinIOHost,
		validFor: 24 * time.Hour,
	}); err != nil {
		t.Fatalf("generate assets: %v", err)
	}
	information, err := os.Stat(output)
	if err != nil || information.Mode().Perm() != 0o700 {
		t.Fatalf("asset directory mode = %v error=%v", information, err)
	}
	for _, relative := range []string{
		"manifest.json", "env/database.env", "env/control-secret.env",
		"nats/nats.conf", "nats/outbox.creds", "nats/scheduler.creds",
		"pki/control-worker.crt", "pki/worker-1.crt", "pki/worker-2.crt",
		"control/lease.json", "control/webhook.json", "smoke/bearer-credential",
	} {
		path := filepath.Join(output, relative)
		information, err := os.Stat(path)
		if err != nil || information.Mode().Perm() != 0o600 {
			t.Fatalf("asset %s mode = %v error=%v", relative, information, err)
		}
	}
	manifestBytes, err := os.ReadFile(filepath.Join(output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Files map[string]string `json:"files"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("decode asset manifest: %v", err)
	}
	for relative, expected := range manifest.Files {
		content, err := os.ReadFile(filepath.Join(output, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("read manifested asset %s: %v", relative, err)
		}
		digest := sha256.Sum256(content)
		if observed := hex.EncodeToString(digest[:]); observed != expected {
			t.Fatalf("manifest digest for %s = %s, want %s", relative, observed, expected)
		}
	}
	if len(manifest.Files) < 30 {
		t.Fatalf("manifested asset file count = %d, want complete generated set", len(manifest.Files))
	}
	certificate := readCertificate(t, filepath.Join(output, "pki", "worker-1.crt"))
	if len(certificate.URIs) != 1 || certificate.URIs[0].String() != "spiffe://vela.internal/worker/"+worker1Name {
		t.Fatalf("worker certificate URIs = %v", certificate.URIs)
	}
	for _, fixture := range []struct {
		file           string
		name           string
		publishSubject string
	}{
		{file: "outbox.creds", name: "vela-outbox-dispatcher", publishSubject: "vela.events.>"},
		{file: "scheduler.creds", name: "vela-scheduler-consumer", publishSubject: "$JS.API.CONSUMER.MSG.NEXT.VELA_EVENTS.VELA_SCHEDULER"},
	} {
		credential, err := os.ReadFile(filepath.Join(output, "nats", fixture.file))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := jwt.ParseDecoratedJWT(credential)
		if err != nil {
			t.Fatalf("parse %s: %v", fixture.file, err)
		}
		claims, err := jwt.DecodeUserClaims(encoded)
		if err != nil || claims.Subject == "" || claims.Name != fixture.name ||
			!claims.Pub.Allow.Contains(fixture.publishSubject) {
			t.Fatalf("%s claims = %#v error=%v", fixture.file, claims, err)
		}
	}
	bearer, err := os.ReadFile(filepath.Join(output, "smoke", "bearer-credential"))
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(bearer)), "vla_"+smokeCredentialID+".") {
		t.Fatalf("smoke bearer = %q error=%v", bearer, err)
	}
	nodeAgentBytes, err := os.ReadFile(filepath.Join(output, "control", "node-agents.json"))
	if err != nil {
		t.Fatal(err)
	}
	var nodeAgents map[string]struct {
		WorkerID       string `json:"worker_id"`
		WorkerEpoch    int64  `json:"worker_epoch"`
		SPIFFEIdentity string `json:"spiffe_identity"`
	}
	if err := json.Unmarshal(nodeAgentBytes, &nodeAgents); err != nil {
		t.Fatalf("decode Node Agent endpoints: %v", err)
	}
	endpoint, ok := nodeAgents[worker1Name]
	wantSPIFFEIdentity := "spiffe://vela.internal/node-agent/" +
		base64.RawURLEncoding.EncodeToString([]byte(worker1Name)) + "/" + worker1ID
	if !ok || endpoint.WorkerID != worker1ID || endpoint.WorkerEpoch != 1 ||
		endpoint.SPIFFEIdentity != wantSPIFFEIdentity {
		t.Fatalf("Node Agent endpoint = %#v, want SPIFFE identity %q", endpoint, wantSPIFFEIdentity)
	}
}

func TestGenerateRefusesExistingOutput(t *testing.T) {
	output := filepath.Join(t.TempDir(), "assets")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	err := generate(options{
		output: output, postgresHost: defaultPostgresHost,
		natsHost: defaultNATSHost, minioHost: defaultMinIOHost,
		validFor: 24 * time.Hour,
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing output error = %v", err)
	}
}

func readCertificate(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(content)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("decode certificate %s", path)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}
