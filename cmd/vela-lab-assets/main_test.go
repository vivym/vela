package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/vivym/vela/internal/labv2contract"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testRuntimeImage          = "10.1.200.17:5443/vela-lab-next/vela-h3-stage-runtime@sha256:7cba327fcc04f689d72140287a8c206aeb6080a7ddff641bc19efeebff1d537b"
	testThumbnailRuntimeImage = "10.1.200.17:5443/vela-lab-next/vela-lab-bootstrap@sha256:8cba327fcc04f689d72140287a8c206aeb6080a7ddff641bc19efeebff1d537b"
)

func TestGenerateCreatesProtectedCompleteAssets(t *testing.T) {
	output := filepath.Join(t.TempDir(), "assets")
	if err := generate(options{
		output: output, postgresHost: defaultPostgresHost,
		natsHost: defaultNATSHost, minioHost: defaultMinIOHost,
		runtimeImage: testRuntimeImage, thumbnailRuntimeImage: testThumbnailRuntimeImage,
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
		"pki/control-stage-worker.crt", "pki/stage-worker-1.crt", "pki/stage-worker-2.crt",
		"pki/stage-worker-thumbnail.crt",
		"pki/fleet-client.crt", "pki/fleet-admission.crt", "pki/minio-server.crt",
		"control/lease.json", "control/webhook.json", "control/model-runtime-verifier.json",
		"control/stage-worker-identity-key", "stage/worker-1-launch.json",
		"stage/worker-2-launch.json", "stage/worker-thumbnail-launch.json",
		"env/stage-worker-1.env", "env/stage-worker-2.env", "env/stage-worker-thumbnail.env",
		"smoke/bearer-credential",
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
	certificate := readCertificate(t, filepath.Join(output, "pki", "stage-worker-1.crt"))
	if len(certificate.URIs) != 1 || certificate.URIs[0].String() != stageWorkerSPIFFEIdentity(worker1MemberID) {
		t.Fatalf("worker certificate URIs = %v", certificate.URIs)
	}
	minioCertificate := readCertificate(t, filepath.Join(output, "pki", "minio-server.crt"))
	if err := minioCertificate.VerifyHostname(defaultMinIOHost); err != nil {
		t.Fatalf("MinIO certificate does not cover %q: %v", defaultMinIOHost, err)
	}
	for _, index := range []string{"1", "2", "thumbnail"} {
		launch, err := modelruntime.LoadLaunchManifest(
			filepath.Join(output, "stage", "worker-"+index+"-launch.json"),
		)
		if err != nil {
			t.Fatalf("load worker %s launch manifest: %v", index, err)
		}
		wantDigest := "7cba327fcc04f689d72140287a8c206aeb6080a7ddff641bc19efeebff1d537b"
		if index == "thumbnail" {
			wantDigest = "8cba327fcc04f689d72140287a8c206aeb6080a7ddff641bc19efeebff1d537b"
		}
		for _, runtime := range launch.Runtimes {
			if runtime.RuntimeImageDigest != wantDigest {
				t.Fatalf("worker %s runtime image digest = %q", index, runtime.RuntimeImageDigest)
			}
		}
		if index == "thumbnail" && (launch.WorkerRole != "cpu-thumbnail" ||
			len(launch.LocalDevices) != 1 || launch.LocalDevices[0].ResourceClass != "CPU") {
			t.Fatalf("thumbnail launch manifest = %#v", launch)
		}
	}
	thumbnailCertificate := readCertificate(t, filepath.Join(output, "pki", "stage-worker-thumbnail.crt"))
	if len(thumbnailCertificate.URIs) != 1 ||
		thumbnailCertificate.URIs[0].String() != stageWorkerSPIFFEIdentity(thumbnailWorkerMemberID) {
		t.Fatalf("thumbnail worker certificate URIs = %v", thumbnailCertificate.URIs)
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
	var nodeAgents map[string]nodeagent.AgentEndpoint
	decoder := json.NewDecoder(bytes.NewReader(nodeAgentBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&nodeAgents); err != nil {
		t.Fatalf("decode Node Agent endpoints: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("Node Agent endpoints contain trailing content: %v", err)
	}
	endpoint, ok := nodeAgents[worker1Name]
	wantSPIFFEIdentity := "spiffe://vela.internal/node-agent/" +
		base64.RawURLEncoding.EncodeToString([]byte(worker1Name)) + "/" + worker1ID
	if !ok || endpoint.Address != "vela-lab-node-agent.invalid:9444" ||
		endpoint.ServerName != "vela-lab-node-agent.invalid" ||
		endpoint.AgentID.String() != worker1ID || endpoint.AgentEpoch != 1 ||
		endpoint.SPIFFEIdentity != wantSPIFFEIdentity {
		t.Fatalf("Node Agent endpoint = %#v, want SPIFFE identity %q", endpoint, wantSPIFFEIdentity)
	}
	resolver, err := nodeagent.NewStaticAgentResolver(nodeAgents, nodeagent.ClientTLSConfig{
		CertificatePath: "/non-production-lab/client.crt",
		PrivateKeyPath:  "/non-production-lab/client.key",
		RootCAPath:      "/non-production-lab/ca.crt",
	}, "controller/non-production-lab")
	if err != nil {
		t.Fatalf("construct Node Agent resolver from generated endpoints: %v", err)
	}
	if err := resolver.Close(); err != nil {
		t.Fatalf("close Node Agent resolver: %v", err)
	}
}

func TestGenerateCreatesConsumableStageAuthorityKeyrings(t *testing.T) {
	output := filepath.Join(t.TempDir(), "assets")
	if err := generate(options{
		output: output, postgresHost: defaultPostgresHost,
		natsHost: defaultNATSHost, minioHost: defaultMinIOHost,
		runtimeImage: testRuntimeImage, thumbnailRuntimeImage: testThumbnailRuntimeImage,
		validFor: 24 * time.Hour,
	}); err != nil {
		t.Fatalf("generate assets: %v", err)
	}

	signingKeys, err := stageauthority.ReadKeyringFile(filepath.Join(output, "control", "lease.json"))
	if err != nil {
		t.Fatalf("read generated StageAuthority signing keyring: %v", err)
	}
	defer stageauthority.ClearKeyring(signingKeys)
	verifierKeys, err := stageauthority.ReadVerifierKeyringFile(
		filepath.Join(output, "control", "model-runtime-verifier.json"),
	)
	if err != nil {
		t.Fatalf("read generated StageAuthority verifier keyring: %v", err)
	}
	defer stageauthority.ClearKeyring(verifierKeys)
	if len(signingKeys) != 1 || len(verifierKeys) != 1 ||
		signingKeys[labv2contract.StageAuthorityKeyID] == nil ||
		verifierKeys[labv2contract.StageAuthorityKeyID] == nil {
		t.Fatalf("generated StageAuthority key IDs = signing:%v verifier:%v, want only %q",
			keyIDs(signingKeys), keyIDs(verifierKeys), labv2contract.StageAuthorityKeyID)
	}

	now := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	signer, err := stageauthority.NewSigner(signingKeys)
	if err != nil {
		t.Fatalf("construct signer from generated keyring: %v", err)
	}
	verifier, err := stageauthority.NewVerifier(verifierKeys, func() time.Time { return now.Add(time.Second) })
	if err != nil {
		t.Fatalf("construct verifier from generated keyring: %v", err)
	}
	signed, err := signer.Sign(generatedStageAuthority(now))
	if err != nil {
		t.Fatalf("sign StageAuthority with generated keyring: %v", err)
	}
	verified, err := verifier.ValidateEnvelope(signed)
	if err != nil {
		t.Fatalf("verify StageAuthority with generated verifier keyring: %v", err)
	}
	if verified.Authority.GetSigningKeyId() != labv2contract.StageAuthorityKeyID {
		t.Fatalf("verified StageAuthority key ID = %q, want %q",
			verified.Authority.GetSigningKeyId(), labv2contract.StageAuthorityKeyID)
	}
}

func generatedStageAuthority(now time.Time) *velav1.StageAuthority {
	return &velav1.StageAuthority{
		SchemaVersion:       stageauthority.SchemaVersionV1,
		JobId:               "10000000-0000-0000-0000-000000000001",
		AttemptId:           "10000000-0000-0000-0000-000000000002",
		StageRunId:          "10000000-0000-0000-0000-000000000003",
		StageAttemptId:      "10000000-0000-0000-0000-000000000004",
		StageAllocationId:   "10000000-0000-0000-0000-000000000005",
		StageLeaseId:        "10000000-0000-0000-0000-000000000006",
		AttemptFence:        1,
		StageFence:          1,
		StageVersion:        1,
		WorkerInstanceId:    "20000000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch: 1,
		DeviceSetDigest:     bytes.Repeat([]byte{0x11}, sha256.Size),
		Devices: []*velav1.StageAuthorityDeviceEpoch{
			{DeviceId: "30000000-0000-0000-0000-000000000001", DeviceEpoch: 1},
		},
		MembershipDigest: bytes.Repeat([]byte{0x22}, sha256.Size),
		Members: []*velav1.StageAuthorityMemberEpoch{
			{
				WorkerMemberId: "40000000-0000-0000-0000-000000000001",
				MemberEpoch:    1, ModelRuntimeEpoch: 1,
				IdentityDigest: bytes.Repeat([]byte{0x33}, sha256.Size),
			},
		},
		ModelResidencyId:              "50000000-0000-0000-0000-000000000001",
		ModelRuntimeIdentity:          "lab-stage-runtime-v1",
		ModelRuntimeBarrierGeneration: 1,
		StageProfileRevisionId:        "60000000-0000-0000-0000-000000000001",
		CapacityObservationSequence:   1,
		CapacityVector:                map[string]int64{"active_stage_slots": 1},
		LeaseToken:                    bytes.Repeat([]byte{0x44}, sha256.Size),
		ExecutionNonce:                bytes.Repeat([]byte{0x55}, sha256.Size),
		ExecutionSpecDigest:           bytes.Repeat([]byte{0x66}, sha256.Size),
		SigningKeyId:                  labv2contract.StageAuthorityKeyID,
		IssuedAt:                      timestamppb.New(now),
		ExpiresAt:                     timestamppb.New(now.Add(30 * time.Second)),
		MonotonicValidFor:             durationpb.New(30 * time.Second),
	}
}

func keyIDs(keyring map[string][]byte) []string {
	identities := make([]string, 0, len(keyring))
	for identity := range keyring {
		identities = append(identities, identity)
	}
	return identities
}

func TestGenerateRefusesExistingOutput(t *testing.T) {
	output := filepath.Join(t.TempDir(), "assets")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	err := generate(options{
		output: output, postgresHost: defaultPostgresHost,
		natsHost: defaultNATSHost, minioHost: defaultMinIOHost,
		runtimeImage: testRuntimeImage, thumbnailRuntimeImage: testThumbnailRuntimeImage,
		validFor: 24 * time.Hour,
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing output error = %v", err)
	}
}

func TestGenerateRequiresDistinctRuntimeImageDigests(t *testing.T) {
	digest := strings.Split(testRuntimeImage, "@sha256:")[1]
	err := generate(options{
		output: filepath.Join(t.TempDir(), "assets"), postgresHost: defaultPostgresHost,
		natsHost: defaultNATSHost, minioHost: defaultMinIOHost,
		runtimeImage:          testRuntimeImage,
		thumbnailRuntimeImage: "10.1.200.17:5443/vela-lab-next/other@sha256:" + digest,
		validFor:              24 * time.Hour,
	})
	if err == nil || !strings.Contains(err.Error(), "distinct digests") {
		t.Fatalf("same runtime digest error = %v", err)
	}
}

func TestGenerateBindsNATSAssetsToConfiguredKubernetesNamespace(t *testing.T) {
	const namespace = "vela-commercial-rehearsal"
	output := filepath.Join(t.TempDir(), "assets")
	if err := generate(options{
		output: output, postgresHost: "vela-lab-postgres." + namespace + ".svc",
		natsHost:              "vela-lab-nats." + namespace + ".svc",
		minioHost:             "vela-lab-minio." + namespace + ".svc",
		kubernetesNamespace:   namespace,
		runtimeImage:          testRuntimeImage,
		thumbnailRuntimeImage: testThumbnailRuntimeImage,
		validFor:              24 * time.Hour,
	}); err != nil {
		t.Fatalf("generate namespace-scoped assets: %v", err)
	}

	configuration, err := os.ReadFile(filepath.Join(output, "nats", "nats.conf"))
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		route := "nats://vela-lab-nats-" + string(rune('0'+index)) +
			".vela-lab-nats-headless." + namespace + ".svc:6222"
		if !strings.Contains(string(configuration), route) {
			t.Fatalf("NATS configuration is missing namespace-scoped route %q", route)
		}
	}
	if strings.Contains(string(configuration), ".vela-lab.svc:6222") {
		t.Fatal("NATS configuration retained the default namespace route")
	}

	certificate := readCertificate(t, filepath.Join(output, "pki", "nats-server.crt"))
	for _, name := range []string{
		"vela-lab-nats." + namespace + ".svc",
		"vela-lab-nats-headless." + namespace + ".svc",
		"vela-lab-nats-0.vela-lab-nats-headless." + namespace + ".svc",
		"vela-lab-nats-1.vela-lab-nats-headless." + namespace + ".svc",
		"vela-lab-nats-2.vela-lab-nats-headless." + namespace + ".svc",
	} {
		if err := certificate.VerifyHostname(name); err != nil {
			t.Fatalf("NATS certificate does not cover %q: %v", name, err)
		}
	}

	manifestBytes, err := os.ReadFile(filepath.Join(output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		KubernetesNamespace string `json:"kubernetes_namespace"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("decode namespace-scoped asset manifest: %v", err)
	}
	if manifest.KubernetesNamespace != namespace {
		t.Fatalf("asset manifest namespace = %q, want %q", manifest.KubernetesNamespace, namespace)
	}
}

func TestGenerateDerivesDefaultServiceHostsFromKubernetesNamespace(t *testing.T) {
	const namespace = "0-vela-rehearsal"
	output := filepath.Join(t.TempDir(), "assets")
	if err := generate(options{
		output: output, postgresHost: defaultPostgresHost,
		natsHost: defaultNATSHost, minioHost: defaultMinIOHost,
		kubernetesNamespace:   namespace,
		runtimeImage:          testRuntimeImage,
		thumbnailRuntimeImage: testThumbnailRuntimeImage,
		validFor:              24 * time.Hour,
	}); err != nil {
		t.Fatalf("generate namespace-derived service hosts: %v", err)
	}

	manifestBytes, err := os.ReadFile(filepath.Join(output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		PostgresHost string `json:"postgres_host"`
		NATSHost     string `json:"nats_host"`
		MinIOHost    string `json:"minio_host"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("decode namespace-derived asset manifest: %v", err)
	}
	if manifest.PostgresHost != "vela-lab-postgres."+namespace+".svc" ||
		manifest.NATSHost != "vela-lab-nats."+namespace+".svc" ||
		manifest.MinIOHost != "vela-lab-minio."+namespace+".svc" {
		t.Fatalf("namespace-derived service hosts = %#v", manifest)
	}
}

func TestPrivateRegistryImageDigestRejectsMutableOrExternalReferences(t *testing.T) {
	want := "7cba327fcc04f689d72140287a8c206aeb6080a7ddff641bc19efeebff1d537b"
	if got, err := privateRegistryImageDigest(testRuntimeImage); err != nil || got != want {
		t.Fatalf("private Registry image digest = %q error=%v", got, err)
	}
	for _, value := range []string{
		"10.1.200.17:5443/vela/runtime:latest",
		"ghcr.io/vivym/runtime@sha256:" + want,
		"10.1.200.17:5443/vela/../runtime@sha256:" + want,
		"10.1.200.17:5443/vela/runtime@sha256:" + strings.ToUpper(want),
	} {
		if _, err := privateRegistryImageDigest(value); err == nil {
			t.Fatalf("accepted invalid private Registry image %q", value)
		}
	}
}

func TestValidKubernetesNamespaceMatchesDNS1123Label(t *testing.T) {
	tests := map[string]bool{
		"vela-lab":                     true,
		"0":                            true,
		"0-vela":                       true,
		strings.Repeat("a", 63):        true,
		"":                             false,
		"-vela":                        false,
		"vela-":                        false,
		"Vela":                         false,
		"vela.lab":                     false,
		"vela_lab":                     false,
		"vela-" + string(rune(0x6f14)): false,
		strings.Repeat("a", 64):        false,
	}
	for value, want := range tests {
		if got := validKubernetesNamespace(value); got != want {
			t.Errorf("validKubernetesNamespace(%q) = %t, want %t", value, got, want)
		}
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
