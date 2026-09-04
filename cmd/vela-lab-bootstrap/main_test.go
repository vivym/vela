package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testRuntimeDigest          = "sha256:7cba327fcc04f689d72140287a8c206aeb6080a7ddff641bc19efeebff1d537b"
	testThumbnailRuntimeDigest = "sha256:8cba327fcc04f689d72140287a8c206aeb6080a7ddff641bc19efeebff1d537b"
)

func TestLoadConfigurationRequiresEverySeparatedDatabaseURL(t *testing.T) {
	setValidEnvironment(t)
	_ = os.Unsetenv("VELA_FLEET_DATABASE_URL")
	_, err := loadConfiguration(validOptions(t))
	if err == nil || !strings.Contains(err.Error(), "VELA_FLEET_DATABASE_URL is required") {
		t.Fatalf("missing Fleet database error = %v", err)
	}
}

func TestLoadConfigurationAcceptsGeneratedShape(t *testing.T) {
	setValidEnvironment(t)
	configuration, err := loadConfiguration(validOptions(t))
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}
	if len(configuration.databaseURLs) != len(requiredDatabaseEnvironments) ||
		len(configuration.loginPasswords) != len(requiredDatabaseEnvironments) ||
		len(configuration.credentialPepper) != 32 ||
		configuration.runtimeImageDigest != testRuntimeDigest ||
		configuration.thumbnailRuntimeImageDigest != testThumbnailRuntimeDigest {
		t.Fatalf("configuration is incomplete: %#v", configuration)
	}
}

func TestLoadConfigurationRequiresDistinctRuntimeImageDigests(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("VELA_LAB_THUMBNAIL_RUNTIME_IMAGE_DIGEST", testRuntimeDigest)
	if _, err := loadConfiguration(validOptions(t)); err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("same runtime digest error = %v", err)
	}
}

func TestLoadConfigurationRequiresHTTPSMinIOWithRootCA(t *testing.T) {
	setValidEnvironment(t)
	options := validOptions(t)
	t.Setenv("VELA_LAB_MINIO_ENDPOINT", "http://minio:9000")
	if _, err := loadConfiguration(options); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("HTTP MinIO endpoint error = %v", err)
	}
	t.Setenv("VELA_LAB_MINIO_ENDPOINT", "https://minio:9000")
	options.minioRootCA = "relative/ca.crt"
	if _, err := loadConfiguration(options); err == nil || !strings.Contains(err.Error(), "MinIO Root CA path") {
		t.Fatalf("relative MinIO CA error = %v", err)
	}
}

func TestLabStageAndWorkerFixturesBindTheirImmutableRuntimeDigests(t *testing.T) {
	profiles, err := labStageProfiles(testRuntimeDigest, testThumbnailRuntimeDigest)
	if err != nil {
		t.Fatalf("build StageProfile fixtures: %v", err)
	}
	workers, err := labWorkerFixtures(testRuntimeDigest, testThumbnailRuntimeDigest)
	if err != nil {
		t.Fatalf("build WorkerInstance fixtures: %v", err)
	}
	if len(profiles) != 4 || len(workers) != 3 {
		t.Fatalf("fixture counts profiles=%d workers=%d", len(profiles), len(workers))
	}
	wantProfiles := map[string]string{
		encoderStageProfileID:   auxWorkerProfileID,
		vaeStageProfileID:       auxWorkerProfileID,
		ditStageProfileID:       ditWorkerProfileID,
		thumbnailStageProfileID: thumbnailWorkerProfileID,
	}
	seenDevices := make(map[string]struct{}, len(workers))
	seenResidencies := make(map[string]struct{}, 4)
	for _, profile := range profiles {
		wantDigest := testRuntimeDigest
		if profile.id == thumbnailStageProfileID {
			wantDigest = testThumbnailRuntimeDigest
		}
		if profile.runtimeImageDigest != wantDigest ||
			wantProfiles[profile.id] != profile.workerProfileID {
			t.Fatalf("StageProfile fixture = %#v", profile)
		}
	}
	for _, worker := range workers {
		if _, duplicate := seenDevices[worker.deviceID]; duplicate {
			t.Fatalf("synthetic Device %q is shared", worker.deviceID)
		}
		seenDevices[worker.deviceID] = struct{}{}
		if len(worker.routes) != len(worker.residencies) || len(worker.routes) == 0 {
			t.Fatalf("WorkerInstance routes/residencies = %#v", worker)
		}
		for index, residency := range worker.residencies {
			wantDigest := testRuntimeDigest
			if worker.id == thumbnailWorkerID {
				wantDigest = testThumbnailRuntimeDigest
			}
			if residency.runtimeImageDigest != wantDigest ||
				worker.routes[index].modelResidencyID != residency.id ||
				worker.routes[index].stageProfileID != residency.stageProfileID {
				t.Fatalf("runtime route mismatch worker=%s route=%#v residency=%#v", worker.id, worker.routes[index], residency)
			}
			if _, duplicate := seenResidencies[residency.id]; duplicate {
				t.Fatalf("ModelResidency %q is shared", residency.id)
			}
			seenResidencies[residency.id] = struct{}{}
		}
	}
	if len(workers[0].routes) != 2 || len(workers[1].routes) != 1 || len(workers[2].routes) != 1 ||
		workers[2].resourceClass != "CPU" {
		t.Fatalf("AUX/DiT/thumbnail fixtures = %#v", workers)
	}
}

func TestBuildWorkerEvidenceMatchesGeneratedLaunchDigests(t *testing.T) {
	workers, err := labWorkerFixtures(testRuntimeDigest, testThumbnailRuntimeDigest)
	if err != nil {
		t.Fatal(err)
	}
	wantCanaryDigests := map[string]string{
		encoderResidencyID:   "afa26c9c16640cb300f23244fdab78e34e85d770dbe605d53e8b40e3cf8989ce",
		vaeResidencyID:       "cb507a103033c86bd577a0b26173dd7d67565b7cb5bd46ac69cbc57f5c53db8a",
		ditResidencyID:       "1ed06e1ebb176adbbb56f72e3009d378b6078dad780461ac42ba3871fcc222f7",
		thumbnailResidencyID: "8fc0748fd6cc22708c520096f47f051b468717daf2e9028dea164ebe4b3fb633",
	}
	for _, worker := range workers {
		observedAt := time.Date(2026, 9, 4, 1, 2, 3, 0, time.UTC)
		encoded, err := buildWorkerEvidence(worker, 7, observedAt)
		if err != nil {
			t.Fatalf("build %s evidence: %v", worker.id, err)
		}
		var evidence struct {
			WorkerInstanceID string `json:"worker_instance_id"`
			DeviceSet        struct {
				MembershipDigest string `json:"membership_digest"`
				TopologyDigest   string `json:"topology_digest"`
				Devices          []struct {
					ID      string `json:"id"`
					Kind    string `json:"kind"`
					GPUUUID string `json:"gpu_uuid"`
					PCIBDF  string `json:"pci_bdf"`
				} `json:"devices"`
			} `json:"device_set"`
			Residencies []struct {
				ID                   string `json:"id"`
				RuntimeImageDigest   string `json:"runtime_image_digest"`
				CanaryEvidenceDigest string `json:"canary_evidence_digest"`
			} `json:"residencies"`
			Capacity struct {
				Sequence int64 `json:"sequence"`
			} `json:"capacity"`
		}
		if err := json.Unmarshal(encoded, &evidence); err != nil {
			t.Fatalf("decode %s evidence: %v", worker.id, err)
		}
		membership := sha256.Sum256([]byte("vela/lab-v2/" + worker.name + "/membership/v1"))
		topology := sha256.Sum256([]byte("vela/lab-v2/" + worker.name + "/topology/v1"))
		if evidence.WorkerInstanceID != worker.id || evidence.Capacity.Sequence != 7 ||
			evidence.DeviceSet.MembershipDigest != hex.EncodeToString(membership[:]) ||
			evidence.DeviceSet.TopologyDigest != hex.EncodeToString(topology[:]) ||
			len(evidence.DeviceSet.Devices) != 1 || evidence.DeviceSet.Devices[0].ID != worker.deviceID {
			t.Fatalf("WorkerInstance evidence identity mismatch: %#v", evidence)
		}
		device := evidence.DeviceSet.Devices[0]
		if device.Kind != worker.resourceClass ||
			(worker.resourceClass == "CPU" && (device.GPUUUID != "" || device.PCIBDF != "")) {
			t.Fatalf("WorkerInstance device class mismatch: %#v", device)
		}
		for _, residency := range evidence.Residencies {
			wantDigest := testRuntimeDigest
			if worker.id == thumbnailWorkerID {
				wantDigest = testThumbnailRuntimeDigest
			}
			if residency.RuntimeImageDigest != wantDigest {
				t.Fatalf("ModelResidency %s digest = %q", residency.ID, residency.RuntimeImageDigest)
			}
			if residency.CanaryEvidenceDigest != wantCanaryDigests[residency.ID] {
				t.Fatalf(
					"ModelResidency %s canary digest = %q, want live mock aggregate %q",
					residency.ID, residency.CanaryEvidenceDigest, wantCanaryDigests[residency.ID],
				)
			}
		}
	}
}

func TestRequiredControlPrincipalFixturesAreDeterministicAndDistinct(t *testing.T) {
	seenIDs := make(map[string]struct{}, len(requiredControlPrincipals))
	seenRoles := make(map[string]struct{}, len(requiredControlPrincipals))
	for _, fixture := range requiredControlPrincipals {
		if _, duplicate := seenIDs[fixture.principalID]; duplicate {
			t.Fatalf("duplicate control Principal ID %q", fixture.principalID)
		}
		seenIDs[fixture.principalID] = struct{}{}
		if _, duplicate := seenRoles[fixture.databaseRole]; duplicate {
			t.Fatalf("duplicate control database role %q", fixture.databaseRole)
		}
		seenRoles[fixture.databaseRole] = struct{}{}
		identity, err := url.Parse(fixture.tlsURI)
		if err != nil || identity.Scheme != "spiffe" || identity.Host != "vela.internal" ||
			identity.RawQuery != "" || identity.Fragment != "" {
			t.Fatalf("%s SPIFFE identity %q is invalid", fixture.name, fixture.tlsURI)
		}
		if !identifierPattern.MatchString(fixture.databaseRole) ||
			!strings.HasSuffix(fixture.databaseRole, "_login") ||
			!identifierPattern.MatchString(fixture.principalTable) ||
			!identifierPattern.MatchString(fixture.bindingTable) {
			t.Fatalf("%s fixture identifiers are invalid: %#v", fixture.name, fixture)
		}
	}
	if len(requiredControlPrincipals) != 2 {
		t.Fatalf("required control Principal count = %d, want 2", len(requiredControlPrincipals))
	}
}

func setValidEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("VELA_LAB_POSTGRES_ADMIN_URL", "postgresql://postgres:secret@postgres:5432/vela?sslmode=disable")
	t.Setenv("VELA_LAB_MINIO_ENDPOINT", "https://minio:9000")
	t.Setenv("VELA_LAB_MINIO_ACCESS_KEY", "velalab")
	t.Setenv("VELA_LAB_MINIO_SECRET_KEY", "secret")
	t.Setenv("VELA_LAB_NATS_URL", "tls://nats:4222")
	t.Setenv("VELA_LAB_RUNTIME_IMAGE_DIGEST", testRuntimeDigest)
	t.Setenv("VELA_LAB_THUMBNAIL_RUNTIME_IMAGE_DIGEST", testThumbnailRuntimeDigest)
	t.Setenv("VELA_CREDENTIAL_PEPPER_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	passwords := make(map[string]string, len(requiredDatabaseEnvironments))
	for index, name := range requiredDatabaseEnvironments {
		login := "vela_test_" + strings.ToLower(strings.TrimPrefix(name, "VELA_")) + "_login"
		login = strings.ReplaceAll(login, "_database_url", "")
		if index == 0 {
			login = "vela_artifact_replication_login"
		}
		passwords[login] = strings.Repeat("a", 48)
		t.Setenv(name, "postgresql://"+login+":"+strings.Repeat("a", 48)+"@postgres:5432/vela?sslmode=disable")
	}
	encoded, err := json.Marshal(passwords)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("VELA_LAB_DATABASE_LOGIN_PASSWORDS", string(encoded))
}

func validOptions(t *testing.T) options {
	t.Helper()
	root := t.TempDir()
	paths := []string{"nats.creds", "ca.crt", "minio-ca.crt", "tls.crt", "tls.key", "smoke-secret"}
	for _, name := range paths {
		if err := os.WriteFile(filepath.Join(root, name), []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return options{
		databaseRoot: root, natsCredential: filepath.Join(root, "nats.creds"),
		natsRootCA: filepath.Join(root, "ca.crt"), natsCert: filepath.Join(root, "tls.crt"),
		natsKey: filepath.Join(root, "tls.key"), smokeSecret: filepath.Join(root, "smoke-secret"),
		minioRootCA: filepath.Join(root, "minio-ca.crt"),
		timeout:     time.Minute,
	}
}
