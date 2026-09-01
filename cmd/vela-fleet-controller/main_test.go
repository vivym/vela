package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
)

type readinessProbe func() bool

func (probe readinessProbe) Ready() bool { return probe() }

func TestReadinessEndpointTracksMostRecentCompleteFleetCycle(t *testing.T) {
	ready := false
	handler := newFleetHTTPHandler(http.NotFoundHandler(), readinessProbe(func() bool {
		return ready
	}), "spiffe://vela.internal/kube-apiserver/admission")
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial Fleet readiness status = %d", response.Code)
	}

	ready = true
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("successful-cycle Fleet readiness status = %d", response.Code)
	}
}

func TestValidationEndpointRequiresVerifiedKubeAPIServerSPIFFEIdentity(t *testing.T) {
	const expectedIdentity = "spiffe://vela.internal/kube-apiserver/admission"
	validationCalls := 0
	handler := newFleetHTTPHandler(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		validationCalls++
		writer.WriteHeader(http.StatusNoContent)
	}), readinessProbe(func() bool { return true }), expectedIdentity)

	for _, test := range []struct {
		name       string
		identity   string
		verified   bool
		wantStatus int
	}{
		{name: "missing certificate", wantStatus: http.StatusUnauthorized},
		{
			name: "unverified certificate", identity: expectedIdentity,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:     "wrong verified identity",
			identity: "spiffe://vela.internal/kube-apiserver/other", verified: true,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "exact verified identity", identity: expectedIdentity, verified: true,
			wantStatus: http.StatusNoContent,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/validate", strings.NewReader("{}"))
			if test.identity != "" {
				identity, err := url.Parse(test.identity)
				if err != nil {
					t.Fatalf("parse test SPIFFE identity: %v", err)
				}
				certificate := &x509.Certificate{Raw: []byte(test.identity), URIs: []*url.URL{identity}}
				request.TLS = &tls.ConnectionState{
					HandshakeComplete: true,
					PeerCertificates:  []*x509.Certificate{certificate},
				}
				if test.verified {
					request.TLS.VerifiedChains = [][]*x509.Certificate{{certificate}}
				}
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("validation status = %d, want %d", response.Code, test.wantStatus)
			}
		})
	}
	if validationCalls != 1 {
		t.Fatalf("authenticated validation calls = %d, want 1", validationCalls)
	}
}

func TestLoadConfigSupportsResidencyPlanOnlyRuntime(t *testing.T) {
	for _, name := range fleetEnvironmentNames() {
		t.Setenv(name, "")
	}
	directory := t.TempDir()
	rolloutPath := filepath.Join(directory, "residency-plan-rollouts.json")
	encoded, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"rollouts":       []fleetcontroller.ResidencyPlanRollout{singleGPUResidencyPlanRollout(t)},
	})
	if err != nil {
		t.Fatalf("encode target-only ResidencyPlan rollout: %v", err)
	}
	if err := os.WriteFile(rolloutPath, encoded, 0o600); err != nil {
		t.Fatalf("write target-only ResidencyPlan rollout: %v", err)
	}
	values := map[string]string{
		"VELA_FLEET_NAMESPACE":                    "vela-system",
		"VELA_FLEET_RESIDENCY_PLAN_ROLLOUTS_FILE": rolloutPath,
		"VELA_FLEET_MAINTENANCE_ADDRESS":          "vela-control.vela-system.svc:8444",
		"VELA_FLEET_MAINTENANCE_SERVER_NAME":      "vela-control.vela-system.svc",
		"VELA_FLEET_TLS_CERT_FILE":                filepath.Join(directory, "tls.crt"),
		"VELA_FLEET_TLS_KEY_FILE":                 filepath.Join(directory, "tls.key"),
		"VELA_FLEET_CA_FILE":                      filepath.Join(directory, "ca.crt"),
		"VELA_FLEET_ADMISSION_ADDRESS":            ":9443",
		"VELA_FLEET_ADMISSION_TLS_CERT_FILE":      filepath.Join(directory, "admission.crt"),
		"VELA_FLEET_ADMISSION_TLS_KEY_FILE":       filepath.Join(directory, "admission.key"),
		"VELA_FLEET_ADMISSION_CLIENT_CA_FILE":     filepath.Join(directory, "admission-client-ca.crt"),
		"VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID":   "spiffe://vela.internal/kube-apiserver/admission",
		"VELA_FLEET_KUBERNETES_USERNAME":          "system:serviceaccount:vela-system:vela-fleet-controller",
		"VELA_FLEET_POLL_INTERVAL":                "2s",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load target-only Fleet controller config: %v", err)
	}
	if len(configuration.residencyPlanRollouts) != 1 {
		t.Fatalf("target-only Fleet controller config = %#v", configuration)
	}
}

func singleGPUResidencyPlanRollout(t *testing.T) fleetcontroller.ResidencyPlanRollout {
	t.Helper()
	planID := uuid.MustParse("49310000-0000-0000-0000-000000000001")
	bundleID := uuid.MustParse("49310000-0000-0000-0000-000000000002")
	poolID := uuid.MustParse("49310000-0000-0000-0000-000000000003")
	workerID := uuid.MustParse("49310000-0000-0000-0000-000000000004")
	profileID := uuid.MustParse("49310000-0000-0000-0000-000000000005")
	bundle := fleetcontroller.WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: planID, WorkerBundleID: bundleID,
		Namespace: "vela-system", InitImage: pinnedTestImage("busybox", 'a'),
		StageWorkerAgentImage:          pinnedTestImage("vela-stage-worker-agent", 'b'),
		RuntimeImage:                   pinnedTestImage("vela-h3-stage-runtime", 'c'),
		StageWorkerConfigMap:           "stage-worker-runtime-r1",
		ModelRuntimeVerifierConfigMap:  "model-runtime-verifier-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-tls-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "artifact-store-credentials-r1",
		ArtifactStoreCASecret:          "artifact-store-ca-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: profileID,
			CapacityPoolID: poolID, Role: "dit", CapacitySlots: 1,
			DeviceSetDigest: testDigest('1'), MembershipDigest: testDigest('2'),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				ModelResidencyID:       uuid.MustParse("49310000-0000-0000-0000-000000000009"),
				StageProfileRevisionID: uuid.MustParse("49310000-0000-0000-0000-000000000007"),
				ModelRuntimeEpochFloor: 1,
				Component:              "DIT", ModelComponentRevision: "h3-dit-v1",
				RuntimeIdentity: "h3-dit-runtime-v1", Command: []string{"/opt/vela/bin/h3-dit"},
				InitializationTimeout: "2h", ShutdownTimeout: "2m",
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID: uuid.MustParse("49310000-0000-0000-0000-000000000006"), MemberEpoch: 11,
				Key: "member-0", NodeIdentity: "h3-node-01", ResourceClass: "GPU", DeviceCount: 1,
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{
					DeviceID: uuid.MustParse("49310000-0000-0000-0000-000000000008"), DeviceEpoch: 3,
					GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
				}},
			}},
		}},
	}
	var err error
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatalf("digest target-only WorkerBundle: %v", err)
	}
	return fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: fleet.ApprovedResidencyPlan{
			SchemaVersion: 1, ID: planID, StableID: "h3-target-only", Revision: 1,
			ContentDigest: bundle.RevisionDigest, ApprovalEvidenceDigest: testDigest('d'),
			ApprovedAt: time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC), ApprovedBy: "fleet/operator-1",
			CapacityPools: []fleet.PlannedCapacityPool{{
				ID: poolID, StableID: "h3-dit", StageProfileRevisionID: uuid.MustParse("49310000-0000-0000-0000-000000000007"),
				ResourceClass: "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai", MaxReadyQueueDepth: 128,
			}},
			WorkerBundles: []fleet.PlannedWorkerBundle{{
				ID: bundleID, StableID: "h3-node-01", DesiredGeneration: 1, LayoutDigest: bundle.RevisionDigest,
			}},
			WorkerInstances: []fleet.PlannedWorkerInstance{{
				ID: workerID, WorkerProfileRevisionID: profileID, CapacityPoolID: poolID,
				WorkerBundleID: bundleID, DesiredMemberCount: 1, DesiredDeviceCount: 1,
			}},
		},
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{bundle},
	}
}

func pinnedTestImage(name string, digit rune) string {
	return "ghcr.io/vivym/" + name + "@sha256:" + testDigest(digit)
}

func testDigest(digit rune) string {
	return strings.Repeat(string(digit), 64)
}

func fleetEnvironmentNames() []string {
	return []string{
		"VELA_FLEET_NAMESPACE",
		"VELA_FLEET_RESIDENCY_PLAN_ROLLOUTS_FILE",
		"VELA_FLEET_MAINTENANCE_ADDRESS",
		"VELA_FLEET_MAINTENANCE_SERVER_NAME",
		"VELA_FLEET_TLS_CERT_FILE",
		"VELA_FLEET_TLS_KEY_FILE",
		"VELA_FLEET_CA_FILE",
		"VELA_FLEET_ADMISSION_ADDRESS",
		"VELA_FLEET_ADMISSION_TLS_CERT_FILE",
		"VELA_FLEET_ADMISSION_TLS_KEY_FILE",
		"VELA_FLEET_ADMISSION_CLIENT_CA_FILE",
		"VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID",
		"VELA_FLEET_KUBERNETES_USERNAME",
		"VELA_FLEET_POLL_INTERVAL",
	}
}
