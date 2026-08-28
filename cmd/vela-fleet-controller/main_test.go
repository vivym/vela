package main

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestLoadConfigRequiresExactFleetRuntimeInputs(t *testing.T) {
	for _, name := range fleetEnvironmentNames() {
		t.Setenv(name, "")
	}
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_FLEET_NAMESPACE") {
		t.Fatalf("missing Fleet namespace error = %v", err)
	}

	directory := t.TempDir()
	desiredPath := filepath.Join(directory, "desired.yaml")
	if err := os.WriteFile(desiredPath, []byte(validDesiredInputWithRetirement), 0o600); err != nil {
		t.Fatalf("write Fleet desired input: %v", err)
	}
	values := map[string]string{
		"VELA_FLEET_NAMESPACE":                  "vela-system",
		"VELA_FLEET_DESIRED_INPUT_FILE":         desiredPath,
		"VELA_FLEET_MAINTENANCE_ADDRESS":        "vela-control.vela-system.svc:8444",
		"VELA_FLEET_MAINTENANCE_SERVER_NAME":    "vela-control.vela-system.svc",
		"VELA_FLEET_TLS_CERT_FILE":              filepath.Join(directory, "tls.crt"),
		"VELA_FLEET_TLS_KEY_FILE":               filepath.Join(directory, "tls.key"),
		"VELA_FLEET_CA_FILE":                    filepath.Join(directory, "ca.crt"),
		"VELA_FLEET_ADMISSION_ADDRESS":          ":9443",
		"VELA_FLEET_ADMISSION_TLS_CERT_FILE":    filepath.Join(directory, "admission.crt"),
		"VELA_FLEET_ADMISSION_TLS_KEY_FILE":     filepath.Join(directory, "admission.key"),
		"VELA_FLEET_ADMISSION_CLIENT_CA_FILE":   filepath.Join(directory, "admission-client-ca.crt"),
		"VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID": "spiffe://vela.internal/kube-apiserver/admission",
		"VELA_FLEET_KUBERNETES_USERNAME":        "system:serviceaccount:vela-system:vela-fleet-controller",
		"VELA_FLEET_POD_CONTROLLER_USERNAME":    "system:kube-controller-manager",
		"VELA_FLEET_POLL_INTERVAL":              "2s",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load Fleet controller config: %v", err)
	}
	if configuration.namespace != "vela-system" ||
		configuration.desiredInputFile != desiredPath ||
		configuration.maintenanceAddress != "vela-control.vela-system.svc:8444" ||
		configuration.admissionClientSPIFFEIdentity !=
			"spiffe://vela.internal/kube-apiserver/admission" ||
		configuration.pollInterval != 2*time.Second ||
		len(configuration.desiredRevisions) != 1 ||
		configuration.desiredRevisions[0].Namespace != "vela-system" ||
		configuration.desiredRevisions[0].Name != "h3-worker-pool-primary" ||
		configuration.desiredRevisions[0].ExecutionProfileRevisionID.String() !=
			"23000000-0000-0000-0000-000000000043" ||
		configuration.desiredRevisions[0].InferenceBackendRevision != "sglang-h3-v3" ||
		len(configuration.desiredRevisions[0].Placements) != 1 ||
		configuration.desiredRevisions[0].Placements[0].NodeIdentity != "h3-node-01" ||
		configuration.desiredRevisions[0].ReadinessTimeout != 30*time.Minute ||
		configuration.desiredRevisions[0].CapacityPolicy.WorkerHighWatermarkBytes != 800 ||
		configuration.desiredRevisions[0].CapacityPolicy.WorkerLowWatermarkBytes != 400 ||
		configuration.desiredRevisions[0].CapacityPolicy.WorkerCriticalFreeBytes != 100 ||
		configuration.desiredRevisions[0].CapacityPolicy.PoolHighWatermarkBytes != 5600 ||
		configuration.desiredRevisions[0].CapacityPolicy.PoolLowWatermarkBytes != 2800 ||
		configuration.desiredRevisions[0].CapacityPolicy.ObservationMaxAge != 2*time.Minute ||
		len(configuration.retirementPlans) != 1 ||
		configuration.retirementPlans[0].Revision !=
			"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" ||
		configuration.retirementPlans[0].WorkerPoolID.String() !=
			"23000000-0000-0000-0000-000000000085" ||
		configuration.retirementPlans[0].WorkerPoolKubernetesUID !=
			"kubernetes-old-worker-pool-uid" ||
		len(configuration.retirementPlans[0].Placements) != 1 ||
		configuration.retirementPlans[0].Placements[0].DaemonSetKubernetesUID !=
			"kubernetes-old-daemonset-uid" ||
		configuration.retirementPlans[0].Reason != "retire immutable H3 revision eeee" ||
		!configuration.retirementPlans[0].Deadline.Equal(
			time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		) || len(configuration.retirementPlans[0].Placements[0].Workers) != 1 ||
		configuration.retirementPlans[0].Placements[0].Workers[0].OperationID.String() !=
			"23000000-0000-0000-0000-000000000081" ||
		configuration.retirementPlans[0].Placements[0].Workers[0].WorkerEpoch != 8 ||
		configuration.retirementPlans[0].Placements[0].Workers[0].PodKubernetesUID !=
			"kubernetes-old-worker-pod-uid-1" {
		t.Fatalf("Fleet controller config = %#v", configuration)
	}
}

func TestLoadDesiredInputRejectsUnknownOrUnpinnedConfiguration(t *testing.T) {
	directory := t.TempDir()
	for _, test := range []struct {
		name    string
		content string
	}{
		{
			name: "unknown field",
			content: strings.Replace(
				validDesiredInput,
				"kind: FleetDesiredRevisions",
				"kind: FleetDesiredRevisions\nunknown: value",
				1,
			),
		},
		{
			name: "unpinned image",
			content: strings.Replace(
				validDesiredInput,
				"ghcr.io/vivym/vela-worker-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"ghcr.io/vivym/vela-worker-agent:latest",
				1,
			),
		},
		{
			name: "WorkerPool name violates CRD DNS contract",
			content: strings.Replace(
				validDesiredInput,
				"name: h3-worker-pool-primary",
				"name: a..b",
				1,
			),
		},
		{
			name: "readiness timeout exceeds bound",
			content: strings.Replace(
				validDesiredInput,
				"readinessTimeout: 30m",
				"readinessTimeout: 2h1s",
				1,
			),
		},
		{
			name: "worker capacity low watermark reaches high watermark",
			content: strings.Replace(
				validDesiredInput,
				"workerLowWatermarkBytes: 400",
				"workerLowWatermarkBytes: 800",
				1,
			),
		},
		{
			name: "capacity observation age below authority bound",
			content: strings.Replace(
				validDesiredInput,
				"observationMaxAge: 2m",
				"observationMaxAge: 9s",
				1,
			),
		},
		{
			name: "unknown retirement field",
			content: strings.Replace(
				validDesiredInputWithRetirement,
				"    reason: retire immutable H3 revision eeee",
				"    reason: retire immutable H3 revision eeee\n    unknown: value",
				1,
			),
		},
		{
			name: "retirement deadline is not RFC3339",
			content: strings.Replace(
				validDesiredInputWithRetirement,
				`deadline: "2026-08-27T12:00:00Z"`,
				`deadline: "2026-08-27 12:00:00"`,
				1,
			),
		},
		{
			name: "desired and retirement WorkerPool overlap",
			content: strings.Replace(
				validDesiredInputWithRetirement,
				"workerPoolID: 23000000-0000-0000-0000-000000000085",
				"workerPoolID: 00000000-0000-0000-0000-000000000005",
				1,
			),
		},
		{
			name: "retirement reuses DrainOperation id",
			content: validDesiredInputWithRetirement + `  - revision: ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
    workerPoolID: 23000000-0000-0000-0000-000000000095
    workerPoolName: h3-worker-pool-other
    workerPoolKubernetesUID: kubernetes-other-worker-pool-uid
    daemonSetName: h3-worker-pool-other
    daemonSetKubernetesUID: kubernetes-other-daemonset-uid
    reason: reject reused DrainOperation identity
    deadline: "2026-08-27T13:00:00Z"
    workers:
      - operationID: 23000000-0000-0000-0000-000000000081
        workerID: 23000000-0000-0000-0000-000000000096
        workerEpoch: 9
        podName: h3-worker-other-node-1
        podKubernetesUID: kubernetes-other-worker-pod-uid-1
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(directory, strings.ReplaceAll(test.name, " ", "-")+".yaml")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatalf("write invalid Fleet desired input: %v", err)
			}
			if _, err := loadDesiredRevisions(path, "vela-system"); err == nil {
				t.Fatal("invalid Fleet desired input was accepted")
			}
		})
	}
}

func fleetEnvironmentNames() []string {
	return []string{
		"VELA_FLEET_NAMESPACE",
		"VELA_FLEET_DESIRED_INPUT_FILE",
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
		"VELA_FLEET_POD_CONTROLLER_USERNAME",
		"VELA_FLEET_POLL_INTERVAL",
	}
}

const validDesiredInput = `apiVersion: fleet.vela.ai/v1alpha1
kind: FleetDesiredRevisions
revisions:
  - workerPoolID: 00000000-0000-0000-0000-000000000005
    name: h3-worker-pool-primary
    revision: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    workerProfile: h3
    nodeSelector:
      vela.ai/worker-profile: h3
      vela.ai/worker-pool: launch
    initImage: docker.io/library/busybox@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
    workerAgentImage: ghcr.io/vivym/vela-worker-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    runnerImage: ghcr.io/vivym/vela-h3-runner@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
    artifactStoreTLSSecret: vela-artifact-store-ca-a1
    executionProfileRevisionID: 23000000-0000-0000-0000-000000000043
    inferenceBackendRevision: sglang-h3-v3
    readinessTimeout: 30m
    capacityPolicy:
      workerHighWatermarkBytes: 800
      workerLowWatermarkBytes: 400
      workerCriticalFreeBytes: 100
      poolHighWatermarkBytes: 5600
      poolLowWatermarkBytes: 2800
      observationMaxAge: 2m
    placements:
      - nodeIdentity: h3-node-01
        daemonSetName: h3-worker-pool-primary-node-01
        workerRuntimeConfigMap: vela-worker-runtime-a1
        runnerProfilesConfigMap: vela-runner-profiles-a1
        runnerGPURolesConfigMap: vela-runner-gpu-roles-a1
        workerControlTLSSecret: vela-worker-control-mtls-a1
`

const validDesiredInputWithRetirement = validDesiredInput + `retirements:
  - revision: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    workerPoolID: 23000000-0000-0000-0000-000000000085
    workerPoolName: h3-worker-pool-old
    workerPoolKubernetesUID: kubernetes-old-worker-pool-uid
    reason: retire immutable H3 revision eeee
    deadline: "2026-08-27T12:00:00Z"
    placements:
      - daemonSetName: h3-worker-pool-old
        daemonSetKubernetesUID: kubernetes-old-daemonset-uid
        workers:
          - operationID: 23000000-0000-0000-0000-000000000081
            workerID: 23000000-0000-0000-0000-000000000082
            workerEpoch: 8
            podName: h3-worker-old-node-1
            podKubernetesUID: kubernetes-old-worker-pod-uid-1
`
