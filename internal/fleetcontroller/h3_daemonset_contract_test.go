package fleetcontroller

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

func TestH3WorkerPodTemplateExactlyMatchesStaticDeploymentContract(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve H3 deployment contract test path")
	}
	payload, err := os.ReadFile(filepath.Join(
		filepath.Dir(currentFile), "..", "..", "deploy", "worker-agent", "daemonset.yaml",
	))
	if err != nil {
		t.Fatalf("read static H3 DaemonSet contract: %v", err)
	}
	var static appsv1.DaemonSet
	if err := yaml.UnmarshalStrict(payload, &static); err != nil {
		t.Fatalf("parse static H3 DaemonSet contract: %v", err)
	}
	desired := DesiredRevision{
		WorkerPoolID: uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		Revision:     "0000000000000000000000000000000000000000000000000000000000000000",
		NodeSelector: map[string]string{
			"vela.ai/worker-profile": "h3",
			"vela.ai/worker-pool":    "launch",
		},
		InitImage:                "docker.io/library/busybox@sha256:0000000000000000000000000000000000000000000000000000000000000000",
		WorkerAgentImage:         "ghcr.io/vivym/vela-worker-agent@sha256:0000000000000000000000000000000000000000000000000000000000000000",
		RunnerImage:              "ghcr.io/vivym/vela-h3-runner@sha256:0000000000000000000000000000000000000000000000000000000000000000",
		WorkerRuntimeConfigMap:   "vela-worker-runtime",
		RunnerProfilesConfigMap:  "vela-runner-profiles",
		RunnerGPURolesConfigMap:  "vela-runner-gpu-roles",
		WorkerControlTLSSecret:   "vela-worker-control-mtls",
		ArtifactStoreTLSSecret:   "vela-artifact-store-ca",
		InferenceBackendRevision: "replace-with-approved-backend-revision",
		CapacityPolicy: CapacityPolicySpec{
			ObservationMaxAge: 2 * time.Minute,
		},
	}
	if err := ValidateDesiredRevision(desired); err == nil {
		t.Fatal("static H3 deployment base must remain an explicitly invalid template")
	}
	materialized := h3WorkerPodTemplate(desired, map[string]string{
		"app.kubernetes.io/name": "vela-h3-worker",
	})
	if !reflect.DeepEqual(static.Spec.Template, materialized) {
		t.Fatalf(
			"static H3 PodTemplate drifted from Fleet materialization\nstatic: %#v\nmaterialized: %#v",
			static.Spec.Template,
			materialized,
		)
	}
}
