//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/releasebundle"
)

type catalogReleaseResource struct {
	apiVersion string
	kind       string
	namespace  string
	name       string
}

var catalogReleaseInventory = map[string][]catalogReleaseResource{
	"control-storage": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "nats-config"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-barman-cloud-plugin-contract-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-jetstream-contract-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-recovery-contract"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "nats"},
		{apiVersion: "apps/v1", kind: "StatefulSet", namespace: "vela-system", name: "nats"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "nats"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "vela-postgres"},
		{apiVersion: "barmancloud.cnpg.io/v1", kind: "ObjectStore", namespace: "vela-system", name: "vela-postgres-backup"},
		{apiVersion: "postgresql.cnpg.io/v1", kind: "Cluster", namespace: "vela-system", name: "vela-postgres"},
		{apiVersion: "postgresql.cnpg.io/v1", kind: "ScheduledBackup", namespace: "vela-system", name: "vela-postgres-daily"},
	},
	"fleet-controller": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "apiextensions.k8s.io/v1", kind: "CustomResourceDefinition", name: "workerpools.fleet.vela.ai"},
		{apiVersion: "v1", kind: "ServiceAccount", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", name: "vela-fleet-controller-node-reader"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "RoleBinding", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRoleBinding", name: "vela-fleet-controller-node-reader"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-fleet-desired-r1"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-fleet-admission"},
		{apiVersion: "apps/v1", kind: "Deployment", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "admissionregistration.k8s.io/v1", kind: "ValidatingWebhookConfiguration", name: "vela-fleet-protection"},
	},
	"observability": {
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-observability", name: "vela-slo-alert-rules-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-observability", name: "vela-slo-contract-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-observability", name: "vela-slo-dashboard-r1"},
		{apiVersion: "monitoring.coreos.com/v1", kind: "PodMonitor", namespace: "vela-observability", name: "vela-control"},
	},
	"vela-control": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "v1", kind: "ServiceAccount", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-control-node-agents-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-control-runtime-r1"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-api"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-compliance"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-finance-reconciliation"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-worker-control"},
		{apiVersion: "scheduling.k8s.io/v1", kind: "PriorityClass", name: "vela-control-critical"},
		{apiVersion: "apps/v1", kind: "Deployment", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-api"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-compliance"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-finance"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-fleet"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-observability"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-worker"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-default-deny-ingress"},
	},
	"worker-agent": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "v1", kind: "ServiceAccount", namespace: "vela-system", name: "vela-worker"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "vela-h3-worker"},
		{apiVersion: "apps/v1", kind: "DaemonSet", namespace: "vela-system", name: "vela-h3-worker"},
	},
}

func writeCatalogReleaseBundleFixture(t *testing.T, directory, variant string) releasebundle.Bundle {
	t.Helper()
	controlManifest := writeCatalogOCIManifest(t, directory, "control")
	workerManifest := writeCatalogOCIManifest(t, directory, "worker")
	images := []string{
		"ghcr.io/vivym/vela-control@" + controlManifest.digest,
		"ghcr.io/vivym/vela-worker-agent@" + workerManifest.digest,
	}
	for index, name := range []string{"control-storage", "fleet-controller", "observability", "vela-control", "worker-agent"} {
		writeCatalogReleaseFile(
			t,
			filepath.Join(directory, "render-"+name+".yaml"),
			[]byte(catalogReleaseRender(name, images[index%len(images)], images)),
		)
	}
	writeCatalogReleaseFile(t, filepath.Join(directory, "node-agent.service"), []byte(`[Unit]
Description=Vela host remediation Node Agent
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/vela-node-agent
EnvironmentFile=/etc/vela/node-agent.env
Restart=on-failure
RestartSec=5s
UMask=0077
RuntimeDirectory=vela-node-agent
RuntimeDirectoryMode=0755
StateDirectory=vela-node-agent
ProtectHome=true
PrivateTmp=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
LockPersonality=true
MemoryDenyWriteExecute=true
NoNewPrivileges=false
LimitNOFILE=4096

[Install]
WantedBy=multi-user.target
`))

	packages := make([]releasebundle.PackageInput, 0, 2)
	for _, name := range []string{"h3-runner", "node-agent"} {
		artifactRef := name + ".tar"
		artifact := []byte("production package for " + name + variant)
		writeCatalogReleaseFile(t, filepath.Join(directory, artifactRef), artifact)
		entrypoint := "/opt/vela/bin/vela-h3-runner"
		if name == "node-agent" {
			entrypoint = "/usr/local/bin/vela-node-agent"
		}
		contractRef := name + "-contract.json"
		writeJSONFixture(t, filepath.Join(directory, contractRef), releasebundle.PackageContract{
			SchemaVersion:     1,
			Name:              "vela-" + name,
			OS:                "linux",
			Architecture:      "amd64",
			Revision:          "release-r1",
			Entrypoint:        entrypoint,
			ArtifactDigest:    catalogReleaseContentDigest(artifact),
			ArtifactSizeBytes: int64(len(artifact)),
		})
		packages = append(packages, releasebundle.PackageInput{
			Name: name, ContractRef: contractRef, ArtifactRef: artifactRef,
		})
	}

	worker := releasebundle.WorkerMaterializationInput{
		NodeIdentity: "h3-node-01", Namespace: "vela-system",
		WorkerID: "10000000-0000-0000-0000-000000000001", WorkerEpoch: 7,
		WorkerPoolID: "20000000-0000-0000-0000-000000000001", FleetRevision: strings.Repeat("a", 64),
		WorkerRuntimeConfigMap: "worker-runtime-node-1", WorkerRuntimeRef: "worker-runtime.yaml",
		RunnerProfilesConfigMap: "runner-profiles-node-1", RunnerProfilesRef: "runner-profiles.yaml",
		RunnerGPURolesConfigMap: "runner-gpu-roles-node-1", RunnerGPURolesRef: "runner-gpu-roles.yaml",
		WorkerControlTLSSecret: "worker-control-tls-node-1", WorkerControlTLSSecretRevision: catalogReleaseDigest("worker tls"),
		ExecutionProfileRevisionID: "30000000-0000-0000-0000-000000000001",
		InferenceBackendRevision:   "h3-backend-r1", ModelRevisionID: "40000000-0000-0000-0000-000000000001",
	}
	worker.NodeAgentIdentity = "spiffe://vela.internal/node-agent/" +
		base64.RawURLEncoding.EncodeToString([]byte(worker.NodeIdentity)) + "/" + worker.WorkerID
	writeCatalogWorkerMaterialization(t, directory, worker)
	writeCatalogFleetDesiredRender(t, directory, worker, images)

	plan := releasebundle.BuildPlan{
		SchemaVersion: 1,
		NodeAgentUnit: releasebundle.ArtifactInput{
			Name: "node-agent-systemd-unit", Ref: "node-agent.service",
		},
		Packages:               packages,
		WorkerMaterializations: []releasebundle.WorkerMaterializationInput{worker},
		ExternalResources: []releasebundle.ExternalResource{
			{
				Kind: "Secret", Namespace: "vela-system", Name: "artifact-store-ca-r1", Revision: catalogReleaseDigest("artifact ca"),
				RequiredKeys: []string{"ca.crt"}, Consumers: []string{
					"ConfigMap/vela-system/vela-fleet-desired-r1",
					"DaemonSet/vela-system/vela-h3-worker",
				},
			},
			{
				Kind: "Secret", Namespace: "vela-observability", Name: "shared-secret-r1", Revision: catalogReleaseDigest("observability shared"),
				RequiredKeys: []string{"token"}, Consumers: []string{"PodMonitor/vela-observability/vela-control"},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "shared-secret-r1", Revision: catalogReleaseDigest("shared"),
				RequiredKeys: []string{"token"}, Consumers: []string{
					"DaemonSet/vela-system/vela-h3-worker",
					"Deployment/vela-system/vela-control",
					"Deployment/vela-system/vela-fleet-controller",
					"StatefulSet/vela-system/nats",
				},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: worker.WorkerControlTLSSecret,
				Revision:     worker.WorkerControlTLSSecretRevision,
				RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"},
				Consumers: []string{
					"ConfigMap/vela-system/vela-fleet-desired-r1",
					"DaemonSet/vela-system/vela-h3-worker",
					"WorkerMaterialization/vela-system/" + worker.NodeIdentity + "/" + worker.WorkerID,
				},
			},
		},
		OCIManifests: []releasebundle.OCIManifestInput{
			{Image: images[0], Ref: controlManifest.ref, ConfigRef: controlManifest.configRef},
			{Image: images[1], Ref: workerManifest.ref, ConfigRef: workerManifest.configRef},
		},
	}
	for _, name := range []string{"control-storage", "fleet-controller", "observability", "vela-control", "worker-agent"} {
		plan.FinalRenders = append(plan.FinalRenders, releasebundle.ArtifactInput{Name: name, Ref: "render-" + name + ".yaml"})
	}
	planPath := filepath.Join(directory, "release-bundle-plan.json")
	writeJSONFixture(t, planPath, plan)
	bundle, encoded, err := releasebundle.Build(planPath)
	if err != nil {
		t.Fatalf("build Catalog release bundle fixture: %v", err)
	}
	writeCatalogReleaseFile(t, filepath.Join(directory, "release-bundle.json"), encoded)
	return bundle
}

func writeCatalogFleetDesiredRender(
	t *testing.T,
	directory string,
	worker releasebundle.WorkerMaterializationInput,
	images []string,
) {
	t.Helper()
	render := catalogReleaseRender("fleet-controller", images[1], images)
	identity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: vela-fleet-desired-r1\n  namespace: vela-system\n"
	desired := fmt.Sprintf(`apiVersion: fleet.vela.ai/v1alpha1
kind: FleetDesiredRevisions
revisions:
  - workerPoolID: %s
    name: h3-worker-pool-primary
    revision: %s
    workerProfile: h3
    nodeSelector:
      vela.ai/worker-profile: h3
      vela.ai/worker-pool: launch
    initImage: %s
    workerAgentImage: %s
    runnerImage: %s
    artifactStoreTLSSecret: artifact-store-ca-r1
    executionProfileRevisionID: %s
    inferenceBackendRevision: %s
    readinessTimeout: 30m
    capacityPolicy:
      workerHighWatermarkBytes: 800
      workerLowWatermarkBytes: 400
      workerCriticalFreeBytes: 100
      poolHighWatermarkBytes: 5600
      poolLowWatermarkBytes: 2800
      observationMaxAge: 2m
    placements:
      - nodeIdentity: %s
        daemonSetName: h3-worker-pool-primary-node-01
        workerRuntimeConfigMap: %s
        runnerProfilesConfigMap: %s
        runnerGPURolesConfigMap: %s
        workerControlTLSSecret: %s
retirements: []
`, worker.WorkerPoolID, worker.FleetRevision, images[0], images[1], images[1],
		worker.ExecutionProfileRevisionID, worker.InferenceBackendRevision,
		worker.NodeIdentity, worker.WorkerRuntimeConfigMap, worker.RunnerProfilesConfigMap,
		worker.RunnerGPURolesConfigMap, worker.WorkerControlTLSSecret)
	data := "data:\n  desired.yaml: |\n    " + strings.ReplaceAll(strings.TrimSuffix(desired, "\n"), "\n", "\n    ") + "\n"
	writeCatalogReleaseFile(
		t,
		filepath.Join(directory, "render-fleet-controller.yaml"),
		[]byte(strings.Replace(render, identity, identity+data, 1)),
	)
}

func relocateCatalogReleaseBundleFixture(
	t *testing.T,
	directory,
	bundleRef,
	destinationDirectory string,
) string {
	t.Helper()
	bundle, err := releasebundle.LoadWithin(directory, bundleRef)
	if err != nil {
		t.Fatalf("load Catalog release bundle before relocation: %v", err)
	}
	references := make(map[string]struct{})
	for _, render := range bundle.ConfigurationManifest.FinalRenders {
		references[render.Artifact.Ref] = struct{}{}
	}
	references[bundle.ConfigurationManifest.NodeAgentUnit.Artifact.Ref] = struct{}{}
	for _, item := range bundle.ConfigurationManifest.Packages {
		references[item.Contract.Ref] = struct{}{}
		references[item.Artifact.Ref] = struct{}{}
	}
	for _, item := range bundle.ConfigurationManifest.WorkerMaterializations {
		references[item.WorkerRuntime.Ref] = struct{}{}
		references[item.RunnerProfiles.Ref] = struct{}{}
		references[item.RunnerGPURoles.Ref] = struct{}{}
	}
	for _, image := range bundle.OCIImages {
		references[image.Descriptor.Ref] = struct{}{}
		references[image.Config.Ref] = struct{}{}
	}
	for reference := range references {
		source := filepath.Join(directory, filepath.FromSlash(reference))
		destination := filepath.Join(directory, destinationDirectory, filepath.FromSlash(reference))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			t.Fatalf("create nested Catalog release artifact directory: %v", err)
		}
		if err := os.Rename(source, destination); err != nil {
			t.Fatalf("relocate Catalog release artifact %s: %v", reference, err)
		}
	}
	nestedBundleRef := filepath.ToSlash(filepath.Join(destinationDirectory, filepath.Base(bundleRef)))
	if err := os.Rename(
		filepath.Join(directory, filepath.FromSlash(bundleRef)),
		filepath.Join(directory, filepath.FromSlash(nestedBundleRef)),
	); err != nil {
		t.Fatalf("relocate Catalog release bundle: %v", err)
	}
	return nestedBundleRef
}

type catalogOCIManifest struct {
	ref       string
	configRef string
	digest    string
}

func writeCatalogOCIManifest(t *testing.T, directory, name string) catalogOCIManifest {
	t.Helper()
	config, err := json.Marshal(map[string]any{
		"architecture": "amd64",
		"os":           "linux",
		"rootfs":       map[string]any{"type": "layers", "diff_ids": []string{}},
	})
	if err != nil {
		t.Fatalf("encode Catalog OCI config: %v", err)
	}
	configRef := "oci-" + name + "-config.json"
	writeCatalogReleaseFile(t, filepath.Join(directory, configRef), config)
	manifest, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     releasebundle.OCIManifestMediaType,
		"config": map[string]any{
			"mediaType": releasebundle.OCIImageConfigMediaType,
			"digest":    catalogReleaseContentDigest(config),
			"size":      len(config),
		},
		"layers": []any{map[string]any{
			"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
			"digest":    catalogReleaseDigest(name + " layer"),
			"size":      256,
		}},
	})
	if err != nil {
		t.Fatalf("encode Catalog OCI manifest: %v", err)
	}
	ref := "oci-" + name + ".json"
	writeCatalogReleaseFile(t, filepath.Join(directory, ref), manifest)
	return catalogOCIManifest{ref: ref, configRef: configRef, digest: catalogReleaseContentDigest(manifest)}
}

func catalogReleaseRender(name, image string, images []string) string {
	var rendered strings.Builder
	for _, resource := range catalogReleaseInventory[name] {
		isWorkload := resource.kind == "StatefulSet" || resource.kind == "Deployment" ||
			resource.kind == "DaemonSet" || (name == "observability" && resource.kind == "PodMonitor")
		if !isWorkload {
			rendered.WriteString(catalogReleaseObject(resource))
			continue
		}
		document := catalogReleaseWorkload(resource, image)
		if name == "worker-agent" && resource.kind == "DaemonSet" {
			document = strings.Replace(document, "spec:\n", "spec:\n"+
				"  workerRuntimeConfigMap: worker-runtime-node-1\n"+
				"  runnerProfilesConfigMap: runner-profiles-node-1\n"+
				"  runnerGPURolesConfigMap: runner-gpu-roles-node-1\n"+
				"  workerControlTLSSecret: worker-control-tls-node-1\n"+
				"  artifactStoreTLSSecret: artifact-store-ca-r1\n"+
				"  initImage: "+images[0]+"\n"+
				"  workerAgentImage: "+images[1]+"\n"+
				"  runnerImage: "+images[1]+"\n", 1)
		}
		rendered.WriteString(document)
	}
	return rendered.String()
}

func catalogReleaseObject(resource catalogReleaseResource) string {
	encoded := "apiVersion: " + resource.apiVersion + "\nkind: " + resource.kind +
		"\nmetadata:\n  name: " + resource.name + "\n"
	if resource.namespace != "" {
		encoded += "  namespace: " + resource.namespace + "\n"
	}
	return encoded + "---\n"
}

func catalogReleaseWorkload(resource catalogReleaseResource, image string) string {
	return "apiVersion: " + resource.apiVersion + "\nkind: " + resource.kind +
		"\nmetadata:\n  name: " + resource.name + "\n  namespace: " + resource.namespace + `
spec:
  template:
    spec:
      containers:
        - name: service
          image: ` + image + `
      volumes:
        - name: credentials
          secret:
            secretName: shared-secret-r1
            items:
              - key: token
                path: token
---
`
}

func writeCatalogWorkerMaterialization(
	t *testing.T,
	directory string,
	worker releasebundle.WorkerMaterializationInput,
) {
	t.Helper()
	runtime := `apiVersion: v1
kind: ConfigMap
metadata:
  name: ` + worker.WorkerRuntimeConfigMap + `
  namespace: ` + worker.Namespace + `
immutable: true
data:
  artifact-store-health-url: https://artifacts.example.com/healthz
  attempt-quota-bytes: "1000000"
  control-address: control.vela-system.svc:9443
  control-server-name: control.vela-system.svc
  critical-free-bytes: "100"
  high-watermark-bytes: "800"
  low-watermark-bytes: "700"
  max-entries: "1000"
  max-entry-bytes: "1000000"
  output-cleanup-min-bytes-per-second: "100000"
  xfs-device: /dev/nvme0n1
  xfs-project-id: "1001"
`
	writeCatalogReleaseFile(t, filepath.Join(directory, worker.WorkerRuntimeRef), []byte(runtime))
	profiles, err := json.Marshal(map[string]any{
		"schema_version":   1,
		"backend_revision": worker.InferenceBackendRevision,
		"profiles": []any{map[string]any{
			"model_revision_id":             worker.ModelRevisionID,
			"generation_preset_revision_id": "50000000-0000-0000-0000-000000000001",
			"execution_profile_revision_id": worker.ExecutionProfileRevisionID,
			"output_spec_id":                "60000000-0000-0000-0000-000000000001",
		}},
	})
	if err != nil {
		t.Fatalf("encode Catalog runner profiles: %v", err)
	}
	writeCatalogReleaseFile(t, filepath.Join(directory, worker.RunnerProfilesRef), []byte(
		catalogReleaseConfigMap(worker.Namespace, worker.RunnerProfilesConfigMap, "profiles.json", string(profiles)),
	))
	roles, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"encoder_vae":    "GPU-00000000-0000-0000-0000-000000000001",
		"dit": []string{
			"GPU-00000000-0000-0000-0000-000000000002",
			"GPU-00000000-0000-0000-0000-000000000003",
			"GPU-00000000-0000-0000-0000-000000000004",
			"GPU-00000000-0000-0000-0000-000000000005",
			"GPU-00000000-0000-0000-0000-000000000006",
			"GPU-00000000-0000-0000-0000-000000000007",
			"GPU-00000000-0000-0000-0000-000000000008",
		},
	})
	if err != nil {
		t.Fatalf("encode Catalog runner GPU roles: %v", err)
	}
	writeCatalogReleaseFile(t, filepath.Join(directory, worker.RunnerGPURolesRef), []byte(
		catalogReleaseConfigMap(worker.Namespace, worker.RunnerGPURolesConfigMap, "gpu-roles.json", string(roles)),
	))
}

func catalogReleaseConfigMap(namespace, name, key, value string) string {
	return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n  namespace: " + namespace +
		"\nimmutable: true\ndata:\n  " + key + ": |-\n    " + strings.ReplaceAll(value, "\n", "\n    ") + "\n"
}

func writeCatalogReleaseFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write Catalog release fixture: %v", err)
	}
}

func catalogReleaseDigest(value string) string {
	return catalogReleaseContentDigest([]byte(value))
}

func catalogReleaseContentDigest(content []byte) string {
	digest := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(digest[:])
}
